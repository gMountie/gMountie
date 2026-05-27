# Operations & Packaging

How gMountie is built, packaged, and deployed: the container image, the Helm
chart, the docker-compose example, and the release supply chain. The goal is
that the artifacts we ship are deployable by a careful operator without
surprises.

## Container image

- **Pinned base.** `FROM alpine:3.20` pinned by its multi-arch (amd64+arm64)
  digest, for reproducible builds. Bumped manually until Dependabot owns it.
- **Non-root.** An unprivileged `gmountie` user (uid 1000) runs the binary.
  uid 1000 is deliberately the same across the image, the Helm
  `securityContext.runAsUser`, and the compose `user:` mapping, so data-dir
  ownership lines up everywhere.
- **Healthcheck.** A `wget`-based `HEALTHCHECK` hits the Phase 2 ops HTTP
  server (`/healthz` on the metrics port). Alpine keeps a shell, so this needs
  zero Go code — the reason we pin Alpine rather than distroless (which would
  need a dedicated `gMountie healthcheck` subcommand) or scratch (no in-image
  healthcheck at all).
  - *Known coupling:* the port (`9090`) is hardcoded in the healthcheck. A deploy
    that overrides `server.metrics_addr` makes the Docker healthcheck stale;
    Kubernetes probes are unaffected because they read the port from the chart's
    values.
- **No build stage.** goreleaser supplies the prebuilt static binary
  (`CGO_ENABLED=0`), so the image just copies it in — there is no compile stage.
  OCI labels are set by goreleaser (`dockers_v2.labels`), not the Dockerfile.

## Helm chart (`deployments/charts/gmountie-server`)

- **`podSecurityContext.fsGroup: 1000` is required, not cosmetic.** The chart
  runs as `runAsUser: 1000`, but the ReadWriteOnce PVC is created root-owned, so
  without an `fsGroup` the non-root container cannot write its data directory.
  `fsGroup: 1000` makes the volume group-writable by the runtime user.
- **Conservative default resources.** `requests` cpu `100m` / memory `128Mi`,
  with a memory limit of `512Mi` and **no CPU limit** (CPU limits throttle
  rather than protect). Operators raise these per cluster; cache size drives
  memory.
- Already correct (not changed by Phase 6): liveness `/healthz` + readiness
  `/readyz` probes, a hardened `securityContext` (`runAsNonRoot`, `drop: [ALL]`,
  `allowPrivilegeEscalation: false`), and `image.tag` falling back to
  `Chart.AppVersion`.

## docker-compose example (`deployments/compose`)

- **Least-privilege data dir.** A one-shot init container `chown 1000:1000
  /data` (replacing an old `chmod 777` sidecar); the server then runs as
  `user: "1000:1000"`.
- **Credentials are file-based, not env.** gMountie has **no env-var override
  for `auth.users`** — only `auth.type` is env-bound; the user list is
  unmarshalled from the `auth` config subtree. So compose cannot inject
  credentials via `.env`. `config.yaml` ships a `CHANGE_ME_BEFORE_DEPLOY`
  placeholder under a loud "demo only" warning; `.env.example` covers only the
  knobs Compose *can* interpolate (image tag, host port mappings).

## Release & supply chain

Releases are cut by goreleaser from the `Release` workflow (`workflow_dispatch`,
gated on green CI).

- **Keyless signing (cosign + Sigstore/Fulcio).** Signed via the GitHub Actions
  OIDC identity and recorded in Rekor — no private key to manage. goreleaser
  signs both the container image/manifest list (`docker_signs`, `artifacts:
  all`) and the `checksums.txt` file (`signs` → `sign-blob`).
- **cosign is pinned to v2.5.2 in the workflow.** goreleaser's `signs` block
  uses `sign-blob --output-certificate/--output-signature`, which cosign **v3
  removed**. Do not bump `cosign-installer` without first migrating that block
  to v3's `--bundle` syntax.
- **SBOMs.** Per-archive SPDX SBOMs (goreleaser `sboms`, syft) are uploaded as
  release assets. The `release` block has **no `ids` filter** — an earlier
  `ids: [gMountie]` silently dropped the SBOMs and signatures from the upload.
- **No image SBOM attestation (deliberate).** `cosign attest --type spdx
  --predicate <syft-spdx>` mangles the SPDX predicate: cosign v2.5.2 embeds it
  as a JSON *string* (verifiable only by cosign v2.x), and `--new-bundle-format`
  produced an attestation no cosign version can verify; cosign v3's `attest`
  errors on the same input. Since the image is signed and the archive SBOMs
  ship, the in-registry image SBOM isn't worth the breakage. If it's wanted
  later, use [`actions/attest-sbom`](https://github.com/actions/attest-sbom),
  which embeds the SBOM as a proper predicate — not the `cosign attest` path.
- **Build flags:** `-trimpath` and `-buildvcs=true`.

**Verify a released image:**

```
cosign verify \
  --certificate-identity-regexp 'https://github.com/gMountie/gMountie/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/gmountie/gmountie-server:<tag>
```

## Out of scope

macOS / Windows server builds, a Kubernetes operator, and desktop release
artifacts (the Wails desktop build is deferred to Phase 8).
