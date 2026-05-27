# Phase 6 — Operations & Packaging — Design

**Status:** approved 2026-05-27
**Roadmap:** [Phase 6 — Operations and packaging](../../roadmap.md) (Planned)
**Transient:** consolidate the durable bits into `docs/design/` on completion, then prune this file.

## Goal

The artifacts we ship are deployable by a careful operator: a hardened
non-root container image with a healthcheck, a Helm chart whose non-root pod
can actually write its data volume, a docker-compose example without the
`chmod 777` footgun, and signed release artifacts with SBOMs.

## Scope reconciliation (the roadmap is stale)

The roadmap's Phase 6 bullet list pre-dates several incremental fixes. Verified
state on `master` @ `1d637a3`:

| Roadmap claim | Actual state | Remaining work |
|---|---|---|
| Helm "probes commented out" | liveness `/healthz` + readiness `/readyz` already wired | none |
| Helm "empty securityContext" | already `runAsNonRoot:1000`, `drop:[ALL]`, `allowPrivilegeEscalation:false` | none |
| Helm "mutable `image.tag: master`" | `tag:""` → falls back to `Chart.AppVersion` | none |
| Helm "empty podSecurityContext / resources" | both `{}` | **fsGroup for PVC writes; default resources** |
| Dockerfile multi-stage/non-root/HEALTHCHECK/labels | `FROM alpine:latest`, root, no HEALTHCHECK; OCI labels live in goreleaser `dockers_v2.labels` | **pin base, non-root USER, HEALTHCHECK** |
| compose `chmod 777` sidecar + `admin/admin` | true | **uid/gid mapping + `.env`** |
| goreleaser SBOM/cosign/trimpath/buildvcs | `-trimpath` present; no SBOM, no signing | **SBOM + keyless cosign; add `-buildvcs=true`** |

## Decisions

- **Base image:** Alpine pinned to a digest + `wget`-based `HEALTHCHECK`. Keeps a
  shell so the healthcheck needs zero Go code. (Rejected: distroless — would
  require a new `gMountie healthcheck` subcommand; scratch — drops the
  roadmap's in-image HEALTHCHECK requirement.)
- **Signing:** cosign **keyless** (Sigstore/Fulcio OIDC via GitHub Actions
  identity, recorded in Rekor). No private key to manage. (Rejected:
  key-based — adds key storage/rotation burden.)
- **Delivery:** three file-disjoint PRs off `master`, landable in any order.

## PR1 — Dockerfile + compose hardening

**Files:** `Dockerfile`, `deployments/compose/docker-compose.yaml`,
`deployments/compose/config.yaml`, `deployments/compose/.env.example` (new).

- **Dockerfile:**
  - Pin `FROM alpine:3.20@sha256:<resolved>` (resolve the digest at PR time;
    Phase 5's Dependabot will own future bumps — manual pin for now).
  - `RUN adduser -D -u 1000 gmountie`; `USER gmountie`.
  - `HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 CMD wget -q -O- http://127.0.0.1:9090/healthz || exit 1`.
  - Keep the `COPY $TARGETPLATFORM/gMountie` flow (goreleaser supplies the
    built binary; no build stage needed under `dockers_v2`).
  - *Known coupling (documented, not fixed):* the HEALTHCHECK hardcodes metrics
    port `9090`. A deploy that overrides `metrics_addr` makes the Docker
    healthcheck stale; k8s probes still read the port from `values.yaml`.
    Exposing it via env is scope creep — deferred.
- **compose:**
  - Replace the `fix-permissions` sidecar (`chmod 777 /data`) with a one-shot
    `chown 1000:1000 /data` init (least-privilege, still runs as root then
    exits) and set `user: "1000:1000"` on the `server` service.
  - **Credentials:** the roadmap's "move creds to `.env`" assumes the server
    can read basic-auth creds from env vars. It can't — `pkg/server/config`
    only env-binds a fixed key list (`auth.type` is bound, but the
    `auth.users[]` list is `Unmarshal`ed from `v.Sub("auth")`, with no
    per-user env override). Honoring that intent would require a server config
    change, which would push PR1 beyond compose. **Decision:** keep
    `config.yaml` as a file; replace `admin/admin` with a placeholder
    `CHANGE_ME_BEFORE_DEPLOY` under a loud `DEMO ONLY — CHANGE` comment.
  - Add `deployments/compose/.env.example` documenting only the knobs Compose
    *can* interpolate from the compose file: image tag and host port mappings
    (`GMOUNTIE_IMAGE_TAG`, `GRPC_PORT`, `METRICS_PORT`). Wire them into the
    compose file as `${VAR:-default}`.

## PR2 — Helm chart hardening

**Files:** `deployments/charts/gmountie-server/values.yaml`.

- `podSecurityContext.fsGroup: 1000` — the ReadWriteOnce PVC is otherwise
  root-owned and the `runAsUser:1000` container cannot write it. This is a real
  latent bug today, not cosmetic.
- Default `resources`: `requests` `cpu:100m` / `memory:128Mi`; memory limit
  `512Mi`; **no CPU limit** (CPU limits cause throttling). Operators override
  per cluster.
- Probes / securityContext / image.tag fallback already correct — untouched.

## PR3 — goreleaser SBOM + keyless signing

**Files:** `.goreleaser.yaml`, `.github/workflows/release.yml`.

- **SBOM (both archive and image):**
  - `sboms:` block → SPDX attached to archives (syft default).
  - Image SBOM → `cosign attest --type spdx --predicate <sbom> <image>@<digest>`
    as a step after `goreleaser release` (image SBOM is what supply-chain
    consumers look for).
- **Keyless signing in goreleaser:**
  - `docker_signs:` → `cosign sign --yes ${artifact}@${digest}` (`artifacts: all`).
  - `signs:` → `cosign sign-blob --yes ...` over `checksum` artifacts.
  - Add `-buildvcs=true` to `builds[].flags` (alongside existing `-trimpath`).
- **release.yml prerequisites:**
  - Add `id-token: write` to the job `permissions` (currently only
    `contents: write`, `packages: write`).
  - Add a `sigstore/cosign-installer@v3` step before the GoReleaser action.
- **Snapshots:** `--snapshot` does not push images, so `docker_signs` and the
  image-attest step no-op naturally — no conditional needed.

### Definition of done (testable)

- `docker run` of the built image as the non-root user serves a volume; the
  container reports `healthy`.
- `helm install` with default values produces a pod that passes its readiness
  probe and can write to its PVC (fsGroup verified).
- After an alpha release, signature verification succeeds:
  ```
  cosign verify \
    --certificate-identity-regexp 'https://github.com/gMountie/gMountie/.github/workflows/release.yml@.*' \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    ghcr.io/gmountie/gmountie-server:<tag>
  ```
- `goreleaser release --snapshot --clean` succeeds locally with the new
  SBOM/sign config present (signing steps skipped on snapshot).

## Out of scope

- macOS / Windows server builds; Kubernetes operator; desktop release artifacts
  (Phase 8). The `pkg/errors`→stdlib migration and proto package rename (Phase 5)
  are deliberately sequenced after the open `codecv2-zerocopy` worktree lands.
