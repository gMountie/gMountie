---
id: roadmap
title: Roadmap
sidebar_label: Roadmap
description: What's shipped, what's in flight, what's planned. Phase ordering and the rationale behind it.
---

# Roadmap

**Status / last refreshed:** 2026-06-09

**Legend:**
- **Done** — shipped; see linked design docs for the implementation record.
- **In progress** — currently being worked.
- **Planned** — scoped and ordered; not yet started.
- **Deferred** — acknowledged but explicitly not yet scheduled.

---

## Project north star

gMountie exists to be an **NFS-over-the-internet replacement** — the user wants to mount their server's storage from anywhere on the public internet, with no VPN, and UX as simple as mounting a local NFS share. Every design decision below is evaluated against that goal first. In practice:

- **Latency tolerance is core, not an afterthought.** Anything that adds RTTs per syscall (chatty metadata, no caching, no readahead, no batching) is a bug, not a polish item.
- **Reconnect/resume must be invisible.** Network blips are expected; mounts and open file handles must survive them.
- **Single-user-ish trust model, for now.** Multi-tenant security is not the priority, but the wire is the public internet — the security hardening phase (Phase 7, now **shipped**) made TLS, mandatory auth, hashed credentials, ACLs, and kernel-native identity enforcement the baseline.

## What this is, and what it is not

This is a roadmap, not an implementation plan. Each phase below is scoped tightly enough to spawn its own design + implementation plan when it's time to do the work. The intent is one shared document that captures priorities, explicit non-goals, and the gaps we are knowingly deferring — so a future session can pick up the right thread without re-doing the analysis.

This roadmap deliberately does not target "production-ready" in the strict sense. It targets **functional reliability, observable behavior, good internet performance, and the headline client-cache feature** — with security hardening tracked separately.

**The desktop UI has been extracted to a separate repository.** Phases 1–7 targeted the CLI (`gmountie mount`, `gmountie serve`), the shared library (`pkg/client/`, `pkg/server/`, `pkg/common/`), and the protocol (`api/proto/`). The Wails desktop app and VFS multi-volume mounter have been removed from this repo and extracted to a future separate desktop repo. This repo is CLI + library only.

## What "works perfectly end-to-end" means

A successful endpoint of Phases 1–6 (CLI and library only; desktop UI is a separate repo):

- The CLI client mounts a volume from a server on the public internet (real DNS, real NAT, real ISP-grade RTT in the tens of milliseconds) with one command.
- That mount survives a 30-second network drop, an ISP IP renumber, and a server restart — without manual `umount` or re-mount.
- File handles open before a network blip remain valid after reconnect.
- Reading a file already in the local cache hits the network only to validate freshness, not to fetch bytes. A cold-cache read of a multi-GB file streams without hitting the 4 MiB unary ceiling.
- `gmountie serve` runs for days under typical workloads without crashing, leaking file descriptors, or accumulating zombie state.
- SIGTERM to the server completes in-flight RPCs and shuts down cleanly.
- Performance targets (measured against `test/e2e/fs/io_bench_test.go`):
  - **localhost:** sequential read of 1 GiB ≥ 70% of raw loopback FUSE throughput; metadata ops in single-digit milliseconds.
  - **internet (≥ 20 ms RTT):** sequential read of a cached 1 GiB file ≥ 80% of local disk throughput (cache hit path); cold-cache read ≥ 70% of available network throughput; `ls` on a 1000-entry directory completes in well under a second on warm metadata cache.
- The two previously-skipped tests in `pkg/server/config/config_test.go` are passing.
- Operators can answer "what version is running?", "is the server healthy?", "how many open files are there per volume right now?", and "what's the per-volume error rate?" without reading source code.
- Every new feature path has a unit test, and every previously-broken failure mode is covered by an e2e or integration test.

**Explicitly not** part of success criteria, even by the end of Phase 6:

- TLS transport.
- Authenticated/authorized access beyond the existing basic-auth-or-none knob.
- Resistance to a hostile client.
- Multi-tenant safety.

These are Phase 7 work; they are listed in Appendix A so they don't get forgotten.

## Why this ordering

Reliability comes before everything because a crashing or hanging server makes every other improvement invisible. Inside Phase 1 we also add the *session* concept and *idempotency tokens* — they're reliability prerequisites (no fd reclamation across reconnect = no reliable mount over the internet) but they're also protocol changes that everything downstream depends on, so they happen early and once.

Observability comes before performance because you cannot tune what you cannot measure, and the same instrumentation that diagnoses a perf regression diagnoses a reliability regression.

Performance comes before the cache because the cache layer reuses the streaming RPCs, and because measuring cache effectiveness needs a stable perf baseline.

The client cache is its own phase, not a perf sub-task, because it touches the protocol (invalidation signals), the client architecture (unifying the two-layer seam between `pathfs.FileSystem` and `GrpcFile`), persistence (on-disk format reusable across process restarts), and configuration. It's the headline feature for the internet-NFS goal.

Quality / deps / docs and Ops & packaging follow. Security is next-to-last, but capped (see Phase 7). The desktop UI is in a separate repo.

---

## Phase 1 — Functional reliability + session and idempotency foundations — **Done**

See [Architecture & Protocol](design/architecture.md) for the implementation record.

**Goal:** the server doesn't crash on adversarial-but-non-malicious input, the client doesn't hang or lose state when something goes wrong, and the protocol gets the minimal changes needed to make safe retry and reconnect possible.

**Delivered:**

1. Removed every `log.Fatal` from the request path (metrics port collision, `setfsuid`/`setfsgid` failure, logger init, FUSE mount error — all converted to returned errors or best-effort goroutines).
2. Fixed nil-deref panics in controllers (`utils.go` nil-guards for `caller.Owner`; `fs.go` nil guard on `StatFs` reply).
3. Wired `GracefulStop` to SIGTERM / SIGINT with a bounded shutdown deadline.
4. Propagated FUSE-thread contexts with per-RPC timeouts through every client RPC call (replacing `context.Background()`).
5. Introduced the session concept: `SessionID` established on connect, fd table scoped to `(session, fd)`, reap on peer disconnect.
6. Added idempotency tokens to all mutating proto messages; server keeps an LRU cache of `(session_id, request_id)` → reply.
7. Wired `retry-go` retries on transient gRPC errors (`Unavailable`, `DeadlineExceeded`) across client I/O paths, with bounded backoff.
8. Fixed server-side fd lifecycle: entries are no longer leaked when `Open`/`Create` returns a non-OK status.
9. Un-skipped the two config tests (`TestParse_EmptyConfig`, `TestParse_EnvVarOverride`).

---

## Phase 2 — Observability — **Done**

See [Architecture & Protocol](design/architecture.md) for the implementation record.

**Goal:** enough instrumentation that the next time something is slow, broken, or weird, the answer is in a log line or a metric.

**Delivered:**

1. JSON logging in non-TTY mode; console format when stderr is a terminal; configurable.
2. Per-RPC request ID + session ID + user + volume + op as log fields; joinable across client and server log lines.
3. Per-volume / per-op business metrics: `gmountie_server_open_files`, `gmountie_server_bytes_total`, `gmountie_server_rpc_errors_total`, `gmountie_server_request_duration_seconds`, `gmountie_server_sessions_active`; client-side: `gmountie_client_retry_total`, `gmountie_client_in_flight`, and cache metrics (landed with Phase 4).
4. gRPC health protocol + `/healthz` / `/readyz` endpoints.
5. `Version` gRPC + `/version` HTTP endpoint wired to `pkg.GetBuildInfo()`.
6. Ops HTTP server (metrics + health) address configurable via the `server.ops.addr` config key.

**Deferred (still deferred):**
- OpenTelemetry tracing — log-correlation IDs cover most of the value at a fraction of the build complexity.
- Authentication on the metrics endpoint — captured in Appendix A.

---

## Phase 3 — Performance: streaming, batching, tuning — **Done**

See [Performance](design/performance.md) for the streaming/batching optimizations and tuning notes.

**Goal:** measured wins on the fio suite over localhost; preparation of the streaming + batched RPCs that Phase 4's cache depends on.

**Delivered:**

1. **Streaming reads and writes** — `ReadRequest`/`WriteRequest` moved from unary to server-streaming reads / client-streaming writes, eliminating the 4 MiB gRPC ceiling. Per-frame size is tunable; session + request IDs carried through for safe retry.
2. **Compound metadata RPCs** — NFSv4-style `Compound` RPC batches metadata ops into one round-trip. *(Removed in protocol v-next: no client path ever drove it; superseded by the streaming `ReadDir` with per-entry attrs.)*
3. **Compression opt-in** — Snappy is now opt-in rather than always-on; metadata RPCs skip it by default. Frame-size negotiation between client and server.
4. **FUSE mount-option tuning** — `MaxRead`, `MaxWrite`, `MaxBackground`, `CongestionThreshold`, and `WritebackCache` tuned; values negotiated with the server.
5. **Client-side readahead and write coalescing** — sequential read patterns trigger pre-fetch of the next streaming chunk; small sequential writes from a single fd are coalesced into the streaming frame size. See [Performance § readahead](design/performance.md) for the `readahead_window` and `readahead_chunk_bytes` knobs.
6. **gRPC keepalives** — `KeepaliveParams` and `KeepaliveEnforcementPolicy` configured so dead connections surface as Phase 1 retries.

**Out of scope (still out of scope):**
- On-disk cache (Phase 4).
- Server-side content caching.
- Multi-server / replication.

---

## Phase 4 — Persistent client-side cache — **Done**

See [Caching & Consistency](design/caching-and-consistency.md) for the design, consistency model, and configuration reference. See [Performance](design/performance.md) for the related write-path optimizations and Bencher tracking.

**Goal:** the headline user-facing feature. After the first read, the same bytes don't cross the network. The cache survives client restarts.

**Delivered:**

- **Protocol additions:** `Attr.version` (monotonic counter); `GetAttrIfChanged(path, known_version)` shortcut RPC; server-streaming `Subscribe(volume)` for push invalidation; `Read` request optional `version` field.
- **FUSE layer migration:** client migrated from `pathfs` to `go-fuse/v2/fs` (inode-based, stable inode numbers, inode-based file handles; fixes the "ignoring `Ino`" workaround in the old `fs.go`).
- **Unified `FileSystemBackend` interface:** cache and retry middleware sit once and intercept all FUSE ops.
- **Three-tier cache:** attribute cache, directory cache, and chunked data cache (1 MiB default chunks; content-addressable layout).
- **Eviction:** LRU under configurable memory and disk caps (`memory_max_bytes`, `disk_max_bytes`); negative caching for `ENOENT` with short TTL.
- **Persistence:** bbolt (`index.db`) for the LRU index and quota accounting; content-addressable `chunks/` directory; format-versioned for future upgrades; lock file prevents concurrent processes on the same cache dir.
- **Configuration:** `cache.enabled`, `cache.path`, `cache.memory_max_bytes`, `cache.disk_max_bytes`, `cache.chunk_size_bytes`, `cache.attr_ttl`, `cache.dir_ttl`, `cache.negative_ttl`, `cache.subscribe_enabled`.

**Follow-on perf work (shipped after the spec was written):**
- **Cache-fsync reduction** — batch fsync strategy to reduce the per-write fsync overhead on the persistent cache.
- **`WriteAndFlush` fused writes** — coalesces write + flush into a single RPC to reduce WAN round-trips on write-heavy workloads.
- **WAN writeback-cache opt-in** — opt-in write-back mode for single-writer workloads over high-latency links, with documented consistency limitations.
- **`Utimens` RPC** — added to support accurate mtime preservation on writeback paths. *(Later folded into the single-RPC `SetAttr` in protocol v-next.)*
- **Continuous perf tracking via Bencher** — release-gated benchmark series running on the self-hosted VM runner; LAN and WAN (netem) profiles; alert-only. See [Performance § Bencher](design/performance.md).

**Out of scope (still deferred):**
- Write-back caching in general (the opt-in above covers a narrow single-writer case; the general case remains deferred).
- Shared cache across multiple client processes (desktop UI scope, separate repo).
- Encrypted cache at rest.
- Cache pre-warming / explicit prefetch APIs.

---

## Phase 5 — Quality, dependencies, and docs — **Mostly done**

**Goal:** the test suite is trustworthy, the doc copy-paste examples actually work, and dependencies are current.

**Delivered:**

1. **CI hardening (most of it).**
   - ~~Add `-race`~~ **Done** — `task test:race` + a dedicated CI job.
   - ~~`govulncheck` and `Trivy`~~ **Done** — govulncheck job in CI; scheduled Trivy scan of the published image.
   - Dependabot for `go.mod` and GitHub Actions — **Done**. Still open: enable the `npm` ecosystem for `website/` (the docs site; the old `ui/frontend/` is gone).
   - ~~golangci-lint pin mismatch~~ **Done / obsolete** — lint runs a single pinned version via `task lint` (`go run …golangci-lint@v2.x`, config `.golangci.yaml`).
2. **E2E coverage for failure modes** — **Done** (PR #59): auth rejection, server killed mid-read/mid-write, reconnect with open fds, ≥ 4 GiB files (env-gated), concurrent clients; cache hit/miss/persistence suites landed with Phase 4.
3. **Readiness gate** — **Done**: the e2e harness waits on the gRPC health probe instead of sleeping.
4. **Dependency refresh** — cobra is current (v1.10.x). Still open: replace `github.com/pkg/errors` with stdlib `errors` + `%w` incrementally.
5. **Doc fixes** — `auth:` key rename **Done**; `CONTRIBUTING.md` **Done**; the alternatives page **Done** (`docs/comparison.md`).
6. **CLI client config profiles** — **Done** (2026-06-02): shipped as per-file profiles `~/.config/gmountie/profiles/<name>.yaml` selected with `--profile <name>` (not a single `profiles.yaml`); secrets via `auth.password_command` / `auth.password_file`.

**Still open:**

- Real coverage threshold in `vladopajic/go-test-coverage` (badge-only today) — start at the current measured value, ratchet up.
- Dependabot `npm` ecosystem for `website/`.
- Replace `github.com/pkg/errors` incrementally.
- **Proto package rename.** Move `api/proto/*.proto` to `package gmountie.v1;` and `pkg/proto/v1/` for naming clarity. (No wire-compatibility obligation — see Appendix C.)
- An "internet deployment" guide (TLS setup, NAT / firewall, expected latencies, cache sizing recommendations).
- Document the gRPC service-split intent in the proto files (see Appendix B item 6).

**Out of scope:**
- Desktop app work of any kind — separate desktop repo.

---

## Phase 6 — Operations and packaging — **Done**

**Goal:** the artifacts we ship are deployable by a careful operator.

See [Operations & Packaging](design/operations-and-packaging.md) for the
shipped design (hardened non-root image, Helm `fsGroup`/resource defaults,
compose hygiene, keyless release signing + SBOMs).

**In scope:**

1. **Dockerfile.** Make it multi-stage, non-root, `HEALTHCHECK` against the Phase 2 endpoint, OCI labels, minimal runtime image.

2. **Helm chart.** `deployments/charts/gmountie-server` has probes commented out, empty `resources` / `podSecurityContext` / `securityContext`, mutable `image.tag: master`. Wire probes to Phase 2 endpoints; sensible defaults; `runAsNonRoot: true`; parameterised image tag pinned via `appVersion`.

3. **docker-compose example hygiene.** Replace the `fix-permissions` sidecar that `chmod 777`'s the data dir with explicit uid/gid mapping. Move `admin/admin` credentials to a `.env` example file with a "change me" note.

4. **Goreleaser.** SBOM generation, cosign signing for the server binary and Docker image, `-trimpath` / `-buildvcs=true`.

**Out of scope:**
- macOS / Windows server builds (Linux-only server; the macOS client CLI cross-compiles via CGO_ENABLED=0).
- Kubernetes operator.
- Desktop release artifacts — separate desktop repo.

**Definition of done:**
- `docker run` of the released image as a non-root user serves a volume.
- `helm install` with default values produces a pod that passes its readiness probe.
- `cosign verify` succeeds on a released server artifact.

---

## Phase 7 — Security hardening — **Done**

**Goal:** make gMountie deployable on a non-trusted network — the
internet-deployment bar. Shipped across three PRs; see
[Security and Transport](design/security-and-transport.md) for the durable
design.

Delivered:

1. **Transport (PR #53).** Server-TLS terminates every connection;
   zero-config first start auto-generates a self-signed cert (SSH
   host-key pattern) and logs its fingerprint; `gmountie fingerprint`
   reads it back. Client verification modes `verify` / `tofu` (trust-on-
   first-use pin) / `insecure`, plus `expected_fingerprint`. Basic-auth
   now requires transport security (no plaintext credential leak).
2. **Server hardening sweep (PR #54).** argon2id password storage at rest
   + `gmountie genpass` + fail-closed startup on plaintext; ops endpoints
   bind to loopback by default with optional basic-auth; gRPC reflection
   opt-in (default off); per-connection DoS limits (recv size, concurrent
   streams, idle/age).
3. **Identity tightening (PR #55).** Per-user volume ACL
   (`auth.users[].volumes`, `auth.default_allow`, single-chokepoint
   `PrincipalCanAccess`) and mTLS auth (`auth.type: mtls`, client-cert CN
   as principal).

Every gap in [Appendix A](#appendix-a--known-security-gaps) is closed.

**Deferred follow-ups (not blockers):** OIDC/JWT auth, per-connection
byte-rate limiting, OS-keyring client credential storage (the last is a
desktop-UI concern — separate desktop repo).

---

## Phase 8 — Desktop UI — **Extracted to separate repo**

The Wails desktop app (`ui/`, `pkg/ui/`) and the VFS multi-volume mounter (`pkg/client/mount/vfs.go`) have been removed from this repository. The desktop UI and all work that depends on it lives in a dedicated future repo. This repo is CLI + library only (`cmd/`, `pkg/client/`, `pkg/server/`, `pkg/common/`, `pkg/utils/`).

---

## Shipped after Phase 7 (post-phase work, see CHANGELOG for detail)

- **Session-scoped auth + session ownership binding** (v0.7/v0.8) — verify credentials once per session, authorize by `session_id`; sessions bound to the caller identity; `session_id` redacted in logs.
- **Cert revocation without restart** (v0.10) — `auth.revoked_serials` + `POST /ops/acl/reload`; mount-time referrals via `VolumeService.Resolve`.
- **macOS client** (v0.12) and **single-blob `GMOUNTIE_CREDENTIALS` mounts** (v0.14).
- **Server-side `copy_file_range`, `lseek` (`SEEK_DATA`/`SEEK_HOLE`), xattr writes** (v0.15) — see [server-side-copy-and-fs-ops.md](design/server-side-copy-and-fs-ops.md).
- **Server TLS leaf live-reload** (v0.15) — cert rotation without restart on both listeners; see [Security & Transport](design/security-and-transport.md).
- **Resilient mount retry** (v0.16) — transient FS-RPC failures retry within `rpc.retry_window` (default 60 s) with fresh per-attempt deadlines and a session-change guard; `server.session.grace_period` configurable (default 60 s, must stay ≥ the client window).
- **`get.gmountie.dev` install script** (v0.16) — `curl | sh` installer published from `website/static/install.sh`; see [Operations & Packaging](design/operations-and-packaging.md).
- **Session reclaim across server restarts** — client transparently reopens open files against the new server session after a `serve` restart (no external DB), gated on a per-process **boot epoch** (`boot_epoch` on `SessionCreateReply`) so it reclaims only on a true restart and fails cleanly on a same-process session reap — preserving the resilient-retry "fail cleanly past grace" contract. Sanitized reopen flags guard against truncation; `classFdOp` self-heals within `rpc.retry_window`. See [Reliability & Recovery](design/reliability-and-recovery.md). Byte-range lock re-assertion and recovery of unlinked-but-open files deferred (Design B).

---

## Near-term deferred performance levers

These items post-date the original spec and are the most valuable remaining WAN performance wins. They are tracked in [Performance](design/performance.md).

### SP5 — Partial-consume readahead redesign (WAN read throughput win) — **Done**

The readahead engine now serves partial/cross-chunk sub-ranges and retains the unconsumed tail, so a deep window of frame-sized fetches actually pipelines sequential reads over a high-RTT link (the SP5 redesign measured ≈2× sequential-read throughput at `window=4` vs `window=1`; the shipped default is now `readahead_window = 16` / `readahead_chunk_bytes = 1 MiB`). See [Performance § 2.5](design/performance.md). Remaining follow-up: pool the per-fd prefetch buffers if the allocation cost is flagged.

### Zero-copy `CodecV2` gRPC marshaling

`google.golang.org/protobuf` v1.36 has no zero-copy path for `bytes` fields. A custom codec using the gRPC `CodecV2` interface could thread the FUSE-provided destination buffer directly through the protobuf unmarshal, saving a copy on every read. This requires migrating to the `CodecV2` buffer model, which is non-trivial. Defer until the measured copy cost justifies the work. See [Performance § CodecV2](design/performance.md#52-zero-copy-codecv2-marshaling-serialization-win).

---

## Appendix A — Known security gaps

> File:line references were from the tree as of 2026-05-13. **All items below
> are now closed** — by the identity feature (PRs #31–#48) and Phase 7
> (PRs #53–#55). Kept as a record of what was hardened.

- ~~`pkg/client/grpc/client.go` — `insecure.NewCredentials()` hardcoded; TLS commented out.~~ **Closed (PR #53):** client dials TLS via `pkg/client/tls.BuildConfig`.
- ~~`pkg/client/grpc/auth.go` — `RequireTransportSecurity() = false`.~~ **Closed (PR #53):** returns `true`.
- ~~`pkg/server/service/auth.go` — plaintext string equality on password compare.~~ **Closed (PR #54):** argon2id `passhash.Verify`, constant-time.
- ~~`pkg/server/config/auth.go` — `BasicAuthConfigUser.Password` plain string.~~ **Closed (PR #54):** `PasswordHash` (argon2id PHC); plaintext rejected at startup.
- ~~`deployments/compose/config.yaml` — `admin/admin` default credentials.~~ **Closed (PR #54):** first-run default writes a hashed `admin` with a `# CHANGE ME` comment.
- ~~client-supplied uid/gid fed to `setfsuid`/`setfsgid`.~~ **Closed (identity Phase 1a, #31):** `AssumeUserMiddleware` deleted; identity is the authenticated principal, enforced kernel-native; wire uid advisory.
- ~~`pkg/server/controller/{fs,file,volume}.go` — no per-user volume ACL.~~ **Closed (PR #55):** `VolumeService.PrincipalCanAccess` folded into `BindIdentity` + `List` filter.
- ~~gRPC reflection registered without auth guard.~~ **Closed (PR #54):** `server.grpc.reflection` opt-in, default off.
- ~~`/metrics` endpoint world-readable.~~ **Closed (PR #54):** ops endpoints bind loopback by default + optional basic-auth.
- ~~`MaxRecvMsgSize` / `KeepaliveEnforcementPolicy`.~~ **Closed:** present since Phases 1/3; DoS limits formalised in PR #54.
- ~~`pkg/server/controller/fs.go`, `file.go` — no path cleaning / normalisation.~~ **Closed (confinement Phase 2, #42):** every wire path resolves beneath the volume root via `openat2(RESOLVE_BENEATH)`.
- ~~`pkg/server/config/auth.go` — `type: none` accepted with only a log warning.~~ **Closed (#33):** `auth: none` removed; the factory fails closed (`denyAllAuthService`) on unknown types.

---

## Appendix B — Architectural findings

Design-level observations from the architecture review. Not separate phases; addressed when the relevant phase touches the code, or carried as known debt.

1. **Two-layer client seam.** `pkg/client/io/fs.go` (a `pathfs.FileSystem`) and `pkg/client/io/file.go` (a `nodefs.File`) independently talked gRPC, making cache/retry middleware silently incomplete if only one was wrapped. **Addressed in Phase 4** as part of the `FileSystemBackend` interface unification and the `pathfs` → `go-fuse/v2/fs` migration.

2. **`VolumeRegistry` abstraction missing.** The desktop UI controller carried a `vfsMounted` boolean and lazy-init logic that belongs in a domain object. This was a desktop-UI concern; it moved to the separate desktop repo along with `ui/` and `pkg/ui/`. **Resolved by extraction.**

3. **Config schema duplication.** `pkg/common/config/load.go` is a shell; `pkg/server/config` and `pkg/client/config` each re-implement Viper sub-key parsing by hand; client config imports `pkg/server/config.AuthConfig` (asymmetric dependency); `GMOUNTIE_` env-var prefix wired on server but not client. **Addressed in Phase 5** alongside doc cleanup.

4. **Wails v3 type leak.** `VolumeControllerImpl.OnStartup` took `application.ServiceOptions`, a Wails-specific type. Moved to the separate desktop repo along with the entire UI layer. **Resolved by extraction.**

5. **`pathfs` vs `fs` go-fuse API.** Promoted into Phase 4 scope — see Phase 4 above. The cache layer depends on inode stability; migrating once in the same change that introduces the cache was cheaper than building on `pathfs` and ripping it out later. **Done.**

6. **Three-service gRPC split is a strength.** Metadata (`RpcFs`), data (`RpcFile`), and volume listing (`VolumeService`) are split along the right axis for internet deployment — they can be routed, compressed, and scaled independently. **Document this intent** in the proto files in Phase 5 so a future "for simplicity" merge doesn't happen.

7. **Future transport: gRPC over QUIC / HTTP/3.** TCP head-of-line blocking and the lack of connection migration are gRPC-over-TCP's two real weaknesses for internet-deployed workloads. `grpc-go` has no stable HTTP/3 support as of mid-2026, so a swap is not viable today. The Phase 1 session ID + idempotency tokens are the prep work that makes a future swap cheap (session semantics are decoupled from TCP connection identity). **Reassess ~2027-05**: if `grpc-go` ships QUIC support, evaluate the swap as a perf experiment.

---

## Appendix C — Working agreements

- Each phase opens with its own brainstorm → design doc and implementation plan (transient working docs), then code. Durable architecture is consolidated into `docs/design/` and the working docs pruned once the work ships.
- Commit messages: plain `type: subject`; no `Co-Authored-By:` / `Signed-off-by:` trailers.
- "Reliable" and "works perfectly end-to-end" are measured by the criteria in the section above. Add to that section if we discover new criteria; don't redefine it silently.
- **Backwards compatibility is not a concern.** Wire protocol, config file shape, on-disk cache format, library API — we control both ends and have no external consumers. If a change is the right design, make it; release notes document the break; users re-install, re-edit the config, or wipe the cache. No additive-only proto rules, no deprecation cycles, no migration tooling, no shim code. (External contracts we don't own — the FUSE syscall surface, the gRPC framing protocol — still hold.)

---

## Appendix D — Future capabilities (noted, not yet phased)

Each is a real product capability rather than incremental debt cleanup; promotion to a numbered phase happens when prioritisation calls for it.

### D1 — gMountie cache proxy / edge tier

Run a gMountie process in shared-cache mode in a cloud AZ (e.g. AWS) that sits between an on-prem origin server and N downstream gmountie mounts in the same AZ. The proxy is a gMountie *client* upstream (it mounts the origin volume) and a gMountie *server* downstream (it exposes the same RPCs over a local network). Downstream clients hit the proxy; only the proxy reaches across the WAN to the origin.

**Design hooks already in place after Phase 4:**

- **Cache persistence tier.** The proxy's local store is the Phase 4 persist tier, sized for the AZ's working set.
- **Subscribe + version push.** The proxy subscribes to the origin's `Subscribe` stream and re-broadcasts events to its own downstream subscribers. Origin invalidation flows transitively. The `Attr.version` token is the same value at every tier.
- **Phase 1 session and idempotency primitives.** The proxy survives origin reconnects without invalidating downstream sessions; downstream-to-proxy and proxy-to-origin idempotency tokens are independent.

**Open design questions to revisit when promoting:**

- *Auth pass-through vs proxy-local auth.* Does the origin trust the proxy as a single principal, or does each downstream re-auth against the origin?
- *Write semantics.* Read-only proxies are simpler. Write-through proxies need ordering guarantees to preserve the Subscribe event ordering downstream clients see.
- *Cache coherence across multiple proxies in the same AZ.* Two HA proxies have independent persist tiers and independent Subscribe streams; downstream clients on different proxies may see slightly different invalidation timing — bounded by the heartbeat interval and acceptable.
- *Discovery.* Downstream clients need to know to connect to the proxy, not the origin. DNS, config, or service-mesh routing — a deployment concern, out of scope for the gMountie binary.

This is "Phase 9+" material. The Phase 4 protocol additions (`Attr.version`, `GetAttrIfChanged`, `Subscribe`) deliberately don't preclude it: the same RPCs that today flow origin↔client will flow origin↔proxy↔client unchanged.
