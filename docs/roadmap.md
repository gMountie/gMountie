# gMountie roadmap

**Status / last refreshed:** 2026-05-27

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
- **Single-user-ish trust model, for now.** Multi-tenant security is not the priority, but TLS becomes much more pressing because the wire is the public internet. The security hardening phase is deferred but capped — it cannot stay deferred indefinitely once internet exposure is real.

## What this is, and what it is not

This is a roadmap, not an implementation plan. Each phase below is scoped tightly enough to spawn its own design + implementation plan when it's time to do the work. The intent is one shared document that captures priorities, explicit non-goals, and the gaps we are knowingly deferring — so a future session can pick up the right thread without re-doing the analysis.

This roadmap deliberately does not target "production-ready" in the strict sense. It targets **functional reliability, observable behavior, good internet performance, and the headline client-cache feature** — with security hardening tracked separately.

**The desktop UI is deferred to last.** Phases 1–7 target the CLI (`gMountie mount`, `gMountie serve`), the shared library (`pkg/client/`, `pkg/server/`, `pkg/common/`), and the protocol (`api/proto/`). The Wails desktop app under `ui/` and `pkg/ui/` is Phase 8 — once the library underneath it is correct, the UI is mostly re-binding work. Until Phase 8, the UI policy is: **"don't actively break, don't actively improve."**

## What "works perfectly end-to-end" means

A successful endpoint of Phases 1–6 (CLI and library only; desktop UI excluded until Phase 8):

- The CLI client mounts a volume from a server on the public internet (real DNS, real NAT, real ISP-grade RTT in the tens of milliseconds) with one command.
- That mount survives a 30-second network drop, an ISP IP renumber, and a server restart — without manual `umount` or re-mount.
- File handles open before a network blip remain valid after reconnect.
- Reading a file already in the local cache hits the network only to validate freshness, not to fetch bytes. A cold-cache read of a multi-GB file streams without hitting the 4 MiB unary ceiling.
- `gMountie serve` runs for days under typical workloads without crashing, leaking file descriptors, or accumulating zombie state.
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

Quality / deps / docs and Ops & packaging follow. Security is next-to-last, but capped (see Phase 7). The desktop UI is intentionally last (Phase 8).

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
6. Metrics port moved to `server.metrics_addr` config key.

**Deferred (still deferred):**
- OpenTelemetry tracing — log-correlation IDs cover most of the value at a fraction of the build complexity.
- Authentication on the metrics endpoint — captured in Appendix A.

---

## Phase 3 — Performance: streaming, batching, tuning — **Done**

See [Performance](design/performance.md) for the streaming/batching optimizations and tuning notes.

**Goal:** measured wins on the fio suite over localhost; preparation of the streaming + batched RPCs that Phase 4's cache depends on.

**Delivered:**

1. **Streaming reads and writes** — `ReadRequest`/`WriteRequest` moved from unary to server-streaming reads / client-streaming writes, eliminating the 4 MiB gRPC ceiling. Per-frame size is tunable; session + request IDs carried through for safe retry.
2. **Compound metadata RPCs** — NFSv4-style `Compound` RPC batches metadata ops into one round-trip; used by `Readdir`-with-stat patterns and Phase 4 cache validation.
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
- **Eviction:** LRU under a configurable `max_size_bytes` cap; negative caching for `ENOENT` with short TTL.
- **Persistence:** bbolt (`index.db`) for the LRU index and quota accounting; content-addressable `chunks/` directory; format-versioned for future upgrades; lock file prevents concurrent processes on the same cache dir.
- **Configuration:** `cache.enabled`, `cache.path`, `cache.max_size_bytes`, `cache.chunk_size_bytes`, `cache.attr_ttl_seconds`, `cache.negative_ttl_seconds`.

**Follow-on perf work (shipped after the spec was written):**
- **Cache-fsync reduction** — batch fsync strategy to reduce the per-write fsync overhead on the persistent cache.
- **`WriteAndFlush` compound writes** — coalesces write + flush into a single RPC to reduce WAN round-trips on write-heavy workloads.
- **WAN writeback-cache opt-in** — opt-in write-back mode for single-writer workloads over high-latency links, with documented consistency limitations.
- **`Utimens` RPC** — added to support accurate mtime preservation on writeback paths.
- **Continuous perf tracking via Bencher** — release-gated benchmark series running on the self-hosted VM runner; LAN and WAN (netem) profiles; alert-only. See [Performance § Bencher](design/performance.md).

**Out of scope (still deferred):**
- Write-back caching in general (the opt-in above covers a narrow single-writer case; the general case remains deferred).
- Shared cache across multiple client processes (Phase 8, UI).
- Encrypted cache at rest.
- Cache pre-warming / explicit prefetch APIs.

---

## Phase 5 — Quality, dependencies, and docs — **Planned**

**Goal:** the test suite is trustworthy, the doc copy-paste examples actually work, and dependencies are current.

**In scope:**

1. **CI hardening.**
   - Add `-race` to `task test` (separate `task test:race` if it makes coverage too slow).
   - Add `govulncheck` and `Trivy` (on the released Docker image) to CI.
   - Configure Dependabot for `go.mod`, `npm` (`ui/frontend/`), and GitHub Actions.
   - Set a real coverage threshold in `vladopajic/go-test-coverage` — start at the current measured value, ratchet up.
   - Update pinned `golangci-lint@v1.60` in `.github/workflows/ci.yml` to match what `.golang-ci.yaml` declares (v1.62+).

2. **E2E coverage for failure modes.**
   - Auth failure (basic-auth wrong password).
   - Server killed mid-read and mid-write.
   - Network drop / reconnect with open file handle (validates session reclamation from Phase 1).
   - Large files (≥ 4 GiB).
   - Many concurrent clients on the same volume.
   - Cache hit/miss/eviction paths (Phase 4 coverage).
   - (`VFSVolumeMounter` multi-volume e2e coverage is deferred to Phase 8.)

3. **Drop the 1-second sleep readiness gate** in `test/e2e/utils/app.go` — wait on the gRPC health probe instead.

4. **Dependency refresh (server + CLI only).**
   - `cobra v0.0.3` → current.
   - Replace `github.com/pkg/errors` with stdlib `errors` + `fmt.Errorf("%w", err)` incrementally.
   - (Wails v3 alpha pin: leave alone; reassessed in Phase 8.)

5. **Proto package rename.** Move `api/proto/*.proto` to `package gmountie.v1;` and `pkg/proto/v1/` for naming clarity. Do it once after Phase 1 + 3 + 4 have stopped churning fields. (No wire-compatibility obligation — see Appendix C.)

6. **Doc fixes.**
   - `docs/server/config.md` and `docs/quickstart.md` use `authentication:` — parser expects `auth:`. Replace.
   - Add `CONTRIBUTING.md` (linked from `README.md`, currently 404).
   - Replace the placeholder `https://gmountie.docs.com` in `README.md`.
   - Add an "internet deployment" guide (TLS setup, NAT / firewall, expected latencies, cache sizing recommendations).
   - Add an **"alternatives — when not to use gMountie"** page comparing honestly against Tailscale + NFS, rclone mount, SSHFS, and Cloudflare Tunnel + WebDAV.
   - Document the three-service gRPC split intent in the proto files (see Appendix B item 6).

7. **CLI client config profiles.** `gMountie mount` currently takes every parameter on the command line. Add `~/.config/gMountie/profiles.yaml` for named profiles (`gMountie mount <mountpoint> --profile myserver`). Each profile gets its own cache path / size. Pure UX; no protocol changes.

**Out of scope:**
- Frontend (SvelteKit) test scaffolding — Phase 8.
- Anything under `ui/` or `pkg/ui/`.

**Definition of done:**
- CI red on `-race` failures or coverage drop.
- Every e2e failure-mode test passes deterministically.
- `go.mod` no longer references pre-1.0 cobra or `pkg/errors`.
- `docs/server/config.md` examples can be pasted into a YAML file and the server starts.

---

## Phase 6 — Operations and packaging — **Planned**

**Goal:** the artifacts we ship are deployable by a careful operator.

**In scope:**

1. **Dockerfile.** Make it multi-stage, non-root, `HEALTHCHECK` against the Phase 2 endpoint, OCI labels, minimal runtime image.

2. **Helm chart.** `deployments/charts/gmountie-server` has probes commented out, empty `resources` / `podSecurityContext` / `securityContext`, mutable `image.tag: master`. Wire probes to Phase 2 endpoints; sensible defaults; `runAsNonRoot: true`; parameterised image tag pinned via `appVersion`.

3. **docker-compose example hygiene.** Replace the `fix-permissions` sidecar that `chmod 777`'s the data dir with explicit uid/gid mapping. Move `admin/admin` credentials to a `.env` example file with a "change me" note.

4. **Goreleaser.** SBOM generation, cosign signing for the server binary and Docker image, `-trimpath` / `-buildvcs=true`. Desktop binary (`gMountie-desktop`, AppImage) continues to build to keep the pipeline alive but is not actively maintained — desktop release artifacts are deferred to Phase 8.

**Out of scope:**
- macOS / Windows server builds (Linux-only; desktop UI is the only Wails target, Phase 8).
- Kubernetes operator.
- Anything under `ui/` or `pkg/ui/`.

**Definition of done:**
- `docker run` of the released image as a non-root user serves a volume.
- `helm install` with default values produces a pod that passes its readiness probe.
- `cosign verify` succeeds on a released server artifact.

---

## Phase 7 — Security hardening — **Deferred, but capped**

**This phase is deferred but not unbounded.** The internet-deployment goal makes TLS in particular hard to defer indefinitely; treat the start of Phase 7 as "the moment we open the server to a non-trusted network for real."

The known gaps (file:line references in [Appendix A](#appendix-a-known-security-gaps)):

- TLS is advertised but not implemented — every connection is today plaintext.
- Basic-auth credentials travel in plaintext.
- Passwords are stored and compared in plaintext.
- Privilege escalation via client-supplied uid.
- No per-user volume ACL.
- gRPC reflection registered without auth guard.
- `/metrics` is world-readable.
- No request size or concurrency limits.
- Path inputs are not normalised at the controller layer.
- `type: none` auth is silently allowed.

When this phase opens, it gets its own design doc and decomposition.

---

## Phase 8 — Desktop UI (Wails) — **Deferred**

**Goal:** bring the desktop app up to parity with the now-mature CLI/library.

**In scope:**

1. **Adopt the matured library.** Re-validate `pkg/ui/service/AppService` and `pkg/ui/controller/*` against the post-Phase-6 `AppContext`. Add unit tests for the controller and service packages.
2. **Remove the Wails type leak.** `VolumeControllerImpl.OnStartup` currently takes `application.ServiceOptions` (a Wails v3 type). Move the lifecycle hook behind a UI-local `Lifecycle` interface so `AppContext` stays framework-agnostic.
3. **Introduce a `VolumeRegistry` abstraction.** Move `vfsMounted` boolean and lazy MemFS initialisation out of `VolumeControllerImpl` into a domain object that owns volume lifecycle and routing. Replace or wrap `VFSVolumeMounterImpl` with this registry.
4. **Wails v3 alpha reassessment.** Pin to the newest stable alpha (or beta/release if available), document the version rationale, add an upgrade-tracking note.
5. **Cache sharing across mounts in one UI process.** The Phase 4 cache is per-process / per-server in the CLI. The UI mounts multiple volumes on the same server in one process; the cache should be a single instance shared across those mounts.
6. **Connection sharing across mounts in one UI process.** Share one `grpc.ClientConn` across mounts to the same server. (Deferred from Phase 3.)
7. **Frontend test scaffolding.** Vitest + smoke tests in `ui/frontend/`. Add `task ui:test`. At least one component test for the login flow and one for the volume list.
8. **E2E coverage of `VFSVolumeMounter` / `VolumeRegistry`.** (Deferred from Phase 5.)
9. **Desktop release artifacts.** Resume active maintenance of the goreleaser `gMountie-desktop` build, the AppImage, and signing in parity with the Phase 6 server pipeline.
10. **UI surface for Phase 7 security hardening.** TLS settings, credential storage (OS keyring rather than YAML config), ACL UI if applicable.

**Out of scope:**
- macOS / Windows desktop builds (Linux-only; revisit only if there is user demand).
- Native menus, system tray, autoupdate.
- Mobile clients.

**Definition of done:**
- `pkg/ui/` package has meaningful test coverage (target the same threshold as the rest of the codebase).
- `task ui:test` runs Svelte tests green.
- A user can install the AppImage, point it at a server, mount three volumes simultaneously, see them all in the UI, and observe shared cache hits across them.
- TLS toggle in the UI actually negotiates TLS (depends on Phase 7).

---

## Near-term deferred performance levers

These two items post-date the original spec and are the most valuable remaining WAN performance wins. They are tracked in [Performance § deferred levers](design/performance.md).

### SP5 — Partial-consume readahead redesign (WAN read throughput win)

The current readahead streams a fixed-size chunk; if the FUSE layer only consumes a sub-range, the remainder is discarded. This means `readahead_window > 1` is currently counterproductive (wasted bandwidth, increased memory pressure). The fix requires the readahead engine to serve sub-ranges from a fetched chunk and retain the unconsumed tail for the next FUSE read.

**Until SP5 lands:** keep `readahead_window` at its default of 1. The full design is in [Performance § SP5](design/performance.md#51-sp5-partial-consume-readahead-redesign-wan-read-win).

### Zero-copy `CodecV2` gRPC marshaling

`google.golang.org/protobuf` v1.36 has no zero-copy path for `bytes` fields. A custom codec using the gRPC `CodecV2` interface could thread the FUSE-provided destination buffer directly through the protobuf unmarshal, saving a copy on every read. This requires migrating to the `CodecV2` buffer model, which is non-trivial. Defer until the measured copy cost justifies the work. See [Performance § CodecV2](design/performance.md#52-zero-copy-codecv2-marshaling-serialization-win).

---

## Appendix A — Known security gaps

> All file:line references are from the tree as of 2026-05-13. Verified spots are annotated inline.

- `pkg/client/grpc/client.go:257` — `insecure.NewCredentials()` hardcoded; TLS line commented out. *(verify — line moved from :120 to :257, still open)*
- `pkg/client/grpc/auth.go:31` — `RequireTransportSecurity() = false`. *(still at this line, open)*
- `pkg/server/service/auth.go:87` — plaintext string equality on password compare. *(still open)*
- `pkg/server/config/auth.go:75-77` — `BasicAuthConfigUser.Password` plain string. *(verify — "none" auth accepted around :39 in current tree; original line may have shifted)*
- `deployments/compose/config.yaml:9` — `admin/admin` shipped as default credentials.
- `pkg/server/controller/utils.go:12-19` and `pkg/server/io/middleware/asume_user.go:29` — client-supplied uid/gid fed to `setfsuid`/`setfsgid`. *(nil-guard at utils.go:12-19 was fixed in Phase 1; the underlying uid-from-wire escalation vector in asume_user.go remains open)*
- `pkg/server/controller/{fs,file,volume}.go` — no per-user volume ACL.
- `pkg/server/grpc/server.go:107-108` — gRPC reflection registered without auth guard. *(verify — line moved from :83 to :107-108, still open)*
- `pkg/server/grpc/server.go` — `/metrics` endpoint world-readable; HTTP server now in `pkg/server/ops` (not at original :183). *(verify — endpoint moved to pkg/server/ops; no auth added)*
- `pkg/server/grpc/server.go` — `MaxRecvMsgSize` and `KeepaliveEnforcementPolicy` now present at :201/:205, added in Phases 1/3. *(fixed — no longer a gap)*
- `pkg/server/controller/fs.go:36`, `pkg/server/controller/file.go:51` — no path cleaning / normalisation.
- `pkg/server/config/auth.go:35-36` — `type: none` accepted with only a runtime log warning.

---

## Appendix B — Architectural findings

Design-level observations from the architecture review. Not separate phases; addressed when the relevant phase touches the code, or carried as known debt.

1. **Two-layer client seam.** `pkg/client/io/fs.go` (a `pathfs.FileSystem`) and `pkg/client/io/file.go` (a `nodefs.File`) independently talked gRPC, making cache/retry middleware silently incomplete if only one was wrapped. **Addressed in Phase 4** as part of the `FileSystemBackend` interface unification and the `pathfs` → `go-fuse/v2/fs` migration.

2. **`VolumeRegistry` abstraction missing.** `pkg/ui/controller/volume.go` carries a `vfsMounted` boolean and lazy-init logic that belongs in a domain object, not a UI controller. The two mount modes (`SingleVolumeMounter`, `VFSVolumeMounterImpl`) would benefit from a registry that owns lifecycle and routing decisions. **Addressed in Phase 8** (UI phase).

3. **Config schema duplication.** `pkg/common/config/load.go` is a shell; `pkg/server/config` and `pkg/client/config` each re-implement Viper sub-key parsing by hand; client config imports `pkg/server/config.AuthConfig` (asymmetric dependency); `GMOUNTIE_` env-var prefix wired on server but not client. **Addressed in Phase 5** alongside doc cleanup.

4. **Wails v3 type leak.** `VolumeControllerImpl.OnStartup` takes `application.ServiceOptions`, a Wails-specific type. `AppContext` is otherwise framework-agnostic. **Addressed in Phase 8.**

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

Run a gMountie process in shared-cache mode in a cloud AZ (e.g. AWS) that sits between an on-prem origin server and N downstream gMountie mounts in the same AZ. The proxy is a gMountie *client* upstream (it mounts the origin volume) and a gMountie *server* downstream (it exposes the same RPCs over a local network). Downstream clients hit the proxy; only the proxy reaches across the WAN to the origin.

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
