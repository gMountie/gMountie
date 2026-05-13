# gMountie roadmap: reliability, performance, and client-side caching

**Status:** Draft v2
**Date:** 2026-05-13
**Author:** John Buluba (with Claude Code)
**Branch where work happens:** `develop`

## Project north star

gMountie exists to be an **NFS-over-the-internet replacement** — the user wants to mount their server's storage from anywhere on the public internet, with no VPN, and UX as simple as mounting a local NFS share. Every design decision below is evaluated against that goal first. In practice:

- **Latency tolerance is core, not an afterthought.** Anything that adds RTTs per syscall (chatty metadata, no caching, no readahead, no batching) is a bug, not a polish item.
- **Reconnect/resume must be invisible.** Network blips are expected; mounts and open file handles must survive them.
- **Single-user-ish trust model, for now.** Multi-tenant security is not the priority, but TLS becomes much more pressing because the wire is the public internet. The security hardening phase is deferred but capped — it cannot stay deferred indefinitely once internet exposure is real.

## What this is, and what it is not

This is a roadmap, not an implementation plan. Each phase below is scoped tightly enough to spawn its own design + implementation plan when it's time to do the work. The intent is one shared document that captures priorities, explicit non-goals, and the gaps we are knowingly deferring — so a future session (mine, yours, a contributor's) can pick up the right thread without re-doing the analysis.

This roadmap deliberately does not target "production-ready" in the strict sense. It targets **functional reliability, observable behavior, good internet performance, and the headline client-cache feature** — with security hardening tracked separately.

**The desktop UI is deferred to last.** Phases 1–7 target the CLI (`gMountie mount`, `gMountie serve`), the shared library (`pkg/client/`, `pkg/server/`, `pkg/common/`), and the protocol (`api/proto/`). The Wails desktop app under `ui/` and `pkg/ui/` is Phase 8 — once the library underneath it is correct, the UI is mostly re-binding work. Until Phase 8, the UI is "don't actively break, don't actively improve."

## What we mean by "works perfectly end-to-end"

A successful endpoint of Phases 1–6 looks like (CLI and library only; the desktop UI is excluded until Phase 8):

- The CLI client mounts a volume from a server on the public internet (real DNS, real NAT, real ISP-grade RTT in the tens of milliseconds) with one command.
- That mount survives a 30-second network drop, an ISP IP renumber, and a server restart — without manual `umount` or re-mount.
- File handles open before a network blip remain valid after reconnect.
- Reading a file that's already in the local cache hits the network only to validate freshness, not to fetch bytes. A cold-cache read of a multi-GB file streams without hitting the 4 MiB unary ceiling.
- `gMountie serve` runs for days under typical workloads without crashing, leaking file descriptors, or accumulating zombie state.
- SIGTERM to the server completes in-flight RPCs and shuts down cleanly.
- Performance targets (measured against `test/e2e/fs/io_bench_test.go`):
  - **localhost:** sequential read of 1 GiB ≥ 70% of raw loopback FUSE throughput; metadata ops in single-digit milliseconds.
  - **internet (≥ 20 ms RTT):** sequential read of a cached 1 GiB file ≥ 80% of local disk throughput (cache hit path); cold-cache read ≥ 70% of the available network throughput; `ls` on a 1000-entry directory completes in well under a second on warm metadata cache.
- The two skipped tests in `pkg/server/config/config_test.go` are passing.
- Operators can answer "what version is running?", "is the server healthy?", "how many open files are there per volume right now?", and "what's the per-volume error rate?" without reading source code.
- Every new feature path has a unit test, and every previously-broken failure mode is covered by an e2e or integration test.

What is **explicitly not** part of the success criteria, even by the end of Phase 6:

- TLS transport.
- Authenticated/authorized access beyond the existing basic-auth-or-none knob (any authenticated user still has access to every volume).
- Resistance to a hostile client (a client can still claim uid 0).
- Multi-tenant safety.

These are Phase 7 work; they are listed in the appendix so they don't get forgotten.

## Why this ordering

Reliability comes before everything because a crashing or hanging server makes every other improvement invisible. Inside Phase 1 we also add the *session* concept and *idempotency tokens* — they're reliability prerequisites (no fd reclamation across reconnect = no reliable mount over the internet) but they're also protocol changes that everything downstream depends on, so they happen early and once.

Observability comes before performance because you cannot tune what you cannot measure, and the same instrumentation that diagnoses a perf regression diagnoses a reliability regression.

Performance comes before the cache because the cache layer reuses the streaming RPCs, and because measuring cache effectiveness needs a stable perf baseline.

The client cache is its own phase, not a perf sub-task, because it touches the protocol (invalidation signals), the client architecture (unifying the two-layer seam between `pathfs.FileSystem` and `GrpcFile`), persistence (on-disk format reusable across process restarts), and configuration. It's the headline feature for the internet-NFS goal.

Quality / deps / docs and Ops & packaging follow. Security is next-to-last, but capped (see Phase 7). The desktop UI is intentionally last (Phase 8) — see the note above.

---

## Phase 1 — Functional reliability + session and idempotency foundations

**Goal:** the server doesn't crash on adversarial-but-non-malicious input, the client doesn't hang or lose state when something goes wrong, and the protocol gets the minimal changes needed to make safe retry and reconnect possible.

**In scope:**

1. **Remove every `log.Fatal` from the request path.** Today a metrics port collision (`pkg/server/grpc/server.go:186`), a `setfsuid`/`setfsgid` failure (`pkg/server/io/middleware/asume_user.go:31,35,40,44`), an `init()`-time mistake in the logger (`pkg/utils/log/log.go:36-38`), or a FUSE mount error (`pkg/client/mount/single.go:48`) all terminate the process. These should return errors that bubble up to the caller, with the metrics endpoint specifically becoming a non-fatal best-effort goroutine. A per-request `setfsuid` failure should fail the *request*, not the server.

2. **Fix nil-deref panics in controllers.** `pkg/server/controller/utils.go:12-19` dereferences `caller.Owner.Uid/Gid` with no nil check; `pkg/server/controller/fs.go:121-131` dereferences a possibly-nil `StatFs` reply. Add nil guards that translate to clean gRPC errors.

3. **Wire `GracefulStop` to SIGTERM / SIGINT** in `cmd/commands/serve.go`. `pkg/server/grpc/server.go:101` already has `GracefulStop()` — it's never invoked. Drain in-flight RPCs within a bounded shutdown deadline (e.g. 30s) before falling back to `Stop()`.

4. **Propagate the FUSE-thread context with a deadline through every client RPC.** Every call in `pkg/client/io/file.go:34,50,66,76,88,101,118,133,149` and `pkg/client/io/fs.go:270` uses `context.Background()` today. Replace with a context derived from the FUSE op (or a fresh one with a per-RPC timeout, configurable; default e.g. 30s for I/O, 5s for metadata). A stalled server should fail the RPC, not hang the FUSE kernel thread.

5. **Introduce a session concept (architectural).** Today `pkg/server/controller/file.go` holds an `xsync.MapOf` of file descriptors keyed by a process-global atomic counter — fd IDs are shared across all peers and survive client crashes forever. Introduce a `SessionID` established on connect (negotiated via an opening RPC, or extracted from an auth-bound token), scope the fd table to `(session, fd)`, and reap on gRPC peer disconnect using server-stream cancellation hooks. A reconnecting client passes the same `SessionID` and reclaims its file handles. The proto change is additive (new `Session` message; existing RPCs gain a `session_id` field).

6. **Add idempotency tokens to mutating proto messages.** `Write`, `Create`, `Mkdir`, `Rmdir`, `Rename`, `Unlink`, `Symlink`, `Link`, `Setattr` all gain a `request_id` field. The server keeps a small LRU of recently-seen `(session_id, request_id)` → reply and returns the cached reply for duplicates. This is what makes the retry work in (7) safe.

7. **Make `retry-go` actually retry I/O.** Today retry is only used on unmount (`pkg/client/mount/common.go:71`, `vfs.go:139`). Wrap transient gRPC errors (`Unavailable`, `DeadlineExceeded`) in retries with bounded backoff on the client I/O paths in `pkg/client/io/`. Idempotent ops first; mutating ops use the tokens from (6).

8. **Server-side fd lifecycle correctness.** `pkg/server/controller/file.go:51-58,61-73` registers an entry in `r.files` after `Open`/`Create` regardless of return status; on a non-OK return the client never calls `Release` and the entry leaks. Fix in tandem with the session scoping from (5).

9. **Un-skip the two config tests.** `pkg/server/config/config_test.go:105,144` — `TestParse_EmptyConfig` and `TestParse_EnvVarOverride`.

**Out of scope for this phase:**

- TLS, password hashing, ACLs (Phase 7).
- Streaming I/O — the 4 MiB unary ceiling is a real functional bug for large files, but the fix belongs with the rest of the perf work (Phase 3).
- Cache invalidation protocol bits (Phase 4).
- Restructuring the controller / service / io layering. Tactical fixes only here, except for the session concept which is unavoidable.

**Definition of done:**

- No `log.Fatal` reachable from a serving server's request path.
- New unit tests cover: a request with a nil `Caller`, a request that triggers a non-OK `Open`, a request whose context is cancelled mid-flight, a duplicate `request_id` returning the cached reply.
- New e2e test: kill `gMountie serve`, restart it within 10s; the existing client mount recovers and a previously-open file handle continues to work.
- New e2e test: SIGTERM the server while a 500 MiB write is in flight — the write either completes or fails cleanly, and the server exits within the shutdown deadline.
- Run the existing fio suite for a sustained hour — fd count on the server is steady-state under reconnect churn, not monotonically growing.

---

## Phase 2 — Observability for debugging reliability and performance

**Goal:** enough instrumentation that the next time something is slow, broken, or weird, the answer is in a log line or a metric.

**In scope:**

1. **Switch the logger to JSON in non-TTY mode.** `pkg/utils/log/log.go:22` is hardcoded to `console`. Keep console as the default when stderr is a terminal, JSON otherwise. Make it configurable.

2. **Per-RPC request ID + session ID + user + volume + op as log fields.** Server interceptor generates (or extracts from metadata) a request ID, stores it in the context, attaches it to every log line via a context-aware zap helper. Mirror on the client. A client log line and a server log line for the same op can be joined.

3. **Per-volume / per-op business metrics.** Add to `pkg/server/grpc/server.go:198-204`:
   - `gmountie_server_open_files{volume,session}` gauge.
   - `gmountie_server_bytes_total{volume,direction}` counter.
   - `gmountie_server_rpc_errors_total{volume,op,code}` counter.
   - `gmountie_server_request_duration_seconds{volume,op}` histogram.
   - `gmountie_server_sessions_active` gauge.
   - On the client: `gmountie_client_retry_total{op,code}`, `gmountie_client_in_flight{op}`, and (when Phase 4 lands) `gmountie_client_cache_{hits,misses,evictions,bytes}`.

4. **gRPC health protocol + `/healthz`/`/readyz`.** Register `grpc/health/v1`. Add HTTP `/healthz` (process alive) and `/readyz` (filesystem accessible, listener bound).

5. **Version + build info at runtime.** `pkg.GetBuildInfo()` exists (`pkg/version.go`) but only `gMountie version` uses it. Add a `Version` gRPC and `/version` HTTP endpoint.

6. **Make the metrics port configurable.** Move it under `server.metrics_addr` in the config; today it's hardcoded `:9090` (`pkg/server/grpc/server.go:183`).

**Explicitly deferred from this phase:**

- OpenTelemetry tracing — log-correlation IDs cover most of the value at a fraction of the build complexity.
- Authentication on the metrics endpoint — captured in the security appendix.

**Definition of done:**

- A grep for a request ID returns every log line for that RPC, on both client and server.
- A Prometheus scrape returns the metrics above with realistic values during the fio test.
- `grpc_health_probe -addr localhost:9449` returns SERVING.
- `curl localhost:<metrics_port>/version` returns the build version.

---

## Phase 3 — Performance: streaming, batching, tuning

**Goal:** measured wins on the existing fio suite over localhost; preparation of the streaming + batched RPCs that Phase 4's cache depends on.

**Baseline first.** Before any change, capture a run of `test/e2e/fs/io_bench_test.go` and `simple_test.go` on the developer machine and CI runner. Record numbers (and surrounding hardware) in `docs/perf/baseline-YYYY-MM-DD.md`. Every change reports its delta against that file. Repeat with a `tc netem` 30 ms loopback delay (the existing `scripts/start-slow-loopback.sh` already does this) — the internet number is the one that matters for the project goal.

**In scope:**

1. **Streaming reads and writes.** `ReadRequest`/`ReadReply` and `WriteRequest` (`api/proto/file.proto`) are unary, so the gRPC 4 MiB default cap is a hard ceiling on `MaxRead`/`MaxWrite`. Move to server-streaming reads and client-streaming writes, with the per-frame size tunable. Carry `(session_id, request_id)` from Phase 1 so retry semantics survive the move.

2. **Batched / compound metadata RPCs.** Add a `Compound` RPC (NFSv4-style) that takes a list of metadata ops and returns a list of replies, so a directory walk with stats is one round-trip rather than `1 + N`. Used by `Readdir`-with-stat patterns and (in Phase 4) cache validation.

3. **Tune Snappy.** The Snappy codec is applied to all RPCs (`pkg/server/grpc/snappy/`). Metadata RPCs (`Lookup`, `GetAttr`, `Readdir`) don't benefit and pay the CPU cost. Restrict compression to read/write payloads, or switch to per-call codec selection.

4. **FUSE mount-option tuning.** `pkg/client/mount/single.go` and `vfs.go` currently use defaults. Set `MaxRead`, `MaxWrite`, `MaxBackground`, `CongestionThreshold`, and the writeback cache (`WritebackCache`) where integrity allows. Negotiate values with the server (the server caps `MaxRead` based on its frame size).

5. **Client-side readahead and write coalescing.** For sequential read patterns (detected by offset progression), pre-fetch the next streaming chunk. For sequential writes from a single fd, coalesce small writes into the streaming frame size.

6. **gRPC connection management.** Configure `KeepaliveParams` + `KeepaliveEnforcementPolicy` on the client so dead connections are detected, surfacing as the Phase 1 retries. (Sharing a single `grpc.ClientConn` across multiple mounts in one process is a UI-only optimisation today and is deferred to Phase 8.)

**Out of scope for this phase:**

- The on-disk cache itself (Phase 4).
- Server-side caching of file content (correctness risk; revisit later).
- Multi-server / replication.

**Definition of done:**

- Sequential read of a 1 GiB file from a tmpfs-backed volume reaches ≥ 70% of raw loopback FUSE throughput on localhost.
- Write of a 1 GiB file completes without OOM and without hitting the unary cap.
- Metadata ops latency does not regress more than 10% vs the Phase 2 baseline.
- New e2e test: mount a volume, copy a 4 GiB file in both directions, verify bit-exact content.
- A `Compound` of 100 `GetAttr`s completes in one RTT.

---

## Phase 4 — Persistent client-side cache

**Goal:** the headline user-facing feature. After the first read, the same bytes don't cross the network. The cache survives client restarts. The user can point it at a path and cap its size.

This phase has the largest design surface of the roadmap because it touches the protocol (invalidation signals), the client architecture (unifying the `pathfs.FileSystem` + `GrpcFile` seam), persistence (on-disk format), and configuration. It gets its own design doc when work starts; the bullets below are the scope, not the design.

### Scope

**Protocol additions (server side):**

- `Attr.version` (monotonic counter or content hash) — set by the server. Caches use this to validate freshness.
- A `GetAttrIfChanged(path, known_version)` shortcut RPC that returns either the new attrs or a `NotModified` status. Cheaper than a full `GetAttr`.
- A server-streaming `Subscribe(volume)` RPC that pushes `(path, new_version)` change events for paths the client has cached. Optional: cache works without it (falls back to validation), works *much* better with it.
- A `Read` request gains an optional `version` field; on mismatch the server returns the new version + bytes; on match (read for revalidation), it can short-circuit.

**Client architecture:**

- Unify the two-layer seam. Today `pkg/client/io/fs.go` is a `pathfs.FileSystem` and `pkg/client/io/file.go` is a `nodefs.File`, each independently talking gRPC. Define a single `FileSystemBackend` (or equivalent) interface that both surfaces use, so cache and retry middleware sit once and intercept everything.
- The cache itself is a middleware-layer implementation of that interface. It wraps a `BackendClient` (the gRPC-backed implementation) with attribute, directory, and data caching.
- One cache instance per `gMountie mount` process, scoped to the target server. (The "shared across mounts in one process" sharing model — which only matters for the UI — is deferred to Phase 8.)

**Cache content & eviction:**

- **Read-through, write-through to start.** Writes go to the server immediately *and* update the cache. Write-back is deferred (consistency hazards on shared volumes); document the limitation.
- **Three caches in one store:**
  - **Attribute cache** — `GetAttr` results, keyed by `(volume, path)`, validated against `Attr.version` or by elapsed TTL fallback.
  - **Directory cache** — `Readdir` results, same validation strategy.
  - **Data cache** — file contents stored in fixed-size chunks (e.g. 1 MiB), keyed by `(volume, path, version, chunk_index)`. Content-addressable layout means a file rename with no content change costs nothing.
- **Eviction: LRU under a configurable `max_size_bytes` cap.** Sized accounting includes all three caches. Eviction is opportunistic (on cache write that would exceed the cap, evict until under).
- **Negative caching.** A `Lookup` that returned `ENOENT` is cached briefly (short TTL) to avoid hammering the server on tab-completion patterns; invalidated immediately on `Subscribe` events for the same path.

**Persistence:**

- On-disk layout under the user-configured `cache_path`:
  - `index.db` — a small embedded KV store (BoltDB or sqlite) for the LRU index, version tags, and quota accounting.
  - `chunks/` — content-addressable chunk files (sharded by hash prefix to avoid millions of files in one directory).
- The format is documented (cheap inspection / external eviction tooling possible) and versioned (a `format_version` key so future upgrades can migrate).
- A lock file prevents two client processes from using the same cache dir simultaneously (out of scope: shared multi-process cache).
- On startup, the cache loads the index lazily and validates against the configured quota — over-quota state from a previous run triggers eviction before serving requests.

**Configuration (client side):**

- `cache.enabled: bool` (default `true` once stable).
- `cache.path: string` (default an XDG cache subdirectory).
- `cache.max_size_bytes: int` (default e.g. 1 GiB).
- `cache.chunk_size_bytes: int` (default 1 MiB).
- `cache.attr_ttl_seconds: int` (fallback for when `Subscribe` is unavailable; default short, e.g. 5s).
- `cache.negative_ttl_seconds: int` (default a few seconds).

### Out of scope for this phase

- **Write-back caching.** Risk-heavy; revisit once we have telemetry on real-world write patterns.
- **Shared cache across multiple client processes.** Single-process exclusivity is simpler and matches the typical mount-on-laptop use case.
- **Encrypted cache at rest.** Trust the local disk for now; document the assumption.
- **Cache pre-warming / explicit prefetch APIs.** Future work.

### Definition of done

- A read of a file already in the cache hits the network only for an `Attr.version` validation (or zero RPCs when `Subscribe` is active).
- The cache survives a `gMountie mount` process restart — the same file read after restart still hits the cache.
- Pointing two `gMountie mount` invocations at the same cache path fails fast with a clear error.
- An e2e test sets `cache.max_size_bytes` to 100 MiB and reads 1 GiB of distinct files; the cache stays under the cap and serves the most-recent files from disk.
- A `Subscribe`-driven invalidation: client A writes a file, client B (with cache active) sees the new version on its next read.
- Cache hit rate, eviction rate, and bytes in cache visible in Prometheus metrics.

---

## Phase 5 — Quality, dependencies, and docs

**Goal:** the test suite is trustworthy, the doc copy-paste examples actually work, and dependencies are current.

**In scope:**

1. **CI hardening.**
   - Add `-race` to `task test` (separate `task test:race` if it makes coverage too slow).
   - Add `govulncheck` and `Trivy` (on the released Docker image) to CI.
   - Configure Dependabot for `go.mod`, `npm` (`ui/frontend/`), and GitHub Actions.
   - Set a real coverage threshold in `vladopajic/go-test-coverage` — start at the current measured value, ratchet up.
   - Update pinned `golangci-lint@v1.60` in `.github/workflows/ci.yml:28` to match what `.golang-ci.yaml` declares (v1.62+).

2. **E2E coverage for failure modes.**
   - Auth failure (basic-auth wrong password).
   - Server killed mid-read and mid-write.
   - Network drop / reconnect with open file handle (validates session reclamation from Phase 1).
   - Large files (≥ 4 GiB).
   - Many concurrent clients on the same volume.
   - Cache hit/miss/eviction paths (Phase 4 coverage).
   - (`VFSVolumeMounter` multi-volume e2e coverage is deferred to Phase 8 alongside the UI work that drives it.)

3. **Drop the 1-second sleep readiness gate** in `test/e2e/utils/app.go:156` — wait on the gRPC health probe instead (introduced in Phase 2).

4. **Dependency refresh (server + CLI only).**
   - `cobra v0.0.3` → current. Pre-1.0 is missing seven years of fixes.
   - Replace `github.com/pkg/errors` with stdlib `errors` + `fmt.Errorf("%w", err)` incrementally.
   - (Wails v3 alpha pin: leave alone; reassessed in Phase 8.)

5. **Proto package rename (organisational only).** Move `api/proto/*.proto` to `package gmountie.v1;` and `pkg/proto/v1/` for naming clarity. We don't promise wire compatibility across releases (see Appendix C) — this is purely about file organisation. Do it once at the end of the protocol work (after Phase 1 + 3 + 4 have stopped churning fields) so there's only one rename diff to review.

6. **Doc fixes.**
   - `docs/server/config.md:20,109` and `docs/quickstart.md:14` use `authentication:` — parser expects `auth:`. Replace.
   - Add `CONTRIBUTING.md` (linked from `README.md:58`, currently 404).
   - Replace the placeholder `https://gmountie.docs.com` (`README.md:28`).
   - Add an "internet deployment" guide (TLS setup, NAT / firewall, expected latencies, cache sizing recommendations).

**Out of scope:**

- Frontend (SvelteKit) test scaffolding — Phase 8.
- Anything under `ui/` or `pkg/ui/`.

**Definition of done:**

- CI red on `-race` failures or coverage drop.
- Every e2e failure-mode test passes deterministically.
- `go.mod` no longer references pre-1.0 cobra or `pkg/errors`.
- `docs/server/config.md` examples can be pasted into a YAML file and the server starts.

---

## Phase 6 — Operations and packaging

**Goal:** the artifacts we ship are deployable by a careful operator.

**In scope:**

1. **Dockerfile.** `Dockerfile` is single-stage, root-running. Make it multi-stage, non-root, `HEALTHCHECK` against the Phase 2 endpoint, OCI labels, minimal runtime image.

2. **Helm chart.** `deployments/charts/gmountie-server` has probes commented out (`templates/deployment.yaml:45-48`), empty `resources` / `podSecurityContext` / `securityContext`, mutable `image.tag: master`. Wire probes to the Phase 2 endpoints, sensible defaults, `runAsNonRoot: true`, parameterised image tag pinned via `appVersion`.

3. **docker-compose example hygiene.** `deployments/compose/docker-compose.yaml:17-20` runs a `fix-permissions` sidecar that `chmod 777`'s the data dir. Replace with explicit uid/gid mapping. Move `admin/admin` to a `.env` example file with a clear "change me".

4. **Goreleaser.** SBOM generation, cosign signing for the server binary and the Docker image, `-trimpath` / `-buildvcs=true` builds. The desktop binary (`gMountie-desktop`, AppImage) continues to build to keep the pipeline alive but is not actively maintained — release artifacts for desktop are deferred to Phase 8.

**Out of scope:**

- macOS / Windows server builds (Linux-only; desktop UI is the only Wails target, and that's Phase 8).
- Anything under `ui/` or `pkg/ui/`.
- Frontend test scaffolding — Phase 8.
- Kubernetes operator.

**Definition of done:**

- `docker run` of the released image as a non-root user serves a volume.
- `helm install` with default values produces a pod that passes its readiness probe.
- `cosign verify` succeeds on a released server artifact.

---

## Phase 7 — Security hardening (deferred, but capped)

**This phase is deferred but not unbounded.** The internet-deployment goal makes TLS in particular hard to defer indefinitely; treat the start of Phase 7 as "the moment we open the server to a non-trusted network for real."

The known gaps (all with file:line citations in Appendix A):

- **TLS is advertised but not implemented.** Every connection today is plaintext.
- **Basic-auth credentials travel in plaintext** because the gRPC client sets `RequireTransportSecurity() = false`.
- **Passwords are stored and compared in plaintext.** Needs bcrypt or argon2 + constant-time compare.
- **Privilege escalation via client-supplied uid.** `Caller.Owner.{Uid,Gid}` is read from the wire. Identity should come from the authenticated user.
- **No per-user volume ACL.** Any authenticated user can access any volume.
- **gRPC reflection registered before any auth check.**
- **`/metrics` is world-readable.**
- **No request size or concurrency limits.**
- **Path inputs are not normalised** at the controller layer.
- **"none" auth is silently allowed** at config parse time.

When this phase opens, it gets its own design doc and decomposition.

---

## Phase 8 — Desktop UI (Wails)

**Goal:** bring the desktop app up to parity with the now-mature CLI/library. This phase exists last because the UI is a thin layer over `AppContext` — fixing it before the library is solid would be premature, and most fixes here are mechanical once the library is right.

**In scope:**

1. **Adopt the matured library.** Re-validate `pkg/ui/service/AppService` and `pkg/ui/controller/*` against the post-Phase-6 `AppContext`. Add unit tests for `pkg/ui/controller/login.go`, `pkg/ui/controller/volume.go`, `pkg/ui/service/app.go`, `pkg/ui/service/config.go`.

2. **Remove the Wails type leak.** `VolumeControllerImpl.OnStartup` currently takes `application.ServiceOptions` (a Wails v3 type). Move the lifecycle hook behind a UI-local `Lifecycle` interface so `AppContext` stays framework-agnostic. (Architecture review finding; see Appendix B item 4.)

3. **Introduce a `VolumeRegistry` abstraction.** Move the `vfsMounted` boolean and lazy MemFS initialisation logic out of `VolumeControllerImpl` into a domain object that owns volume lifecycle and routing. Replace or wrap `VFSVolumeMounterImpl` (currently UI-only) with this registry. (Architecture review finding; see Appendix B item 2.)

4. **Wails v3 alpha reassessment.** Pin to the newest stable alpha (or beta/release if available by this point), document the chosen version rationale, and add an upgrade-tracking note. If Wails v3 stable has landed, schedule a single dedicated upgrade pass.

5. **Cache sharing across mounts in one UI process.** The Phase 4 cache is per-process / per-server in the CLI. The UI mounts multiple volumes on the same server in one process; the cache should be a single instance shared across those mounts (which the cache design already accommodates), and the cache config should surface in the UI settings.

6. **Connection sharing across mounts in one UI process.** Share one `grpc.ClientConn` across mounts to the same server in the UI process. (Deferred from Phase 3.)

7. **Frontend test scaffolding.** Vitest + a smoke test in `ui/frontend/`. Add `task ui:test`. Add at least one component test for the login flow and one for the volume list.

8. **E2E coverage of `VFSVolumeMounter` / `VolumeRegistry`.** The multi-volume path is not currently e2e-tested. (Deferred from Phase 5.)

9. **Desktop release artifacts.** Resume active maintenance of the goreleaser `gMountie-desktop` build, the AppImage, and any signing the server pipeline already does in Phase 6.

10. **UI surface for the security hardening from Phase 7.** TLS settings, credential storage (use the OS keyring rather than the YAML config), ACL UI if applicable.

**Out of scope:**

- macOS / Windows desktop builds (Linux-only project; revisit only if there's user demand).
- Native menus, system tray, autoupdate.
- Mobile clients.

**Definition of done:**

- `pkg/ui/` package has meaningful test coverage (target the same threshold as the rest of the codebase).
- `task ui:test` runs Svelte tests green.
- A user can install the AppImage, point it at a server, mount three volumes simultaneously, see them all in the UI, and observe shared cache hits across them.
- TLS toggle in the UI actually negotiates TLS (depends on Phase 7).

---

## Appendix A — Known security gaps with file:line refs

- `pkg/client/grpc/client.go:120` — `insecure.NewCredentials()` hardcoded; TLS line commented out.
- `pkg/client/grpc/auth.go:31` — `RequireTransportSecurity() = false`.
- `pkg/server/service/auth.go:87` — plaintext string equality on password compare.
- `pkg/server/config/auth.go:75-77` — `BasicAuthConfigUser.Password` plain string.
- `deployments/compose/config.yaml:9` — `admin/admin` shipped.
- `pkg/server/controller/utils.go:12-19` and `pkg/server/io/middleware/asume_user.go:29` — client-supplied uid fed to `setfsuid`.
- `pkg/server/controller/{fs,file,volume}.go` — no per-user volume ACL.
- `pkg/server/grpc/server.go:83` — gRPC reflection registered before auth interceptors.
- `pkg/server/grpc/server.go:183` — `/metrics` on default mux, no auth.
- `pkg/server/grpc/server.go:147-151` — no `MaxRecvMsgSize`, no `KeepaliveEnforcementPolicy`.
- `pkg/server/controller/fs.go:36`, `pkg/server/controller/file.go:51` — no path cleaning.
- `pkg/server/config/auth.go:35-36` — `type: none` accepted with only a runtime log warning.

## Appendix B — Architectural findings to address opportunistically

These are design-level observations from the architecture review. They are not separate phases; they are addressed when the relevant phase touches the code, or carried as known debt.

1. **Two-layer client seam.** `pkg/client/io/fs.go` (a `pathfs.FileSystem`) and `pkg/client/io/file.go` (a `nodefs.File`) independently talk gRPC. A cache or retry middleware that wraps only one is silently incomplete. **Addressed in Phase 4** as part of the cache backend interface unification.

2. **`VolumeRegistry` abstraction missing.** `pkg/ui/controller/volume.go` carries a `vfsMounted` boolean and lazy-init logic that belongs in a domain object, not a UI controller. The two mount modes (`SingleVolumeMounter`, `VFSVolumeMounterImpl`) would also benefit from a registry that owns lifecycle and routing decisions. **Addressed in Phase 8** (UI phase) since `VFSVolumeMounter` is currently UI-only.

3. **Config schema duplication.** `pkg/common/config/load.go` is a shell, not a schema. `pkg/server/config` and `pkg/client/config` each re-implement Viper sub-key parsing by hand; client config imports `pkg/server/config.AuthConfig` (asymmetric cross-package dependency); the documented `GMOUNTIE_` env-var prefix is wired on the server but not the client. **Addressed in Phase 5** alongside the doc cleanup.

4. **Wails v3 type leak.** `VolumeControllerImpl.OnStartup` takes `application.ServiceOptions`, a Wails-specific type. The `AppContext` is otherwise framework-agnostic. **Addressed in Phase 8.**

5. **`pathfs` vs `fs` go-fuse API.** The codebase uses the older path-based `pathfs` API. The newer `fs` package is better suited to caching and inode stability (the comment at `pkg/client/io/fs.go:57` about ignoring `Ino` is a symptom). Full migration is non-trivial; **carry as known debt** and reassess if cache work in Phase 4 hits inode-instability problems.

6. **Three-service gRPC split is a strength.** Metadata (`RpcFs`), data (`RpcFile`), and volume listing (`VolumeService`) are split along the right axis for internet deployment — they can be routed, compressed, and scaled independently. **Document this intent** in the proto files in Phase 5 so a future "for simplicity" merge doesn't happen.

## Appendix C — Working agreements

- Each phase opens with its own brainstorm → design doc in `docs/superpowers/specs/`, then an implementation plan, then code.
- All work happens on `develop` (or branches off `develop`). `master` only receives merged phase milestones.
- Commit messages: plain `type: subject`; no `Co-Authored-By:` / `Signed-off-by:` trailers for this repo.
- "Reliable" and "works perfectly end-to-end" are measured by the criteria above. Add to that section if we discover new criteria; don't redefine it silently.
- **Backwards compatibility is not a concern.** Wire protocol, config file shape, on-disk cache format, library API — we control both ends and have no external consumers. If a change is the right design, make it; release notes document the break; users re-install, re-edit the config, or wipe the cache. No additive-only proto rules, no deprecation cycles, no migration tooling, no shim code. (External contracts we don't own — the FUSE syscall surface, the gRPC framing protocol — still hold.)
