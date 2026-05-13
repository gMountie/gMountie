# gMountie roadmap: reliability and performance

**Status:** Draft
**Date:** 2026-05-13
**Author:** John Buluba (with Claude Code)
**Branch where work happens:** `develop`

## What this is, and what it is not

This is a roadmap, not an implementation plan. Each phase below is scoped tightly enough to spawn its own design + implementation plan when it's time to do the work. The intent is to have one shared document that captures the priorities, the explicit non-goals, and the gaps we are knowingly deferring — so a future session (mine, yours, a contributor's) can pick up the right thread without re-doing the analysis.

This roadmap deliberately does **not** target "production-ready" in the strict sense. It targets **functional reliability, observable behavior, and good performance** — with the security hardening tracked separately and deferred. Calling the result "production-ready" would be misleading until Phase 6 lands; everything before that improves the project for a trusting / single-tenant / LAN deployment.

## What we mean by "works perfectly end-to-end"

A successful endpoint of Phases 1–5 looks like:

- `gMountie serve` runs for days under typical workloads without crashing, leaking file descriptors, or accumulating zombie state.
- A client mount survives a server restart, a brief network blip, and arbitrarily large files (multi-GB) without hanging the FUSE kernel thread.
- SIGTERM to the server completes in-flight RPCs and shuts down cleanly.
- The existing `test/e2e/fs/io_bench_test.go` fio suite reports throughput within a small constant factor of the loopback FUSE baseline (target: ≥ 70% of loopback throughput for sequential read on a 1 GiB file over localhost; ≥ 50% over a typical LAN). Latency-sensitive ops (stat, readdir on cold cache) finish in single-digit milliseconds on localhost.
- The two skipped tests in `pkg/server/config/config_test.go` are passing.
- Operators can answer "what version is running?", "is the server healthy?", "how many open files are there per volume right now?", and "what's the per-volume error rate?" without reading source code.
- Every new feature path has a unit test, and every previously-broken failure mode is covered by an e2e or integration test.

What is **explicitly not** part of the success criteria, even by the end of Phase 5:

- TLS transport.
- Authenticated/authorized access beyond the existing basic-auth-or-none knob (any authenticated user still has access to every volume).
- Resistance to a hostile client (a client can still claim uid 0).
- Multi-tenant safety.

These are Phase 6 work; they are listed in the appendix so they don't get forgotten.

## Why this ordering

Reliability comes before everything because a crashing or hanging server makes every other improvement invisible. Observability comes before performance because you cannot tune what you cannot measure, and the same instrumentation that diagnoses a perf regression is what diagnoses a reliability regression — so the cost is amortised. Quality / deps / docs ride alongside as the cross-cutting prerequisite for everything to be testable and contributable. Ops & packaging land after the code is good enough to deploy meaningfully. Security is last because we have decided, knowingly, that the current trust model (LAN, trusted clients, ops-controlled credentials) is acceptable for now — and we'd rather build a system that works correctly first than build a hardened version of a buggy one.

---

## Phase 1 — Functional reliability

**Goal:** the server doesn't crash on adversarial-but-non-malicious input, doesn't hang the client when something goes wrong, and doesn't leak resources over time.

**In scope:**

1. **Remove every `log.Fatal` from the request path.** Today, a metrics port collision (`pkg/server/grpc/server.go:186`), a `setfsuid`/`setfsgid` failure (`pkg/server/io/middleware/asume_user.go:31,35,40,44`), an `init()`-time mistake in the logger (`pkg/utils/log/log.go:36-38`), or a FUSE mount error in `gMountie mount` (`pkg/client/mount/single.go:48`) all terminate the process. These should return errors that bubble up to the caller, with the metrics endpoint specifically becoming a non-fatal best-effort goroutine that logs and exits if it fails. The middleware case is subtle: if `setfsuid` fails on a per-request basis, the request — not the server — should fail.

2. **Fix nil-deref panics in controllers.** `createContext` in `pkg/server/controller/utils.go:12-19` dereferences `caller.Owner.Uid/Gid` with no nil check; `StatFs` in `pkg/server/controller/fs.go:121-131` dereferences a possibly-nil reply from the underlying filesystem. Add nil guards that translate to a clean gRPC error.

3. **Wire `GracefulStop` to SIGTERM / SIGINT** in `cmd/commands/serve.go`. The server already calls `grpc.Server.GracefulStop()` in `pkg/server/grpc/server.go:101` — it's never invoked. Drain in-flight RPCs within a bounded shutdown deadline (e.g. 30s) before falling back to `Stop()`.

4. **Propagate the FUSE-thread context with a deadline through every client RPC.** Today every call in `pkg/client/io/file.go:34,50,66,76,88,101,118,133,149` and `pkg/client/io/fs.go:270` uses `context.Background()`. Replace with a context derived from the FUSE op (or a fresh one with a per-RPC timeout, configurable, default e.g. 30s for I/O / 5s for metadata). A stalled server should fail the RPC, not hang the FUSE kernel thread.

5. **Server-side fd lifecycle correctness.** `pkg/server/controller/file.go:51-58,61-73` registers an entry in `r.files` after `Open`/`Create` *regardless of return status*; on a non-OK return the client never calls `Release` and the entry leaks. Plus there is no per-connection cleanup — a client that crashes leaves its open files in `r.files` forever. Tie the fd table to the gRPC peer / a server-side session ID, and reap on disconnect (gRPC has `ServerStream`-level cancellation hooks for this).

6. **Make `retry-go` actually retry I/O.** Today retry is only used on unmount (`pkg/client/mount/common.go:71`, `vfs.go:139`). Wrap transient gRPC errors (`Unavailable`, `DeadlineExceeded` for idempotent ops) in retries with bounded backoff on read/write/stat paths in `pkg/client/io/`. Be careful: writes are not safely retryable unless we also de-duplicate; start with idempotent ops only.

7. **Un-skip the two config tests.** `pkg/server/config/config_test.go:105,144` — `TestParse_EmptyConfig` and `TestParse_EnvVarOverride`. The env-var case in particular is a documented feature.

**Out of scope for this phase:**

- TLS, password hashing, ACLs (Phase 6).
- Streaming I/O — the 4 MiB unary ceiling is a real functional bug for large files, but the fix belongs with the rest of the perf work because it shares the streaming RPC design (Phase 3).
- Restructuring the controller / service / io layering. Tactical fixes only here.

**Definition of done:**

- No `log.Fatal` reachable from a serving server's request path (grep + review).
- New unit tests cover: a request with a nil `Caller`, a request that triggers a non-OK `Open`, a request whose context is cancelled mid-flight.
- New e2e test: SIGTERM the server while a 500 MiB write is in flight — the write either completes or fails cleanly, the server exits within the shutdown deadline, and the mount survives.
- New e2e test: kill the server, restart it, the mount recovers and a subsequent read succeeds.
- Run the existing fio suite for a sustained hour — fd count on the server is steady-state, not monotonically growing.

---

## Phase 2 — Observability for debugging reliability and performance

**Goal:** enough instrumentation that the next time something is slow, broken, or weird, the answer is in a log line or a metric — not in a `pprof` session a developer has to set up by hand.

**In scope:**

1. **Switch the logger to JSON in non-TTY mode.** `pkg/utils/log/log.go:22` is hardcoded to `console`. Keep console as the default when stderr is a terminal, JSON otherwise. Make it configurable.

2. **Per-RPC request ID + user + volume + op as log fields.** Add a server interceptor that generates (or extracts from metadata) a request ID, stores it in the context, and attaches it to every log line via a context-aware zap helper. Mirror on the client side so a client log line and a server log line for the same op can be joined.

3. **Per-volume / per-op business metrics.** The current Prometheus integration (`pkg/server/grpc/server.go:198-204`) exports only the generic `go-grpc-middleware` histograms. Add:
   - `gmountie_server_open_files{volume="…"}` gauge.
   - `gmountie_server_bytes_total{volume,direction}` counter.
   - `gmountie_server_rpc_errors_total{volume,op,code}` counter.
   - `gmountie_server_request_duration_seconds{volume,op}` histogram.
   - On the client: `gmountie_client_retry_total{op,code}`, `gmountie_client_in_flight{op}` gauge.

4. **gRPC health protocol + `/healthz`/`/readyz`.** Register `grpc/health/v1` so Kubernetes / external probes can use it. Add an HTTP `/healthz` (process alive) and `/readyz` (filesystem accessible, listener bound) for non-gRPC probers.

5. **Version + build info reported at runtime.** `pkg.GetBuildInfo()` exists (`pkg/version.go`) and is printed by `gMountie version` but never surfaces over gRPC or HTTP. Add a `Version` RPC (or include it as gRPC server reflection metadata) and a `/version` HTTP endpoint.

6. **Make the metrics port configurable**, not hardcoded `:9090` (`pkg/server/grpc/server.go:183`). Move it under `server.metrics_addr` in the config.

**Explicitly deferred from this phase:**

- OpenTelemetry tracing. The log-correlation IDs cover most of the value at a fraction of the build complexity; OTel can be added later without changing the log/metric API.
- Authentication on the metrics endpoint. Caught in the security appendix.

**Definition of done:**

- A grep for a request ID returns every log line for that RPC, on both client and server.
- A Prometheus scrape returns the five metrics above with realistic values during the fio test.
- `grpc_health_probe -addr localhost:9449` returns SERVING.
- `curl localhost:<metrics_port>/version` returns the build version.

---

## Phase 3 — Performance

**Goal:** measured wins on the existing fio benchmark suite, no regressions on the latency-sensitive metadata ops.

**Baseline first.** Before any change, capture a `task test:coverage`-free run of `test/e2e/fs/io_bench_test.go` and `simple_test.go` on the developer machine and CI runner. Record numbers (and the surrounding hardware) in a `docs/perf/baseline-YYYY-MM-DD.md`. Every change in this phase reports its delta against that file.

**In scope:**

1. **Streaming reads and writes.** Today `ReadRequest`/`ReadReply` and `WriteRequest` (`api/proto/file.proto`) are unary, so the gRPC default 4 MiB cap is a hard ceiling on `MaxRead`/`MaxWrite` mount options, and large files require many round-trips. Move to server-streaming reads and client-streaming writes, with the per-frame size tunable. This is also the natural place to fix the leak/lifetime issues from Phase 1 because it changes the RPC shape.

2. **Tune Snappy.** The Snappy codec is applied to *all* RPCs (`pkg/server/grpc/snappy/`). Metadata RPCs (`Lookup`, `GetAttr`, `Readdir`) don't benefit and pay the CPU cost. Restrict compression to read/write payloads, or switch to per-call codec selection.

3. **FUSE mount-option tuning.** `pkg/client/mount/single.go` and `vfs.go` currently use defaults. Set `MaxRead`, `MaxWrite`, `MaxBackground`, `CongestionThreshold`, and the writeback cache (`WritebackCache`) where data integrity allows. Negotiate the values with the server (e.g. server caps `MaxRead` based on its frame size).

4. **Client-side readahead and write coalescing.** For sequential read patterns (detected by offset progression), pre-fetch the next streaming chunk. For sequential writes from a single fd, coalesce small writes into the streaming frame size. Both are bounded by the streaming-RPC design from item 1.

5. **gRPC connection management.** The desktop UI creates a fresh client per mount today. Share one `grpc.ClientConn` across mounts on the same client process, configure keepalive (`KeepaliveParams` + `KeepaliveEnforcementPolicy`) to detect dead connections, and surface those as Phase 1 retries.

**Out of scope for this phase:**

- Server-side caching of file content (correctness risk; revisit later).
- Multi-server / replication.

**Definition of done:**

- Sequential read of a 1 GiB file from a tmpfs-backed volume reaches ≥ 70% of raw loopback FUSE throughput on localhost (measured by `io_bench_test.go`).
- Write of a 1 GiB file completes without OOM and without hitting the unary cap.
- Metadata ops (`stat`, `readdir`) latency does not regress more than 10% vs the Phase 2 baseline.
- New e2e test mounts a volume, copies a 4 GiB file in both directions, verifies bit-exact content.

---

## Phase 4 — Quality, dependencies, and docs

**Goal:** the test suite is trustworthy, the doc copy-paste examples actually work, and we are not pinned to a 2017-era Cobra.

**In scope:**

1. **CI hardening.**
   - Add `-race` to `task test` (and a separate `task test:race` if `-race` makes coverage too slow).
   - Add `govulncheck` and `Trivy` (on the released Docker image) to CI.
   - Configure Dependabot for `go.mod`, `npm` (`ui/frontend/`), and GitHub Actions.
   - Set a real coverage threshold in `vladopajic/go-test-coverage` — start at the current measured value, ratchet up over time. The current CI uploads a badge but does not gate merges.
   - Update the pinned `golangci-lint@v1.60` in `.github/workflows/ci.yml:28` to match what `.golang-ci.yaml` declares (v1.62+).

2. **E2E coverage for failure modes.** `test/e2e/` covers a happy path; expand to:
   - Auth failure (basic-auth wrong password).
   - Server killed mid-read and mid-write (relates to Phase 1 graceful shutdown).
   - Network drop / re-connect.
   - Large files (≥ 4 GiB) — directly exercises the streaming work in Phase 3.
   - Many concurrent clients on the same volume.
   - The multi-volume `VFSVolumeMounter` path (currently only `SingleVolumeMounter` is e2e-tested).

3. **Drop the 1-second sleep readiness gate.** `test/e2e/utils/app.go:156`. Wait on the gRPC health probe instead (introduced in Phase 2).

4. **Dependency refresh.**
   - `cobra v0.0.3` → current. The pre-1.0 version is missing seven years of fixes.
   - Replace `github.com/pkg/errors` with stdlib `errors` + `fmt.Errorf("%w", err)` incrementally; the wrapping API is equivalent.
   - Reassess `wailsapp/wails/v3 v3.0.0-alpha.7` — pin to a specific newer alpha if one is stable, or document the pin rationale. Alpha software is acceptable for a side-project desktop app but it should be a conscious choice.

5. **Proto versioning.** Move `api/proto/*.proto` to `package gmountie.v1;` and the Go package to `pkg/proto/v1/`. Document the breaking-change policy (new fields are additive; field renumbering / removal requires a v2). Add `reserved` declarations in places that have already churned.

6. **Doc fixes.**
   - `docs/server/config.md:20,109` and `docs/quickstart.md:14` use `authentication:` — the parser expects `auth:`. Replace.
   - Add `CONTRIBUTING.md` (linked from `README.md:58`, currently 404).
   - Replace the placeholder `https://gmountie.docs.com` (`README.md:28`) with the actual docs URL or remove it.
   - Add a "running gMountie" guide that covers config layout, expected XDG paths, log/metric inspection.

**Out of scope:**

- Frontend (SvelteKit) test scaffolding — listed as a follow-up in Phase 5 ops work, because it's intertwined with the UI build pipeline.

**Definition of done:**

- CI red on `-race` failures or coverage drop.
- Every e2e failure-mode test passes deterministically (no sleep-based waits).
- `go.mod` no longer references pre-1.0 cobra or `pkg/errors`.
- `docs/server/config.md` examples can be pasted into a YAML file and the server starts.

---

## Phase 5 — Operations and packaging

**Goal:** the artifacts we ship are something a careful operator would deploy, even if they're not multi-tenant-safe.

**In scope:**

1. **Dockerfile.** `Dockerfile` is a single-stage Alpine copy that runs as root. Make it multi-stage (build then runtime), run as a non-root user, add a `HEALTHCHECK` against the Phase 2 health endpoint, attach OCI labels, drop unnecessary tools from the runtime image.

2. **Helm chart.** `deployments/charts/gmountie-server` has the right skeleton but probes are commented out (`templates/deployment.yaml:45-48`), `resources`, `podSecurityContext`, and `securityContext` are empty, and `image.tag: master` (`values.yaml:14,44,62`) is mutable. Wire the probes to the Phase 2 health endpoints, add sensible defaults for resource requests/limits, set `runAsNonRoot: true` and a `readOnlyRootFilesystem`-compatible layout, parameterise the image tag and pin it via the chart `appVersion`.

3. **docker-compose example hygiene.** `deployments/compose/docker-compose.yaml:17-20` runs a `fix-permissions` sidecar that `chmod 777`'s the data dir. Replace with explicit uid/gid mapping and document the required host-side setup. Move the `admin/admin` password to a `.env` example file with a clear "change me" comment.

4. **Goreleaser.** Add SBOM generation, cosign signing for binaries and the Docker image, and `-trimpath`/`-buildvcs=true` builds. Track Linux arm64 release artifacts (the `goreleaser` config already builds them but they're not in the release archive in some places — verify).

5. **Frontend test scaffolding.** Add Vitest + a smoke test in `ui/frontend/` so adding tests stops being a setup task. Add a `task ui:test` target.

**Out of scope:**

- macOS / Windows server builds (project is Linux-only; the desktop UI is the only Wails target).
- Kubernetes operator.

**Definition of done:**

- `docker run` of the released image as a non-root user serves a volume.
- `helm install` with default values produces a pod that passes its readiness probe.
- `cosign verify` succeeds on a released artifact.
- `task ui:test` runs at least one Svelte test green.

---

## Phase 6 — Security hardening (deferred, not implemented)

**This phase is documented so it isn't forgotten. It is explicitly out of scope until Phases 1–5 are done.** Some items here are critical and may be reordered up if the project's deployment context changes (e.g. anything outside a trusted LAN).

The known gaps:

- **TLS is advertised but not implemented.** `pkg/client/grpc/client.go:120` hardcodes `insecure.NewCredentials()` and the docs (`docs/client/config.md:39`) suggest a `tls: bool` field that no code path honours. Every connection today is plaintext.
- **Basic-auth credentials travel in plaintext** because the gRPC client sets `RequireTransportSecurity() = false` (`pkg/client/grpc/auth.go:31`). With #1, every request leaks the password.
- **Passwords are stored and compared in plaintext.** `pkg/server/service/auth.go:87` does string equality; `pkg/server/config/auth.go:75-77` stores them as plain `string`; `deployments/compose/config.yaml:9` ships `admin/admin`. Needs bcrypt or argon2 + constant-time compare.
- **Privilege escalation via client-supplied uid.** `pkg/server/controller/utils.go:12-19` populates `fuse.Context.Owner.{Uid,Gid}` from the proto request, and `pkg/server/io/middleware/asume_user.go:29` feeds that to `setfsuid`. A client can claim uid 0. The uid should come from the authenticated user, not from the wire.
- **No per-user volume ACL.** Any authenticated user can access any volume by setting the `volume` field. `pkg/server/controller/{fs,file,volume}.go`.
- **gRPC reflection registered before any auth check** (`pkg/server/grpc/server.go:83`). Schema is enumerable by any unauthenticated peer.
- **`/metrics` is world-readable.** `pkg/server/grpc/server.go:183` binds the default HTTP mux with no auth.
- **No request size or concurrency limits.** No `grpc.MaxRecvMsgSize`, no `KeepaliveEnforcementPolicy`, no rate-limiting interceptor. A WriteRequest decompresses Snappy into memory without bounds.
- **Path inputs are not normalised** at the controller layer. The loopback FS is the only defence against `../` (`pkg/server/controller/fs.go:36`, `pkg/server/controller/file.go:51`).
- **"none" auth is silently allowed** at config parse time (`pkg/server/config/auth.go:35-36`).

When this phase opens, it gets its own design doc and decomposition.

## Appendix: working agreements

- Each phase opens with its own brainstorm → design doc in `docs/superpowers/specs/`, then an implementation plan, then code.
- All work happens on `develop` (or branches off `develop`). `master` only receives merged phase milestones.
- Commit messages: plain `type: subject`; no `Co-Authored-By:` / `Signed-off-by:` trailers for this repo.
- "Reliable" is measured by the criteria in [What we mean by "works perfectly end-to-end"](#what-we-mean-by-works-perfectly-end-to-end). Add to that section if we discover new criteria; don't redefine it silently.
