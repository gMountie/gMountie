# Failure-Mode E2E Suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Prove gMountie's reliability story under adversarial conditions — wrong credentials, the server dying mid-transfer, a network drop with an open file handle, multi-gigabyte files, and many concurrent clients — with deterministic, non-flaky e2e tests.

**Architecture:** Build on the existing `test/e2e/utils.AppTestingContext` harness. Add a small set of harness primitives the failure tests need (health-probe readiness, server stop/restart on a stable address, a controllable TCP proxy for network-drop simulation), then add focused test suites. Mount-dependent cases run as real FUSE mounts on the kubevirt VM (kernel 6.8, Go 1.26.2); credential/concurrency cases that don't need a kernel mount run in-process via bufconn.

**Tech Stack:** Go, testify suites, `google.golang.org/grpc` (real loopback TCP via `WithTCPTransport`), gRPC health probe, go-fuse mounts on the VM.

**Reference:** roadmap Phase 5 §2 (E2E failure modes) + §3 (drop the 1s sleep readiness gate). Phase 1 session reclamation: `pkg/server/service/session.go` (`GracePeriod`, default 30s; `Resume` cancels the reap timer). Memory: `feedback-vm-availability` (VM reached via `virtctl ssh ubuntu@vmi/gmountie-dev/gmountie-test -i ~/.ssh/id_rsa -t '-o StrictHostKeyChecking=no' -t '-o UserKnownHostsFile=/dev/null'`); `feedback-fuse-test-env` (mounts need the VM, not the sandbox).

---

## VM run recipe (for every task that mounts FUSE)

```bash
REPO=/home/john/git/gMountie/gMountie/.claude/worktrees/<this-worktree>
git -C "$REPO" archive --format=tar.gz -o /tmp/fme.tgz HEAD   # commit first, then archive
virtctl scp -i ~/.ssh/id_rsa -t '-o StrictHostKeyChecking=no' -t '-o UserKnownHostsFile=/dev/null' \
  /tmp/fme.tgz ubuntu@vmi/gmountie-dev/gmountie-test:/tmp/fme.tgz
virtctl ssh ubuntu@vmi/gmountie-dev/gmountie-test -i ~/.ssh/id_rsa \
  -t '-o StrictHostKeyChecking=no' -t '-o UserKnownHostsFile=/dev/null' \
  -c 'cd /tmp && rm -rf fme && mkdir fme && tar xzf fme.tgz -C fme && cd fme && go test -count=1 -timeout 300s -run <Suite> ./test/e2e/fs/ 2>&1 | tail -30'
```
FUSE mount in the VM works as the `ubuntu` user (fusermount3 present); no sudo needed unless a case sets identity enforcement.

---

## File Structure

**Create:**
- `test/e2e/utils/proxy.go` — `TCPProxy`: a controllable loopback forwarder (`Sever()` drops all conns, `Restore()` resumes accepting). ~70 LOC.
- `test/e2e/utils/proxy_test.go` — unit test for the proxy (sever drops an in-flight conn; restore accepts again).
- `test/e2e/fs/failure_killed_test.go` — server-killed-mid-read / mid-write (VM).
- `test/e2e/fs/failure_reconnect_test.go` — network drop + open-fd session reclamation (VM, via proxy).
- `test/e2e/fs/failure_largefile_test.go` — ≥4 GiB integrity (VM, size-gated).
- `test/e2e/api/failure_auth_test.go` — wrong-password handshake rejection (in-process).
- `test/e2e/api/failure_concurrent_test.go` — many concurrent clients on one volume (in-process).

**Modify:**
- `test/e2e/utils/app.go` — replace the `time.Sleep(1*time.Second)` readiness gate in `Start()` with a gRPC health-probe poll; add `StopServer()`, `StartServer()`, `RestartServer()`; let `WithTCPTransport` optionally route the client through a `TCPProxy` (a `WithProxy()` option, or expose the listener addr so a test wires its own proxy).

---

## Task 1: Harness foundation — health-probe readiness + server restart + TCP proxy

**Files:** `test/e2e/utils/app.go`, `test/e2e/utils/proxy.go`, `test/e2e/utils/proxy_test.go`

This is the prerequisite for all mount-based failure tests. No flaky sleeps; deterministic stop/restart.

- [ ] **Step 1 — health-probe readiness.** Replace `time.Sleep(1 * time.Second)` in `Start()` with a poll of the gRPC health service (`grpc_health_v1.NewHealthClient(conn).Check(... service:"")`) until `SERVING`, with a 5s deadline. The server already registers `grpc.health.v1.Health` (`HealthService`, always on). Removes the single worst source of e2e timing flakiness.
  - Run: existing `./test/e2e/api/...` still green (no behavior change beyond faster/robust startup).
- [ ] **Step 2 — server stop/restart.** Add:
  - `(*AppTestingContext) StopServer()` — `c.server.Stop(true)` (graceful) and wait for Serve to return.
  - `(*AppTestingContext) StartServer() error` — for TCP, re-`net.Listen("tcp", c.tcpListener.Addr().String())` on the same address (Go sets SO_REUSEADDR; loopback rebinds cleanly), rebuild the gRPC server (`grpcServer.NewServer(...)` with the existing `serverCtx` + server options + creds) over the new listener, `go Serve()`, health-probe to SERVING. Reuse `serverCtx` (NewServerAppContext re-registration is `-count=N` safe).
  - `(*AppTestingContext) RestartServer() error` — `StopServer()` then `StartServer()`.
  - **bufconn note:** restart is only supported under `WithTCPTransport` (bufconn listeners can't rebind). `StartServer` returns an error if called on a bufconn context.
- [ ] **Step 3 — TCP proxy** (`proxy.go`): `NewTCPProxy(upstream string) (*TCPProxy, error)` listens on `127.0.0.1:0`, forwards bidirectionally to `upstream`. `Addr() string`. `Sever()` closes all active conns + pauses accepting. `Restore()` resumes accepting. `Close()`. Track active conns under a mutex; `Sever` closes them so the client's `ClientConn` sees the drop.
- [ ] **Step 4 — proxy unit test** (`proxy_test.go`): start a trivial echo upstream; dial through the proxy; `Sever()` → in-flight conn errors; `Restore()` → a fresh dial succeeds. testify suite.
- [ ] **Verify:** `go test -count=1 ./test/e2e/utils/`; `go vet ./test/e2e/...`; existing api e2e still green.
- [ ] **Commit:** `test(e2e/utils): health-probe readiness + server restart + TCP proxy`

## Task 2: Auth failure (in-process)

**Files:** `test/e2e/api/failure_auth_test.go`

- [ ] **Test** `AuthFailureSuite`:
  - `TestWrongPasswordRejected`: server with `WithBasicAuth("test", "correct")`; build a second client via `NewClientAs("test", "wrong")`; its `Connect()`/first RPC must fail with `codes.Unauthenticated` (the handshake `Create` is rejected). Assert via `status.FromError`.
  - `TestUnknownUserRejected`: `NewClientAs("ghost", "x")` → Unauthenticated.
  - `TestCorrectPasswordStillWorks`: positive control — the configured user authenticates and lists the volume.
- [ ] No FUSE; in-process. Run locally: `go test -count=1 -run AuthFailureSuite ./test/e2e/api/`.
- [ ] **Commit:** `test(e2e): auth-failure handshake rejection`

## Task 3: Server killed mid-read / mid-write (VM, real FUSE)

**Files:** `test/e2e/fs/failure_killed_test.go` (uses Task 1 restart + `WithTCPTransport`)

- [ ] **Test** `ServerKilledSuite` (skip if `os.Geteuid()` can't mount / not on VM — guard like other fs suites):
  - `TestKilledMidReadSurfacesErrorThenRecovers`: write a multi-MiB file server-side; mount; start reading it in a goroutine while `StopServer()` fires mid-stream; assert the read returns an error (EIO-class, **not** a hang or panic) within a bounded time. Then `StartServer()`; assert a **fresh** `os.ReadFile` of the same path succeeds and content-matches (mount self-heals via session recovery → fresh Create).
  - `TestKilledMidWriteSurfacesError`: begin writing a large file; `StopServer()` mid-write; assert the write/close surfaces an error rather than silently succeeding; after `StartServer()`, a fresh write+read round-trips.
  - Use a modest size (e.g. 64–128 MiB) + readahead/coalesce defaults so the transfer is long enough to interrupt deterministically; if timing is racy, throttle via `scripts/start-slow-loopback.sh` semantics or a small `WithReadahead`/frame size so the stream spans the kill.
- [ ] **Run on the VM** (recipe above), `-timeout 120s`.
- [ ] **Commit:** `test(e2e/fs): server killed mid-read/mid-write surfaces error and recovers`

## Task 4: Network drop + open-fd session reclamation (VM, real FUSE, via proxy)

**Files:** `test/e2e/fs/failure_reconnect_test.go`

This is the marquee Phase 1 validation: the connection drops but the **server stays up**, so the session is reclaimed (within `GracePeriod`, 30s default) and an **open file handle survives**.

- [ ] **Setup:** `WithTCPTransport`; put a `TCPProxy` between client and server (client endpoint = proxy addr; proxy upstream = server's tcpListener addr). The harness `WithProxy()` (Task 1) or wire it in the test.
- [ ] **Test** `ReconnectOpenFDSuite`:
  - `TestOpenFDSurvivesConnectionDrop`: write a file server-side; mount; `os.Open` it and read the first chunk (holds a server-side fd). `proxy.Sever()` → the gRPC conn drops, keepalive errors, the client enters recovery. Wait briefly (< GracePeriod so the session isn't reaped). `proxy.Restore()`. Assert the client's session recovers via **Resume** (`SessionID()` unchanged — capture before/after through `GetClient().SessionID()`), and **continue reading the same open fd** to EOF with correct content. (If Resume can't reattach the fd, the read returns ESTALE — assert the *graceful* outcome the design specifies, not a hang.)
  - `TestReadAfterRestoreContentMatches`: end-to-end integrity of the bytes read across the drop.
- [ ] **Note on expected behavior:** confirm against `session.go` semantics — Resume within `GracePeriod` reclaims the session (fds preserved); the test asserts the open fd keeps working. If the implementation reaps fds on disconnect regardless, assert ESTALE + a successful re-open instead, and document which contract holds.
- [ ] **Run on the VM**, `-timeout 120s`.
- [ ] **Commit:** `test(e2e/fs): open fd survives a connection drop (session reclamation)`

## Task 5: Large files ≥4 GiB (VM, real FUSE, size-gated)

**Files:** `test/e2e/fs/failure_largefile_test.go`

Guards against 32-bit offset overflow at the 4 GiB boundary.

- [ ] **Test** `LargeFileSuite`, **gated** behind an env flag (`GMOUNTIE_E2E_LARGEFILE=1`) or `testing.Short()` skip, so it doesn't bloat every CI run:
  - `TestWriteReadAcross4GiB`: stream a deterministic pattern (e.g. a counter or `rand` with fixed seed) of `(4 GiB + 8 MiB)` through the mount with `io.Copy`, then read it back and verify a rolling sha256 (don't hold 4 GiB in RAM — hash on the fly). Assert size + digest. Crosses the `int32`/`uint32` offset boundary in Read/Write/Truncate paths.
  - Optionally also a single `pread` at offset `> 4 GiB` to assert offset plumbing directly.
- [ ] **Run on the VM** with the env flag set, generous `-timeout` (e.g. 600s); confirm VM disk has room (`df`).
- [ ] **Commit:** `test(e2e/fs): ≥4 GiB file integrity across the offset boundary`

## Task 6: Many concurrent clients (in-process)

**Files:** `test/e2e/api/failure_concurrent_test.go`

- [ ] **Test** `ConcurrentClientsSuite`:
  - `TestConcurrentReadersOneVolume`: one server, one volume seeded with N files; spin M clients (`NewSiblingClient`/`NewClientAs`) issuing concurrent GetAttr/Read/List against the same volume; `errgroup` join; assert no errors, no data races (this suite runs under the `-race` job too), and every read content-matches.
  - `TestConcurrentWritersDistinctPaths`: M clients each write their own path concurrently; assert all land intact (distinct paths → no contention expected, validates the server's per-request volume lookup + identity binding under load).
- [ ] In-process (bufconn) is sufficient for the concurrency-correctness goal and keeps it fast + in the `-race` job. Note in a comment that multi-mount FUSE concurrency is Phase 8 (VFS).
- [ ] **Run:** `go test -race -count=1 -run ConcurrentClientsSuite ./test/e2e/api/`.
- [ ] **Commit:** `test(e2e): concurrent clients on one volume`

## Task 7 (optional): Cache hit/miss/eviction audit

**Files:** review `test/e2e/api/cache_test.go`, `cache_persist_test.go`, `cache_subscribe_test.go`

- [ ] **Audit** existing cache coverage against the roadmap's "hit/miss/eviction paths." Persist + subscribe + restart are already covered. If the **disk-cap eviction** path (`DiskMaxBytes` exceeded → LRU eviction) lacks a direct assertion, add one targeted test; otherwise document that coverage is sufficient and close the item.
- [ ] **Commit** (only if a gap is filled): `test(e2e): disk-cap eviction path`

---

## Self-Review
- **Roadmap coverage:** auth-fail (T2), killed-mid-read/write (T3), drop/reconnect-with-open-fd (T4), ≥4 GiB (T5), concurrent clients (T6), cache paths (T7), drop-the-1s-sleep (T1 step 1). VFS multi-volume e2e remains deferred to Phase 8 (per roadmap).
- **Determinism:** no `time.Sleep` readiness; health-probe gating; the proxy + restart give deterministic drop/kill points. The reconnect test stays within `GracePeriod` so the Resume-vs-Create outcome is deterministic.
- **No placeholders:** each task names files, the test cases, the run command, and the VM recipe for mount-based suites.
- **Cost control:** large-file is env-gated; concurrent + auth are in-process; only kill/reconnect/large-file need the VM.

## Execution Handoff
Subagent-driven, T1 first (everything depends on the harness primitives), then T2–T6 (independent; T2/T6 in-process, T3/T4/T5 on the VM), T7 optional. Each mount-based task verifies on the VM before its commit. Open one PR for the suite (or split T1+in-process vs VM-tests into two PRs if review size warrants).
