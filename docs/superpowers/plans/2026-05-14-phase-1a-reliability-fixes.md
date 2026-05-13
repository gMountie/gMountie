# Phase 1a — Reliability Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop the server from killing itself on non-fatal conditions, stop the controllers from panicking on missing fields, wire SIGTERM to a graceful shutdown, and un-skip the two config tests that should be passing. This is the first of four plans implementing roadmap Phase 1.

**Architecture:** No protocol or major refactor. Each fix is local to a file or to a small handful of files. The goal is to make the existing system *stable enough* that we can build sessions + idempotency on top of it in Plan 1c without dragging crashes along.

**Tech Stack:** Go 1.26, go-fuse v2.10, cobra v1, viper v1.21, zap.

**Scope context (read once, then forget):**
- Phase 1 of the roadmap (`docs/superpowers/specs/2026-05-13-roadmap-reliability-and-performance.md`) has 9 items. This plan covers items 1–3, 8, 9 (the "tactical" half). Items 4–7 (context propagation, session concept, idempotency tokens, retry) are deferred to Plans 1b, 1c, 1d.
- FUSE-touching tests (`pkg/client/mount/...`, `test/e2e/fs/...`) cannot be run in the Claude sandbox or GoLand's terminal due to known fd-leakage; the user runs them in a plain terminal. The CI workflow is the final gate.
- Backwards compatibility is explicitly *not* a concern (see roadmap Appendix C).

---

## File Structure

Files this plan touches:

- **Modify:** `pkg/utils/log/log.go` — drop the broken `defer Sync()` from `init()`.
- **Modify:** `pkg/server/grpc/server.go` — metrics-server failure logs+returns instead of `log.Fatal`.
- **Modify:** `pkg/client/mount/single.go` — `Mount` returns the `fuse.NewServer` error instead of `log.Fatal`.
- **Modify:** `pkg/server/io/middleware/asume_user.go` — `changeUser` returns an error; all wrapper methods translate it to `fuse.EPERM`.
- **Modify:** `pkg/server/controller/utils.go` — `createContext` no-longer panics on a nil `Caller` or `Owner`; it returns a zero-valued context.
- **Modify:** `pkg/server/controller/fs.go` — `StatFs` handler guards the nil reply from the underlying filesystem.
- **Modify:** `pkg/server/app.go` — `Start` accepts a `context.Context` and shuts down gracefully when it's cancelled.
- **Modify:** `cmd/commands/serve.go` — installs a `SIGTERM`/`SIGINT` handler, passes a cancellable context to `server.Start`.
- **Modify:** `cmd/commands/serve_test.go` — updates the `serverStart` stub signature to match.
- **Modify:** `pkg/server/config/auth.go` — `NewFromConfig` returns an error on a nil viper instead of nil-derefing.
- **Modify:** `pkg/server/config/config.go` — `ParseConfig` binds env vars to nested keys so `GMOUNTIE_SERVER_PORT` overrides `server.port`.
- **Modify:** `pkg/server/config/config_test.go` — un-skip `TestParse_EmptyConfig` and `TestParse_EnvVarOverride`; both are expected to pass after the two fixes above.
- **Modify:** `pkg/server/io/middleware/asume_user_test.go` — add a test for the `setfsuid`-fails-returns-EPERM behaviour.
- **Modify:** `pkg/server/controller/utils_test.go` (create if missing) — add a test that `createContext` with a nil `Caller` returns a sane context.
- **Modify:** `pkg/server/controller/fs_test.go` — add a test that `StatFs` returns an error when the underlying filesystem returns nil.

**Working-set test command (Claude sandbox / GoLand terminal):**
```
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

**Full validation (user, plain terminal):**
```
task test
task lint
```

---

## Task 1: Remove the broken `defer Log.Sync()` in `init()`

**Why:** The `defer` fires when `init()` returns, not at process exit, so it's currently a no-op (or worse, syncs immediately before anything was logged). Roadmap §Phase 1.1.

**Files:**
- Modify: `pkg/utils/log/log.go:36-38`
- Test: none — there are no existing tests in `pkg/utils/log/` and adding one for a deletion is overkill.

- [ ] **Step 1: Open and inspect the current `init()` to confirm location**

```bash
sed -n '30,40p' pkg/utils/log/log.go
```

Expected: the three-line `defer func(...) { _ = Log.Sync() }(Log)` block at lines 36-38.

- [ ] **Step 2: Delete the broken defer**

Apply this edit to `pkg/utils/log/log.go`:

Replace:
```go
	log.Default().SetOutput(logger.Writer())
	defer func(Log *zap.Logger) {
		_ = Log.Sync()
	}(Log) // Flushes buffer, if any
}
```

With:
```go
	log.Default().SetOutput(logger.Writer())
}
```

- [ ] **Step 3: Verify the working-set test suite still passes**

```bash
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

Expected: every package `ok`.

- [ ] **Step 4: Commit**

```bash
git add pkg/utils/log/log.go
git commit -m "fix(log): drop broken defer Sync() in init()"
```

---

## Task 2: Make metrics-server startup failure non-fatal

**Why:** `log.Fatal` on metrics-listener failure currently kills the whole server if anything else (an earlier instance, another exporter) holds `:9090`. Metrics are observability, not core. Roadmap §Phase 1.1.

**Files:**
- Modify: `pkg/server/grpc/server.go:181-189`
- Test: `pkg/server/grpc/server_test.go` (create — no existing test file).

- [ ] **Step 1: Write the failing test**

Create `pkg/server/grpc/server_test.go`:

```go
package grpc

import (
	"net"
	"testing"
	"time"

	"gmountie/pkg/server/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStartMetricsServer_PortInUseDoesNotPanic verifies that when the
// metrics port is already occupied, startMetricsServer logs and returns
// instead of crashing the process via log.Fatal.
func TestStartMetricsServer_PortInUseDoesNotPanic(t *testing.T) {
	// Occupy :9090 so the metrics server's ListenAndServe will fail.
	blocker, err := net.Listen("tcp", ":9090")
	require.NoError(t, err, "if this fails, :9090 was already busy externally")
	defer blocker.Close()

	s := &Server{
		config: &config.Config{
			Server: &config.ServerConfig{Address: "127.0.0.1", Port: 0, Metrics: true},
		},
		metricsServer: nil,
	}
	// Initialise then start. Both must complete without exiting the process.
	s.initMetricsServer()
	require.NotNil(t, s.metricsServer)

	// Replace the no-op global mux with a fresh one for hygiene.
	// (We just need to confirm the goroutine doesn't crash us.)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.startMetricsServer()
		// startMetricsServer launches a goroutine; give it a beat to fail.
		time.Sleep(150 * time.Millisecond)
	}()

	select {
	case <-done:
		// If we got here without the test binary exiting via log.Fatal, we win.
		assert.True(t, true)
	case <-time.After(2 * time.Second):
		t.Fatal("startMetricsServer hung")
	}
}

// Unused-import insurance:
var _ = prometheus.NewRegistry
```

- [ ] **Step 2: Run the test and confirm it fails (or kills the process)**

```bash
go test -count=1 -run TestStartMetricsServer_PortInUseDoesNotPanic ./pkg/server/grpc/...
```

Expected before the fix: the test runner exits with status `1` and a `FATAL ... failed to start metrics server` line (because `log.Log.Fatal` calls `os.Exit(1)` from inside the goroutine).

- [ ] **Step 3: Replace the Fatal with a logged error**

In `pkg/server/grpc/server.go`, replace the `startMetricsServer` body:

```go
// startMetricsServer starts the metrics server.
func (s *Server) startMetricsServer() {
	if s.metricsServer == nil {
		log.Log.Debug("metrics server is disabled")
		return
	}
	s.metricsServer.InitializeMetrics(s.server)
	// Start the metrics server.
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Log.Info("starting metrics server", zap.String("port", "9090"), zap.String("path", "/metrics"))
		if err := http.ListenAndServe(":9090", nil); err != nil {
			// Best-effort: a metrics-listener failure must not kill the server.
			log.Log.Error("metrics server stopped", zap.Error(err))
		}
	}()
}
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
go test -count=1 -run TestStartMetricsServer_PortInUseDoesNotPanic ./pkg/server/grpc/...
```

Expected: `PASS`.

- [ ] **Step 5: Run the broader server-grpc tests to catch regressions**

```bash
go test -count=1 ./pkg/server/grpc/...
```

Expected: every test PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/grpc/server.go pkg/server/grpc/server_test.go
git commit -m "fix(server): make metrics listener failure non-fatal"
```

---

## Task 3: Propagate `fuse.NewServer` error from `SingleVolumeMounter.Mount`

**Why:** `log.Log.Sugar().Fatalf("mount fail: %v\n", err)` exits the *client* process. The caller already returns `error`; we should too. Roadmap §Phase 1.1.

**Files:**
- Modify: `pkg/client/mount/single.go:46-49`
- Test: `pkg/client/mount/single_test.go` — there is no clean way to mock `fuse.NewServer` (it's a concrete function), so we don't add a new unit test for this path. The user's plain-terminal `task test` run is the gate.

- [ ] **Step 1: Replace the Fatal with an error return**

In `pkg/client/mount/single.go`, replace:

```go
	server, err := fuse.NewServer(connector.RawFS(), path, createMountOptions(m.client.GetEndpoint(), volume))
	if err != nil {
		log.Log.Sugar().Fatalf("mount fail: %v\n", err)
	}
```

With:

```go
	server, err := fuse.NewServer(connector.RawFS(), path, createMountOptions(m.client.GetEndpoint(), volume))
	if err != nil {
		return errors.Wrap(err, "mount fail")
	}
```

(`errors` from `github.com/pkg/errors` is already imported at the top of the file.)

- [ ] **Step 2: Remove the now-unused `log` import**

Check the file after the edit:

```bash
grep -E '"gmountie/pkg/utils/log"' pkg/client/mount/single.go && echo "REMOVE IT" || echo "already gone"
```

If `log` is still imported but no longer referenced anywhere in the file, delete the import line. (`single.go` also uses `log.Log.Info`/`Error` in `Unmount` — verify with `grep -n 'log\.' pkg/client/mount/single.go` before removing.)

- [ ] **Step 3: Build to confirm no regression**

```bash
go build ./pkg/client/mount/...
```

Expected: silent success.

- [ ] **Step 4: Commit**

```bash
git add pkg/client/mount/single.go
git commit -m "fix(mount): return fuse.NewServer error instead of log.Fatal"
```

---

## Task 4: Make `setfsuid`/`setfsgid` failures fail the request, not the server

**Why:** `pkg/server/io/middleware/asume_user.go` calls `log.Log.Fatal` on every per-request `setfsuid` or `setfsgid` failure. A request-level syscall failure must not kill the server. Roadmap §Phase 1.1. Also: the *cleanup* failure (restoring root) is genuinely dangerous — if it fails, the OS thread is left running as the user. The safe response there is to *not* call `UnlockOSThread`, so the tainted thread dies with the goroutine (per `runtime.LockOSThread` semantics).

**Files:**
- Modify: `pkg/server/io/middleware/asume_user.go` — refactor `changeUser` to return `(func(), error)`; update all 16 wrapper methods.
- Modify: `pkg/server/io/middleware/asume_user_test.go` — add a test that `setfsuid` failure → request returns `fuse.EPERM`.

- [ ] **Step 1: Write the failing test for the error path**

Append to `pkg/server/io/middleware/asume_user_test.go` (before `func TestAssumeUserMiddlewareTestSuite`):

```go
// TestGetAttr_SetfsuidFailureReturnsEPERM verifies that when setfsuid fails,
// the wrapper returns fuse.EPERM rather than killing the process.
func (s *AssumeUserMiddlewareTestSuite) TestGetAttr_SetfsuidFailureReturnsEPERM() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}},
	}
	// setfsuid returns an error; the wrapper must NOT invoke setfsgid or the
	// underlying FileSystem, and must return EPERM.
	s.setfs.On("Setfsuid", 1000).Return(syscall.EPERM)

	attr, status := s.middleware.GetAttr("testfile", ctx)

	s.Nil(attr)
	s.Equal(fuse.EPERM, status)
	s.setfs.AssertExpectations(s.T())
	// fs should NOT have been called.
	s.fs.AssertNotCalled(s.T(), "GetAttr", mock.Anything, mock.Anything)
}
```

- [ ] **Step 2: Run the new test and confirm it fails by killing the process**

```bash
go test -count=1 -run TestAssumeUserMiddlewareTestSuite/TestGetAttr_SetfsuidFailureReturnsEPERM ./pkg/server/io/middleware/...
```

Expected before the fix: process exits with `FATAL ... failed to set user id`.

- [ ] **Step 3: Refactor `changeUser` to return an error**

In `pkg/server/io/middleware/asume_user.go`, replace the existing `changeUser` with:

```go
// changeUser changes the user and group of the current OS thread to those in
// the FUSE context. It returns a cleanup function that restores root, plus an
// error. On error, the OS thread is unlocked before returning (so callers must
// not call the cleanup function).
//
// If the cleanup function itself fails to restore root, it deliberately does
// NOT call runtime.UnlockOSThread — the tainted thread will die with the
// goroutine, which is the safe outcome per runtime.LockOSThread semantics.
func changeUser(context *fuse.Context) (func(), error) {
	userId := context.Owner.Uid
	groupId := context.Owner.Gid
	runtime.LockOSThread()

	if err := setfsuid(int(userId)); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsgid(int(groupId)); err != nil {
		// uid was set; best-effort restore before unlocking
		_ = setfsuid(syscall.Geteuid())
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setfsuid(syscall.Geteuid()); err != nil {
			log.Log.Error("failed to restore root uid; leaking OS thread", zap.Error(err))
			return // do NOT UnlockOSThread; let the tainted thread die with the goroutine
		}
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("failed to restore root gid; leaking OS thread", zap.Error(err))
			return
		}
		runtime.UnlockOSThread()
	}, nil
}
```

- [ ] **Step 4: Update every wrapper method to handle the error**

There are 16 wrappers (`GetAttr`, `Chmod`, `Chown`, `Utimens`, `Truncate`, `Access`, `Link`, `Mkdir`, `Mknod`, `Rename`, `Rmdir`, `Unlink`, `GetXAttr`, `ListXAttr`, `RemoveXAttr`, `SetXAttr`, `Open`, `Create`, `OpenDir`, `Symlink`, `Readlink`). Each follows the same pattern. Here is the new `GetAttr`:

```go
func (a *assumeUserMiddleware) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.GetAttr(name, context)
}
```

Apply the same shape to every method. For methods that return only `fuse.Status` (no first value), the early-return is just `return fuse.EPERM`. For methods that return three values like `GetXAttr` (which returns `(data, code)`), nil out the data: `return nil, fuse.EPERM`. For `Readlink` which returns `(string, fuse.Status)`: `return "", fuse.EPERM`. For `OpenDir` which returns `([]fuse.DirEntry, fuse.Status)`: `return nil, fuse.EPERM`. For `Open`/`Create` which return `(nodefs.File, fuse.Status)`: `return nil, fuse.EPERM`.

Each method should look like:

```go
func (a *assumeUserMiddleware) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeUser(context)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Chmod(name, mode, context)
}
```

- [ ] **Step 5: Run the failing test — it should pass now**

```bash
go test -count=1 -run TestAssumeUserMiddlewareTestSuite/TestGetAttr_SetfsuidFailureReturnsEPERM ./pkg/server/io/middleware/...
```

Expected: `PASS`.

- [ ] **Step 6: Run the full middleware suite to catch regressions**

```bash
go test -count=1 ./pkg/server/io/middleware/...
```

Expected: every test PASS. (The existing happy-path tests already set up the setfsuid mocks; they should still work because `changeUser` returns a non-nil cleanup function on success.)

- [ ] **Step 7: Commit**

```bash
git add pkg/server/io/middleware/asume_user.go pkg/server/io/middleware/asume_user_test.go
git commit -m "fix(middleware): assume-user failure returns EPERM, not log.Fatal"
```

---

## Task 5: Nil-guard `createContext` against missing `Caller`/`Owner`

**Why:** `pkg/server/controller/utils.go:11-22` dereferences `caller.Owner.Uid/Gid` and `caller.Pid` with no nil check. A request whose `Caller` field is `nil` (malformed client, future proto extension) currently panics the handler goroutine. Roadmap §Phase 1.1.

**Files:**
- Modify: `pkg/server/controller/utils.go`
- Test: `pkg/server/controller/utils_test.go` (create).

- [ ] **Step 1: Write the failing test**

Create `pkg/server/controller/utils_test.go`:

```go
package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateContext_NilCallerDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("createContext panicked on nil caller: %v", r)
		}
	}()
	c := createContext(context.Background(), nil)
	assert.NotNil(t, c)
	assert.Equal(t, uint32(0), c.Caller.Owner.Uid)
	assert.Equal(t, uint32(0), c.Caller.Owner.Gid)
	assert.Equal(t, uint32(0), c.Caller.Pid)
}
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
go test -count=1 -run TestCreateContext_NilCallerDoesNotPanic ./pkg/server/controller/...
```

Expected: `panic: runtime error: invalid memory address or nil pointer dereference`.

- [ ] **Step 3: Add the nil guard**

Replace the body of `createContext` in `pkg/server/controller/utils.go`:

```go
// createContext creates a new fuse.Context from the given context.Context.
// A nil caller (or nil caller.Owner) is treated as uid/gid/pid = 0 — the
// downstream filesystem layer is responsible for rejecting requests that
// require a real caller identity (e.g. the assume-user middleware will fail
// authentication checks on uid 0 if the server isn't running as root).
func createContext(ctx context.Context, caller *proto.Caller) *fuse.Context {
	var uid, gid, pid uint32
	if caller != nil {
		pid = caller.Pid
		if caller.Owner != nil {
			uid = caller.Owner.Uid
			gid = caller.Owner.Gid
		}
	}
	return &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: uid, Gid: gid},
			Pid:   pid,
		},
		Cancel: ctx.Done(),
	}
}
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
go test -count=1 -run TestCreateContext_NilCallerDoesNotPanic ./pkg/server/controller/...
```

Expected: `PASS`.

- [ ] **Step 5: Run the full controller suite**

```bash
go test -count=1 ./pkg/server/controller/...
```

Expected: every test PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/controller/utils.go pkg/server/controller/utils_test.go
git commit -m "fix(controller): guard createContext against nil Caller/Owner"
```

---

## Task 6: Nil-guard the `StatFs` handler against a nil filesystem reply

**Why:** `pkg/server/controller/fs.go:115-132` calls `fs.StatFs(request.Path)` and immediately dereferences the result. The go-fuse loopback implementation returns `nil` on error; that panics the handler goroutine. Roadmap §Phase 1.1.

**Files:**
- Modify: `pkg/server/controller/fs.go`
- Test: `pkg/server/controller/fs_test.go` (file exists; add a test).

- [ ] **Step 1: Locate the StatFs test setup in `fs_test.go`**

```bash
grep -n "StatFs\|TestStatFs\|func.*Suite" pkg/server/controller/fs_test.go | head
```

Read the surrounding test setup to mimic its pattern (mock filesystem, test suite). The test file already has a `*MockFileSystem` (mockery-generated) available.

- [ ] **Step 2: Write the failing test**

Append a method to the existing `RpcServerTestSuite` (or whichever suite owns the fs.go handlers) in `pkg/server/controller/fs_test.go`:

```go
// TestStatFs_NilReplyReturnsError verifies that when the underlying
// filesystem returns a nil StatFs reply, the handler returns an error
// rather than panicking on a nil deref.
func (s *RpcServerTestSuite) TestStatFs_NilReplyReturnsError() {
	s.fsService.EXPECT().GetVolumeFileSystem("vol").Return(s.fs, nil)
	s.fs.EXPECT().StatFs("/").Return(nil)

	reply, err := s.rpcServer.StatFs(context.Background(), &proto.StatFsRequest{
		Volume: "vol",
		Path:   "/",
	})

	s.Require().Error(err)
	s.Nil(reply)
}
```

If the existing suite uses different mock variable names (`s.mockFs`, etc.), adapt the test to use whichever pattern the file already uses. Discover with:

```bash
grep -n "fsService\|MockFileSystem\|rpcServer" pkg/server/controller/fs_test.go | head -20
```

- [ ] **Step 3: Run the test and confirm it fails**

```bash
go test -count=1 -run TestRpcServerTestSuite/TestStatFs_NilReplyReturnsError ./pkg/server/controller/...
```

Expected: panic with nil-deref.

- [ ] **Step 4: Add the nil guard**

In `pkg/server/controller/fs.go`, replace the `StatFs` handler body:

```go
func (r *RpcServerImpl) StatFs(ctx context.Context, request *proto.StatFsRequest) (*proto.StatFsReply, error) {
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	statfs := fs.StatFs(request.Path)
	if statfs == nil {
		return nil, status.Errorf(codes.NotFound, "statfs: filesystem returned no data for path %q", request.Path)
	}
	reply := &proto.StatFsReply{
		Blocks:  statfs.Blocks,
		Bfree:   statfs.Bfree,
		Bavail:  statfs.Bavail,
		Files:   statfs.Files,
		Ffree:   statfs.Ffree,
		Bsize:   statfs.Bsize,
		Namelen: statfs.NameLen,
		Frsize:  statfs.Frsize,
	}
	return reply, nil
}
```

Add the `status`/`codes` imports if not already present:

```go
import (
    // ...existing imports...
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)
```

- [ ] **Step 5: Run the test and confirm it passes**

```bash
go test -count=1 -run TestRpcServerTestSuite/TestStatFs_NilReplyReturnsError ./pkg/server/controller/...
```

Expected: `PASS`.

- [ ] **Step 6: Run the full controller suite**

```bash
go test -count=1 ./pkg/server/controller/...
```

Expected: every test PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/controller/fs.go pkg/server/controller/fs_test.go
git commit -m "fix(controller): return error when StatFs reply is nil"
```

---

## Task 7: Wire graceful shutdown to SIGTERM / SIGINT

**Why:** `cmd/commands/serve.go` never installs a signal handler. SIGTERM is an abrupt kill with in-flight RPCs dropped. `pkg/server/grpc/server.go:96-105` already exposes `Stop(graceful bool)`, calling `GracefulStop()` — it just isn't wired up. Roadmap §Phase 1.1.

**Files:**
- Modify: `pkg/server/app.go` — `Start` takes a context, returns when context is cancelled (after a graceful-stop deadline).
- Modify: `cmd/commands/serve.go` — installs `signal.NotifyContext` and passes the context to `Start`.
- Modify: `cmd/commands/serve_test.go` — update the `serverStart` stub signature.

- [ ] **Step 1: Update `Start` in `pkg/server/app.go` to accept a context**

Replace the `Start` function in `pkg/server/app.go`:

```go
// Start runs the server until ctx is cancelled. On cancellation it triggers a
// graceful shutdown bounded by shutdownDeadline; if that doesn't complete in
// time it forces a stop. Returns the first non-nil error among serve errors
// and shutdown errors.
func Start(ctx context.Context, cfg *config.Config) error {
	const shutdownDeadline = 30 * time.Second

	appCtx := NewServerAppContext(cfg)
	s := grpc.NewServer(
		cfg,
		appCtx.AuthService,
		appCtx.GetGrpcServices(),
	)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.Serve()
	}()

	select {
	case err := <-serveErr:
		if err != nil {
			return errors.Wrap(err, "failed to start server")
		}
		return nil
	case <-ctx.Done():
		log.Log.Info("shutdown signal received; draining in-flight requests",
			zap.Duration("deadline", shutdownDeadline))

		stopped := make(chan struct{})
		go func() {
			s.Stop(true)
			close(stopped)
		}()

		select {
		case <-stopped:
			log.Log.Info("server shut down gracefully")
			return nil
		case <-time.After(shutdownDeadline):
			log.Log.Warn("graceful shutdown timed out; forcing stop")
			s.Stop(false)
			return errors.New("shutdown deadline exceeded")
		}
	}
}
```

Add imports at the top of `pkg/server/app.go`:

```go
import (
	"context"
	"time"
	// ...existing imports...
	"go.uber.org/zap"
)
```

(Both `context` and `time` may already be present transitively but are not currently imported by `app.go`; verify with `head -20 pkg/server/app.go` after editing.)

- [ ] **Step 2: Update the `serverStart` indirection in `cmd/commands/serve.go`**

Replace the relevant block in `cmd/commands/serve.go`. The change:
1. Imports: add `context`, `os/signal`, `syscall`.
2. The function-type variable `serverStart` becomes `func(context.Context, *config.Config) error`.
3. Inside `RunE`, build a signal-cancellable context and pass it through.

After the edit, the file should look like (showing only changed pieces):

```go
import (
	"context"
	"gmountie/pkg/common/config"
	"gmountie/pkg/server"
	serverConfig "gmountie/pkg/server/config"
	"gmountie/pkg/utils/log"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)
```

```go
// For testing purposes
var serverStart = server.Start
```

```go
		// Start server with signal-driven graceful shutdown.
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		return serverStart(ctx, cfg)
```

(Replace the existing final `return serverStart(cfg)` line with the three lines above.)

- [ ] **Step 3: Update the `serve_test.go` stub to match**

In `cmd/commands/serve_test.go`, replace:

```go
	s.originalServerStart = serverStart
	serverStart = func(cfg *config.Config) error {
		s.serverStartCalled = true
		return nil
	}
```

With:

```go
	s.originalServerStart = serverStart
	serverStart = func(ctx context.Context, cfg *config.Config) error {
		s.serverStartCalled = true
		return nil
	}
```

And in the suite struct definition, change:

```go
	originalServerStart func(config2 *config.Config) error
```

to:

```go
	originalServerStart func(ctx context.Context, cfg *config.Config) error
```

Add `"context"` to the imports.

- [ ] **Step 4: Build to catch compile-time errors**

```bash
go build ./...
```

Expected: silent. (UI packages will continue to fail to build due to GTK — that's environment, not us.)

- [ ] **Step 5: Run the affected test suites**

```bash
go test -count=1 ./cmd/commands/... ./pkg/server/...
```

Expected: every test PASS.

- [ ] **Step 6: Add an integration-style test for graceful shutdown**

Add to `pkg/server/app_test.go` (create if missing):

```go
package server

import (
	"context"
	"net"
	"testing"
	"time"

	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/require"
)

// TestStart_ContextCancellationShutsDownGracefully verifies that cancelling
// the context passed to Start triggers a graceful stop and the function
// returns nil (no error) within a reasonable bound.
func TestStart_ContextCancellationShutsDownGracefully(t *testing.T) {
	// Find a free port so the test isn't flaky on busy machines.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := uint(lis.Addr().(*net.TCPAddr).Port)
	lis.Close()

	cfg := &config.Config{
		Server:  &config.ServerConfig{Address: "127.0.0.1", Port: port, Metrics: false},
		Auth:    config.NewNoneAuthConfig(),
		Volumes: []*config.VolumeConfig{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	startErr := make(chan error, 1)
	go func() {
		startErr <- Start(ctx, cfg)
	}()

	// Give the server a moment to bind.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-startErr:
		require.NoError(t, err, "graceful shutdown should not return an error")
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return within 5s of context cancel")
	}
}
```

If `config.NewNoneAuthConfig` doesn't exist with that exact name, look in `pkg/server/config/auth.go` for the equivalent factory — it might be `&NoneAuthConfig{}` or similar. Adapt the line.

- [ ] **Step 7: Run the new test**

```bash
go test -count=1 -run TestStart_ContextCancellationShutsDownGracefully ./pkg/server/...
```

Expected: `PASS`.

- [ ] **Step 8: Commit**

```bash
git add pkg/server/app.go pkg/server/app_test.go cmd/commands/serve.go cmd/commands/serve_test.go
git commit -m "feat(server): graceful shutdown on SIGTERM/SIGINT"
```

---

## Task 8: Un-skip `TestParse_EmptyConfig` (and fix the underlying panic)

**Why:** Roadmap §Phase 1.9. The test is currently skipped with "This test fails". Running it without the skip produces a nil-deref in `NewFromConfig` because `v.Sub("auth")` returns `nil` for an empty config and `NewFromConfig` deref's the nil viper at line 34 (`v.GetString("type")`).

**Files:**
- Modify: `pkg/server/config/auth.go` — `NewFromConfig` returns an error on a nil viper.
- Modify: `pkg/server/config/config_test.go` — un-skip the test.

- [ ] **Step 1: Confirm the failure mode**

Temporarily remove the `s.T().Skip("This test fails")` line in `TestParse_EmptyConfig` and run:

```bash
go test -count=1 -run TestConfigTestSuite/TestParse_EmptyConfig ./pkg/server/config/...
```

Expected: `panic: runtime error: invalid memory address or nil pointer dereference` originating from `pkg/server/config/auth.go:34`.

(If the panic doesn't reproduce, the failure mode is something else — investigate before proceeding.)

- [ ] **Step 2: Inspect the failure site**

```bash
sed -n '30,50p' pkg/server/config/auth.go
```

Look for the line that deref's `v.GetString(...)`. Note its line number.

- [ ] **Step 3: Add the nil-viper guard**

At the top of `NewFromConfig` in `pkg/server/config/auth.go`, add:

```go
func NewFromConfig(v *viper.Viper) (AuthConfig, error) {
	if v == nil {
		return nil, errors.New("auth: missing 'auth' section in config")
	}
	// ...existing body...
}
```

Add an `errors` import if not present:

```go
import (
	// ...existing imports...
	"github.com/pkg/errors"
)
```

- [ ] **Step 4: Run the test**

```bash
go test -count=1 -run TestConfigTestSuite/TestParse_EmptyConfig ./pkg/server/config/...
```

Expected: `PASS` (the test expects an error; the new error from `NewFromConfig` will bubble up through `ParseConfig` and back to `LoadConfigFromString`).

- [ ] **Step 5: Remove the `Skip` line permanently**

In `pkg/server/config/config_test.go`, delete:

```go
	s.T().Skip("This test fails") // TODO: Fix this test
```

(Just the one line in `TestParse_EmptyConfig`, leaving the rest of the test intact.)

- [ ] **Step 6: Run the full config suite**

```bash
go test -count=1 ./pkg/server/config/...
```

Expected: all tests PASS, including `TestParse_EmptyConfig`.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/config/auth.go pkg/server/config/config_test.go
git commit -m "fix(config): NewFromConfig errors on nil viper; un-skip empty-config test"
```

---

## Task 9: Un-skip `TestParse_EnvVarOverride` (and bind nested env vars)

**Why:** Roadmap §Phase 1.9. The test sets `GMOUNTIE_SERVER_PORT=9000` and expects it to override `server.port: 8000` from the YAML. It fails because `v.SetEnvPrefix("GMOUNTIE") + AutomaticEnv()` operates on the parent viper, but `NewServerConfig` reads from `v.Sub("server")`, a fresh sub-viper that doesn't inherit the env binding.

The fix is to explicitly bind the nested keys we care about on the parent viper before sub-vipering, and to set the env key replacer so `_` separators in env vars map to `.` separators in viper keys.

**Files:**
- Modify: `pkg/server/config/config.go` — bind nested env vars and set the replacer.
- Modify: `pkg/server/config/config_test.go` — un-skip the test.

- [ ] **Step 1: Confirm the failure mode**

Temporarily remove the `s.T().Skip("This test fails")` line in `TestParse_EnvVarOverride` and run:

```bash
go test -count=1 -run TestConfigTestSuite/TestParse_EnvVarOverride ./pkg/server/config/...
```

Expected: `Equal: expected uint(9000), got uint(8000)`.

- [ ] **Step 2: Update `ParseConfig` to bind nested env vars**

In `pkg/server/config/config.go`, update `ParseConfig`:

```go
func ParseConfig(v *viper.Viper) (*Config, error) {
	var result Config

	// Enable environment variable overrides. `GMOUNTIE_SERVER_PORT` →
	// `server.port`, etc. The env key replacer maps `_` to `.` so nested
	// keys can be reached.
	v.SetEnvPrefix(EnvironmentPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Explicitly bind the nested keys we want overridable. Viper's
	// `AutomaticEnv` alone does not propagate through `Sub(...)`, so any
	// key we read from a sub-viper must be bound on the parent.
	for _, key := range []string{
		"server.address",
		"server.port",
		"server.metrics",
		"auth.type",
	} {
		_ = v.BindEnv(key)
	}

	// Parse the server configuration.
	v.SetDefault("server", make(map[string]string))
	result.Server = NewServerConfig(v.Sub("server"))

	// Parse the auth configuration.
	auth, err := NewFromConfig(v.Sub("auth"))
	if err != nil {
		return nil, err
	}
	result.Auth = auth

	// Parse the volume configuration.
	volumes := make([]*VolumeConfig, 0)
	for sub, i := v.Sub("volumes.0"), 0; sub != nil; sub = v.Sub(fmt.Sprintf("volumes.%d", i)) {
		volumes = append(volumes, NewVolumeConfig(sub))
		i++
	}
	result.Volumes = volumes

	// Validate.
	validate := validator.New()
	err = validate.Struct(result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
```

Add `"strings"` to the imports.

- [ ] **Step 3: Update `NewServerConfig` to also read from the parent's bound env**

The change above binds env vars on the parent viper, but `NewServerConfig` reads from `v.Sub("server")`. Sub-vipers know about defaults but not parent env bindings. Two options: (a) pass the parent viper and bound key paths into `NewServerConfig`, or (b) have `NewServerConfig` check viper's parent env explicitly.

Simplest fix that doesn't refactor signatures: change the loop in `ParseConfig` to read `server.*` directly via the parent viper rather than via `v.Sub("server")`. Replace:

```go
	v.SetDefault("server", make(map[string]string))
	result.Server = NewServerConfig(v.Sub("server"))
```

With a direct, parent-aware construction:

```go
	v.SetDefault("server.address", DefaultAddress)
	v.SetDefault("server.port", DefaultPort)
	v.SetDefault("server.metrics", true)
	result.Server = &ServerConfig{
		Address: v.GetString("server.address"),
		Port:    v.GetUint("server.port"),
		Metrics: v.GetBool("server.metrics"),
	}
```

Where `DefaultAddress` and `DefaultPort` come from `pkg/server/config/server.go` (already exported). This bypasses the `Sub` issue entirely.

`NewServerConfig` is still used elsewhere — leave it in place; we're just not using it from `ParseConfig` any more.

- [ ] **Step 4: Run the failing test**

```bash
go test -count=1 -run TestConfigTestSuite/TestParse_EnvVarOverride ./pkg/server/config/...
```

Expected: `PASS`. The port should now be 9000.

- [ ] **Step 5: Remove the `Skip` line permanently**

In `pkg/server/config/config_test.go`, delete:

```go
	s.T().Skip("This test fails") // TODO: Fix this test
```

(In `TestParse_EnvVarOverride`.)

- [ ] **Step 6: Run the full config suite + working-set**

```bash
go test -count=1 ./pkg/server/config/...
```

Expected: every test PASS.

Then run the broader working-set to catch any indirect regressions:

```bash
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

Expected: every package `ok`.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/config/config.go pkg/server/config/config_test.go
git commit -m "fix(config): bind nested env vars; un-skip env-override test"
```

---

## Task 10: Final validation and clean up

**Why:** Make sure the whole plan landed without regressions before moving to Plan 1b.

- [ ] **Step 1: Run the working-set tests one more time**

```bash
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

Expected: every package `ok`.

- [ ] **Step 2: Build everything**

```bash
go build ./cmd/... ./pkg/...
```

Expected: silent.

- [ ] **Step 3: Ask the user to run `task test` and `task lint` from a plain terminal**

The FUSE-mount tests (`pkg/client/mount/...`, `test/e2e/fs/...`) cannot be validated from this environment. The single observable code change to that path is Task 3 (the `log.Fatal` removal in `pkg/client/mount/single.go`), which only changes behaviour on `fuse.NewServer` *failure* — the happy-path tests should be unaffected.

Output to user:

```
Plan 1a complete. Please run from a plain terminal:
    task test
    task lint
to validate the FUSE-mount tests and the lint suite, which I can't run here.
```

- [ ] **Step 4: Verify the commit log**

```bash
git log --oneline -10
```

Expected: nine commits from this plan plus the prior history. No merge or amend commits.

---

## Self-Review Notes (don't modify — for plan reader)

**Spec coverage:** Items 1–3, 8, 9 of roadmap Phase 1 are addressed. Items 4 (context propagation), 5 (sessions), 6 (idempotency tokens), 7 (retry-go on I/O), and the fd-leak-on-failed-Open are deferred to plans 1b/1c/1d.

**Placeholder scan:** None — every step has either exact code or an exact command.

**Type consistency:** `serverStart` becomes `func(context.Context, *config.Config) error` in both the production code (cmd/commands/serve.go) and the test stub (cmd/commands/serve_test.go). `changeUser` signature change in `asume_user.go` is reflected in every caller. `NewFromConfig` returns `(AuthConfig, error)` — confirm by reading the existing return type and adapting.
