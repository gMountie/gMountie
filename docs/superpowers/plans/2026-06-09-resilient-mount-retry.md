# Resilient Mount Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `gmountie mount` survive transient network outages — retry a failed FS op within a configurable window (default 60s) with fresh per-attempt deadlines and gRPC wait-for-ready, instead of aborting the userspace app with `EIO`.

**Architecture:** Replace the fixed "3 attempts sharing one deadline" retry with a wall-clock **window loop** in `pkg/client/io` (each attempt gets its own `timeout_meta`/`timeout_io` deadline, the whole op is bounded by `retry_window` and cancellable by unmount via the client lifetime context). A **session-change guard** keeps the change correct across the server's Resume→Create recovery: idempotent path reads may retry across a new session, but fd-ops and path-mutations stop (their fd is dead / the new session's idempotency cache is empty). Make the server session **grace period configurable** and default it to 60s so transparent resume holds up to the window.

**Tech Stack:** Go (module `go.gmountie.dev/gmountie`, Go 1.26), gRPC, `avast/retry-go` (being replaced by a hand-rolled loop), `hanwen/go-fuse`, Viper config, testify suites.

---

## File Structure

- `pkg/client/config/rpc.go` — add `RetryWindow` knob + default.
- `pkg/client/grpc/client.go` — carry `retryWindow` on `ClientImpl`; `WithRetryWindow` option; `RetryWindow()` getter; add to `Client` interface; expose a client **lifetime** context.
- `pkg/client/grpc/factory.go` — pass `cfg.Rpc.RetryWindow` into the client.
- `pkg/client/io/retry.go` — the windowed `retryOp` + op-class enum (the core; remove the fixed-attempts `retryableCall`).
- `pkg/client/io/backend_grpc.go` — `BackendClient.lifeCtx`; rewire read / mutation / fd-op call sites onto `retryOp`; add `grpc.WaitForReady(true)`.
- `pkg/server/config/server.go` + `config.go` — `SessionConfig.GracePeriod` field + default const + `SetDefault`.
- `pkg/server/service/session.go` — bump `DefaultGracePeriod` 30s→60s (fallback parity).
- `pkg/server/app.go` — wire `cfg.Server.Session.GracePeriod` into `NewSessionManager`.
- Tests alongside each.

---

## Task 1: Client `retry_window` config

**Files:**
- Modify: `pkg/client/config/rpc.go`
- Test: `pkg/client/config/rpc_test.go`

- [ ] **Step 1: Write the failing test**

Add to `rpc_test.go` (testify suite style used in the package):

```go
func (s *RpcConfigSuite) TestRetryWindowDefault() {
	cfg, err := NewRpcConfig(nil)
	s.Require().NoError(err)
	s.Equal(60*time.Second, cfg.RetryWindow)
}

func (s *RpcConfigSuite) TestRetryWindowOverride() {
	v := viper.New()
	v.Set("retry_window", "5m")
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(5*time.Minute, cfg.RetryWindow)
}

func (s *RpcConfigSuite) TestRetryWindowZeroAllowed() {
	v := viper.New()
	v.Set("retry_window", "0s")
	cfg, err := NewRpcConfig(v)
	s.Require().NoError(err)
	s.Equal(time.Duration(0), cfg.RetryWindow)
}
```

(If the package has no suite yet, add a `func TestRpcConfigSuite(t *testing.T){ suite.Run(t, new(RpcConfigSuite)) }` and a `type RpcConfigSuite struct{ suite.Suite }`.)

- [ ] **Step 2: Run, expect FAIL**

Run: `go test ./pkg/client/config/ -run RpcConfigSuite -v`
Expected: FAIL — `cfg.RetryWindow` undefined.

- [ ] **Step 3: Implement**

In `rpc.go`, add the default const next to `DefaultRpcTimeoutIO`:

```go
// DefaultRpcRetryWindow is the total wall-clock budget for retrying a single
// FS operation through transient failures. Within it, attempts retry with a
// fresh per-attempt deadline; past it the last error reaches userspace. 0
// disables retrying (a single attempt — fail fast). Aligned with the server
// session grace period so transparent resume holds for the whole window.
DefaultRpcRetryWindow = 60 * time.Second
```

Add the field to `RpcConfig` (after `TimeoutIO`):

```go
// RetryWindow bounds how long a single FS op retries transient failures
// (Unavailable / DeadlineExceeded) before the error surfaces. 0 = fail fast.
RetryWindow time.Duration `mapstructure:"retry_window" validate:"gte=0"`
```

In `NewRpcConfig`, add to the literal defaults: `RetryWindow: DefaultRpcRetryWindow,` and after the other `SetDefault`s: `v.SetDefault("retry_window", DefaultRpcRetryWindow)`.

- [ ] **Step 4: Run, expect PASS**

Run: `go test ./pkg/client/config/ -run RpcConfigSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/config/rpc.go pkg/client/config/rpc_test.go
git commit -m "feat(client/config): add rpc.retry_window (default 60s)"
```

---

## Task 2: Carry `RetryWindow` + a lifetime context on the client

**Files:**
- Modify: `pkg/client/grpc/client.go`, `pkg/client/grpc/factory.go`
- Test: `pkg/client/grpc/client_test.go`

The `retryOp` loop (Task 4) needs two things from the client: `RetryWindow()` and a **lifetime context** that is cancelled when the client is closed/unmounted (so a long retry aborts on unmount).

- [ ] **Step 1: Write the failing test**

```go
func TestClientRetryWindowAndLifetime(t *testing.T) {
	c, err := NewClient("127.0.0.1:9", WithRetryWindow(42*time.Second))
	require.NoError(t, err)
	require.Equal(t, 42*time.Second, c.(*ClientImpl).RetryWindow())
	require.NotNil(t, c.(*ClientImpl).Lifetime())
	select {
	case <-c.(*ClientImpl).Lifetime().Done():
		t.Fatal("lifetime cancelled before Close")
	default:
	}
}
```

- [ ] **Step 2: Run, expect FAIL**

Run: `go test ./pkg/client/grpc/ -run TestClientRetryWindowAndLifetime -v`
Expected: FAIL — `WithRetryWindow` / `RetryWindow` / `Lifetime` undefined.

- [ ] **Step 3: Implement**

In `client.go`:

Add to the `Client` interface (after `IOTimeout()`):

```go
	// RetryWindow returns the wall-clock budget for retrying a single FS op
	// through transient failures. 0 means fail-fast (single attempt).
	RetryWindow() time.Duration
	// Lifetime returns a context cancelled when the client is closed/unmounted.
	// Long retries derive from it so they abort promptly on teardown.
	Lifetime() context.Context
```

Add fields to `ClientImpl`:

```go
	retryWindow time.Duration
	lifeCtx     context.Context
	lifeCancel  context.CancelFunc
```

Add the option (next to `WithTimeouts`):

```go
// WithRetryWindow sets the per-op transient-retry window on the gRPC Client.
func WithRetryWindow(window time.Duration) ClientOption {
	return func(c *ClientImpl) { c.retryWindow = window }
}
```

In `NewClient`, initialise the lifetime context near the top of the struct
literal setup (before applying options):

```go
	c.lifeCtx, c.lifeCancel = context.WithCancel(context.Background())
```

(Place this right after the `ClientImpl{...}` literal is constructed and before
the `for _, opt := range options` loop; `c` must be addressable — if `NewClient`
currently uses a value `c := ClientImpl{...}`, keep that and set
`c.lifeCtx, c.lifeCancel = context.WithCancel(context.Background())`.)

Add getters and cancel-on-close:

```go
func (c *ClientImpl) RetryWindow() time.Duration { return c.retryWindow }
func (c *ClientImpl) Lifetime() context.Context  { return c.lifeCtx }
```

Find the client's Close/teardown (`grep -n "func (c \*ClientImpl) Close" client.go`)
and add `c.lifeCancel()` at the top of it (guard nil: `if c.lifeCancel != nil { c.lifeCancel() }`).

Default the window in the `NewClient` struct literal so a client built without
the option still has a sane value:

```go
		retryWindow: config.DefaultRpcRetryWindow,
```

(Import `go.gmountie.dev/gmountie/pkg/client/config` if not already; if that
creates an import cycle, instead inline `60 * time.Second` here with a comment
pointing at `config.DefaultRpcRetryWindow`.)

In `factory.go`, where `WithTimeouts(cfg.Rpc.TimeoutMeta, cfg.Rpc.TimeoutIO)` is
appended (line ~55), append alongside it:

```go
		opts = append(opts, WithRetryWindow(cfg.Rpc.RetryWindow))
```

- [ ] **Step 4: Run, expect PASS**

Run: `go test ./pkg/client/grpc/ -run TestClientRetryWindowAndLifetime -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/grpc/client.go pkg/client/grpc/factory.go pkg/client/grpc/client_test.go
git commit -m "feat(client/grpc): carry retry window + lifetime context on the client"
```

---

## Task 3: Server session grace — configurable, default 60s

**Files:**
- Modify: `pkg/server/config/server.go`, `pkg/server/config/config.go`, `pkg/server/service/session.go`, `pkg/server/app.go`
- Test: `pkg/server/config/config_test.go`, `pkg/server/service/session_test.go`

- [ ] **Step 1: Write the failing tests**

In `config_test.go`:

```go
func TestSessionGracePeriodDefault(t *testing.T) {
	cfg, err := config.Load("") // however the suite loads defaults; mirror an existing default test
	require.NoError(t, err)
	require.Equal(t, 60*time.Second, cfg.Server.Session.GracePeriod)
}
```

(Mirror the exact loader call used by the nearest existing default-assertion test
in this file — `grep -n "SubscribeBufferSize\|FrameSizeBytes" config_test.go` to
find the pattern.)

In `session_test.go`, assert the manager honours a configured grace by reaping
after it (use a tiny grace so the test is fast):

```go
func (s *SessionManagerSuite) TestGracePeriodHonored() {
	mgr := NewSessionManager(SessionManagerOptions{GracePeriod: 20 * time.Millisecond})
	id, err := mgr.Create("p", "")
	s.Require().NoError(err)
	mgr.MarkDisconnected(id)
	s.Eventually(func() bool {
		_, err := mgr.Get(id)
		return err != nil // reaped
	}, time.Second, 5*time.Millisecond)
}
```

- [ ] **Step 2: Run, expect FAIL**

Run: `go test ./pkg/server/config/ -run TestSessionGracePeriodDefault -v`
Expected: FAIL — `cfg.Server.Session` undefined.

- [ ] **Step 3: Implement**

In `service/session.go`, bump the fallback default:

```go
const DefaultGracePeriod = 60 * time.Second
```

In `config/server.go`, add the constant (in the `const` block):

```go
	// DefaultSessionGracePeriod is how long the server retains a disconnected
	// client's session — its fd table and idempotency cache — before reaping
	// it, so a brief network drop can Resume the same session. Aligned with the
	// client rpc.retry_window default. Cost: a dropped client's fds and POSIX
	// locks are held for this long before release.
	DefaultSessionGracePeriod = 60 * time.Second
```

Add the struct (near `ServerKeepaliveConfig`):

```go
// SessionConfig controls per-client session retention.
type SessionConfig struct {
	// GracePeriod is how long a disconnected session (fds + idempotency cache)
	// is retained before reaping. Must be >= 1s.
	GracePeriod time.Duration `mapstructure:"grace_period" validate:"gte=1s"`
}
```

Add the field to `ServerConfig` (after `Keepalive`):

```go
	// Session controls per-client session retention (grace period).
	Session SessionConfig `mapstructure:"session"`
```

In `config.go`, add the default alongside the other `server.*` `SetDefault`s:

```go
	v.SetDefault("server.session.grace_period", DefaultSessionGracePeriod)
```

In `app.go:65`, pass it through:

```go
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{
		Metrics:     m,
		GracePeriod: cfg.Server.Session.GracePeriod,
	})
```

- [ ] **Step 4: Run, expect PASS**

Run: `go test ./pkg/server/config/ ./pkg/server/service/ -run 'GracePeriod|SessionManagerSuite' -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/config/ pkg/server/service/session.go pkg/server/app.go
git commit -m "feat(server): configurable session grace_period (default 60s)"
```

---

## Task 4: The windowed retry core (`retryOp`) — logic only, fully unit-tested

**Files:**
- Modify: `pkg/client/io/retry.go`
- Modify: `pkg/client/io/backend_grpc.go` (add `lifeCtx` to `BackendClient`)
- Test: `pkg/client/io/retry_test.go`

This task builds and tests the new core **without** rewiring real call sites yet.

- [ ] **Step 1: Add `lifeCtx` to `BackendClient`**

`grep -n "type BackendClient struct" pkg/client/io/backend_grpc.go` and add a field:

```go
	lifeCtx context.Context // client lifetime; cancels long retries on unmount
```

`grep -n "func NewBackendClient" pkg/client/io/backend_grpc.go`; in the
constructor set `b.lifeCtx = client.Lifetime()` (the `Client` passed in now
exposes `Lifetime()` from Task 2). If the constructor signature only has the
`Client`, read it from there; if a test constructs `BackendClient` directly, set
`lifeCtx: context.Background()` in those tests.

- [ ] **Step 2: Write the failing tests**

Replace `retry_test.go`’s expectations with property tests for `retryOp`. Use a
fake that drives the `Client` surface `retryOp` touches — `SessionID()` and a
controllable error sequence. Define a tiny fake in the test:

```go
type fakeRetryClient struct {
	mu      sync.Mutex
	id      string
	life    context.Context
	window  time.Duration
	meta    time.Duration
}
func (f *fakeRetryClient) SessionID() string        { f.mu.Lock(); defer f.mu.Unlock(); return f.id }
func (f *fakeRetryClient) setID(s string)           { f.mu.Lock(); f.id = s; f.mu.Unlock() }
func (f *fakeRetryClient) RetryWindow() time.Duration { return f.window }
func (f *fakeRetryClient) Lifetime() context.Context  { return f.life }
func (f *fakeRetryClient) MetaTimeout() time.Duration  { return f.meta }
```

Tests (suite `RetryOpSuite`):

```go
// Transient-then-OK: succeeds after K failures, multiple attempts in one window.
func (s *RetryOpSuite) TestRetriesTransientUntilSuccess() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 50 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, status.Error(codes.Unavailable, "down")
		}
		return 7, nil
	})
	s.Require().NoError(err)
	s.GreaterOrEqual(calls, 3)
}

// Permanent error returns immediately, no retry.
func (s *RetryOpSuite) TestPermanentNoRetry() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 50 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.PermissionDenied, "no")
	})
	s.Require().Error(err)
	s.Equal(1, calls)
}

// Window expiry: always transient -> returns after window, >1 attempt.
func (s *RetryOpSuite) TestWindowExpiry() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 150 * time.Millisecond, meta: 20 * time.Millisecond}
	calls := 0
	start := time.Now()
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Greater(calls, 1)
	s.GreaterOrEqual(time.Since(start), 150*time.Millisecond)
}

// window==0 -> exactly one attempt.
func (s *RetryOpSuite) TestZeroWindowSingleAttempt() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 0, meta: 20 * time.Millisecond}
	calls := 0
	_, _ = retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Equal(1, calls)
}

// Session-change guard: a path-mutation stops once SessionID changes.
func (s *RetryOpSuite) TestMutationStopsOnSessionChange() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 20 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "Mkdir", classPathMutation, f.meta, func(ctx context.Context) (int, error) {
		calls++
		f.setID("B") // recovery created a new session mid-op
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Equal(1, calls) // did not retry across the session change
}

// Idempotent read continues across a session change.
func (s *RetryOpSuite) TestReadContinuesOnSessionChange() {
	f := &fakeRetryClient{id: "A", life: context.Background(), window: 2 * time.Second, meta: 20 * time.Millisecond}
	calls := 0
	_, err := retryOp[int](f, context.Background(), "GetAttr", classIdempotentRead, f.meta, func(ctx context.Context) (int, error) {
		calls++
		f.setID("B")
		if calls < 2 {
			return 0, status.Error(codes.Unavailable, "down")
		}
		return 1, nil
	})
	s.Require().NoError(err)
	s.Equal(2, calls)
}

// Lifetime cancel aborts promptly.
func (s *RetryOpSuite) TestLifetimeCancelAborts() {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeRetryClient{id: "A", life: ctx, window: 10 * time.Second, meta: 20 * time.Millisecond}
	go func() { time.Sleep(40 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := retryOp[int](f, context.Background(), "Op", classIdempotentRead, f.meta, func(c context.Context) (int, error) {
		return 0, status.Error(codes.Unavailable, "down")
	})
	s.Require().Error(err)
	s.Less(time.Since(start), 2*time.Second)
}
```

- [ ] **Step 3: Run, expect FAIL**

Run: `go test ./pkg/client/io/ -run RetryOpSuite -v`
Expected: FAIL — `retryOp`, `classIdempotentRead`, etc. undefined.

- [ ] **Step 4: Implement `retryOp` in `retry.go`**

Define the op-class enum and an interface capturing exactly what `retryOp` needs
(so the fake satisfies it and real `*ClientImpl` does too):

```go
// opClass selects the session-change retry policy for an op.
type opClass int

const (
	classIdempotentRead opClass = iota // GetAttr/Access/*XAttr get/OpenDir/Readlink/StatFs — safe across a new session
	classFdOp                          // Read/Write/Flush/Fsync/Release/Allocate/locks — fd dies on a new session
	classPathMutation                  // Mkdir/Rmdir/Rename/Symlink/Link/Unlink/Chmod/Chown/Utimens/SetXAttr/path-Truncate — replay-unsafe on a new session
)

// retryClient is the slice of the gRPC Client that retryOp depends on.
type retryClient interface {
	SessionID() string
	RetryWindow() time.Duration
	Lifetime() context.Context
}

// retryOp runs fn under the transient-retry window. fuseCtx supplies caller
// values; each attempt gets its own perAttempt deadline derived from a
// background base and cancelled when the client lifetime ends (unmount/Close),
// so a spurious FUSE_INTERRUPT can't abort the RPC but unmount can. Transient
// errors (Unavailable/DeadlineExceeded) retry with backoff until the window
// elapses; permanent errors return immediately. On a session-id change
// (Create-fallback recovery), only classIdempotentRead keeps retrying — fd-ops
// and path-mutations stop because their fd is dead / the new idempotency cache
// is empty.
func retryOp[T any](c retryClient, fuseCtx context.Context, op string, class opClass, perAttempt time.Duration, fn func(context.Context) (T, error)) (T, error) {
	var zero T
	window := c.RetryWindow()
	life := c.Lifetime()
	startID := c.SessionID()
	deadline := time.Now().Add(window)
	backoff := retryInitialDelay

	for {
		// Per-attempt ctx: carries fuseCtx values, own deadline, NOT cancelled
		// by the FUSE op ctx (async-preemption fix preserved), but cancelled by
		// the client lifetime.
		attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(fuseCtx), perAttempt)
		stop := context.AfterFunc(life, cancel)
		res, err := fn(attemptCtx)
		stop()
		cancel()

		if err == nil {
			return res, nil
		}
		if !isRetryableGrpcError(err) {
			return zero, err // permanent
		}
		if window <= 0 || life.Err() != nil {
			return zero, err // fail-fast mode, or unmounting
		}
		if c.SessionID() != startID && class != classIdempotentRead {
			return zero, err // session changed: fd dead / replay-unsafe
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return zero, err // window exhausted
		}
		sleep := backoff
		if sleep > remaining {
			sleep = remaining
		}
		select {
		case <-time.After(sleep):
		case <-life.Done():
			return zero, err
		}
		if backoff < retryMaxDelay {
			backoff *= 2
			if backoff > retryMaxDelay {
				backoff = retryMaxDelay
			}
		}
		metrics.OnRetry(op, status.Code(err).String())
	}
}
```

Keep `isRetryableGrpcError`, `retryInitialDelay`, `retryMaxDelay`. **Delete** the
old `retryableCall` and `withTimeout` (replaced) — the compiler will flag the
call sites, which Tasks 5–7 fix. Remove the now-unused `retry-go` import.
`*ClientImpl` already satisfies `retryClient` (Task 2 added the three methods).

- [ ] **Step 5: Run, expect PASS (retry_test only)**

Run: `go test ./pkg/client/io/ -run RetryOpSuite -v`
Expected: PASS. (The package won't fully build yet — call sites still reference
the deleted `retryableCall`; that's fixed in Tasks 5–7. Use `-run RetryOpSuite`
with `go test -run` on the single file is not enough since the package must
compile; if so, temporarily keep `retryableCall` as a thin shim calling `retryOp`
with `classIdempotentRead` + the client's MetaTimeout until Task 7, then delete
it. Prefer the shim to keep the tree compiling between commits.)

**Shim (temporary), add to `retry.go`:**

```go
// Deprecated shim — removed in Task 7 once all call sites use retryOp directly.
func retryableCall[T any](ctx context.Context, op string, fn func(context.Context) (T, error)) (T, error) {
	// Legacy callers pass an already-deadline-bounded ctx; treat as single attempt.
	return fn(ctx)
}
```

- [ ] **Step 6: Commit**

```bash
git add pkg/client/io/retry.go pkg/client/io/retry_test.go pkg/client/io/backend_grpc.go
git commit -m "feat(client/io): windowed retryOp with session-change guard (logic + tests)"
```

---

## Task 5: Rewire idempotent reads onto `retryOp` + WaitForReady

**Files:**
- Modify: `pkg/client/io/backend_grpc.go`
- Test: existing `pkg/client/io` suite (behaviour unchanged on happy path)

**Sites (all use `b.metaCtx(ctx)` + `retryableCall` today, class `classIdempotentRead`):**
`Stat`/GetAttr (line ~148), `GetAttrIfChanged` (~171), `Access`, `GetXAttr`,
`ListXAttr`, `OpenDir`/Readdir, `Readlink`, `StatFs`. Find them:
`grep -n "b.metaCtx(ctx)" pkg/client/io/backend_grpc.go`.

- [ ] **Step 1: Transform each read site (worked example — `Stat`)**

Before:

```go
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "GetAttr", func(ctx context.Context) (*proto.GetAttrReply, error) {
		return b.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
			Volume: b.volume, Caller: callerFromCtx(ctx), Path: path,
		})
	})
```

After:

```go
	res, err := retryOp(b.client, ctx, "GetAttr", classIdempotentRead, b.client.MetaTimeout(),
		func(ctx context.Context) (*proto.GetAttrReply, error) {
			return b.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
				Volume: b.volume, Caller: callerFromCtx(ctx), Path: path,
			}, grpc.WaitForReady(true))
		})
```

Notes: drop the `ctx2, cancel := b.metaCtx(ctx)` / `defer cancel()` lines; pass
`b.client` (it satisfies `retryClient`); add `grpc.WaitForReady(true)` as the
final arg to the inner `Fs().XXX(...)` call. `callerFromCtx(ctx)` still works —
`retryOp` propagates fuseCtx values into the attempt ctx.

Apply the identical transform to every read site listed above (same class, same
`MetaTimeout()`).

- [ ] **Step 2: Build + run the package tests**

Run: `go test ./pkg/client/io/ -v`
Expected: PASS (reads now go through `retryOp`; the shim still covers any
not-yet-migrated mutation/fd sites).

- [ ] **Step 3: Commit**

```bash
git add pkg/client/io/backend_grpc.go
git commit -m "refactor(client/io): route metadata reads through retryOp + WaitForReady"
```

---

## Task 6: Rewire mutations (`mutatePath`) onto `retryOp`

**Files:**
- Modify: `pkg/client/io/backend_grpc.go`

`mutatePath` is the single seam for path mutations; changing it covers all of
`Mkdir`/`Rmdir`/`Rename`/`Symlink`/`Link`/`Unlink`/`Chmod`/`Chown`/`Utimens`/
`SetXAttr`/`RemoveXAttr`/path-`Truncate`.

- [ ] **Step 1: Rewrite `mutatePath` (line ~273)**

```go
func mutatePath[Rep any](
	b *BackendClient,
	ctx context.Context,
	op string,
	fn func(ctx context.Context, requestID string) (Rep, error),
	statusOf func(Rep) int32,
) fuse.Status {
	requestID := uuid.NewString() // outside retryOp: stable across attempts for idempotency
	res, err := retryOp(b.client, ctx, op, classPathMutation, b.client.MetaTimeout(),
		func(ctx context.Context) (Rep, error) {
			return fn(ctx, requestID)
		})
	if err != nil {
		log.Log.Error("error in call: "+op, zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(statusOf(res))
}
```

- [ ] **Step 2: Update every `mutatePath(...)` caller**

`grep -n "mutatePath(ctx," pkg/client/io/backend_grpc.go`. For each, change the
leading args from `mutatePath(ctx, "Op", b.metaCtx,` to `mutatePath(b, ctx, "Op",`
(drop the `b.metaCtx` argument; prepend `b`). Add `grpc.WaitForReady(true)` to
the inner `Fs().XXX(ctx, &proto.XXXRequest{...})` call in each closure (final
arg). Example for `Mkdir`:

```go
	return mutatePath(b, ctx, "Mkdir",
		func(ctx context.Context, requestID string) (*proto.MkdirReply, error) {
			return b.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
				Volume: b.volume, Caller: callerFromCtx(ctx), Path: path, Mode: mode,
				SessionId: b.client.SessionID(), RequestId: requestID,
			}, grpc.WaitForReady(true))
		},
		func(r *proto.MkdirReply) int32 { return r.Status },
	)
```

- [ ] **Step 3: Build + run**

Run: `go test ./pkg/client/io/ -v`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add pkg/client/io/backend_grpc.go
git commit -m "refactor(client/io): route path mutations through retryOp (session-change-safe)"
```

---

## Task 7: Rewire fd-ops onto `retryOp`, remove the shim

**Files:**
- Modify: `pkg/client/io/backend_grpc.go`, `pkg/client/io/retry.go`

**Sites use `ioCtx(...)` / `h.ioTimeout`, class `classFdOp`:** `Read`, `Write`
(non-streaming path), `Flush`, `Fsync`, `Release`, `Allocate`, `GetLk`, `SetLk`,
and any other `ioCtx(`/`retryableCall(` left. Find them:
`grep -n "ioCtx(\|retryableCall(\|withTimeout(" pkg/client/io/backend_grpc.go`.

fd-ops live on the `GrpcFile` handle (`h`), which must reach the client. Confirm
the handle has a `*BackendClient` or `Client` reference
(`grep -n "type GrpcFile struct" pkg/client/io`); if it holds the `Client`
directly, call `retryOp(h.client, ...)`; if it holds `*BackendClient`, use
`retryOp(h.backend.client, ...)`. The per-attempt timeout is `h.ioTimeout`
(or `b.client.IOTimeout()`).

- [ ] **Step 1: Transform each fd-op (worked example — a GetAttr-by-fd / Fsync)**

Before (representative):

```go
	ctx2, cancel := ioCtx(ctx, h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx2, "Fsync", func(ctx context.Context) (*proto.FsyncReply, error) {
		return h.client.File().Fsync(ctx, &proto.FsyncRequest{ /* … */ })
	})
```

After:

```go
	res, err := retryOp(h.client, ctx, "Fsync", classFdOp, h.ioTimeout,
		func(ctx context.Context) (*proto.FsyncReply, error) {
			return h.client.File().Fsync(ctx, &proto.FsyncRequest{ /* … */ }, grpc.WaitForReady(true))
		})
```

For **streaming** Write / readahead paths that already run off `h.lifeCtx` /
`context.Background()` (not the FUSE ctx), keep their base context but route the
retryable unary calls they issue through `retryOp` with `classFdOp`; pass that
base context as the `fuseCtx` arg (it only supplies values + is detached anyway).
Do **not** add `WaitForReady` to the streaming send loop frames — only to unary
calls.

- [ ] **Step 2: Delete the shim**

Remove the temporary `retryableCall` shim and the old `withTimeout`/`metaCtx`/
`ioCtx` helpers if now unused (`grep -n "metaCtx\|ioCtx\|retryableCall\|withTimeout"`
should show no remaining references). Remove dead imports.

- [ ] **Step 3: Build + full package test**

Run: `go test ./pkg/client/io/... -v`
Expected: PASS, package compiles with no `retryableCall` references.

- [ ] **Step 4: Lint**

Run: `task lint`
Expected: clean (fix any unused-import / shadow findings).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/
git commit -m "refactor(client/io): route fd-ops through retryOp; drop legacy retry helpers"
```

---

## Task 8: Integration test — drop under vs over grace (bufconn)

**Files:**
- Test: `pkg/client/io/resilience_integration_test.go` (or `test/e2e/...` if the
  bufconn harness lives there — `grep -rln "bufconn" pkg test`)

- [ ] **Step 1: Write the integration test**

Stand up a real server + in-process client over bufconn (reuse the existing
harness). Drive two scenarios. testify suite.

```go
// Drop shorter than grace: Resume keeps the session; an op straddling the drop
// completes.
func (s *ResilienceSuite) TestResumesWithinGrace() {
	// server grace = 1s; window = 5s; drop the keepalive/conn for ~300ms mid-op.
	// Assert a GetAttr issued during the drop ultimately returns OK and the
	// session id is unchanged (Resume, not Create).
}

// Drop longer than grace: session reaped -> Create. fd-op fails cleanly; a
// path-mutation retried across it is applied at most once.
func (s *ResilienceSuite) TestCleanFailAndNoDoubleApplyPastGrace() {
	// server grace = 200ms; window = 3s; force the session to be reaped (drop
	// longer than grace, or call the manager's MarkDisconnected + wait).
	// 1) A Write on an fd opened before the drop returns an error (EBADF/EIO),
	//    not a hang.
	// 2) A Mkdir("x") whose first attempt applied server-side then lost its
	//    reply across the reap is NOT retried on the new session — assert the
	//    directory exists exactly once and the caller did not get a spurious
	//    EEXIST masking success. (Inject the lost-reply by failing the response
	//    send once via a test interceptor, then forcing reap.)
}
```

Fill in using the package's existing server-construction helpers
(`grep -n "func.*newTestServer\|StartTestServer\|bufconn" pkg/client/io test/e2e`).
Configure the server grace via the new `SessionConfig.GracePeriod`.

- [ ] **Step 2: Run, expect PASS**

Run: `go test ./pkg/client/io/ -run ResilienceSuite -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add pkg/client/io/resilience_integration_test.go
git commit -m "test(client/io): resume-within-grace and clean-fail-past-grace integration"
```

---

## Task 9: Docs + manual netem verification + cloud follow-up

**Files:**
- Modify: client config reference doc (`grep -rln "timeout_meta\|timeout_io" docs website` to find it), server config reference (`grace_period`).
- Modify: `docs/superpowers/specs/2026-06-09-resilient-mount-retry-design.md` (delete per the repo's spec lifecycle once promoted) — see Step 3.

- [ ] **Step 1: Document the new knobs**

In the client RPC config reference, add `rpc.retry_window` (default 60s, `0` =
fail fast, "set high for hard-mount") next to `timeout_meta`/`timeout_io`, noting
the two timeouts are now **per-attempt**. In the server config reference, add
`server.session.grace_period` (default 60s) and note it should be **≥ the client
`retry_window`** for transparent resume to hold, and the held-fds/locks cost.

- [ ] **Step 2: Manual netem verification (record results in the PR, not CI)**

```bash
sudo scripts/start-slow-loopback.sh        # throttle loopback
# mount against a local server, then:
cp big.mkv /mnt/gmountie/                   # the original failing case — expect success now
# repeat with a concurrent bulk writer to exercise contention:
( dd if=/dev/zero of=/mnt/gmountie/bulk bs=1M count=512 & ); cp big.mkv /mnt/gmountie/
sudo scripts/stop-slow-loopback.sh
```

Expected: copies complete (pauses, not `EIO`). If metadata still starves under
the concurrent write, note it in the PR — the composing lever is raising
`timeout_meta` (per-attempt); the window alone won't beat sustained saturation.

- [ ] **Step 3: Promote + delete the spec (repo lifecycle)**

Per the repo's docs discipline, fold any durable content into the client/server
config reference (done in Step 1), then delete the spec and this plan is the
record until merge:

```bash
git rm docs/superpowers/specs/2026-06-09-resilient-mount-retry-design.md
```

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: document retry_window + session grace_period; remove shipped spec"
```

- [ ] **Step 5: Cloud follow-up (separate repo — do NOT do here)**

Open a one-line row in `gMountie-cloud/docs/roadmap.md`: pin the data-plane
server `grace_period` to 60s explicitly (it otherwise inherits the new OSS
default) and confirm the held-fds/locks cost is acceptable on the shared
data-plane. Tracked there, implemented in a cloud PR.

---

## Final verification (before opening the PR)

- [ ] `task test` — full suite green.
- [ ] `task lint` — clean.
- [ ] Manual netem case (Task 9 Step 2) result recorded in the PR body.
- [ ] PR description notes: client + server change in one PR (per decision); server grace default 60s; cloud follow-up row linked.
