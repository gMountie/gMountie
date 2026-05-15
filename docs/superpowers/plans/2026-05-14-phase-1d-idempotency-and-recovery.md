# Phase 1d — Idempotency Tokens & Session Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make mutating RPCs safe to retry by attaching a per-call `request_id`, and let the server short-circuit duplicate requests from a small per-session reply cache. With that in place, extend the client's retry policy to cover mutating ops too. Also: when the Keepalive stream breaks (transient disconnect), the client now actively reattaches via `Session.Resume`, falling back to a fresh `Session.Create` when the server has already reaped the session.

**Architecture:** A new `string request_id` field on the 10 mutating proto requests (`Open`, `Create`, `Write`, `Mkdir`, `Rmdir`, `Rename`, `Unlink`, `Truncate`, `Chmod`, `Chown`) becomes the dedup key. Server `Session` gains a `DoOnce(requestID, fn)` primitive backed by `hashicorp/golang-lru/v2` (256 entries per session) plus `golang.org/x/sync/singleflight` so concurrent retries of the same id collapse to one execution. A generic `withIdempotency[T]` helper in `pkg/server/controller/` wraps every mutating handler. On the client, each mutating call site stamps a fresh `uuid.NewString()` request_id (allocated *outside* the retry closure so retries reuse the same id) and switches from "timeout only" to the same `retryableCall` wrapper the idempotent ops already use. Finally, the client's `SessionHandshake.drainKeepalive` is replaced with a recovery loop: on `stream.Recv` error it calls `Resume(currentID)`, then falls back to `Create()` with capped exponential backoff and reopens the Keepalive stream, exiting only when `Close` cancels the long-lived stream context.

**Tech Stack:** Go 1.26, protobuf v1.36.11, grpc-go v1.81.0, `github.com/avast/retry-go/v4` (already used), `github.com/google/uuid` (already used), `github.com/hashicorp/golang-lru/v2` (new — to be added), `golang.org/x/sync/singleflight` (likely already a transitive dep — `go mod tidy` will promote if needed).

---

## Scope context (read once, then forget)

- This is the fourth and final plan of roadmap Phase 1. Plans 1a (reliability fixes), 1b (timeouts + retry on idempotent ops), and 1c (sessions + fd lifecycle) are merged on `develop`. After 1d, Phase 1's "definition of done" is met except for the *across-server-restart* recovery DoD — that needs durable server state and is properly Phase 2 / future work, not 1d.
- Roadmap items implemented here: **#6 (idempotency tokens)** and the second half of **#7 (retry on mutating ops, now safe because of #6)**. Also closes the deferred client-side `Resume` invocation noted in Plan 1c's final review.
- Across-server-restart fd recovery is **out of scope**: when the server restarts, all session state is lost and the client gets a fresh `session_id` via fallback `Create`; previously-open fds become invalid and Read/Write/etc. return `NotFound`. The kernel will see EIO and the userspace app reopens. That matches NFS-like behaviour and is the right shape until durable session state lands.
- Backwards compatibility is **not** a concern. The proto change is additive in field number (the field is required at the application layer — the server rejects empty `request_id` on mutating RPCs with `InvalidArgument`).
- FUSE-touching tests run on the kubevirt VM in `testing/scratch/` (see `feedback-fuse-test-env` memory). `task -t testing/scratch/Taskfile.yml test` is the green-light command.
- All mocks are regenerated via `task gen:mocks` — never hand-edited.
- Tests must be testify suites (`feedback-test-style-suites`). Commits must have conventional-commit subject + descriptive body, no `Co-Authored-By:` / `Signed-off-by:` / `🤖 Generated with Claude Code` trailers (`feedback-commit-style`).

## File Structure

**Proto / generated:**
- **Modify:** `api/proto/file.proto` — add `string request_id` to `OpenRequest`, `CreateRequest`, `WriteRequest`.
- **Modify:** `api/proto/fs.proto` — add `string request_id` to `MkdirRequest`, `RmdirRequest`, `RenameRequest`, `UnlinkRequest`, `TruncateRequest`, `ChmodRequest`, `ChownRequest`.
- **Regen:** `pkg/proto/file.pb.go`, `pkg/proto/fs.pb.go` via `task gen:grpc`.
- **Regen:** `internal/mocks/...` via `task gen:mocks` — signatures unchanged so the diff should be empty or trivial.

**Server:**
- **Modify:** `pkg/server/service/session.go` — `Session` interface gains `DoOnce(requestID string, fn func() (any, error)) (any, error)`. `sessionImpl` gets an LRU + singleflight.Group.
- **Modify:** `pkg/server/service/session_test.go` — add suite cases for the dedup primitive.
- **Create:** `pkg/server/controller/idempotency.go` — `withIdempotency[T any]` generic helper.
- **Create:** `pkg/server/controller/idempotency_test.go` — unit tests for the helper.
- **Modify:** `pkg/server/controller/file.go` — wrap `Open`, `Create`, `Write` handlers with `withIdempotency`.
- **Modify:** `pkg/server/controller/fs.go` — wrap `Mkdir`, `Rmdir`, `Rename`, `Unlink`, `Truncate`, `Chmod`, `Chown` handlers with `withIdempotency`.
- **Modify:** `pkg/server/controller/file_test.go` — every mutating test now passes a non-empty `RequestId`. Add a regression test for empty `RequestId` → `InvalidArgument` and for duplicate `RequestId` returning the cached reply.
- **Modify:** `pkg/server/controller/fs_test.go` — same pattern for the seven fs mutating ops.

**Client:**
- **Modify:** `pkg/client/io/fs.go` — 8 mutating call sites (`Mkdir`, `Rmdir`, `Rename`, `Open`, `Create`, `Unlink`, `Truncate`, `Chmod`, `Chown` — 9 actually; see Task list) gain `uuid.NewString()` outside the retry closure and a `RequestId` field in the request; switch from raw call to `retryableCall`.
- **Modify:** `pkg/client/io/file.go` — `Write` and `Allocate` get the same treatment. `Release`/`Flush`/`Fsync`/`GetLk`/`SetLk`/`SetLkw` stay timeout-only (Release is naturally idempotent; the others are deliberately out of scope for retry until a real use-case lands).
- **Modify:** `pkg/client/io/fs_test.go` — mock expectations pin `req.RequestId != ""` on the new mutating paths.
- **Modify:** `pkg/client/io/file_test.go` — same for `Write`/`Allocate`.
- **Modify:** `pkg/client/grpc/session.go` — replace `drainKeepalive` with a recovery loop. `SessionID()` may now return different values over time (under mutex). Add `streamCtx` / `streamCancel` fields and per-stream restart logic.
- **Modify:** `pkg/client/grpc/session_test.go` — add cases for `stream-error → Resume succeeds → session id unchanged`, `Resume fails → Create succeeds → session id changes`, `Close interrupts recovery cleanly`, `recovery survives transient Resume errors with backoff`.

**Working-set test command (Claude sandbox / GoLand terminal):**

```
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

**Full validation (kubevirt VM):**

```
task -t testing/scratch/Taskfile.yml test
```

---

## Task 1: Add `request_id` to mutating proto requests; regenerate

**Why:** Every mutating RPC needs a stable dedup key. Add the field first, regen, verify build.

**Files:**
- Modify: `api/proto/file.proto`
- Modify: `api/proto/fs.proto`
- Regen: `pkg/proto/file.pb.go`, `pkg/proto/fs.pb.go`
- Regen: `internal/mocks/pkg/proto/*`

- [ ] **Step 1: Survey current field numbers**

Read both proto files and confirm the highest field number in each of the 10 mutating request messages. Plan 1c already added `session_id` so numbers will be one past the existing top.

- [ ] **Step 2: Append `string request_id = N;` to each mutating request**

For `api/proto/file.proto`, append to `OpenRequest`, `CreateRequest`, `WriteRequest` using the next free field number (you'll find these are 6, 7, and 6 respectively after the Plan 1c additions — verify against the file). Example for `WriteRequest` which currently uses fields 1-5:

```protobuf
message WriteRequest {
  string volume = 1;
  uint64 fd = 2;
  bytes bytes = 3;
  int64 offset = 4;
  string session_id = 5;
  string request_id = 6;
}
```

For `api/proto/fs.proto`, append to `MkdirRequest`, `RmdirRequest`, `RenameRequest`, `UnlinkRequest`, `TruncateRequest`, `ChmodRequest`, `ChownRequest`. Example for `MkdirRequest` which currently uses fields 1-4:

```protobuf
message MkdirRequest {
  string volume = 1;
  Caller caller = 2;
  string path = 3;
  uint32 mode = 4;
  string request_id = 5;
}
```

Apply the same pattern (next free field number, do NOT renumber existing fields) to each of the 10 mutating messages. Reply messages do NOT change.

- [ ] **Step 3: Regenerate stubs and mocks**

Run: `task gen:grpc && task gen:mocks`
Expected: `pkg/proto/file.pb.go` and `pkg/proto/fs.pb.go` are regenerated with `RequestId string` fields on the 10 structs. `internal/mocks/` may rewrite some files but signatures don't change — likely zero diff there.

- [ ] **Step 4: Verify build**

Run: `go build ./...`
Expected: success — the new fields are unused so far.

- [ ] **Step 5: Confirm counts**

Run:
```
grep -c "RequestId string" pkg/proto/file.pb.go
grep -c "RequestId string" pkg/proto/fs.pb.go
```
Expected: file.pb.go → 3 (`Open`, `Create`, `Write`); fs.pb.go → 7 (`Mkdir`, `Rmdir`, `Rename`, `Unlink`, `Truncate`, `Chmod`, `Chown`). Total 10.

- [ ] **Step 6: Commit**

```bash
git add api/proto/file.proto api/proto/fs.proto pkg/proto/ internal/mocks/
git commit -m "$(cat <<'EOF'
feat(proto): add request_id to every mutating file RPC

Open/Create/Write/Mkdir/Rmdir/Rename/Unlink/Truncate/Chmod/Chown each
grow a request_id field for server-side dedup. Field numbers are
appended (not renumbered) so existing wire ordering stays stable. The
server-side cache and client-side stamping land in the next tasks.
EOF
)"
```

---

## Task 2: Server `Session.DoOnce` — per-session idempotency cache

**Why:** The Session is the natural place for the dedup state — it's already keyed by `(session_id)` and reaped together with the session. A bounded LRU prevents unbounded growth; singleflight collapses concurrent retries of the same `request_id`.

**Files:**
- Modify: `pkg/server/service/session.go`
- Modify: `pkg/server/service/session_test.go`
- Modify: `go.mod`, `go.sum` (adds `github.com/hashicorp/golang-lru/v2`)

- [ ] **Step 1: Write the failing test**

Append four new cases to `SessionManagerTestSuite` in `pkg/server/service/session_test.go`. Imports needed: `errors` (stdlib) and `sync` for the concurrency case.

```go
func (s *SessionManagerTestSuite) TestDoOnceCachesSuccessfulReply() {
    id, _ := s.mgr.Create()
    sess, _ := s.mgr.Get(id)

    calls := 0
    fn := func() (any, error) {
        calls++
        return "reply-1", nil
    }

    r1, err := sess.DoOnce("req-A", fn)
    s.Require().NoError(err)
    s.Assert().Equal("reply-1", r1)

    r2, err := sess.DoOnce("req-A", fn)
    s.Require().NoError(err)
    s.Assert().Equal("reply-1", r2)

    s.Assert().Equal(1, calls, "fn must execute only once for the same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceDoesNotCacheErrors() {
    id, _ := s.mgr.Create()
    sess, _ := s.mgr.Get(id)

    calls := 0
    fn := func() (any, error) {
        calls++
        if calls == 1 {
            return nil, errors.New("transient")
        }
        return "reply-ok", nil
    }

    _, err := sess.DoOnce("req-B", fn)
    s.Require().Error(err)

    r, err := sess.DoOnce("req-B", fn)
    s.Require().NoError(err)
    s.Assert().Equal("reply-ok", r)
    s.Assert().Equal(2, calls, "errored fn must re-execute on retry with same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceCollapsesConcurrentDuplicates() {
    id, _ := s.mgr.Create()
    sess, _ := s.mgr.Get(id)

    var mu sync.Mutex
    calls := 0
    block := make(chan struct{})
    fn := func() (any, error) {
        mu.Lock()
        calls++
        mu.Unlock()
        <-block
        return "reply", nil
    }

    type result struct {
        v   any
        err error
    }
    results := make(chan result, 5)
    for i := 0; i < 5; i++ {
        go func() {
            v, err := sess.DoOnce("req-C", fn)
            results <- result{v, err}
        }()
    }

    // Give the goroutines a moment to all enter DoOnce and queue on singleflight.
    time.Sleep(50 * time.Millisecond)
    close(block)

    for i := 0; i < 5; i++ {
        r := <-results
        s.Require().NoError(r.err)
        s.Assert().Equal("reply", r.v)
    }

    mu.Lock()
    defer mu.Unlock()
    s.Assert().Equal(1, calls, "fn must run exactly once even with 5 concurrent callers using the same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceLRUEvictsOldEntries() {
    id, _ := s.mgr.Create()
    sess, _ := s.mgr.Get(id)

    // Saturate the LRU (256 entries) and verify the first one is gone.
    for i := 0; i < 300; i++ {
        reqID := fmt.Sprintf("req-%d", i)
        _, err := sess.DoOnce(reqID, func() (any, error) { return i, nil })
        s.Require().NoError(err)
    }

    // req-0 should be evicted by now; calling DoOnce with it re-executes.
    calls := 0
    _, err := sess.DoOnce("req-0", func() (any, error) {
        calls++
        return 999, nil
    })
    s.Require().NoError(err)
    s.Assert().Equal(1, calls, "evicted request_id must re-execute")
}
```

The suite already imports `context`, `time`, `testing`. Add `errors`, `sync`, `fmt` to the imports.

- [ ] **Step 2: Run, confirm failure**

Run: `go test -v ./pkg/server/service/ -run TestSessionManagerTestSuite`
Expected: build failure — `sess.DoOnce` undefined.

- [ ] **Step 3: Add the LRU dep**

Run: `go get github.com/hashicorp/golang-lru/v2@latest`
Expected: `go.mod` gains a direct require.

- [ ] **Step 4: Extend the `Session` interface and implementation**

In `pkg/server/service/session.go`:

Top-of-file imports add:
```go
    lru "github.com/hashicorp/golang-lru/v2"
    "golang.org/x/sync/singleflight"
```

Extend the `Session` interface:
```go
type Session interface {
    ID() string
    RegisterFile(path string, file nodefs.File) uint64
    GetFile(fd uint64) (*FileEntry, bool)
    ReleaseFile(fd uint64)
    ReleaseAll()
    // DoOnce returns the cached reply for requestID if present; otherwise it
    // calls fn, caches the successful reply, and returns it. Concurrent calls
    // with the same requestID are collapsed via singleflight so fn runs at
    // most once. Errored fns are NOT cached — the next caller re-executes.
    DoOnce(requestID string, fn func() (any, error)) (any, error)
}
```

Add a constant near `DefaultGracePeriod`:
```go
// DefaultIdempotencyCacheSize is the per-session LRU size for dedup. 256 covers
// a comfortable churn window for typical FUSE traffic without bloating memory.
const DefaultIdempotencyCacheSize = 256
```

Extend `sessionImpl` (the unexported struct):
```go
type sessionImpl struct {
    id      string
    fdNum   atomic.Uint64
    files   *xsync.MapOf[uint64, *FileEntry]
    replies *lru.Cache[string, any]
    sf      singleflight.Group
}
```

Update the `sessionImpl` construction inside `Create`:
```go
func (m *sessionManagerImpl) Create() (string, error) {
    id := uuid.NewString()
    replies, err := lru.New[string, any](DefaultIdempotencyCacheSize)
    if err != nil {
        return "", errors.Wrap(err, "create idempotency cache")
    }
    sess := &sessionImpl{
        id:      id,
        files:   xsync.NewMapOf[uint64, *FileEntry](),
        replies: replies,
    }
    m.sessions.Store(id, sess)
    return id, nil
}
```

Add the method on `sessionImpl`:
```go
func (s *sessionImpl) DoOnce(requestID string, fn func() (any, error)) (any, error) {
    if v, ok := s.replies.Get(requestID); ok {
        return v, nil
    }
    v, err, _ := s.sf.Do(requestID, func() (any, error) {
        // Double-check after the singleflight wait — another caller may have
        // completed while we queued.
        if cached, ok := s.replies.Get(requestID); ok {
            return cached, nil
        }
        out, err := fn()
        if err != nil {
            return nil, err
        }
        s.replies.Add(requestID, out)
        return out, nil
    })
    return v, err
}
```

- [ ] **Step 5: Run the tests, watch them pass**

Run: `go test -v ./pkg/server/service/ -run TestSessionManagerTestSuite -race -count=5`
Expected: all 13 cases pass × 5 iterations, no race warnings.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/service/session.go pkg/server/service/session_test.go go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(server/service): per-session idempotency cache via LRU + singleflight

Session.DoOnce caches successful replies keyed by request_id (256-entry
LRU per session). Errors are not cached so retries re-execute on
recoverable failure. Concurrent duplicate requests collapse to a single
fn invocation via singleflight, giving exactly-once semantics for
mutating RPCs even when client retries race in flight.
EOF
)"
```

---

## Task 3: Server `withIdempotency[T]` helper + wire into 10 handlers

**Why:** The controller layer needs a uniform way to apply `Session.DoOnce` to each mutating RPC. A typed generic helper keeps the call sites three lines instead of fifteen.

**Files:**
- Create: `pkg/server/controller/idempotency.go`
- Create: `pkg/server/controller/idempotency_test.go`
- Modify: `pkg/server/controller/file.go` (wrap `Open`, `Create`, `Write`)
- Modify: `pkg/server/controller/fs.go` (wrap `Mkdir`, `Rmdir`, `Rename`, `Unlink`, `Truncate`, `Chmod`, `Chown`)
- Modify: `pkg/server/controller/file_test.go` (mutating tests pass `RequestId`)
- Modify: `pkg/server/controller/fs_test.go` (mutating tests pass `RequestId`)

- [ ] **Step 1: Write the failing test for the helper**

Create `pkg/server/controller/idempotency_test.go`:

```go
package controller

import (
    "context"
    "testing"

    "gmountie/pkg/server/service"

    "github.com/pkg/errors"
    "github.com/stretchr/testify/suite"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

type IdempotencyTestSuite struct {
    suite.Suite
    mgr     service.SessionManager
    session service.Session
}

func (s *IdempotencyTestSuite) SetupTest() {
    s.mgr = service.NewSessionManager(service.SessionManagerOptions{})
    id, err := s.mgr.Create()
    s.Require().NoError(err)
    s.session, err = s.mgr.Get(id)
    s.Require().NoError(err)
}

func (s *IdempotencyTestSuite) TearDownTest() {
    _ = s.mgr.Stop(context.Background())
}

func (s *IdempotencyTestSuite) TestEmptyRequestIDRejected() {
    _, err := withIdempotency(s.session, "", func() (*stubReply, error) {
        return &stubReply{V: 1}, nil
    })
    s.Require().Error(err)
    st, ok := status.FromError(err)
    s.Require().True(ok)
    s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *IdempotencyTestSuite) TestFirstCallExecutesAndCaches() {
    calls := 0
    r1, err := withIdempotency(s.session, "req-1", func() (*stubReply, error) {
        calls++
        return &stubReply{V: 7}, nil
    })
    s.Require().NoError(err)
    s.Assert().Equal(&stubReply{V: 7}, r1)

    r2, err := withIdempotency(s.session, "req-1", func() (*stubReply, error) {
        calls++
        return &stubReply{V: 999}, nil
    })
    s.Require().NoError(err)
    s.Assert().Equal(&stubReply{V: 7}, r2, "second call must return the cached reply")
    s.Assert().Equal(1, calls)
}

func (s *IdempotencyTestSuite) TestErrorNotCached() {
    calls := 0
    fn := func() (*stubReply, error) {
        calls++
        if calls == 1 {
            return nil, errors.New("first fails")
        }
        return &stubReply{V: 42}, nil
    }

    _, err := withIdempotency(s.session, "req-err", fn)
    s.Require().Error(err)

    r, err := withIdempotency(s.session, "req-err", fn)
    s.Require().NoError(err)
    s.Assert().Equal(&stubReply{V: 42}, r)
    s.Assert().Equal(2, calls)
}

type stubReply struct {
    V int
}

func TestIdempotencyTestSuite(t *testing.T) {
    suite.Run(t, new(IdempotencyTestSuite))
}
```

- [ ] **Step 2: Run, confirm failure**

Run: `go test -v ./pkg/server/controller/ -run TestIdempotencyTestSuite`
Expected: build failure — `withIdempotency` undefined.

- [ ] **Step 3: Write `pkg/server/controller/idempotency.go`**

```go
package controller

import (
    "gmountie/pkg/server/service"

    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// withIdempotency wraps do with the session's request_id dedup cache. An
// empty requestID is rejected with InvalidArgument — every mutating RPC
// must carry one. The generic parameter T is the concrete reply type so
// callers get a typed value back instead of any.
func withIdempotency[T any](sess service.Session, requestID string, do func() (T, error)) (T, error) {
    var zero T
    if requestID == "" {
        return zero, status.Error(codes.InvalidArgument, "request_id is required")
    }
    raw, err := sess.DoOnce(requestID, func() (any, error) {
        v, err := do()
        if err != nil {
            return nil, err
        }
        return v, nil
    })
    if err != nil {
        return zero, err
    }
    typed, ok := raw.(T)
    if !ok {
        return zero, status.Error(codes.Internal, "idempotency cache: unexpected reply type")
    }
    return typed, nil
}
```

- [ ] **Step 4: Run the helper tests**

Run: `go test -v ./pkg/server/controller/ -run TestIdempotencyTestSuite`
Expected: all three cases pass.

- [ ] **Step 5: Wire `withIdempotency` into the three file.go mutating handlers**

Modify `pkg/server/controller/file.go`. Replace the body of `Open`:

```go
func (r *RpcFileServerImpl) Open(ctx context.Context, request *proto.OpenRequest) (*proto.OpenReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
    if err != nil {
        return nil, err
    }
    return withIdempotency(sess, request.RequestId, func() (*proto.OpenReply, error) {
        file, s := fs.Open(request.Path, request.Flags, createContext(ctx, request.Caller))
        reply := &proto.OpenReply{Status: int32(s)}
        if s == fuse.OK {
            reply.Fd = sess.RegisterFile(request.Path, file)
        }
        return reply, nil
    })
}
```

Same shape for `Create`:

```go
func (r *RpcFileServerImpl) Create(ctx context.Context, request *proto.CreateRequest) (*proto.CreateReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
    if err != nil {
        return nil, err
    }
    return withIdempotency(sess, request.RequestId, func() (*proto.CreateReply, error) {
        file, s := fs.Create(request.Path, request.Flags, request.Mode, createContext(ctx, request.Caller))
        reply := &proto.CreateReply{Status: int32(s)}
        if s == fuse.OK {
            reply.Fd = sess.RegisterFile(request.Path, file)
        }
        return reply, nil
    })
}
```

And `Write`:

```go
func (r *RpcFileServerImpl) Write(_ context.Context, request *proto.WriteRequest) (*proto.WriteReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    return withIdempotency(sess, request.RequestId, func() (*proto.WriteReply, error) {
        written, s := entry.File.Write(request.Bytes, request.Offset)
        return &proto.WriteReply{Written: written, Status: int32(s)}, nil
    })
}
```

(Other handlers in `file.go` — `Read`, `Fsync`, `Release`, `Flush`, `GetLk`, `SetLk`, `SetLkw`, `Allocate` — are unchanged in this task. `Allocate` is mutating but not covered by 1d's scope per the roadmap; revisit later if needed.)

- [ ] **Step 6: Wire into fs.go (seven handlers)**

Modify `pkg/server/controller/fs.go`. For each of `Mkdir`, `Rmdir`, `Rename`, `Unlink`, `Truncate`, `Chmod`, `Chown`, restructure the handler to:

1. Resolve the session FIRST.
2. Wrap the work in `withIdempotency`.

Read `pkg/server/controller/fs.go` to confirm the current shape (it follows the same `resolveSession → do work → return reply` pattern as the file handlers). Example for `Mkdir`:

```go
func (r *RpcFsServerImpl) Mkdir(ctx context.Context, request *proto.MkdirRequest) (*proto.MkdirReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
    if err != nil {
        return nil, err
    }
    return withIdempotency(sess, request.RequestId, func() (*proto.MkdirReply, error) {
        s := fs.Mkdir(request.Path, request.Mode, createContext(ctx, request.Caller))
        return &proto.MkdirReply{Status: int32(s)}, nil
    })
}
```

If `pkg/server/controller/fs.go` does NOT currently have `resolveSession` (i.e., it doesn't yet route through the SessionManager), add it: lift the `resolveSession` helper from `file.go` into a shared location (a small refactor — move it to `idempotency.go` as a method on `*RpcFsServerImpl` or as a free function that accepts a `service.SessionManager`). Most cleanly: in this task, factor it as:

```go
// In pkg/server/controller/utils.go or similar shared file:
func resolveSession(sessions service.SessionManager, sessionID string) (service.Session, error) {
    if sessionID == "" {
        return nil, status.Error(codes.InvalidArgument, "session_id is required")
    }
    sess, err := sessions.Get(sessionID)
    if err != nil {
        return nil, status.Errorf(codes.NotFound, "session not found: %s", sessionID)
    }
    return sess, nil
}
```

Then `file.go`'s existing receiver method and the new `fs.go` usage both call it. Replace the receiver method in `file.go` with a call to the free function. (Do the split as a small intermediate commit if you want, OR fold it into the same commit as the fs.go changes — the diff is mechanical.)

**Important:** the existing `pkg/server/controller/fs.go` does NOT currently take a `SessionManager`. Plan 1c added session-scoping only to `file.go`. Bring `fs.go` into the same world: extend `NewRpcFsServer` to take a `service.SessionManager`, store it on `RpcFsServerImpl`, update the call site in `pkg/server/app.go`'s `GetGrpcServices`.

- [ ] **Step 7: Update `pkg/server/app.go`**

In `GetGrpcServices`, the existing call `controller.NewGrpcServer(c.VolumeService)` (the fs handler — historically called `GrpcServer`) needs to gain the SessionManager parameter. Read app.go and verify the actual constructor name in use; rename for clarity if needed. The single-line change is appending `c.SessionManager` to the call.

- [ ] **Step 8: Update tests in `pkg/server/controller/file_test.go`**

Read the file to understand the suite layout. For each mutating test (`TestOpen`, `TestCreate`, `TestWrite`, plus the regression tests `TestOpenNonOkDoesNotRegisterFd`/`TestCreateNonOkDoesNotRegisterFd`), add `RequestId: "test-req-X"` (or generate a unique one per test) to the request struct.

Add a new regression test that pins behavior:

```go
func (s *RpcFileServerTestSuite) TestMkdirEmptyRequestIDFails() {
    // Note: this lives in fs_test.go in practice — see Task 6's test file.
    // Here we cover Open as the file.go example.
    mockFs := new(pathfs2.MockFileSystem)
    s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)

    request := &proto.OpenRequest{
        Volume: "testVolume", Path: "/p", Flags: 0,
        Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
        RequestId: "",
    }
    _, err := s.server.Open(context.Background(), request)
    s.Require().Error(err)
    st, ok := status.FromError(err)
    s.Require().True(ok)
    s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *RpcFileServerTestSuite) TestOpenDuplicateRequestIDReturnsCachedReply() {
    mockFs := new(pathfs2.MockFileSystem)
    s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
    // Open should be invoked AT MOST ONCE — we don't set .Times(1) directly
    // because EXPECT().Once() does it for us.
    mockFs.EXPECT().Open("/p", uint32(0), mock.Anything).
        Return(nodefs.NewDefaultFile(), fuse.OK).Once()

    request := &proto.OpenRequest{
        Volume: "testVolume", Path: "/p", Flags: 0,
        Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
        RequestId: "dup-req-1",
    }
    r1, err := s.server.Open(context.Background(), request)
    s.Require().NoError(err)

    r2, err := s.server.Open(context.Background(), request)
    s.Require().NoError(err)
    s.Assert().Equal(r1.Fd, r2.Fd, "duplicate request_id must return the same fd from the cache")
    s.Assert().Equal(r1.Status, r2.Status)
}
```

The `.Once()` on `mockFs.EXPECT().Open` is the load-bearing assertion: if the second call bypasses the cache and reaches the filesystem, the mock will report an unexpected second invocation.

- [ ] **Step 9: Update tests in `pkg/server/controller/fs_test.go`**

Same pattern: add `RequestId` to every mutating-test request, plus two regression tests on (say) `Mkdir`:

```go
func (s *FsServerTestSuite) TestMkdirEmptyRequestIDFails() { /* analogous */ }
func (s *FsServerTestSuite) TestMkdirDuplicateRequestIDReturnsCachedReply() { /* analogous, asserts .Once() on the mock */ }
```

Read `pkg/server/controller/fs_test.go` to find the actual suite name and follow its existing setup. The SetupTest must now build a `SessionManager`, Create a session, and stash the id on the suite struct (mirror the pattern from `file_test.go`).

- [ ] **Step 10: Run all server tests with race**

Run: `go test -race -count=2 ./pkg/server/...`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
git add pkg/server/controller/ pkg/server/app.go
git commit -m "$(cat <<'EOF'
feat(server/controller): wire idempotency cache into mutating handlers

Open/Create/Write in file.go and Mkdir/Rmdir/Rename/Unlink/Truncate/
Chmod/Chown in fs.go now all route through withIdempotency. Empty
request_id is rejected with InvalidArgument; duplicate request_ids
short-circuit to the cached reply. fs.go also gains the session_id
resolution it was missing — RpcFsServer now takes a SessionManager and
its handlers reject empty/unknown session_id symmetrically with the
file controller.
EOF
)"
```

(Note: depending on how invasive the fs.go session-wiring change is, split this into two commits — one for the cross-cutting `resolveSession` extraction and `fs.go` wiring, one for the idempotency layer. Implementer's call.)

---

## Task 4: Client — stamp `request_id` on mutating ops in `fs.go`; switch from raw call to `retryableCall`

**Why:** With the server now dedup'ing and rejecting empty `request_id`, the client side has to produce one per mutating call. Since `request_id` makes the op safe to retry, fold the retry wrapper in too.

**Files:**
- Modify: `pkg/client/io/fs.go` (9 mutating call sites: `Mkdir`, `Rmdir`, `Rename`, `Open`, `Create`, `Unlink`, `Truncate`, `Chmod`, `Chown`)
- Modify: `pkg/client/io/fs_test.go`

- [ ] **Step 1: Add `uuid` import if missing**

Read the top of `pkg/client/io/fs.go`. If `github.com/google/uuid` is already imported, skip. Otherwise add it. The package is already a direct dep (Plan 1c).

- [ ] **Step 2: Update each mutating method**

For each of the 9 mutating methods, the change pattern is identical:

1. Allocate `requestID := uuid.NewString()` **outside** the retry closure so all retry attempts share the same id.
2. Add `RequestId: requestID` to the request struct.
3. Wrap the call in `retryableCall`.

Example transformation for `Mkdir`. Current (Plan 1c shape):

```go
func (fs *LocalFileSystem) Mkdir(name string, mode uint32, fctx *fuse.Context) fuse.Status {
    ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
    defer cancel()
    res, err := fs.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
        Volume:    fs.volume,
        Caller:    createCaller(fctx),
        Path:      name,
        Mode:      mode,
        SessionId: fs.client.SessionID(),
    })
    if err != nil || res == nil {
        log.Log.Error("error in call: MkDir", zap.String("path", name), zap.Error(err))
        return fuse.EIO
    }
    return fuse.Status(res.Status)
}
```

After:

```go
func (fs *LocalFileSystem) Mkdir(name string, mode uint32, fctx *fuse.Context) fuse.Status {
    ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
    defer cancel()
    requestID := uuid.NewString()
    res, err := retryableCall(ctx, "Mkdir", func(ctx context.Context) (*proto.MkdirReply, error) {
        return fs.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
            Volume:    fs.volume,
            Caller:    createCaller(fctx),
            Path:      name,
            Mode:      mode,
            SessionId: fs.client.SessionID(),
            RequestId: requestID,
        })
    })
    if err != nil || res == nil {
        log.Log.Error("error in call: MkDir", zap.String("path", name), zap.Error(err))
        return fuse.EIO
    }
    return fuse.Status(res.Status)
}
```

Replicate this transformation for `Rmdir`, `Rename`, `Open`, `Create`, `Unlink`, `Truncate`, `Chmod`, `Chown`. For each, the existing `withMetaTimeout` + raw RPC call becomes `withMetaTimeout` + `uuid.NewString()` + `retryableCall(...)`.

**Careful with `Open` and `Create`:** these return a `nodefs.File` constructed from `res.Fd` — that construction stays *outside* the `retryableCall`. Only the gRPC call itself goes inside.

- [ ] **Step 3: Update `fs_test.go`**

Two changes:
1. Every test that exercises a mutating path needs the mock to accept `mock.MatchedBy(func(req *proto.MkdirRequest) bool { return req.RequestId != "" })` (or similar) — the easiest is to keep the existing exact-struct matchers but loosen the matcher to `mock.Anything` for the request, OR switch to MatchedBy that asserts `RequestId != "" && req.SessionId == "test-session"`.
2. Add a focused assertion that retry actually happens on `Unavailable`. Example to add as `TestMkdirRetriesOnUnavailable`:

```go
func (s *LocalFileSystemTestSuite) TestMkdirRetriesOnUnavailable() {
    // First call fails with Unavailable; second succeeds.
    s.fsClient.EXPECT().Mkdir(mock.Anything, mock.Anything).
        Return(nil, status.Error(codes.Unavailable, "transient")).Once()
    s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
        return req.RequestId != ""
    })).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil).Once()

    st := s.fs.Mkdir("/d", 0755, fakeFuseContext())
    s.Assert().Equal(fuse.OK, st)
}
```

Adjust `s.fsClient` to the actual field name in the test suite (read `fs_test.go` to confirm). `fakeFuseContext()` should mirror whatever the existing tests use.

Add a second test asserting that the SAME `request_id` is used across both retry attempts:

```go
func (s *LocalFileSystemTestSuite) TestMkdirRetryReusesRequestID() {
    var firstID string
    s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
        firstID = req.RequestId
        return req.RequestId != ""
    })).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

    s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
        return req.RequestId == firstID
    })).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil).Once()

    st := s.fs.Mkdir("/d", 0755, fakeFuseContext())
    s.Assert().Equal(fuse.OK, st)
    s.Assert().NotEmpty(firstID)
}
```

- [ ] **Step 4: Run the io tests**

Run: `go test -race -count=2 ./pkg/client/io/...`
Expected: PASS.

- [ ] **Step 5: Verify counts**

Run:
```
grep -c "RequestId:" pkg/client/io/fs.go
grep -c "retryableCall" pkg/client/io/fs.go
```
Expected: `RequestId:` → 9; `retryableCall` → 14 (5 idempotent ops from Plan 1b + 9 newly-wrapped mutating ops).

- [ ] **Step 6: Commit**

```bash
git add pkg/client/io/fs.go pkg/client/io/fs_test.go
git commit -m "$(cat <<'EOF'
feat(client/io): stamp request_id on fs mutating ops and enable retry

Mkdir/Rmdir/Rename/Open/Create/Unlink/Truncate/Chmod/Chown each
generate a UUID outside the retry closure and pass it through every
attempt, so the server's per-session dedup cache short-circuits any
retry that the network or a stalled server forced us into. All nine
ops now share the same retryableCall wrapper that the idempotent
metadata RPCs use — the request_id is what makes that safe.
EOF
)"
```

---

## Task 5: Client — same for `Write` and `Allocate` in `file.go`

**Why:** Write is the high-traffic mutating op; without retry, any transient `Unavailable` during a 1 GB copy fails the whole operation. Allocate is the only other mutating op in `file.go` and gets the same treatment for consistency.

**Files:**
- Modify: `pkg/client/io/file.go`
- Modify: `pkg/client/io/file_test.go`

- [ ] **Step 1: Update `Write`**

Replace `GrpcFile.Write` in `pkg/client/io/file.go`:

```go
func (f *GrpcFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
    ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
    defer cancel()
    requestID := uuid.NewString()
    res, err := retryableCall(ctx, "Write", func(ctx context.Context) (*proto.WriteReply, error) {
        return f.fileClient.Write(ctx, &proto.WriteRequest{
            Volume:    f.volume,
            Fd:        f.fd,
            Offset:    off,
            Bytes:     data,
            SessionId: f.sessionID,
            RequestId: requestID,
        },
            grpc.UseCompressor(snappy.Name),
        )
    })
    if err != nil || res == nil {
        log.Log.Error("error in call: Write", zap.String("path", f.path), zap.Error(err))
        return 0, fuse.EIO
    }
    return res.Written, fuse.Status(res.Status)
}
```

Add `"github.com/google/uuid"` to the imports if not already there.

- [ ] **Step 2: Update `Allocate`**

Same pattern for `Allocate`. Note `Allocate`'s reply type is `*proto.AllocateReply`:

```go
func (f *GrpcFile) Allocate(off uint64, size uint64, mode uint32) fuse.Status {
    ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
    defer cancel()
    requestID := uuid.NewString()
    res, err := retryableCall(ctx, "Allocate", func(ctx context.Context) (*proto.AllocateReply, error) {
        return f.fileClient.Allocate(ctx, &proto.AllocateRequest{
            Volume:    f.volume,
            Fd:        f.fd,
            Off:       off,
            Size:      size,
            Mode:      mode,
            SessionId: f.sessionID,
            RequestId: requestID,
        })
    })
    if err != nil {
        log.Log.Error("error in call: Allocate", zap.String("path", f.path), zap.Error(err))
        return fuse.EIO
    }
    return fuse.Status(res.Status)
}
```

(Note: `Allocate` doesn't currently get a `request_id` field in Task 1 — verify that's still true. Roadmap doesn't list `Allocate` in item 6, but for completeness it's included here. If the proto in Task 1 didn't get the field, add it there before this task lands.)

**Re-check Task 1 scope:** Looking at the Task 1 file list, `Allocate` is NOT in the 10 mutating messages. Decision: keep `Allocate` *out* of 1d (no request_id, no retry), matching the roadmap. **Remove the `Allocate` change from this task and the test below.** The `Write` change stands alone.

- [ ] **Step 3: Update `file_test.go`**

Read the file to confirm the existing GrpcFile test suite. Add a regression test that retry works on `Write`:

```go
func (s *GrpcFileTestSuite) TestWriteRetriesOnUnavailable() {
    s.fileClient.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).
        Return(nil, status.Error(codes.Unavailable, "transient")).Once()
    s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
        return req.RequestId != "" && req.SessionId == "test-session"
    }), mock.Anything).Return(&proto.WriteReply{Written: 5, Status: 0}, nil).Once()

    f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session")
    n, st := f.Write([]byte("hello"), 0)
    s.Require().Equal(fuse.OK, st)
    s.Assert().Equal(uint32(5), n)
}

func (s *GrpcFileTestSuite) TestWriteRetryReusesRequestID() {
    var firstID string
    s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
        firstID = req.RequestId
        return req.RequestId != ""
    }), mock.Anything).Return(nil, status.Error(codes.Unavailable, "transient")).Once()
    s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
        return req.RequestId == firstID
    }), mock.Anything).Return(&proto.WriteReply{Written: 5, Status: 0}, nil).Once()

    f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session")
    _, _ = f.Write([]byte("hello"), 0)
    s.Assert().NotEmpty(firstID)
}
```

(Also update any existing `TestWrite`-style happy-path test to expect `RequestId != ""` instead of an exact-struct match.)

- [ ] **Step 4: Run**

Run: `go test -race -count=2 ./pkg/client/io/...`
Expected: PASS.

- [ ] **Step 5: Confirm counts**

Run: `grep -c "RequestId:" pkg/client/io/file.go`
Expected: 1 (`Write` only — `Allocate` deferred).

- [ ] **Step 6: Commit**

```bash
git add pkg/client/io/file.go pkg/client/io/file_test.go
git commit -m "$(cat <<'EOF'
feat(client/io): make Write retryable via request_id

GrpcFile.Write generates a UUID outside the retry closure and passes
it through every attempt so the server's per-session dedup cache
short-circuits duplicate work. With this, transient gRPC errors during
a large file copy no longer fail the whole operation — they retry
with exactly-once semantics on the server. Read already retries
(idempotent); Release/Flush/Fsync/locks stay timeout-only by design.
EOF
)"
```

---

## Task 6: Client — `SessionHandshake` recovery loop

**Why:** Today, when the Keepalive stream errors out, the client gives up and `SessionID()` keeps returning a session id the server may have already reaped. The fix: on stream error, the client actively reattaches — first via `Resume(currentID)`, then `Create()` if Resume fails — with capped exponential backoff. The session id can change over time; callers reading `SessionID()` see the current value under the mutex.

**Files:**
- Modify: `pkg/client/grpc/session.go`
- Modify: `pkg/client/grpc/session_test.go`

- [ ] **Step 1: Write the failing test for "stream error → Resume succeeds → same id"**

In `pkg/client/grpc/session_test.go`, append to `SessionHandshakeTestSuite`:

```go
func (s *SessionHandshakeTestSuite) TestKeepaliveStreamErrorTriggersResume() {
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()

    // First stream: emit one Recv that returns an error.
    stream1 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
    stream1.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "transient")).Once()

    // Second stream: block forever until test closes the handshake.
    stream2 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
    block := make(chan struct{})
    stream2.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
        <-block
        return nil, io.EOF
    }).Maybe()

    // After stream1 errors, the handshake calls Resume(abc-123) — succeeds.
    s.sessionClient.EXPECT().Resume(mock.Anything, mock.MatchedBy(func(req *proto.SessionResumeRequest) bool {
        return req.SessionId == "abc-123"
    })).Return(&proto.SessionResumeReply{Resumed: true}, nil).Once()

    // First Keepalive (during Establish) returns stream1; second (during recover) returns stream2.
    s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
        Return(stream1, nil).Once()
    s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
        Return(stream2, nil).Once()

    handshake := NewSessionHandshake(s.sessionClient)
    s.Require().NoError(handshake.Establish(context.Background()))
    s.Require().Equal("abc-123", handshake.SessionID())

    // Wait for the recovery to happen. SessionID should remain "abc-123".
    s.Require().Eventually(func() bool {
        // Indirect signal: Resume + new Keepalive happened.
        // We assert success by checking the EXPECT calls completed — but since
        // testify only verifies on Cleanup, we add an extra observable here.
        return handshake.SessionID() == "abc-123" && handshake.IsRunning()
    }, time.Second, 10*time.Millisecond)

    close(block)
    s.Require().NoError(handshake.Close())
}

func (s *SessionHandshakeTestSuite) TestKeepaliveResumeFailureFallsBackToCreate() {
    // Initial Create returns the first id.
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()
    // First stream errors.
    stream1 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
    stream1.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "transient")).Once()
    // Resume returns Resumed=false (server already reaped).
    s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
        Return(&proto.SessionResumeReply{Resumed: false}, nil).Once()
    // Second Create returns a NEW id.
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(&proto.SessionCreateReply{SessionId: "xyz-789"}, nil).Once()
    // Second stream blocks forever.
    stream2 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
    block := make(chan struct{})
    stream2.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
        <-block
        return nil, io.EOF
    }).Maybe()

    s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
        Return(stream1, nil).Once()
    s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
        return req.SessionId == "xyz-789"
    })).Return(stream2, nil).Once()

    handshake := NewSessionHandshake(s.sessionClient)
    s.Require().NoError(handshake.Establish(context.Background()))
    s.Require().Equal("abc-123", handshake.SessionID())

    s.Require().Eventually(func() bool {
        return handshake.SessionID() == "xyz-789"
    }, time.Second, 10*time.Millisecond, "session id must update after fallback Create")

    close(block)
    s.Require().NoError(handshake.Close())
}

func (s *SessionHandshakeTestSuite) TestCloseInterruptsRecovery() {
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()
    // First stream errors immediately.
    stream1 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
    stream1.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "transient")).Once()
    // Resume always fails — drives the loop into backoff.
    s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
        Return(nil, status.Error(codes.Unavailable, "still down")).Maybe()
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(nil, status.Error(codes.Unavailable, "still down")).Maybe()

    s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
        Return(stream1, nil).Once()

    handshake := NewSessionHandshake(s.sessionClient)
    s.Require().NoError(handshake.Establish(context.Background()))

    // Give recovery a moment to enter its backoff loop.
    time.Sleep(50 * time.Millisecond)

    // Close must return promptly even while the loop is mid-backoff.
    done := make(chan struct{})
    go func() {
        _ = handshake.Close()
        close(done)
    }()
    select {
    case <-done:
    case <-time.After(time.Second):
        s.FailNow("Close did not return promptly under recovery backoff")
    }
}
```

Imports needed at the top of the test file (some may already be present): `io`, `time`, `google.golang.org/grpc/codes`, `google.golang.org/grpc/status`, `gmountie/pkg/proto`, `github.com/stretchr/testify/mock`, `github.com/stretchr/testify/suite`.

- [ ] **Step 2: Run, watch them fail**

Run: `go test -v ./pkg/client/grpc/ -run TestSessionHandshakeTestSuite`
Expected: the three new cases fail because the current `drainKeepalive` doesn't recover.

- [ ] **Step 3: Rewrite `pkg/client/grpc/session.go`**

Replace the file's contents (use the existing version as base — only `Establish`, `drainKeepalive` → renamed `keepaliveLoop`, and `Close` change shape; the rest stays):

```go
package grpc

import (
    "context"
    "io"
    "sync"
    "sync/atomic"
    "time"

    "gmountie/pkg/proto"
    "gmountie/pkg/utils/log"

    "github.com/pkg/errors"
    "go.uber.org/zap"
)

const (
    recoveryInitialBackoff = 200 * time.Millisecond
    recoveryMaxBackoff     = 5 * time.Second
)

// SessionHandshake owns the client-side lifecycle of a server session: it
// calls Create on connect, runs a goroutine that drains the Keepalive
// stream, and — when the stream breaks — reattaches via Resume (or falls
// back to a fresh Create) and reopens the stream. The loop exits only
// when Close cancels the long-lived stream context.
type SessionHandshake struct {
    client    proto.SessionServiceClient
    sessionID string
    running   atomic.Bool

    streamCtx    context.Context
    streamCancel context.CancelFunc
    done         chan struct{}

    mu sync.Mutex
}

func NewSessionHandshake(client proto.SessionServiceClient) *SessionHandshake {
    return &SessionHandshake{client: client}
}

func (h *SessionHandshake) SessionID() string {
    h.mu.Lock()
    defer h.mu.Unlock()
    return h.sessionID
}

func (h *SessionHandshake) IsRunning() bool {
    return h.running.Load()
}

func (h *SessionHandshake) setSessionID(id string) {
    h.mu.Lock()
    defer h.mu.Unlock()
    h.sessionID = id
}

// Establish calls Create and opens the initial Keepalive stream, then
// launches the background recovery loop.
func (h *SessionHandshake) Establish(ctx context.Context) error {
    h.mu.Lock()
    if h.sessionID != "" {
        h.mu.Unlock()
        return nil
    }
    h.mu.Unlock()

    reply, err := h.client.Create(ctx, &proto.SessionCreateRequest{})
    if err != nil {
        return errors.Wrap(err, "session create")
    }
    h.setSessionID(reply.SessionId)

    streamCtx, cancel := context.WithCancel(context.Background())
    stream, err := h.client.Keepalive(streamCtx, &proto.KeepaliveRequest{SessionId: reply.SessionId})
    if err != nil {
        cancel()
        return errors.Wrap(err, "session keepalive open")
    }

    h.streamCtx = streamCtx
    h.streamCancel = cancel
    h.done = make(chan struct{})
    h.running.Store(true)
    go h.keepaliveLoop(stream)
    return nil
}

// keepaliveLoop drains the current stream, and on any non-EOF error tries
// to re-establish the session (Resume → Create fallback). Exits only when
// h.streamCtx is cancelled.
func (h *SessionHandshake) keepaliveLoop(initial proto.SessionService_KeepaliveClient) {
    defer func() {
        h.running.Store(false)
        close(h.done)
    }()

    stream := initial
    for {
        // Drain the current stream until error.
        for {
            if _, err := stream.Recv(); err != nil {
                if h.streamCtx.Err() != nil {
                    return
                }
                if err == io.EOF {
                    log.Log.Info("keepalive stream closed by server; recovering",
                        zap.String("session_id", h.SessionID()))
                } else {
                    log.Log.Warn("keepalive stream errored; recovering",
                        zap.String("session_id", h.SessionID()),
                        zap.Error(err))
                }
                break
            }
        }

        newStream, err := h.recover()
        if err != nil {
            // recover() only returns an error when streamCtx is cancelled.
            return
        }
        stream = newStream
    }
}

// recover attempts to reattach via Resume, falling back to Create, with
// capped exponential backoff. Returns the new Keepalive stream on success,
// or an error if h.streamCtx is cancelled.
func (h *SessionHandshake) recover() (proto.SessionService_KeepaliveClient, error) {
    backoff := recoveryInitialBackoff
    for {
        if h.streamCtx.Err() != nil {
            return nil, h.streamCtx.Err()
        }

        stream, err := h.tryReattach()
        if err == nil {
            return stream, nil
        }
        log.Log.Warn("session recovery attempt failed; backing off",
            zap.Duration("backoff", backoff),
            zap.Error(err))

        select {
        case <-h.streamCtx.Done():
            return nil, h.streamCtx.Err()
        case <-time.After(backoff):
        }
        if backoff < recoveryMaxBackoff {
            backoff *= 2
            if backoff > recoveryMaxBackoff {
                backoff = recoveryMaxBackoff
            }
        }
    }
}

// tryReattach makes ONE attempt at session recovery: Resume first, then a
// fresh Create if Resume returns Resumed=false. Returns the new Keepalive
// stream on success.
func (h *SessionHandshake) tryReattach() (proto.SessionService_KeepaliveClient, error) {
    currentID := h.SessionID()
    if currentID != "" {
        resp, err := h.client.Resume(h.streamCtx, &proto.SessionResumeRequest{SessionId: currentID})
        if err != nil {
            return nil, errors.Wrap(err, "resume")
        }
        if resp.Resumed {
            log.Log.Info("session resumed", zap.String("session_id", currentID))
            return h.client.Keepalive(h.streamCtx, &proto.KeepaliveRequest{SessionId: currentID})
        }
    }
    // Resume said no — fall back to a fresh Create.
    resp, err := h.client.Create(h.streamCtx, &proto.SessionCreateRequest{})
    if err != nil {
        return nil, errors.Wrap(err, "create after resume failure")
    }
    h.setSessionID(resp.SessionId)
    log.Log.Info("session re-created after resume failure (open fds are now invalid)",
        zap.String("old_session_id", currentID),
        zap.String("new_session_id", resp.SessionId))
    return h.client.Keepalive(h.streamCtx, &proto.KeepaliveRequest{SessionId: resp.SessionId})
}

// Close cancels the long-lived stream context and waits for the recovery
// loop to exit. Safe to call when Establish was never called or failed.
func (h *SessionHandshake) Close() error {
    if h.streamCancel != nil {
        h.streamCancel()
    }
    if h.done != nil {
        <-h.done
    }
    return nil
}
```

- [ ] **Step 4: Run the suite**

Run: `go test -v ./pkg/client/grpc/ -run TestSessionHandshakeTestSuite -race -count=5`
Expected: all 6 cases pass (3 from Plan 1c + 3 new), no races.

- [ ] **Step 5: Confirm the existing partial-establish-deadlock fix still holds**

Run: `go test -v ./pkg/client/grpc/ -run TestSessionHandshakeTestSuite/TestEstablishKeepaliveFailureLeavesCloseSafe`
Expected: PASS. The new structure preserves the invariant: `h.streamCtx`/`h.streamCancel`/`h.done` are only assigned after Keepalive succeeds, so Close's nil-guards still short-circuit cleanly on partial Establish failure.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/grpc/session.go pkg/client/grpc/session_test.go
git commit -m "$(cat <<'EOF'
feat(client/grpc): self-healing Keepalive loop with Resume/Create fallback

When stream.Recv returns an error, the handshake now reattaches via
SessionService.Resume; if the server has already reaped the session,
falls back to a fresh Create. Capped exponential backoff (200ms→5s)
keeps the loop polite on persistent outages, and Close cleanly
interrupts both the stream drain and the backoff sleep. SessionID()
may now change over time (under mutex) — callers reading it per-RPC
already see the latest value.
EOF
)"
```

---

## Task 7: Final sweep + opus review

**Why:** Confirm the whole plan is green end-to-end before declaring done.

- [ ] **Step 1: Run the full working-set test command**

Run:
```
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```
Expected: PASS across the board.

- [ ] **Step 2: Run with race**

Run:
```
go test -race -count=2 ./pkg/server/service/... ./pkg/server/controller/... ./pkg/client/grpc/... ./pkg/client/io/...
```
Expected: PASS, no race warnings.

- [ ] **Step 3: Run `go vet ./...`**

Run: `go vet ./...`
Expected: no output.

- [ ] **Step 4: Run the full suite on the kubevirt VM**

Run: `task -t testing/scratch/Taskfile.yml test`
Expected: PASS for every package, including `test/e2e/fs` (the FUSE fio suite).

- [ ] **Step 5: Dispatch an opus final reviewer**

Send the full plan range to an opus-model reviewer asking for the same Critical/Important/Minor categorisation used in Plan 1c's final review. Pay particular attention to:
- Race correctness of the recovery loop under concurrent Close.
- Cache-hit correctness when `request_id`s collide across RPC types (verify `withIdempotency`'s type assertion is safe).
- Whether `Allocate` should also gain `request_id` and retry (currently deferred).
- Whether `SessionHandshake.Establish` idempotency check still holds after the recovery loop's `setSessionID` calls.

- [ ] **Step 6: No commit needed — verification only**

---

## Self-review notes

- **Spec coverage:**
  - Roadmap item #6 (idempotency tokens) → Tasks 1, 2, 3.
  - Roadmap item #7's second half (retry on mutating ops) → Tasks 4, 5.
  - Deferred-from-1c client-side `Resume` invocation → Task 6.
  - Roadmap DoD "duplicate `request_id` returning the cached reply" → `TestOpenDuplicateRequestIDReturnsCachedReply` in Task 3.
- **Type consistency:** `Session.DoOnce` / `withIdempotency[T]` / `RequestId`/`requestID` naming is used identically across Tasks 2-5. The recovery loop's `streamCtx` / `streamCancel` / `setSessionID` / `tryReattach` are used consistently in Task 6.
- **Out of scope explicitly:** `Allocate` retry (no request_id added in Task 1); cross-server-restart fd recovery (needs durable state); retry on `Release`/`Flush`/`Fsync`/`GetLk`/`SetLk`/`SetLkw` (deferred until a real use-case appears).
- **No placeholders:** every step has concrete code; the only "look at the file first" hints (Task 3 Step 6 / Step 9; Task 4 Step 3) are where the existing test layout needs adapting and an exhaustive copy in the plan would just rot.
