# Phase 1c — Sessions & Server FD Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the global, process-wide server fd table with a per-session fd table that is reaped on client disconnect. Every client connection establishes a `SessionID` on connect; every file RPC carries that `session_id`. On disconnect, the session's fds are released after a short grace period (so a fast reconnect can reclaim them). Also fix the existing fd leak where the server registers an fd entry even when `Open`/`Create` returns a non-OK FUSE status.

**Architecture:** A new gRPC service `Session` (`api/proto/session.proto`) exposes `Create` and `Resume` (unary) and `Keepalive` (server-stream). The client calls `Create` on connect, stores the returned `session_id`, opens a long-lived `Keepalive` stream, and adds `session_id` to every file RPC. The server keeps an in-memory `SessionManager` mapping `session_id → *session` (each session owns its own fd table). When a `Keepalive` stream's context cancels, the session enters a grace period; if the client doesn't `Resume` within that window, the fds are released. The `fdNum` counter moves *into* the session, so fds are no longer process-global. The fd-leak fix is a one-line correction inside `Open`/`Create`: only register the fd when status is `fuse.OK`.

**Tech Stack:** Go 1.26, protobuf v1.36.11, grpc-go v1.81.0, `github.com/google/uuid` for session IDs (already a transitive dep via Wails — verify and add explicitly if needed), `github.com/puzpuzpuz/xsync/v3` for concurrent maps (already used).

---

## Scope context (read once, then forget)

- This is the third of four plans implementing roadmap Phase 1. Plans 1a (reliability fixes) and 1b (client timeouts + retry) are merged on `develop`. Plan 1d (idempotency tokens on mutating RPCs, plus retry on those mutations) follows.
- Roadmap items addressed here: **#5 (session concept)** and **#8 (server-side fd lifecycle correctness)**. See `docs/superpowers/specs/2026-05-13-roadmap-reliability-and-performance.md`.
- Backwards compatibility is **not** a concern. The proto change is **not** additive in the on-the-wire sense — `session_id` is added as a required field and the old behavior is removed. The user controls both ends of the wire.
- **Resume vs. recover-after-server-restart:** Plan 1c lands the server-side `Resume` RPC and grace-timer machinery for *client reconnect to a still-running server*, but the client-side caller that *invokes* `Resume` on transient disconnect is deferred to Plan 1d (where it lands alongside the retry-with-idempotency-token work that needs the same disconnect-detection plumbing). Recovering across a server restart (the roadmap DoD "kill server, restart within 10s, client recovers") needs durable server state — that's also Plan 1d / future work.
- FUSE-touching tests (`pkg/client/mount/...`, `test/e2e/fs/...`) cannot run in the Claude sandbox or GoLand integrated terminal — validate via plain terminal or CI.
- `task gen:mocks` is the **only** way to update files under `internal/mocks/` — never hand-edit those files (see `feedback-mocks-no-hand-edit` memory). The repo is now on `mockery v3.7.0` with config in `.mockery.yml` (note: `.yml`, not `.yaml`). After any `task gen:grpc` that adds or changes an exported interface method, follow up with `task gen:mocks`. The v3 testify template auto-registers `t.Cleanup(AssertExpectations)` — so use `.Maybe()` on incidental expectations.
- Tests must be testify suites (see `feedback-test-style-suites` memory).
- Commit messages: conventional-commit subject + descriptive body (see `feedback-commit-style` memory). No `Co-Authored-By:` / `Signed-off-by:` trailers.

## File Structure

Files this plan touches:

**Proto / generated:**
- **Create:** `api/proto/session.proto` — `Session` message, `SessionService` with `Create`/`Resume`/`Keepalive` RPCs.
- **Modify:** `api/proto/file.proto` — add `string session_id` field to every fd-carrying request (`OpenRequest`, `CreateRequest`, `ReadRequest`, `WriteRequest`, `ReleaseRequest`, `FsyncRequest`, `FlushRequest`, `GetLkRequest`, `SetLkRequest`, `SetLkwRequest`, `AllocateRequest`).
- **Regen:** `pkg/proto/session.pb.go`, `pkg/proto/session_grpc.pb.go`, `pkg/proto/file.pb.go`, `pkg/proto/file_grpc.pb.go` via `task gen:grpc`.
- **Hand-edit:** `internal/mocks/pkg/proto/mock_*.go` for any new methods on `RpcFileClient` / new generated interface `SessionServiceClient` / `SessionServiceServer`.

**Server:**
- **Create:** `pkg/server/service/session.go` — `SessionManager` interface + `sessionManagerImpl` (manages session lifecycle, fd reaping, grace timer).
- **Create:** `pkg/server/service/session_test.go` — suite covering create/resume/reap/grace-period.
- **Create:** `pkg/server/controller/session.go` — gRPC handler implementing `SessionService`.
- **Create:** `pkg/server/controller/session_test.go` — suite for the controller.
- **Modify:** `pkg/server/controller/file.go` — drop the global `files`/`fdNum`; resolve fd via `SessionManager.GetSession(session_id).GetFile(fd)`; only register on `fuse.OK`.
- **Modify:** `pkg/server/controller/file_test.go` — adapt setup to provide a `SessionManager`; add a non-OK-Open regression test.
- **Modify:** `pkg/server/app.go` — construct + wire `SessionManager`; register the session controller.

**Client:**
- **Create:** `pkg/client/grpc/session.go` — client-side session handshake (call `Create`, hold `Keepalive` stream, expose `SessionID() string`).
- **Create:** `pkg/client/grpc/session_test.go` — suite using a mock `SessionServiceClient`.
- **Modify:** `pkg/client/grpc/client.go` — add `SessionID() string` to the `Client` interface; integrate session handshake into `NewClient` / `Connect`.
- **Modify:** `pkg/client/grpc/factory.go` — drive the handshake from the factory.
- **Modify:** `pkg/client/grpc/factory_test.go` — assertions still pass.
- **Modify:** `pkg/client/io/fs.go` — pass `c.SessionID()` to `Open`/`Create` requests.
- **Modify:** `pkg/client/io/file.go` — `GrpcFile` stores a `sessionID string` field; pass it on every fd-carrying RPC.
- **Modify:** `pkg/client/io/fs_test.go` / `pkg/client/io/file_test.go` — update mock `Client` expectations.

**Mocks (regenerated via `task gen:mocks`, never hand-edited):**
- `internal/mocks/pkg/client/grpc/mock_Client.go` — picks up `SessionID()` automatically after the interface change.
- `internal/mocks/pkg/proto/mock_SessionServiceClient.go`, `mock_SessionServiceServer.go`, `mock_UnsafeSessionServiceServer.go` — generated from `pkg/proto` after Task 1's `task gen:grpc`.
- Streaming mocks for `SessionService_KeepaliveClient` / `SessionService_KeepaliveServer` — mockery v3 emits these automatically alongside the service mocks.

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

## Task 1: Add `SessionService` proto and regenerate stubs

**Why:** Everything downstream depends on the generated `proto.SessionServiceClient` / `SessionServiceServer` types. Get the wire definition right first, regenerate, and verify the build still compiles (no callers yet — that's later tasks).

**Files:**
- Create: `api/proto/session.proto`
- Regen: `pkg/proto/session.pb.go`, `pkg/proto/session_grpc.pb.go`

- [ ] **Step 1: Write `api/proto/session.proto`**

```protobuf
syntax = "proto3";
package gmountie;
option go_package = "pkg/proto";

// SessionCreateRequest opens a new session on the server. The server allocates
// a session_id (UUID v4) and returns it; the client must pass this id on every
// subsequent file RPC and hold open a Keepalive stream for the session's
// lifetime.
message SessionCreateRequest {
}

message SessionCreateReply {
  string session_id = 1;
}

// SessionResumeRequest re-attaches to a session that the server is still
// tracking. The server returns OK only if the session exists and has not yet
// been reaped past its grace period. On OK, any pending grace-period timer for
// that session is cancelled and the client's previously-opened fds remain
// valid.
message SessionResumeRequest {
  string session_id = 1;
}

message SessionResumeReply {
  bool resumed = 1;
}

// KeepaliveRequest is the single message a client sends to open the
// liveness stream. The server holds the stream open and never writes; when
// the stream context cancels, the server starts the grace timer for that
// session.
message KeepaliveRequest {
  string session_id = 1;
}

// KeepalivePing is the server's heartbeat. It carries no payload — its
// presence on the wire is the heartbeat. Server emits one every ~10s so a
// silently-broken connection surfaces faster than gRPC keepalive alone.
message KeepalivePing {
}

service SessionService {
  rpc Create (SessionCreateRequest) returns (SessionCreateReply);
  rpc Resume (SessionResumeRequest) returns (SessionResumeReply);
  rpc Keepalive (KeepaliveRequest) returns (stream KeepalivePing);
}
```

- [ ] **Step 2: Regenerate stubs**

Run: `task gen:grpc`
Expected: `pkg/proto/session.pb.go` and `pkg/proto/session_grpc.pb.go` are created. `file.pb.go` / `file_grpc.pb.go` are *not* changed by this task (we'll touch `file.proto` in Task 6).

- [ ] **Step 3: Regenerate mocks for the new service**

Run: `task gen:mocks`
Expected: `internal/mocks/pkg/proto/mock_SessionServiceClient.go`, `mock_SessionServiceServer.go`, `mock_UnsafeSessionServiceServer.go` are emitted (and the Keepalive stream types are included). Other existing mocks may be rewritten verbatim — that's fine.

- [ ] **Step 4: Verify build still compiles**

Run: `go build ./...`
Expected: success — no callers yet.

- [ ] **Step 5: Commit**

```bash
git add api/proto/session.proto pkg/proto/ internal/mocks/
git commit -m "$(cat <<'EOF'
feat(proto): add SessionService for per-connection session lifecycle

Introduces Create/Resume/Keepalive RPCs. Create allocates a server-side
session_id; Keepalive is a server-stream the client holds open as a
liveness signal; Resume re-attaches before the grace period expires.
Stubs and mockery v3 mocks are regenerated; consumers are wired in
subsequent tasks.
EOF
)"
```

---

## Task 2: (removed — mocks are regenerated in Task 1 via `task gen:mocks`)

Originally this task hand-wrote mock files. The repo is now on mockery v3.7.0 (`.mockery.yml`, `template: testify`), and per project policy `internal/mocks/` is **never** edited by hand. `task gen:mocks` in Task 1 covers the new SessionService mocks. Task numbering below is preserved for stability of references in commit messages.

---

## Task 3: `SessionManager` service — happy path

**Why:** The server needs a place to hold per-session fd tables and to coordinate creation / resume. Build the service interface with passing tests for the basic create/get/release path before wiring it into the controller in later tasks.

**Files:**
- Create: `pkg/server/service/session.go`
- Create: `pkg/server/service/session_test.go`

- [ ] **Step 1: Write the failing test**

```go
package service

import (
    "context"
    "testing"
    "time"

    "github.com/hanwen/go-fuse/v2/fuse/nodefs"
    "github.com/stretchr/testify/suite"
)

type SessionManagerTestSuite struct {
    suite.Suite
    mgr SessionManager
}

func (s *SessionManagerTestSuite) SetupTest() {
    s.mgr = NewSessionManager(SessionManagerOptions{
        GracePeriod: 100 * time.Millisecond,
    })
}

func (s *SessionManagerTestSuite) TestCreateReturnsUniqueIDs() {
    id1, err := s.mgr.Create()
    s.Require().NoError(err)
    s.Require().NotEmpty(id1)

    id2, err := s.mgr.Create()
    s.Require().NoError(err)
    s.Assert().NotEqual(id1, id2)
}

func (s *SessionManagerTestSuite) TestGetReturnsTheSession() {
    id, err := s.mgr.Create()
    s.Require().NoError(err)

    sess, err := s.mgr.Get(id)
    s.Require().NoError(err)
    s.Assert().Equal(id, sess.ID())
}

func (s *SessionManagerTestSuite) TestGetUnknownSessionErrors() {
    _, err := s.mgr.Get("does-not-exist")
    s.Assert().Error(err)
}

func (s *SessionManagerTestSuite) TestSessionFdTableRegisterAndLookup() {
    id, err := s.mgr.Create()
    s.Require().NoError(err)
    sess, err := s.mgr.Get(id)
    s.Require().NoError(err)

    fd := sess.RegisterFile("/some/path", nodefs.NewDefaultFile())
    s.Assert().NotZero(fd)

    entry, ok := sess.GetFile(fd)
    s.Assert().True(ok)
    s.Assert().Equal("/some/path", entry.Path)
}

func (s *SessionManagerTestSuite) TestSessionReleaseFile() {
    id, err := s.mgr.Create()
    s.Require().NoError(err)
    sess, _ := s.mgr.Get(id)
    fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

    sess.ReleaseFile(fd)
    _, ok := sess.GetFile(fd)
    s.Assert().False(ok)
}

func (s *SessionManagerTestSuite) TearDownTest() {
    if c, ok := s.mgr.(interface{ Stop(context.Context) error }); ok {
        _ = c.Stop(context.Background())
    }
}

func TestSessionManagerTestSuite(t *testing.T) {
    suite.Run(t, new(SessionManagerTestSuite))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v ./pkg/server/service/ -run TestSessionManagerTestSuite`
Expected: build failure — `undefined: SessionManager`, `NewSessionManager`, etc.

- [ ] **Step 3: Write `pkg/server/service/session.go` — minimal implementation**

```go
package service

import (
    "context"
    "sync/atomic"
    "time"

    "github.com/google/uuid"
    "github.com/hanwen/go-fuse/v2/fuse/nodefs"
    "github.com/pkg/errors"
    "github.com/puzpuzpuz/xsync/v3"
)

// FileEntry is a per-session record for an open file.
type FileEntry struct {
    File nodefs.File
    Path string
    Fd   uint64
}

// Session is the per-client view of server state. Each session owns its own
// fd numbering and fd table.
type Session interface {
    ID() string
    RegisterFile(path string, file nodefs.File) uint64
    GetFile(fd uint64) (*FileEntry, bool)
    ReleaseFile(fd uint64)
    // ReleaseAll releases every fd in the session. Called by the manager when
    // the session is reaped.
    ReleaseAll()
}

// SessionManager is the per-server registry of sessions.
type SessionManager interface {
    Create() (string, error)
    Get(id string) (Session, error)
    // Resume cancels a pending reap timer for the given session if there is
    // one. Returns (true, nil) if the session existed and was reattached;
    // (false, nil) if the session is unknown (caller should Create a new one).
    Resume(id string) (bool, error)
    // MarkDisconnected starts the grace-period reap timer for the given
    // session. Idempotent: calling twice without a Resume in between is a
    // no-op.
    MarkDisconnected(id string)
    // Stop cancels any in-flight grace timers and forcibly releases all fds.
    // Called on server shutdown.
    Stop(ctx context.Context) error
}

type SessionManagerOptions struct {
    GracePeriod time.Duration
}

const DefaultGracePeriod = 30 * time.Second

type sessionImpl struct {
    id     string
    fdNum  atomic.Uint64
    files  *xsync.MapOf[uint64, *FileEntry]
}

func (s *sessionImpl) ID() string { return s.id }

func (s *sessionImpl) RegisterFile(path string, file nodefs.File) uint64 {
    fd := s.fdNum.Add(1)
    s.files.Store(fd, &FileEntry{File: file, Path: path, Fd: fd})
    return fd
}

func (s *sessionImpl) GetFile(fd uint64) (*FileEntry, bool) {
    return s.files.Load(fd)
}

func (s *sessionImpl) ReleaseFile(fd uint64) {
    entry, ok := s.files.LoadAndDelete(fd)
    if ok && entry.File != nil {
        entry.File.Release()
    }
}

func (s *sessionImpl) ReleaseAll() {
    s.files.Range(func(fd uint64, entry *FileEntry) bool {
        s.files.Delete(fd)
        if entry.File != nil {
            entry.File.Release()
        }
        return true
    })
}

type pendingReap struct {
    cancel context.CancelFunc
}

type sessionManagerImpl struct {
    sessions *xsync.MapOf[string, *sessionImpl]
    reapers  *xsync.MapOf[string, *pendingReap]
    grace    time.Duration
}

func NewSessionManager(opts SessionManagerOptions) SessionManager {
    grace := opts.GracePeriod
    if grace == 0 {
        grace = DefaultGracePeriod
    }
    return &sessionManagerImpl{
        sessions: xsync.NewMapOf[string, *sessionImpl](),
        reapers:  xsync.NewMapOf[string, *pendingReap](),
        grace:    grace,
    }
}

func (m *sessionManagerImpl) Create() (string, error) {
    id := uuid.NewString()
    sess := &sessionImpl{
        id:    id,
        files: xsync.NewMapOf[uint64, *FileEntry](),
    }
    m.sessions.Store(id, sess)
    return id, nil
}

func (m *sessionManagerImpl) Get(id string) (Session, error) {
    sess, ok := m.sessions.Load(id)
    if !ok {
        return nil, errors.Errorf("session not found: %s", id)
    }
    return sess, nil
}

func (m *sessionManagerImpl) Resume(id string) (bool, error) {
    if _, ok := m.sessions.Load(id); !ok {
        return false, nil
    }
    if reaper, ok := m.reapers.LoadAndDelete(id); ok {
        reaper.cancel()
    }
    return true, nil
}

func (m *sessionManagerImpl) MarkDisconnected(id string) {
    sess, ok := m.sessions.Load(id)
    if !ok {
        return
    }
    if _, exists := m.reapers.Load(id); exists {
        return
    }
    ctx, cancel := context.WithCancel(context.Background())
    m.reapers.Store(id, &pendingReap{cancel: cancel})
    go func() {
        select {
        case <-ctx.Done():
            // Cancelled by Resume.
        case <-time.After(m.grace):
            m.sessions.Delete(sess.id)
            m.reapers.Delete(sess.id)
            sess.ReleaseAll()
        }
    }()
}

func (m *sessionManagerImpl) Stop(_ context.Context) error {
    m.reapers.Range(func(_ string, r *pendingReap) bool {
        r.cancel()
        return true
    })
    m.sessions.Range(func(_ string, sess *sessionImpl) bool {
        sess.ReleaseAll()
        m.sessions.Delete(sess.id)
        return true
    })
    return nil
}
```

- [ ] **Step 4: Verify go.mod has `github.com/google/uuid`**

Run: `go mod tidy && go build ./...`
Expected: success. If `uuid` was a transitive dep only, `go mod tidy` will promote it to a direct require.

- [ ] **Step 5: Run the test, watch it pass**

Run: `go test -v ./pkg/server/service/ -run TestSessionManagerTestSuite`
Expected: PASS for all 5 cases.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/service/session.go pkg/server/service/session_test.go go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(server/service): add SessionManager with per-session fd tables

Sessions own their own fd numbering and an xsync.Map of FileEntry.
SessionManager.Create/Get/Resume/MarkDisconnected/Stop give controllers
a single place to coordinate session lifecycle; the reap goroutine
honours a configurable grace period before releasing fds. Wired into
the controller layer in Task 6.
EOF
)"
```

---

## Task 4: `SessionManager` — grace-period reaping tests

**Why:** The whole point of the grace period is "a fast reconnect doesn't lose the fd table." Lock that behavior in with concrete tests before the controller starts depending on it.

**Files:**
- Modify: `pkg/server/service/session_test.go`

- [ ] **Step 1: Add a failing test for "disconnect → wait past grace → fds gone"**

Append to `SessionManagerTestSuite`:

```go
func (s *SessionManagerTestSuite) TestDisconnectThenGraceExpiryReapsFds() {
    id, err := s.mgr.Create()
    s.Require().NoError(err)
    sess, _ := s.mgr.Get(id)
    fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

    s.mgr.MarkDisconnected(id)

    // Wait well past the 100ms grace period configured in SetupTest.
    s.Require().Eventually(func() bool {
        _, err := s.mgr.Get(id)
        return err != nil
    }, 500*time.Millisecond, 10*time.Millisecond, "session should be reaped after grace period")

    _ = fd // fd entry is now unreachable through Get; that is the assertion.
}

func (s *SessionManagerTestSuite) TestResumeBeforeGraceCancelsReap() {
    id, err := s.mgr.Create()
    s.Require().NoError(err)
    sess, _ := s.mgr.Get(id)
    fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

    s.mgr.MarkDisconnected(id)

    // Resume immediately — well within the 100ms grace period.
    resumed, err := s.mgr.Resume(id)
    s.Require().NoError(err)
    s.Require().True(resumed)

    // Wait long enough that the original timer *would* have fired.
    time.Sleep(200 * time.Millisecond)

    sess2, err := s.mgr.Get(id)
    s.Require().NoError(err)
    entry, ok := sess2.GetFile(fd)
    s.Require().True(ok)
    s.Assert().Equal("/p", entry.Path)
}

func (s *SessionManagerTestSuite) TestResumeUnknownSessionReturnsFalse() {
    resumed, err := s.mgr.Resume("nope")
    s.Require().NoError(err)
    s.Assert().False(resumed)
}

func (s *SessionManagerTestSuite) TestStopReleasesAllFds() {
    id, err := s.mgr.Create()
    s.Require().NoError(err)
    sess, _ := s.mgr.Get(id)
    _ = sess.RegisterFile("/p", nodefs.NewDefaultFile())

    err = s.mgr.Stop(context.Background())
    s.Require().NoError(err)

    _, err = s.mgr.Get(id)
    s.Assert().Error(err)
}
```

- [ ] **Step 2: Run, confirm pass (the implementation from Task 3 already covers this)**

Run: `go test -v ./pkg/server/service/ -run TestSessionManagerTestSuite`
Expected: all 9 cases PASS.

If any fails, fix the implementation in `session.go` — typical issues to watch for:
- `Resume` not cancelling the reap goroutine.
- Goroutine leak when `Stop` is called before any `MarkDisconnected`.
- Double-`MarkDisconnected` accidentally spawning a second reap goroutine — the early-return inside `MarkDisconnected` should prevent this.

- [ ] **Step 3: Commit**

```bash
git add pkg/server/service/session_test.go
git commit -m "$(cat <<'EOF'
test(server/service): cover grace-period reap, resume cancellation, Stop

Adds four cases to SessionManagerTestSuite exercising the lifecycle
edges: disconnect+expire reaps fds, Resume before expiry preserves the
fd table, Resume of an unknown id is a clean false, and Stop releases
every session synchronously.
EOF
)"
```

---

## Task 5: `SessionService` gRPC controller

**Why:** This is the wire-facing shim over `SessionManager`. It needs to translate `Create`/`Resume` into manager calls and run the `Keepalive` stream that triggers `MarkDisconnected` on cancellation.

**Files:**
- Create: `pkg/server/controller/session.go`
- Create: `pkg/server/controller/session_test.go`

- [ ] **Step 1: Write the failing test**

```go
package controller

import (
    "context"
    "testing"
    "time"

    "gmountie/pkg/proto"
    "gmountie/pkg/server/service"

    "github.com/stretchr/testify/suite"
)

type SessionControllerTestSuite struct {
    suite.Suite
    mgr        service.SessionManager
    controller *SessionController
}

func (s *SessionControllerTestSuite) SetupTest() {
    s.mgr = service.NewSessionManager(service.SessionManagerOptions{
        GracePeriod: 100 * time.Millisecond,
    })
    s.controller = NewSessionController(s.mgr)
}

func (s *SessionControllerTestSuite) TearDownTest() {
    _ = s.mgr.Stop(context.Background())
}

func (s *SessionControllerTestSuite) TestCreateReturnsSessionID() {
    reply, err := s.controller.Create(context.Background(), &proto.SessionCreateRequest{})
    s.Require().NoError(err)
    s.Assert().NotEmpty(reply.SessionId)
}

func (s *SessionControllerTestSuite) TestResumeKnownSession() {
    createReply, err := s.controller.Create(context.Background(), &proto.SessionCreateRequest{})
    s.Require().NoError(err)

    s.mgr.MarkDisconnected(createReply.SessionId)

    resumeReply, err := s.controller.Resume(context.Background(),
        &proto.SessionResumeRequest{SessionId: createReply.SessionId})
    s.Require().NoError(err)
    s.Assert().True(resumeReply.Resumed)
}

func (s *SessionControllerTestSuite) TestResumeUnknownSession() {
    reply, err := s.controller.Resume(context.Background(),
        &proto.SessionResumeRequest{SessionId: "ghost"})
    s.Require().NoError(err)
    s.Assert().False(reply.Resumed)
}

func TestSessionControllerTestSuite(t *testing.T) {
    suite.Run(t, new(SessionControllerTestSuite))
}
```

(Keepalive is exercised via integration in Task 9 — the streaming-mock plumbing isn't worth the unit-test setup overhead here.)

- [ ] **Step 2: Run, verify failure**

Run: `go test -v ./pkg/server/controller/ -run TestSessionControllerTestSuite`
Expected: build error — `undefined: NewSessionController`.

- [ ] **Step 3: Write the implementation**

`pkg/server/controller/session.go`:

```go
package controller

import (
    "context"
    "time"

    "gmountie/pkg/proto"
    "gmountie/pkg/server/service"
    "gmountie/pkg/utils/log"

    "go.uber.org/zap"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

// keepalivePingInterval is how often the server emits a heartbeat to clients
// holding open a Keepalive stream. Short enough that a half-broken TCP
// connection surfaces before the next file RPC.
const keepalivePingInterval = 10 * time.Second

type SessionController struct {
    sessions service.SessionManager
    proto.UnimplementedSessionServiceServer
}

var _ proto.SessionServiceServer = (*SessionController)(nil)

func NewSessionController(mgr service.SessionManager) *SessionController {
    return &SessionController{sessions: mgr}
}

func (c *SessionController) Register(server *grpc.Server) {
    proto.RegisterSessionServiceServer(server, c)
}

func (c *SessionController) Create(_ context.Context, _ *proto.SessionCreateRequest) (*proto.SessionCreateReply, error) {
    id, err := c.sessions.Create()
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to create session: %v", err)
    }
    log.Log.Info("session created", zap.String("session_id", id))
    return &proto.SessionCreateReply{SessionId: id}, nil
}

func (c *SessionController) Resume(_ context.Context, req *proto.SessionResumeRequest) (*proto.SessionResumeReply, error) {
    if req.SessionId == "" {
        return nil, status.Error(codes.InvalidArgument, "session_id is required")
    }
    resumed, err := c.sessions.Resume(req.SessionId)
    if err != nil {
        return nil, status.Errorf(codes.Internal, "resume failed: %v", err)
    }
    log.Log.Info("session resume requested",
        zap.String("session_id", req.SessionId),
        zap.Bool("resumed", resumed))
    return &proto.SessionResumeReply{Resumed: resumed}, nil
}

func (c *SessionController) Keepalive(req *proto.KeepaliveRequest, stream proto.SessionService_KeepaliveServer) error {
    if req.SessionId == "" {
        return status.Error(codes.InvalidArgument, "session_id is required")
    }
    if _, err := c.sessions.Get(req.SessionId); err != nil {
        return status.Errorf(codes.NotFound, "unknown session: %s", req.SessionId)
    }

    log.Log.Info("keepalive stream opened", zap.String("session_id", req.SessionId))
    defer func() {
        c.sessions.MarkDisconnected(req.SessionId)
        log.Log.Info("keepalive stream closed; session marked disconnected",
            zap.String("session_id", req.SessionId))
    }()

    ticker := time.NewTicker(keepalivePingInterval)
    defer ticker.Stop()
    for {
        select {
        case <-stream.Context().Done():
            return nil
        case <-ticker.C:
            if err := stream.Send(&proto.KeepalivePing{}); err != nil {
                return err
            }
        }
    }
}
```

- [ ] **Step 4: Run the test, watch it pass**

Run: `go test -v ./pkg/server/controller/ -run TestSessionControllerTestSuite`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/controller/session.go pkg/server/controller/session_test.go
git commit -m "$(cat <<'EOF'
feat(server/controller): wire SessionService gRPC handler

Create/Resume delegate to SessionManager. Keepalive is a server-stream
that emits a heartbeat every 10s and, on cancellation, marks the
session disconnected so the grace timer starts. The controller is
registered with the gRPC server in the next task.
EOF
)"
```

---

## Task 6: Extend file.proto with `session_id`; regenerate

**Why:** Every fd-carrying RPC needs to know which session's fd table to look in. Adding the field is a focused proto change; the server / client wiring follows.

**Files:**
- Modify: `api/proto/file.proto`
- Regen: `pkg/proto/file.pb.go`, `pkg/proto/file_grpc.pb.go`

- [ ] **Step 1: Add `string session_id = N;` to every fd-carrying request**

Modify `api/proto/file.proto`. For each of `OpenRequest`, `CreateRequest`, `ReadRequest`, `WriteRequest`, `ReleaseRequest`, `FsyncRequest`, `FlushRequest`, `GetLkRequest`, `SetLkRequest`, `SetLkwRequest`, `AllocateRequest`, append `session_id` with the next available field number (do not renumber existing fields).

Example, for `OpenRequest` (currently uses fields 1-4):

```protobuf
message OpenRequest {
  string volume = 1;
  Caller caller = 2;
  string path = 3;
  uint32 flags = 4;
  string session_id = 5;
}
```

For `ReadRequest` (currently uses 1-4):

```protobuf
message ReadRequest {
  string volume = 1;
  uint64 fd = 2;
  int64 offset = 3;
  uint32 size = 4;
  string session_id = 5;
}
```

Apply the same pattern (next free field number) to all 11 messages listed above. `ReleaseReply` and other replies don't change.

- [ ] **Step 2: Regenerate**

Run: `task gen:grpc`
Expected: `pkg/proto/file.pb.go` is regenerated with `SessionId` fields.

- [ ] **Step 3: Verify build still compiles**

Run: `go build ./...`
Expected: success — the new fields are unused so far.

- [ ] **Step 4: Commit**

```bash
git add api/proto/file.proto pkg/proto/file.pb.go pkg/proto/file_grpc.pb.go
git commit -m "$(cat <<'EOF'
feat(proto): add session_id to every fd-carrying file RPC

Open/Create/Read/Write/Release/Fsync/Flush/GetLk/SetLk/SetLkw/Allocate
each grow a session_id field. The field is appended (not renumbered) so
existing field numbers stay stable. Used by the server fd-table
lookup in the next task.
EOF
)"
```

---

## Task 7: Re-key the server fd table by session; fix the fd leak

**Why:** Item #8 of the roadmap. Today `RpcFileServerImpl.files` is process-global and `Open`/`Create` register an entry even on non-OK status. Move the fd table into the session, and only register on `fuse.OK`.

**Files:**
- Modify: `pkg/server/controller/file.go`
- Modify: `pkg/server/controller/file_test.go`
- Modify: `pkg/server/app.go`

- [ ] **Step 1: Add a failing test for the leak fix**

In `pkg/server/controller/file_test.go`, extend `RpcFileServerTestSuite` so the suite holds a real `SessionManager` (light enough for unit tests):

Top of file, add to the imports: `"gmountie/pkg/server/service"`, and replace the existing `SetupTest`:

```go
func (s *RpcFileServerTestSuite) SetupTest() {
    s.fsService = new(service.MockVolumeService)
    s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
    sid, err := s.sessionMgr.Create()
    s.Require().NoError(err)
    s.sessionID = sid
    s.server = NewRpcFileServer(s.fsService, s.sessionMgr)
}

func (s *RpcFileServerTestSuite) TearDownTest() {
    _ = s.sessionMgr.Stop(context.Background())
}
```

Add `sessionMgr` and `sessionID` fields to the suite struct.

Update every existing `Open`/`Create`/`Read`/`Write`/`Fsync`/`Release`/`Flush` request to include `SessionId: s.sessionID`. For `addFile`-using tests, replace `s.server.addFile(1, "/test/path", mockFile)` with:

```go
sess, _ := s.sessionMgr.Get(s.sessionID)
fd := sess.RegisterFile("/test/path", mockFile)
// then use fd in the request
```

Now add the new regression test:

```go
func (s *RpcFileServerTestSuite) TestOpenNonOkDoesNotRegisterFd() {
    mockFs := new(pathfs2.MockFileSystem)
    s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
    // Open returns a non-OK status.
    mockFs.EXPECT().Open("/test/path", uint32(0), mock.Anything).
        Return(nil, fuse.ENOENT)

    request := &proto.OpenRequest{
        Volume: "testVolume", Path: "/test/path", Flags: 0,
        Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
    }
    reply, err := s.server.Open(context.Background(), request)
    s.Require().NoError(err)
    s.Require().Equal(int32(fuse.ENOENT), reply.Status)

    // The fd in the reply must NOT be registered on the session.
    sess, err := s.sessionMgr.Get(s.sessionID)
    s.Require().NoError(err)
    _, ok := sess.GetFile(reply.Fd)
    s.Assert().False(ok, "non-OK Open should not have registered an fd")
}

func (s *RpcFileServerTestSuite) TestCreateNonOkDoesNotRegisterFd() {
    mockFs := new(pathfs2.MockFileSystem)
    s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
    mockFs.EXPECT().Create("/p", uint32(0), uint32(0), mock.Anything).
        Return(nil, fuse.EACCES)

    request := &proto.CreateRequest{
        Volume: "testVolume", Path: "/p",
        Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
    }
    reply, err := s.server.Create(context.Background(), request)
    s.Require().NoError(err)
    s.Require().Equal(int32(fuse.EACCES), reply.Status)

    sess, _ := s.sessionMgr.Get(s.sessionID)
    _, ok := sess.GetFile(reply.Fd)
    s.Assert().False(ok)
}

func (s *RpcFileServerTestSuite) TestUnknownSessionReturnsError() {
    request := &proto.ReadRequest{
        Volume: "testVolume", Fd: 1, Size: 1, Offset: 0,
        SessionId: "no-such-session",
    }
    _, err := s.server.Read(context.Background(), request)
    s.Require().Error(err)
    st, ok := status.FromError(err)
    s.Require().True(ok)
    s.Assert().Equal(codes.NotFound, st.Code())
}
```

- [ ] **Step 2: Run, confirm failures**

Run: `go test -v ./pkg/server/controller/ -run TestRpcFileServerTestSuite`
Expected: build failures (NewRpcFileServer signature changed; `SessionId` field), then test failures.

- [ ] **Step 3: Rewrite `pkg/server/controller/file.go`**

Full new file:

```go
package controller

import (
    "context"
    "gmountie/pkg/proto"
    "gmountie/pkg/server/service"

    "github.com/hanwen/go-fuse/v2/fuse"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

type RpcFileServerImpl struct {
    fsService service.VolumeService
    sessions  service.SessionManager
    proto.UnimplementedRpcFileServer
}

var _ proto.RpcFileServer = (*RpcFileServerImpl)(nil)

func NewRpcFileServer(fsService service.VolumeService, sessions service.SessionManager) *RpcFileServerImpl {
    return &RpcFileServerImpl{fsService: fsService, sessions: sessions}
}

func (r *RpcFileServerImpl) Register(server *grpc.Server) {
    proto.RegisterRpcFileServer(server, r)
}

func (r *RpcFileServerImpl) resolveSession(sessionID string) (service.Session, error) {
    if sessionID == "" {
        return nil, status.Error(codes.InvalidArgument, "session_id is required")
    }
    sess, err := r.sessions.Get(sessionID)
    if err != nil {
        return nil, status.Errorf(codes.NotFound, "session not found: %s", sessionID)
    }
    return sess, nil
}

func (r *RpcFileServerImpl) Open(ctx context.Context, request *proto.OpenRequest) (*proto.OpenReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
    if err != nil {
        return nil, err
    }
    file, s := fs.Open(request.Path, request.Flags, createContext(ctx, request.Caller))
    reply := &proto.OpenReply{Status: int32(s)}
    if s == fuse.OK {
        reply.Fd = sess.RegisterFile(request.Path, file)
    }
    return reply, nil
}

func (r *RpcFileServerImpl) Create(ctx context.Context, request *proto.CreateRequest) (*proto.CreateReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
    if err != nil {
        return nil, err
    }
    file, s := fs.Create(request.Path, request.Flags, request.Mode, createContext(ctx, request.Caller))
    reply := &proto.CreateReply{Status: int32(s)}
    if s == fuse.OK {
        reply.Fd = sess.RegisterFile(request.Path, file)
    }
    return reply, nil
}

func (r *RpcFileServerImpl) Read(_ context.Context, request *proto.ReadRequest) (*proto.ReadReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    buf := make([]byte, request.Size)
    n, s := entry.File.Read(buf, request.Offset)
    if s != fuse.OK {
        return &proto.ReadReply{Status: int32(s)}, nil
    }
    buf, s = n.Bytes(buf)
    return &proto.ReadReply{Size: int64(n.Size()), Bytes: buf, Status: int32(s)}, nil
}

func (r *RpcFileServerImpl) Write(_ context.Context, request *proto.WriteRequest) (*proto.WriteReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    written, s := entry.File.Write(request.Bytes, request.Offset)
    return &proto.WriteReply{Written: written, Status: int32(s)}, nil
}

func (r *RpcFileServerImpl) Fsync(_ context.Context, request *proto.FsyncRequest) (*proto.FsyncReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    return &proto.FsyncReply{Status: int32(entry.File.Fsync(int(request.Flags)))}, nil
}

func (r *RpcFileServerImpl) Release(_ context.Context, request *proto.ReleaseRequest) (*proto.ReleaseReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    sess.ReleaseFile(request.Fd)
    return &proto.ReleaseReply{}, nil
}

func (r *RpcFileServerImpl) Flush(_ context.Context, request *proto.FlushRequest) (*proto.FlushReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    return &proto.FlushReply{Status: int32(entry.File.Flush())}, nil
}

func (r *RpcFileServerImpl) GetLk(_ context.Context, request *proto.GetLkRequest) (*proto.GetLkReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
    out := &fuse.FileLock{}
    s := entry.File.GetLk(request.Owner, lock, request.Flags, out)
    return &proto.GetLkReply{
        Lk:     &proto.FileLock{Start: out.Start, End: out.End, Typ: out.Typ, Pid: out.Pid},
        Status: int32(s),
    }, nil
}

func (r *RpcFileServerImpl) SetLk(_ context.Context, request *proto.SetLkRequest) (*proto.SetLkReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
    return &proto.SetLkReply{Status: int32(entry.File.SetLk(request.Owner, lock, request.Flags))}, nil
}

func (r *RpcFileServerImpl) SetLkw(_ context.Context, request *proto.SetLkwRequest) (*proto.SetLkwReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    lock := &fuse.FileLock{Start: request.Lk.Start, End: request.Lk.End, Typ: request.Lk.Typ, Pid: request.Lk.Pid}
    return &proto.SetLkwReply{Status: int32(entry.File.SetLkw(request.Owner, lock, request.Flags))}, nil
}

func (r *RpcFileServerImpl) Allocate(_ context.Context, request *proto.AllocateRequest) (*proto.AllocateReply, error) {
    sess, err := r.resolveSession(request.SessionId)
    if err != nil {
        return nil, err
    }
    entry, ok := sess.GetFile(request.Fd)
    if !ok {
        return nil, status.Errorf(codes.NotFound, "fd %d not found in session", request.Fd)
    }
    return &proto.AllocateReply{Status: int32(entry.File.Allocate(request.Off, request.Size, request.Mode))}, nil
}
```

- [ ] **Step 4: Update `pkg/server/app.go` to construct and inject `SessionManager`**

Modify `AppContext`:

```go
type AppContext struct {
    Config         *config.Config
    VolumeService  service.VolumeService
    AuthService    service.AuthService
    SessionManager service.SessionManager
}

func NewServerAppContext(cfg *config.Config) *AppContext {
    volumeService := service.NewVolumeService(cfg, service.WithMiddleware(getVolumeMiddleware()...))
    authService := service.NewAuthServiceFromConfig(cfg.Auth)
    sessionMgr := service.NewSessionManager(service.SessionManagerOptions{})
    return &AppContext{
        Config:         cfg,
        VolumeService:  volumeService,
        AuthService:    authService,
        SessionManager: sessionMgr,
    }
}

func (c *AppContext) GetGrpcServices() []grpc.ServiceRegistrar {
    return []grpc.ServiceRegistrar{
        controller.NewGrpcServer(c.VolumeService),
        controller.NewRpcFileServer(c.VolumeService, c.SessionManager),
        controller.NewVolumeService(c.VolumeService),
        controller.NewSessionController(c.SessionManager),
    }
}
```

Also extend `Start` so that on shutdown the session manager is stopped (after `Stop(true)` returns, before the function returns nil). Add inside the `case <-stopped:` branch:

```go
case <-stopped:
    if err := appCtx.SessionManager.Stop(context.Background()); err != nil {
        log.Log.Warn("session manager stop returned error", zap.Error(err))
    }
    log.Log.Info("server shut down gracefully")
    return nil
```

(The forced-`Stop(false)` branch can stay as-is — the process is exiting anyway.)

- [ ] **Step 5: Run all affected tests**

Run: `go test -count=1 ./pkg/server/...`
Expected: all PASS.

- [ ] **Step 6: Verify the leak regression**

Run: `go test -v ./pkg/server/controller/ -run TestRpcFileServerTestSuite/TestOpenNonOkDoesNotRegisterFd`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/controller/file.go pkg/server/controller/file_test.go pkg/server/app.go
git commit -m "$(cat <<'EOF'
fix(server/controller): scope fd table per session; only register on OK

Replaces the process-global xsync map of fds with per-session fd tables
owned by the SessionManager. Each fd RPC now resolves the session by id
first; an empty or unknown session_id returns InvalidArgument /
NotFound. Open/Create only register the fd when the underlying FUSE
call returns OK, fixing the leak where non-OK opens left a phantom
entry forever (roadmap item #8). Server shutdown now calls
SessionManager.Stop() to release every remaining fd synchronously.
EOF
)"
```

---

## Task 8: Client-side session handshake

**Why:** The client must establish a session before any file RPC, and it must hold the `Keepalive` stream open for the lifetime of the connection.

**Files:**
- Create: `pkg/client/grpc/session.go`
- Create: `pkg/client/grpc/session_test.go`
- Modify: `pkg/client/grpc/client.go`
- Modify: `internal/mocks/pkg/client/grpc/mock_Client.go`

- [ ] **Step 1: Write the failing test for the handshake helper**

```go
package grpc

import (
    "context"
    "errors"
    "io"
    "testing"
    "time"

    "gmountie/pkg/proto"
    mockProto "gmountie/internal/mocks/pkg/proto"

    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/suite"
)

type SessionHandshakeTestSuite struct {
    suite.Suite
    sessionClient *mockProto.MockSessionServiceClient
}

func (s *SessionHandshakeTestSuite) SetupTest() {
    s.sessionClient = mockProto.NewMockSessionServiceClient(s.T())
}

func (s *SessionHandshakeTestSuite) TestEstablishCallsCreateAndStartsKeepalive() {
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()

    stream := mockProto.NewMockSessionService_KeepaliveClient(s.T())
    // Recv blocks until the test signals; we end it with io.EOF.
    blockCh := make(chan struct{})
    stream.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
        <-blockCh
        return nil, io.EOF
    }).Maybe()
    stream.EXPECT().CloseSend().Return(nil).Maybe()

    s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
        return req.SessionId == "abc-123"
    })).Return(stream, nil).Once()

    handshake := NewSessionHandshake(s.sessionClient)
    err := handshake.Establish(context.Background())
    s.Require().NoError(err)
    s.Assert().Equal("abc-123", handshake.SessionID())

    // Close the handshake — the Recv goroutine unblocks.
    close(blockCh)
    s.Require().NoError(handshake.Close())

    // Give the background goroutine a moment to wind down.
    s.Require().Eventually(func() bool {
        return !handshake.IsRunning()
    }, time.Second, 10*time.Millisecond)
}

func (s *SessionHandshakeTestSuite) TestEstablishReturnsErrorWhenCreateFails() {
    s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
        Return(nil, errors.New("network")).Once()

    handshake := NewSessionHandshake(s.sessionClient)
    err := handshake.Establish(context.Background())
    s.Require().Error(err)
    s.Assert().Empty(handshake.SessionID())
}

func TestSessionHandshakeTestSuite(t *testing.T) {
    suite.Run(t, new(SessionHandshakeTestSuite))
}
```

- [ ] **Step 2: Run, confirm failure (`undefined: NewSessionHandshake`)**

Run: `go test -v ./pkg/client/grpc/ -run TestSessionHandshakeTestSuite`
Expected: build failure.

- [ ] **Step 3: Write `pkg/client/grpc/session.go`**

```go
package grpc

import (
    "context"
    "io"
    "sync"
    "sync/atomic"

    "gmountie/pkg/proto"
    "gmountie/pkg/utils/log"

    "github.com/pkg/errors"
    "go.uber.org/zap"
)

// SessionHandshake owns the client-side lifecycle of a server session: it
// calls Create on connect, then runs a goroutine that drains the Keepalive
// stream until either the server closes it or the local Close() is called.
type SessionHandshake struct {
    client    proto.SessionServiceClient
    sessionID string
    running   atomic.Bool

    cancel context.CancelFunc
    done   chan struct{}

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

// Establish calls Create then starts the Keepalive stream in a goroutine.
// Returns as soon as Create succeeds; the Keepalive goroutine continues in
// the background. Subsequent calls without an intervening Close are a
// no-op (returns nil, leaves existing sessionID intact).
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

    h.mu.Lock()
    h.sessionID = reply.SessionId
    h.mu.Unlock()

    streamCtx, cancel := context.WithCancel(context.Background())
    h.cancel = cancel
    h.done = make(chan struct{})

    stream, err := h.client.Keepalive(streamCtx, &proto.KeepaliveRequest{SessionId: reply.SessionId})
    if err != nil {
        cancel()
        return errors.Wrap(err, "session keepalive open")
    }

    h.running.Store(true)
    go h.drainKeepalive(stream)
    return nil
}

func (h *SessionHandshake) drainKeepalive(stream proto.SessionService_KeepaliveClient) {
    defer func() {
        h.running.Store(false)
        close(h.done)
    }()
    for {
        if _, err := stream.Recv(); err != nil {
            if err != io.EOF {
                log.Log.Warn("session keepalive stream ended",
                    zap.String("session_id", h.SessionID()),
                    zap.Error(err))
            }
            return
        }
    }
}

// Close cancels the keepalive stream and waits for the background goroutine
// to exit.
func (h *SessionHandshake) Close() error {
    if h.cancel != nil {
        h.cancel()
    }
    if h.done != nil {
        <-h.done
    }
    return nil
}
```

- [ ] **Step 4: Run the suite, verify pass**

Run: `go test -v ./pkg/client/grpc/ -run TestSessionHandshakeTestSuite`
Expected: PASS.

- [ ] **Step 5: Wire `SessionHandshake` into `Client`**

Modify `pkg/client/grpc/client.go`:

Add `session proto.SessionServiceClient` and `handshake *SessionHandshake` to `ClientImpl`. Construct in `NewClient`:

```go
c.session = proto.NewSessionServiceClient(conn)
c.handshake = NewSessionHandshake(c.session)
```

Extend the `Client` interface:

```go
type Client interface {
    // ... existing methods ...
    SessionID() string
}
```

Add method:

```go
func (c *ClientImpl) SessionID() string {
    return c.handshake.SessionID()
}
```

Add `Session() proto.SessionServiceClient` for symmetry — *only if* a caller needs it. (Plan default: don't add. Keep the surface tight.)

Modify `Connect()` so it triggers the handshake:

```go
func (c *ClientImpl) Connect() {
    c.conn.Connect()
    if err := c.handshake.Establish(context.Background()); err != nil {
        log.Log.Error("session handshake failed", zap.Error(err))
    }
}
```

(Top-of-file imports: add `"context"` and `"gmountie/pkg/utils/log"` and `"go.uber.org/zap"`.)

Update `Close()`:

```go
func (c *ClientImpl) Close() error {
    _ = c.handshake.Close()
    return c.conn.Close()
}
```

- [ ] **Step 6: Regenerate the `Client` mock**

Run: `task gen:mocks`
Expected: `internal/mocks/pkg/client/grpc/mock_Client.go` is rewritten and now has the `SessionID()` method (plus Expecter entry). Do not hand-edit.

- [ ] **Step 7: Verify build + tests**

Run: `go build ./... && go test -count=1 ./pkg/client/grpc/...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/client/grpc/session.go pkg/client/grpc/session_test.go pkg/client/grpc/client.go internal/mocks/pkg/client/grpc/mock_Client.go
git commit -m "$(cat <<'EOF'
feat(client/grpc): session handshake on connect, keepalive in background

Client.Connect() now calls SessionService.Create to obtain a session id
and opens a long-running Keepalive stream in a goroutine. Close()
tears down the stream cleanly. SessionID() is exposed on the Client
interface so callers in pkg/client/io can stamp every file RPC with
the right session id (next task).
EOF
)"
```

---

## Task 9: Pass `session_id` on every fd-carrying client RPC

**Why:** Without this, every file RPC will fail with `InvalidArgument` from the server's new `resolveSession` check.

**Files:**
- Modify: `pkg/client/io/fs.go`
- Modify: `pkg/client/io/file.go`
- Modify: `pkg/client/io/fs_test.go`
- Modify: `pkg/client/io/file_test.go`

- [ ] **Step 1: Pass session_id from `Open`/`Create` in `fs.go`**

In `pkg/client/io/fs.go`, the `Open` method:

```go
res, err := fs.client.File().Open(ctx, &proto.OpenRequest{
    Volume:    fs.volume,
    Caller:    createCaller(fctx),
    Path:      name,
    Flags:     flags,
    SessionId: fs.client.SessionID(),
})
```

Same for `Create`:

```go
res, err := fs.client.File().Create(ctx, &proto.CreateRequest{
    Volume:    fs.volume,
    Caller:    createCaller(fctx),
    Path:      name,
    Flags:     flags,
    Mode:      mode,
    SessionId: fs.client.SessionID(),
})
```

After `NewGrpcFile(...)`, pass the session id into the file constructor (signature change):

```go
return NewGrpcFile(fs.client.File(), fs.volume, name, res.Fd, fs.client.IOTimeout(), fs.client.SessionID()), fuse.Status(res.Status)
```

- [ ] **Step 2: Extend `GrpcFile` to carry `sessionID` and stamp every RPC**

In `pkg/client/io/file.go`:

```go
type GrpcFile struct {
    fileClient proto.RpcFileClient
    path       string
    volume     string
    fd         uint64
    ioTimeout  time.Duration
    sessionID  string
    nodefs.File
}

func NewGrpcFile(fileClient proto.RpcFileClient, volume, path string, fd uint64, ioTimeout time.Duration, sessionID string) *GrpcFile {
    return &GrpcFile{
        fileClient: fileClient,
        path:       path,
        volume:     volume,
        fd:         fd,
        ioTimeout:  ioTimeout,
        sessionID:  sessionID,
        File:       nodefs.NewDefaultFile(),
    }
}
```

For every RPC in `file.go` (`Read`, `Write`, `Release`, `Flush`, `Fsync`, `GetLk`, `SetLk`, `SetLkw`, `Allocate`), add `SessionId: f.sessionID,` to the request struct. Example for `Read`:

```go
return f.fileClient.Read(ctx, &proto.ReadRequest{
    Volume:    f.volume,
    Fd:        f.fd,
    Offset:    off,
    Size:      uint32(len(dest)),
    SessionId: f.sessionID,
},
    grpc.UseCompressor(snappy.Name),
)
```

- [ ] **Step 3: Update tests for the new `Client.SessionID()` expectation**

In `pkg/client/io/fs_test.go` and `pkg/client/io/file_test.go`, add to the suite `SetupTest()`:

```go
s.client.EXPECT().SessionID().Return("test-session").Maybe()
```

Also update the `pkg/client/mount/single_test.go` and `pkg/client/mount/vfs_test.go` suites the same way (`.Maybe()` only — the FUSE kernel may or may not pump RPCs through during mount setup).

- [ ] **Step 4: Add a focused assertion that `session_id` actually reaches the RPC**

In `pkg/client/io/file_test.go`, add a test that pins the field on a `Read` call:

```go
func (s *GrpcFileTestSuite) TestReadStampsSessionID() {
    s.fileClient.EXPECT().Read(mock.Anything, mock.MatchedBy(func(req *proto.ReadRequest) bool {
        return req.SessionId == "test-session"
    }), mock.Anything).Return(&proto.ReadReply{Status: 0}, nil).Once()

    f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session")
    buf := make([]byte, 4)
    _, st := f.Read(buf, 0)
    s.Assert().Equal(fuse.OK, st)
}
```

(If `GrpcFileTestSuite` doesn't exist yet, create a minimal one following the pattern from `RetryTestSuite`.)

Similarly in `fs_test.go`, add a test that `Open` stamps the session id.

- [ ] **Step 5: Run the working-set test command**

Run:
```
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/io/ pkg/client/mount/
git commit -m "$(cat <<'EOF'
feat(client/io): stamp session_id on every fd-carrying RPC

Open/Create copy the client's session id into the request, and
GrpcFile carries it forward so Read/Write/Release/Flush/Fsync/GetLk/
SetLk/SetLkw/Allocate all reach the server with the right session_id.
Mount suites get a Maybe() expectation for the new SessionID() mock
method so the FUSE kernel's churn of incidental calls doesn't trip
assertions.
EOF
)"
```

---

## Task 10: End-to-end session test using the in-process server

**Why:** The unit tests verify the pieces; an integration test verifies the wire round-trip. The `test/e2e/api/` suite already spins up a real server + client, so reuse that machinery.

**Files:**
- Create: `test/e2e/api/session_test.go`

- [ ] **Step 1: Survey existing e2e tests**

Read `test/e2e/api/` to find the existing harness (look for whatever currently constructs a real `pkg/server` and a `pkg/client/grpc` against it). Reuse the same setup pattern — do not invent a new harness.

- [ ] **Step 2: Write the failing test**

```go
package api

import (
    "context"
    "testing"

    "gmountie/pkg/proto"

    "github.com/stretchr/testify/suite"
)

type SessionE2ETestSuite struct {
    suite.Suite
    // Embed or use whatever the existing harness in this package exposes.
    // Replace `harness` with the existing fixture type.
    harness *E2EHarness
}

func (s *SessionE2ETestSuite) SetupTest() {
    s.harness = NewE2EHarness(s.T())
}

func (s *SessionE2ETestSuite) TearDownTest() {
    s.harness.Stop()
}

func (s *SessionE2ETestSuite) TestSessionCreateReturnsID() {
    sessionClient := proto.NewSessionServiceClient(s.harness.ClientConn())
    reply, err := sessionClient.Create(context.Background(), &proto.SessionCreateRequest{})
    s.Require().NoError(err)
    s.Assert().NotEmpty(reply.SessionId)
}

func (s *SessionE2ETestSuite) TestOpenWithoutSessionIDFails() {
    fileClient := proto.NewRpcFileClient(s.harness.ClientConn())
    _, err := fileClient.Open(context.Background(), &proto.OpenRequest{
        Volume: s.harness.VolumeName(), Path: "/", Flags: 0,
        Caller: &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}, Pid: 0},
        // SessionId intentionally empty.
    })
    s.Require().Error(err)
}

func (s *SessionE2ETestSuite) TestOpenWithSessionIDSucceeds() {
    sessionClient := proto.NewSessionServiceClient(s.harness.ClientConn())
    sess, err := sessionClient.Create(context.Background(), &proto.SessionCreateRequest{})
    s.Require().NoError(err)

    fileClient := proto.NewRpcFileClient(s.harness.ClientConn())
    reply, err := fileClient.Open(context.Background(), &proto.OpenRequest{
        Volume: s.harness.VolumeName(), Path: "/somefile",
        Flags: 0,
        Caller: &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}, Pid: 0},
        SessionId: sess.SessionId,
    })
    s.Require().NoError(err)
    // We don't care whether the FUSE status is OK here — only that the
    // session-resolution layer didn't reject it.
    _ = reply
}

func TestSessionE2ETestSuite(t *testing.T) {
    suite.Run(t, new(SessionE2ETestSuite))
}
```

If the harness type or method names differ, adjust to match what's actually in `test/e2e/api/`. (The implementer should read the existing harness and substitute names — these placeholders flag the exact integration points to wire up; they are *not* a sign-off for inventing a new harness.)

- [ ] **Step 3: Run**

Run: `go test -count=1 ./test/e2e/api/ -run TestSessionE2ETestSuite`
Expected: PASS for all three cases.

If the first case fails because the harness doesn't expose a raw `ClientConn`, add an accessor on the harness (smallest possible surface — one method returning `*grpc.ClientConn`).

- [ ] **Step 4: Commit**

```bash
git add test/e2e/api/session_test.go
git commit -m "$(cat <<'EOF'
test(e2e): cover session create, missing-session reject, and OK path

End-to-end suite using the existing api harness: a session id round-
trips, an Open without session_id is rejected (InvalidArgument), and
an Open carrying a valid session_id reaches the FUSE layer. Locks in
the wire contract between client and server for plan 1c.
EOF
)"
```

---

## Task 11: Plan complete — final sweep

**Why:** Confirm the whole plan is green and the leak fix actually holds under concurrent load before declaring done.

**Files:** None — verification only.

- [ ] **Step 1: Run the full working-set test command**

Run:
```
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```
Expected: PASS across the board.

- [ ] **Step 2: Run `go vet ./...`**

Run: `go vet ./...`
Expected: no output.

- [ ] **Step 3: Hand-grep for the things we deliberately removed**

Run:
```
grep -RIn "fdNum atomic" pkg/server/
grep -RIn "r\.files\.Store" pkg/server/
```
Expected: no matches — the old global fd table is gone.

- [ ] **Step 4: Hand off to the user for plain-terminal `task test`**

The Claude sandbox / GoLand-terminal limitation on FUSE mount tests still applies (see `feedback-fuse-test-env`). Ask the user to run:

```
task test
task lint
```

Document any failures back here; do not declare the plan done until the user reports clean output.

- [ ] **Step 5: No commit needed — verification only**

---

## Self-review notes

- **Spec coverage:** Item #5 (session concept) → Tasks 1, 3, 4, 5, 8. Item #8 (fd lifecycle) → Task 7 (specifically `TestOpenNonOkDoesNotRegisterFd` / `TestCreateNonOkDoesNotRegisterFd`). The "kill server, restart, recover" DoD line in the roadmap is explicitly out of scope here (it needs durable state); the `Resume` RPC handles the *client-reconnect-to-still-running-server* case which is what's tractable in 1c.
- **Type consistency:** `SessionManager` / `Session` / `FileEntry` / `RegisterFile` / `GetFile` / `ReleaseFile` / `MarkDisconnected` / `Resume` / `Stop` are used identically across Tasks 3, 4, 5, 7, 8. `SessionHandshake.Establish` / `SessionID` / `Close` / `IsRunning` are used consistently across Task 8 and (via the `Client` interface) Task 9.
- **No placeholders left:** every code block is concrete; the e2e harness names in Task 10 are flagged as needing substitution against the existing fixture, not as gaps in the plan.
- **Backwards compatibility:** intentionally none. The `session_id` field on file RPCs is now required; clients without it fail with `InvalidArgument`.
