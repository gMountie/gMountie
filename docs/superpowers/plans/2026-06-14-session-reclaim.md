# Session Reclaim Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make open files survive a `gmountie serve` process restart by having the client transparently reopen its file handles against the new session when the old one dies.

**Architecture:** Client-only, no proto change. A `gmountie serve` restart mints a new `session_id` on the client (existing `Resume`→`Create` fallback in `SessionHandshake`). fd-carrying RPCs already send a *snapshot* of the session id (`h.sessionID`) captured at open time, while the live id is `client.SessionID()` — so `h.sessionID != client.SessionID()` is a precise "my fd is stale" predicate. Each `grpcFileHandle` gains sanitized reopen-flags and a `reclaimIfStale` method that reopens itself (via `Open`, never `Create`) under a per-handle mutex; every fd-op attempt calls it first. The `retryOp` guard is relaxed so `classFdOp` keeps retrying across a session change instead of bailing, letting the handle self-heal inside the retry window. The server side already drains in-flight RPCs via `GracefulStop` and needs no change.

**Tech Stack:** Go, `github.com/hanwen/go-fuse/v2`, gRPC, testify suites, `syscall` open-flag constants.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `pkg/client/io/reclaim.go` | New: `sanitizeReopenFlags` + the `reclaimIfStale` method | Create |
| `pkg/client/io/reclaim_test.go` | New: unit tests for the sanitizer and `reclaimIfStale` | Create |
| `pkg/client/io/backend_grpc.go` | `grpcFileHandle` struct (add `reopenFlags`, `reopenMu`); `newGrpcFileHandle` (accept + sanitize flags); `Open`/`Create` callers (pass flags); fd-op closures call `reclaimIfStale` | Modify |
| `pkg/client/io/retry.go` | Relax the session-change guard so `classFdOp` continues | Modify |
| `pkg/client/io/retry_test.go` | Guard behaviour test (fd-op continues, path-mutation bails) | Modify/Create |
| `test/e2e/...` | E2E: restart server under an open file, assert resume | Create |
| `docs/design/reliability-and-recovery.md` | Durable design record | Create |
| `docs/roadmap.md` | One-line "Done" row | Modify |

**Reclaim is lazy/per-handle** — no central open-file registry. A handle that is never touched again is simply never reopened (and `Release` of a stale handle is a best-effort no-op on the new server).

---

## Task 1: Sanitize reopen flags

A reopened handle must NOT re-apply `O_CREAT|O_EXCL|O_TRUNC` (truncation = data loss; `O_EXCL` would fail). Keep the access mode (`O_RDONLY/O_WRONLY/O_RDWR`) and `O_APPEND`.

**Files:**
- Create: `pkg/client/io/reclaim.go`
- Create: `pkg/client/io/reclaim_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/client/io/reclaim_test.go`:

```go
package io

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReclaimFlagsSuite struct {
	suite.Suite
}

func (s *ReclaimFlagsSuite) TestStripsCreateExclTrunc() {
	in := uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC)
	s.Equal(uint32(syscall.O_RDWR), sanitizeReopenFlags(in))
}

func (s *ReclaimFlagsSuite) TestPreservesAppend() {
	in := uint32(syscall.O_WRONLY | syscall.O_APPEND)
	s.Equal(uint32(syscall.O_WRONLY|syscall.O_APPEND), sanitizeReopenFlags(in))
}

func (s *ReclaimFlagsSuite) TestReadOnlyUnchanged() {
	s.Equal(uint32(syscall.O_RDONLY), sanitizeReopenFlags(uint32(syscall.O_RDONLY)))
}

func (s *ReclaimFlagsSuite) TestStripsCreateKeepsAppend() {
	in := uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_APPEND)
	s.Equal(uint32(syscall.O_WRONLY|syscall.O_APPEND), sanitizeReopenFlags(in))
}

func TestReclaimFlagsSuite(t *testing.T) {
	suite.Run(t, new(ReclaimFlagsSuite))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestReclaimFlagsSuite ./pkg/client/io/`
Expected: FAIL — `undefined: sanitizeReopenFlags`.

- [ ] **Step 3: Write minimal implementation**

Create `pkg/client/io/reclaim.go`:

```go
package io

import "syscall"

// sanitizeReopenFlags returns the open flags to use when REOPENING an
// already-open file during reclaim. The file already exists and already holds
// the application's data, so creation/exclusivity/truncation flags must be
// stripped: O_TRUNC would discard the bytes the app has been writing, and
// O_EXCL would fail because the path now exists. The access mode and O_APPEND
// are preserved so reads/writes keep the same semantics.
func sanitizeReopenFlags(flags uint32) uint32 {
	const strip = uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_TRUNC)
	return flags &^ strip
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestReclaimFlagsSuite ./pkg/client/io/`
Expected: PASS (4 subtests).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/reclaim.go pkg/client/io/reclaim_test.go
git commit -m "feat(client): sanitize reopen flags for handle reclaim"
```

---

## Task 2: Add reopen state to grpcFileHandle

Give the handle what it needs to reopen itself: the sanitized reopen-flags and a mutex to serialize concurrent reopens. `grpcFileHandle` already holds `client`, `volume`, `path`, `fd`, `sessionID`.

**Files:**
- Modify: `pkg/client/io/backend_grpc.go` (`grpcFileHandle` struct ~1341–1378; `newGrpcFileHandle` ~1417–1445; callers in `Open` ~640 and `Create` ~678)

- [ ] **Step 1: Add fields to the struct**

In `pkg/client/io/backend_grpc.go`, add to the `grpcFileHandle` struct (next to `fd` and `sessionID`):

```go
	// reopenFlags are the access-mode + O_APPEND flags to use when reclaiming
	// this handle after a server restart (creation/trunc flags stripped). Set
	// once at construction; never mutated.
	reopenFlags uint32
	// reopenMu serializes reclaimIfStale so concurrent fd-ops on this handle
	// reopen the server fd exactly once. It guards the fd/sessionID swap.
	reopenMu sync.Mutex
```

- [ ] **Step 2: Thread flags through the constructor**

Change `newGrpcFileHandle`'s signature to accept the original open `flags uint32` and store the sanitized form. Update the signature and the struct literal:

```go
func newGrpcFileHandle(
	client grpcclient.Client,
	volume, path string,
	fd uint64,
	flags uint32,
	ioTimeout time.Duration,
	sessionID string,
	cfg grpcclient.PerFileConfig,
) *grpcFileHandle {
	ctx, cancel := context.WithCancel(context.Background())
	h := &grpcFileHandle{
		client:            client,
		fileClient:        client.File(),
		volume:            volume,
		path:              path,
		fd:                fd,
		reopenFlags:       sanitizeReopenFlags(flags),
		ioTimeout:         ioTimeout,
		sessionID:         sessionID,
		coalesceThreshold: cfg.WriteCoalesceBytes,
		lifeCtx:           ctx,
		lifeCancel:        cancel,
	}
	// ... unchanged readahead/coalescer setup ...
	return h
}
```

- [ ] **Step 3: Update the two callers**

In `BackendClient.Open` (the `return newGrpcFileHandle(...)` ~line 640), pass `flags`:

```go
	return newGrpcFileHandle(
		b.client, b.volume, path, res.Fd, flags,
		b.client.IOTimeout(), b.client.SessionID(),
		b.client.PerFileConfig(),
	), fuse.OK
```

In `BackendClient.Create` (the `h := newGrpcFileHandle(...)` ~line 678), pass `flags` (the original create flags — they get sanitized to the access mode, so reclaim reopens, never re-creates):

```go
	h := newGrpcFileHandle(
		b.client, b.volume, path, res.Fd, flags,
		b.client.IOTimeout(), b.client.SessionID(),
		b.client.PerFileConfig(),
	)
```

- [ ] **Step 4: Verify it compiles and existing tests pass**

Run: `go build ./pkg/client/... && go test -run TestBackend ./pkg/client/io/`
Expected: builds; existing backend tests PASS (no behaviour change yet).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend_grpc.go
git commit -m "feat(client): carry sanitized reopen flags + reopen mutex on grpcFileHandle"
```

---

## Task 3: Implement reclaimIfStale

The core. Under `reopenMu`, if the handle's snapshot session id differs from the client's live id, reopen the file and swap in the new fd + session id.

**Files:**
- Modify: `pkg/client/io/reclaim.go`
- Modify: `pkg/client/io/reclaim_test.go`

This needs a tiny seam over `grpcclient.Client` for the test. `grpcFileHandle` already stores the concrete `client grpcclient.Client`; the method uses `h.client.SessionID()` and `h.client.File().Open(...)`, both already on that interface (confirmed in `backend_grpc.go`).

- [ ] **Step 1: Write the failing test**

Add to `pkg/client/io/reclaim_test.go`. Use a minimal fake implementing just the methods `reclaimIfStale` touches; if `grpcclient.Client` is large, prefer the generated mock in `internal/mocks` — but a hand fake keeps the call-count assertion explicit:

```go
type ReclaimStaleSuite struct {
	suite.Suite
}

// fakeReclaimClient implements only the grpcclient.Client surface that
// reclaimIfStale calls: SessionID() and File().Open(). Everything else panics
// so the test fails loudly if reclaim grows an unexpected dependency.
//
// NOTE: if grpcclient.Client is too wide to hand-fake, embed the generated
// mocks.Client and override SessionID/File instead.

func (s *ReclaimStaleSuite) TestReopensWhenStale() {
	// handle snapshot "A", live client id "B" -> stale -> one Open, fd swapped.
	// Assert: h.fd updated to the reopened fd, h.sessionID == "B",
	//         Open called exactly once with sanitized flags.
}

func (s *ReclaimStaleSuite) TestNoopWhenFresh() {
	// handle snapshot == live id -> reclaimIfStale returns OK, Open NOT called.
}

func (s *ReclaimStaleSuite) TestConcurrentReopenOnce() {
	// 8 goroutines call reclaimIfStale on a stale handle -> Open called once.
}

func TestReclaimStaleSuite(t *testing.T) {
	suite.Run(t, new(ReclaimStaleSuite))
}
```

Fill in the bodies with the fake (model it on existing fakes/mocks in `pkg/client/io/*_test.go`; an `atomic.Int32` Open counter and a settable `SessionID` string). The three assertions — *fd swapped*, *no-op when fresh*, *single reopen under concurrency* — are the properties that matter.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestReclaimStaleSuite ./pkg/client/io/`
Expected: FAIL — `h.reclaimIfStale undefined`.

- [ ] **Step 3: Write the implementation**

Add to `pkg/client/io/reclaim.go`:

```go
import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"
)

// reclaimIfStale reopens this handle's server-side fd against the current
// session when the server has restarted. The predicate is the handle's session
// snapshot (taken at open) vs the client's live session id: they diverge
// exactly when SessionHandshake fell back from Resume to Create — i.e. the
// server lost the original session and every fd under it.
//
// It is safe to call on EVERY fd-op attempt: when the handle is fresh it is a
// cheap compare-and-return. reopenMu serializes concurrent callers so the fd is
// reopened once; each caller re-checks the predicate under the lock.
//
// On success h.fd and h.sessionID are swapped to the new values and subsequent
// RPCs (which read h.fd / h.sessionID) target the live session. On failure the
// fuse.Status surfaces — notably the unlinked-but-open case, where the path no
// longer resolves and reopen cannot succeed.
func (h *grpcFileHandle) reclaimIfStale(ctx context.Context) fuse.Status {
	if h.sessionID == h.client.SessionID() {
		return fuse.OK
	}
	h.reopenMu.Lock()
	defer h.reopenMu.Unlock()

	// Re-check under the lock: a racing caller may have already reclaimed.
	live := h.client.SessionID()
	if h.sessionID == live {
		return fuse.OK
	}

	reply, err := h.client.File().Open(ctx, &proto.OpenRequest{
		Volume:    h.volume,
		Caller:    callerFromCtx(ctx),
		Path:      h.path,
		Flags:     h.reopenFlags,
		SessionId: live,
		RequestId: uuid.NewString(),
	}, grpc.WaitForReady(true))
	if err != nil {
		return statusFromRPCError(err)
	}
	if fuse.Status(reply.Status) != fuse.OK {
		return fuse.Status(reply.Status)
	}

	log.Log.Info("reclaimed file handle after server restart",
		zap.String("path", h.path),
		zap.Uint64("old_fd", h.fd), zap.Uint64("new_fd", reply.Fd))
	h.fd = reply.Fd
	h.sessionID = live
	return fuse.OK
}
```

Notes for the implementer:
- `callerFromCtx`, `statusFromRPCError`, and the `uuid` import already exist in `backend_grpc.go` (same package) — reuse them; add the `uuid` import to `reclaim.go` if the file needs it directly.
- Concurrent fd reads of `h.fd`/`h.sessionID` by other ops while reclaim writes them: those other ops are themselves about to call `reclaimIfStale` and block on `reopenMu`, so they read the post-swap values. The only readers that race are ops that already passed their own `reclaimIfStale` this attempt — they retry next attempt. (If the race detector flags `h.fd`, promote `fd`/`sessionID` to `atomic.Uint64`/guarded access in a follow-up; keep it simple first and let `-race` in CI decide.)

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -race -run TestReclaimStaleSuite ./pkg/client/io/`
Expected: PASS (3 subtests, no race).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/reclaim.go pkg/client/io/reclaim_test.go
git commit -m "feat(client): reclaimIfStale reopens a handle against the new session"
```

---

## Task 4: Call reclaimIfStale from every fd-op

Wire the self-heal into the fd-op closures so a stale handle reopens before the RPC. Each fd-op on `grpcFileHandle` runs its RPC inside a `retryOp(h.client, ...)` closure; add the reclaim call as the first statement of each closure.

**Files:**
- Modify: `pkg/client/io/backend_grpc.go` (fd-op closures: `Read` ~755, streaming `Write` ~830, `Release` ~1025, `Flush` ~1083, `Fsync` ~1118, `Allocate`, `GetLk`/`SetLk`/`SetLkw`, `CopyFileRange`, `Lseek`; and `drainCoalescer` ~986)

- [ ] **Step 1: Add the reclaim call to each fd-op closure**

For every fd-op whose closure issues an RPC with `Fd: h.fd, SessionId: h.sessionID`, insert at the top of the closure:

```go
		func(ctx context.Context) (*proto.XxxReply, error) {
			if st := h.reclaimIfStale(ctx); !st.Ok() {
				// Surface as a non-retryable fuse error so retryOp stops and the
				// status reaches userspace (e.g. unlinked-open path is gone).
				return nil, errFromStatus(st)
			}
			return h.fileClient.Xxx(ctx, &proto.XxxRequest{
				Volume:    h.volume,
				Fd:        h.fd,        // reads the (possibly reopened) fd
				SessionId: h.sessionID, // reads the (possibly swapped) id
				// ... op-specific fields ...
			}, grpc.WaitForReady(true))
		})
```

For the streaming `Write` (which builds frames before sending), call `h.reclaimIfStale(ctx)` before constructing the first frame, and read `h.fd`/`h.sessionID` into the header frame after it returns.

For `drainCoalescer` (~986), the pending bytes are sent via the same streaming `Write` path — ensure that path runs `reclaimIfStale` first so buffered coalesced writes replay to the reopened fd rather than the dead one.

- [ ] **Step 2: Add the `errFromStatus` helper if absent**

`retryOp` classifies retryability via `isRetryableGrpcError`. A reclaim failure (e.g. path gone) must be **non-retryable** so it surfaces immediately. If the package lacks a `fuse.Status → error` that `statusFromRPCError`'s inverse understands, add a tiny sentinel in `reclaim.go`:

```go
// errFromStatus wraps a fuse.Status as a non-retryable error so a failed
// reclaim short-circuits retryOp and the status reaches userspace unchanged.
type reclaimError struct{ st fuse.Status }

func (e reclaimError) Error() string { return "reclaim failed: " + e.st.String() }

func errFromStatus(st fuse.Status) error { return reclaimError{st} }
```

Then in each fd-op, map a `reclaimError` back to its `fuse.Status` where the op converts `err` to a status (most fd-ops already call `statusFromRPCError(err)` on failure — extend it, or check `errors.As(err, &reclaimError{})` first). Keep the conversion in ONE place: add to `statusFromRPCError`:

```go
	var re reclaimError
	if errors.As(err, &re) {
		return re.st
	}
```

- [ ] **Step 3: Verify build + existing fd-op tests pass**

Run: `go test -race -run 'TestBackend|TestFile|TestWrite|TestRead' ./pkg/client/io/`
Expected: PASS — no behaviour change when the handle is fresh (reclaimIfStale is a no-op).

- [ ] **Step 4: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/reclaim.go
git commit -m "feat(client): self-heal fd-ops via reclaimIfStale on every attempt"
```

---

## Task 5: Relax the retryOp guard so classFdOp self-heals

Today `retry.go` (~line 95) stops retrying a `classFdOp` the moment the live session id diverges from the attempt's start id, because the fd is dead. With reclaim the fd self-heals, so `classFdOp` must keep retrying within the window. `classPathMutation` keeps bailing (no fd to reopen; replay against the new session's empty idempotency cache is unsafe).

**Files:**
- Modify: `pkg/client/io/retry.go` (~line 95–96)
- Modify/Create: `pkg/client/io/retry_test.go`

- [ ] **Step 1: Write the failing test**

Add to `pkg/client/io/retry_test.go` (model `retryClient` on the existing test double in this file if present):

```go
func (s *RetrySuite) TestFdOpContinuesAcrossSessionChange() {
	// retryClient whose SessionID() flips A->B after the first attempt.
	// fn: first call returns codes.Unavailable, second returns nil.
	// class classFdOp -> expect SUCCESS (no early bail on the id change).
}

func (s *RetrySuite) TestPathMutationStillBailsOnSessionChange() {
	// same flip, class classPathMutation, fn always Unavailable
	// -> expect the error returned promptly after the id change (bailed).
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run 'TestRetrySuite/TestFdOpContinues' ./pkg/client/io/`
Expected: FAIL — current guard bails `classFdOp`, so it returns the error instead of succeeding.

- [ ] **Step 3: Change the guard**

In `retry.go`, change the session-change guard from stopping `classFdOp` to allowing it. The current line:

```go
		if c.SessionID() != startID && class != classIdempotentRead {
			return zero, err // session changed: fd dead / replay-unsafe
		}
```

becomes:

```go
		// Session changed (Create-fallback after a server restart). classFdOp
		// handles run reclaimIfStale on each attempt and reopen their fd, so
		// they keep retrying within the window. classPathMutation still bails:
		// it has no fd to reopen and a replay against the new session's empty
		// idempotency cache could surface a spurious EEXIST/ENOENT.
		if c.SessionID() != startID && class == classPathMutation {
			return zero, err
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -v -race -run TestRetrySuite ./pkg/client/io/`
Expected: PASS — fd-op continues, path-mutation bails, idempotent-read unchanged.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/retry.go pkg/client/io/retry_test.go
git commit -m "feat(client): let classFdOp retry across a session change for reclaim"
```

---

## Task 6: E2E — open file survives a server restart

Prove the whole path against a real FUSE mount and a real server process restart in-place (same data dir, fresh session table).

**Files:**
- Create: `test/e2e/fs/reclaim_test.go` (place beside the existing fs e2e suites; reuse `test/e2e/utils` for the in-process server + mount harness)

- [ ] **Step 1: Write the E2E test**

Model setup/teardown on an existing `test/e2e/fs` suite. The shape:

```go
// 1. Start server (utils harness) over a temp data dir; mount a volume.
// 2. Create + open a file for writing; write a first chunk; fsync.
// 3. Restart the server process IN PLACE: stop it (new boot => empty session
//    table) and start a fresh one over the SAME data dir + same listen addr,
//    within the client rpc.retry_window.
// 4. Continue writing through the SAME open fd; close.
// 5. Assert: no error reached the app, and the file's full content == chunk1+chunk2.
```

Key assertions: the post-restart `write()`/`close()` return success (reclaim happened), and the on-disk bytes are the concatenation (no truncation — proves flag sanitization).

- [ ] **Step 2: Run it locally where FUSE is available**

FUSE mounts do not work in the sandbox/GoLand. Run on the dev VM or a plain terminal with `/dev/fuse`:

Run: `go test -v -race -run TestReclaim ./test/e2e/fs/`
Expected: PASS. (CI runners have `/dev/fuse`, so this runs in CI too — do NOT skip on mount failure; gate only genuinely VM-only variants behind the existing `GMOUNTIE_E2E_VM` flag as the first statement in `SetupSuite`.)

- [ ] **Step 3: Commit**

```bash
git add test/e2e/fs/reclaim_test.go
git commit -m "test(e2e): open file survives an in-place server restart"
```

---

## Task 7: Durable docs + roadmap, prune the spec

**Files:**
- Create: `docs/design/reliability-and-recovery.md`
- Modify: `docs/roadmap.md`
- Delete: `docs/superpowers/specs/2026-06-14-session-reclaim-design.md` (consolidated)

- [ ] **Step 1: Write the design doc**

Create `docs/design/reliability-and-recovery.md` with a Docusaurus front-matter header matching the sibling docs in `docs/design/` (look at `caching-and-consistency.md` for the exact `id/title/sidebar_label/description` shape). Content: the problem, why no external DB (offset-based protocol, fds are process-bound), the `session_id`-change restart signal, lazy per-handle reclaim (sanitized reopen flags, `reclaimIfStale`, the relaxed `classFdOp` guard), `GracefulStop` draining on the server, and the documented limitations (unlinked-open files, locks not re-asserted → Design B). Cross-link `security-and-transport.md` (sessions) and `caching-and-consistency.md`.

- [ ] **Step 2: Add the roadmap row**

In `docs/roadmap.md`, under the **Done** section, add a one-line row:

```markdown
- **Session reclaim across server restarts** — client transparently reopens open files against the new session after a `serve` restart; no external DB. See [reliability-and-recovery](design/reliability-and-recovery.md). Locks + unlinked-open files deferred (Design B).
```

- [ ] **Step 3: Prune the transient spec**

```bash
git rm docs/superpowers/specs/2026-06-14-session-reclaim-design.md
```

- [ ] **Step 4: Commit**

```bash
git add docs/design/reliability-and-recovery.md docs/roadmap.md
git commit -m "docs: reliability-and-recovery design + roadmap row for session reclaim"
```

---

## Task 8: Full verification before PR

- [ ] **Step 1: Lint**

Run: `task lint`
Expected: no new findings.

- [ ] **Step 2: Full unit + race**

Run: `go test -race ./pkg/client/...`
Expected: PASS.

- [ ] **Step 3: E2E (on a FUSE-capable host / dev VM)**

Run: `go test -race ./test/e2e/fs/ -run TestReclaim`
Expected: PASS.

- [ ] **Step 4: Open the PR**

```bash
git push -u origin worktree-session-reclaim
gh pr create --fill
```

Body: summarize the problem, the client-only reclaim approach (no DB, no proto change), and the two deferred items (locks, unlinked-open). No AI attribution.

---

## Self-Review notes (for the implementer)

- **`flags` field name:** the plan calls the stored field `reopenFlags` (sanitized) everywhere — do not introduce a second raw `flags` field on the handle.
- **`reclaimIfStale` is called on EVERY fd-op attempt** including the coalescer drain — that is deliberate (it is the only way buffered writes replay to the new fd). Fresh-handle cost is one string compare.
- **Do not reopen via `Create`** — a `Create`'d handle reclaims through `Open` with sanitized flags. There is no `Create` in the reclaim path.
- **Server side has no task** — `GracefulStop` already drains in-flight RPCs. If verification shows a streaming `Read`/`Write` is *not* awaited by `GracefulStop` in this go-grpc version, add a server task; otherwise none.
- **Race detector is the arbiter** on the `h.fd`/`h.sessionID` swap. Start with the plain fields + mutex; only promote to atomics if `-race` flags a real reader.
