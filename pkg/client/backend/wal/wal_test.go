package wal

// wal_test.go — TDD tests for the WAL Coordinator (Task 10a).
//
// Test coverage:
//   1. Drain on a DELEGATED path: op lands in BboltLog (Replay shows it) + the
//      Overlay reflects the write; the wireFlush callback is NOT invoked.
//   2. Drain on a NON-DELEGATED path: wireFlush IS invoked with the correct
//      bytes/offset; Log and Overlay are untouched.
//   3. Drain on a DELEGATED path with zero-byte pending: no op appended, no
//      wireFlush called, returns FS_OK.
//   4. RecordOp: appends to the log (Replay) and applies to the overlay (Has).
//   5. RecordOp with a log failure: Apply is NOT called (durability-first).
//
// Setup: real delegation.Manager (controlled via mgr.Apply with a DelegationGrant),
// real BboltLog (opened against t.TempDir()), real Overlay. No mocks needed —
// the wireFlush callback is a plain closure with a captured bool.

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// noopInvalidator satisfies delegation.CacheInvalidator without doing anything.
type noopInvalidator struct{}

func (noopInvalidator) InvalidateSubtree(_ string) {}

// openTestLog opens a fresh BboltLog in t.TempDir().
func openTestLog(t *testing.T) *BboltLog {
	t.Helper()
	l, err := Open(filepath.Join(t.TempDir(), "wal.db"))
	if err != nil {
		t.Fatalf("Open wal: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// ── Coordinator suite ─────────────────────────────────────────────────────────

type CoordinatorSuite struct {
	suite.Suite
	mgr     *delegation.Manager
	log     *BboltLog
	overlay *Overlay
	coord   *Coordinator
}

func (s *CoordinatorSuite) SetupTest() {
	s.mgr = delegation.NewManager(noopInvalidator{})
	s.log = openTestLog(s.T())
	s.overlay = NewOverlay()
	s.coord = NewCoordinator(s.mgr, s.log, s.overlay)
}

// grant makes path (and everything under it) delegated for this test.
func (s *CoordinatorSuite) grant(root string) {
	s.mgr.Apply(&proto.DelegationGrant{GrantedRoot: root})
}

// ── Test 1: Drain on delegated path ──────────────────────────────────────────

func (s *CoordinatorSuite) TestDrain_DelegatedPath_LoggedAndOverlayApplied_WireNotCalled() {
	s.grant("docs")

	path := "docs/readme.md"
	pending := []byte("hello world")
	off := int64(0)

	wireCalled := false
	wireFlush := func(_ context.Context, data []byte, wOff int64, _ string) proto.FsError {
		wireCalled = true
		return proto.FsError_FS_OK
	}

	st := s.coord.Drain(context.Background(), path, pending, off, "req-1", wireFlush)

	s.Require().Equal(proto.FsError_FS_OK, st, "Drain on delegated path must return FS_OK")
	s.Assert().False(wireCalled, "wireFlush must NOT be called on a delegated path")

	// Op must appear in the log.
	ops, err := s.log.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(ops, 1, "one Op must be in the log")
	s.Assert().Equal(OpWrite, ops[0].Kind)
	s.Assert().Equal(path, ops[0].Path)
	s.Assert().Equal(off, ops[0].Offset)
	s.Assert().Equal(pending, ops[0].Data)

	// Overlay must reflect the write.
	s.Assert().True(s.overlay.Has(path), "overlay must have pending state for the path")
	merged := s.overlay.ReadMerge(path, 0, make([]byte, len(pending)))
	s.Assert().Equal(pending, merged, "overlay ReadMerge must return the pending bytes")
}

// ── Test 2: Drain on non-delegated path ──────────────────────────────────────

func (s *CoordinatorSuite) TestDrain_NonDelegatedPath_WireCalledLogAndOverlayUntouched() {
	// No grant → path is not delegated.
	path := "work/data.bin"
	pending := []byte("raw bytes")
	off := int64(42)

	var capturedData []byte
	var capturedOff int64
	wireFlush := func(_ context.Context, data []byte, wOff int64, _ string) proto.FsError {
		capturedData = data
		capturedOff = wOff
		return proto.FsError_FS_OK
	}

	st := s.coord.Drain(context.Background(), path, pending, off, "req-2", wireFlush)

	s.Require().Equal(proto.FsError_FS_OK, st, "Drain must propagate wireFlush's return value")
	s.Assert().Equal(pending, capturedData, "wireFlush must receive the pending bytes")
	s.Assert().Equal(off, capturedOff, "wireFlush must receive the correct offset")

	// Log must be empty.
	ops, err := s.log.Replay(0)
	s.Require().NoError(err)
	s.Assert().Empty(ops, "log must be untouched for a non-delegated path")

	// Overlay must have no state.
	s.Assert().False(s.overlay.Has(path), "overlay must be untouched for a non-delegated path")
}

// ── Test 3: Drain on delegated path with zero-byte pending ───────────────────

func (s *CoordinatorSuite) TestDrain_DelegatedPath_ZeroBytePending_NoOpAndWireNotCalled() {
	// Grant the root of the tree (non-empty root that matches all paths below it).
	// Note: delegation.Manager.Apply ignores empty-root grants (it treats them as
	// denials per the protocol), so we use a concrete root here.
	s.grant("any")

	path := "any/file.txt"

	wireCalled := false
	wireFlush := func(_ context.Context, _ []byte, _ int64, _ string) proto.FsError {
		wireCalled = true
		return proto.FsError_FS_OK
	}

	// Zero-length pending (a clean Flush with nothing coalesced but dirty=true).
	st := s.coord.Drain(context.Background(), path, nil, 0, "req-3", wireFlush)

	s.Require().Equal(proto.FsError_FS_OK, st, "zero-byte delegated Drain must return FS_OK")
	s.Assert().False(wireCalled, "wireFlush must not be called for zero-byte pending")

	// No log entry.
	ops, err := s.log.Replay(0)
	s.Require().NoError(err)
	s.Assert().Empty(ops, "log must be empty after zero-byte delegated drain")

	s.Assert().False(s.overlay.Has(path), "overlay must be untouched for zero-byte drain")
}

// ── Test 4: RecordOp ─────────────────────────────────────────────────────────

func (s *CoordinatorSuite) TestRecordOp_AppendsToLogAndAppliesOverlay() {
	// RecordOp now admits only write-delegated paths (Task 3): grant the root
	// first, or the op is refused with ErrNotDelegated.
	s.grant("newdir")

	op := Op{
		Kind: OpMkdir,
		Path: "newdir",
		Mode: 0o40755,
	}

	err := s.coord.RecordOp(op)
	s.Require().NoError(err)

	// Log must have one entry.
	ops, err := s.log.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(ops, 1)
	s.Assert().Equal(OpMkdir, ops[0].Kind)
	s.Assert().Equal("newdir", ops[0].Path)
	s.Assert().Equal(uint32(0o40755), ops[0].Mode)

	// Overlay must reflect the mkdir.
	s.Assert().True(s.overlay.Has("newdir"), "overlay must have the new directory")
	attr, ok, tombstoned, baseDelta, _ := s.overlay.Stat("newdir")
	s.Assert().True(ok)
	s.Assert().False(tombstoned)
	s.Assert().False(baseDelta)
	s.Assert().Equal(uint32(0o40755), attr.Mode)
}

// ── Test 5: RecordOp with log failure skips Apply ────────────────────────────

func (s *CoordinatorSuite) TestRecordOpLogFailureSkipsApply() {
	// Grant the root first — RecordOp now admits only write-delegated paths
	// (Task 3), and this test wants to exercise the Append failure, not the
	// admission refusal.
	s.grant("faildir")

	// Close the log to force Append to fail on the next RecordOp.
	err := s.log.Close()
	s.Require().NoError(err)

	op := Op{
		Kind: OpMkdir,
		Path: "faildir",
		Mode: 0o40755,
	}

	// RecordOp should fail on log Append.
	err = s.coord.RecordOp(op)
	s.Require().Error(err, "RecordOp must return an error when Append fails")

	// The crucial durability invariant: overlay must NOT have the op
	// (Apply was skipped because Append failed).
	s.Assert().False(s.overlay.Has("faildir"), "overlay must NOT have the directory after log failure")

	// Verify overlay is still empty (no ops recorded).
	s.Assert().False(s.overlay.Has("faildir"))
}

// ── Test 6: Drain propagates wireFlush error for non-delegated path ──────────

func (s *CoordinatorSuite) TestDrain_NonDelegatedPath_PropagatesWireError() {
	path := "file.dat"
	wireFlush := func(_ context.Context, _ []byte, _ int64, _ string) proto.FsError {
		return proto.FsError_FS_ENOSPC
	}

	st := s.coord.Drain(context.Background(), path, []byte("x"), 0, "req-4", wireFlush)
	s.Assert().Equal(proto.FsError_FS_ENOSPC, st, "non-delegated Drain must propagate wire error")
}

// ── Test 7: Read accessors pass through to Overlay ───────────────────────────

func (s *CoordinatorSuite) TestReadAccessors_PassThroughToOverlay() {
	// RecordOp now admits only write-delegated paths (Task 3).
	s.grant("sub")

	// Apply a write via RecordOp so the overlay has data.
	err := s.coord.RecordOp(Op{
		Kind:   OpWrite,
		Path:   "sub/file.txt",
		Offset: 0,
		Data:   []byte("coordinator"),
	})
	s.Require().NoError(err)

	// Has
	s.Assert().True(s.coord.Has("sub/file.txt"))
	s.Assert().False(s.coord.Has("sub/other.txt"))

	// ReadMerge
	merged := s.coord.ReadMerge("sub/file.txt", 0, make([]byte, 11))
	s.Assert().Equal([]byte("coordinator"), merged)

	// Stat — a write to a path not previously created by the overlay produces a
	// baseDelta node (the overlay doesn't own the inode; the base does). See
	// Overlay.applyWrite / newBaseDeltaNode.
	attr, ok, tombstoned, baseDelta, _ := s.coord.Stat("sub/file.txt")
	s.Require().True(ok)
	s.Assert().False(tombstoned)
	s.Assert().True(baseDelta, "a write to a pre-existing base path must produce a baseDelta node")
	s.Assert().NotNil(attr)

	// ListMerge — overlay-created entries appear in the result.
	entries := s.coord.ListMerge("sub", nil)
	s.Require().Len(entries, 1)
	s.Assert().Equal("file.txt", entries[0].Name)

	// Xattr — no xattr pending, returns false/false.
	_, set, removed := s.coord.Xattr("sub/file.txt", "user.foo")
	s.Assert().False(set)
	s.Assert().False(removed)
}

// ── Test 8: StartIntervalFlusher goroutine exits on Close ─────────────────────

// TestStartIntervalFlusher_ExitsOnClose verifies that the goroutine started by
// StartIntervalFlusher exits cleanly when Close() is called. It uses a very
// short interval to exercise the tick path, then calls Close and waits for
// completion to assert no goroutine leak.
func (s *CoordinatorSuite) TestStartIntervalFlusher_ExitsOnClose() {
	mgr := delegation.NewManager(noopInvalidator{})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	// Start with a very short interval so the goroutine has time to tick.
	coord.StartIntervalFlusher(10 * time.Millisecond)

	// Give the goroutine a couple of ticks so we are sure it is alive.
	time.Sleep(30 * time.Millisecond)

	// Close must stop the goroutine and return cleanly (no deadlock, no leak).
	done := make(chan struct{})
	go func() {
		_ = coord.Close()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown.
	case <-time.After(3 * time.Second):
		s.Fail("Close() did not return within 3s — interval flusher goroutine leaked")
	}
}

// TestClose_SafeWithoutIntervalFlusher verifies that Close() is safe even when
// StartIntervalFlusher was never called (no goroutine to stop, flusherWg at
// zero, stopFlusher is still idempotent).
func (s *CoordinatorSuite) TestClose_SafeWithoutIntervalFlusher() {
	mgr := delegation.NewManager(noopInvalidator{})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	// Must not panic or deadlock.
	done := make(chan struct{})
	go func() {
		_ = coord.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.Fail("Close() deadlocked when flusher was never started")
	}
}

// TestClose_IsIdempotent verifies that calling Close() multiple times does not
// panic (flushStopOnce + WaitGroup semantics are robust under concurrent calls).
func (s *CoordinatorSuite) TestClose_IsIdempotent() {
	mgr := delegation.NewManager(noopInvalidator{})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())
	coord.StartIntervalFlusher(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = coord.Close()
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		s.Fail("multiple Close() calls deadlocked")
	}
}

// TestStartIntervalFlusher_CloseRace verifies that Close() correctly waits for
// the interval flusher goroutine to exit before closing the log. This test is
// specifically designed to expose a use-after-close race under -race: it writes
// ops to the log so the goroutine actually calls log.Replay on every tick,
// then calls Close concurrently. If flusherWg does not cover the Replay-calling
// goroutine, the race detector will flag concurrent bbolt access.
func (s *CoordinatorSuite) TestStartIntervalFlusher_CloseRace() {
	// Use the suite-level mgr/log/overlay so openTestLog's Cleanup handles log.Close
	// only if we don't Close the coord first — coord.Close() is the only closer here.
	mgr := delegation.NewManager(noopInvalidator{})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	// Write a real op so Replay finds len(ops) > 0 on every tick. The op won't
	// actually be flushed (no applyFactory wired) but Replay WILL access the log.
	op := Op{Kind: OpWrite, Path: "race/test.bin", Data: []byte("test")}
	_, err := l.Append(op)
	s.Require().NoError(err)

	// Start the flusher with a very short interval so the goroutine is likely
	// executing Replay concurrently with Close.
	coord.StartIntervalFlusher(1 * time.Millisecond)

	// Give the goroutine a few ticks.
	time.Sleep(20 * time.Millisecond)

	// Close must wait for the goroutine — no use-after-close, no deadlock.
	done := make(chan struct{})
	go func() {
		_ = coord.Close()
		close(done)
	}()

	select {
	case <-done:
		// flusherWg correctly fenced the log-touching goroutine.
	case <-time.After(3 * time.Second):
		s.Fail("Close() did not return within 3s — interval flusher goroutine leaked or deadlocked")
	}
}

// ── Test 9a: StartupReplay_CloseRace ─────────────────────────────────────────

// TestStartupReplay_CloseRace mirrors TestStartIntervalFlusher_CloseRace for
// the startup replay goroutine.  It verifies that Close() correctly waits for
// the StartupReplay goroutine to exit before closing the log, preventing a
// use-after-close race between log.Replay (inside the goroutine) and
// log.Close (inside coord.Close).
//
// The test seeds a non-empty log so the goroutine actually calls log.Replay,
// then races Close against the goroutine.  Under -race, a missing flusherWg
// guard produces a concurrent bbolt access; a missing cancel produces a hang.
func (s *CoordinatorSuite) TestStartupReplay_CloseRace() {
	mgr := delegation.NewManager(noopInvalidator{})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	// Seed a real op so Replay finds len(ops) > 0 and actually reads the log
	// inside the goroutine (not a fast no-op return).
	op := Op{Kind: OpWrite, Path: "race/startup.bin", Data: []byte("recovery")}
	_, err := l.Append(op)
	s.Require().NoError(err)

	// StartupReplay registers the goroutine in flusherWg; no applyFactory means
	// Replay returns immediately after reading the log — but the -race detector
	// will catch any concurrent access if flusherWg is absent.
	coord.StartupReplay(context.Background())

	// Close must wait for the goroutine — no use-after-close, no deadlock.
	done := make(chan struct{})
	go func() {
		_ = coord.Close()
		close(done)
	}()

	select {
	case <-done:
		// flusherWg correctly fenced the log-touching goroutine.
	case <-time.After(3 * time.Second):
		s.Fail("Close() did not return within 3s — StartupReplay goroutine leaked or deadlocked")
	}
}

// ── Test 9: RebuildOverlay — empty WAL is a no-op ────────────────────────────

// TestRebuildOverlay_EmptyWAL_NoOp verifies that RebuildOverlay on a fresh
// (empty) log returns nil and leaves the overlay untouched.
func (s *CoordinatorSuite) TestRebuildOverlay_EmptyWAL_NoOp() {
	// Fresh coordinator with no ops recorded.
	err := s.coord.RebuildOverlay()
	s.Require().NoError(err, "RebuildOverlay on empty WAL must succeed")
	// Overlay must remain pristine: no paths, no state.
	s.Assert().False(s.overlay.Has("any/path"), "overlay must be empty after no-op rebuild")
}

// ── Test 10: RebuildOverlay — leftover ops populate the overlay ───────────────

// TestRebuildOverlay_LeftoverOps_OverlayPopulated verifies the dead-process
// recovery path:
//
//  1. Record ops into a BboltLog without acking them (simulate a crash: write
//     ops to the log but do NOT call coord.Close(), which would flush them).
//  2. Close ONLY the BboltLog (release the bbolt file lock).
//  3. Open a fresh BboltLog on the same file.
//  4. Build a fresh Coordinator with the new log and a fresh empty Overlay.
//  5. Call RebuildOverlay.
//  6. Assert (a) the overlay has the pending state (RYOW correct immediately)
//     before any Replay.
func (s *CoordinatorSuite) TestRebuildOverlay_LeftoverOps_OverlayPopulated() {
	// Step 1: record ops into the existing log WITHOUT closing the coord
	// (closing coord auto-flushes, which would empty the log).
	walPath := filepath.Join(s.T().TempDir(), "recovery.db")
	seedLog, err := Open(walPath)
	s.Require().NoError(err, "open seed log")

	seedMgr := delegation.NewManager(noopInvalidator{})
	seedOverlay := NewOverlay()
	seedCoord := NewCoordinator(seedMgr, seedLog, seedOverlay)

	// RecordOp now admits only write-delegated paths (Task 3).
	seedMgr.Apply(&proto.DelegationGrant{GrantedRoot: "crash"})

	// Record a Mkdir and a Create op.
	s.Require().NoError(seedCoord.RecordOp(Op{Kind: OpMkdir, Path: "crash/dir", Mode: 0o40755}))
	s.Require().NoError(seedCoord.RecordOp(Op{Kind: OpCreate, Path: "crash/dir/file.txt", Mode: 0o644}))

	// Step 2: close ONLY the log (not the coordinator — that would flush).
	s.Require().NoError(seedLog.Close())

	// Step 3: open a fresh log on the same file.
	freshLog, err := Open(walPath)
	s.Require().NoError(err, "open fresh log on same file")
	s.T().Cleanup(func() { _ = freshLog.Close() })

	// Step 4: fresh coordinator + empty overlay (simulates new process).
	freshOverlay := NewOverlay()
	freshMgr := delegation.NewManager(noopInvalidator{})
	freshCoord := NewCoordinator(freshMgr, freshLog, freshOverlay)

	// Step 5: RebuildOverlay.
	s.Require().NoError(freshCoord.RebuildOverlay())

	// Step 6(a): overlay must reflect the seeded ops — RYOW before any Replay.
	s.Assert().True(freshOverlay.Has("crash/dir"), "overlay must have the crashed mkdir")
	s.Assert().True(freshOverlay.Has("crash/dir/file.txt"), "overlay must have the crashed create")

	// The log must still have all ops (RebuildOverlay is read-only).
	ops, err := freshLog.Replay(0)
	s.Require().NoError(err)
	s.Assert().Len(ops, 2, "log must retain all ops after RebuildOverlay (no truncation)")
}

// ── Test 11: RecordOp admission (Task 3) ─────────────────────────────────────

// TestRecordOpStampsGenAndRefusesUndelegated verifies that RecordOp stamps the
// op with the covering grant's gen (so the server's revoked-gen fence can
// reject a stale replay), and that a path outside any grant is refused with
// ErrNotDelegated instead of being silently appended.
func (s *CoordinatorSuite) TestRecordOpStampsGenAndRefusesUndelegated() {
	mgr := delegation.NewManager(noopInvalidator{})
	mgr.Apply(&proto.DelegationGrant{GrantedRoot: "dir", Gen: 9})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	s.Require().NoError(coord.RecordOp(Op{Kind: OpMkdir, Path: "dir/sub", Mode: 0o755}))
	ops, err := l.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(ops, 1)
	s.Equal(uint64(9), ops[0].Gen, "recorded op carries the covering grant's gen")

	err = coord.RecordOp(Op{Kind: OpMkdir, Path: "other/sub", Mode: 0o755})
	s.ErrorIs(err, ErrNotDelegated, "undelegated path is refused, caller goes synchronous")
}

// TestRecordOpRefusesRenameWithUndelegatedNewPath verifies that a rename whose
// destination falls outside the covering grant is refused even though the
// source path is delegated — both endpoints must be write-delegated for the
// rename to be deferred.
func (s *CoordinatorSuite) TestRecordOpRefusesRenameWithUndelegatedNewPath() {
	mgr := delegation.NewManager(noopInvalidator{})
	mgr.Apply(&proto.DelegationGrant{GrantedRoot: "dir", Gen: 3})
	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	err := coord.RecordOp(Op{Kind: OpRename, Path: "dir/a", NewPath: "outside/b"})
	s.ErrorIs(err, ErrNotDelegated)
}

// blockingRecallFlusher implements delegation.RecallFlusher and blocks
// FlushForRecall until release is closed — used to hold a root in the
// "draining" state for the duration of a test.
type blockingRecallFlusher struct {
	release chan struct{}
}

func (b *blockingRecallFlusher) FlushForRecall(_ context.Context, _ string) error {
	<-b.release
	return nil
}

// TestDrain_ParksWhileRootDrainsThenWiresAfterHandoff verifies Drain's
// park-until-resolved contract: a Drain issued while the covering root is
// draining must NOT write to the wire — a wire Write would race the in-flight
// recall Apply stream carrying OLDER deferred writes for the same path, and
// the server serializes the two arbitrarily (newer bytes silently overwritten
// by older = lost update). Drain must park until the drain resolves and reach
// the wire only AFTER the handoff completes with the grant gone.
func (s *CoordinatorSuite) TestDrain_ParksWhileRootDrainsThenWiresAfterHandoff() {
	mgr := delegation.NewManager(noopInvalidator{})
	mgr.Apply(&proto.DelegationGrant{GrantedRoot: "dir", Gen: 1})
	release := make(chan struct{})
	mgr.SetRecallFlusher(&blockingRecallFlusher{release: release})

	l := openTestLog(s.T())
	coord := NewCoordinator(mgr, l, NewOverlay())

	recallDone := make(chan struct{})
	go func() {
		_ = mgr.OnRecall(context.Background(), "dir")
		close(recallDone)
	}()

	// Wait for the recall to mark the root draining.
	for mgr.IsWriteDelegated("dir/f.txt") {
		time.Sleep(time.Millisecond)
	}
	s.True(mgr.IsDelegated("dir/f.txt"), "the grant is still held while draining (reads stay served from the overlay)")

	var wireCalls atomic.Int32
	wireFlush := func(_ context.Context, _ []byte, _ int64, _ string) proto.FsError {
		wireCalls.Add(1)
		return proto.FsError_FS_OK
	}

	drainDone := make(chan proto.FsError, 1)
	go func() {
		drainDone <- coord.Drain(context.Background(), "dir/f.txt", []byte("x"), 0, "req-drain", wireFlush)
	}()

	// While the recall flush is parked, Drain must neither return nor touch
	// the wire (bounded negative window).
	select {
	case <-drainDone:
		s.Fail("Drain must park while the root is draining, not return")
	case <-time.After(50 * time.Millisecond):
	}
	s.Zero(wireCalls.Load(), "Drain must not wire-write mid-drain (would race the recall Apply stream)")

	// Complete the recall: the handoff drops the grant, the parked Drain
	// wakes, is refused deferral (grant gone), and only then goes to the wire.
	close(release)
	<-recallDone

	var st proto.FsError
	select {
	case st = <-drainDone:
	case <-time.After(2 * time.Second):
		s.FailNow("Drain did not resolve after the drain completed")
	}
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal(int32(1), wireCalls.Load(), "Drain must reach the wire exactly once, after the handoff")
}

func TestCoordinatorSuite(t *testing.T) {
	suite.Run(t, new(CoordinatorSuite))
}
