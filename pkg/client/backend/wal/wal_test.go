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

func TestCoordinatorSuite(t *testing.T) {
	suite.Run(t, new(CoordinatorSuite))
}
