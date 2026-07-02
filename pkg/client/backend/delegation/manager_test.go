package delegation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type noopInvalidator struct{}

func (noopInvalidator) InvalidateSubtree(string) {}

type fakeInv struct{ subtrees []string }

func (f *fakeInv) InvalidateSubtree(p string) { f.subtrees = append(f.subtrees, p) }

// fakeInvOrdered appends "inv:<root>" into a shared event log to allow ordering
// assertions together with fakeRecallFlusher.
type fakeInvOrdered struct {
	events *[]string
}

func (f *fakeInvOrdered) InvalidateSubtree(p string) {
	*f.events = append(*f.events, "inv:"+p)
}

// fakeRecallFlusher is a test double for RecallFlusher. It records each
// FlushForRecall call as "flush:<root>" in the shared event log, and returns
// the configured error (nil by default → success).
type fakeRecallFlusher struct {
	events *[]string
	err    error
}

func (f *fakeRecallFlusher) FlushForRecall(_ context.Context, root string) error {
	*f.events = append(*f.events, "flush:"+root)
	return f.err
}

// recallFlusherFunc is an adapter allowing a function to implement RecallFlusher.
type recallFlusherFunc func(ctx context.Context, root string) error

func (f recallFlusherFunc) FlushForRecall(ctx context.Context, root string) error {
	return f(ctx, root)
}

type ManagerSuite struct{ suite.Suite }

func TestManagerSuite(t *testing.T) { suite.Run(t, new(ManagerSuite)) }

// TestIsDelegated_NilReceiver: a nil *Manager must report nothing delegated,
// never panic. With WAL disabled the cache receives a nil delegation oracle; a
// typed-nil reaching IsDelegated previously crashed the FUSE loop ("transport
// endpoint not connected"). The nil-receiver guard makes it safe.
func (s *ManagerSuite) TestIsDelegated_NilReceiver() {
	var m *Manager // nil
	s.NotPanics(func() {
		s.False(m.IsDelegated("anything/x"))
	})
}

func (s *ManagerSuite) TestApplyThenIsDelegated() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})
	s.True(m.IsDelegated("proj/src/a.go"))
	s.False(m.IsDelegated("other/x"))
}

func (s *ManagerSuite) TestExcludedPathNotDelegated() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", ExcludedPaths: []string{"proj/vendor"}})
	s.True(m.IsDelegated("proj/src/a.go"))
	s.False(m.IsDelegated("proj/vendor/dep/x"))
}

func (s *ManagerSuite) TestOnRecallDropsAndInvalidates() {
	inv := &fakeInv{}
	m := NewManager(inv)
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})
	err := m.OnRecall(context.Background(), "proj")
	s.Require().NoError(err)
	s.False(m.IsDelegated("proj/src/a.go"))
	s.Equal([]string{"proj"}, inv.subtrees)
}

func (s *ManagerSuite) TestEmptyGrantNoOp() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{}) // denied
	s.False(m.IsDelegated("anything"))
}

// TestOnRecall_FlushBeforeInvalidate asserts that with a RecallFlusher wired,
// FlushForRecall is called BEFORE InvalidateSubtree and BEFORE the grant is
// dropped (the handoff barrier ordering guarantee).
func (s *ManagerSuite) TestOnRecall_FlushBeforeInvalidate() {
	events := make([]string, 0, 4)
	inv := &fakeInvOrdered{events: &events}
	m := NewManager(inv)
	defer m.Close()

	flusher := &fakeRecallFlusher{events: &events}
	m.SetRecallFlusher(flusher)
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})

	err := m.OnRecall(context.Background(), "proj")
	s.Require().NoError(err)

	// flush must appear before inv in the shared event log.
	s.Require().Len(events, 2, "expected exactly one flush event and one inv event")
	s.Equal("flush:proj", events[0], "flush must occur before invalidation")
	s.Equal("inv:proj", events[1], "invalidation must occur after flush")

	// Grant must be dropped after successful flush.
	s.False(m.IsDelegated("proj/src/a.go"), "grant must be dropped after successful flush")
}

// TestOnRecall_FlushSuccess_ThenInvalidateAndDrop verifies the success path end-to-end:
// flush → invalidate → grant dropped, OnRecall returns nil.
func (s *ManagerSuite) TestOnRecall_FlushSuccess_ThenInvalidateAndDrop() {
	events := make([]string, 0, 4)
	inv := &fakeInvOrdered{events: &events}
	m := NewManager(inv)
	defer m.Close()

	m.SetRecallFlusher(&fakeRecallFlusher{events: &events})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "a/b"})

	err := m.OnRecall(context.Background(), "a/b")
	s.Require().NoError(err)
	s.Equal([]string{"flush:a/b", "inv:a/b"}, events)
	s.False(m.IsDelegated("a/b/c"))
}

// TestOnRecall_FlushFailure_AbortHandoff verifies the failure path:
//   - OnRecall returns the flush error.
//   - The grant is NOT dropped (IsDelegated still true after the failed recall).
//   - The cache is NOT invalidated (InvalidateSubtree was not called).
//
// The recall loop must NOT send a RecallAck when OnRecall returns an error, so
// the server-side registry times out rather than accepting a false clean handoff.
func (s *ManagerSuite) TestOnRecall_FlushFailure_AbortHandoff() {
	events := make([]string, 0, 4)
	inv := &fakeInvOrdered{events: &events}
	m := NewManager(inv)
	defer m.Close()

	flushErr := errors.New("ENOSPC: WAL flush failed")
	m.SetRecallFlusher(&fakeRecallFlusher{events: &events, err: flushErr})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})

	err := m.OnRecall(context.Background(), "proj")
	s.Require().Error(err, "OnRecall must surface the flush error")
	s.Require().ErrorIs(err, flushErr)

	// Only flush should appear in the event log — no invalidation.
	s.Equal([]string{"flush:proj"}, events, "InvalidateSubtree must not be called on flush failure")

	// Grant must NOT be dropped — handoff was aborted.
	s.True(m.IsDelegated("proj/src/a.go"), "grant must be retained when flush fails (handoff aborted)")
}

// TestOnRecall_NilFlusher_Phase1Behavior verifies that without a RecallFlusher
// wired, OnRecall behaves identically to Phase 1: invalidate + drop, no panic.
func (s *ManagerSuite) TestOnRecall_NilFlusher_Phase1Behavior() {
	inv := &fakeInv{}
	m := NewManager(inv)
	defer m.Close()
	// No SetRecallFlusher call.
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})

	err := m.OnRecall(context.Background(), "proj")
	s.Require().NoError(err)
	s.False(m.IsDelegated("proj/x"), "grant must be dropped")
	s.Equal([]string{"proj"}, inv.subtrees, "cache must be invalidated")
}

func (s *ManagerSuite) TestApplyStoresGenAndGenForReturnsIt() {
	m := NewManager(noopInvalidator{})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", Gen: 42})

	s.Equal(uint64(42), m.GenFor("proj/src/a.txt"))
	s.Equal(uint64(42), m.GenFor("proj"))
	s.Zero(m.GenFor("elsewhere/b.txt"), "uncovered path has gen 0")
}

func (s *ManagerSuite) TestGenForRespectsExclusionsAndNilReceiver() {
	m := NewManager(noopInvalidator{})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", ExcludedPaths: []string{"proj/hot"}, Gen: 7})

	s.Zero(m.GenFor("proj/hot/x"), "excluded sub-path is not delegated")
	var nilMgr *Manager
	s.Zero(nilMgr.GenFor("proj/src/a.txt"), "nil manager returns 0")
}

func (s *ManagerSuite) TestIsWriteDelegatedFalseWhileDraining() {
	m := NewManager(noopInvalidator{})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", Gen: 1})

	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	m.SetRecallFlusher(recallFlusherFunc(func(ctx context.Context, root string) error {
		close(flushStarted)
		<-releaseFlush
		return nil
	}))

	done := make(chan error, 1)
	go func() { done <- m.OnRecall(context.Background(), "proj") }()

	<-flushStarted
	s.False(m.IsWriteDelegated("proj/src/a.txt"), "write admission stops during drain")
	s.True(m.IsDelegated("proj/src/a.txt"), "reads keep merging the overlay during drain")

	close(releaseFlush)
	s.Require().NoError(<-done)
	s.False(m.IsDelegated("proj/src/a.txt"), "grant dropped after handoff")
}

func (s *ManagerSuite) TestWaitDrainedBlocksUntilRecallCompletes() {
	m := NewManager(noopInvalidator{})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", Gen: 1})

	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	m.SetRecallFlusher(recallFlusherFunc(func(ctx context.Context, root string) error {
		close(flushStarted)
		<-releaseFlush
		return nil
	}))

	recallDone := make(chan error, 1)
	go func() { recallDone <- m.OnRecall(context.Background(), "proj") }()
	<-flushStarted

	waited := make(chan struct{})
	go func() {
		m.WaitDrained(context.Background(), "proj/src/a.txt")
		close(waited)
	}()

	select {
	case <-waited:
		s.Fail("WaitDrained returned while the recall flush was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFlush)
	s.Require().NoError(<-recallDone)
	select {
	case <-waited:
	case <-time.After(time.Second):
		s.Fail("WaitDrained did not return after the recall completed")
	}
}

// TestConcurrentSameRootRecallsKeepDrainingUntilLastFinishes verifies that when
// two OnRecall calls for the same root interleave, the root stays marked draining
// until the LAST in-flight recall's flush finishes, in EVERY completion ordering.
// The newer recall is made to finish (and fail) FIRST: its flush error means the
// grant is retained (no grant-drop to mask the verdict), so only the draining
// entry can be keeping write admission closed while the older recall still
// flushes. This discriminates against the pre-fix instance-checked delete, which
// would let the newer recall's completion prematurely un-drain the root.
func (s *ManagerSuite) TestConcurrentSameRootRecallsKeepDrainingUntilLastFinishes() {
	m := NewManager(noopInvalidator{})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", Gen: 1})

	type flushCall struct {
		started chan struct{}
		release chan struct{}
		fail    bool
	}
	older := &flushCall{started: make(chan struct{}), release: make(chan struct{})}
	newer := &flushCall{started: make(chan struct{}), release: make(chan struct{}), fail: true}
	queue := make(chan *flushCall, 2)
	queue <- older
	queue <- newer
	m.SetRecallFlusher(recallFlusherFunc(func(ctx context.Context, root string) error {
		c := <-queue
		close(c.started)
		<-c.release
		if c.fail {
			return errors.New("flush failed")
		}
		return nil
	}))

	doneOlder := make(chan error, 1)
	go func() { doneOlder <- m.OnRecall(context.Background(), "proj") }()
	<-older.started
	doneNewer := make(chan error, 1)
	go func() { doneNewer <- m.OnRecall(context.Background(), "proj") }()
	<-newer.started

	// The NEWER recall fails first. Its flush error means the grant is
	// RETAINED (no grant-drop to mask the verdict) — only the draining entry
	// can keep write admission closed while the older recall still flushes.
	close(newer.release)
	s.Require().Error(<-doneNewer)
	s.False(m.IsWriteDelegated("proj/a.txt"),
		"root must stay draining until the LAST in-flight recall finishes")
	s.True(m.IsDelegated("proj/a.txt"), "failed recall retains the grant")

	close(older.release)
	s.Require().NoError(<-doneOlder)
	s.False(m.IsDelegated("proj/a.txt"), "successful recall drops the grant")
	s.False(m.IsWriteDelegated("proj/a.txt"))
}

// TestWaitDrainedReturnsOnContextCancel verifies that WaitDrained unblocks on
// context cancellation even when the recall flusher is still running (i.e., the
// context cancel returns the waiter, not the completion of the flush itself).
func (s *ManagerSuite) TestWaitDrainedReturnsOnContextCancel() {
	m := NewManager(noopInvalidator{})
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", Gen: 1})
	blocked := make(chan struct{})
	stop := make(chan struct{})
	defer close(stop)
	m.SetRecallFlusher(recallFlusherFunc(func(ctx context.Context, root string) error {
		close(blocked)
		<-stop // blocks until the test ends; ctx cancel must free the waiter, not the flush
		return nil
	}))
	go func() { _ = m.OnRecall(context.Background(), "proj") }()
	<-blocked

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() { m.WaitDrained(ctx, "proj/x"); close(returned) }()
	cancel()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		s.Fail("WaitDrained ignored context cancellation")
	}
}
