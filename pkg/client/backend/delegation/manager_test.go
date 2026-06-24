package delegation

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/proto"
)

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
