package delegation

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type fakeRecaller struct {
	mu     sync.Mutex
	calls  []string // "owner:root"
	failOn map[string]bool
	block  chan struct{} // if non-nil, Recall blocks on this channel after recording the call
}

func (f *fakeRecaller) Recall(owner, root string) error {
	f.mu.Lock()
	f.calls = append(f.calls, owner+":"+root)
	fail := f.failOn[owner]
	ch := f.block
	f.mu.Unlock()
	if ch != nil {
		<-ch // wait without holding the mutex
	}
	if fail {
		return assertErr
	}
	return nil
}

func (f *fakeRecaller) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

var assertErr = errInfo("recall failed")

type errInfo string

func (e errInfo) Error() string { return string(e) }

type ArbiterSuite struct {
	suite.Suite
	clock time.Time
}

func TestArbiterSuite(t *testing.T) { suite.Run(t, new(ArbiterSuite)) }

func (s *ArbiterSuite) now() time.Time { return s.clock }

func (s *ArbiterSuite) newArbiter(r Recaller) *Arbiter {
	s.clock = time.Unix(1000, 0)
	return NewArbiter(r, Config{
		Cooldown: cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
	}, s.now)
}

func (s *ArbiterSuite) TestGrantThenForeignMutationRecalls() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	g := a.Request("sessA", "proj")
	s.Equal("proj", g.GrantedRoot)

	// B mutates inside A's subtree -> A recalled, A's grant dropped.
	s.NoError(a.OnMutation("sessB", "proj/file"))
	s.Equal([]string{"sessA:proj"}, fr.calls)

	// A's delegation is gone now; B mutating again must NOT recall (no owner).
	fr.calls = nil
	s.NoError(a.OnMutation("sessB", "proj/file"))
	s.Empty(fr.calls)
}

func (s *ArbiterSuite) TestSelfMutationNeverRecalls() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	s.NoError(a.OnMutation("sessA", "proj/file")) // own subtree
	s.Empty(fr.calls)
}

func (s *ArbiterSuite) TestCooldownBlocksImmediateRegrant() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	s.NoError(a.OnMutation("sessB", "proj/file")) // recall + trip cooldown on "proj"
	// A re-requests immediately -> denied (cooling).
	g := a.Request("sessA", "proj")
	s.Equal("", g.GrantedRoot)
	s.Greater(g.RetryAfterMs, uint64(0))
}

func (s *ArbiterSuite) TestReleaseSessionFreesSubtree() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	a.ReleaseSession("sessA")
	// No owner now -> B's mutation recalls nothing; B can take it.
	s.NoError(a.OnMutation("sessB", "proj/x"))
	s.Empty(fr.calls)
	g := a.Request("sessB", "proj")
	s.Equal("proj", g.GrantedRoot)
}

func (s *ArbiterSuite) TestRecallFailurePropagates() {
	fr := &fakeRecaller{failOn: map[string]bool{"sessA": true}}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	s.Error(a.OnMutation("sessB", "proj/file")) // handler maps to FS_EAGAIN
}

// fakeMetrics records delegation.Metrics calls for assertions.
type fakeMetrics struct {
	grantsActive []int
	recalls      int
	cooldowns    int
}

func (f *fakeMetrics) GrantsActiveSet(n int) { f.grantsActive = append(f.grantsActive, n) }
func (f *fakeMetrics) RecallInc()            { f.recalls++ }
func (f *fakeMetrics) CooldownTripInc()      { f.cooldowns++ }

func (s *ArbiterSuite) TestMetricsWiredOnGrantAndRecall() {
	fr := &fakeRecaller{}
	fm := &fakeMetrics{}
	s.clock = time.Unix(1000, 0)
	a := NewArbiter(fr, Config{
		Cooldown: cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
		Metrics:  fm,
	}, s.now)

	// Grant to sessA: should emit GrantsActiveSet(1).
	g := a.Request("sessA", "proj")
	s.Equal("proj", g.GrantedRoot)
	s.Equal([]int{1}, fm.grantsActive, "grant must emit GrantsActiveSet(1)")

	// Foreign mutation from sessB: recall fires, then RecallInc, CooldownTripInc,
	// and GrantsActiveSet(0) (table is now empty after release).
	s.NoError(a.OnMutation("sessB", "proj/file"))
	s.Equal(1, fm.recalls, "exactly one recall")
	s.Equal(1, fm.cooldowns, "exactly one cooldown trip")
	s.Equal([]int{1, 0}, fm.grantsActive, "GrantsActiveSet must end at 0 after recall")
}

func (s *ArbiterSuite) TestMetricsReleaseSession() {
	fr := &fakeRecaller{}
	fm := &fakeMetrics{}
	s.clock = time.Unix(1000, 0)
	a := NewArbiter(fr, Config{
		Cooldown: cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
		Metrics:  fm,
	}, s.now)

	a.Request("sessA", "proj")
	fm.grantsActive = nil // reset after grant observation
	a.ReleaseSession("sessA")
	s.Equal([]int{0}, fm.grantsActive, "ReleaseSession must emit GrantsActiveSet(0)")
}

func (s *ArbiterSuite) TestConcurrentContendersCoalesce() {
	fr := &fakeRecaller{block: make(chan struct{})}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// Goroutine #1: triggers the recall; blocks inside Recall on fr.block.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = a.OnMutation("sessB", "proj/x")
	}()

	// Wait until the recall is in-flight (exactly one call recorded).
	s.Eventually(func() bool {
		return fr.callCount() == 1
	}, time.Second, time.Millisecond)

	// Goroutine #2: contends on the same root while recall #1 is still blocked.
	// It must coalesce onto the in-flight recall (wait on done) and NOT fire a second recall.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = a.OnMutation("sessB", "proj/x")
	}()

	// Give goroutine #2 a moment to reach the coalesce branch, then unblock recall #1.
	time.Sleep(10 * time.Millisecond)
	close(fr.block)

	wg.Wait()

	s.NoError(errs[0])
	s.NoError(errs[1])
	s.Equal(1, fr.callCount(), "exactly one recall must fire despite two concurrent contenders")
}

// TestConcurrentContendersCoalesceFailure verifies that when the leader's recall
// FAILS, every coalesced waiter also returns a non-nil error — they must NOT
// return nil (safe to proceed) while the root is still foreign-owned.
// This is the C3 coherence fix: prevent coalesced waiters from mutating a
// still-delegated subtree.
func (s *ArbiterSuite) TestConcurrentContendersCoalesceFailure() {
	block := make(chan struct{})
	fr := &fakeRecaller{
		failOn: map[string]bool{"sessA": true},
		block:  block,
	}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")

	var wg sync.WaitGroup
	errs := make([]error, 2)

	// Goroutine #1: triggers the recall; the recaller blocks then fails.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[0] = a.OnMutation("sessB", "proj/x")
	}()

	// Wait until the recall is in-flight.
	s.Eventually(func() bool {
		return fr.callCount() == 1
	}, time.Second, time.Millisecond)

	// Goroutine #2: coalesces onto the in-flight (failing) recall.
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs[1] = a.OnMutation("sessB", "proj/x")
	}()

	// Give goroutine #2 a moment to enter the coalesce branch, then let the
	// recall fail.
	time.Sleep(10 * time.Millisecond)
	close(block)

	wg.Wait()

	// BOTH goroutines must return non-nil: leader because recall failed;
	// coalesced waiter because the root is still foreign-owned after the failure.
	s.Error(errs[0], "leader must return error on recall failure")
	s.Error(errs[1], "coalesced waiter must also return error when recall failed")
	s.Equal(1, fr.callCount(), "exactly one recall must fire despite two concurrent contenders")
}
