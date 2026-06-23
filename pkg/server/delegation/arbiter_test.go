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
