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
}

func (f *fakeRecaller) Recall(owner, root string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, owner+":"+root)
	if f.failOn[owner] {
		return assertErr
	}
	return nil
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
		RecallTimeout: time.Second,
		Cooldown:      cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
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
