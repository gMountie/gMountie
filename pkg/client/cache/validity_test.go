package cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ValidityTrackerSuite struct{ suite.Suite }

func (s *ValidityTrackerSuite) TestDefaultIsUnverified() {
	v := newValidityTracker()
	s.Assert().Equal(stateUnverified, v.globalState())
}

func (s *ValidityTrackerSuite) TestMarkVerifiedTransitions() {
	v := newValidityTracker()
	v.markGlobalVerified()
	s.Assert().Equal(stateVerified, v.globalState())
}

func (s *ValidityTrackerSuite) TestMarkUnverifiedTransitions() {
	v := newValidityTracker()
	v.markGlobalVerified()
	v.markGlobalUnverified()
	s.Assert().Equal(stateUnverified, v.globalState())
}

func (s *ValidityTrackerSuite) TestPerPathMarking() {
	v := newValidityTracker()
	s.Assert().False(v.isPathVerified("a"))
	v.markPathVerified("a")
	s.Assert().True(v.isPathVerified("a"))
	s.Assert().False(v.isPathVerified("b"))
}

func (s *ValidityTrackerSuite) TestPathFlagsClearedOnGlobalUnverified() {
	v := newValidityTracker()
	v.markPathVerified("a")
	v.markGlobalUnverified()
	// After going unverified globally, per-path stamps from the previous
	// epoch must not survive — otherwise stale chunks could be served on
	// the next reconnect cycle.
	s.Assert().False(v.isPathVerified("a"))
}

func (s *ValidityTrackerSuite) TestConcurrentFlipRace() {
	v := newValidityTracker()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); v.markGlobalVerified() }()
		go func() { defer wg.Done(); v.markGlobalUnverified() }()
	}
	wg.Wait()
	// No assertion; passing under -race is the test.
}

func TestValidityTrackerSuite(t *testing.T) { suite.Run(t, new(ValidityTrackerSuite)) }
