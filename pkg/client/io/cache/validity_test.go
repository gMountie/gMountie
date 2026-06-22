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
	v.markPathVerified("a", v.currentEpoch())
	s.Assert().True(v.isPathVerified("a"))
	s.Assert().False(v.isPathVerified("b"))
}

func (s *ValidityTrackerSuite) TestPathFlagsClearedOnGlobalUnverified() {
	v := newValidityTracker()
	v.markPathVerified("a", v.currentEpoch())
	v.markGlobalUnverified()
	// After going unverified globally, per-path stamps from the previous
	// epoch must not survive — otherwise stale chunks could be served on
	// the next reconnect cycle.
	s.Assert().False(v.isPathVerified("a"))
}

// TestStaleStampDoesNotSurviveUnverify is the CQ-M2 regression: a stamp
// captured in epoch 0 must not remain authoritative after markGlobalUnverified
// advances the epoch. Deterministic — no goroutines.
func (s *ValidityTrackerSuite) TestStaleStampDoesNotSurviveUnverify() {
	v := newValidityTracker()
	v.markPathVerified("p", v.currentEpoch()) // stamp at epoch 0
	v.markGlobalUnverified()                  // epoch -> 1
	s.Assert().False(v.isPathVerified("p"),
		"a stamp from before the unverify flip must not stay authoritative")
}

// TestLateStampFromOldEpochNotAuthoritative models the lost-update race
// directly: a revalidation goroutine captured the epoch, then a
// markGlobalUnverified landed, then the goroutine finally stamped. Because the
// stamp carries the OLD captured epoch (not a fresh load), it must not become
// authoritative in the new epoch. (Under the pre-fix code the stamp survived
// because markPathVerified loaded no epoch and markGlobalUnverified's clear had
// already run before this late Store.)
func (s *ValidityTrackerSuite) TestLateStampFromOldEpochNotAuthoritative() {
	v := newValidityTracker()
	captured := v.currentEpoch() // 0, as a FUSE-op goroutine would at revalidate start
	v.markGlobalUnverified()     // epoch -> 1 while the goroutine is mid-flight
	v.markPathVerified("p", captured)
	s.Assert().False(v.isPathVerified("p"),
		"a late stamp carrying a stale captured epoch must not be authoritative")
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
