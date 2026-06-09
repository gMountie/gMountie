package service

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/suite"
)

type ReapSuite struct{ suite.Suite }

func TestReapSuite(t *testing.T) { suite.Run(t, new(ReapSuite)) }

func (s *ReapSuite) newManager() *sessionManagerImpl {
	return NewSessionManager(SessionManagerOptions{}).(*sessionManagerImpl)
}

// countingFile counts Release() calls so a test can assert a session's fds are
// released exactly once even when multiple reap paths could race.
type countingFile struct {
	nodefs.File
	releases *atomic.Int32
}

func (f *countingFile) Release() { f.releases.Add(1) }

func (s *ReapSuite) TestReapIf_BlockedSerialReaped() {
	m := s.newManager()
	keepID, _ := m.Create("alice", "1111")
	reapID, _ := m.Create("alice", "dead") // same principal, revoked device

	n := m.ReapIf(func(_ string, serial string) bool { return serial == "dead" })
	s.Equal(1, n)
	_, err := m.Get(reapID)
	s.Require().Error(err) // reaped
	_, err = m.Get(keepID)
	s.Require().NoError(err) // other device survives
}

func (s *ReapSuite) TestReapIf_AdditiveReapsNothing() {
	m := s.newManager()
	a, _ := m.Create("alice", "1111")
	b, _ := m.Create("bob", "2222")
	// Predicate that never matches (an additive reload: nothing revoked).
	n := m.ReapIf(func(string, string) bool { return false })
	s.Equal(0, n)
	_, errA := m.Get(a)
	_, errB := m.Get(b)
	s.NoError(errA)
	s.NoError(errB)
}

func (s *ReapSuite) TestSessionExposesSerial() {
	m := s.newManager()
	id, _ := m.Create("alice", "abcd")
	sess, err := m.Get(id)
	s.Require().NoError(err)
	s.Equal("abcd", sess.Serial())
}

// TestReapIf_AfterMarkDisconnected_ReleasesOnceAndCancelsReaper covers the
// interaction between ReapIf and the grace reaper: when a session already has a
// pending grace-reaper, ReapIf must release its fds exactly once (no
// double-close) and cancel the pending reaper so its goroutine doesn't linger.
func (s *ReapSuite) TestReapIf_AfterMarkDisconnected_ReleasesOnceAndCancelsReaper() {
	// Long grace so the grace goroutine can't fire on its own during the test —
	// ReapIf is the only thing that reaps.
	m := NewSessionManager(SessionManagerOptions{GracePeriod: time.Hour}).(*sessionManagerImpl)
	id, _ := m.Create("alice", "dead")
	sess, err := m.Get(id)
	s.Require().NoError(err)
	var releases atomic.Int32
	sess.RegisterFile("vol", "/p", &countingFile{File: nodefs.NewDefaultFile(), releases: &releases})

	m.MarkDisconnected(id) // schedules a grace reaper (won't fire for an hour)
	n := m.ReapIf(func(_, serial string) bool { return serial == "dead" })

	s.Equal(1, n)
	s.EqualValues(1, releases.Load()) // released exactly once, not double-closed
	_, err = m.Get(id)
	s.Require().Error(err) // session gone

	// ReapIf cancelled the pending grace reaper, so Stop returns promptly
	// instead of blocking on the (hour-long) grace goroutine.
	done := make(chan struct{})
	go func() { _ = m.Stop(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.FailNow("Stop blocked — ReapIf did not cancel the pending grace reaper")
	}
}
