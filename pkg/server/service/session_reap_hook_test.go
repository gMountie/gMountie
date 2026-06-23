package service

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// SessionManagerReapHookSuite is a separate suite so the new reap-hook tests
// don't bloat the existing SessionManagerTestSuite.
type SessionManagerReapHookSuite struct {
	suite.Suite
}

func TestSessionManagerReapHookSuite(t *testing.T) {
	suite.Run(t, new(SessionManagerReapHookSuite))
}

func (s *SessionManagerReapHookSuite) TestOnReapCalledOnGraceExpiry() {
	var mu sync.Mutex
	var reaped []string
	mgr := NewSessionManager(SessionManagerOptions{
		GracePeriod: 10 * time.Millisecond,
		OnReap:      func(id string) { mu.Lock(); reaped = append(reaped, id); mu.Unlock() },
	})
	id, _ := mgr.Create("alice", "")
	mgr.MarkDisconnected(id)
	s.Eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reaped) == 1 && reaped[0] == id
	}, time.Second, 5*time.Millisecond)
}

func (s *SessionManagerReapHookSuite) TestOnReapCalledByReapIf() {
	var mu sync.Mutex
	var reaped []string
	mgr := NewSessionManager(SessionManagerOptions{
		GracePeriod: 30 * time.Second, // long so timer never fires
		OnReap:      func(id string) { mu.Lock(); reaped = append(reaped, id); mu.Unlock() },
	})
	id, _ := mgr.Create("bob", "")
	n := mgr.ReapIf(func(principal, _ string) bool { return principal == "bob" })
	s.Equal(1, n)
	mu.Lock()
	defer mu.Unlock()
	s.Require().Len(reaped, 1)
	s.Equal(id, reaped[0])
}

func (s *SessionManagerReapHookSuite) TestOnReapCalledByStop() {
	var mu sync.Mutex
	var reaped []string
	mgr := NewSessionManager(SessionManagerOptions{
		GracePeriod: 30 * time.Second,
		OnReap:      func(id string) { mu.Lock(); reaped = append(reaped, id); mu.Unlock() },
	})
	id, _ := mgr.Create("carol", "")
	_ = mgr.Stop(s.T().Context())
	mu.Lock()
	defer mu.Unlock()
	s.Require().Len(reaped, 1)
	s.Equal(id, reaped[0])
}
