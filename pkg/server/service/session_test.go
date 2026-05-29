package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/suite"
)

type SessionManagerTestSuite struct {
	suite.Suite
	mgr SessionManager
}

func (s *SessionManagerTestSuite) SetupTest() {
	s.mgr = NewSessionManager(SessionManagerOptions{
		GracePeriod: 100 * time.Millisecond,
	})
}

func (s *SessionManagerTestSuite) TestCreateReturnsUniqueIDs() {
	id1, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	s.Require().NotEmpty(id1)

	id2, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	s.Assert().NotEqual(id1, id2)
}

func (s *SessionManagerTestSuite) TestGetReturnsTheSession() {
	id, err := s.mgr.Create("test-user")
	s.Require().NoError(err)

	sess, err := s.mgr.Get(id)
	s.Require().NoError(err)
	s.Assert().Equal(id, sess.ID())
}

func (s *SessionManagerTestSuite) TestGetUnknownSessionErrors() {
	_, err := s.mgr.Get("does-not-exist")
	s.Assert().Error(err)
}

func (s *SessionManagerTestSuite) TestSessionFdTableRegisterAndLookup() {
	id, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	sess, err := s.mgr.Get(id)
	s.Require().NoError(err)

	fd := sess.RegisterFile("/some/path", nodefs.NewDefaultFile())
	s.Assert().NotZero(fd)

	entry, ok := sess.GetFile(fd)
	s.Assert().True(ok)
	s.Assert().Equal("/some/path", entry.Path)
}

func (s *SessionManagerTestSuite) TestSessionReleaseFile() {
	id, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

	sess.ReleaseFile(fd)
	_, ok := sess.GetFile(fd)
	s.Assert().False(ok)
}

func (s *SessionManagerTestSuite) TestDisconnectThenGraceExpiryReapsFds() {
	id, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

	s.mgr.MarkDisconnected(id)

	// Wait well past the 100ms grace period configured in SetupTest.
	s.Require().Eventually(func() bool {
		_, err := s.mgr.Get(id)
		return err != nil
	}, 500*time.Millisecond, 10*time.Millisecond, "session should be reaped after grace period")

	_ = fd // fd entry is now unreachable through Get; that is the assertion.
}

func (s *SessionManagerTestSuite) TestResumeBeforeGraceCancelsReap() {
	id, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

	s.mgr.MarkDisconnected(id)

	// Resume immediately — well within the 100ms grace period.
	resumed, err := s.mgr.Resume(id)
	s.Require().NoError(err)
	s.Require().True(resumed)

	// Wait long enough that the original timer *would* have fired.
	time.Sleep(200 * time.Millisecond)

	sess2, err := s.mgr.Get(id)
	s.Require().NoError(err)
	entry, ok := sess2.GetFile(fd)
	s.Require().True(ok)
	s.Assert().Equal("/p", entry.Path)
}

func (s *SessionManagerTestSuite) TestResumeUnknownSessionReturnsFalse() {
	resumed, err := s.mgr.Resume("nope")
	s.Require().NoError(err)
	s.Assert().False(resumed)
}

func (s *SessionManagerTestSuite) TestStopReleasesAllFds() {
	id, err := s.mgr.Create("test-user")
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	_ = sess.RegisterFile("/p", nodefs.NewDefaultFile())

	err = s.mgr.Stop(context.Background())
	s.Require().NoError(err)

	_, err = s.mgr.Get(id)
	s.Assert().Error(err)
}

func (s *SessionManagerTestSuite) TestDoOnceCachesSuccessfulReply() {
	id, _ := s.mgr.Create("test-user")
	sess, _ := s.mgr.Get(id)

	calls := 0
	fn := func() (any, error) {
		calls++
		return "reply-1", nil
	}

	r1, err := sess.DoOnce("req-A", fn)
	s.Require().NoError(err)
	s.Assert().Equal("reply-1", r1)

	r2, err := sess.DoOnce("req-A", fn)
	s.Require().NoError(err)
	s.Assert().Equal("reply-1", r2)

	s.Assert().Equal(1, calls, "fn must execute only once for the same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceDoesNotCacheErrors() {
	id, _ := s.mgr.Create("test-user")
	sess, _ := s.mgr.Get(id)

	calls := 0
	fn := func() (any, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("first fails")
		}
		return "reply-ok", nil
	}

	_, err := sess.DoOnce("req-B", fn)
	s.Require().Error(err)

	r, err := sess.DoOnce("req-B", fn)
	s.Require().NoError(err)
	s.Assert().Equal("reply-ok", r)
	s.Assert().Equal(2, calls, "errored fn must re-execute on retry with same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceCollapsesConcurrentDuplicates() {
	id, _ := s.mgr.Create("test-user")
	sess, _ := s.mgr.Get(id)

	var mu sync.Mutex
	calls := 0
	block := make(chan struct{})
	fn := func() (any, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		<-block
		return "reply", nil
	}

	type result struct {
		v   any
		err error
	}
	results := make(chan result, 5)
	for i := 0; i < 5; i++ {
		go func() {
			v, err := sess.DoOnce("req-C", fn)
			results <- result{v, err}
		}()
	}

	// Give the goroutines a moment to all enter DoOnce and queue on singleflight.
	time.Sleep(50 * time.Millisecond)
	close(block)

	for i := 0; i < 5; i++ {
		r := <-results
		s.Require().NoError(r.err)
		s.Assert().Equal("reply", r.v)
	}

	mu.Lock()
	defer mu.Unlock()
	s.Assert().Equal(1, calls, "fn must run exactly once even with 5 concurrent callers using the same request_id")
}

func (s *SessionManagerTestSuite) TestDoOnceLRUEvictsOldEntries() {
	id, _ := s.mgr.Create("test-user")
	sess, _ := s.mgr.Get(id)

	// Saturate the LRU (256 entries) and verify the first one is gone.
	for i := 0; i < 300; i++ {
		reqID := fmt.Sprintf("req-%d", i)
		_, err := sess.DoOnce(reqID, func() (any, error) { return i, nil })
		s.Require().NoError(err)
	}

	// req-0 should be evicted by now; calling DoOnce with it re-executes.
	calls := 0
	_, err := sess.DoOnce("req-0", func() (any, error) {
		calls++
		return 999, nil
	})
	s.Require().NoError(err)
	s.Assert().Equal(1, calls, "evicted request_id must re-execute")
}

func (s *SessionManagerTestSuite) TearDownTest() {
	_ = s.mgr.Stop(context.Background())
}

func TestSessionManagerTestSuite(t *testing.T) {
	suite.Run(t, new(SessionManagerTestSuite))
}
