package service

import (
	"context"
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
	id1, err := s.mgr.Create()
	s.Require().NoError(err)
	s.Require().NotEmpty(id1)

	id2, err := s.mgr.Create()
	s.Require().NoError(err)
	s.Assert().NotEqual(id1, id2)
}

func (s *SessionManagerTestSuite) TestGetReturnsTheSession() {
	id, err := s.mgr.Create()
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
	id, err := s.mgr.Create()
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
	id, err := s.mgr.Create()
	s.Require().NoError(err)
	sess, _ := s.mgr.Get(id)
	fd := sess.RegisterFile("/p", nodefs.NewDefaultFile())

	sess.ReleaseFile(fd)
	_, ok := sess.GetFile(fd)
	s.Assert().False(ok)
}

func (s *SessionManagerTestSuite) TearDownTest() {
	_ = s.mgr.Stop(context.Background())
}

func TestSessionManagerTestSuite(t *testing.T) {
	suite.Run(t, new(SessionManagerTestSuite))
}
