package controller

import (
	"context"
	"testing"
	"time"

	"gmountie/pkg/proto"
	"gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
)

type SessionControllerTestSuite struct {
	suite.Suite
	mgr        service.SessionManager
	controller *SessionController
}

func (s *SessionControllerTestSuite) SetupTest() {
	s.mgr = service.NewSessionManager(service.SessionManagerOptions{
		GracePeriod: 100 * time.Millisecond,
	})
	s.controller = NewSessionController(s.mgr)
}

func (s *SessionControllerTestSuite) TearDownTest() {
	_ = s.mgr.Stop(context.Background())
}

func (s *SessionControllerTestSuite) TestCreateReturnsSessionID() {
	reply, err := s.controller.Create(context.Background(), &proto.SessionCreateRequest{})
	s.Require().NoError(err)
	s.Assert().NotEmpty(reply.SessionId)
}

func (s *SessionControllerTestSuite) TestResumeKnownSession() {
	createReply, err := s.controller.Create(context.Background(), &proto.SessionCreateRequest{})
	s.Require().NoError(err)

	s.mgr.MarkDisconnected(createReply.SessionId)

	resumeReply, err := s.controller.Resume(context.Background(),
		&proto.SessionResumeRequest{SessionId: createReply.SessionId})
	s.Require().NoError(err)
	s.Assert().True(resumeReply.Resumed)
}

func (s *SessionControllerTestSuite) TestResumeUnknownSession() {
	reply, err := s.controller.Resume(context.Background(),
		&proto.SessionResumeRequest{SessionId: "ghost"})
	s.Require().NoError(err)
	s.Assert().False(reply.Resumed)
}

func TestSessionControllerTestSuite(t *testing.T) {
	suite.Run(t, new(SessionControllerTestSuite))
}
