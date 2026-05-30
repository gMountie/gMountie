package controller

import (
	"context"
	"testing"
	"time"

	mockservice "gmountie/internal/mocks/pkg/server/service"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/principal"
	"gmountie/pkg/server/service"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionControllerTestSuite struct {
	suite.Suite
	mgr        service.SessionManager
	volSvc     *mockservice.MockVolumeService
	controller *SessionController
}

func (s *SessionControllerTestSuite) SetupTest() {
	s.mgr = service.NewSessionManager(service.SessionManagerOptions{
		GracePeriod: 100 * time.Millisecond,
	})
	s.volSvc = mockservice.NewMockVolumeService(s.T())
	s.controller = NewSessionController(s.mgr, s.volSvc)
}

func (s *SessionControllerTestSuite) TearDownTest() {
	_ = s.mgr.Stop(context.Background())
}

func (s *SessionControllerTestSuite) TestCreateReturnsSessionID() {
	ctx := principal.WithPrincipal(context.Background(), "alice")
	reply, err := s.controller.Create(ctx, &proto.SessionCreateRequest{})
	s.Require().NoError(err)
	s.Assert().NotEmpty(reply.SessionId)
}

func (s *SessionControllerTestSuite) TestResumeKnownSession() {
	ctx := principal.WithPrincipal(context.Background(), "alice")
	createReply, err := s.controller.Create(ctx, &proto.SessionCreateRequest{})
	s.Require().NoError(err)

	s.mgr.MarkDisconnected(createReply.SessionId)

	// Resume as the owner (alice) — ownership is now enforced.
	resumeReply, err := s.controller.Resume(ctx,
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

// TestResumeForeignSessionDenied verifies a caller cannot resume a session
// owned by a different principal (the cross-user lifetime vector).
func (s *SessionControllerTestSuite) TestResumeForeignSessionDenied() {
	owner := principal.WithPrincipal(context.Background(), "alice")
	createReply, err := s.controller.Create(owner, &proto.SessionCreateRequest{})
	s.Require().NoError(err)
	s.mgr.MarkDisconnected(createReply.SessionId)

	// mallory tries to resume alice's session.
	attacker := principal.WithPrincipal(context.Background(), "mallory")
	_, err = s.controller.Resume(attacker, &proto.SessionResumeRequest{SessionId: createReply.SessionId})
	s.Require().Error(err)
	s.Equal(codes.PermissionDenied, status.Code(err))
}

func (s *SessionControllerTestSuite) TestWhoAmI() {
	s.volSvc.EXPECT().PrincipalCanAccess(mock.Anything, "v").Return(nil)
	s.volSvc.EXPECT().ResolveIdentity(mock.Anything, "v", mock.Anything).
		Return(service.Identity{
			Principal:  "alice",
			Uid:        1001,
			Gid:        1001,
			Gids:       []uint32{1001, 2000},
			UserName:   "alice",
			GroupNames: map[uint32]string{2000: "developers"},
		}, nil)
	ctx := principal.WithPrincipal(context.Background(), "alice")
	reply, err := s.controller.WhoAmI(ctx, &proto.WhoAmIRequest{Volume: "v"})
	s.Require().NoError(err)
	s.Equal("alice", reply.Principal)
	s.Equal(uint32(1001), reply.Uid)
	s.Equal(uint32(1001), reply.PrimaryGid)
	s.ElementsMatch([]uint32{1001, 2000}, reply.Gids)
	s.Equal("alice", reply.UserName)
	s.Equal("developers", reply.GroupNames[2000])
}

func TestSessionControllerTestSuite(t *testing.T) {
	suite.Run(t, new(SessionControllerTestSuite))
}
