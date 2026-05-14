package api

import (
	"context"
	"testing"

	"gmountie/pkg/proto"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionE2ETestSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
}

func (s *SessionE2ETestSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(testAppCtx.Start())
	// Trigger the client-side handshake so the session is established.
	testAppCtx.GetClient().Connect()
	s.testAppCtx = testAppCtx
}

func (s *SessionE2ETestSuite) TearDownSuite() {
	s.Require().NoError(s.testAppCtx.Close())
}

func (s *SessionE2ETestSuite) TestCreateReturnsID() {
	s.Require().NotEmpty(s.testAppCtx.GetClient().SessionID(),
		"Connect() must have triggered the SessionService.Create handshake and populated SessionID")
}

func (s *SessionE2ETestSuite) TestFileOpenWithoutSessionIDFails() {
	volume := s.testAppCtx.GetVolumes()[0]
	_, err := s.testAppCtx.GetClient().File().Open(context.Background(), &proto.OpenRequest{
		Volume: volume.Name,
		Path:   "/somefile",
		Flags:  0,
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}, Pid: 0},
		// SessionId intentionally empty.
	})
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok, "error should be a gRPC status")
	s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *SessionE2ETestSuite) TestFileOpenWithSessionIDSucceeds() {
	volume := s.testAppCtx.GetVolumes()[0]
	sid := s.testAppCtx.GetClient().SessionID()
	s.Require().NotEmpty(sid)

	reply, err := s.testAppCtx.GetClient().File().Open(context.Background(), &proto.OpenRequest{
		Volume:    volume.Name,
		Path:      "/",
		Flags:     0,
		Caller:    &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}, Pid: 0},
		SessionId: sid,
	})
	// We only assert the session-resolution layer didn't reject this; the
	// FUSE status code may vary depending on the volume's filesystem state.
	s.Require().NoError(err, "Open with a valid session_id should not produce a transport error")
	_ = reply
}

func TestSessionE2ETestSuite(t *testing.T) {
	suite.Run(t, new(SessionE2ETestSuite))
}
