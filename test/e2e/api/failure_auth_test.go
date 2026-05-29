package api

import (
	"context"
	"testing"

	"gmountie/pkg/proto"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// AuthFailureSuite proves that the server cleanly rejects bad credentials at
// the session-handshake boundary (the Create RPC) and that correct credentials
// continue to work. All cases run in-process via bufconn; no FUSE mount is
// needed. The server's BasicAuthService returns codes.Unauthenticated for an
// unknown user or wrong password; that status propagates through the auth
// interceptor and causes the Create RPC to fail, leaving the client without a
// session ID. NewClientAs detects the empty session ID and returns a plain
// sentinel error ("NewClientAs: session handshake failed").
type AuthFailureSuite struct {
	suite.Suite
	ctx        *utils.AppTestingContext
	volumeName string
}

func (s *AuthFailureSuite) SetupSuite() {
	appCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "correct"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err, "build test app context")
	s.Require().NoError(appCtx.Start(), "start server")
	s.ctx = appCtx
	s.volumeName = s.ctx.GetVolumes()[0].Name
}

func (s *AuthFailureSuite) TearDownSuite() {
	if s.ctx != nil {
		s.Require().NoError(s.ctx.Close())
	}
}

// TestWrongPasswordRejected: correct username but wrong password must fail the
// handshake. NewClientAs returns a non-nil error and a nil client.
func (s *AuthFailureSuite) TestWrongPasswordRejected() {
	cli, err := s.ctx.NewClientAs("test", "wrong")
	s.Require().Error(err, "NewClientAs must fail with wrong password")
	s.Require().Nil(cli, "returned client must be nil on handshake failure")
}

// TestUnknownUserRejected: a principal not registered in the server config must
// also fail the handshake. The server returns codes.Unauthenticated
// ("invalid user or password") for an unknown user so that the response is
// indistinguishable from a wrong-password attempt (no user enumeration).
func (s *AuthFailureSuite) TestUnknownUserRejected() {
	cli, err := s.ctx.NewClientAs("ghost", "x")
	s.Require().Error(err, "NewClientAs must fail for an unknown user")
	s.Require().Nil(cli, "returned client must be nil on handshake failure")
}

// TestCorrectPasswordStillWorks: positive control. The configured principal
// must complete the handshake and be able to issue an authenticated RPC.
// This guards against a regression where both correct and incorrect credentials
// are rejected (e.g. a misconfigured auth service).
func (s *AuthFailureSuite) TestCorrectPasswordStillWorks() {
	cli, err := s.ctx.NewClientAs("test", "correct")
	s.Require().NoError(err, "NewClientAs must succeed with correct credentials")
	s.Require().NotNil(cli, "returned client must not be nil on success")
	defer func() { _ = cli.Close() }()

	// Issue a real authenticated RPC to prove the session is usable, not just
	// that the handshake returned a non-empty session ID.
	reply, err := cli.Fs().GetAttr(context.Background(), &proto.GetAttrRequest{
		Volume: s.volumeName,
		Path:   "/",
	})
	s.Require().NoError(err, "GetAttr must succeed with valid session")
	s.Require().NotNil(reply, "GetAttr reply must not be nil")
}

func TestAuthFailureSuite(t *testing.T) {
	suite.Run(t, new(AuthFailureSuite))
}
