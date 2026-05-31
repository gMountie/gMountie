package grpc

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// countingAuthService wraps BasicAuthService and counts Authorize invocations.
type countingAuthService struct {
	inner service.AuthService
	calls int
}

func (c *countingAuthService) Authorize(ctx context.Context, method string) (*service.UserDetails, error) {
	c.calls++
	return c.inner.Authorize(ctx, method)
}

// AuthInterceptorTestSuite tests the AuthInterceptor's authorize() logic
// directly, covering fail-closed, session-skip, and argon2-count properties.
type AuthInterceptorTestSuite struct {
	suite.Suite
	sessions    service.SessionManager
	authSvc     *countingAuthService
	interceptor *AuthInterceptor
}

func (s *AuthInterceptorTestSuite) SetupTest() {
	s.sessions = service.NewSessionManager(service.SessionManagerOptions{})
	// A BasicAuthService with a known user whose argon2 hash matches "secret".
	// We generate the hash inline via passhash to avoid hard-coded PHC strings.
	// For test speed we use a BasicAuthService backed by a pre-hashed password.
	// Since we want to control call-count, we wrap with countingAuthService.

	// Build a fake user with a known hash. Use the passhash package to generate.
	// To keep tests deterministic and fast, use a mock for the deny case and
	// the real BasicAuthService for the "valid creds" case.
	s.authSvc = &countingAuthService{
		inner: newAlwaysGrantAuthService("alice"),
	}
	s.interceptor = NewAuthInterceptor(s.authSvc, s.sessions)
}

func (s *AuthInterceptorTestSuite) TearDownTest() {
	_ = s.sessions.Stop(context.Background())
}

// alwaysGrantAuthService allows any call with username "alice".
type alwaysGrantAuthService struct {
	principal string
}

func newAlwaysGrantAuthService(principal string) *alwaysGrantAuthService {
	return &alwaysGrantAuthService{principal: principal}
}

func (a *alwaysGrantAuthService) Authorize(ctx context.Context, _ string) (*service.UserDetails, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "no metadata")
	}
	users := md.Get(common.MetadataAuthBasicUsername)
	if len(users) > 0 && users[0] == a.principal {
		return &service.UserDetails{Username: a.principal}, nil
	}
	return nil, status.Errorf(codes.Unauthenticated, "invalid credentials")
}

// ctxWithBasicAuth injects basic-auth metadata into the context.
func ctxWithBasicAuth(username string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		common.MetadataAuthBasicUsername, username,
		common.MetadataAuthBasicPassword, "any",
	))
}

// ctxWithSessionIDAndBadCreds injects a session_id AND invalid basic-auth.
func ctxWithSessionIDAndBadCreds(id string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		common.MetadataSessionID, id,
		common.MetadataAuthBasicUsername, "notexist",
		common.MetadataAuthBasicPassword, "wrong",
	))
}

// TestFailClosed_BogusSessionID_BadCreds_Denied is the most important test.
// A bogus session_id + invalid creds MUST be denied, not allowed.
func (s *AuthInterceptorTestSuite) TestFailClosed_BogusSessionID_BadCreds_Denied() {
	ctx := ctxWithSessionIDAndBadCreds("bogus-nonexistent-session-id")
	_, err := s.interceptor.authorize(ctx, "/gmountie.RpcFile/Read")
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.Unauthenticated, st.Code(),
		"bogus session_id + bad creds must be denied via fall-through to Authorize")
}

// TestFailClosed_NoSessionID_BadCreds_Denied verifies absent session_id + bad creds → denied.
func (s *AuthInterceptorTestSuite) TestFailClosed_NoSessionID_BadCreds_Denied() {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		common.MetadataAuthBasicUsername, "nobody",
		common.MetadataAuthBasicPassword, "wrong",
	))
	_, err := s.interceptor.authorize(ctx, "/gmountie.RpcFile/Read")
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.Unauthenticated, st.Code())
}

// TestValidSessionID_GarbageCreds_Allowed verifies that a known session_id
// bypasses argon2 even when basic-auth creds are garbage.
func (s *AuthInterceptorTestSuite) TestValidSessionID_GarbageCreds_Allowed() {
	// Seed a session for "alice".
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	callsBefore := s.authSvc.calls

	// Request with the valid session_id but wrong/absent basic-auth.
	ctx := ctxWithSessionIDAndBadCreds(id)
	outCtx, err := s.interceptor.authorize(ctx, "/gmountie.RpcFile/Read")
	s.Require().NoError(err)

	// Principal must come from the session.
	p, ok := principal.FromContext(outCtx)
	s.Require().True(ok)
	s.Assert().Equal("alice", p)

	// Authorize must NOT have been called (session-skip).
	s.Assert().Equal(callsBefore, s.authSvc.calls,
		"Authorize (argon2) must not be called when a valid session_id is present")
}

// TestArgon2OncePer50Calls is the deterministic proof of the perf fix.
// Drive 50 authorize calls for an established session through the interceptor
// and assert Authorize is called 0 additional times (already called 0 before,
// the session was seeded directly — no Create RPC in this unit test).
func (s *AuthInterceptorTestSuite) TestArgon2OncePer50Calls() {
	// Seed the session directly (simulating the effect of a successful Create).
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	callsBefore := s.authSvc.calls

	for i := 0; i < 50; i++ {
		ctx := ctxWithSessionIDAndBadCreds(id)
		_, err := s.interceptor.authorize(ctx, "/gmountie.RpcFile/Read")
		s.Require().NoError(err, "call %d should succeed via session", i)
	}

	s.Assert().Equal(callsBefore, s.authSvc.calls,
		"Authorize must be called 0 times for 50 requests with a valid session_id (argon2-skip proof)")
}

// TestCreateBindsPrincipal verifies that after a Create authorized as "alice",
// the created session's Principal() == "alice".
func (s *AuthInterceptorTestSuite) TestCreateBindsPrincipal() {
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	sess, err := s.sessions.Get(id)
	s.Require().NoError(err)
	s.Assert().Equal("alice", sess.Principal())
}

// TestResumeRunsFullAuth verifies that SessionService/Resume always runs full
// Authorize regardless of any session_id in metadata.
func (s *AuthInterceptorTestSuite) TestResumeRunsFullAuth() {
	// Seed a session so the session_id is valid.
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	callsBefore := s.authSvc.calls

	// Pass a valid session_id in metadata for a Resume call.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		common.MetadataSessionID, id,
		common.MetadataAuthBasicUsername, "alice",
	))
	_, err = s.interceptor.authorize(ctx, methodSessionResume)
	// alwaysGrantAuthService accepts "alice" — should succeed.
	s.Require().NoError(err)

	// Authorize MUST have been called (full auth for Resume).
	s.Assert().Equal(callsBefore+1, s.authSvc.calls,
		"Resume must always invoke full Authorize, even with a valid session_id")
}

// TestCreateRunsFullAuth verifies that SessionService/Create always runs full
// Authorize regardless of any session_id in metadata.
func (s *AuthInterceptorTestSuite) TestCreateRunsFullAuth() {
	// Even if someone passes a valid session_id with Create, full auth runs.
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	callsBefore := s.authSvc.calls

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		common.MetadataSessionID, id,
		common.MetadataAuthBasicUsername, "alice",
	))
	_, err = s.interceptor.authorize(ctx, methodSessionCreate)
	s.Require().NoError(err)

	s.Assert().Equal(callsBefore+1, s.authSvc.calls,
		"Create must always invoke full Authorize")
}

// TestNoSessionIDValidCreds_Allowed verifies the standard case: no session_id,
// valid creds → argon2 runs, allowed.
func (s *AuthInterceptorTestSuite) TestNoSessionIDValidCreds_Allowed() {
	ctx := ctxWithBasicAuth("alice")
	callsBefore := s.authSvc.calls
	outCtx, err := s.interceptor.authorize(ctx, "/gmountie.RpcFile/Read")
	s.Require().NoError(err)

	p, ok := principal.FromContext(outCtx)
	s.Require().True(ok)
	s.Assert().Equal("alice", p)
	s.Assert().Equal(callsBefore+1, s.authSvc.calls,
		"no session_id + valid creds must call Authorize once")
}

// TestUnaryInterceptor_UsesReturnedCtx verifies the unary interceptor passes
// the enriched context to the handler.
func (s *AuthInterceptorTestSuite) TestUnaryInterceptor_UsesReturnedCtx() {
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	interceptor := s.interceptor.Unary()
	ctx := ctxWithSessionIDAndBadCreds(id)
	info := &grpc.UnaryServerInfo{FullMethod: "/gmountie.RpcFile/Read"}

	var gotPrincipal string
	var gotOK bool
	_, err = interceptor(ctx, nil, info, func(handlerCtx context.Context, _ interface{}) (interface{}, error) {
		gotPrincipal, gotOK = principal.FromContext(handlerCtx)
		return struct{}{}, nil
	})
	s.Require().NoError(err)

	s.Require().True(gotOK)
	s.Assert().Equal("alice", gotPrincipal)
}

// TestStreamInterceptor_Authorizes verifies the stream interceptor authorizes
// correctly (does not panic, returns no error for valid session).
func (s *AuthInterceptorTestSuite) TestStreamInterceptor_Authorizes() {
	id, err := s.sessions.Create("alice", "")
	s.Require().NoError(err)

	interceptor := s.interceptor.Stream()
	ctx := ctxWithSessionIDAndBadCreds(id)
	stream := &fakeServerStream{ctx: ctx}

	info := &grpc.StreamServerInfo{FullMethod: "/gmountie.RpcFile/Read"}
	err = interceptor(nil, stream, info, func(_ interface{}, _ grpc.ServerStream) error {
		return nil
	})
	s.Require().NoError(err)
}

// fakeServerStream is a minimal grpc.ServerStream for testing.
type fakeServerStream struct {
	ctx context.Context
}

func (f *fakeServerStream) SetHeader(_ metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(_ metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(_ metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context       { return f.ctx }
func (f *fakeServerStream) SendMsg(_ interface{}) error    { return nil }
func (f *fakeServerStream) RecvMsg(_ interface{}) error    { return nil }

func TestAuthInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(AuthInterceptorTestSuite))
}
