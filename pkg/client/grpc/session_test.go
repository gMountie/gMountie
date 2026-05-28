package grpc

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	mockProto "gmountie/internal/mocks/pkg/proto"
	"gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type SessionHandshakeTestSuite struct {
	suite.Suite
	sessionClient *mockProto.MockSessionServiceClient
}

func (s *SessionHandshakeTestSuite) SetupTest() {
	s.sessionClient = mockProto.NewMockSessionServiceClient(s.T())
}

// newParkingKeepaliveStream returns a Keepalive stream stub whose Recv blocks
// until the stream's own context is cancelled, then returns that context's
// error. keepaliveLoop calls Keepalive(streamCtx, …) then Recv on the returned
// stream, so binding the stream to streamCtx parks the loop there: it spins no
// further recover cycle and exits the instant Close() cancels streamCtx. That
// keeps mock expectations exact and the suite deterministic under -race (no
// unexpected calls fired during teardown, no ungated state transitions). The
// returned bind func must be invoked with the ctx from the Keepalive
// expectation's RunAndReturn.
func newParkingKeepaliveStream(t *testing.T) (stream *mockProto.MockSessionService_KeepaliveClient, bind func(context.Context)) {
	stream = mockProto.NewMockSessionService_KeepaliveClient(t)
	ctxCh := make(chan context.Context, 1)
	stream.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
		ctx := <-ctxCh
		<-ctx.Done()
		return nil, ctx.Err()
	}).Maybe()
	return stream, func(ctx context.Context) { ctxCh <- ctx }
}

// newGatedErrorKeepaliveStream returns a Keepalive stream stub whose Recv
// blocks until release is closed, then returns err. Gating the stream break
// lets a test assert the pre-recovery session state with the loop parked,
// removing the race between Establish returning and the recovery goroutine
// mutating that state.
func newGatedErrorKeepaliveStream(t *testing.T, release <-chan struct{}, err error) *mockProto.MockSessionService_KeepaliveClient {
	stream := mockProto.NewMockSessionService_KeepaliveClient(t)
	stream.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
		<-release
		return nil, err
	}).Once()
	return stream
}

func (s *SessionHandshakeTestSuite) TestEstablishCallsCreateAndStartsKeepalive() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()

	// The keepalive stream parks on streamCtx and exits cleanly on Close —
	// no EOF that would otherwise drive an (unmocked) recover cycle.
	stream, bind := newParkingKeepaliveStream(s.T())
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
		return req.SessionId == "abc-123"
	})).RunAndReturn(func(ctx context.Context, _ *proto.KeepaliveRequest, _ ...grpc.CallOption) (proto.SessionService_KeepaliveClient, error) {
		bind(ctx)
		return stream, nil
	}).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	err := handshake.Establish(context.Background())
	s.Require().NoError(err)
	s.Assert().Equal("abc-123", handshake.SessionID())

	s.Require().NoError(handshake.Close())
	s.Require().Eventually(func() bool {
		return !handshake.IsRunning()
	}, time.Second, 10*time.Millisecond)
}

func (s *SessionHandshakeTestSuite) TestEstablishReturnsErrorWhenCreateFails() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(nil, errors.New("network")).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	err := handshake.Establish(context.Background())
	s.Require().Error(err)
	s.Assert().Empty(handshake.SessionID())
}

func (s *SessionHandshakeTestSuite) TestEstablishKeepaliveFailureLeavesCloseSafe() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(nil, errors.New("network")).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	err := handshake.Establish(context.Background())
	s.Require().Error(err)

	// Close must not deadlock even though Establish failed mid-way.
	done := make(chan struct{})
	go func() {
		_ = handshake.Close()
		close(done)
	}()
	s.Require().Eventually(func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond, "Close should not block when Establish failed")
}

func (s *SessionHandshakeTestSuite) TestKeepaliveStreamErrorTriggersResume() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()

	// First stream breaks only once the test releases it (so the pre-recovery
	// assertion runs with the loop parked); the second parks until Close.
	release := make(chan struct{})
	stream1 := newGatedErrorKeepaliveStream(s.T(), release, status.Error(codes.Unavailable, "transient"))
	stream2, bind2 := newParkingKeepaliveStream(s.T())

	// On the stream break the loop resumes (abc-123) — Resume succeeds, so the
	// session id is unchanged.
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.MatchedBy(func(req *proto.SessionResumeRequest) bool {
		return req.SessionId == "abc-123"
	})).Return(&proto.SessionResumeReply{Resumed: true}, nil).Once()

	// First Keepalive (Establish) → stream1; second (recover) → stream2.
	reopened := make(chan struct{})
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream1, nil).Once()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *proto.KeepaliveRequest, _ ...grpc.CallOption) (proto.SessionService_KeepaliveClient, error) {
			bind2(ctx)
			close(reopened)
			return stream2, nil
		}).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	s.Require().NoError(handshake.Establish(context.Background()))
	// Loop parked in stream1.Recv (release not yet closed): deterministic state.
	s.Require().Equal("abc-123", handshake.SessionID())
	s.Require().True(handshake.IsRunning())

	close(release) // let stream1 break → Resume → reopen
	select {
	case <-reopened:
	case <-time.After(2 * time.Second):
		s.FailNow("recovery did not reopen the Keepalive stream")
	}
	s.Require().Equal("abc-123", handshake.SessionID()) // Resume kept the id

	s.Require().NoError(handshake.Close())
	s.Require().Eventually(func() bool { return !handshake.IsRunning() }, time.Second, 5*time.Millisecond)
}

func (s *SessionHandshakeTestSuite) TestKeepaliveResumeFailureFallsBackToCreate() {
	// Initial Create → first id.
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()

	// First stream breaks only once released; second parks until Close.
	release := make(chan struct{})
	stream1 := newGatedErrorKeepaliveStream(s.T(), release, status.Error(codes.Unavailable, "transient"))
	stream2, bind2 := newParkingKeepaliveStream(s.T())

	// Resume reports the session reaped → loop falls back to a fresh Create
	// with a NEW id.
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
		Return(&proto.SessionResumeReply{Resumed: false}, nil).Once()
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "xyz-789"}, nil).Once()

	reopened := make(chan struct{})
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream1, nil).Once()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
		return req.SessionId == "xyz-789"
	})).RunAndReturn(func(ctx context.Context, _ *proto.KeepaliveRequest, _ ...grpc.CallOption) (proto.SessionService_KeepaliveClient, error) {
		bind2(ctx)
		close(reopened)
		return stream2, nil
	}).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	s.Require().NoError(handshake.Establish(context.Background()))
	// Loop parked in stream1.Recv: the pre-recovery id is observable, no race.
	s.Require().Equal("abc-123", handshake.SessionID())

	close(release) // let stream1 break → Resume(false) → Create(xyz-789) → reopen
	select {
	case <-reopened:
	case <-time.After(2 * time.Second):
		s.FailNow("recovery did not reopen the Keepalive stream after fallback Create")
	}
	// setSessionID(xyz-789) happens before the reopened Keepalive call, so this
	// is deterministic once reopened fires.
	s.Require().Equal("xyz-789", handshake.SessionID())

	s.Require().NoError(handshake.Close())
	s.Require().Eventually(func() bool { return !handshake.IsRunning() }, time.Second, 5*time.Millisecond)
}

func (s *SessionHandshakeTestSuite) TestCloseInterruptsRecovery() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()
	// First stream errors immediately.
	stream1 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
	stream1.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	// Resume always fails — drives the loop into backoff.
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "still down")).Maybe()
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "still down")).Maybe()

	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream1, nil).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	s.Require().NoError(handshake.Establish(context.Background()))

	// Give recovery a moment to enter its backoff loop.
	time.Sleep(50 * time.Millisecond)

	// Close must return promptly even while the loop is mid-backoff.
	done := make(chan struct{})
	go func() {
		_ = handshake.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.FailNow("Close did not return promptly under recovery backoff")
	}
}

func (s *SessionHandshakeTestSuite) TestEstablishKeepaliveFailureClearsSessionID() {
	// Create succeeds.
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "first-id"}, nil).Once()
	// Keepalive fails.
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(nil, errors.New("network")).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	err := handshake.Establish(context.Background())
	s.Require().Error(err)
	s.Assert().Empty(handshake.SessionID(),
		"SessionID must be cleared so a retry of Establish runs the full handshake")
}

func TestSessionHandshakeTestSuite(t *testing.T) {
	suite.Run(t, new(SessionHandshakeTestSuite))
}

// ---------------------------------------------------------------------------
// ClientImpl.WhoAmI
// ---------------------------------------------------------------------------

type ClientImplWhoAmITestSuite struct {
	suite.Suite
	sessionClient *mockProto.MockSessionServiceClient
	client        *ClientImpl
}

func (s *ClientImplWhoAmITestSuite) SetupTest() {
	s.sessionClient = mockProto.NewMockSessionServiceClient(s.T())
	s.client = &ClientImpl{session: s.sessionClient}
}

func (s *ClientImplWhoAmITestSuite) TestWhoAmIReturnsIdentityFromServer() {
	want := &proto.Identity{Uid: 1001, PrimaryGid: 1001, Gids: []uint32{1001}}

	s.sessionClient.EXPECT().
		WhoAmI(mock.Anything, mock.MatchedBy(func(req *proto.WhoAmIRequest) bool {
			return req.Volume == "v" &&
				req.Caller != nil &&
				req.Caller.Owner != nil &&
				req.Caller.Owner.Uid == uint32(os.Getuid()) &&
				req.Caller.Owner.Gid == uint32(os.Getgid())
		})).
		Return(want, nil).Once()

	got, err := s.client.WhoAmI(context.Background(), "v")
	s.Require().NoError(err)
	s.Assert().Equal(want, got)
}

func (s *ClientImplWhoAmITestSuite) TestWhoAmIPropagatesServerError() {
	s.sessionClient.EXPECT().
		WhoAmI(mock.Anything, mock.MatchedBy(func(req *proto.WhoAmIRequest) bool {
			return req.Volume == "myvol" && req.Caller != nil
		})).
		Return(nil, errors.New("server unavailable")).Once()

	got, err := s.client.WhoAmI(context.Background(), "myvol")
	s.Require().Error(err)
	s.Assert().Nil(got)
}

func TestClientImplWhoAmITestSuite(t *testing.T) {
	suite.Run(t, new(ClientImplWhoAmITestSuite))
}
