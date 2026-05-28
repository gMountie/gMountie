package grpc

import (
	"context"
	"errors"
	"io"
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

func (s *SessionHandshakeTestSuite) TestEstablishCallsCreateAndStartsKeepalive() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()

	stream := mockProto.NewMockSessionService_KeepaliveClient(s.T())
	// Recv blocks until the test signals; we end it with io.EOF.
	blockCh := make(chan struct{})
	stream.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
		<-blockCh
		return nil, io.EOF
	}).Maybe()
	stream.EXPECT().CloseSend().Return(nil).Maybe()

	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
		return req.SessionId == "abc-123"
	})).Return(stream, nil).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	err := handshake.Establish(context.Background())
	s.Require().NoError(err)
	s.Assert().Equal("abc-123", handshake.SessionID())

	// Close the handshake — the Recv goroutine unblocks.
	close(blockCh)
	s.Require().NoError(handshake.Close())

	// Give the background goroutine a moment to wind down.
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

	// First stream: emit one Recv that returns an error.
	stream1 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
	stream1.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	// Second stream: block forever until test closes the handshake.
	stream2 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
	block := make(chan struct{})
	stream2.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
		<-block
		return nil, io.EOF
	}).Maybe()

	// After stream1 errors, the handshake calls Resume(abc-123) — succeeds.
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.MatchedBy(func(req *proto.SessionResumeRequest) bool {
		return req.SessionId == "abc-123"
	})).Return(&proto.SessionResumeReply{Resumed: true}, nil).Once()

	// First Keepalive (during Establish) returns stream1; second (during recover) returns stream2.
	secondKeepalive := make(chan struct{})
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream1, nil).Once()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, req *proto.KeepaliveRequest, opts ...grpc.CallOption) (proto.SessionService_KeepaliveClient, error) {
			close(secondKeepalive)
			return stream2, nil
		}).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	s.Require().NoError(handshake.Establish(context.Background()))
	s.Require().Equal("abc-123", handshake.SessionID())

	// Wait for recovery to actually reopen the Keepalive stream.
	select {
	case <-secondKeepalive:
	case <-time.After(time.Second):
		s.FailNow("recovery did not reopen Keepalive stream")
	}
	s.Require().Equal("abc-123", handshake.SessionID())
	s.Require().True(handshake.IsRunning())

	// Close races the recovery loop: once we unblock Recv it returns EOF,
	// and depending on goroutine scheduling the loop may fire one more
	// Resume/Keepalive cycle before Close's streamCancel takes effect. The
	// unexpected mock call would invoke testify's reflective diagnostic,
	// which reads streamCtx at the same moment streamCancel writes it —
	// a -race finding from CI run #119. Pre-register permissive expectations
	// to absorb the race-window calls instead of asserting exact counts.
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
		Return(&proto.SessionResumeReply{Resumed: true}, nil).Maybe()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream2, nil).Maybe()

	close(block)
	s.Require().NoError(handshake.Close())
}

func (s *SessionHandshakeTestSuite) TestKeepaliveResumeFailureFallsBackToCreate() {
	// Initial Create returns the first id.
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "abc-123"}, nil).Once()
	// First stream errors.
	stream1 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
	stream1.EXPECT().Recv().Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	// Resume returns Resumed=false (server already reaped).
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
		Return(&proto.SessionResumeReply{Resumed: false}, nil).Once()
	// Second Create returns a NEW id.
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "xyz-789"}, nil).Once()
	// Second stream blocks forever.
	stream2 := mockProto.NewMockSessionService_KeepaliveClient(s.T())
	block := make(chan struct{})
	stream2.EXPECT().Recv().RunAndReturn(func() (*proto.KeepalivePing, error) {
		<-block
		return nil, io.EOF
	}).Maybe()

	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream1, nil).Once()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
		return req.SessionId == "xyz-789"
	})).Return(stream2, nil).Once()

	handshake := NewSessionHandshake(s.sessionClient)
	s.Require().NoError(handshake.Establish(context.Background()))
	s.Require().Equal("abc-123", handshake.SessionID())

	s.Require().Eventually(func() bool {
		return handshake.SessionID() == "xyz-789"
	}, time.Second, 10*time.Millisecond, "session id must update after fallback Create")

	// Same race window as TestKeepaliveStreamErrorTriggersResume: once
	// close(block) returns Recv from EOF, the loop may fire one more
	// Resume/Create/Keepalive cycle before streamCancel takes effect.
	// Pre-register permissive expectations so testify's reflective
	// diagnostic has nothing to inspect alongside the cancel.
	s.sessionClient.EXPECT().Resume(mock.Anything, mock.Anything).
		Return(&proto.SessionResumeReply{Resumed: true}, nil).Maybe()
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "xyz-789"}, nil).Maybe()
	s.sessionClient.EXPECT().Keepalive(mock.Anything, mock.Anything).
		Return(stream2, nil).Maybe()

	close(block)
	s.Require().NoError(handshake.Close())
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
