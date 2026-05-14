package grpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	mockProto "gmountie/internal/mocks/pkg/proto"
	"gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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

func TestSessionHandshakeTestSuite(t *testing.T) {
	suite.Run(t, new(SessionHandshakeTestSuite))
}
