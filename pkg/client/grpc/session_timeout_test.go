package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// newClientWithMockSession builds a ClientImpl wired to a mock SessionServiceClient
// and the given metaTimeout, without dialling a real server. The underlying
// grpc.ClientConn is created lazily (grpc.NewClient never dials eagerly), so
// the test controls all session traffic through the mock. Call Close() when done.
func newClientWithMockSession(t *testing.T, sessionClient proto.SessionServiceClient, metaTimeout time.Duration) *ClientImpl {
	t.Helper()
	c, err := NewClient(
		"passthrough:///test-unreachable",
		WithDialOptions([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}),
		WithTimeouts(metaTimeout, 30*time.Second),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	impl := c.(*ClientImpl)
	// Inject the mock — replaces both the session proto client and the handshake.
	impl.session = sessionClient
	impl.handshake = NewSessionHandshake(sessionClient)
	return impl
}

// SessionTimeoutSuite verifies that Connect bounds session establishment so a
// slow/unresponsive server cannot hang the caller indefinitely.
type SessionTimeoutSuite struct {
	suite.Suite
	sessionClient *mockProto.MockSessionServiceClient
}

func (s *SessionTimeoutSuite) SetupTest() {
	s.sessionClient = mockProto.NewMockSessionServiceClient(s.T())
}

// TestConnectTimesOutOnUnresponsiveServer asserts that ClientImpl.Connect()
// returns an error when the server's Create RPC never responds, within a
// bounded window well short of the test timeout.
//
// This is the primary regression test for the "gmountie mount hangs forever on
// a half-open server" bug: Connect must bound the Establish context so callers
// are never stuck regardless of server responsiveness.
func (s *SessionTimeoutSuite) TestConnectTimesOutOnUnresponsiveServer() {
	// Create blocks until the context passed to it is cancelled, simulating a
	// server that accepts the TCP connection but never answers SessionService/Create.
	s.sessionClient.EXPECT().
		Create(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *proto.SessionCreateRequest, _ ...grpc.CallOption) (*proto.SessionCreateReply, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}).Once()

	const shortMeta = 50 * time.Millisecond
	c := newClientWithMockSession(s.T(), s.sessionClient, shortMeta)
	defer func() { _ = c.Close() }()

	start := time.Now()
	err := c.Connect()
	elapsed := time.Since(start)

	s.Require().Error(err, "Connect must fail when server never responds")
	// 3×metaTimeout = 150 ms; allow 5× headroom for scheduling variance.
	s.Assert().Less(elapsed, 5*3*shortMeta,
		"Connect must return within a bounded window, not hang indefinitely")
}

// TestConnectDeadlinePropagatesToCreate asserts that the context Connect passes
// to Establish (and hence to Create) carries a deadline ≤ 3×metaTimeout.
// This directly verifies the protective property: setting metaTimeout=T
// guarantees Create completes within 3T.
func (s *SessionTimeoutSuite) TestConnectDeadlinePropagatesToCreate() {
	const shortMeta = 100 * time.Millisecond

	// Capture the deadline seen by Create, then abort so Connect returns.
	deadlineCh := make(chan time.Time, 1)
	s.sessionClient.EXPECT().
		Create(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, _ *proto.SessionCreateRequest, _ ...grpc.CallOption) (*proto.SessionCreateReply, error) {
			dl, ok := ctx.Deadline()
			if ok {
				deadlineCh <- dl
			} else {
				deadlineCh <- time.Time{} // zero = no deadline set
			}
			return nil, errors.New("deadline probe: aborting")
		}).Once()

	before := time.Now()
	c := newClientWithMockSession(s.T(), s.sessionClient, shortMeta)
	defer func() { _ = c.Close() }()
	_ = c.Connect()

	deadline := <-deadlineCh
	s.Require().False(deadline.IsZero(),
		"context passed to Create must carry a deadline")
	// 3×metaTimeout budget from the time Connect was called; small scheduling
	// slack (50 ms) covers the time between before and the actual WithTimeout call.
	maxDeadline := before.Add(3*shortMeta + 50*time.Millisecond)
	s.Assert().True(deadline.Before(maxDeadline),
		"Create deadline must be ≤ 3×metaTimeout; got %v, max %v", deadline, maxDeadline)
}

// TestKeepaliveStreamSurvivesBoundedEstablishContext is the regression
// assertion for the key interaction property: Connect bounds the context for
// the unary Create call, but Establish opens the Keepalive stream on its own
// long-lived streamCtx (a WithCancel(Background()) context). The keepalive
// stream must therefore survive past the 3×metaTimeout window.
func (s *SessionTimeoutSuite) TestKeepaliveStreamSurvivesBoundedEstablishContext() {
	s.sessionClient.EXPECT().Create(mock.Anything, mock.Anything).
		Return(&proto.SessionCreateReply{SessionId: "ka-test-123"}, nil).Once()

	stream, bind := newParkingKeepaliveStream(s.T())
	s.sessionClient.EXPECT().
		Keepalive(mock.Anything, mock.MatchedBy(func(req *proto.KeepaliveRequest) bool {
			return req.SessionId == "ka-test-123"
		})).
		RunAndReturn(func(ctx context.Context, _ *proto.KeepaliveRequest, _ ...grpc.CallOption) (proto.SessionService_KeepaliveClient, error) {
			bind(ctx)
			return stream, nil
		}).Once()

	const shortMeta = 20 * time.Millisecond
	c := newClientWithMockSession(s.T(), s.sessionClient, shortMeta)
	defer func() { _ = c.Close() }()

	s.Require().NoError(c.Connect())

	// Sleep well past 3×metaTimeout; the keepalive stream must still be alive.
	time.Sleep(5 * 3 * shortMeta)

	s.Assert().True(c.handshake.IsHealthy(),
		"keepalive stream must remain healthy after the establishment deadline elapses")
	s.Assert().Equal("ka-test-123", c.SessionID())
}

func TestSessionTimeoutSuite(t *testing.T) {
	suite.Run(t, new(SessionTimeoutSuite))
}
