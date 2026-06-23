package controller

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/delegation"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubRecallStream is a scriptable bidi stream for the Recall controller tests.
// It feeds a pre-loaded queue of RecallAcks to Recv() and captures RecallMsgs
// pushed via Send(). When the queue is drained, Recv() blocks until the
// stream's context is cancelled, then returns io.EOF.
type stubRecallStream struct {
	ctx context.Context

	mu      sync.Mutex
	recvQ   []*proto.RecallAck // pre-loaded acks for Recv to return
	sent    []*proto.RecallMsg  // msgs pushed by Send
	recvCh  chan *proto.RecallAck
	sendErr error // injected Send error, if any
}

func newStubRecallStream(ctx context.Context, acks ...*proto.RecallAck) *stubRecallStream {
	ch := make(chan *proto.RecallAck, len(acks)+1)
	for _, a := range acks {
		ch <- a
	}
	return &stubRecallStream{ctx: ctx, recvCh: ch}
}

// closeRecv drains the ack channel, causing subsequent Recv calls to block
// until context cancellation. Safe to call multiple times.
func (s *stubRecallStream) closeRecv() {
	// Draining only; don't close (to avoid a panic on double-close). The
	// blocking select on ctx.Done() handles the terminal case.
}

func (s *stubRecallStream) Send(msg *proto.RecallMsg) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, msg)
	return nil
}

func (s *stubRecallStream) Recv() (*proto.RecallAck, error) {
	select {
	case ack, ok := <-s.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return ack, nil
	case <-s.ctx.Done():
		return nil, io.EOF
	}
}

func (s *stubRecallStream) Context() context.Context     { return s.ctx }
func (s *stubRecallStream) SetHeader(metadata.MD) error  { return nil }
func (s *stubRecallStream) SendHeader(metadata.MD) error { return nil }
func (s *stubRecallStream) SetTrailer(metadata.MD)       {}
func (s *stubRecallStream) SendMsg(any) error            { return nil }
func (s *stubRecallStream) RecvMsg(any) error            { return nil }

// RecallStreamSuite tests the Recall bidi-stream controller.
type RecallStreamSuite struct {
	suite.Suite
}

func TestRecallStreamSuite(t *testing.T) {
	suite.Run(t, new(RecallStreamSuite))
}

// newRecallServer builds an RpcServerImpl with a real RecallRegistry and a
// stub bus.
func (s *RecallStreamSuite) newRecallServer() (*RpcServerImpl, *delegation.RecallRegistry, service.SessionManager) {
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.T().Cleanup(func() { bus.Close() })
	reg := delegation.NewRecallRegistry(2 * time.Second)
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{})
	arbiter := delegation.NewArbiter(reg, delegation.Config{
		Cooldown: delegation.CooldownConfigDefault(),
	}, time.Now)
	srv := NewGrpcServer(nil, sessionMgr, bus, nil, arbiter, reg)
	return srv, reg, sessionMgr
}

// ctxWithSession builds a context that carries a session_id in gRPC incoming
// metadata, as the auth interceptor stamps on every post-handshake stream.
// Uses common.MetadataSessionID ("session-id") — the same key sessionIDFromContext reads.
func ctxWithSession(ctx context.Context, sessionID string) context.Context {
	md := metadata.Pairs(common.MetadataSessionID, sessionID)
	return metadata.NewIncomingContext(ctx, md)
}

// TestRecall_NoSession returns Unauthenticated when the stream context has no
// session_id.
func (s *RecallStreamSuite) TestRecall_NoSession() {
	srv, _, _ := s.newRecallServer()
	stream := newStubRecallStream(context.Background())
	err := srv.Recall(stream)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Equal(codes.Unauthenticated, st.Code())
}

// TestRecall_RegistersAndDeregisters verifies that:
//   - After Recall() starts, the registry can send a recall to the session.
//   - When the stream context is cancelled (simulating disconnect), Recall()
//     returns and the registry can no longer send.
func (s *RecallStreamSuite) TestRecall_RegistersAndDeregisters() {
	srv, reg, sessionMgr := s.newRecallServer()

	streamCtx, cancelStream := context.WithCancel(context.Background())

	// Create a REAL session (empty principal → ownership check is a no-op).
	sessionID, err := sessionMgr.Create("", "")
	s.Require().NoError(err)

	// Use a dynamically-fed ack channel: when the registry pushes a RecallMsg
	// via stream.Send, a watcher goroutine injects the matching RecallAck back
	// into recvCh so the controller can forward it to registry.Ack.
	ackQ := make(chan *proto.RecallAck, 4)
	stream := newStubRecallStream(ctxWithSession(streamCtx, sessionID))
	stream.recvCh = ackQ

	// Run the controller.
	controllerDone := make(chan error, 1)
	go func() { controllerDone <- srv.Recall(stream) }()

	// A watcher that auto-acks whatever RecallMsg the registry sends.
	go func() {
		for {
			stream.mu.Lock()
			n := len(stream.sent)
			stream.mu.Unlock()
			if n > 0 {
				stream.mu.Lock()
				msg := stream.sent[n-1]
				stream.mu.Unlock()
				ackQ <- &proto.RecallAck{RecallId: msg.RecallId}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Fire reg.Recall in a goroutine; it will block until an ack arrives.
	recallResult := make(chan error, 1)
	go func() {
		time.Sleep(30 * time.Millisecond) // let controller register first
		recallResult <- reg.Recall(sessionID, "recall-test-root")
	}()

	select {
	case err := <-recallResult:
		s.NoError(err, "recall after registration should succeed")
	case <-time.After(3 * time.Second):
		s.FailNow("reg.Recall did not complete within 3s")
	}

	// Cancel the stream — the controller should exit.
	cancelStream()
	select {
	case err := <-controllerDone:
		// EOF or context cancel — both are acceptable exits.
		s.True(err == nil || err == io.EOF ||
			status.Code(err) == codes.Canceled ||
			err.Error() == "EOF",
			"unexpected error: %v", err)
	case <-time.After(2 * time.Second):
		s.FailNow("Recall() did not return after stream context cancelled")
	}

	// After the controller exits, the registry should no longer have a stream.
	postErr := reg.Recall(sessionID, "post-deregister-root")
	s.Error(postErr, "registry should reject recall after deregistration")
}

// TestRecall_AckForwardsToRegistry verifies that a RecallAck received from the
// client stream is forwarded to registry.Ack so a concurrent registry.Recall()
// call unblocks successfully (without timing out).
//
// Flow: controller starts and registers → registry.Recall() sends RecallMsg
// (id=1) → stubRecallStream.Send receives it and notes the id → test feeds
// RecallAck{RecallId: 1} into the stub recv channel → controller forwards it
// to registry.Ack(sessionID, 1) → registry.Recall() unblocks with nil error.
func (s *RecallStreamSuite) TestRecall_AckForwardsToRegistry() {
	srv, reg, sessionMgr := s.newRecallServer()

	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()

	// Create a REAL session (empty principal → ownership check is a no-op).
	sessionID, err := sessionMgr.Create("", "")
	s.Require().NoError(err)

	ackQ := make(chan *proto.RecallAck, 4)
	stream := newStubRecallStream(ctxWithSession(streamCtx, sessionID))
	// Redirect acks so the test can inject them dynamically.
	stream.recvCh = ackQ

	// Start the controller.
	go func() { _ = srv.Recall(stream) }()

	// Allow the controller goroutine to register.
	time.Sleep(30 * time.Millisecond)

	// Fire reg.Recall in a goroutine; it will block until an ack arrives.
	recallDone := make(chan error, 1)
	go func() { recallDone <- reg.Recall(sessionID, "ack-test-root") }()

	// Allow reg.Recall to send the RecallMsg; then inject the matching ack.
	// The registry assigns id=1 for the first Recall call.
	time.Sleep(30 * time.Millisecond)
	ackQ <- &proto.RecallAck{RecallId: 1}

	select {
	case err := <-recallDone:
		s.NoError(err, "registry.Recall should complete without error when ack arrives")
	case <-time.After(3 * time.Second):
		s.FailNow("registry.Recall did not complete after ack was injected")
	}
}

// TestRecall_SessionOwnershipEnforced verifies that a Recall stream opened with
// a session_id whose ctx principal does NOT match the session owner is rejected
// (resolveSession returns PermissionDenied) and the stream is never registered.
// This is the C2 security fix: prevents principal B from hijacking A's recall
// stream by opening it with A's session_id.
func (s *RecallStreamSuite) TestRecall_SessionOwnershipEnforced() {
	srv, reg, sessionMgr := s.newRecallServer()

	// Create a session owned by "alice".
	aliceSessionID, err := sessionMgr.Create("alice", "")
	s.Require().NoError(err)

	// Build a stream context carrying alice's session_id but with principal "bob".
	streamCtx := principal.WithPrincipal(
		ctxWithSession(context.Background(), aliceSessionID),
		"bob",
	)

	// Bob tries to open a Recall stream under alice's session_id.
	stream := newStubRecallStream(streamCtx)
	recallErr := srv.Recall(stream)

	// Must be rejected with PermissionDenied.
	s.Require().Error(recallErr)
	st, ok := status.FromError(recallErr)
	s.Require().True(ok)
	s.Equal(codes.PermissionDenied, st.Code(), "foreign principal must not register under another session")

	// Confirm the registry never got a registration: sending a recall to
	// aliceSessionID must fail immediately (no stream registered).
	regErr := reg.Recall(aliceSessionID, "some-root")
	s.Error(regErr, "registry must have no registration for alice's session after the rejected stream")

	// Positive control: alice herself can open the stream.
	aliceAckQ := make(chan *proto.RecallAck, 4)
	aliceCtx, cancelAlice := context.WithCancel(
		principal.WithPrincipal(ctxWithSession(context.Background(), aliceSessionID), "alice"),
	)
	defer cancelAlice()
	aliceStream := newStubRecallStream(aliceCtx)
	aliceStream.recvCh = aliceAckQ
	controllerDone := make(chan error, 1)
	go func() { controllerDone <- srv.Recall(aliceStream) }()

	// Auto-ack watcher: as soon as the controller pushes a RecallMsg, inject the
	// matching ack so the registry.Recall call can complete.
	go func() {
		for {
			aliceStream.mu.Lock()
			n := len(aliceStream.sent)
			aliceStream.mu.Unlock()
			if n > 0 {
				aliceStream.mu.Lock()
				msg := aliceStream.sent[n-1]
				aliceStream.mu.Unlock()
				aliceAckQ <- &proto.RecallAck{RecallId: msg.RecallId}
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Give controller a moment to register, then fire the recall.
	time.Sleep(30 * time.Millisecond)
	recallDone := make(chan error, 1)
	go func() { recallDone <- reg.Recall(aliceSessionID, "alice-root") }()
	select {
	case rerr := <-recallDone:
		s.NoError(rerr, "alice's legitimate recall stream must work")
	case <-time.After(3 * time.Second):
		s.FailNow("alice's recall did not complete in time")
	}
	cancelAlice()
	<-controllerDone
}
