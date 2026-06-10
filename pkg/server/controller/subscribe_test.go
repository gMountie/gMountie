package controller

import (
	"context"
	"testing"
	"time"

	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/service"

	pathfs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

// stubSubscribeStream implements proto.RpcFs_SubscribeServer for tests.
type stubSubscribeStream struct {
	ctx  context.Context
	mu   chan struct{}
	sent []*proto.SubscribeEvent
}

func newStubSubscribeStream(ctx context.Context) *stubSubscribeStream {
	return &stubSubscribeStream{ctx: ctx, mu: make(chan struct{}, 1)}
}

func (s *stubSubscribeStream) Send(ev *proto.SubscribeEvent) error {
	s.mu <- struct{}{}
	s.sent = append(s.sent, ev)
	<-s.mu
	return nil
}
func (s *stubSubscribeStream) Context() context.Context     { return s.ctx }
func (s *stubSubscribeStream) SetHeader(metadata.MD) error  { return nil }
func (s *stubSubscribeStream) SendHeader(metadata.MD) error { return nil }
func (s *stubSubscribeStream) SetTrailer(metadata.MD)       {}
func (s *stubSubscribeStream) SendMsg(any) error            { return nil }
func (s *stubSubscribeStream) RecvMsg(any) error            { return nil }

// newRpcServerWithBus builds an RpcServerImpl backed by the given bus and a
// mock VolumeService that accepts any volume name by returning a mock
// filesystem whose Access always returns OK. Helper for tests that don't
// care about the per-path filter; tests that exercise the filter should
// construct their own server with explicit Access expectations.
func newRpcServerWithBus(t *testing.T, bus serverio.EventBus) *RpcServerImpl {
	t.Helper()
	fsService := new(mockservice.MockVolumeService)
	mockFs := new(pathfs2.MockFileSystem)
	fsService.On("BindIdentity", mock.Anything, "vol-test", mock.Anything).Return(mockFs, service.Identity{}, nil).Maybe()
	fsService.On("BindIdentity", mock.Anything, "v", mock.Anything).Return(mockFs, service.Identity{}, nil).Maybe()
	// Access always OK — this helper is for tests that don't care about filtering.
	mockFs.EXPECT().Access(mock.Anything, mock.Anything, mock.Anything).Return(fuse.OK).Maybe()
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{})
	return NewGrpcServer(fsService, sessionMgr, bus, nil)
}

type SubscribeStreamSuite struct {
	suite.Suite
}

// newSignallingBus builds a bus whose OnSubscribe hook signals registration,
// plus a wait func tests call to deterministically join the Subscribe
// goroutine's registration instead of sleeping (TEST-7).
func (s *SubscribeStreamSuite) newSignallingBus() (serverio.EventBus, func()) {
	registered := make(chan struct{}, 4)
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{
		BufferSize:  16,
		OnSubscribe: func(string) { registered <- struct{}{} },
	})
	wait := func() {
		select {
		case <-registered:
		case <-time.After(2 * time.Second):
			s.FailNow("Subscribe stream never registered with the bus")
		}
	}
	return bus, wait
}

// joinSubscribe cancels the stream ctx and joins the Subscribe goroutine.
func (s *SubscribeStreamSuite) joinSubscribe(cancel context.CancelFunc, done <-chan error) {
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		s.FailNow("Subscribe didn't return after cancel")
	}
}

func (s *SubscribeStreamSuite) TestEmittedEventsReachStream() {
	bus, waitRegistered := s.newSignallingBus()
	defer bus.Close()
	srv := newRpcServerWithBus(s.T(), bus)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)

	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()

	waitRegistered() // deterministic: the subscriber IS registered before Emit
	bus.Emit("vol-test", "p1", 42, serverio.KindMutated)

	s.Require().Eventually(func() bool {
		for _, ev := range snapshotSent(stream) {
			if ev.Path == "p1" && ev.Kind == proto.SubscribeEvent_MUTATED && ev.NewVersion == 42 {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "stream did not see the emitted event")
	s.joinSubscribe(cancel, done)
}

func (s *SubscribeStreamSuite) TestCtxCancelTearsDownStream() {
	bus, waitRegistered := s.newSignallingBus()
	defer bus.Close()
	srv := newRpcServerWithBus(s.T(), bus)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)

	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "v"}, stream) }()
	waitRegistered()
	cancel()
	select {
	case err := <-done:
		s.Require().Error(err) // ctx.Err() returned
	case <-time.After(time.Second):
		s.FailNow("Subscribe didn't return after ctx cancel")
	}
}

// newRpcServerWithAccessFilter builds an RpcServerImpl with explicit Access
// expectations: allowedPaths return OK, denied return EACCES. This is the
// per-path authorization filter under test (spec §11 follow-up).
func newRpcServerWithAccessFilter(t *testing.T, bus serverio.EventBus, allowedPaths map[string]bool) *RpcServerImpl {
	t.Helper()
	fsService := new(mockservice.MockVolumeService)
	mockFs := new(pathfs2.MockFileSystem)
	fsService.On("BindIdentity", mock.Anything, "vol-test", mock.Anything).Return(mockFs, service.Identity{}, nil).Maybe()
	mockFs.EXPECT().Access(mock.MatchedBy(func(p string) bool { return allowedPaths[p] }),
		mock.Anything, mock.Anything).Return(fuse.OK).Maybe()
	mockFs.EXPECT().Access(mock.MatchedBy(func(p string) bool { return !allowedPaths[p] }),
		mock.Anything, mock.Anything).Return(fuse.EACCES).Maybe()
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{})
	return NewGrpcServer(fsService, sessionMgr, bus, nil)
}

// snapshotSent returns the events the stub received without racing the
// Subscribe goroutine still in flight.
func snapshotSent(stream *stubSubscribeStream) []*proto.SubscribeEvent {
	stream.mu <- struct{}{}
	defer func() { <-stream.mu }()
	out := make([]*proto.SubscribeEvent, len(stream.sent))
	copy(out, stream.sent)
	return out
}

func (s *SubscribeStreamSuite) TestDeniedPathIsFiltered() {
	bus, waitRegistered := s.newSignallingBus()
	defer bus.Close()
	srv := newRpcServerWithAccessFilter(s.T(), bus, map[string]bool{"/public": true})

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()
	waitRegistered()

	// /secret first, /public second: per-subscriber channel ordering means
	// that once /public is observed, /secret has definitely been processed
	// (and filtered) — no fixed drain needed.
	bus.Emit("vol-test", "/secret", 1, serverio.KindMutated)
	bus.Emit("vol-test", "/public", 2, serverio.KindMutated)

	s.Require().Eventually(func() bool {
		for _, ev := range snapshotSent(stream) {
			if ev.Path == "/public" {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "allowed-path event must reach subscriber")

	for _, ev := range snapshotSent(stream) {
		s.NotEqual("/secret", ev.Path, "denied-path event must not reach subscriber")
	}
	s.joinSubscribe(cancel, done)
}

func (s *SubscribeStreamSuite) TestHeartbeatBypassesFilter() {
	// Heartbeats carry no path; the filter must always forward them.
	bus, waitRegistered := s.newSignallingBus()
	defer bus.Close()
	srv := newRpcServerWithAccessFilter(s.T(), bus, map[string]bool{}) // nothing allowed

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()
	waitRegistered()

	bus.Emit("vol-test", "", 0, serverio.KindHeartbeat)

	s.Require().Eventually(func() bool {
		for _, ev := range snapshotSent(stream) {
			if ev.Kind == proto.SubscribeEvent_HEARTBEAT {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond,
		"heartbeat must always reach subscriber even when no paths are allowed")
	s.joinSubscribe(cancel, done)
}

func (s *SubscribeStreamSuite) TestRenameRequiresBothPathsAccessible() {
	// A rename where the subscriber can see only one side reveals that a
	// rename happened between two paths it shouldn't know about. Skip.
	bus, waitRegistered := s.newSignallingBus()
	defer bus.Close()
	srv := newRpcServerWithAccessFilter(s.T(), bus, map[string]bool{"/visible": true})

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()
	waitRegistered()

	// Half-visible rename: only the new path is allowed. The trailing
	// heartbeat is a sentinel — heartbeats always pass the filter and the
	// per-subscriber channel preserves order, so once it arrives the rename
	// has definitely been processed (and filtered).
	bus.EmitRename("vol-test", "/hidden", "/visible", 1)
	bus.Emit("vol-test", "", 0, serverio.KindHeartbeat)

	s.Require().Eventually(func() bool {
		for _, ev := range snapshotSent(stream) {
			if ev.Kind == proto.SubscribeEvent_HEARTBEAT {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond, "sentinel heartbeat never arrived")

	for _, ev := range snapshotSent(stream) {
		s.NotEqual(proto.SubscribeEvent_RENAMED, ev.Kind,
			"rename with one inaccessible side must be filtered, not leaked")
	}
	s.joinSubscribe(cancel, done)
}

func TestSubscribeStreamSuite(t *testing.T) { suite.Run(t, new(SubscribeStreamSuite)) }
