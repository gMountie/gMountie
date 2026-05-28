package controller

import (
	"context"
	"testing"
	"time"

	mockservice "gmountie/internal/mocks/pkg/server/service"
	"gmountie/pkg/proto"
	serverio "gmountie/pkg/server/io"
	"gmountie/pkg/server/service"

	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

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
	fsService.On("BindIdentity", mock.Anything, "vol-test", mock.Anything).Return(mockFs, nil).Maybe()
	fsService.On("BindIdentity", mock.Anything, "v", mock.Anything).Return(mockFs, nil).Maybe()
	// Access always OK — this helper is for tests that don't care about filtering.
	mockFs.EXPECT().Access(mock.Anything, mock.Anything, mock.Anything).Return(fuse.OK).Maybe()
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{})
	return NewGrpcServer(fsService, sessionMgr, 0, bus, nil)
}

type SubscribeStreamSuite struct {
	suite.Suite
}

func (s *SubscribeStreamSuite) TestEmittedEventsReachStream() {
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	srv := newRpcServerWithBus(s.T(), bus)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)

	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()

	// Wait a tick to ensure the subscriber is registered before we Emit.
	time.Sleep(50 * time.Millisecond)
	bus.Emit("vol-test", "p1", 42, serverio.KindMutated)
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done

	// Snapshot sent under the stub's internal lock.
	stream.mu <- struct{}{}
	defer func() { <-stream.mu }()
	s.Require().GreaterOrEqual(len(stream.sent), 1)
	found := false
	for _, ev := range stream.sent {
		if ev.Path == "p1" && ev.Kind == proto.SubscribeEvent_MUTATED && ev.NewVersion == 42 {
			found = true
		}
	}
	s.Assert().True(found, "stream did not see the emitted event")
}

func (s *SubscribeStreamSuite) TestCtxCancelTearsDownStream() {
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	srv := newRpcServerWithBus(s.T(), bus)

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)

	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "v"}, stream) }()
	time.Sleep(50 * time.Millisecond)
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
	fsService.On("BindIdentity", mock.Anything, "vol-test", mock.Anything).Return(mockFs, nil).Maybe()
	mockFs.EXPECT().Access(mock.MatchedBy(func(p string) bool { return allowedPaths[p] }),
		mock.Anything, mock.Anything).Return(fuse.OK).Maybe()
	mockFs.EXPECT().Access(mock.MatchedBy(func(p string) bool { return !allowedPaths[p] }),
		mock.Anything, mock.Anything).Return(fuse.EACCES).Maybe()
	sessionMgr := service.NewSessionManager(service.SessionManagerOptions{})
	return NewGrpcServer(fsService, sessionMgr, 0, bus, nil)
}

// drainAndCancel emits, waits a tick for events to flow, then cancels and
// joins the Subscribe goroutine. Centralizes the per-test boilerplate.
func drainAndCancel(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Subscribe didn't return after cancel")
	}
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
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	srv := newRpcServerWithAccessFilter(s.T(), bus, map[string]bool{"/public": true})

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()
	time.Sleep(50 * time.Millisecond)

	bus.Emit("vol-test", "/secret", 1, serverio.KindMutated)
	bus.Emit("vol-test", "/public", 2, serverio.KindMutated)
	drainAndCancel(s.T(), cancel, done)

	sent := snapshotSent(stream)
	for _, ev := range sent {
		s.NotEqual("/secret", ev.Path, "denied-path event must not reach subscriber")
	}
	publicSeen := false
	for _, ev := range sent {
		if ev.Path == "/public" {
			publicSeen = true
		}
	}
	s.True(publicSeen, "allowed-path event must reach subscriber")
}

func (s *SubscribeStreamSuite) TestHeartbeatBypassesFilter() {
	// Heartbeats carry no path; the filter must always forward them.
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	srv := newRpcServerWithAccessFilter(s.T(), bus, map[string]bool{}) // nothing allowed

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()
	time.Sleep(50 * time.Millisecond)

	bus.Emit("vol-test", "", 0, serverio.KindHeartbeat)
	drainAndCancel(s.T(), cancel, done)

	sent := snapshotSent(stream)
	heartbeatSeen := false
	for _, ev := range sent {
		if ev.Kind == proto.SubscribeEvent_HEARTBEAT {
			heartbeatSeen = true
		}
	}
	s.True(heartbeatSeen, "heartbeat must always reach subscriber even when no paths are allowed")
}

func (s *SubscribeStreamSuite) TestRenameRequiresBothPathsAccessible() {
	// A rename where the subscriber can see only one side reveals that a
	// rename happened between two paths it shouldn't know about. Skip.
	bus := serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	defer bus.Close()
	srv := newRpcServerWithAccessFilter(s.T(), bus, map[string]bool{"/visible": true})

	ctx, cancel := context.WithCancel(context.Background())
	stream := newStubSubscribeStream(ctx)
	done := make(chan error, 1)
	go func() { done <- srv.Subscribe(&proto.SubscribeRequest{Volume: "vol-test"}, stream) }()
	time.Sleep(50 * time.Millisecond)

	// Half-visible rename: only the new path is allowed.
	bus.EmitRename("vol-test", "/hidden", "/visible", 1)
	drainAndCancel(s.T(), cancel, done)

	sent := snapshotSent(stream)
	for _, ev := range sent {
		s.NotEqual(proto.SubscribeEvent_RENAMED, ev.Kind,
			"rename with one inaccessible side must be filtered, not leaked")
	}
}

func TestSubscribeStreamSuite(t *testing.T) { suite.Run(t, new(SubscribeStreamSuite)) }
