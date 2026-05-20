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
// mock VolumeService that accepts any volume name by returning a mock filesystem.
func newRpcServerWithBus(t *testing.T, bus serverio.EventBus) *RpcServerImpl {
	t.Helper()
	fsService := new(mockservice.MockVolumeService)
	mockFs := new(pathfs2.MockFileSystem)
	// Accept any volume name — Subscribe only calls GetVolumeFileSystem to gate access.
	fsService.On("GetVolumeFileSystem", "vol-test").Return(mockFs, nil).Maybe()
	fsService.On("GetVolumeFileSystem", "v").Return(mockFs, nil).Maybe()
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

func TestSubscribeStreamSuite(t *testing.T) { suite.Run(t, new(SubscribeStreamSuite)) }
