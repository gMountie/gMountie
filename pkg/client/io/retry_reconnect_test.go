package io

// retry_reconnect_test.go asserts the protective property that streaming
// Read and Write pass grpc.WaitForReady(true) to their stream-open calls.
//
// Without WaitForReady the stream-open returns an instant Unavailable when
// the channel is CONNECTING (e.g. after a server restart), burning the
// entire retryOp window in sub-millisecond failed attempts instead of
// parking until the channel reaches READY.
//
// Harness: option (b) — mock-based. RunAndReturn captures the opts slice
// so we can assert the option is present without building a bufconn harness.

import (
	"context"
	stdio "io"
	"testing"

	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
)

// waitForReadyOpt is the concrete call-option value produced by
// grpc.WaitForReady(true). Declaring it once lets the assertions below
// stay readable without inlining the concrete type.
var waitForReadyOpt = grpc.WaitForReady(true)

// RetryReconnectTestSuite tests that streaming ops carry WaitForReady(true).
// It embeds BackendClientTestSuite to reuse its setup / handle helpers.
type RetryReconnectTestSuite struct {
	BackendClientTestSuite
}

// TestRead_PassesWaitForReady asserts that BackendClient.Read opens the
// server-streaming Read RPC with grpc.WaitForReady(true). Without it the
// stream-open fast-fails on a CONNECTING channel and burns the retry window.
func (s *RetryReconnectTestSuite) TestRead_PassesWaitForReady() {
	var capturedOpts []grpc.CallOption

	stream := mockProto.NewMockRpcFile_ReadClient(s.T())
	stream.EXPECT().Recv().Return(&proto.ReadFrame{
		Data:   []byte("hello"),
		Status: int32(fuse.OK),
	}, nil).Once()
	stream.EXPECT().Recv().Return(nil, stdio.EOF).Once()

	// Register with the WaitForReady option so the variadic spread matches:
	// the mock's Called() receives (ctx, req, opt) as three separate args.
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, waitForReadyOpt).
		RunAndReturn(func(_ context.Context, _ *proto.ReadRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			capturedOpts = opts
			return stream, nil
		})

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 8)
	_, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)

	s.Require().NotEmpty(capturedOpts, "Read must pass at least one CallOption")
	s.Assert().Contains(capturedOpts, waitForReadyOpt,
		"Read stream-open must carry WaitForReady(true) so it parks on a CONNECTING channel")
}

// TestWrite_PassesWaitForReady asserts that streamingWrite opens the
// client-streaming Write RPC with grpc.WaitForReady(true).
func (s *RetryReconnectTestSuite) TestWrite_PassesWaitForReady() {
	var capturedOpts []grpc.CallOption

	writeStub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{
		Written: 5,
		Status:  int32(fuse.OK),
	}, nil)

	// Register with the WaitForReady option so the variadic spread matches:
	// the mock's Called() receives (ctx, opt) as two separate args.
	s.fileClient.EXPECT().Write(mock.Anything, waitForReadyOpt).
		RunAndReturn(func(_ context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[proto.WriteFrame, proto.WriteReply], error) {
			capturedOpts = opts
			return writeStub, nil
		})

	h := s.newHandle(grpcclient.PerFileConfig{})
	_, st := s.backend.Write(context.Background(), h, 0, []byte("hello"))
	s.Require().Equal(fuse.OK, st)

	s.Require().NotEmpty(capturedOpts, "Write must pass at least one CallOption")
	s.Assert().Contains(capturedOpts, waitForReadyOpt,
		"Write stream-open must carry WaitForReady(true) so it parks on a CONNECTING channel")
}

func TestRetryReconnectTestSuite(t *testing.T) {
	suite.Run(t, new(RetryReconnectTestSuite))
}
