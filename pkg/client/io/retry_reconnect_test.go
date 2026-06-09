package io

// retry_reconnect_test.go asserts the protective property that streaming
// Read and Write pass grpc.WaitForReady(true) to their stream-open calls.
//
// Without WaitForReady the stream-open returns an instant Unavailable when
// the channel is CONNECTING (e.g. after a server restart), burning the
// entire retryOp window in sub-millisecond failed attempts instead of
// parking until the channel reaches READY.
//
// Both methods live on BackendClientTestSuite so testify discovers them
// alongside the rest of the backend tests with no extra runner func.

import (
	"context"
	stdio "io"

	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

// waitForReadyOpt is the concrete call-option value produced by
// grpc.WaitForReady(true). Declaring it once lets the assertions below
// stay readable without inlining the concrete type.
var waitForReadyOpt = grpc.WaitForReady(true)

// TestRead_PassesWaitForReady asserts that BackendClient.Read opens the
// server-streaming Read RPC with grpc.WaitForReady(true). Without it the
// stream-open fast-fails on a CONNECTING channel and burns the retry window.
//
// The positional waitForReadyOpt matcher IS the assertion: testify fails the
// test immediately if the mock receives a call that does not match, so no
// additional Contains check is needed.
func (s *BackendClientTestSuite) TestRead_PassesWaitForReady() {
	stream := mockProto.NewMockRpcFile_ReadClient(s.T())
	stream.EXPECT().Recv().Return(&proto.ReadFrame{
		Data:   []byte("hello"),
		Status: int32(fuse.OK),
	}, nil).Once()
	stream.EXPECT().Recv().Return(nil, stdio.EOF).Once()

	// positional waitForReadyOpt: an unmatched call fails the test immediately.
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, waitForReadyOpt).
		Return(stream, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 8)
	_, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
}

// TestWrite_PassesWaitForReady asserts that streamingWrite opens the
// client-streaming Write RPC with grpc.WaitForReady(true).
//
// The positional waitForReadyOpt matcher IS the assertion: testify fails the
// test immediately if the mock receives a call that does not match, so no
// additional Contains check is needed.
func (s *BackendClientTestSuite) TestWrite_PassesWaitForReady() {
	writeStub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{
		Written: 5,
		Status:  int32(fuse.OK),
	}, nil)

	// positional waitForReadyOpt: an unmatched call fails the test immediately.
	s.fileClient.EXPECT().Write(mock.Anything, waitForReadyOpt).
		Return(writeStub, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	_, st := s.backend.Write(context.Background(), h, 0, []byte("hello"))
	s.Require().Equal(fuse.OK, st)
}
