package io

import (
	"context"
	stdio "io"
	"testing"
	"time"

	mockProto "gmountie/internal/mocks/pkg/proto"
	grpcclient "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newReadStreamStub returns a MockRpcFile_ReadClient that yields the given
// frames in order via Recv(), then EOF. Used to drive the streaming Read
// client through its frame-accumulation loop in unit tests.
func newReadStreamStub(t *testing.T, frames ...*proto.ReadFrame) *mockProto.MockRpcFile_ReadClient {
	stub := mockProto.NewMockRpcFile_ReadClient(t)
	for _, f := range frames {
		stub.EXPECT().Recv().Return(f, nil).Once()
	}
	stub.EXPECT().Recv().Return(nil, stdio.EOF).Maybe()
	return stub
}

// writeStreamStub captures every WriteFrame sent through the streaming Write
// client and returns the configured WriteReply / error on CloseAndRecv. Send
// copies the data slice since real gRPC marshals synchronously but our caller
// reuses the source buffer across frames.
type writeStreamStub struct {
	*mockProto.MockRpcFile_WriteClient
	frames        []*proto.WriteFrame
	closeReply    *proto.WriteReply
	closeErr      error
	sendErr       error
	sendErrAfterN int
}

func newWriteStreamStub(t *testing.T, reply *proto.WriteReply, err error) *writeStreamStub {
	stub := &writeStreamStub{
		MockRpcFile_WriteClient: mockProto.NewMockRpcFile_WriteClient(t),
		closeReply:              reply,
		closeErr:                err,
	}
	stub.EXPECT().Send(mock.AnythingOfType("*proto.WriteFrame")).RunAndReturn(stub.send).Maybe()
	stub.EXPECT().CloseAndRecv().RunAndReturn(stub.recv).Maybe()
	return stub
}

func (w *writeStreamStub) send(f *proto.WriteFrame) error {
	if w.sendErr != nil && len(w.frames) >= w.sendErrAfterN {
		return w.sendErr
	}
	dup := &proto.WriteFrame{
		Volume:    f.Volume,
		Fd:        f.Fd,
		SessionId: f.SessionId,
		RequestId: f.RequestId,
		Offset:    f.Offset,
		Data:      append([]byte(nil), f.Data...),
	}
	w.frames = append(w.frames, dup)
	return nil
}

func (w *writeStreamStub) recv() (*proto.WriteReply, error) {
	return w.closeReply, w.closeErr
}

type GrpcFileTestSuite struct {
	suite.Suite
	fileClient *mockProto.MockRpcFileClient
	file       *GrpcFile
}

func (s *GrpcFileTestSuite) SetupTest() {
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.file = NewGrpcFile(s.fileClient, "testVolume", "/test/path", 1, 30*time.Second, "test-session", grpcclient.PerFileConfig{})
}

func (s *GrpcFileTestSuite) TestRead() {
	// Setup
	testData := []byte("test data")
	stream := newReadStreamStub(s.T(),
		&proto.ReadFrame{Data: testData, Status: int32(fuse.OK)},
		&proto.ReadFrame{Status: int32(fuse.OK)},
	)
	s.fileClient.EXPECT().Read(mock.Anything, &proto.ReadRequest{
		Volume:    "testVolume",
		Fd:        1,
		Offset:    0,
		Size:      1024,
		SessionId: "test-session",
	}, mock.Anything).Return(stream, nil)

	// Test
	dest := make([]byte, 1024)
	result, status := s.file.Read(dest, 0)

	// Verify
	s.Require().Equal(fuse.OK, status)
	s.Require().NotNil(result)
	s.Assert().Equal(len(testData), result.Size())
	rData, rStatus := result.Bytes(make([]byte, result.Size()))
	s.Assert().Equal(testData, rData)
	s.Assert().Equal(fuse.OK, rStatus)
}

func (s *GrpcFileTestSuite) TestWrite() {
	// Setup
	testData := []byte("test data")
	stub := newWriteStreamStub(s.T(), &proto.WriteReply{
		Written: uint32(len(testData)),
		Status:  int32(fuse.OK),
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil)

	// Test
	written, status := s.file.Write(testData, 0)

	// Verify
	s.Require().Equal(fuse.OK, status)
	s.Assert().Equal(uint32(len(testData)), written)
	s.Require().Len(stub.frames, 1, "small payload should fit in a single frame")
	header := stub.frames[0]
	s.Assert().Equal("testVolume", header.Volume)
	s.Assert().Equal(uint64(1), header.Fd)
	s.Assert().Equal("test-session", header.SessionId)
	s.Assert().NotEmpty(header.RequestId)
	s.Assert().Equal(int64(0), header.Offset)
	s.Assert().Equal(testData, header.Data)
}

func (s *GrpcFileTestSuite) TestWriteRetriesOnUnavailable() {
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	stub := newWriteStreamStub(s.T(), &proto.WriteReply{Written: 5, Status: 0}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil).Once()

	f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session", grpcclient.PerFileConfig{})
	n, st := f.Write([]byte("hello"), 0)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(5), n)
	s.Require().Len(stub.frames, 1)
	s.Assert().NotEmpty(stub.frames[0].RequestId)
}

func (s *GrpcFileTestSuite) TestWriteRetryReusesRequestID() {
	// Phase 1d invariant: requestID must be generated once outside the retry
	// closure so the server's idempotency cache can short-circuit the replay.
	// Attempt 1 sends the header frame successfully, then CloseAndRecv fails
	// with a retryable error — that lets us capture attempt-1 frames and
	// compare the RequestId against attempt 2's.
	attempt1 := newWriteStreamStub(s.T(), nil, status.Error(codes.Unavailable, "transient"))
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(attempt1, nil).Once()
	attempt2 := newWriteStreamStub(s.T(), &proto.WriteReply{Written: 5, Status: 0}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(attempt2, nil).Once()

	f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session", grpcclient.PerFileConfig{})
	n, st := f.Write([]byte("hello"), 0)

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(5), n)
	s.Require().Len(attempt1.frames, 1, "attempt 1 should have sent the header frame before CloseAndRecv failed")
	s.Require().Len(attempt2.frames, 1, "attempt 2 should have sent the header frame")
	s.Require().NotEmpty(attempt1.frames[0].RequestId)
	s.Assert().Equal(attempt1.frames[0].RequestId, attempt2.frames[0].RequestId,
		"retry must reuse the same RequestId so the server idempotency LRU short-circuits the replay")
}

// TestWriteChunksLargePayload verifies the client splits payloads larger
// than writeFrameSizeBytes into multiple frames, with the header only on
// frame 1.
func (s *GrpcFileTestSuite) TestWriteChunksLargePayload() {
	payload := make([]byte, (3*writeFrameSizeBytes)+1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	stub := newWriteStreamStub(s.T(), &proto.WriteReply{
		Written: uint32(len(payload)),
		Status:  int32(fuse.OK),
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil)

	written, st := s.file.Write(payload, 7)

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(len(payload)), written)
	s.Require().Len(stub.frames, 4, "expected 4 frames for 3*frame + 1024 bytes")
	// Frame 1: header populated.
	header := stub.frames[0]
	s.Assert().Equal("testVolume", header.Volume)
	s.Assert().Equal(uint64(1), header.Fd)
	s.Assert().Equal("test-session", header.SessionId)
	s.Assert().NotEmpty(header.RequestId)
	s.Assert().Equal(int64(7), header.Offset)
	s.Assert().Equal(writeFrameSizeBytes, len(header.Data))
	// Subsequent frames: header fields zero.
	for i, frame := range stub.frames[1:] {
		s.Assert().Empty(frame.Volume, "frame %d volume must be zero", i+1)
		s.Assert().Equal(uint64(0), frame.Fd, "frame %d fd must be zero", i+1)
		s.Assert().Empty(frame.SessionId, "frame %d session_id must be zero", i+1)
		s.Assert().Empty(frame.RequestId, "frame %d request_id must be zero", i+1)
		s.Assert().Equal(int64(0), frame.Offset, "frame %d offset must be zero", i+1)
	}
	// Bytes reassemble in order.
	got := make([]byte, 0, len(payload))
	for _, frame := range stub.frames {
		got = append(got, frame.Data...)
	}
	s.Assert().Equal(payload, got)
}

func (s *GrpcFileTestSuite) TestRelease() {
	// Setup
	s.fileClient.EXPECT().Release(mock.Anything, &proto.ReleaseRequest{
		Volume:    "testVolume",
		Fd:        1,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.ReleaseReply{}, nil)

	// Test
	s.file.Release()

	// Verify implicitly through mock expectations
}

func (s *GrpcFileTestSuite) TestFlush() {
	// Setup
	s.fileClient.EXPECT().Flush(mock.Anything, &proto.FlushRequest{
		Volume:    "testVolume",
		Fd:        1,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.FlushReply{
		Status: int32(fuse.OK),
	}, nil)

	// Test
	status := s.file.Flush()

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *GrpcFileTestSuite) TestFsync() {
	// Setup
	s.fileClient.EXPECT().Fsync(mock.Anything, &proto.FsyncRequest{
		Volume:    "testVolume",
		Fd:        1,
		Flags:     0,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.FsyncReply{
		Status: int32(fuse.OK),
	}, nil)

	// Test
	status := s.file.Fsync(0)

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *GrpcFileTestSuite) TestGetLk() {
	// Setup
	testLock := &fuse.FileLock{
		Start: 0,
		End:   100,
		Typ:   fuse.FUSE_LK_FLOCK,
		Pid:   1234,
	}
	s.fileClient.EXPECT().GetLk(mock.Anything, &proto.GetLkRequest{
		Volume: "testVolume",
		Fd:     1,
		Owner:  1,
		Flags:  0,
		Lk: &proto.FileLock{
			Start: testLock.Start,
			End:   testLock.End,
			Typ:   testLock.Typ,
			Pid:   testLock.Pid,
		},
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.GetLkReply{
		Status: int32(fuse.OK),
		Lk: &proto.FileLock{
			Start: 0,
			End:   100,
			Typ:   fuse.FUSE_LK_FLOCK,
			Pid:   1234,
		},
	}, nil)

	// Test
	outLock := &fuse.FileLock{}
	status := s.file.GetLk(1, testLock, 0, outLock)

	// Verify
	s.Assert().Equal(fuse.OK, status)
	s.Assert().Equal(testLock.Start, outLock.Start)
	s.Assert().Equal(testLock.End, outLock.End)
	s.Assert().Equal(testLock.Typ, outLock.Typ)
	s.Assert().Equal(testLock.Pid, outLock.Pid)
}

func (s *GrpcFileTestSuite) TestSetLk() {
	// Setup
	testLock := &fuse.FileLock{
		Start: 0,
		End:   100,
		Typ:   fuse.FUSE_LK_FLOCK,
		Pid:   1234,
	}
	s.fileClient.EXPECT().SetLk(mock.Anything, &proto.SetLkRequest{
		Volume: "testVolume",
		Fd:     1,
		Owner:  1,
		Flags:  0,
		Lk: &proto.FileLock{
			Start: testLock.Start,
			End:   testLock.End,
			Typ:   testLock.Typ,
			Pid:   testLock.Pid,
		},
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.SetLkReply{
		Status: int32(fuse.OK),
	}, nil)

	// Test
	status := s.file.SetLk(1, testLock, 0)

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *GrpcFileTestSuite) TestSetLkw() {
	// Setup
	testLock := &fuse.FileLock{
		Start: 0,
		End:   100,
		Typ:   fuse.FUSE_LK_FLOCK,
		Pid:   1234,
	}
	s.fileClient.EXPECT().SetLkw(mock.Anything, &proto.SetLkwRequest{
		Volume: "testVolume",
		Fd:     1,
		Owner:  1,
		Flags:  0,
		Lk: &proto.FileLock{
			Start: testLock.Start,
			End:   testLock.End,
			Typ:   testLock.Typ,
			Pid:   testLock.Pid,
		},
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.SetLkwReply{
		Status: int32(fuse.OK),
	}, nil)

	// Test
	status := s.file.SetLkw(1, testLock, 0)

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *GrpcFileTestSuite) TestAllocate() {
	// Setup
	s.fileClient.EXPECT().Allocate(mock.Anything, &proto.AllocateRequest{
		Volume:    "testVolume",
		Fd:        1,
		Off:       0,
		Size:      1024,
		Mode:      0,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.AllocateReply{
		Status: int32(fuse.OK),
	}, nil)

	// Test
	status := s.file.Allocate(0, 1024, 0)

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

// Error cases
func (s *GrpcFileTestSuite) TestRead_Error() {
	// Setup: stream open fails.
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	// Test
	dest := make([]byte, 1024)
	result, status := s.file.Read(dest, 0)

	// Verify
	s.Assert().Equal(fuse.EIO, status)
	s.Assert().Nil(result)
}

func (s *GrpcFileTestSuite) TestWrite_Error() {
	// Setup: stream open fails.
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	// Test
	written, status := s.file.Write([]byte("test"), 0)

	// Verify
	s.Assert().Equal(fuse.EIO, status)
	s.Assert().Equal(uint32(0), written)
}

// TestRead_RetriesOnUnavailable verifies that Read survives a single
// transient Unavailable. Each retry opens a fresh stream.
func (s *GrpcFileTestSuite) TestRead_RetriesOnUnavailable() {
	dest := make([]byte, 10)

	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	streamOK := newReadStreamStub(s.T(),
		&proto.ReadFrame{Data: []byte("0123456789"), Status: int32(fuse.OK)},
		&proto.ReadFrame{Status: int32(fuse.OK)},
	)
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(streamOK, nil).Once()

	result, st := s.file.Read(dest, 0)

	s.Require().Equal(fuse.OK, st)
	s.NotNil(result)
	s.fileClient.AssertNumberOfCalls(s.T(), "Read", 2)
}

func (s *GrpcFileTestSuite) TestReadStampsSessionID() {
	stream := newReadStreamStub(s.T(),
		&proto.ReadFrame{Status: int32(fuse.OK)},
	)
	s.fileClient.EXPECT().Read(mock.Anything, mock.MatchedBy(func(req *proto.ReadRequest) bool {
		return req.SessionId == "test-session"
	}), mock.Anything).Return(stream, nil).Once()

	f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session", grpcclient.PerFileConfig{})
	buf := make([]byte, 4)
	_, st := f.Read(buf, 0)
	s.Assert().Equal(fuse.OK, st)
}

func TestGrpcFileTestSuite(t *testing.T) {
	suite.Run(t, new(GrpcFileTestSuite))
}
