package io

import (
	"context"
	stdio "io"
	"testing"
	"time"

	mockProto "gmountie/internal/mocks/pkg/proto"
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

type GrpcFileTestSuite struct {
	suite.Suite
	fileClient *mockProto.MockRpcFileClient
	file       *GrpcFile
}

func (s *GrpcFileTestSuite) SetupTest() {
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.file = NewGrpcFile(s.fileClient, "testVolume", "/test/path", 1, 30*time.Second, "test-session")
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
	s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
		return req.Volume == "testVolume" &&
			req.Fd == 1 &&
			req.Offset == 0 &&
			string(req.Bytes) == string(testData) &&
			req.SessionId == "test-session" &&
			req.RequestId != ""
	}), mock.Anything).Return(&proto.WriteReply{
		Written: uint32(len(testData)),
		Status:  int32(fuse.OK),
	}, nil)

	// Test
	written, status := s.file.Write(testData, 0)

	// Verify
	s.Require().Equal(fuse.OK, status)
	s.Assert().Equal(uint32(len(testData)), written)
}

func (s *GrpcFileTestSuite) TestWriteRetriesOnUnavailable() {
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
		return req.RequestId != "" && req.SessionId == "test-session"
	}), mock.Anything).Return(&proto.WriteReply{Written: 5, Status: 0}, nil).Once()

	f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session")
	n, st := f.Write([]byte("hello"), 0)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(5), n)
}

func (s *GrpcFileTestSuite) TestWriteRetryReusesRequestID() {
	var firstID string
	s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
		firstID = req.RequestId
		return req.RequestId != ""
	}), mock.Anything).Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	s.fileClient.EXPECT().Write(mock.Anything, mock.MatchedBy(func(req *proto.WriteRequest) bool {
		return req.RequestId == firstID
	}), mock.Anything).Return(&proto.WriteReply{Written: 5, Status: 0}, nil).Once()

	f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session")
	_, _ = f.Write([]byte("hello"), 0)
	s.Assert().NotEmpty(firstID)
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
	// Setup
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything, mock.Anything).
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

	f := NewGrpcFile(s.fileClient, "vol", "/p", 1, time.Second, "test-session")
	buf := make([]byte, 4)
	_, st := f.Read(buf, 0)
	s.Assert().Equal(fuse.OK, st)
}

func TestGrpcFileTestSuite(t *testing.T) {
	suite.Run(t, new(GrpcFileTestSuite))
}
