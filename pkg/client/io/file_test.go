package io

import (
	"context"
	mockProto "gmountie/internal/mocks/pkg/proto"
	"gmountie/pkg/proto"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GrpcFileTestSuite struct {
	suite.Suite
	fileClient *mockProto.MockRpcFileClient
	file       *GrpcFile
}

func (s *GrpcFileTestSuite) SetupTest() {
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.file = NewGrpcFile(s.fileClient, "testVolume", "/test/path", 1, 30*time.Second)
}

func (s *GrpcFileTestSuite) TestRead() {
	// Setup
	testData := []byte("test data")
	s.fileClient.EXPECT().Read(mock.Anything, &proto.ReadRequest{
		Volume: "testVolume",
		Fd:     1,
		Offset: 0,
		Size:   1024,
	}, mock.Anything).Return(&proto.ReadReply{
		Bytes:  testData,
		Size:   int64(len(testData)),
		Status: int32(fuse.OK),
	}, nil)

	// Test
	dest := make([]byte, 1024)
	result, status := s.file.Read(dest, 0)

	// Verify
	s.Require().Equal(fuse.OK, status)
	rData, rStatus := result.Bytes(make([]byte, result.Size()))
	s.Assert().Equal(testData, rData)
	s.Assert().Equal(fuse.OK, rStatus)
}

func (s *GrpcFileTestSuite) TestWrite() {
	// Setup
	testData := []byte("test data")
	s.fileClient.EXPECT().Write(mock.Anything, &proto.WriteRequest{
		Volume: "testVolume",
		Fd:     1,
		Bytes:  testData,
		Offset: 0,
	}, mock.Anything).Return(&proto.WriteReply{
		Written: uint32(len(testData)),
		Status:  int32(fuse.OK),
	}, nil)

	// Test
	written, status := s.file.Write(testData, 0)

	// Verify
	s.Require().Equal(fuse.OK, status)
	s.Assert().Equal(uint32(len(testData)), written)
}

func (s *GrpcFileTestSuite) TestRelease() {
	// Setup
	s.fileClient.EXPECT().Release(mock.Anything, &proto.ReleaseRequest{
		Volume: "testVolume",
		Fd:     1,
	}, mock.Anything).Return(&proto.ReleaseReply{}, nil)

	// Test
	s.file.Release()

	// Verify implicitly through mock expectations
}

func (s *GrpcFileTestSuite) TestFlush() {
	// Setup
	s.fileClient.EXPECT().Flush(mock.Anything, &proto.FlushRequest{
		Volume: "testVolume",
		Fd:     1,
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
		Volume: "testVolume",
		Fd:     1,
		Flags:  0,
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
		Volume: "testVolume",
		Fd:     1,
		Off:    0,
		Size:   1024,
		Mode:   0,
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
	// Setup
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
// transient Unavailable.
func (s *GrpcFileTestSuite) TestRead_RetriesOnUnavailable() {
	dest := make([]byte, 10)

	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.ReadReply{
			Bytes:  []byte("0123456789"),
			Status: int32(fuse.OK),
		}, nil).Once()

	result, st := s.file.Read(dest, 0)

	s.Require().Equal(fuse.OK, st)
	s.NotNil(result)
	s.fileClient.AssertNumberOfCalls(s.T(), "Read", 2)
}

func TestGrpcFileTestSuite(t *testing.T) {
	suite.Run(t, new(GrpcFileTestSuite))
}
