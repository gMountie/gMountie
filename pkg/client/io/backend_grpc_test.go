package io

import (
	"context"
	stdio "io"
	"testing"
	"time"

	grpcmocks "gmountie/internal/mocks/pkg/client/grpc"
	mockProto "gmountie/internal/mocks/pkg/proto"
	grpcclient "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newBackendReadStreamStub returns a MockRpcFile_ReadClient that yields
// the given frames in order via Recv(), then EOF. Identical to the
// helper used by the legacy GrpcFile tests; duplicated here so this test
// file stands alone once file_test.go goes away in Task 5.
func newBackendReadStreamStub(t *testing.T, frames ...*proto.ReadFrame) *mockProto.MockRpcFile_ReadClient {
	stub := mockProto.NewMockRpcFile_ReadClient(t)
	for _, f := range frames {
		stub.EXPECT().Recv().Return(f, nil).Once()
	}
	stub.EXPECT().Recv().Return(nil, stdio.EOF).Maybe()
	return stub
}

// backendWriteStreamStub captures every WriteFrame sent through the
// streaming Write client and returns the configured WriteReply / error
// on CloseAndRecv. Send copies the data slice since real gRPC marshals
// synchronously but our caller reuses the source buffer across frames.
type backendWriteStreamStub struct {
	*mockProto.MockRpcFile_WriteClient
	frames     []*proto.WriteFrame
	closeReply *proto.WriteReply
	closeErr   error
}

func newBackendWriteStreamStub(t *testing.T, reply *proto.WriteReply, err error) *backendWriteStreamStub {
	stub := &backendWriteStreamStub{
		MockRpcFile_WriteClient: mockProto.NewMockRpcFile_WriteClient(t),
		closeReply:              reply,
		closeErr:                err,
	}
	stub.EXPECT().Send(mock.AnythingOfType("*proto.WriteFrame")).RunAndReturn(stub.send).Maybe()
	stub.EXPECT().CloseAndRecv().RunAndReturn(stub.recv).Maybe()
	return stub
}

func (w *backendWriteStreamStub) send(f *proto.WriteFrame) error {
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

func (w *backendWriteStreamStub) recv() (*proto.WriteReply, error) {
	return w.closeReply, w.closeErr
}

type BackendClientTestSuite struct {
	suite.Suite
	client     *grpcmocks.MockClient
	fsClient   *mockProto.MockRpcFsClient
	fileClient *mockProto.MockRpcFileClient
	backend    *BackendClient
}

func (s *BackendClientTestSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.fsClient = mockProto.NewMockRpcFsClient(s.T())
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.client.EXPECT().Fs().Return(s.fsClient).Maybe()
	s.client.EXPECT().File().Return(s.fileClient).Maybe()
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
	s.client.EXPECT().PerFileConfig().Return(grpcclient.PerFileConfig{}).Maybe()
	s.backend = NewBackendClient(s.client, "testVolume")
}

// newHandle constructs a *grpcFileHandle directly for tests that exercise
// the fd-level RPCs (Read/Write/Flush/Release/Fsync). The handle is
// otherwise identical to one returned by Open/Create.
func (s *BackendClientTestSuite) newHandle(cfg grpcclient.PerFileConfig) *grpcFileHandle {
	return newGrpcFileHandle(s.fileClient, "testVolume", "/test/path", 1, 30*time.Second, "test-session", cfg)
}

// --- path-level ops ---

func (s *BackendClientTestSuite) TestStat() {
	testAttr := &proto.Attr{
		Ino:     1,
		Size:    100,
		Blocks:  1,
		Atime:   1000,
		Mtime:   1000,
		Ctime:   1000,
		Mode:    0755,
		Nlink:   1,
		Owner:   &proto.Owner{Uid: 1000, Gid: 1000},
		Rdev:    0,
		Blksize: 4096,
	}
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.MatchedBy(func(req *proto.GetAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test"
	})).Return(&proto.GetAttrReply{
		Status:     int32(fuse.OK),
		Attributes: testAttr,
	}, nil)

	attr, st := s.backend.Stat(context.Background(), "/test")

	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(testAttr.Size, attr.Size)
	s.Assert().Equal(testAttr.Mode, attr.Mode)
	s.Assert().Equal(testAttr.Ino, attr.Ino)
	s.Assert().Equal(uint32(1000), attr.Uid)
	s.Assert().Equal(uint32(1000), attr.Gid)
}

func (s *BackendClientTestSuite) TestStat_Error() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	attr, st := s.backend.Stat(context.Background(), "/test")
	s.Assert().Equal(fuse.EIO, st)
	s.Assert().Nil(attr)
}

// TestStat_RetriesOnUnavailable verifies that an idempotent metadata
// call survives a single transient Unavailable via the retry wrapper.
func (s *BackendClientTestSuite) TestStat_RetriesOnUnavailable() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).
		Return(&proto.GetAttrReply{
			Status:     int32(fuse.OK),
			Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
		}, nil).Once()

	attr, st := s.backend.Stat(context.Background(), "file")

	s.Require().Equal(fuse.OK, st)
	s.NotNil(attr)
	s.fsClient.AssertNumberOfCalls(s.T(), "GetAttr", 2)
}

// TestLookup verifies Lookup is implemented atop GetAttr on the joined
// parent/name path. The returned Attr carries the inode.
func (s *BackendClientTestSuite) TestLookup() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.MatchedBy(func(req *proto.GetAttrRequest) bool {
		return req.Path == "/parent/child"
	})).Return(&proto.GetAttrReply{
		Status:     int32(fuse.OK),
		Attributes: &proto.Attr{Ino: 42, Mode: 0o644, Owner: &proto.Owner{Uid: 1, Gid: 1}},
	}, nil)

	attr, st := s.backend.Lookup(context.Background(), "/parent", "child")
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(42), attr.Ino)
}

func (s *BackendClientTestSuite) TestListDir() {
	entries := []*proto.DirEntry{
		{Name: "file1", Mode: 0644, Ino: 1},
		{Name: "file2", Mode: 0644, Ino: 2},
	}
	s.fsClient.EXPECT().OpenDir(mock.Anything, mock.MatchedBy(func(req *proto.OpenDirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test"
	})).Return(&proto.OpenDirReply{
		Status:  int32(fuse.OK),
		Entries: entries,
	}, nil)

	result, st := s.backend.ListDir(context.Background(), "/test")

	s.Require().Equal(fuse.OK, st)
	s.Require().Len(result, 2)
	s.Assert().Equal("file1", result[0].Name)
	s.Assert().Equal("file2", result[1].Name)
	s.Assert().Equal(uint64(1), result[0].Ino)
}

func (s *BackendClientTestSuite) TestAccess() {
	s.fsClient.EXPECT().Access(mock.Anything, mock.MatchedBy(func(req *proto.AccessRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Mode == 0444
	})).Return(&proto.AccessReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Access(context.Background(), "/test", 0444)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestStatFs() {
	s.fsClient.EXPECT().StatFs(mock.Anything, &proto.StatFsRequest{
		Volume: "testVolume",
		Path:   "/test",
	}).Return(&proto.StatFsReply{
		Blocks:  1000,
		Bfree:   500,
		Bavail:  400,
		Files:   100,
		Ffree:   50,
		Bsize:   4096,
		Namelen: 255,
		Frsize:  4096,
	}, nil)

	stats, st := s.backend.StatFs(context.Background(), "/test")
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(stats)
	s.Assert().Equal(uint64(1000), stats.Blocks)
	s.Assert().Equal(uint64(500), stats.Bfree)
	s.Assert().Equal(uint32(4096), stats.Bsize)
}

func (s *BackendClientTestSuite) TestGetXAttr() {
	expectedData := []byte("xattr_value")
	s.fsClient.EXPECT().GetXAttr(mock.Anything, mock.MatchedBy(func(req *proto.GetXAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Attribute == "user.test"
	})).Return(&proto.GetXAttrReply{
		Status: int32(fuse.OK),
		Data:   expectedData,
	}, nil)

	data, st := s.backend.GetXAttr(context.Background(), "/test", "user.test")
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(expectedData, data)
}

func (s *BackendClientTestSuite) TestMkdir() {
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Mode == 0755 &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Mkdir(context.Background(), "/test", 0755)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestRmdir() {
	s.fsClient.EXPECT().Rmdir(mock.Anything, mock.MatchedBy(func(req *proto.RmdirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.RmdirReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Rmdir(context.Background(), "/test")
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestUnlink() {
	s.fsClient.EXPECT().Unlink(mock.Anything, mock.MatchedBy(func(req *proto.UnlinkRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.UnlinkReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Unlink(context.Background(), "/test")
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestRename() {
	s.fsClient.EXPECT().Rename(mock.Anything, mock.MatchedBy(func(req *proto.RenameRequest) bool {
		return req.Volume == "testVolume" && req.OldName == "/old" && req.NewName == "/new" &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.RenameReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Rename(context.Background(), "/old", "/new")
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestTruncate() {
	s.fsClient.EXPECT().Truncate(mock.Anything, mock.MatchedBy(func(req *proto.TruncateRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Size == 1024 &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.TruncateReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Truncate(context.Background(), "/test", 1024)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestChmod() {
	s.fsClient.EXPECT().Chmod(mock.Anything, mock.MatchedBy(func(req *proto.ChmodRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Mode == 0644 &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.ChmodReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Chmod(context.Background(), "/test", 0644)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestChown() {
	s.fsClient.EXPECT().Chown(mock.Anything, mock.MatchedBy(func(req *proto.ChownRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Uid == 1001 && req.Gid == 1001 &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.ChownReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Chown(context.Background(), "/test", 1001, 1001)
	s.Assert().Equal(fuse.OK, st)
}

// TestMkdir_RetryReusesRequestID is the load-bearing Phase 1d assertion
// for path-level mutating ops: the same request_id must be reused across
// retries so the server's dedup cache can short-circuit the duplicate.
func (s *BackendClientTestSuite) TestMkdir_RetryReusesRequestID() {
	var firstID string
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
		firstID = req.RequestId
		return req.RequestId != ""
	})).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
		return req.RequestId == firstID
	})).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil).Once()

	st := s.backend.Mkdir(context.Background(), "/d", 0755)
	s.Assert().Equal(fuse.OK, st)
	s.Assert().NotEmpty(firstID)
}

// --- Open / Create ---

func (s *BackendClientTestSuite) TestOpenReturnsHandle() {
	s.fileClient.EXPECT().Open(mock.Anything, mock.MatchedBy(func(req *proto.OpenRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Flags == 0 &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.OpenReply{
		Status: int32(fuse.OK),
		Fd:     1,
	}, nil)

	fh, st := s.backend.Open(context.Background(), "/test", 0)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(fh)
	s.Assert().IsType(&grpcFileHandle{}, fh)
	s.Assert().Equal("/test", fh.Path())
}

func (s *BackendClientTestSuite) TestOpen_Error() {
	s.fileClient.EXPECT().Open(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	fh, st := s.backend.Open(context.Background(), "/test", 0)
	s.Assert().Equal(fuse.EIO, st)
	s.Assert().Nil(fh)
}

// TestCreateReturnsHandle verifies the joined-path semantics for Create
// (parent + "/" + name) and that Attr is nil — Task 3's node adapter
// must follow up with a Stat for the EntryOut.
func (s *BackendClientTestSuite) TestCreateReturnsHandle() {
	s.fileClient.EXPECT().Create(mock.Anything, mock.MatchedBy(func(req *proto.CreateRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/dir/file" && req.Flags == 0 && req.Mode == 0644 &&
			req.SessionId == "test-session" && req.RequestId != ""
	})).Return(&proto.CreateReply{
		Status: int32(fuse.OK),
		Fd:     7,
	}, nil)

	fh, attr, st := s.backend.Create(context.Background(), "/dir", "file", 0, 0644)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(fh)
	s.Assert().IsType(&grpcFileHandle{}, fh)
	s.Assert().Nil(attr, "current proto.CreateReply carries no Attr; node adapter must follow up with Stat")
}

// --- fd-level ops ---

func (s *BackendClientTestSuite) TestRead_MultiFrame() {
	testData := []byte("test data")
	stream := newBackendReadStreamStub(s.T(),
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

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 1024)
	n, st := s.backend.Read(context.Background(), h, 0, dest)

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(len(testData), n)
	s.Assert().Equal(testData, dest[:n])
}

func (s *BackendClientTestSuite) TestRead_ErrorReturnsEIO() {
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 1024)
	n, st := s.backend.Read(context.Background(), h, 0, dest)

	s.Assert().Equal(fuse.EIO, st)
	s.Assert().Equal(0, n)
}

// TestRead_RetriesOnUnavailable verifies that Read survives a single
// transient Unavailable. Each retry opens a fresh stream.
func (s *BackendClientTestSuite) TestRead_RetriesOnUnavailable() {
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	streamOK := newBackendReadStreamStub(s.T(),
		&proto.ReadFrame{Data: []byte("0123456789"), Status: int32(fuse.OK)},
		&proto.ReadFrame{Status: int32(fuse.OK)},
	)
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(streamOK, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 10)
	n, st := s.backend.Read(context.Background(), h, 0, dest)

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(10, n)
	s.fileClient.AssertNumberOfCalls(s.T(), "Read", 2)
}

func (s *BackendClientTestSuite) TestWrite_SmallPayload() {
	testData := []byte("test data")
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{
		Written: uint32(len(testData)),
		Status:  int32(fuse.OK),
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	written, st := s.backend.Write(context.Background(), h, 0, testData)

	s.Require().Equal(fuse.OK, st)
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

// TestWrite_LargePayloadChunks verifies the client splits payloads
// larger than writeFrameSizeBytes into multiple frames, with the header
// only on frame 1.
func (s *BackendClientTestSuite) TestWrite_LargePayloadChunks() {
	payload := make([]byte, (3*writeFrameSizeBytes)+1024)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{
		Written: uint32(len(payload)),
		Status:  int32(fuse.OK),
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	written, st := s.backend.Write(context.Background(), h, 7, payload)

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(len(payload)), written)
	s.Require().Len(stub.frames, 4, "expected 4 frames for 3*frame + 1024 bytes")
	header := stub.frames[0]
	s.Assert().Equal("testVolume", header.Volume)
	s.Assert().Equal(uint64(1), header.Fd)
	s.Assert().Equal("test-session", header.SessionId)
	s.Assert().NotEmpty(header.RequestId)
	s.Assert().Equal(int64(7), header.Offset)
	s.Assert().Equal(writeFrameSizeBytes, len(header.Data))
	for i, frame := range stub.frames[1:] {
		s.Assert().Empty(frame.Volume, "frame %d volume must be zero", i+1)
		s.Assert().Equal(uint64(0), frame.Fd, "frame %d fd must be zero", i+1)
		s.Assert().Empty(frame.SessionId, "frame %d session_id must be zero", i+1)
		s.Assert().Empty(frame.RequestId, "frame %d request_id must be zero", i+1)
		s.Assert().Equal(int64(0), frame.Offset, "frame %d offset must be zero", i+1)
	}
	got := make([]byte, 0, len(payload))
	for _, frame := range stub.frames {
		got = append(got, frame.Data...)
	}
	s.Assert().Equal(payload, got)
}

func (s *BackendClientTestSuite) TestWrite_RetriesOnUnavailable() {
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 5, Status: 0}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	n, st := s.backend.Write(context.Background(), h, 0, []byte("hello"))
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(5), n)
	s.Require().Len(stub.frames, 1)
	s.Assert().NotEmpty(stub.frames[0].RequestId)
}

// TestWrite_RetryReusesRequestID is the load-bearing Phase 1d invariant
// for the fd-level streaming Write: requestID must be generated once
// outside the retry closure so the server's idempotency LRU can
// short-circuit the replay on the second attempt.
func (s *BackendClientTestSuite) TestWrite_RetryReusesRequestID() {
	attempt1 := newBackendWriteStreamStub(s.T(), nil, status.Error(codes.Unavailable, "transient"))
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(attempt1, nil).Once()
	attempt2 := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 5, Status: 0}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(attempt2, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	n, st := s.backend.Write(context.Background(), h, 0, []byte("hello"))

	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint32(5), n)
	s.Require().Len(attempt1.frames, 1, "attempt 1 should have sent the header frame before CloseAndRecv failed")
	s.Require().Len(attempt2.frames, 1, "attempt 2 should have sent the header frame")
	s.Require().NotEmpty(attempt1.frames[0].RequestId)
	s.Assert().Equal(attempt1.frames[0].RequestId, attempt2.frames[0].RequestId,
		"retry must reuse the same RequestId so the server idempotency LRU short-circuits the replay")
}

func (s *BackendClientTestSuite) TestWrite_Error() {
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	written, st := s.backend.Write(context.Background(), h, 0, []byte("test"))
	s.Assert().Equal(fuse.EIO, st)
	s.Assert().Equal(uint32(0), written)
}

// TestRelease verifies that Release cancels lifeCtx, then issues the
// server-side Release RPC. The lifeCtx is observable via h.lifeCtx.Err()
// after the call.
func (s *BackendClientTestSuite) TestRelease() {
	s.fileClient.EXPECT().Release(mock.Anything, &proto.ReleaseRequest{
		Volume:    "testVolume",
		Fd:        1,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.ReleaseReply{}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Release(context.Background(), h)
	s.Assert().Equal(fuse.OK, st)
	s.Assert().Error(h.lifeCtx.Err(), "lifeCtx must be cancelled after Release")
}

func (s *BackendClientTestSuite) TestFlush() {
	s.fileClient.EXPECT().Flush(mock.Anything, &proto.FlushRequest{
		Volume:    "testVolume",
		Fd:        1,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.FlushReply{
		Status: int32(fuse.OK),
	}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestFsync() {
	s.fileClient.EXPECT().Fsync(mock.Anything, &proto.FsyncRequest{
		Volume:    "testVolume",
		Fd:        1,
		Flags:     0,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.FsyncReply{
		Status: int32(fuse.OK),
	}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Fsync(context.Background(), h, 0)
	s.Assert().Equal(fuse.OK, st)
}

// TestRead_BadHandleEBADF verifies that passing a non-*grpcFileHandle to
// the fd-level ops fails fast with EBADF rather than panicking.
func (s *BackendClientTestSuite) TestRead_BadHandleEBADF() {
	n, st := s.backend.Read(context.Background(), badHandle{}, 0, make([]byte, 8))
	s.Assert().Equal(0, n)
	s.Assert().Equal(fuse.EBADF, st)
}

// badHandle is a FileHandle implementation that is not a *grpcFileHandle,
// used to exercise the type-assertion guard on fd-level ops.
type badHandle struct{}

func (badHandle) Path() string { return "/bad" }

func TestBackendClientTestSuite(t *testing.T) {
	suite.Run(t, new(BackendClientTestSuite))
}
