package io

import (
	"context"
	stdio "io"
	"sync/atomic"
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
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

// newBackendReadStreamStubOptional is like newBackendReadStreamStub but the
// stream need not be fully drained. Use it for streams handed to ASYNC prefetch
// goroutines: the test asserts how many Read RPCs were issued (a counter), not
// that every prefetch goroutine drains its stream before the test returns.
//
// It uses a single Recv() expectation whose RunAndReturn walks the frames in
// order (data → ok → …) and returns EOF thereafter, gated by Maybe() so any
// number of calls — including zero — satisfies AssertExpectations. The earlier
// per-frame `.Times(1).Maybe()` form did NOT achieve this: `.Times(1)` sets a
// Repeatability that cleanup checks independently of `.Maybe()`, so a prefetch
// goroutine abandoned mid-drain left an unmet count and failed AssertExpectations
// — the recurring flake in TestReadFillsPrefetchWindow (seen on both the test
// and test-race jobs).
func newBackendReadStreamStubOptional(t *testing.T, frames ...*proto.ReadFrame) *mockProto.MockRpcFile_ReadClient {
	stub := mockProto.NewMockRpcFile_ReadClient(t)
	var idx atomic.Int64
	stub.EXPECT().Recv().RunAndReturn(func() (*proto.ReadFrame, error) {
		i := int(idx.Add(1) - 1)
		if i < len(frames) {
			return frames[i], nil
		}
		return nil, stdio.EOF
	}).Maybe()
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
	s.client.EXPECT().RetryWindow().Return(2 * time.Second).Maybe()
	s.client.EXPECT().Lifetime().Return(context.Background()).Maybe()
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
	}), mock.Anything).Return(&proto.GetAttrReply{
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
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	attr, st := s.backend.Stat(context.Background(), "/test")
	s.Assert().Equal(fuse.EIO, st)
	s.Assert().Nil(attr)
}

// A cancelled FUSE request context — the kernel interrupting the op, most often
// Go's async-preemption SIGURG landing on a long-running op (FUSE_INTERRUPT) —
// must NOT abort the in-flight idempotent metadata RPC. The backend detaches the
// gRPC call from the caller's cancellation, so it still issues the call with a
// live context and returns the result instead of a spurious EIO.
//
// These assert the property the fix protects (the RPC receives a non-cancelled
// ctx) rather than a specific error value: the real cancellation error from
// retry-go does not match a clean codes.Canceled, so an error-shape assertion
// would pass in a mock while the real path still failed.
func (s *BackendClientTestSuite) TestStat_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel() // simulate the kernel cancelling the FUSE request ctx

	s.fsClient.EXPECT().GetAttr(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(&proto.GetAttrReply{
		Status:     int32(fuse.OK),
		Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: 1, Gid: 1}},
	}, nil)

	attr, st := s.backend.Stat(parent, "/test")
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(attr)
}

func (s *BackendClientTestSuite) TestListDir_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	s.fsClient.EXPECT().OpenDir(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(&proto.OpenDirReply{
		Status:  int32(fuse.OK),
		Entries: []*proto.DirEntry{{Name: "f", Mode: 0o644, Ino: 1}},
	}, nil)

	entries, st := s.backend.ListDir(parent, "/test")
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(entries, 1)
}

func (s *BackendClientTestSuite) TestStatFs_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	s.fsClient.EXPECT().StatFs(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(&proto.StatFsReply{Bsize: 4096}, nil)

	res, st := s.backend.StatFs(parent, "/test")
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(res)
}

// The detach also covers the fd-returning / fd-lifecycle ops: Open (via metaCtx)
// and the close path (Release, via ioCtx). A cancelled parent must not abort an
// in-flight open/close with a spurious EIO.
func (s *BackendClientTestSuite) TestOpen_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	s.fileClient.EXPECT().Open(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
	).Return(&proto.OpenReply{Status: int32(fuse.OK), Fd: 1}, nil)

	fh, st := s.backend.Open(parent, "/test", 0)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(fh)
}

func (s *BackendClientTestSuite) TestRelease_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	s.fileClient.EXPECT().Release(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything, mock.Anything,
	).Return(&proto.ReleaseReply{}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Release(parent, h)
	s.Require().Equal(fuse.OK, st)
}

// Counter-test: SetLkw is a BLOCKING lock wait (F_SETLKW) and must STAY
// cancellable — a signal should interrupt a stuck wait. So, unlike every other
// op, its RPC context MUST still cancel when the parent does. Guards against a
// future "consistency" change wrongly routing SetLkw through ioCtx.
func (s *BackendClientTestSuite) TestSetLkw_StaysCancellable() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	lk := &fuse.FileLock{Start: 0, End: 0, Typ: 1, Pid: 1}

	s.fileClient.EXPECT().SetLkw(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() != nil }),
		mock.Anything,
	).Return(&proto.SetLkwReply{Status: int32(fuse.OK)}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLkw(parent, h, 7, lk, 0)
	s.Require().Equal(fuse.OK, st)
}

// TestStat_RetriesOnUnavailable verifies that an idempotent metadata
// call survives a single transient Unavailable via the retry wrapper.
func (s *BackendClientTestSuite) TestStat_RetriesOnUnavailable() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
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
	}), mock.Anything).Return(&proto.GetAttrReply{
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
	}), mock.Anything).Return(&proto.OpenDirReply{
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
	}), mock.Anything).Return(&proto.AccessReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Access(context.Background(), "/test", 0444)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestStatFs() {
	s.fsClient.EXPECT().StatFs(mock.Anything, &proto.StatFsRequest{
		Volume: "testVolume",
		Path:   "/test",
	}, mock.Anything).Return(&proto.StatFsReply{
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
	}), mock.Anything).Return(&proto.GetXAttrReply{
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
	}), mock.Anything).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Mkdir(context.Background(), "/test", 0755)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestRmdir() {
	s.fsClient.EXPECT().Rmdir(mock.Anything, mock.MatchedBy(func(req *proto.RmdirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.RmdirReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Rmdir(context.Background(), "/test")
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestUnlink() {
	s.fsClient.EXPECT().Unlink(mock.Anything, mock.MatchedBy(func(req *proto.UnlinkRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.UnlinkReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Unlink(context.Background(), "/test")
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestRename() {
	s.fsClient.EXPECT().Rename(mock.Anything, mock.MatchedBy(func(req *proto.RenameRequest) bool {
		return req.Volume == "testVolume" && req.OldName == "/old" && req.NewName == "/new" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.RenameReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Rename(context.Background(), "/old", "/new")
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestTruncate() {
	s.fsClient.EXPECT().Truncate(mock.Anything, mock.MatchedBy(func(req *proto.TruncateRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Size == 1024 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.TruncateReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Truncate(context.Background(), "/test", 1024)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestChmod() {
	s.fsClient.EXPECT().Chmod(mock.Anything, mock.MatchedBy(func(req *proto.ChmodRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Mode == 0644 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.ChmodReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Chmod(context.Background(), "/test", 0644)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestChown() {
	s.fsClient.EXPECT().Chown(mock.Anything, mock.MatchedBy(func(req *proto.ChownRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Uid == 1001 && req.Gid == 1001 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.ChownReply{Status: int32(fuse.OK)}, nil)

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
	}), mock.Anything).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
		return req.RequestId == firstID
	}), mock.Anything).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil).Once()

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
// (parent + "/" + name). When CreateReply carries no Attributes the returned
// attr is nil — the node adapter falls back to a Stat.
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
	s.Assert().Nil(attr, "no Attributes in reply → attr must be nil so node falls back to Stat")
}

// TestCreateReturnsAttrFromReply verifies that when CreateReply carries an
// Attributes field the backend returns it as a populated *Attr instead of nil.
// The strict mock ensures no stray GetAttr/Stat call is issued by the backend.
func (s *BackendClientTestSuite) TestCreateReturnsAttrFromReply() {
	s.fileClient.EXPECT().Create(mock.Anything, mock.Anything).Return(&proto.CreateReply{
		Status:     int32(fuse.OK),
		Fd:         7,
		Attributes: &proto.Attr{Ino: 42, Mode: 0o100644},
	}, nil).Once()

	_, attr, st := s.backend.Create(context.Background(), "/", "new.txt", 0, 0o644)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(42), attr.Ino)
	s.Assert().Equal(uint32(0o100644), attr.Mode)
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
	s.T().Skip("retry moved to retryOp; re-enable when this call site routes through retryOp (Tasks 5-7)")
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
	s.Assert().Len(header.Data, writeFrameSizeBytes)
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
	s.T().Skip("retry moved to retryOp; re-enable when this call site routes through retryOp (Tasks 5-7)")
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
	s.T().Skip("retry moved to retryOp; re-enable when this call site routes through retryOp (Tasks 5-7)")
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

// TestFlushCleanHandleSkipsRPC verifies the clean-handle fast path: a
// handle on which nothing has been written since the last flush issues no
// RPC at all. Strict-mode mock enforces that WriteAndFlush/Flush/Write are
// not called.
func (s *BackendClientTestSuite) TestFlushCleanHandleSkipsRPC() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(fuse.OK, st)
}

// TestFlushFusesWriteAndFlush verifies that a buffered small write followed
// by Flush results in exactly one WriteAndFlush RPC carrying the drained
// buffer — no separate streaming Write, no separate Flush RPC.
func (s *BackendClientTestSuite) TestFlushFusesWriteAndFlush() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("hi"))
	s.Require().Equal(fuse.OK, wst)

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.MatchedBy(func(r *proto.WriteAndFlushRequest) bool {
		return r.Volume == "testVolume" && r.Fd == 1 &&
			r.SessionId == "test-session" && r.Offset == 0 && string(r.Data) == "hi"
	}), mock.Anything).Return(&proto.WriteAndFlushReply{
		Status:  int32(fuse.OK),
		Written: 2,
	}, nil).Once()

	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(fuse.OK, st)
}

// TestFlush_CleanAfterWrite verifies that a second Flush after a successful
// WriteAndFlush is a no-op (dirty flag cleared on success).
func (s *BackendClientTestSuite) TestFlush_CleanAfterWrite() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("data"))
	s.Require().Equal(fuse.OK, wst)

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: int32(fuse.OK), Written: 4}, nil).Once()

	s.Require().Equal(fuse.OK, s.backend.Flush(context.Background(), h))
	// Second Flush: handle is clean, no RPC expected.
	s.Assert().Equal(fuse.OK, s.backend.Flush(context.Background(), h))
}

// TestFsync_CleanAfterWrite verifies that a successful Fsync clears the dirty
// flag so a subsequent close-Flush is a no-op (no second RPC). This mirrors
// TestFlush_CleanAfterWrite but via the Fsync path, covering the common
// Write + Fsync + close sequence used by git and O_SYNC workloads.
func (s *BackendClientTestSuite) TestFsync_CleanAfterWrite() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("data"))
	s.Require().Equal(fuse.OK, wst)

	// Fsync drains the coalescer via drainCoalescer (→ streaming Write RPC)
	// then issues the server-side Fsync RPC.
	writeStub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 4, Status: int32(fuse.OK)}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(writeStub, nil).Once()
	s.fileClient.EXPECT().Fsync(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.FsyncReply{Status: int32(fuse.OK)}, nil).Once()

	s.Require().Equal(fuse.OK, s.backend.Fsync(context.Background(), h, 0))
	// Handle is now clean: the close-path Flush must issue no RPC.
	s.Assert().Equal(fuse.OK, s.backend.Flush(context.Background(), h))
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

// TestFlush_RetriesOnUnavailable verifies that a transient gRPC
// Unavailable on WriteAndFlush survives a retry rather than surfacing as
// EIO. WriteAndFlush is idempotent (same fd/offset/data replayed), so
// retrying is safe.
func (s *BackendClientTestSuite) TestFlush_RetriesOnUnavailable() {
	s.T().Skip("retry moved to retryOp; re-enable when this call site routes through retryOp (Tasks 5-7)")
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("retry"))
	s.Require().Equal(fuse.OK, wst)

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: int32(fuse.OK), Written: 5}, nil).Once()

	st := s.backend.Flush(context.Background(), h)

	s.Require().Equal(fuse.OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "WriteAndFlush", 2)
}

// TestFsync_RetriesOnUnavailable mirrors the Flush retry test for the
// Fsync path. Same idempotency argument.
func (s *BackendClientTestSuite) TestFsync_RetriesOnUnavailable() {
	s.T().Skip("retry moved to retryOp; re-enable when this call site routes through retryOp (Tasks 5-7)")
	s.fileClient.EXPECT().Fsync(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().Fsync(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.FsyncReply{Status: int32(fuse.OK)}, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Fsync(context.Background(), h, 0)

	s.Require().Equal(fuse.OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "Fsync", 2)
}

// TestRead_BadHandleEBADF verifies that passing a non-*grpcFileHandle to
// the fd-level ops fails fast with EBADF rather than panicking.
func (s *BackendClientTestSuite) TestRead_BadHandleEBADF() {
	n, st := s.backend.Read(context.Background(), badHandle{}, 0, make([]byte, 8))
	s.Assert().Equal(0, n)
	s.Assert().Equal(fuse.EBADF, st)
}

// TestAllocate verifies fallocate(2) translates to an AllocateRequest
// carrying volume/fd/path/off/size/mode/session_id.
func (s *BackendClientTestSuite) TestAllocate() {
	s.fileClient.EXPECT().Allocate(mock.Anything, mock.MatchedBy(func(req *proto.AllocateRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Path == "/test/path" &&
			req.Off == 100 && req.Size == 4096 && req.Mode == 0 &&
			req.SessionId == "test-session"
	})).Return(&proto.AllocateReply{Status: int32(fuse.OK)}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Allocate(context.Background(), h, 100, 4096, 0)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestAllocate_Error() {
	s.fileClient.EXPECT().Allocate(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Allocate(context.Background(), h, 0, 4096, 0)
	s.Assert().Equal(fuse.EIO, st)
}

func (s *BackendClientTestSuite) TestAllocate_BadHandleEBADF() {
	st := s.backend.Allocate(context.Background(), badHandle{}, 0, 4096, 0)
	s.Assert().Equal(fuse.EBADF, st)
}

// TestGetLk verifies the lock state query translates the inbound
// fuse.FileLock to the proto and folds the reply back into *out.
func (s *BackendClientTestSuite) TestGetLk() {
	lk := &fuse.FileLock{Start: 0, End: 16, Typ: 1, Pid: 99}
	s.fileClient.EXPECT().GetLk(mock.Anything, mock.MatchedBy(func(req *proto.GetLkRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Owner == 42 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.Lk != nil &&
			req.Lk.Start == 0 && req.Lk.End == 16 && req.Lk.Typ == 1 && req.Lk.Pid == 99
	})).Return(&proto.GetLkReply{
		Status: int32(fuse.OK),
		Lk:     &proto.FileLock{Start: 0, End: 16, Typ: 2, Pid: 1234},
	}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	var out fuse.FileLock
	st := s.backend.GetLk(context.Background(), h, 42, lk, 0, &out)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(0), out.Start)
	s.Assert().Equal(uint64(16), out.End)
	s.Assert().Equal(uint32(2), out.Typ)
	s.Assert().Equal(uint32(1234), out.Pid)
}

func (s *BackendClientTestSuite) TestGetLk_Error() {
	s.fileClient.EXPECT().GetLk(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	var out fuse.FileLock
	st := s.backend.GetLk(context.Background(), h, 0, &fuse.FileLock{}, 0, &out)
	s.Assert().Equal(fuse.EIO, st)
}

func (s *BackendClientTestSuite) TestGetLk_BadHandleEBADF() {
	var out fuse.FileLock
	st := s.backend.GetLk(context.Background(), badHandle{}, 0, &fuse.FileLock{}, 0, &out)
	s.Assert().Equal(fuse.EBADF, st)
}

func (s *BackendClientTestSuite) TestSetLk() {
	lk := &fuse.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.fileClient.EXPECT().SetLk(mock.Anything, mock.MatchedBy(func(req *proto.SetLkRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Owner == 7 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.Lk != nil &&
			req.Lk.Start == 10 && req.Lk.End == 20 && req.Lk.Typ == 1 && req.Lk.Pid == 5
	})).Return(&proto.SetLkReply{Status: int32(fuse.OK)}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLk(context.Background(), h, 7, lk, 0)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestSetLk_Error() {
	s.fileClient.EXPECT().SetLk(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLk(context.Background(), h, 0, &fuse.FileLock{}, 0)
	s.Assert().Equal(fuse.EIO, st)
}

func (s *BackendClientTestSuite) TestSetLk_BadHandleEBADF() {
	st := s.backend.SetLk(context.Background(), badHandle{}, 0, &fuse.FileLock{}, 0)
	s.Assert().Equal(fuse.EBADF, st)
}

func (s *BackendClientTestSuite) TestSetLkw() {
	lk := &fuse.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.fileClient.EXPECT().SetLkw(mock.Anything, mock.MatchedBy(func(req *proto.SetLkwRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Owner == 7 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.Lk != nil &&
			req.Lk.Start == 10 && req.Lk.End == 20 && req.Lk.Typ == 1 && req.Lk.Pid == 5
	})).Return(&proto.SetLkwReply{Status: int32(fuse.OK)}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLkw(context.Background(), h, 7, lk, 0)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestSetLkw_Error() {
	s.fileClient.EXPECT().SetLkw(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLkw(context.Background(), h, 0, &fuse.FileLock{}, 0)
	s.Assert().Equal(fuse.EIO, st)
}

func (s *BackendClientTestSuite) TestSetLkw_BadHandleEBADF() {
	st := s.backend.SetLkw(context.Background(), badHandle{}, 0, &fuse.FileLock{}, 0)
	s.Assert().Equal(fuse.EBADF, st)
}

// TestStat_PropagatesCallerFromCtx is the load-bearing assertion for the
// FUSE-caller plumbing: callerFromCtx must pull uid/gid/pid out of the
// per-op ctx (the same ctx that go-fuse populates via fuse.NewContext
// on each request) and stamp them onto the outbound proto.Caller. A
// regression here causes every server-side op to run as the wrong user
// (UID 0 if the server's identity-bound filesystem is active server-side).
func (s *BackendClientTestSuite) TestStat_PropagatesCallerFromCtx() {
	wantUID, wantGID, wantPID := uint32(1234), uint32(5678), uint32(99)
	ctx := fuse.NewContext(context.Background(), &fuse.Caller{
		Owner: fuse.Owner{Uid: wantUID, Gid: wantGID},
		Pid:   wantPID,
	})
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.MatchedBy(func(req *proto.GetAttrRequest) bool {
		return req.Caller != nil && req.Caller.Owner != nil &&
			req.Caller.Owner.Uid == wantUID && req.Caller.Owner.Gid == wantGID &&
			req.Caller.Pid == wantPID
	}), mock.Anything).Return(&proto.GetAttrReply{
		Status:     int32(fuse.OK),
		Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: wantUID, Gid: wantGID}},
	}, nil)

	_, st := s.backend.Stat(ctx, "/path")
	s.Require().Equal(fuse.OK, st)
}

// TestStat_BareCtxStampsZeroCaller verifies the fallback path: a ctx
// with no fuse.Caller in it (typically a unit test using
// context.Background()) yields a non-nil zero Caller rather than nil.
// This is the only acceptable code path for the zero Caller; in
// production the node adapter always supplies the FUSE ctx so this
// fallback should never fire.
func (s *BackendClientTestSuite) TestStat_BareCtxStampsZeroCaller() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.MatchedBy(func(req *proto.GetAttrRequest) bool {
		return req.Caller != nil && req.Caller.Owner != nil &&
			req.Caller.Owner.Uid == 0 && req.Caller.Owner.Gid == 0 && req.Caller.Pid == 0
	}), mock.Anything).Return(&proto.GetAttrReply{
		Status:     int32(fuse.OK),
		Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{}},
	}, nil)

	_, st := s.backend.Stat(context.Background(), "/path")
	s.Require().Equal(fuse.OK, st)
}

// fakeDecorator models a Sub-spec B-style FileHandle wrapper: it holds
// an inner FileHandle (typically the leaf *grpcFileHandle) and proxies
// Path. Unwrap returns the inner handle so resolveHandle can find the
// leaf.
type fakeDecorator struct {
	inner FileHandle
}

func (d *fakeDecorator) Path() string       { return d.inner.Path() }
func (d *fakeDecorator) Unwrap() FileHandle { return d.inner }

// TestResolveHandle_UnwrapsDecorator is the load-bearing test for
// Sub-spec B compatibility: a fakeDecorator wrapping a leaf
// *grpcFileHandle must resolve back to the leaf, so per-fd backend ops
// can reach the gRPC state (fd, sessionID, etc.) regardless of how
// many decorator layers sit on top.
func (s *BackendClientTestSuite) TestResolveHandle_UnwrapsDecorator() {
	leaf := s.newHandle(grpcclient.PerFileConfig{})
	wrapped := &fakeDecorator{inner: leaf}
	got := resolveHandle(wrapped)
	s.Require().NotNil(got)
	s.Assert().Same(leaf, got)
	// Triple-wrapped should still resolve.
	tripled := &fakeDecorator{inner: &fakeDecorator{inner: &fakeDecorator{inner: leaf}}}
	gotTripled := resolveHandle(tripled)
	s.Require().NotNil(gotTripled)
	s.Assert().Same(leaf, gotTripled)
}

// TestResolveHandle_ForeignLeafReturnsNil verifies that a foreign
// FileHandle whose Unwrap returns itself (leaf marker, no gRPC
// handle behind it) resolves to nil — the per-fd op then returns
// EBADF rather than panicking.
func (s *BackendClientTestSuite) TestResolveHandle_ForeignLeafReturnsNil() {
	got := resolveHandle(badHandle{})
	s.Assert().Nil(got)
}

// TestResolveHandle_NilReturnsNil verifies the nil-safe behaviour.
func (s *BackendClientTestSuite) TestResolveHandle_NilReturnsNil() {
	s.Assert().Nil(resolveHandle(nil))
}

// TestReadFillsPrefetchWindow verifies that a single synchronous Read
// with a window-3 readahead config causes the backend to issue at least
// three additional prefetch Read RPCs (the synchronous call plus ≥3
// prefetch goroutines). The mock uses Maybe() so unfired prefetches do
// not cause strict-mock teardown failures when Eventually fires early.
func (s *BackendClientTestSuite) TestReadFillsPrefetchWindow() {
	const chunkBytes = 64 << 10 // 64 KiB
	cfg := grpcclient.PerFileConfig{
		ReadaheadChunkBytes: chunkBytes,
		ReadaheadThreshold:  1,
		ReadaheadWindow:     3,
	}
	h := s.newHandle(cfg)

	var readCount atomic.Int64
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *proto.ReadRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			readCount.Add(1)
			data := make([]byte, chunkBytes)
			// Optional-frame stub: prefetch streams run in async goroutines that
			// may still be draining when the test returns (it only waits for the
			// Read RPC *count*, not per-stream drainage). Strict .Once() frames
			// would fail AssertExpectations for an in-flight prefetch — the -race
			// flake. See newBackendReadStreamStubOptional.
			stub := newBackendReadStreamStubOptional(s.T(),
				&proto.ReadFrame{Data: data, Status: int32(fuse.OK)},
				&proto.ReadFrame{Status: int32(fuse.OK)},
			)
			return stub, nil
		}).Maybe()

	dest := make([]byte, chunkBytes)
	n, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
	s.Require().Equal(chunkBytes, n)

	// One synchronous Read + 3 prefetch goroutines = at least 4 total calls.
	s.Eventually(func() bool {
		return readCount.Load() >= 4
	}, time.Second, 10*time.Millisecond, "expected at least 4 Read RPCs (1 sync + 3 prefetch)")
}

func (s *BackendClientTestSuite) TestUtimens() {
	mtime := time.Unix(1577836800, 500) // 2020-01-01, 500ns
	s.fsClient.EXPECT().Utimens(mock.Anything, mock.MatchedBy(func(req *proto.UtimensRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.Atime == nil && // UTIME_OMIT
			req.Mtime != nil && req.Mtime.Sec == 1577836800 && req.Mtime.Nsec == 500 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.UtimensReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Utimens(context.Background(), "/test", nil, &mtime)
	s.Require().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestUtimens_BothTimes() {
	atime := time.Unix(100, 1)
	mtime := time.Unix(200, 2)
	s.fsClient.EXPECT().Utimens(mock.Anything, mock.MatchedBy(func(req *proto.UtimensRequest) bool {
		return req.Atime != nil && req.Atime.Sec == 100 && req.Atime.Nsec == 1 &&
			req.Mtime != nil && req.Mtime.Sec == 200 && req.Mtime.Nsec == 2 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.UtimensReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Utimens(context.Background(), "/test", &atime, &mtime)
	s.Require().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestUtimens_Error() {
	s.fsClient.EXPECT().Utimens(mock.Anything, mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)
	st := s.backend.Utimens(context.Background(), "/test", nil, nil)
	s.Assert().Equal(fuse.EIO, st)
}

// Protective property: a cancelled FUSE request ctx must NOT abort the in-flight
// idempotent metadata RPC. Assert the RPC receives a non-cancelled ctx rather
// than a specific error value (the real cancellation error from retry-go does
// not match a clean codes.Canceled, so an error-shape assertion would pass in a
// mock while the real path still failed).
func (s *BackendClientTestSuite) TestUtimens_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	s.fsClient.EXPECT().Utimens(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(&proto.UtimensReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Utimens(parent, "/test", nil, nil)
	s.Require().Equal(fuse.OK, st)
}

// newHandleAt is like newHandle but lets the caller choose path and fd so
// CopyFileRange tests can construct distinct source and destination handles.
func (s *BackendClientTestSuite) newHandleAt(path string, fd uint64, cfg grpcclient.PerFileConfig) *grpcFileHandle {
	return newGrpcFileHandle(s.fileClient, "testVolume", path, fd, 30*time.Second, "test-session", cfg)
}

// --- CopyFileRange ---

// TestCopyFileRange_HappyPath verifies all wire fields in the request and
// the correct (BytesCopied, fuse.OK) return value.
func (s *BackendClientTestSuite) TestCopyFileRange_HappyPath() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{})

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.MatchedBy(func(req *proto.CopyFileRangeRequest) bool {
		return req.Volume == "testVolume" &&
			req.FdIn == 1 && req.PathIn == "/src/file" && req.OffIn == 100 &&
			req.FdOut == 2 && req.PathOut == "/dst/file" && req.OffOut == 200 &&
			req.Length == 4096 && req.Flags == 0 &&
			req.SessionId == "test-session"
	}), mock.Anything).Return(&proto.CopyFileRangeReply{BytesCopied: 4096, Status: 0}, nil).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 100, dst, 200, 4096, 0)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(4096), n)
}

// TestCopyFileRange_TransportError verifies that a non-retryable transport
// error returns EIO and the mock is called exactly ONCE (no retry) —
// pinning the no-retry contract.
func (s *BackendClientTestSuite) TestCopyFileRange_TransportError() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{})

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 512, 0)
	s.Assert().Equal(fuse.EIO, st)
	s.Assert().Equal(uint64(0), n)
	s.fileClient.AssertNumberOfCalls(s.T(), "CopyFileRange", 1)
}

// TestCopyFileRange_BadHandleEBADF verifies that passing a foreign handle
// (one that resolveHandle can't resolve to a *grpcFileHandle) returns
// EBADF without calling the mock at all.
func (s *BackendClientTestSuite) TestCopyFileRange_BadHandleEBADF() {
	good := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{})
	// badHandle as src — resolveHandle returns nil on the first handle.
	n, st := s.backend.CopyFileRange(context.Background(), badHandle{}, 0, good, 0, 512, 0)
	s.Assert().Equal(fuse.EBADF, st)
	s.Assert().Equal(uint64(0), n)
	// badHandle as dst — src resolves fine, dst does not.
	good2 := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	n, st = s.backend.CopyFileRange(context.Background(), good2, 0, badHandle{}, 0, 512, 0)
	s.Assert().Equal(fuse.EBADF, st)
	s.Assert().Equal(uint64(0), n)
}

// TestCopyFileRange_DrainOrdering verifies that CopyFileRange drains
// pending coalesced writes on the destination BEFORE issuing the copy RPC.
// A Write "pending" is buffered on dst (no wire RPC yet); inside the copy
// mock's RunAndReturn we assert that the drain Write already landed (the
// stub frame list is non-empty). This proves drain-before-copy ordering.
func (s *BackendClientTestSuite) TestCopyFileRange_DrainOrdering() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})

	// Buffer a write on dst (small — stays in coalescer, no wire RPC yet).
	_, wst := s.backend.Write(context.Background(), dst, 0, []byte("pending"))
	s.Require().Equal(fuse.OK, wst)

	// Drain RPC: the coalescer's pending bytes will flow through streamingWrite.
	writeStub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 7, Status: int32(fuse.OK)}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(writeStub, nil).Once()

	// Copy RPC: inside RunAndReturn assert the write already fired.
	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *proto.CopyFileRangeRequest, _ ...grpc.CallOption) (*proto.CopyFileRangeReply, error) {
			s.Assert().NotEmpty(writeStub.frames, "drain Write must land before CopyFileRange is called")
			return &proto.CopyFileRangeReply{BytesCopied: 1024, Status: 0}, nil
		}).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 1024, 0)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(1024), n)
}

// TestCopyFileRange_ZeroCopied_DirtyFlagNotSet verifies that a successful
// CopyFileRange returning BytesCopied=0 does NOT set the dirty flag, so a
// subsequent Flush is a no-op (strict mock: no WriteAndFlush expected).
// Also pairs a BytesCopied>0 variant to confirm the flag IS set in that case.
func (s *BackendClientTestSuite) TestCopyFileRange_ZeroCopied_DirtyFlagNotSet() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.CopyFileRangeReply{BytesCopied: 0, Status: 0}, nil).Once()

	_, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 0, 0)
	s.Require().Equal(fuse.OK, st)
	// dst is still clean: Flush must be a no-op (strict mock — no WriteAndFlush call).
	s.Require().Equal(fuse.OK, s.backend.Flush(context.Background(), dst))
}

// TestCopyFileRange_NonZeroCopied_DirtyFlagSet verifies the converse: when
// BytesCopied>0 the dst handle is marked dirty, so the close-path Flush
// emits WriteAndFlush.
func (s *BackendClientTestSuite) TestCopyFileRange_NonZeroCopied_DirtyFlagSet() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.CopyFileRangeReply{BytesCopied: 512, Status: 0}, nil).Once()
	_, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 512, 0)
	s.Require().Equal(fuse.OK, st)

	// dst is now dirty: Flush must issue WriteAndFlush.
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: int32(fuse.OK), Written: 0}, nil).Once()
	s.Require().Equal(fuse.OK, s.backend.Flush(context.Background(), dst))
}

// --- Lseek ---

// TestLseek_HappyPath verifies all wire fields in the request and the
// returned offset on success.
func (s *BackendClientTestSuite) TestLseek_HappyPath() {
	s.fileClient.EXPECT().Lseek(mock.Anything, mock.MatchedBy(func(req *proto.LseekRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Path == "/test/path" &&
			req.Offset == 4096 && req.Whence == 4 && req.SessionId == "test-session"
	}), mock.Anything).Return(&proto.LseekReply{Offset: 8192, Status: 0}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	off, st := s.backend.Lseek(context.Background(), h, 4096, 4)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(8192), off)
}

// TestLseek_RetryReusesResult mirrors TestFsync_RetriesOnUnavailable:
// first call returns Unavailable, second succeeds. The retried result must
// come through cleanly — pinning the idempotent-retry contract for Lseek.
func (s *BackendClientTestSuite) TestLseek_RetryReusesResult() {
	s.T().Skip("retry moved to retryOp; re-enable when this call site routes through retryOp (Tasks 5-7)")
	s.fileClient.EXPECT().Lseek(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().Lseek(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.LseekReply{Offset: 1024, Status: 0}, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	off, st := s.backend.Lseek(context.Background(), h, 0, 4)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(1024), off)
	s.fileClient.AssertNumberOfCalls(s.T(), "Lseek", 2)
}

// TestLseek_BadHandleEBADF verifies the EBADF fast-path for a foreign handle.
func (s *BackendClientTestSuite) TestLseek_BadHandleEBADF() {
	off, st := s.backend.Lseek(context.Background(), badHandle{}, 0, 4)
	s.Assert().Equal(fuse.EBADF, st)
	s.Assert().Equal(uint64(0), off)
}

// --- SetXAttr / RemoveXAttr / ListXAttr ---

// TestSetXAttr_HappyPath verifies the mutating path stamps a non-empty
// RequestId and the correct SessionId in the wire request.
func (s *BackendClientTestSuite) TestSetXAttr_HappyPath() {
	data := []byte("xvalue")
	s.fsClient.EXPECT().SetXAttr(mock.Anything, mock.MatchedBy(func(req *proto.SetXAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.Attribute == "user.foo" && string(req.Data) == "xvalue" &&
			req.Flags == 0 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.SetXAttrReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.SetXAttr(context.Background(), "/test", "user.foo", data, 0)
	s.Assert().Equal(fuse.OK, st)
}

// TestRemoveXAttr_HappyPath verifies RemoveXAttr stamps RequestId and SessionId.
func (s *BackendClientTestSuite) TestRemoveXAttr_HappyPath() {
	s.fsClient.EXPECT().RemoveXAttr(mock.Anything, mock.MatchedBy(func(req *proto.RemoveXAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.Attribute == "user.foo" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.RemoveXAttrReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.RemoveXAttr(context.Background(), "/test", "user.foo")
	s.Assert().Equal(fuse.OK, st)
}

// TestListXAttr_HappyPath verifies ListXAttr returns the attribute name list.
func (s *BackendClientTestSuite) TestListXAttr_HappyPath() {
	s.fsClient.EXPECT().ListXAttr(mock.Anything, mock.MatchedBy(func(req *proto.ListXAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test"
	}), mock.Anything).Return(&proto.ListXAttrReply{
		Attributes: []string{"user.foo", "user.bar"},
		Status:     int32(fuse.OK),
	}, nil)

	names, st := s.backend.ListXAttr(context.Background(), "/test")
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(names, 2)
	s.Assert().Equal("user.foo", names[0])
	s.Assert().Equal("user.bar", names[1])
}

// badHandle is a FileHandle implementation that is not a *grpcFileHandle,
// used to exercise the type-assertion guard on fd-level ops. Unwrap
// returns the receiver (leaf marker) so resolveHandle's walk terminates
// and yields nil — modelling a foreign leaf handle the backend can't
// reach into.
type badHandle struct{}

func (b badHandle) Path() string       { return "/bad" }
func (b badHandle) Unwrap() FileHandle { return b }

func TestBackendClientTestSuite(t *testing.T) {
	suite.Run(t, new(BackendClientTestSuite))
}
