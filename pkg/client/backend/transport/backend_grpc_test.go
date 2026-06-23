package transport

import (
	"context"
	stdio "io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	mockProto "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/client/backend"
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

// newReadDirStreamStub returns a MockRpcFs_ReadDirClient that yields the
// given batches in order via Recv(), then EOF. Mirrors
// newBackendReadStreamStub for the streaming ReadDir consumer.
func newReadDirStreamStub(t *testing.T, batches ...*proto.ReadDirBatch) *mockProto.MockRpcFs_ReadDirClient {
	stub := mockProto.NewMockRpcFs_ReadDirClient(t)
	for _, b := range batches {
		stub.EXPECT().Recv().Return(b, nil).Once()
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
		Volume:     f.Volume,
		Fd:         f.Fd,
		SessionId:  f.SessionId,
		RequestId:  f.RequestId,
		Offset:     f.Offset,
		Data:       append([]byte(nil), f.Data...),
		Delegation: f.Delegation, // preserve delegation for test assertions
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
	s.client.EXPECT().DataFileClient().Return(s.fileClient, func() {}).Maybe()
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
	s.client.EXPECT().BootEpoch().Return("epoch-1").Maybe()
	s.client.EXPECT().RetryWindow().Return(2 * time.Second).Maybe()
	s.client.EXPECT().Lifetime().Return(context.Background()).Maybe()
	// retryOp emits the retry counter through the client's Recorder on a retry;
	// nil is fine (the call site nil-checks) and these tests don't assert metrics.
	s.client.EXPECT().Metrics().Return(nil).Maybe()
	s.client.EXPECT().PerFileConfig().Return(grpcclient.PerFileConfig{}).Maybe()
	s.backend = NewBackendClient(s.client, "testVolume")
}

// newHandle constructs a handle for tests that exercise the fd-level RPCs
// (Read/Write/Flush/Release/Fsync). The return type is *walHandle: when
// cfg.WriteCoalesceBytes > 0 the walHandle owns a WriteCoalescer; when 0 the
// walHandle has no coalescer and write() passes straight to streamingWrite.
// This mirrors the production Open/Create path (newFileHandle in backend_grpc.go).
//
// All fd-level tests receive a *walHandle regardless of coalescing, which:
//   - allows the characterization tests to call h.takeWriteErr() directly
//   - keeps the production dispatch path (resolveWalHandle → wh.write/flush/…)
//     exercised even in non-coalescing tests
func (s *BackendClientTestSuite) newHandle(cfg grpcclient.PerFileConfig) *walHandle {
	leaf := newGrpcFileHandle(s.client, "testVolume", "/test/path", 1, 0, nil, 30*time.Second, "test-session", "epoch-1", cfg)
	return newWalHandle(s.backend, leaf, cfg.WriteCoalesceBytes, nil)
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
		Status:     proto.FsError_FS_OK,
		Attributes: testAttr,
	}, nil)

	attr, st := s.backend.Stat(context.Background(), "/test")

	s.Require().Equal(proto.FsError_FS_OK, st)
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
	s.Assert().Equal(proto.FsError_FS_EIO, st)
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
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: 1, Gid: 1}},
	}, nil)

	attr, st := s.backend.Stat(parent, "/test")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(attr)
}

func (s *BackendClientTestSuite) TestListDir_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{
		Status:  proto.FsError_FS_OK,
		Entries: []*proto.DirEntryPlus{{Entry: &proto.DirEntry{Name: "f", Mode: 0o644, Ino: 1}}},
	})
	s.fsClient.EXPECT().ReadDir(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(stream, nil)

	entries, st := s.backend.ListDir(parent, "/test")
	s.Require().Equal(proto.FsError_FS_OK, st)
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
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(res)
}

// The detach also covers the fd-returning / fd-lifecycle ops: Open and the
// close path (Release) — both now route through retryOp, which derives each
// attempt's ctx via context.WithoutCancel. A cancelled parent must not abort an
// in-flight open/close with a spurious EIO.
func (s *BackendClientTestSuite) TestOpen_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()

	s.fileClient.EXPECT().Open(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(&proto.OpenReply{Status: proto.FsError_FS_OK, Fd: 1}, nil)

	fh, st := s.backend.Open(parent, "/test", 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
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
	s.Require().Equal(proto.FsError_FS_OK, st)
}

// Counter-test: SetLkw is a BLOCKING lock wait (F_SETLKW) and must STAY
// cancellable — a signal should interrupt a stuck wait. So, unlike every other
// op, its RPC context MUST still cancel when the parent does. Guards against a
// future "consistency" change wrongly detaching SetLkw via retryOp.
func (s *BackendClientTestSuite) TestSetLkw_StaysCancellable() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	lk := &backend.FileLock{Start: 0, End: 0, Typ: 1, Pid: 1}

	s.fileClient.EXPECT().SetLkw(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() != nil }),
		mock.Anything,
	).Return(&proto.SetLkwReply{Status: proto.FsError_FS_OK}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLkw(parent, h, 7, lk, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
}

// TestStat_RetriesOnUnavailable verifies that an idempotent metadata
// call survives a single transient Unavailable via the retry wrapper.
func (s *BackendClientTestSuite) TestStat_RetriesOnUnavailable() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.GetAttrReply{
			Status:     proto.FsError_FS_OK,
			Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
		}, nil).Once()

	attr, st := s.backend.Stat(context.Background(), "file")

	s.Require().Equal(proto.FsError_FS_OK, st)
	s.NotNil(attr)
	s.fsClient.AssertNumberOfCalls(s.T(), "GetAttr", 2)
}

// TestLookup verifies Lookup is implemented atop GetAttr on the joined
// parent/name path. The returned Attr carries the inode.
func (s *BackendClientTestSuite) TestLookup() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.MatchedBy(func(req *proto.GetAttrRequest) bool {
		return req.Path == "/parent/child"
	}), mock.Anything).Return(&proto.GetAttrReply{
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Ino: 42, Mode: 0o644, Owner: &proto.Owner{Uid: 1, Gid: 1}},
	}, nil)

	attr, st := s.backend.Lookup(context.Background(), "/parent", "child")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(42), attr.Ino)
}

// TestListDir consumes the streaming ReadDir RPC. The default backend has
// plus listings DISABLED, so the request must carry Plus=false.
func (s *BackendClientTestSuite) TestListDir() {
	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{
		Status: proto.FsError_FS_OK,
		Entries: []*proto.DirEntryPlus{
			{Entry: &proto.DirEntry{Name: "file1", Mode: 0644, Ino: 1}},
			{Entry: &proto.DirEntry{Name: "file2", Mode: 0644, Ino: 2}},
		},
	})
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.MatchedBy(func(req *proto.ReadDirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && !req.Plus
	}), mock.Anything).Return(stream, nil)

	result, st := s.backend.ListDir(context.Background(), "/test")

	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(result, 2)
	s.Assert().Equal("file1", result[0].Name)
	s.Assert().Equal("file2", result[1].Name)
	s.Assert().Equal(uint64(1), result[0].Ino)
	s.Assert().Nil(result[0].Attr, "plus disabled: no per-entry attrs")
}

// TestListDir_MultiBatchAccumulates: a large directory streams several
// batches; entries accumulate in order across batches.
func (s *BackendClientTestSuite) TestListDir_MultiBatchAccumulates() {
	stream := newReadDirStreamStub(s.T(),
		&proto.ReadDirBatch{Status: proto.FsError_FS_OK, Entries: []*proto.DirEntryPlus{
			{Entry: &proto.DirEntry{Name: "a", Ino: 1}},
			{Entry: &proto.DirEntry{Name: "b", Ino: 2}},
		}},
		&proto.ReadDirBatch{Status: proto.FsError_FS_OK, Entries: []*proto.DirEntryPlus{
			{Entry: &proto.DirEntry{Name: "c", Ino: 3}},
		}},
	)
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.Anything, mock.Anything).Return(stream, nil)

	result, st := s.backend.ListDir(context.Background(), "/big")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(result, 3)
	s.Assert().Equal([]string{"a", "b", "c"},
		[]string{result[0].Name, result[1].Name, result[2].Name})
}

// TestListDir_EmptyDir: the server sends one empty OK batch; the backend
// returns an empty NON-NIL slice (a real "directory with no entries" answer,
// distinguishable from the nil-on-error arm).
func (s *BackendClientTestSuite) TestListDir_EmptyDir() {
	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{Status: proto.FsError_FS_OK})
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.Anything, mock.Anything).Return(stream, nil)

	result, st := s.backend.ListDir(context.Background(), "/empty")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(result)
	s.Assert().Empty(result)
}

// TestListDir_TerminalStatusBatch: a fuse-level failure arrives as one
// terminal batch carrying the status; the backend surfaces it.
func (s *BackendClientTestSuite) TestListDir_TerminalStatusBatch() {
	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{Status: proto.FsError_FS_EACCES})
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.Anything, mock.Anything).Return(stream, nil)

	result, st := s.backend.ListDir(context.Background(), "/forbidden")
	s.Assert().Equal(proto.FsError_FS_EACCES, st)
	s.Assert().Nil(result)
}

// TestListDir_PlusFlagPropagatesAndMapsAttrs: a backend constructed with
// WithPlusListings(true) stamps Plus on the request, and per-entry attrs map
// through attrFromProto. An entry whose server-side stat failed (nil
// Attributes) yields a nil Attr without dropping the dirent.
func (s *BackendClientTestSuite) TestListDir_PlusFlagPropagatesAndMapsAttrs() {
	plusBackend := NewBackendClient(s.client, "testVolume", WithPlusListings(true))
	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{
		Status: proto.FsError_FS_OK,
		Entries: []*proto.DirEntryPlus{
			{
				Entry:      &proto.DirEntry{Name: "f", Mode: 0o644, Ino: 9},
				Attributes: &proto.Attr{Ino: 9, Size: 64, Mode: 0o100644, Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
			},
			{Entry: &proto.DirEntry{Name: "statfailed", Ino: 10}},
		},
	})
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.MatchedBy(func(req *proto.ReadDirRequest) bool {
		return req.Plus && req.Path == "/test"
	}), mock.Anything).Return(stream, nil)

	result, st := plusBackend.ListDir(context.Background(), "/test")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(result, 2)
	s.Require().NotNil(result[0].Attr)
	s.Assert().Equal(uint64(64), result[0].Attr.Size)
	s.Assert().Equal(uint32(1000), result[0].Attr.Uid)
	s.Assert().Nil(result[1].Attr, "per-entry stat failed server-side → nil Attr, dirent kept")
}

// TestListDir_RetriesOnUnavailable: like Read, each retry attempt opens a
// fresh ReadDir stream after a transient Unavailable.
func (s *BackendClientTestSuite) TestListDir_RetriesOnUnavailable() {
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{
		Status:  proto.FsError_FS_OK,
		Entries: []*proto.DirEntryPlus{{Entry: &proto.DirEntry{Name: "f", Ino: 1}}},
	})
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.Anything, mock.Anything).
		Return(stream, nil).Once()

	result, st := s.backend.ListDir(context.Background(), "/test")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(result, 1)
	s.fsClient.AssertNumberOfCalls(s.T(), "ReadDir", 2)
}

// TestListDirMapsXattrNames: a backend constructed with WithXattrListings(true)
// stamps WithXattr on the request and maps XattrListed/XattrNames from the
// per-entry reply into io.DirEntryPlus.
func (s *BackendClientTestSuite) TestListDirMapsXattrNames() {
	xattrBackend := NewBackendClient(s.client, "testVolume", WithXattrListings(true))
	stream := newReadDirStreamStub(s.T(), &proto.ReadDirBatch{
		Status: proto.FsError_FS_OK,
		Entries: []*proto.DirEntryPlus{
			{
				Entry:       &proto.DirEntry{Name: "f", Mode: 0o644},
				XattrListed: true,
				XattrNames:  []string{"user.a"},
			},
		},
	})
	s.fsClient.EXPECT().ReadDir(mock.Anything, mock.MatchedBy(func(req *proto.ReadDirRequest) bool {
		return req.WithXattr
	}), mock.Anything).Return(stream, nil)

	entries, st := xattrBackend.ListDir(context.Background(), "")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(entries, 1)
	s.True(entries[0].XattrListed)
	s.Equal([]string{"user.a"}, entries[0].XattrNames)
}

func (s *BackendClientTestSuite) TestAccess() {
	s.fsClient.EXPECT().Access(mock.Anything, mock.MatchedBy(func(req *proto.AccessRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Mode == 0444
	}), mock.Anything).Return(&proto.AccessReply{Status: proto.FsError_FS_OK}, nil)

	st := s.backend.Access(context.Background(), "/test", 0444)
	s.Assert().Equal(proto.FsError_FS_OK, st)
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
	s.Require().Equal(proto.FsError_FS_OK, st)
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
		Status: proto.FsError_FS_OK,
		Data:   expectedData,
	}, nil)

	data, st := s.backend.GetXAttr(context.Background(), "/test", "user.test")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(expectedData, data)
}

func (s *BackendClientTestSuite) TestMkdir() {
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.MatchedBy(func(req *proto.MkdirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Mode == 0755 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.MkdirReply{Status: proto.FsError_FS_OK}, nil)

	attr, st := s.backend.Mkdir(context.Background(), "/test", 0755)
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.Assert().Nil(attr, "no Attributes in reply → nil attr so callers fall back to Stat")
}

// TestMkdir_ReturnsAttrFromReply: MkdirReply.Attributes maps to a populated
// *Attr — the create-style attrs that let the node skip the trailing Stat.
// The strict mock proves no stray GetAttr is issued.
func (s *BackendClientTestSuite) TestMkdir_ReturnsAttrFromReply() {
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.Anything, mock.Anything).Return(&proto.MkdirReply{
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Ino: 11, Mode: 0o40755, Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
	}, nil).Once()

	attr, st := s.backend.Mkdir(context.Background(), "/test", 0o755)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(11), attr.Ino)
	s.Assert().Equal(uint32(0o40755), attr.Mode)
	s.Assert().Equal(uint32(1000), attr.Uid)
}

// TestMkdir_InBandErrorReturnsNilAttr: a non-OK status travels back with
// nil attrs even if the reply carried some.
func (s *BackendClientTestSuite) TestMkdir_InBandErrorReturnsNilAttr() {
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.MkdirReply{Status: proto.FsError_FS_EACCES}, nil).Once()

	attr, st := s.backend.Mkdir(context.Background(), "/test", 0o755)
	s.Assert().Equal(proto.FsError_FS_EACCES, st)
	s.Assert().Nil(attr)
}

// TestSymlink_ReturnsAttrFromReply mirrors TestMkdir_ReturnsAttrFromReply
// for the Symlink reply attrs, and pins the mutation wire shape
// (session_id + request_id stamped, target/link verbatim).
func (s *BackendClientTestSuite) TestSymlink_ReturnsAttrFromReply() {
	s.fsClient.EXPECT().Symlink(mock.Anything, mock.MatchedBy(func(req *proto.SymlinkRequest) bool {
		return req.Volume == "testVolume" && req.Target == "/target" && req.LinkPath == "/link" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.SymlinkReply{
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Ino: 12, Mode: 0o120777, Owner: &proto.Owner{Uid: 1, Gid: 1}},
	}, nil).Once()

	attr, st := s.backend.Symlink(context.Background(), "/target", "/link")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(12), attr.Ino)
	s.Assert().Equal(uint32(0o120777), attr.Mode)
}

// TestSymlink_NilAttrsInReply: server omits Attributes (its trailing stat
// failed) → nil attr with proto.FsError_FS_OK so the node falls back to Stat.
func (s *BackendClientTestSuite) TestSymlink_NilAttrsInReply() {
	s.fsClient.EXPECT().Symlink(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.SymlinkReply{Status: proto.FsError_FS_OK}, nil).Once()

	attr, st := s.backend.Symlink(context.Background(), "/target", "/link")
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.Assert().Nil(attr)
}

func (s *BackendClientTestSuite) TestRmdir() {
	s.fsClient.EXPECT().Rmdir(mock.Anything, mock.MatchedBy(func(req *proto.RmdirRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.RmdirReply{Status: proto.FsError_FS_OK}, nil)

	st := s.backend.Rmdir(context.Background(), "/test")
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

func (s *BackendClientTestSuite) TestUnlink() {
	s.fsClient.EXPECT().Unlink(mock.Anything, mock.MatchedBy(func(req *proto.UnlinkRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.UnlinkReply{Status: proto.FsError_FS_OK}, nil)

	st := s.backend.Unlink(context.Background(), "/test")
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

func (s *BackendClientTestSuite) TestRename() {
	s.fsClient.EXPECT().Rename(mock.Anything, mock.MatchedBy(func(req *proto.RenameRequest) bool {
		return req.Volume == "testVolume" && req.OldName == "/old" && req.NewName == "/new" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.RenameReply{Status: proto.FsError_FS_OK}, nil)

	st := s.backend.Rename(context.Background(), "/old", "/new")
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestSetAttr verifies the full wire mapping of the single-RPC SetAttr:
// FATTR bits pass through 1:1, value fields land in their proto slots, the
// omitted *time.Time maps to a nil FileTime (UTIME_OMIT) while the set one
// carries Sec/Nsec, session_id + request_id are stamped, and the reply's
// final attrs come back mapped — no follow-up GetAttr.
func (s *BackendClientTestSuite) TestSetAttr() {
	mtime := time.Unix(1577836800, 42)
	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.MatchedBy(func(req *proto.SetAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.Valid == fuse.FATTR_SIZE|fuse.FATTR_MODE|fuse.FATTR_MTIME &&
			req.Size == 1024 && req.Mode == 0o600 &&
			req.Atime == nil && // omitted timestamp ⇒ nil FileTime
			req.Mtime != nil && req.Mtime.Sec == 1577836800 && req.Mtime.Nsec == 42 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.SetAttrReply{
		Status: proto.FsError_FS_OK,
		Attributes: &proto.Attr{
			Ino: 7, Size: 1024, Mode: 0o600, Mtime: 1577836800, Mtimensec: 42,
			Owner: &proto.Owner{Uid: 1000, Gid: 1000},
		},
	}, nil).Once()

	attr, st := s.backend.SetAttr(context.Background(), "/test", backend.SetAttrIn{
		Valid: fuse.FATTR_SIZE | fuse.FATTR_MODE | fuse.FATTR_MTIME,
		Size:  1024,
		Mode:  0o600,
		Mtime: &mtime,
	})
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(7), attr.Ino)
	s.Assert().Equal(uint64(1024), attr.Size)
	s.Assert().Equal(uint32(1000), attr.Uid)
}

// TestSetAttr_MasksNonWireValidBits: only the six contract bits may reach
// the wire. Kernel-only bits (e.g. backend.FATTR_MTIME_NOW, backend.FATTR_FH) passed in
// Valid are stripped, never forwarded to a server that doesn't apply them.
func (s *BackendClientTestSuite) TestSetAttr_MasksNonWireValidBits() {
	mtime := time.Unix(100, 0)
	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.MatchedBy(func(req *proto.SetAttrRequest) bool {
		return req.Valid == uint32(fuse.FATTR_MTIME)
	}), mock.Anything).Return(&proto.SetAttrReply{Status: proto.FsError_FS_OK}, nil).Once()

	_, st := s.backend.SetAttr(context.Background(), "/test", backend.SetAttrIn{
		Valid: fuse.FATTR_MTIME | fuse.FATTR_MTIME_NOW | fuse.FATTR_FH,
		Mtime: &mtime,
	})
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestSetAttr_WireFattrContract pins the numeric FATTR_* values. The proto
// comment on SetAttrRequest.valid PROMISES these exact numbers (1=MODE 2=UID
// 4=GID 8=SIZE 16=ATIME 32=MTIME); the client forwards go-fuse's constants
// untranslated, so this pins the go-fuse constants we map from — if a go-fuse
// bump ever changed them, the wire contract would silently break without this.
func (s *BackendClientTestSuite) TestSetAttr_WireFattrContract() {
	s.Assert().Equal(1, fuse.FATTR_MODE)
	s.Assert().Equal(2, fuse.FATTR_UID)
	s.Assert().Equal(4, fuse.FATTR_GID)
	s.Assert().Equal(8, fuse.FATTR_SIZE)
	s.Assert().Equal(16, fuse.FATTR_ATIME)
	s.Assert().Equal(32, fuse.FATTR_MTIME)

	// De-fuse parity: the local backend.FATTR_* constants the contract now uses MUST
	// equal go-fuse's, since SetAttrIn.Valid still travels the wire untranslated.
	// If a go-fuse bump or an edit to backend.go diverges them, this fails here.
	s.Assert().Equal(fuse.FATTR_MODE, backend.FATTR_MODE)
	s.Assert().Equal(fuse.FATTR_UID, backend.FATTR_UID)
	s.Assert().Equal(fuse.FATTR_GID, backend.FATTR_GID)
	s.Assert().Equal(fuse.FATTR_SIZE, backend.FATTR_SIZE)
	s.Assert().Equal(fuse.FATTR_ATIME, backend.FATTR_ATIME)
	s.Assert().Equal(fuse.FATTR_MTIME, backend.FATTR_MTIME)
}

// TestSetAttr_RetryReusesRequestID — same Phase 1d idempotency property the
// Write/Mkdir suites pin: the request_id is allocated once outside the
// retry loop so the server's dedup cache short-circuits the replay.
//
// Implementation note: a side-effecting MatchedBy closure (capturing
// firstID inside the first expectation) is re-evaluated by testify when
// Arguments.Diff runs against exhausted .Once() expectations on the retry
// call, overwriting firstID with the retry's own id so the equality check
// passes trivially. Use Run() on mock.Anything expectations and collect
// every attempt's id into a slice; assert equality after the call.
func (s *BackendClientTestSuite) TestSetAttr_RetryReusesRequestID() {
	var ids []string
	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.SetAttrRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.SetAttrRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(&proto.SetAttrReply{
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Ino: 1, Owner: &proto.Owner{}},
	}, nil).Once()

	attr, st := s.backend.SetAttr(context.Background(), "/f", backend.SetAttrIn{Valid: fuse.FATTR_SIZE, Size: 0})
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.Assert().NotNil(attr)
	s.Require().Len(ids, 2, "expected exactly two SetAttr attempts")
	s.Assert().NotEmpty(ids[0], "request_id must be non-empty")
	s.Assert().Equal(ids[0], ids[1], "retry must reuse the same request_id for server-side dedup")
}

// TestSetAttr_InBandErrorReturnsStatus: a non-OK in-band Status travels back
// as the fuse.Status with nil attrs (the server stopped mid-sequence).
func (s *BackendClientTestSuite) TestSetAttr_InBandErrorReturnsStatus() {
	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.SetAttrReply{Status: proto.FsError_FS_EACCES}, nil).Once()

	attr, st := s.backend.SetAttr(context.Background(), "/f", backend.SetAttrIn{Valid: fuse.FATTR_MODE, Mode: 0o600})
	s.Assert().Equal(proto.FsError_FS_EACCES, st)
	s.Assert().Nil(attr)
}

// TestMkdir_RetryReusesRequestID is the load-bearing Phase 1d assertion
// for path-level mutating ops: the same request_id must be reused across
// retries so the server's dedup cache can short-circuit the duplicate.
//
// Same testify re-evaluation pitfall as TestSetAttr_RetryReusesRequestID:
// use Run() on mock.Anything expectations to collect ids without side
// effects in the matcher itself.
func (s *BackendClientTestSuite) TestMkdir_RetryReusesRequestID() {
	var ids []string
	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.MkdirRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	s.fsClient.EXPECT().Mkdir(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.MkdirRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(&proto.MkdirReply{Status: proto.FsError_FS_OK}, nil).Once()

	_, st := s.backend.Mkdir(context.Background(), "/d", 0755)
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(ids, 2, "expected exactly two Mkdir attempts")
	s.Assert().NotEmpty(ids[0], "request_id must be non-empty")
	s.Assert().Equal(ids[0], ids[1], "retry must reuse the same request_id for server-side dedup")
}

// --- Open / Create ---

func (s *BackendClientTestSuite) TestOpenReturnsHandle() {
	s.fileClient.EXPECT().Open(mock.Anything, mock.MatchedBy(func(req *proto.OpenRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" && req.Flags == 0 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.OpenReply{
		Status: proto.FsError_FS_OK,
		Fd:     1,
	}, nil)

	fh, st := s.backend.Open(context.Background(), "/test", 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(fh)
	s.Assert().IsType(&walHandle{}, fh, "Open wraps leaf in walHandle")
	s.Assert().Equal("/test", fh.Path())
}

func (s *BackendClientTestSuite) TestOpen_Error() {
	s.fileClient.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	fh, st := s.backend.Open(context.Background(), "/test", 0)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
	s.Assert().Nil(fh)
}

// TestCreateReturnsHandle verifies the joined-path semantics for Create
// (parent + "/" + name). When CreateReply carries no Attributes the returned
// attr is nil — the node adapter falls back to a Stat.
func (s *BackendClientTestSuite) TestCreateReturnsHandle() {
	s.fileClient.EXPECT().Create(mock.Anything, mock.MatchedBy(func(req *proto.CreateRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/dir/file" && req.Flags == 0 && req.Mode == 0644 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.CreateReply{
		Status: proto.FsError_FS_OK,
		Fd:     7,
	}, nil)

	fh, attr, st := s.backend.Create(context.Background(), "/dir", "file", 0, 0644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(fh)
	s.Assert().IsType(&walHandle{}, fh, "Create wraps leaf in walHandle")
	s.Assert().Nil(attr, "no Attributes in reply → attr must be nil so node falls back to Stat")
}

// TestCreateReturnsAttrFromReply verifies that when CreateReply carries an
// Attributes field the backend returns it as a populated *Attr instead of nil.
// The strict mock ensures no stray GetAttr/Stat call is issued by the backend.
func (s *BackendClientTestSuite) TestCreateReturnsAttrFromReply() {
	s.fileClient.EXPECT().Create(mock.Anything, mock.Anything, mock.Anything).Return(&proto.CreateReply{
		Status:     proto.FsError_FS_OK,
		Fd:         7,
		Attributes: &proto.Attr{Ino: 42, Mode: 0o100644},
	}, nil).Once()

	_, attr, st := s.backend.Create(context.Background(), "/", "new.txt", 0, 0o644)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(42), attr.Ino)
	s.Assert().Equal(uint32(0o100644), attr.Mode)
}

// --- fd-level ops ---

func (s *BackendClientTestSuite) TestRead_MultiFrame() {
	testData := []byte("test data")
	stream := newBackendReadStreamStub(s.T(),
		&proto.ReadFrame{Data: testData, Status: proto.FsError_FS_OK},
		&proto.ReadFrame{Status: proto.FsError_FS_OK},
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

	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(len(testData), n)
	s.Assert().Equal(testData, dest[:n])
}

func (s *BackendClientTestSuite) TestRead_ErrorReturnsEIO() {
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 1024)
	n, st := s.backend.Read(context.Background(), h, 0, dest)

	s.Assert().Equal(proto.FsError_FS_EIO, st)
	s.Assert().Equal(0, n)
}

// TestRead_RetriesOnUnavailable verifies that Read survives a single
// transient Unavailable. Each retry opens a fresh stream.
func (s *BackendClientTestSuite) TestRead_RetriesOnUnavailable() {
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	streamOK := newBackendReadStreamStub(s.T(),
		&proto.ReadFrame{Data: []byte("0123456789"), Status: proto.FsError_FS_OK},
		&proto.ReadFrame{Status: proto.FsError_FS_OK},
	)
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(streamOK, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	dest := make([]byte, 10)
	n, st := s.backend.Read(context.Background(), h, 0, dest)

	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(10, n)
	s.fileClient.AssertNumberOfCalls(s.T(), "Read", 2)
}

func (s *BackendClientTestSuite) TestWrite_SmallPayload() {
	testData := []byte("test data")
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{
		Written: uint32(len(testData)),
		Status:  proto.FsError_FS_OK,
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	written, st := s.backend.Write(context.Background(), h, 0, testData)

	s.Require().Equal(proto.FsError_FS_OK, st)
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
		Status:  proto.FsError_FS_OK,
	}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	written, st := s.backend.Write(context.Background(), h, 7, payload)

	s.Require().Equal(proto.FsError_FS_OK, st)
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
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "transient")).Once()
	stub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 5, Status: 0}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(stub, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	n, st := s.backend.Write(context.Background(), h, 0, []byte("hello"))
	s.Require().Equal(proto.FsError_FS_OK, st)
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

	s.Require().Equal(proto.FsError_FS_OK, st)
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
	s.Assert().Equal(proto.FsError_FS_EIO, st)
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
	s.Assert().Equal(proto.FsError_FS_OK, st)
	s.Assert().Error(h.leaf.lifeCtx.Err(), "lifeCtx must be cancelled after Release")
}

// TestFlushCleanHandleSkipsRPC verifies the clean-handle fast path: a
// handle on which nothing has been written since the last flush issues no
// RPC at all. Strict-mode mock enforces that WriteAndFlush/Flush/Write are
// not called.
func (s *BackendClientTestSuite) TestFlushCleanHandleSkipsRPC() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestFlushFusesWriteAndFlush verifies that a buffered small write followed
// by Flush results in exactly one WriteAndFlush RPC carrying the drained
// buffer — no separate streaming Write, no separate Flush RPC.
func (s *BackendClientTestSuite) TestFlushFusesWriteAndFlush() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("hi"))
	s.Require().Equal(proto.FsError_FS_OK, wst)

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.MatchedBy(func(r *proto.WriteAndFlushRequest) bool {
		return r.Volume == "testVolume" && r.Fd == 1 &&
			r.SessionId == "test-session" && r.Offset == 0 && string(r.Data) == "hi"
	}), mock.Anything).Return(&proto.WriteAndFlushReply{
		Status:  proto.FsError_FS_OK,
		Written: 2,
	}, nil).Once()

	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestFlush_CleanAfterWrite verifies that a second Flush after a successful
// WriteAndFlush is a no-op (dirty flag cleared on success).
func (s *BackendClientTestSuite) TestFlush_CleanAfterWrite() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("data"))
	s.Require().Equal(proto.FsError_FS_OK, wst)

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK, Written: 4}, nil).Once()

	s.Require().Equal(proto.FsError_FS_OK, s.backend.Flush(context.Background(), h))
	// Second Flush: handle is clean, no RPC expected.
	s.Assert().Equal(proto.FsError_FS_OK, s.backend.Flush(context.Background(), h))
}

// TestFsync_CleanAfterWrite verifies that a successful Fsync clears the dirty
// flag so a subsequent close-Flush is a no-op (no second RPC). This mirrors
// TestFlush_CleanAfterWrite but via the Fsync path, covering the common
// Write + Fsync + close sequence used by git and O_SYNC workloads.
func (s *BackendClientTestSuite) TestFsync_CleanAfterWrite() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("data"))
	s.Require().Equal(proto.FsError_FS_OK, wst)

	// Fsync drains the coalescer via drainCoalescer (→ streaming Write RPC)
	// then issues the server-side Fsync RPC.
	writeStub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 4, Status: proto.FsError_FS_OK}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(writeStub, nil).Once()
	s.fileClient.EXPECT().Fsync(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.FsyncReply{Status: proto.FsError_FS_OK}, nil).Once()

	s.Require().Equal(proto.FsError_FS_OK, s.backend.Fsync(context.Background(), h, 0))
	// Handle is now clean: the close-path Flush must issue no RPC.
	s.Assert().Equal(proto.FsError_FS_OK, s.backend.Flush(context.Background(), h))
}

func (s *BackendClientTestSuite) TestFsync() {
	s.fileClient.EXPECT().Fsync(mock.Anything, &proto.FsyncRequest{
		Volume:    "testVolume",
		Fd:        1,
		Flags:     0,
		SessionId: "test-session",
	}, mock.Anything).Return(&proto.FsyncReply{
		Status: proto.FsError_FS_OK,
	}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Fsync(context.Background(), h, 0)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestFlush_RetriesOnUnavailable verifies that a transient gRPC
// Unavailable on WriteAndFlush survives a retry rather than surfacing as
// EIO. WriteAndFlush is idempotent (same fd/offset/data replayed), so
// retrying is safe.
func (s *BackendClientTestSuite) TestFlush_RetriesOnUnavailable() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("retry"))
	s.Require().Equal(proto.FsError_FS_OK, wst)

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK, Written: 5}, nil).Once()

	st := s.backend.Flush(context.Background(), h)

	s.Require().Equal(proto.FsError_FS_OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "WriteAndFlush", 2)
}

// TestFlush_RetryReusesRequestID is the load-bearing Phase 1d assertion for
// WriteAndFlush: the request_id must be generated once outside the retry closure
// so the server's idempotency cache can short-circuit a replay of the
// coalesced-write half without re-executing it.
//
// Mutation-verify: moving uuid.NewString() inside the closure makes ids[0] !=
// ids[1] and the assertion fails.
func (s *BackendClientTestSuite) TestFlush_RetryReusesRequestID() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("retry-data"))
	s.Require().Equal(proto.FsError_FS_OK, wst)

	var ids []string
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.WriteAndFlushRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.WriteAndFlushRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK, Written: 10}, nil).Once()

	st := s.backend.Flush(context.Background(), h)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(ids, 2, "expected exactly two WriteAndFlush attempts")
	s.Assert().NotEmpty(ids[0], "request_id must be non-empty")
	s.Assert().Equal(ids[0], ids[1], "retry must reuse the same request_id for server-side dedup")
}

// TestFlush_RequestIdNonEmpty verifies that any WriteAndFlush RPC (flush with
// data from the coalescer) carries a non-empty RequestId — required for the
// server's idempotency cache to key on.
func (s *BackendClientTestSuite) TestFlush_RequestIdNonEmpty() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4096})
	_, wst := s.backend.Write(context.Background(), h, 0, []byte("data"))
	s.Require().Equal(proto.FsError_FS_OK, wst)

	var gotID string
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.WriteAndFlushRequest, _ ...grpc.CallOption) {
			gotID = req.RequestId
		}).Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK, Written: 4}, nil).Once()

	st := s.backend.Flush(context.Background(), h)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().NotEmpty(gotID, "WriteAndFlush RPC must carry a non-empty request_id")
}

// TestFlush_SurfacesLostCoalescedWriteErr is the WO-M1 regression: when a
// coalesced batch flush fails inside Write, the batch was already
// snapshot-and-cleared from the coalescer, so those acked bytes are gone. A
// later Flush finds an empty coalescer and must NOT report a spurious OK over
// the loss — it must surface the original write error exactly once.
//
// Setup: threshold 4. First write "ab" (2<4) buffers and returns OK. Second
// write "cd" at offset 2 is contiguous, fills the buffer to 4, so Append hands
// back the batch which streamingWrite sends — and the stub fails it in-band
// with ENOSPC. Write returns (0, ENOSPC). The bytes are lost; the coalescer is
// now empty. Flush must return ENOSPC (RED before the sticky-error fix: it
// found the handle dirty-but-empty, issued an empty WriteAndFlush, got OK).
func (s *BackendClientTestSuite) TestFlush_SurfacesLostCoalescedWriteErr() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4})

	// First small write buffers — no RPC.
	_, st1 := s.backend.Write(context.Background(), h, 0, []byte("ab"))
	s.Require().Equal(proto.FsError_FS_OK, st1)

	// Second contiguous write overflows the threshold → batch flush, which the
	// stub fails in-band with ENOSPC (no transport error, so no retry storm).
	failStub := newBackendWriteStreamStub(s.T(),
		&proto.WriteReply{Written: 0, Status: proto.FsError_FS_ENOSPC}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(failStub, nil).Once()

	_, st2 := s.backend.Write(context.Background(), h, 2, []byte("cd"))
	s.Require().Equal(proto.FsError_FS_ENOSPC, st2,
		"the failed batch flush must surface to FUSE immediately")

	// The coalescer is now empty but the handle is still dirty. Flush must
	// short-circuit on the sticky write-back error WITHOUT issuing any RPC: a
	// strict mock with no WriteAndFlush expectation enforces that, and the
	// returned status must be the lost write's ENOSPC, not a spurious OK.
	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_ENOSPC, st,
		"Flush must surface the lost coalesced-write error, not mask it with OK")

	// Sticky error is consumed exactly once: the handle is still dirty (the
	// failed write set dirty before buffering), so a second Flush proceeds to
	// the (now empty) WriteAndFlush rather than re-surfacing the cleared error.
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK}, nil).Once()
	s.Assert().Equal(proto.FsError_FS_OK, s.backend.Flush(context.Background(), h),
		"once consumed, the sticky error is not re-surfaced")
}

// TestFsync_SurfacesLostCoalescedWriteErr mirrors the Flush regression for the
// Fsync durability boundary: the same lost-batch sticky error must surface
// through Fsync too, not be masked by an empty-coalescer success.
func (s *BackendClientTestSuite) TestFsync_SurfacesLostCoalescedWriteErr() {
	h := s.newHandle(grpcclient.PerFileConfig{WriteCoalesceBytes: 4})

	_, st1 := s.backend.Write(context.Background(), h, 0, []byte("ab"))
	s.Require().Equal(proto.FsError_FS_OK, st1)

	failStub := newBackendWriteStreamStub(s.T(),
		&proto.WriteReply{Written: 0, Status: proto.FsError_FS_ENOSPC}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(failStub, nil).Once()

	_, st2 := s.backend.Write(context.Background(), h, 2, []byte("cd"))
	s.Require().Equal(proto.FsError_FS_ENOSPC, st2)

	// Strict mock: no Fsync RPC expected — the sticky error short-circuits.
	st := s.backend.Fsync(context.Background(), h, 0)
	s.Assert().Equal(proto.FsError_FS_ENOSPC, st,
		"Fsync must surface the lost coalesced-write error")
}

// TestFsync_RetriesOnUnavailable mirrors the Flush retry test for the
// Fsync path. Same idempotency argument.
func (s *BackendClientTestSuite) TestFsync_RetriesOnUnavailable() {
	s.fileClient.EXPECT().Fsync(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().Fsync(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.FsyncReply{Status: proto.FsError_FS_OK}, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Fsync(context.Background(), h, 0)

	s.Require().Equal(proto.FsError_FS_OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "Fsync", 2)
}

// TestRead_BadHandleEBADF verifies that passing a non-*grpcFileHandle to
// the fd-level ops fails fast with EBADF rather than panicking.
func (s *BackendClientTestSuite) TestRead_BadHandleEBADF() {
	n, st := s.backend.Read(context.Background(), badHandle{}, 0, make([]byte, 8))
	s.Assert().Equal(0, n)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
}

// TestAllocate verifies fallocate(2) translates to an AllocateRequest
// carrying volume/fd/path/off/size/mode/session_id.
func (s *BackendClientTestSuite) TestAllocate() {
	s.fileClient.EXPECT().Allocate(mock.Anything, mock.MatchedBy(func(req *proto.AllocateRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Path == "/test/path" &&
			req.Off == 100 && req.Size == 4096 && req.Mode == 0 &&
			req.SessionId == "test-session"
	}), mock.Anything).Return(&proto.AllocateReply{Status: proto.FsError_FS_OK}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Allocate(context.Background(), h, 100, 4096, 0)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

func (s *BackendClientTestSuite) TestAllocate_Error() {
	s.fileClient.EXPECT().Allocate(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.Allocate(context.Background(), h, 0, 4096, 0)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
}

func (s *BackendClientTestSuite) TestAllocate_BadHandleEBADF() {
	st := s.backend.Allocate(context.Background(), badHandle{}, 0, 4096, 0)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
}

// TestGetLk verifies the lock state query translates the inbound
// FileLock to the proto and folds the reply back into *out.
func (s *BackendClientTestSuite) TestGetLk() {
	lk := &backend.FileLock{Start: 0, End: 16, Typ: 1, Pid: 99}
	s.fileClient.EXPECT().GetLk(mock.Anything, mock.MatchedBy(func(req *proto.GetLkRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Owner == 42 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.Lk != nil &&
			req.Lk.Start == 0 && req.Lk.End == 16 && req.Lk.Typ == 1 && req.Lk.Pid == 99
	}), mock.Anything).Return(&proto.GetLkReply{
		Status: proto.FsError_FS_OK,
		Lk:     &proto.FileLock{Start: 0, End: 16, Typ: 2, Pid: 1234},
	}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	var out backend.FileLock
	st := s.backend.GetLk(context.Background(), h, 42, lk, 0, &out)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(0), out.Start)
	s.Assert().Equal(uint64(16), out.End)
	s.Assert().Equal(uint32(2), out.Typ)
	s.Assert().Equal(uint32(1234), out.Pid)
}

func (s *BackendClientTestSuite) TestGetLk_Error() {
	s.fileClient.EXPECT().GetLk(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	var out backend.FileLock
	st := s.backend.GetLk(context.Background(), h, 0, &backend.FileLock{}, 0, &out)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
}

func (s *BackendClientTestSuite) TestGetLk_BadHandleEBADF() {
	var out backend.FileLock
	st := s.backend.GetLk(context.Background(), badHandle{}, 0, &backend.FileLock{}, 0, &out)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
}

func (s *BackendClientTestSuite) TestSetLk() {
	lk := &backend.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.fileClient.EXPECT().SetLk(mock.Anything, mock.MatchedBy(func(req *proto.SetLkRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Owner == 7 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.Lk != nil &&
			req.Lk.Start == 10 && req.Lk.End == 20 && req.Lk.Typ == 1 && req.Lk.Pid == 5
	}), mock.Anything).Return(&proto.SetLkReply{Status: proto.FsError_FS_OK}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLk(context.Background(), h, 7, lk, 0)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

func (s *BackendClientTestSuite) TestSetLk_Error() {
	s.fileClient.EXPECT().SetLk(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLk(context.Background(), h, 0, &backend.FileLock{}, 0)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
}

func (s *BackendClientTestSuite) TestSetLk_BadHandleEBADF() {
	st := s.backend.SetLk(context.Background(), badHandle{}, 0, &backend.FileLock{}, 0)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
}

func (s *BackendClientTestSuite) TestSetLkw() {
	lk := &backend.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.fileClient.EXPECT().SetLkw(mock.Anything, mock.MatchedBy(func(req *proto.SetLkwRequest) bool {
		return req.Volume == "testVolume" && req.Fd == 1 && req.Owner == 7 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.Lk != nil &&
			req.Lk.Start == 10 && req.Lk.End == 20 && req.Lk.Typ == 1 && req.Lk.Pid == 5
	})).Return(&proto.SetLkwReply{Status: proto.FsError_FS_OK}, nil)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLkw(context.Background(), h, 7, lk, 0)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

func (s *BackendClientTestSuite) TestSetLkw_Error() {
	s.fileClient.EXPECT().SetLkw(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	h := s.newHandle(grpcclient.PerFileConfig{})
	st := s.backend.SetLkw(context.Background(), h, 0, &backend.FileLock{}, 0)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
}

func (s *BackendClientTestSuite) TestSetLkw_BadHandleEBADF() {
	st := s.backend.SetLkw(context.Background(), badHandle{}, 0, &backend.FileLock{}, 0)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
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
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: wantUID, Gid: wantGID}},
	}, nil)

	_, st := s.backend.Stat(ctx, "/path")
	s.Require().Equal(proto.FsError_FS_OK, st)
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
		Status:     proto.FsError_FS_OK,
		Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{}},
	}, nil)

	_, st := s.backend.Stat(context.Background(), "/path")
	s.Require().Equal(proto.FsError_FS_OK, st)
}

// fakeDecorator models a Sub-spec B-style FileHandle wrapper: it holds
// an inner FileHandle (typically the leaf *grpcFileHandle) and proxies
// Path. Unwrap returns the inner handle so resolveHandle can find the
// leaf.
type fakeDecorator struct {
	inner backend.FileHandle
}

func (d *fakeDecorator) Path() string               { return d.inner.Path() }
func (d *fakeDecorator) Unwrap() backend.FileHandle { return d.inner }

// TestResolveHandle_UnwrapsDecorator is the load-bearing test for
// Sub-spec B compatibility: a fakeDecorator wrapping a leaf
// *grpcFileHandle must resolve back to the leaf, so per-fd backend ops
// can reach the gRPC state (fd, sessionID, etc.) regardless of how
// many decorator layers sit on top.
//
// After Task 9b, s.newHandle returns a *walHandle. The innermost
// *grpcFileHandle (the leaf) is walHandle.leaf. resolveHandle must
// unwrap through walHandle → grpcFileHandle.
func (s *BackendClientTestSuite) TestResolveHandle_UnwrapsDecorator() {
	wh := s.newHandle(grpcclient.PerFileConfig{})
	leaf := wh.leaf // the *grpcFileHandle
	wrapped := &fakeDecorator{inner: wh}
	got := resolveHandle(wrapped)
	s.Require().NotNil(got)
	s.Assert().Same(leaf, got)
	// Triple-wrapped should still resolve.
	tripled := &fakeDecorator{inner: &fakeDecorator{inner: &fakeDecorator{inner: wh}}}
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
				&proto.ReadFrame{Data: data, Status: proto.FsError_FS_OK},
				&proto.ReadFrame{Status: proto.FsError_FS_OK},
			)
			return stub, nil
		}).Maybe()

	dest := make([]byte, chunkBytes)
	n, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Equal(chunkBytes, n)

	// One synchronous Read + 3 prefetch goroutines = at least 4 total calls.
	s.Eventually(func() bool {
		return readCount.Load() >= 4
	}, time.Second, 10*time.Millisecond, "expected at least 4 Read RPCs (1 sync + 3 prefetch)")
}

// TestRead_PartialPrefixFetchesOnlyTail: a FUSE read whose first half is
// covered by a warm prefetch chunk and whose second half is not gets the
// prefix from cache and issues exactly ONE live Read RPC, for exactly the
// uncovered tail range. The combined buffer is byte-correct.
func (s *BackendClientTestSuite) TestRead_PartialPrefixFetchesOnlyTail() {
	const chunkBytes = 1024
	cfg := grpcclient.PerFileConfig{
		ReadaheadChunkBytes: chunkBytes,
		ReadaheadThreshold:  1,
		ReadaheadWindow:     2,
	}
	h := s.newHandle(cfg)

	prefix := make([]byte, chunkBytes)
	tail := make([]byte, chunkBytes)
	for i := range prefix {
		prefix[i] = byte(i % 251)
		tail[i] = byte((i + 13) % 251)
	}
	// Warm the window: arm the slots ahead of cursor 1024 and store the chunk
	// at 1024. The slot at 2048 stays in flight, so a 2048-byte read at 1024
	// is exactly half covered.
	h.leaf.readahead.Observe(0, chunkBytes)
	h.leaf.readahead.Store(chunkBytes, prefix)

	var mu sync.Mutex
	var reqs []*proto.ReadRequest
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *proto.ReadRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			mu.Lock()
			reqs = append(reqs, req)
			mu.Unlock()
			if req.Offset == 2*chunkBytes {
				return newBackendReadStreamStubOptional(s.T(),
					&proto.ReadFrame{Data: tail, Status: proto.FsError_FS_OK},
					&proto.ReadFrame{Status: proto.FsError_FS_OK},
				), nil
			}
			// Async window-refill prefetches (offsets >= 3072); content is
			// irrelevant to this test.
			return newBackendReadStreamStubOptional(s.T(),
				&proto.ReadFrame{Data: make([]byte, chunkBytes), Status: proto.FsError_FS_OK},
				&proto.ReadFrame{Status: proto.FsError_FS_OK},
			), nil
		}).Maybe()

	dest := make([]byte, 2*chunkBytes)
	n, st := s.backend.Read(context.Background(), h, chunkBytes, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Equal(2*chunkBytes, n)
	s.Assert().Equal(prefix, dest[:chunkBytes], "first half must come from the warm chunk")
	s.Assert().Equal(tail, dest[chunkBytes:], "second half must come from the live tail RPC")

	// Exactly one synchronous RPC, for exactly the uncovered tail. Prefetch
	// goroutines re-filling the window target offsets >= 3072 and are the
	// only other allowed requests.
	mu.Lock()
	defer mu.Unlock()
	tailReqs := 0
	for _, req := range reqs {
		if req.Offset >= int64(3*chunkBytes) {
			continue // window-refill prefetch
		}
		s.Require().Equal(int64(2*chunkBytes), req.Offset, "no RPC may touch the cache-covered prefix")
		s.Require().Equal(uint32(chunkBytes), req.Size, "the live RPC must request exactly the tail")
		tailReqs++
	}
	s.Assert().Equal(1, tailReqs, "exactly one live RPC for the tail range")
}

// TestRead_PartialPrefixTailErrorNoShortRead: if the live tail fetch after a
// cache-served prefix fails, Read surfaces the error with n=0 — the kernel
// must never see a short/garbled read.
func (s *BackendClientTestSuite) TestRead_PartialPrefixTailErrorNoShortRead() {
	const chunkBytes = 1024
	cfg := grpcclient.PerFileConfig{
		ReadaheadChunkBytes: chunkBytes,
		ReadaheadThreshold:  1,
		ReadaheadWindow:     2,
	}
	h := s.newHandle(cfg)
	h.leaf.readahead.Observe(0, chunkBytes)
	h.leaf.readahead.Store(chunkBytes, make([]byte, chunkBytes))

	// The only RPC allowed is the tail fetch; it fails in-band with EIO.
	// The error path returns before Observe, so no prefetch goroutines spawn.
	s.fileClient.EXPECT().Read(mock.Anything, mock.MatchedBy(func(req *proto.ReadRequest) bool {
		return req.Offset == 2*chunkBytes && req.Size == chunkBytes
	}), mock.Anything).RunAndReturn(
		func(_ context.Context, _ *proto.ReadRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			return newBackendReadStreamStub(s.T(),
				&proto.ReadFrame{Status: proto.FsError_FS_EIO},
			), nil
		}).Once()

	dest := make([]byte, 2*chunkBytes)
	n, st := s.backend.Read(context.Background(), h, chunkBytes, dest)
	s.Require().Equal(proto.FsError_FS_EIO, st)
	s.Assert().Zero(n, "a failed tail fetch must not surface a short read")
}

// TestRead_PartialPrefixShortTailAtEOF: a tail stream that ends cleanly
// before filling the request signals EOF; the combined prefix+tail count is
// returned with OK — short is legitimate at EOF.
func (s *BackendClientTestSuite) TestRead_PartialPrefixShortTailAtEOF() {
	const chunkBytes = 1024
	const tailBytes = 500 // EOF lands mid-tail
	cfg := grpcclient.PerFileConfig{
		ReadaheadChunkBytes: chunkBytes,
		ReadaheadThreshold:  1,
		ReadaheadWindow:     2,
	}
	h := s.newHandle(cfg)

	prefix := make([]byte, chunkBytes)
	tail := make([]byte, tailBytes)
	for i := range prefix {
		prefix[i] = byte(i % 251)
	}
	for i := range tail {
		tail[i] = byte((i + 13) % 251)
	}
	h.leaf.readahead.Observe(0, chunkBytes)
	h.leaf.readahead.Store(chunkBytes, prefix)

	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *proto.ReadRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			if req.Offset == 2*chunkBytes {
				return newBackendReadStreamStubOptional(s.T(),
					&proto.ReadFrame{Data: tail, Status: proto.FsError_FS_OK},
					&proto.ReadFrame{Status: proto.FsError_FS_OK},
				), nil
			}
			// Window-refill prefetch: empty stream, never stored.
			return newBackendReadStreamStubOptional(s.T()), nil
		}).Maybe()

	dest := make([]byte, 2*chunkBytes)
	n, st := s.backend.Read(context.Background(), h, chunkBytes, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Equal(chunkBytes+tailBytes, n, "combined cache prefix + short live tail at EOF")
	s.Assert().Equal(prefix, dest[:chunkBytes])
	s.Assert().Equal(tail, dest[chunkBytes:n])
}

// TestSetAttr_BothTimes: when both timestamps are requested, both FileTime
// messages carry their Sec/Nsec (no nil/UTIME_OMIT slot). Migrated from the
// removed per-field Utimens wire test.
func (s *BackendClientTestSuite) TestSetAttr_BothTimes() {
	atime := time.Unix(100, 1)
	mtime := time.Unix(200, 2)
	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.MatchedBy(func(req *proto.SetAttrRequest) bool {
		return req.Valid == fuse.FATTR_ATIME|fuse.FATTR_MTIME &&
			req.Atime != nil && req.Atime.Sec == 100 && req.Atime.Nsec == 1 &&
			req.Mtime != nil && req.Mtime.Sec == 200 && req.Mtime.Nsec == 2 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.SetAttrReply{Status: proto.FsError_FS_OK}, nil)

	_, st := s.backend.SetAttr(context.Background(), "/test", backend.SetAttrIn{
		Valid: fuse.FATTR_ATIME | fuse.FATTR_MTIME,
		Atime: &atime,
		Mtime: &mtime,
	})
	s.Require().Equal(proto.FsError_FS_OK, st)
}

// TestSetAttr_RPCErrorMapsToErrno: a transport-level error (no reply) maps
// through statusFromRPCError to an errno, not a panic on the nil reply.
func (s *BackendClientTestSuite) TestSetAttr_RPCErrorMapsToErrno() {
	s.fsClient.EXPECT().SetAttr(mock.Anything, mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)
	attr, st := s.backend.SetAttr(context.Background(), "/test", backend.SetAttrIn{Valid: fuse.FATTR_MODE, Mode: 0o600})
	s.Assert().Nil(attr)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
}

// Protective property: a cancelled FUSE request ctx must NOT abort the in-flight
// metadata RPC. Assert the RPC receives a non-cancelled ctx rather
// than a specific error value (the real cancellation error from retry-go does
// not match a clean codes.Canceled, so an error-shape assertion would pass in a
// mock while the real path still failed).
func (s *BackendClientTestSuite) TestSetAttr_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	s.fsClient.EXPECT().SetAttr(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
		mock.Anything,
	).Return(&proto.SetAttrReply{Status: proto.FsError_FS_OK}, nil)

	_, st := s.backend.SetAttr(parent, "/test", backend.SetAttrIn{Valid: fuse.FATTR_MODE, Mode: 0o600})
	s.Require().Equal(proto.FsError_FS_OK, st)
}

// newHandleAt is like newHandle but lets the caller choose path and fd so
// CopyFileRange tests can construct distinct source and destination handles.
// When cfg.WriteCoalesceBytes > 0 the leaf is wrapped in a *walHandle.
func (s *BackendClientTestSuite) newHandleAt(path string, fd uint64, cfg grpcclient.PerFileConfig) *walHandle {
	leaf := newGrpcFileHandle(s.client, "testVolume", path, fd, 0, nil, 30*time.Second, "test-session", "epoch-1", cfg)
	return newWalHandle(s.backend, leaf, cfg.WriteCoalesceBytes, nil)
}

// --- CopyFileRange ---

// TestCopyFileRange_HappyPath verifies all wire fields in the request and
// the correct (BytesCopied, proto.FsError_FS_OK) return value. RequestId must be non-empty
// so the server's idempotency cache can key on it.
func (s *BackendClientTestSuite) TestCopyFileRange_HappyPath() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{})

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.MatchedBy(func(req *proto.CopyFileRangeRequest) bool {
		return req.Volume == "testVolume" &&
			req.FdIn == 1 && req.PathIn == "/src/file" && req.OffIn == 100 &&
			req.FdOut == 2 && req.PathOut == "/dst/file" && req.OffOut == 200 &&
			req.Length == 4096 && req.Flags == 0 &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.CopyFileRangeReply{BytesCopied: 4096, Status: 0}, nil).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 100, dst, 200, 4096, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(4096), n)
}

// TestCopyFileRange_TransportError verifies that a non-retryable transport
// error (Internal is permanent, not Unavailable/DeadlineExceeded) returns EIO
// immediately with exactly one attempt.
func (s *BackendClientTestSuite) TestCopyFileRange_TransportError() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{})

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Internal, "fatal")).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 512, 0)
	s.Assert().Equal(proto.FsError_FS_EIO, st)
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
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
	s.Assert().Equal(uint64(0), n)
	// badHandle as dst — src resolves fine, dst does not.
	good2 := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	n, st = s.backend.CopyFileRange(context.Background(), good2, 0, badHandle{}, 0, 512, 0)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
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
	s.Require().Equal(proto.FsError_FS_OK, wst)

	// Drain RPC: the coalescer's pending bytes will flow through streamingWrite.
	writeStub := newBackendWriteStreamStub(s.T(), &proto.WriteReply{Written: 7, Status: proto.FsError_FS_OK}, nil)
	s.fileClient.EXPECT().Write(mock.Anything, mock.Anything).Return(writeStub, nil).Once()

	// Copy RPC: inside RunAndReturn assert the write already fired.
	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *proto.CopyFileRangeRequest, _ ...grpc.CallOption) (*proto.CopyFileRangeReply, error) {
			s.Assert().NotEmpty(writeStub.frames, "drain Write must land before CopyFileRange is called")
			return &proto.CopyFileRangeReply{BytesCopied: 1024, Status: 0}, nil
		}).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 1024, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(1024), n)
}

// TestCopyFileRange_RetryReusesRequestID is the load-bearing Phase 1d assertion
// for CopyFileRange: the request_id must be generated once outside the retry
// closure so the server's idempotency cache can short-circuit the replay without
// re-executing the copy.
//
// Mutation-verify: moving uuid.NewString() inside the closure makes ids[0] !=
// ids[1] and the assertion fails.
func (s *BackendClientTestSuite) TestCopyFileRange_RetryReusesRequestID() {
	src := s.newHandleAt("/src/file", 1, grpcclient.PerFileConfig{})
	dst := s.newHandleAt("/dst/file", 2, grpcclient.PerFileConfig{})

	var ids []string
	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.CopyFileRangeRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(nil, status.Error(codes.Unavailable, "transient")).Once()

	s.fileClient.EXPECT().CopyFileRange(mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, req *proto.CopyFileRangeRequest, _ ...grpc.CallOption) {
			ids = append(ids, req.RequestId)
		}).Return(&proto.CopyFileRangeReply{BytesCopied: 512, Status: 0}, nil).Once()

	n, st := s.backend.CopyFileRange(context.Background(), src, 0, dst, 0, 512, 0)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(512), n)
	s.Require().Len(ids, 2, "expected exactly two CopyFileRange attempts")
	s.Assert().NotEmpty(ids[0], "request_id must be non-empty")
	s.Assert().Equal(ids[0], ids[1], "retry must reuse the same request_id for server-side dedup")
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
	s.Require().Equal(proto.FsError_FS_OK, st)
	// dst is still clean: Flush must be a no-op (strict mock — no WriteAndFlush call).
	s.Require().Equal(proto.FsError_FS_OK, s.backend.Flush(context.Background(), dst))
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
	s.Require().Equal(proto.FsError_FS_OK, st)

	// dst is now dirty: Flush must issue WriteAndFlush.
	s.fileClient.EXPECT().WriteAndFlush(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK, Written: 0}, nil).Once()
	s.Require().Equal(proto.FsError_FS_OK, s.backend.Flush(context.Background(), dst))
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
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(8192), off)
}

// TestLseek_RetryReusesResult mirrors TestFsync_RetriesOnUnavailable:
// first call returns Unavailable, second succeeds. The retried result must
// come through cleanly — pinning the idempotent-retry contract for Lseek.
func (s *BackendClientTestSuite) TestLseek_RetryReusesResult() {
	s.fileClient.EXPECT().Lseek(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().Lseek(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.LseekReply{Offset: 1024, Status: 0}, nil).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	off, st := s.backend.Lseek(context.Background(), h, 0, 4)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(uint64(1024), off)
	s.fileClient.AssertNumberOfCalls(s.T(), "Lseek", 2)
}

// TestLseek_BadHandleEBADF verifies the EBADF fast-path for a foreign handle.
func (s *BackendClientTestSuite) TestLseek_BadHandleEBADF() {
	off, st := s.backend.Lseek(context.Background(), badHandle{}, 0, 4)
	s.Assert().Equal(proto.FsError_FS_EBADF, st)
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
	}), mock.Anything).Return(&proto.SetXAttrReply{Status: proto.FsError_FS_OK}, nil)

	st := s.backend.SetXAttr(context.Background(), "/test", "user.foo", data, 0)
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestRemoveXAttr_HappyPath verifies RemoveXAttr stamps RequestId and SessionId.
func (s *BackendClientTestSuite) TestRemoveXAttr_HappyPath() {
	s.fsClient.EXPECT().RemoveXAttr(mock.Anything, mock.MatchedBy(func(req *proto.RemoveXAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.Attribute == "user.foo" &&
			req.SessionId == "test-session" && req.RequestId != ""
	}), mock.Anything).Return(&proto.RemoveXAttrReply{Status: proto.FsError_FS_OK}, nil)

	st := s.backend.RemoveXAttr(context.Background(), "/test", "user.foo")
	s.Assert().Equal(proto.FsError_FS_OK, st)
}

// TestListXAttr_HappyPath verifies ListXAttr returns the attribute name list.
func (s *BackendClientTestSuite) TestListXAttr_HappyPath() {
	s.fsClient.EXPECT().ListXAttr(mock.Anything, mock.MatchedBy(func(req *proto.ListXAttrRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test"
	}), mock.Anything).Return(&proto.ListXAttrReply{
		Attributes: []string{"user.foo", "user.bar"},
		Status:     proto.FsError_FS_OK,
	}, nil)

	names, st := s.backend.ListXAttr(context.Background(), "/test")
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Require().Len(names, 2)
	s.Assert().Equal("user.foo", names[0])
	s.Assert().Equal("user.bar", names[1])
}

// TestRead_ReadaheadConcurrentWithRelease drives sequential Reads (readahead
// armed, so prefetch goroutines spawn) concurrently with Release on the same
// handle. Run under -race in the test-race job: the assertions are (1) no
// data race between the prefetch goroutines, the readahead ring and Release's
// teardown, and (2) Release returns only after in-flight prefetches finished
// (the WaitGroup discipline) without deadlocking against a racing Read.
func (s *BackendClientTestSuite) TestRead_ReadaheadConcurrentWithRelease() {
	const chunkBytes = 4 << 10
	h := s.newHandle(grpcclient.PerFileConfig{
		ReadaheadChunkBytes: chunkBytes,
		ReadaheadThreshold:  1,
		ReadaheadWindow:     4,
	})

	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *proto.ReadRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			data := make([]byte, chunkBytes)
			return newBackendReadStreamStubOptional(s.T(),
				&proto.ReadFrame{Data: data, Status: proto.FsError_FS_OK},
				&proto.ReadFrame{Status: proto.FsError_FS_OK},
			), nil
		}).Maybe()
	s.fileClient.EXPECT().Release(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.ReleaseReply{}, nil).Once()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { // reader: sequential pattern keeps prefetches firing
		defer wg.Done()
		<-start
		dest := make([]byte, chunkBytes)
		for i := 0; i < 64; i++ {
			if _, st := s.backend.Read(context.Background(), h, int64(i)*chunkBytes, dest); st != proto.FsError_FS_OK {
				return // an error after Release cancelled lifeCtx is fine; we chase races
			}
		}
	}()
	go func() { // releaser
		defer wg.Done()
		<-start
		time.Sleep(2 * time.Millisecond) // let some prefetches get in flight
		st := s.backend.Release(context.Background(), h)
		s.True(st == proto.FsError_FS_OK || st == proto.FsError_FS_EIO, "Release must terminate cleanly, got %v", st)
	}()
	close(start)
	wg.Wait()
}

// --- transport-error errno fidelity ---

// TestStatusFromRPCError_Mapping pins the transport-error → errno table:
// NotFound→ENOENT, PermissionDenied/Unauthenticated→EACCES, everything
// else→EIO. The server emits these codes for reaped sessions / dead fds /
// denied identities; blanket EIO would mask them as generic I/O errors.
func (s *BackendClientTestSuite) TestStatusFromRPCError_Mapping() {
	tests := []struct {
		name string
		err  error
		want proto.FsError
	}{
		{"NotFound", status.Error(codes.NotFound, "fd not found"), proto.FsError_FS_ENOENT},
		{"PermissionDenied", status.Error(codes.PermissionDenied, "denied"), proto.FsError_FS_EACCES},
		{"Unauthenticated", status.Error(codes.Unauthenticated, "revoked"), proto.FsError_FS_EACCES},
		{"Unavailable", status.Error(codes.Unavailable, "down"), proto.FsError_FS_EIO},
		{"Internal", status.Error(codes.Internal, "boom"), proto.FsError_FS_EIO},
		{"non-grpc error", context.DeadlineExceeded, proto.FsError_FS_EIO},
		{"reclaim failure unwraps to its status", errFromStatus(proto.FsError_FS_ENOENT), proto.FsError_FS_ENOENT},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.Equal(tt.want, statusFromRPCError(tt.err))
		})
	}
}

// TestStat_TransportPermissionDeniedMapsToEACCES verifies the helper is wired
// into a path-level op's error arm: a server-side PermissionDenied (revoked
// session, denied identity) surfaces as EACCES, not EIO.
func (s *BackendClientTestSuite) TestStat_TransportPermissionDeniedMapsToEACCES() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.PermissionDenied, "identity denied")).Once()

	attr, st := s.backend.Stat(context.Background(), "/secret")
	s.Assert().Equal(proto.FsError_FS_EACCES, st)
	s.Assert().Nil(attr)
}

// TestRead_DeadFdNotFoundMapsToESTALE verifies the fd-op error arm: the
// server's NotFound for a dead/reaped fd surfaces as ESTALE ("stale file
// handle"), NOT ENOENT. An fd-op addresses an open server fd, never a path, so
// a NotFound means the fd/session is gone — the file still exists; reporting
// "No such file or directory" would be misleading and unactionable (issue #117).
func (s *BackendClientTestSuite) TestRead_DeadFdNotFoundMapsToESTALE() {
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.NotFound, "fd 1 not found in session")).Once()

	h := s.newHandle(grpcclient.PerFileConfig{})
	n, st := s.backend.Read(context.Background(), h, 0, make([]byte, 8))
	s.Assert().Equal(proto.FsError_FS_ESTALE, st)
	s.Assert().Equal(0, n)
}

// TestRead_ReadaheadArmsAtDefaultThreshold pins the readahead arming
// behaviour at ReadaheadThreshold=3 (the production default). Prefetch
// RPCs must be observed only AFTER the third strictly-sequential Read —
// not on the first or second. Equally important: each Read must call
// Observe exactly once. A double-Observe on the miss path would count
// two sequential hits per Read, halving the effective threshold to 2 and
// silently disarming readahead at the production default threshold
// (every existing integration test uses threshold 1 and cannot catch this).
//
// Mutation-verify: adding a second Observe call in the miss branch causes
// the first Read to count as 2 hits and the second to arm prefetches,
// making readCount reach ≥ 2 before the third Read — the Eventually below
// fires and the assertion s.Zero(readCount.Load(), …) fails.
func (s *BackendClientTestSuite) TestRead_ReadaheadArmsAtDefaultThreshold() {
	const chunkBytes = 1 << 20 // matches config.DefaultReadaheadChunkBytes
	cfg := grpcclient.PerFileConfig{
		ReadaheadChunkBytes: chunkBytes,
		ReadaheadThreshold:  3, // production default
		ReadaheadWindow:     4,
	}
	h := s.newHandle(cfg)

	var readCount atomic.Int64
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *proto.ReadRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[proto.ReadFrame], error) {
			readCount.Add(1)
			data := make([]byte, chunkBytes)
			return newBackendReadStreamStubOptional(s.T(),
				&proto.ReadFrame{Data: data, Status: proto.FsError_FS_OK},
				&proto.ReadFrame{Status: proto.FsError_FS_OK},
			), nil
		}).Maybe()

	dest := make([]byte, chunkBytes)

	// Read 1: seqHits = 1 — no prefetch yet; exactly one RPC (the synchronous live fetch).
	_, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(int64(1), readCount.Load(), "Read 1: exactly one live RPC, no prefetch")

	// Read 2: seqHits = 2 — still below threshold; still no prefetch.
	_, st = s.backend.Read(context.Background(), h, int64(chunkBytes), dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(int64(2), readCount.Load(), "Read 2: still no prefetch RPC")

	// Read 3: seqHits reaches threshold=3; prefetches must fire now.
	_, st = s.backend.Read(context.Background(), h, int64(2*chunkBytes), dest)
	s.Require().Equal(proto.FsError_FS_OK, st)

	// 3 synchronous RPCs + ≥1 prefetch goroutine spawned on the third Read.
	s.Eventually(func() bool {
		return readCount.Load() >= 4
	}, time.Second, 10*time.Millisecond,
		"prefetch RPCs must start after the third sequential Read (threshold=3)")
}

// badHandle is a FileHandle implementation that is not a *grpcFileHandle,
// used to exercise the type-assertion guard on fd-level ops. Unwrap
// returns the receiver (leaf marker) so resolveHandle's walk terminates
// and yields nil — modelling a foreign leaf handle the backend can't
// reach into.
type badHandle struct{}

func (b badHandle) Path() string               { return "/bad" }
func (b badHandle) Unwrap() backend.FileHandle { return b }

func TestBackendClientTestSuite(t *testing.T) {
	suite.Run(t, new(BackendClientTestSuite))
}

// FdOpReclaimSuite proves the two properties that Task 4b's wiring must hold:
//
//  1. A stale handle (snapshotted sessionID != live session) causes reclaimIfStale
//     to call Open exactly once, and the subsequent RPC uses the newly-issued fd.
//
//  2. When reclaimIfStale fails (Open returns a non-OK status), the fd-op surfaces
//     that status and never issues the underlying RPC — no phantom calls.
type FdOpReclaimSuite struct {
	suite.Suite
	client     *grpcmocks.MockClient
	fileClient *mockProto.MockRpcFileClient
	backend    *BackendClient
}

func (s *FdOpReclaimSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.client.EXPECT().File().Return(s.fileClient).Maybe()
	s.client.EXPECT().DataFileClient().Return(s.fileClient, func() {}).Maybe()
	s.client.EXPECT().Fs().Return(mockProto.NewMockRpcFsClient(s.T())).Maybe()
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	// Live session is "live-session"; stale handles snapshot "stale-session".
	s.client.EXPECT().SessionID().Return("live-session").Maybe()
	// Epoch changed (epoch-1 → epoch-2): stale handles represent the RESTART case
	// where the server process died and came back. reclaimIfStale must reopen.
	s.client.EXPECT().BootEpoch().Return("epoch-2").Maybe()
	s.client.EXPECT().RetryWindow().Return(0 * time.Second).Maybe() // fail-fast: no retries
	s.client.EXPECT().Lifetime().Return(context.Background()).Maybe()
	// retryOp emits the retry counter through the client's Recorder on a retry;
	// nil is fine (the call site nil-checks) and these tests don't assert metrics.
	s.client.EXPECT().Metrics().Return(nil).Maybe()
	s.client.EXPECT().PerFileConfig().Return(grpcclient.PerFileConfig{}).Maybe()
	s.backend = NewBackendClient(s.client, "testVolume")
}

// newStaleHandle builds a walHandle wrapping a *grpcFileHandle whose snapshotted
// sessionID ("stale-session") differs from the live client session ("live-session").
// fd=7. The walHandle has no coalescer (threshold=0) — these tests exercise
// reclaim/session mechanics, not coalescing.
func (s *FdOpReclaimSuite) newStaleHandle() *walHandle {
	leaf := newGrpcFileHandle(
		s.client, "testVolume", "/test/file", 7,
		0,   /*flags — O_RDONLY*/
		nil, /*caller — not relevant for FdOpReclaimSuite tests*/
		30*time.Second,
		"stale-session",
		"epoch-1", // stored epoch; live BootEpoch() returns "epoch-2" → restart case
		grpcclient.PerFileConfig{},
	)
	leaf.fileClient = s.fileClient
	return newWalHandle(s.backend, leaf, 0, nil)
}

// expectOpenReturns sets up a single Open expectation for the stale handle's
// reclaim; it returns a reply with the given fd and status.
func (s *FdOpReclaimSuite) expectOpenReturns(newFd uint64, st proto.FsError) {
	s.fileClient.EXPECT().Open(
		mock.Anything,
		mock.MatchedBy(func(req *proto.OpenRequest) bool {
			return req.Volume == "testVolume" &&
				req.Path == "/test/file" &&
				req.SessionId == "live-session"
		}),
		mock.Anything,
	).Return(&proto.OpenReply{Fd: newFd, Status: st}, nil).Once()
}

// TestReadUsesReclaimedFd: Read on a stale handle must trigger reclaim, then
// issue the Read RPC with the new fd from the Open reply.
func (s *FdOpReclaimSuite) TestReadUsesReclaimedFd() {
	const freshFd = uint64(99)
	s.expectOpenReturns(freshFd, proto.FsError_FS_OK)

	readStream := newBackendReadStreamStub(s.T(),
		&proto.ReadFrame{Status: proto.FsError_FS_OK, Data: []byte("hello")},
	)
	s.fileClient.EXPECT().Read(
		mock.Anything,
		mock.MatchedBy(func(req *proto.ReadRequest) bool {
			return req.Fd == freshFd && req.SessionId == "live-session"
		}),
		mock.Anything,
	).Return(readStream, nil).Once()

	h := s.newStaleHandle()
	dest := make([]byte, 64)
	n, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.Assert().Equal(5, n)
	s.fileClient.AssertNumberOfCalls(s.T(), "Open", 1)
}

// TestReadReclaimFailureSurfacesStatus: when reclaim fails (Open returns ENOENT),
// the Read must return ENOENT and must NOT issue the underlying Read RPC.
func (s *FdOpReclaimSuite) TestReadReclaimFailureSurfacesStatus() {
	s.expectOpenReturns(0, proto.FsError_FS_ENOENT)
	// No Read expectation — strict mock proves the RPC is never issued.

	h := s.newStaleHandle()
	dest := make([]byte, 64)
	_, st := s.backend.Read(context.Background(), h, 0, dest)
	s.Assert().Equal(proto.FsError_FS_ENOENT, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "Read", 0)
}

// TestFlushUsesReclaimedFd: Flush on a stale dirty handle reclaims then
// issues WriteAndFlush with the new fd.
func (s *FdOpReclaimSuite) TestFlushUsesReclaimedFd() {
	const freshFd = uint64(42)
	s.expectOpenReturns(freshFd, proto.FsError_FS_OK)

	s.fileClient.EXPECT().WriteAndFlush(
		mock.Anything,
		mock.MatchedBy(func(req *proto.WriteAndFlushRequest) bool {
			return req.Fd == freshFd && req.SessionId == "live-session"
		}),
		mock.Anything,
	).Return(&proto.WriteAndFlushReply{Status: proto.FsError_FS_OK}, nil).Once()

	h := s.newStaleHandle()
	h.dirty.Store(true)
	st := s.backend.Flush(context.Background(), h)
	s.Require().Equal(proto.FsError_FS_OK, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "Open", 1)
}

// TestFlushReclaimFailureSurfacesStatus: when reclaim fails, Flush returns that
// status without calling WriteAndFlush.
func (s *FdOpReclaimSuite) TestFlushReclaimFailureSurfacesStatus() {
	s.expectOpenReturns(0, proto.FsError_FS_EACCES)

	h := s.newStaleHandle()
	h.dirty.Store(true)
	st := s.backend.Flush(context.Background(), h)
	s.Assert().Equal(proto.FsError_FS_EACCES, st)
	s.fileClient.AssertNumberOfCalls(s.T(), "WriteAndFlush", 0)
}

func TestFdOpReclaimSuite(t *testing.T) {
	suite.Run(t, new(FdOpReclaimSuite))
}
