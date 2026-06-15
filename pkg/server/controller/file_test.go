package controller

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/metrics"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"go.gmountie.dev/gmountie/pkg/proto"

	nodefs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/nodefs"
	pathfs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcFileServerTestSuite struct {
	suite.Suite
	server     *RpcFileServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
	bus        serverio.EventBus
}

func (s *RpcFileServerTestSuite) SetupTest() {
	s.fsService = new(mockservice.MockVolumeService)
	// Default permissive stub for ResolveIdentity — Create now consults it to
	// fill Owner.user_name/group_name on the post-create stat. Tests that care
	// about the names can override via .On("ResolveIdentity", ...).
	s.fsService.On("ResolveIdentity", mock.Anything, mock.Anything, mock.Anything).
		Return(service.Identity{}, nil).Maybe()
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create("test-user", "")
	s.Require().NoError(err)
	s.sessionID = sid
	s.bus = serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.server = NewRpcFileServer(s.fsService, s.sessionMgr, metrics.NewMetrics(), 1<<20, s.bus)
}

func (s *RpcFileServerTestSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
	s.bus.Close()
}

func (s *RpcFileServerTestSuite) TestOpen() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
	mockFs.EXPECT().Open("/test/path", uint32(0), mock.Anything).Return(nodefs.NewDefaultFile(), fuse.OK)

	// Test.
	request := &proto.OpenRequest{Volume: "testVolume", Path: "/test/path", Flags: 0, Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID, RequestId: "test-req-open"}
	reply, err := s.server.Open(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestCreate() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
	mockFs.EXPECT().Create("/test/path", uint32(0), uint32(0), mock.Anything).Return(nodefs.NewDefaultFile(), fuse.OK)
	// GetAttr is called unconditionally on successful Create to populate reply.Attributes.
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{Ino: 42, Mode: 0o100644}, fuse.OK)

	// Test.
	request := &proto.CreateRequest{Volume: "testVolume", Path: "/test/path", Flags: 0, Mode: 0, Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID, RequestId: "test-req-create"}
	reply, err := s.server.Create(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
	// The handler must populate reply.Attributes from the post-create GetAttr.
	s.Require().NotNil(reply.Attributes, "Create reply must carry Attributes")
	s.Assert().Equal(uint64(42), reply.Attributes.Ino)
	s.Assert().Equal(uint32(0o100644), reply.Attributes.Mode)
}

func (s *RpcFileServerTestSuite) TestRead() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/test/path", mockFile)
	mockFile.EXPECT().Read(mock.Anything, int64(0)).Return(fuse.ReadResultData([]byte("test data")), fuse.OK).Once()
	// Second call hits the EOF branch (short read returned len("test data")<1024).
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.ReadRequest{Volume: "testVolume", Fd: fd, Size: 1024, Offset: 0, SessionId: s.sessionID}
	stream := newFakeReadStream(testAuthedCtx("test-user"))
	err := s.server.Read(request, stream)

	// Verify.
	s.Require().NoError(err)
	s.Require().NotEmpty(stream.frames)
	// First frame should carry the data, last frame is terminal OK.
	s.Assert().Equal([]byte("test data"), stream.frames[0].Data)
	s.Assert().Equal(int32(fuse.OK), stream.frames[len(stream.frames)-1].Status)
}

func (s *RpcFileServerTestSuite) TestFsync() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/test/path", mockFile)
	ctx := testAuthedCtx("test-user")
	mockFile.EXPECT().Fsync(int(0)).Return(fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.FsyncRequest{Volume: "testVolume", Fd: fd, Flags: 0, SessionId: s.sessionID}
	reply, err := s.server.Fsync(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestRelease() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/test/path", mockFile)
	ctx := testAuthedCtx("test-user")
	mockFile.EXPECT().Release().Return()

	// Test.
	request := &proto.ReleaseRequest{Volume: "testVolume", Fd: fd, SessionId: s.sessionID}
	reply, err := s.server.Release(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
}

func (s *RpcFileServerTestSuite) TestFlush() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/test/path", mockFile)
	ctx := testAuthedCtx("test-user")
	mockFile.EXPECT().Flush().Return(fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.FlushRequest{Volume: "testVolume", Fd: fd, SessionId: s.sessionID}
	reply, err := s.server.Flush(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestOpenNonOkDoesNotRegisterFd() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	// Open returns a non-OK status.
	mockFs.EXPECT().Open("/test/path", uint32(0), mock.Anything).
		Return(nil, fuse.ENOENT)

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/test/path", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "test-req-open-non-ok",
	}
	reply, err := s.server.Open(testAuthedCtx("test-user"), request)
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.ENOENT), reply.Status)

	// The fd in the reply must NOT be registered on the session.
	sess, err := s.sessionMgr.Get(s.sessionID)
	s.Require().NoError(err)
	_, ok := sess.GetFile(reply.Fd)
	s.Assert().False(ok, "non-OK Open should not have registered an fd")
}

func (s *RpcFileServerTestSuite) TestCreateNonOkDoesNotRegisterFd() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Create("/p", uint32(0), uint32(0), mock.Anything).
		Return(nil, fuse.EACCES)

	request := &proto.CreateRequest{
		Volume: "testVolume", Path: "/p",
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "test-req-create-non-ok",
	}
	reply, err := s.server.Create(testAuthedCtx("test-user"), request)
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.EACCES), reply.Status)

	sess, _ := s.sessionMgr.Get(s.sessionID)
	_, ok := sess.GetFile(reply.Fd)
	s.Assert().False(ok)
}

func (s *RpcFileServerTestSuite) TestUnknownSessionReturnsError() {
	request := &proto.ReadRequest{
		Volume: "testVolume", Fd: 1, Size: 1, Offset: 0,
		SessionId: "no-such-session",
	}
	stream := newFakeReadStream(context.Background())
	err := s.server.Read(request, stream)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.NotFound, st.Code())
}

func (s *RpcFileServerTestSuite) TestOpenEmptyRequestIDFails() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/p", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "",
	}
	_, err := s.server.Open(testAuthedCtx("test-user"), request)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *RpcFileServerTestSuite) TestOpenDuplicateRequestIDReturnsCachedReply() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Open("/p", uint32(0), mock.Anything).
		Return(nodefs.NewDefaultFile(), fuse.OK).Once()

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/p", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "dup-req-1",
	}
	r1, err := s.server.Open(testAuthedCtx("test-user"), request)
	s.Require().NoError(err)

	r2, err := s.server.Open(testAuthedCtx("test-user"), request)
	s.Require().NoError(err)
	s.Assert().Equal(r1.Fd, r2.Fd, "duplicate request_id must return the same fd from the cache")
	s.Assert().Equal(r1.Status, r2.Status)
}

func (s *RpcFileServerTestSuite) TestWriteAndFlushWritesThenFlushesAndReturnsAttr() {
	// Setup: register a writable file and a filesystem that can stat it.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/waf.txt", mockFile)
	ctx := testAuthedCtx("test-user")

	mockFile.EXPECT().Write([]byte("hello"), int64(0)).Return(uint32(5), fuse.OK)
	mockFile.EXPECT().Flush().Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/waf.txt", mock.Anything).Return(&fuse.Attr{Size: 5}, fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	reply, err := s.server.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Data: []byte("hello"), SessionId: s.sessionID,
	})

	// Verify.
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
	s.Assert().Equal(uint32(5), reply.Written)
	s.Require().NotNil(reply.FinalAttr)
	s.Assert().Equal(uint64(5), reply.FinalAttr.Size)
}

func (s *RpcFileServerTestSuite) TestWriteAndFlushEmptyDataIsPureFlush() {
	// Setup: register a file; no write call expected, only flush + stat.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/waf2.txt", mockFile)
	ctx := testAuthedCtx("test-user")

	mockFile.EXPECT().Flush().Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/waf2.txt", mock.Anything).Return(&fuse.Attr{Size: 0}, fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	reply, err := s.server.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, SessionId: s.sessionID, // no Data
	})

	// Verify.
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
	s.Assert().Equal(uint32(0), reply.Written)
	s.Require().NotNil(reply.FinalAttr)
}

func (s *RpcFileServerTestSuite) TestWriteAndFlushWriteErrorSkipsFlush() {
	// Setup: register a file whose Write returns EIO.
	// Flush and GetAttr must NOT be called — unexpected mock calls would fail the test.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/err.txt", mockFile)
	ctx := testAuthedCtx("test-user")

	mockFile.EXPECT().Write([]byte("data"), int64(0)).Return(uint32(0), fuse.EIO)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	reply, err := s.server.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Data: []byte("data"), SessionId: s.sessionID,
	})

	// Verify: write error is surfaced; flush was NOT called.
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.EIO), reply.Status)
	s.Assert().Equal(uint32(0), reply.Written)
	mockFile.AssertNotCalled(s.T(), "Flush")
}

func (s *RpcFileServerTestSuite) TestWriteAndFlushEmitsMutationEventOnSuccess() {
	// Subscribe to the bus BEFORE issuing the RPC so we don't miss the event.
	events, _, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	// Setup: register a writable file.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/emit.txt", mockFile)
	ctx := testAuthedCtx("test-user")

	mockFile.EXPECT().Write([]byte("hi"), int64(0)).Return(uint32(2), fuse.OK)
	mockFile.EXPECT().Flush().Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/emit.txt", mock.Anything).Return(&fuse.Attr{Size: 2}, fuse.OK).Once()
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	reply, err := s.server.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Data: []byte("hi"), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.OK), reply.Status)

	// A KindMutated event for /emit.txt must arrive on the bus.
	select {
	case ev := <-events:
		s.Assert().Equal("/emit.txt", ev.Path)
		s.Assert().Equal(serverio.KindMutated, ev.Kind)
	case <-time.After(time.Second):
		s.FailNow("timed out waiting for mutation event from WriteAndFlush")
	}
}

// TestWriteAndFlush_ReplaySameRequestIDExecutesOnce: a retried RPC with the
// same request_id must replay the cached reply, not re-apply the write. The
// .Once() pins on Write/Flush/GetAttr are the executed-exactly-once proof.
func (s *RpcFileServerTestSuite) TestWriteAndFlush_ReplaySameRequestIDExecutesOnce() {
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/waf-replay.txt", mockFile)

	mockFile.EXPECT().Write([]byte("hello"), int64(0)).Return(uint32(5), fuse.OK).Once()
	mockFile.EXPECT().Flush().Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/waf-replay.txt", mock.Anything).Return(&fuse.Attr{Size: 5, Mtime: 1700000000}, fuse.OK).Once()
	mockFile.EXPECT().Release().Return().Maybe()

	req := &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Data: []byte("hello"),
		SessionId: s.sessionID, RequestId: "req-waf-replay",
	}
	r1, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	r2, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)

	s.Equal(r1.Status, r2.Status)
	s.Equal(r1.Written, r2.Written)
	s.Require().NotNil(r2.FinalAttr)
	s.Equal(r1.FinalAttr.Version, r2.FinalAttr.Version, "replay must return the cached reply")
	mockFile.AssertExpectations(s.T()) // .Once() on Write+Flush proves the replay was deduped
	mockFs.AssertExpectations(s.T())
}

// TestWriteAndFlush_EmptyRequestIDExecutesEachCall pins the bypass guard:
// pure-flush WriteAndFlush calls carry NO request_id, and two such calls must
// BOTH execute. Without the empty-id bypass, "" would be one shared cache key
// and the second flush would be silently swallowed by the first one's reply.
func (s *RpcFileServerTestSuite) TestWriteAndFlush_EmptyRequestIDExecutesEachCall() {
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/waf-noid.txt", mockFile)

	mockFile.EXPECT().Flush().Return(fuse.OK).Times(2)
	mockFs.EXPECT().GetAttr("/waf-noid.txt", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Times(2)
	mockFile.EXPECT().Release().Return().Maybe()

	req := &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, SessionId: s.sessionID, // no Data, no RequestId
	}
	r1, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), r1.Status)
	r2, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), r2.Status)
	mockFile.AssertExpectations(s.T()) // Flush .Times(2): both calls really executed
	mockFs.AssertExpectations(s.T())
}

// TestWriteAndFlush_ReplayAfterFdReleaseReturnsCachedReply documents the
// retry-after-reconnect scenario: the first attempt's reply lands in the
// cache, the fd is released (close raced the lost reply), and the retry must
// still get the cached reply — NOT EBADF — because a cache hit never touches
// the fd table.
func (s *RpcFileServerTestSuite) TestWriteAndFlush_ReplayAfterFdReleaseReturnsCachedReply() {
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/waf-released.txt", mockFile)

	mockFile.EXPECT().Write([]byte("hi"), int64(0)).Return(uint32(2), fuse.OK).Once()
	mockFile.EXPECT().Flush().Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/waf-released.txt", mock.Anything).Return(&fuse.Attr{Size: 2}, fuse.OK).Once()
	mockFile.EXPECT().Release().Return().Once() // the ReleaseFile below closes it

	req := &proto.WriteAndFlushRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Data: []byte("hi"),
		SessionId: s.sessionID, RequestId: "req-waf-released",
	}
	r1, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), r1.Status)

	// The fd is gone before the retry lands.
	sess.ReleaseFile(fd)

	r2, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), r2.Status, "cache hit must not consult the (now empty) fd table")
	s.Equal(r1.Written, r2.Written)
	mockFile.AssertExpectations(s.T())
	mockFs.AssertExpectations(s.T())
}

// TestCopyFileRange_ReplaySameRequestIDExecutesOnce: same dedup contract for
// the copy path, via the generic (interface-based) copy loop on mock files so
// the strict .Once() pins on Read/Write prove single execution.
func (s *RpcFileServerTestSuite) TestCopyFileRange_ReplaySameRequestIDExecutesOnce() {
	// versionAfterPath consults GetVolumeFileSystem for the event seed; failing
	// it just yields version 0, which is fine here.
	s.fsService.On("GetVolumeFileSystem", mock.Anything).Return(nil, status.Error(codes.NotFound, "no fs")).Maybe()
	src := new(nodefs2.MockFile)
	dst := new(nodefs2.MockFile)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	srcFd := sess.RegisterFile("testVolume", "/cfr-src", src)
	dstFd := sess.RegisterFile("testVolume", "/cfr-dst", dst)

	// Generic copy loop: one same-inode probe per file (Ino stays 0 — overlap
	// check skipped), then one Read + one Write moving the 3 bytes.
	src.EXPECT().GetAttr(mock.Anything).Return(fuse.OK).Once()
	dst.EXPECT().GetAttr(mock.Anything).Return(fuse.OK).Once()
	src.EXPECT().Read(mock.Anything, int64(0)).Return(fuse.ReadResultData([]byte("abc")), fuse.OK).Once()
	dst.EXPECT().Write([]byte("abc"), int64(0)).Return(uint32(3), fuse.OK).Once()
	src.EXPECT().Release().Return().Maybe()
	dst.EXPECT().Release().Return().Maybe()

	req := &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: dstFd, Length: 3,
		SessionId: s.sessionID, RequestId: "req-cfr-replay",
	}
	r1, err := s.server.CopyFileRange(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), r1.Status)
	s.Equal(uint64(3), r1.BytesCopied)

	r2, err := s.server.CopyFileRange(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Equal(r1.Status, r2.Status)
	s.Equal(r1.BytesCopied, r2.BytesCopied, "replay must return the cached bytes_copied")
	src.AssertExpectations(s.T()) // Read .Once(): the copy ran exactly once
	dst.AssertExpectations(s.T())
}

// TestCopyFileRange_EmptyRequestIDExecutesEachCall: id-less copies (what
// current clients send) must not dedup against each other.
func (s *RpcFileServerTestSuite) TestCopyFileRange_EmptyRequestIDExecutesEachCall() {
	s.fsService.On("GetVolumeFileSystem", mock.Anything).Return(nil, status.Error(codes.NotFound, "no fs")).Maybe()
	src := new(nodefs2.MockFile)
	dst := new(nodefs2.MockFile)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	srcFd := sess.RegisterFile("testVolume", "/cfr-src2", src)
	dstFd := sess.RegisterFile("testVolume", "/cfr-dst2", dst)

	src.EXPECT().GetAttr(mock.Anything).Return(fuse.OK).Times(2)
	dst.EXPECT().GetAttr(mock.Anything).Return(fuse.OK).Times(2)
	src.EXPECT().Read(mock.Anything, int64(0)).Return(fuse.ReadResultData([]byte("ab")), fuse.OK).Times(2)
	dst.EXPECT().Write([]byte("ab"), int64(0)).Return(uint32(2), fuse.OK).Times(2)
	src.EXPECT().Release().Return().Maybe()
	dst.EXPECT().Release().Return().Maybe()

	req := &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: dstFd, Length: 2,
		SessionId: s.sessionID, // no RequestId
	}
	for i := 0; i < 2; i++ {
		reply, err := s.server.CopyFileRange(testAuthedCtx("test-user"), req)
		s.Require().NoError(err)
		s.Equal(int32(fuse.OK), reply.Status)
		s.Equal(uint64(2), reply.BytesCopied)
	}
	src.AssertExpectations(s.T()) // Read .Times(2): both copies really executed
	dst.AssertExpectations(s.T())
}

// TestCopyFileRange_ReplayAfterFdReleaseReturnsCachedReply: retry after the
// fds were released (reconnect scenario) must hit the cache, not EBADF.
func (s *RpcFileServerTestSuite) TestCopyFileRange_ReplayAfterFdReleaseReturnsCachedReply() {
	s.fsService.On("GetVolumeFileSystem", mock.Anything).Return(nil, status.Error(codes.NotFound, "no fs")).Maybe()
	src := new(nodefs2.MockFile)
	dst := new(nodefs2.MockFile)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	srcFd := sess.RegisterFile("testVolume", "/cfr-src3", src)
	dstFd := sess.RegisterFile("testVolume", "/cfr-dst3", dst)

	src.EXPECT().GetAttr(mock.Anything).Return(fuse.OK).Once()
	dst.EXPECT().GetAttr(mock.Anything).Return(fuse.OK).Once()
	src.EXPECT().Read(mock.Anything, int64(0)).Return(fuse.ReadResultData([]byte("xyz")), fuse.OK).Once()
	dst.EXPECT().Write([]byte("xyz"), int64(0)).Return(uint32(3), fuse.OK).Once()
	src.EXPECT().Release().Return().Once() // the ReleaseFile below closes them
	dst.EXPECT().Release().Return().Once()

	req := &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: dstFd, Length: 3,
		SessionId: s.sessionID, RequestId: "req-cfr-released",
	}
	r1, err := s.server.CopyFileRange(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), r1.Status)

	sess.ReleaseFile(srcFd)
	sess.ReleaseFile(dstFd)

	r2, err := s.server.CopyFileRange(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), r2.Status, "cache hit must not consult the (now empty) fd table")
	s.Equal(uint64(3), r2.BytesCopied)
	src.AssertExpectations(s.T())
	dst.AssertExpectations(s.T())
}

// --------- Helper Functions ---------

// CreateCaller creates a caller object
func CreateCaller(uid, gid, pid uint32) *proto.Caller {
	return &proto.Caller{
		Owner: &proto.Owner{
			Uid: uid,
			Gid: gid,
		},
		Pid: pid,
	}
}

// registerRawFile creates a real temp file with content and registers it
// in the test session, returning the wire fd. Exercises the same
// RawFdFile type the confined FS hands out in production.
func (s *RpcFileServerTestSuite) registerRawFile(name string, content []byte) uint64 {
	p := filepath.Join(s.T().TempDir(), name)
	s.Require().NoError(os.WriteFile(p, content, 0o644))
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	rf := serverio.NewRawFdFile(f)
	s.T().Cleanup(func() { rf.Release() })
	sess, err := s.sessionMgr.Get(s.sessionID)
	s.Require().NoError(err)
	return sess.RegisterFile("testVolume", name, rf)
}

func (s *RpcFileServerTestSuite) TestCopyFileRange_Happy() {
	// versionAfterPath consults GetVolumeFileSystem; failing it just
	// yields version 0 on the event, which is fine here.
	s.fsService.On("GetVolumeFileSystem", mock.Anything).Return(nil, status.Error(codes.NotFound, "no fs")).Maybe()
	events, _, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	srcFd := s.registerRawFile("src", []byte("0123456789"))
	dstFd := s.registerRawFile("dst", []byte("XXXXXXXXXX"))

	reply, err := s.server.CopyFileRange(testAuthedCtx("test-user"), &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, OffIn: 2, FdOut: dstFd, OffOut: 5,
		Length: 3, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal(uint64(3), reply.BytesCopied)

	select {
	case ev := <-events:
		s.Equal("dst", ev.Path)
	case <-time.After(time.Second):
		s.Fail("expected a mutation event for the copy destination")
	}
}

// TestWriteAndFlushCrossVolumeFdRejected pins the SEC-1 fix: an fd opened on
// volume A must not be usable with request.Volume = B. The handler must treat
// the mismatch exactly like an unknown fd (EBADF in-reply) and must NOT stat
// the path on volume B (no GetVolumeFileSystem/GetAttr against B) nor emit an
// event on B's bus.
func (s *RpcFileServerTestSuite) TestWriteAndFlushCrossVolumeFdRejected() {
	// fd registered on volume A ("testVolume").
	mockFile := new(nodefs2.MockFile)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/secret.txt", mockFile)
	mockFile.EXPECT().Release().Return().Maybe()

	// Watch volume B's bus: no event may be emitted there.
	events, _, cancel := s.bus.Subscribe("otherVolume")
	defer cancel()

	reply, err := s.server.WriteAndFlush(testAuthedCtx("test-user"), &proto.WriteAndFlushRequest{
		Volume: "otherVolume", Fd: fd, Data: []byte("x"), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.EBADF), reply.Status, "cross-volume fd use must look like an unknown fd")
	s.Assert().Nil(reply.FinalAttr, "no metadata may leak for the foreign volume")

	// The handler must not have touched volume B at all.
	s.fsService.AssertNotCalled(s.T(), "GetVolumeFileSystem", "otherVolume")
	mockFile.AssertNotCalled(s.T(), "Write", mock.Anything, mock.Anything)
	mockFile.AssertNotCalled(s.T(), "Flush")
	select {
	case ev := <-events:
		s.FailNowf("unexpected event on foreign volume bus", "event: %+v", ev)
	default:
	}
}

// TestReadCrossVolumeFdRejected: same property for the streaming Read path —
// the terminal frame carries EBADF and the file is never read.
func (s *RpcFileServerTestSuite) TestReadCrossVolumeFdRejected() {
	mockFile := new(nodefs2.MockFile)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("testVolume", "/secret.txt", mockFile)
	mockFile.EXPECT().Release().Return().Maybe()

	stream := newFakeReadStream(testAuthedCtx("test-user"))
	err := s.server.Read(&proto.ReadRequest{
		Volume: "otherVolume", Fd: fd, Size: 16, SessionId: s.sessionID,
	}, stream)
	s.Require().NoError(err)
	s.Require().Len(stream.frames, 1)
	s.Assert().Equal(int32(fuse.EBADF), stream.frames[0].Status)
	mockFile.AssertNotCalled(s.T(), "Read", mock.Anything, mock.Anything)
}

func (s *RpcFileServerTestSuite) TestCopyFileRange_BadFd() {
	srcFd := s.registerRawFile("src2", []byte("data"))
	reply, err := s.server.CopyFileRange(testAuthedCtx("test-user"), &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: 9999, Length: 4, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EBADF), reply.Status)
}

func (s *RpcFileServerTestSuite) TestCopyFileRange_NonzeroFlags_EINVAL() {
	srcFd := s.registerRawFile("src3", []byte("data"))
	dstFd := s.registerRawFile("dst3", []byte("XXXX")) // distinct fd: without the gate this copy would SUCCEED
	reply, err := s.server.CopyFileRange(testAuthedCtx("test-user"), &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: dstFd, Length: 1, Flags: 1, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EINVAL), reply.Status)
	s.Equal(uint64(0), reply.BytesCopied) // gate fires before any copy
}

func (s *RpcFileServerTestSuite) TestLseek_DataAndPastEOF() {
	fd := s.registerRawFile("lf", []byte("0123456789"))

	reply, err := s.server.Lseek(testAuthedCtx("test-user"), &proto.LseekRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Whence: uint32(unix.SEEK_DATA), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal(uint64(0), reply.Offset)

	reply, err = s.server.Lseek(testAuthedCtx("test-user"), &proto.LseekRequest{
		Volume: "testVolume", Fd: fd, Offset: 100, Whence: uint32(unix.SEEK_DATA), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.Status(syscall.ENXIO)), reply.Status)
}

func (s *RpcFileServerTestSuite) TestLseek_BadWhence_EINVAL() {
	fd := s.registerRawFile("lf2", []byte("x"))
	reply, err := s.server.Lseek(testAuthedCtx("test-user"), &proto.LseekRequest{
		Volume: "testVolume", Fd: fd, Whence: 0 /* SEEK_SET — kernel never sends it */, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EINVAL), reply.Status)
}

func (s *RpcFileServerTestSuite) TestLseek_BadFd() {
	reply, err := s.server.Lseek(testAuthedCtx("test-user"), &proto.LseekRequest{
		Volume: "testVolume", Fd: 9999, Whence: uint32(unix.SEEK_DATA), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EBADF), reply.Status)
}

func TestRpcFileServerTestSuite(t *testing.T) {
	suite.Run(t, new(RpcFileServerTestSuite))
}
