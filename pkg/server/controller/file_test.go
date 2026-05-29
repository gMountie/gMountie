package controller

import (
	"context"
	mockservice "gmountie/internal/mocks/pkg/server/service"
	serverio "gmountie/pkg/server/io"
	"gmountie/pkg/server/metrics"
	"gmountie/pkg/server/service"
	"testing"
	"time"

	"gmountie/pkg/proto"

	nodefs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/nodefs"
	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
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
	sid, err := s.sessionMgr.Create("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
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
	fd := sess.RegisterFile("/test/path", mockFile)
	mockFile.EXPECT().Read(mock.Anything, int64(0)).Return(fuse.ReadResultData([]byte("test data")), fuse.OK).Once()
	// Second call hits the EOF branch (short read returned len("test data")<1024).
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.ReadRequest{Fd: fd, Size: 1024, Offset: 0, SessionId: s.sessionID}
	stream := newFakeReadStream(context.Background())
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
	fd := sess.RegisterFile("/test/path", mockFile)
	ctx := context.Background()
	mockFile.EXPECT().Fsync(int(0)).Return(fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.FsyncRequest{Fd: fd, Flags: 0, SessionId: s.sessionID}
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
	fd := sess.RegisterFile("/test/path", mockFile)
	ctx := context.Background()
	mockFile.EXPECT().Release().Return()

	// Test.
	request := &proto.ReleaseRequest{Fd: fd, SessionId: s.sessionID}
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
	fd := sess.RegisterFile("/test/path", mockFile)
	ctx := context.Background()
	mockFile.EXPECT().Flush().Return(fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.FlushRequest{Fd: fd, SessionId: s.sessionID}
	reply, err := s.server.Flush(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestOpenNonOkDoesNotRegisterFd() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	// Open returns a non-OK status.
	mockFs.EXPECT().Open("/test/path", uint32(0), mock.Anything).
		Return(nil, fuse.ENOENT)

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/test/path", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "test-req-open-non-ok",
	}
	reply, err := s.server.Open(context.Background(), request)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	mockFs.EXPECT().Create("/p", uint32(0), uint32(0), mock.Anything).
		Return(nil, fuse.EACCES)

	request := &proto.CreateRequest{
		Volume: "testVolume", Path: "/p",
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "test-req-create-non-ok",
	}
	reply, err := s.server.Create(context.Background(), request)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/p", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "",
	}
	_, err := s.server.Open(context.Background(), request)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *RpcFileServerTestSuite) TestOpenDuplicateRequestIDReturnsCachedReply() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	mockFs.EXPECT().Open("/p", uint32(0), mock.Anything).
		Return(nodefs.NewDefaultFile(), fuse.OK).Once()

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/p", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
		RequestId: "dup-req-1",
	}
	r1, err := s.server.Open(context.Background(), request)
	s.Require().NoError(err)

	r2, err := s.server.Open(context.Background(), request)
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
	fd := sess.RegisterFile("/waf.txt", mockFile)
	ctx := context.Background()

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
	fd := sess.RegisterFile("/waf2.txt", mockFile)
	ctx := context.Background()

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
	fd := sess.RegisterFile("/err.txt", mockFile)
	ctx := context.Background()

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
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	// Setup: register a writable file.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("/emit.txt", mockFile)
	ctx := context.Background()

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

func TestRpcFileServerTestSuite(t *testing.T) {
	suite.Run(t, new(RpcFileServerTestSuite))
}
