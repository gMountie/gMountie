package controller

import (
	"context"
	mockservice "gmountie/internal/mocks/pkg/server/service"
	serverio "gmountie/pkg/server/io"
	"gmountie/pkg/server/service"
	"testing"
	"time"

	"gmountie/pkg/proto"

	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcServerTestSuite struct {
	suite.Suite
	server     *RpcServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
	bus        serverio.EventBus
}

func (s *RpcServerTestSuite) SetupTest() {
	s.fsService = new(mockservice.MockVolumeService)
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create()
	s.Require().NoError(err)
	s.sessionID = sid
	s.bus = serverio.NewLocalEventBus(serverio.EventBusOptions{BufferSize: 16})
	s.server = NewGrpcServer(s.fsService, s.sessionMgr, 0, s.bus, nil)
}

func (s *RpcServerTestSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
	s.bus.Close()
}

func (s *RpcServerTestSuite) TestGetAttr() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK)

	// Test.
	request := &proto.GetAttrRequest{Volume: "testVolume", Path: "/test/path", Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.GetAttr(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestMkdir() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Mkdir("/test/path", uint32(0), mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	// Test.
	request := &proto.MkdirRequest{
		Volume: "testVolume", Path: "/test/path", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-mkdir",
	}
	reply, err := s.server.Mkdir(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestRmdir() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Rmdir("/test/path", mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.RmdirRequest{
		Volume: "testVolume", Path: "/test/path",
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-rmdir",
	}
	reply, err := s.server.Rmdir(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestRename() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Rename("/old/path", "/new/path", mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/new/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	// Test.
	request := &proto.RenameRequest{
		Volume: "testVolume", OldName: "/old/path", NewName: "/new/path",
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-rename",
	}
	reply, err := s.server.Rename(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestOpenDir() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().OpenDir("/test/path", mock.Anything).Return([]fuse.DirEntry{}, fuse.OK)

	// Test.
	request := &proto.OpenDirRequest{Volume: "testVolume", Path: "/test/path", Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.OpenDir(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestStatFs() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().StatFs("/test/path").Return(&fuse.StatfsOut{})

	// Test.
	request := &proto.StatFsRequest{Volume: "testVolume", Path: "/test/path"}
	reply, err := s.server.StatFs(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
}

func (s *RpcServerTestSuite) TestStatFs_NilReplyReturnsError() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().StatFs("/test/path").Return(nil)

	// Test.
	request := &proto.StatFsRequest{Volume: "testVolume", Path: "/test/path"}
	reply, err := s.server.StatFs(ctx, request)

	// Verify.
	s.Require().Error(err)
	s.Equal(codes.NotFound, status.Code(err))
	s.Nil(reply)
}

func (s *RpcServerTestSuite) TestUnlink() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Unlink("/test/path", mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.UnlinkRequest{
		Volume: "testVolume", Path: "/test/path",
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-unlink",
	}
	reply, err := s.server.Unlink(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestAccess() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Access("/test/path", uint32(0), mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.AccessRequest{Volume: "testVolume", Path: "/test/path", Mode: 0, Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Access(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

// TestGetAttrBindsRequestIdentity asserts a path-op handler resolves its FS via
// BindIdentity, forwarding the request's exact volume and caller.
func (s *RpcServerTestSuite) TestGetAttrBindsRequestIdentity() {
	mockFs := new(pathfs2.MockFileSystem)
	caller := CreateCaller(1001, 2000, 4242)

	var gotVolume string
	var gotCaller *proto.Caller
	s.fsService.EXPECT().
		BindIdentity(mock.Anything, "testVolume", caller).
		Run(func(_ context.Context, volume string, c *proto.Caller) {
			gotVolume = volume
			gotCaller = c
		}).
		Return(mockFs, nil)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK)

	request := &proto.GetAttrRequest{Volume: "testVolume", Path: "/test/path", Caller: caller}
	_, err := s.server.GetAttr(context.Background(), request)

	s.Require().NoError(err)
	s.Assert().Equal("testVolume", gotVolume)
	s.Require().NotNil(gotCaller)
	s.Assert().Equal(caller, gotCaller)
}

func (s *RpcServerTestSuite) TestTruncate() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Truncate("/test/path", uint64(0), mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	// Test.
	request := &proto.TruncateRequest{
		Volume: "testVolume", Path: "/test/path", Size: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-truncate",
	}
	reply, err := s.server.Truncate(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestChmod() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Chmod("/test/path", uint32(0), mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	// Test.
	request := &proto.ChmodRequest{
		Volume: "testVolume", Path: "/test/path", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-chmod",
	}
	reply, err := s.server.Chmod(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestChown() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Chown("/test/path", uint32(0), uint32(0), mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	// Test.
	request := &proto.ChownRequest{
		Volume: "testVolume", Path: "/test/path", Uid: 0, Gid: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-chown",
	}
	reply, err := s.server.Chown(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestGetXAttr() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().GetXAttr("/test/path", "attribute", mock.Anything).Return([]byte("data"), fuse.OK)

	// Test.
	request := &proto.GetXAttrRequest{Volume: "testVolume", Path: "/test/path", Attribute: "attribute", Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.GetXAttr(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestMkdirEmptyRequestIDFails() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)

	request := &proto.MkdirRequest{
		Volume: "testVolume", Path: "/p", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "",
	}
	_, err := s.server.Mkdir(context.Background(), request)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *RpcServerTestSuite) TestMkdirDuplicateRequestIDReturnsCachedReply() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	mockFs.EXPECT().Mkdir("/p", uint32(0), mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/p", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	request := &proto.MkdirRequest{
		Volume: "testVolume", Path: "/p", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "dup-req-mkdir",
	}
	r1, err := s.server.Mkdir(context.Background(), request)
	s.Require().NoError(err)

	r2, err := s.server.Mkdir(context.Background(), request)
	s.Require().NoError(err)
	s.Assert().Equal(r1.Status, r2.Status, "duplicate request_id must return the cached reply")
}

func (s *RpcServerTestSuite) TestMkdirUnknownSessionReturnsNotFound() {
	request := &proto.MkdirRequest{
		Volume: "testVolume", Path: "/p", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: "no-such-session",
		RequestId: "req-1",
	}
	_, err := s.server.Mkdir(context.Background(), request)
	s.Require().Error(err)
	s.Assert().Equal(codes.NotFound, status.Code(err))
}

func (s *RpcServerTestSuite) TestUnlinkEmitsDeletedEvent() {
	// Setup: subscribe before the call.
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	mockFs.EXPECT().Unlink("/test/path", mock.Anything).Return(fuse.OK)

	// Act.
	request := &proto.UnlinkRequest{
		Volume:    "testVolume",
		Path:      "/test/path",
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-unlink-emit",
	}
	reply, err := s.server.Unlink(context.Background(), request)
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), reply.Status)

	// Assert: event must arrive promptly.
	select {
	case ev := <-events:
		s.Assert().Equal(serverio.KindDeleted, ev.Kind)
		s.Assert().Equal("/test/path", ev.Path)
		s.Assert().Equal(uint64(0), ev.NewVersion)
	case <-time.After(time.Second):
		s.FailNow("Unlink did not emit a KindDeleted event within 1s")
	}
}

func (s *RpcServerTestSuite) TestGetAttrIfChanged_NotModified() {
	// Setup. Both GetAttr and GetAttrIfChanged are identity-bound path ops.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()

	// Mock GetAttr to return an attr with a specific version.
	attr := &fuse.Attr{
		Ino: 123, Size: 1024, Mode: 0644, Nlink: 1,
		Owner: fuse.Owner{Uid: 0, Gid: 0},
	}
	mockFs.EXPECT().GetAttr("/existing.bin", mock.Anything).Return(attr, fuse.OK)

	// First call GetAttr to learn the version.
	getAttrReq := &proto.GetAttrRequest{
		Volume: "testVolume",
		Path:   "/existing.bin",
		Caller: CreateCaller(0, 0, 0),
	}
	getAttrReply, err := s.server.GetAttr(ctx, getAttrReq)
	s.Require().NoError(err)
	s.Require().NotNil(getAttrReply.Attributes)
	knownVersion := getAttrReply.Attributes.Version
	s.Require().NotZero(knownVersion)

	// Now call GetAttrIfChanged with that known version.
	// We need another mock for the second GetAttr call in GetAttrIfChanged.
	mockFs.EXPECT().GetAttr("/existing.bin", mock.Anything).Return(attr, fuse.OK)

	request := &proto.GetAttrIfChangedRequest{
		Volume:       "testVolume",
		Path:         "/existing.bin",
		KnownVersion: knownVersion,
	}
	reply, err := s.server.GetAttrIfChanged(ctx, request)
	s.Require().NoError(err)
	s.Assert().True(reply.NotModified)
	s.Assert().Nil(reply.Attrs)
}

func (s *RpcServerTestSuite) TestGetAttrIfChanged_Changed() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()

	attr := &fuse.Attr{
		Ino: 456, Size: 2048, Mode: 0644, Nlink: 1,
		Owner: fuse.Owner{Uid: 0, Gid: 0},
	}
	mockFs.EXPECT().GetAttr("/existing.bin", mock.Anything).Return(attr, fuse.OK)

	// Call with a version that does not match the current version.
	request := &proto.GetAttrIfChangedRequest{
		Volume:       "testVolume",
		Path:         "/existing.bin",
		KnownVersion: 999,
	}
	reply, err := s.server.GetAttrIfChanged(ctx, request)
	s.Require().NoError(err)
	s.Assert().False(reply.NotModified)
	s.Require().NotNil(reply.Attrs)
	s.Assert().NotZero(reply.Attrs.Version)
	s.Assert().Equal(uint64(456), reply.Attrs.Ino)
}

func (s *RpcServerTestSuite) TestGetAttrIfChanged_ENOENT() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()

	mockFs.EXPECT().GetAttr("/no-such.bin", mock.Anything).Return((*fuse.Attr)(nil), fuse.ENOENT)

	// Call with a non-existent path.
	request := &proto.GetAttrIfChangedRequest{
		Volume:       "testVolume",
		Path:         "/no-such.bin",
		KnownVersion: 0,
	}
	reply, err := s.server.GetAttrIfChanged(ctx, request)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.NotFound, st.Code())
	_ = reply
}

func (s *RpcServerTestSuite) TestUtimens() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, nil)
	ctx := context.Background()
	expectedMtime := time.Unix(1577836800, 0)
	mockFs.EXPECT().Utimens(
		"/test/path",
		(*time.Time)(nil), // atime omitted
		&expectedMtime,
		mock.Anything,
	).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	// Test.
	request := &proto.UtimensRequest{
		Volume: "testVolume", Path: "/test/path",
		Mtime:     &proto.FileTime{Sec: 1577836800, Nsec: 0},
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-utimens",
	}
	reply, err := s.server.Utimens(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func TestRpcServerTestSuite(t *testing.T) {
	suite.Run(t, new(RpcServerTestSuite))
}
