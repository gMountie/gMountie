package controller

import (
	"context"
	"testing"
	"time"

	mockservice "go.gmountie.dev/gmountie/internal/mocks/pkg/server/service"
	serverio "go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"go.gmountie.dev/gmountie/pkg/proto"

	pathfs2 "go.gmountie.dev/gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"

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
	// Default permissive stub for ResolveIdentity — attr-returning handlers
	// now consult it to fill Owner.user_name/group_name. Individual tests
	// that care about the names can override via .On("ResolveIdentity", ...).
	s.fsService.On("ResolveIdentity", mock.Anything, mock.Anything, mock.Anything).
		Return(service.Identity{}, nil).Maybe()
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create("test-user", "")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
		Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)

	request := &proto.MkdirRequest{
		Volume: "testVolume", Path: "/p", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "",
	}
	_, err := s.server.Mkdir(testAuthedCtx("test-user"), request)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.InvalidArgument, st.Code())
}

func (s *RpcServerTestSuite) TestMkdirDuplicateRequestIDReturnsCachedReply() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Mkdir("/p", uint32(0), mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/p", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	request := &proto.MkdirRequest{
		Volume: "testVolume", Path: "/p", Mode: 0,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "dup-req-mkdir",
	}
	r1, err := s.server.Mkdir(testAuthedCtx("test-user"), request)
	s.Require().NoError(err)

	r2, err := s.server.Mkdir(testAuthedCtx("test-user"), request)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Unlink("/test/path", mock.Anything).Return(fuse.OK)

	// Act.
	request := &proto.UnlinkRequest{
		Volume:    "testVolume",
		Path:      "/test/path",
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-unlink-emit",
	}
	reply, err := s.server.Unlink(testAuthedCtx("test-user"), request)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
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

// TestGetAttrIfChanged_PassesWireCaller verifies the protective property of
// the spec §11 follow-up: the wire Caller flows into BindIdentity (and thus
// into the resolver's passthrough path), instead of nil — which previously
// would have squashed every cache-revalidation stat to anon. Mapped modes
// still ignore Caller and use the ctx principal; this test asserts the wire
// is plumbed, not the mapping mode itself.
func (s *RpcServerTestSuite) TestGetAttrIfChanged_PassesWireCaller() {
	mockFs := new(pathfs2.MockFileSystem)
	wireCaller := CreateCaller(1234, 5678, 0)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", wireCaller).Return(mockFs, service.Identity{}, nil).Once()

	attr := &fuse.Attr{Ino: 1, Size: 1, Mode: 0o644, Nlink: 1}
	mockFs.EXPECT().GetAttr("/x.bin", mock.Anything).Return(attr, fuse.OK)

	_, err := s.server.GetAttrIfChanged(context.Background(), &proto.GetAttrIfChangedRequest{
		Volume:       "testVolume",
		Path:         "/x.bin",
		KnownVersion: 0,
		Caller:       wireCaller,
	})
	s.Require().NoError(err)
	s.fsService.AssertExpectations(s.T()) // BindIdentity was called with the wire Caller
}

func (s *RpcServerTestSuite) TestUtimens() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	ctx := testAuthedCtx("test-user")
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

// ----- SetAttr (single-RPC attribute application) -----

func (s *RpcServerTestSuite) TestSetAttr_SizeOnly() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	attr := &fuse.Attr{Ino: 7, Size: 128, Mode: 0o644, Nlink: 1, Mtime: 1700000000}
	mockFs.EXPECT().Truncate("/f", uint64(128), mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(attr, fuse.OK).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid: fuse.FATTR_SIZE, Size: 128,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-size",
	})
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), reply.Status)
	// Reply attrs must match what a trailing GetAttr would have returned —
	// the client uses them instead of issuing its own stat.
	s.Require().NotNil(reply.Attributes)
	s.Equal(uint64(7), reply.Attributes.Ino)
	s.Equal(uint64(128), reply.Attributes.Size)
	s.Equal(serverio.VersionFromAttr(attr), reply.Attributes.Version)
	mockFs.AssertExpectations(s.T())
}

func (s *RpcServerTestSuite) TestSetAttr_MultiFieldAppliesAll() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mtime := time.Unix(1577836800, 42)
	mockFs.EXPECT().Truncate("/f", uint64(64), mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().Chmod("/f", uint32(0o600), mock.Anything).Return(fuse.OK).Once()
	// ATIME valid bit is unset, so atime must be passed as nil (UTIME_OMIT)
	// even though the request carries an Atime payload.
	mockFs.EXPECT().Utimens("/f", (*time.Time)(nil), &mtime, mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1, Size: 64}, fuse.OK).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid: fuse.FATTR_SIZE | fuse.FATTR_MODE | fuse.FATTR_MTIME,
		Size:  64, Mode: 0o600,
		Atime:     &proto.FileTime{Sec: 9999, Nsec: 0}, // bit unset — must be ignored
		Mtime:     &proto.FileTime{Sec: 1577836800, Nsec: 42},
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-multi",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.NotNil(reply.Attributes)
	mockFs.AssertExpectations(s.T()) // every requested field was applied exactly once
}

func (s *RpcServerTestSuite) TestSetAttr_StopsAtFirstFailure() {
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Truncate("/f", uint64(0), mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().Chmod("/f", uint32(0o600), mock.Anything).Return(fuse.EPERM).Once()
	// No Chown/Utimens/GetAttr expectations: applying anything after the
	// failed chmod (or statting for reply attrs) fails the test via the
	// strict mock.

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid: fuse.FATTR_SIZE | fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_MTIME,
		Size:  0, Mode: 0o600, Uid: 1, Gid: 1,
		Mtime:     &proto.FileTime{Sec: 1, Nsec: 0},
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-stop",
	})
	s.Require().NoError(err, "fs failures travel in-band, not as RPC errors")
	s.Equal(int32(fuse.EPERM), reply.Status)
	s.Nil(reply.Attributes)

	// The truncate before the failing chmod DID mutate the file, so client
	// caches must still be invalidated — version 0 forces revalidation.
	select {
	case ev := <-events:
		s.Equal(serverio.KindMutated, ev.Kind)
		s.Equal("/f", ev.Path)
		s.Equal(uint64(0), ev.NewVersion)
	case <-time.After(time.Second):
		s.FailNow("partially-applied SetAttr must still emit a KindMutated event")
	}
	mockFs.AssertExpectations(s.T())
}

func (s *RpcServerTestSuite) TestSetAttr_FirstStepFailureEmitsNothing() {
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Truncate("/f", uint64(0), mock.Anything).Return(fuse.EACCES).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid: fuse.FATTR_SIZE | fuse.FATTR_MODE,
		Size:  0, Mode: 0o600,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-fail-first",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EACCES), reply.Status)

	// Nothing was applied — no mutation event may fire. Emission is
	// synchronous (buffered channel push inside the handler), so a
	// non-blocking read after the call is conclusive.
	select {
	case ev := <-events:
		s.FailNowf("unexpected event", "no mutation happened, got %+v", ev)
	default:
	}
	mockFs.AssertExpectations(s.T())
}

// TestSetAttr_NoBitsIsReadOnly asserts the Valid=0 contract: when no FATTR_*
// bits are set, the handler is a pure read — it calls GetAttr once for the
// reply attrs, returns fuse.OK, and calls no mutation methods (Truncate,
// Chmod, Chown, Utimens) and emits no event (nothing mutated).
func (s *RpcServerTestSuite) TestSetAttr_NoBitsIsReadOnly() {
	// Subscribe first so any spurious event would be caught.
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	attr := &fuse.Attr{Ino: 42, Size: 256, Mode: 0o644, Nlink: 1, Mtime: 1700000000}
	// Exactly one GetAttr call for the reply attrs — no mutation expectations
	// registered, so the strict mock will fail if any are made.
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(attr, fuse.OK).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid:     0, // no bits set
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-nobits",
	})
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), reply.Status)
	s.Require().NotNil(reply.Attributes)
	s.Equal(uint64(42), reply.Attributes.Ino)
	s.Equal(uint64(256), reply.Attributes.Size)

	// No mutation event may fire — nothing was changed.
	select {
	case ev := <-events:
		s.FailNowf("unexpected event", "Valid=0 must not emit any event, got %+v", ev)
	default:
	}
	mockFs.AssertExpectations(s.T()) // absence proof: no Truncate/Chmod/Chown/Utimens called
}

func (s *RpcServerTestSuite) TestSetAttr_UidOnlyResolvesGidFromStat() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	attr := &fuse.Attr{Ino: 3, Owner: fuse.Owner{Uid: 500, Gid: 2000}}
	// One stat resolves the unset gid half; a second serves the reply attrs.
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(attr, fuse.OK).Times(2)
	mockFs.EXPECT().Chown("/f", uint32(1000), uint32(2000), mock.Anything).Return(fuse.OK).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid:     fuse.FATTR_UID,
		Uid:       1000,
		Gid:       9999, // GID bit unset — must come from the stat, not the wire
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-uid-only",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	mockFs.AssertExpectations(s.T()) // Chown received the stat's gid 2000
}

func (s *RpcServerTestSuite) TestSetAttr_GidOnlyResolvesUidFromStat() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	attr := &fuse.Attr{Ino: 3, Owner: fuse.Owner{Uid: 500, Gid: 2000}}
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(attr, fuse.OK).Times(2)
	mockFs.EXPECT().Chown("/f", uint32(500), uint32(3000), mock.Anything).Return(fuse.OK).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid:     fuse.FATTR_GID,
		Uid:       9999, // UID bit unset — must come from the stat, not the wire
		Gid:       3000,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-gid-only",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	mockFs.AssertExpectations(s.T()) // Chown received the stat's uid 500
}

func (s *RpcServerTestSuite) TestSetAttr_IdempotentReplay() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	attr := &fuse.Attr{Ino: 9, Size: 32, Mtime: 1700000000}
	mockFs.EXPECT().Truncate("/f", uint64(32), mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(attr, fuse.OK).Once()

	req := &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid: fuse.FATTR_SIZE, Size: 32,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-replay",
	}
	r1, err := s.server.SetAttr(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	r2, err := s.server.SetAttr(testAuthedCtx("test-user"), req)
	s.Require().NoError(err)
	s.Equal(r1.Status, r2.Status)
	s.Require().NotNil(r2.Attributes)
	s.Equal(r1.Attributes.Version, r2.Attributes.Version, "replay must return the cached reply")
	mockFs.AssertExpectations(s.T()) // .Once() on Truncate+GetAttr proves the replay was deduped
}

func (s *RpcServerTestSuite) TestSetAttr_EmitsMutatedSeededFromReplyStat() {
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	attr := &fuse.Attr{Ino: 11, Size: 5, Mtime: 1700000000}
	mockFs.EXPECT().Chmod("/f", uint32(0o640), mock.Anything).Return(fuse.OK).Once()
	// ONE stat total: the reply-attrs GetAttr also seeds the event version —
	// the handler must not re-stat via versionAfter.
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(attr, fuse.OK).Once()

	reply, err := s.server.SetAttr(testAuthedCtx("test-user"), &proto.SetAttrRequest{
		Volume: "testVolume", Path: "/f",
		Valid: fuse.FATTR_MODE, Mode: 0o640,
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID, RequestId: "req-setattr-emit",
	})
	s.Require().NoError(err)
	s.Require().Equal(int32(fuse.OK), reply.Status)

	select {
	case ev := <-events:
		s.Equal(serverio.KindMutated, ev.Kind)
		s.Equal("/f", ev.Path)
		s.NotZero(ev.NewVersion)
		s.Equal(serverio.VersionFromAttr(attr), ev.NewVersion)
	case <-time.After(time.Second):
		s.FailNow("SetAttr did not emit a KindMutated event within 1s")
	}
	mockFs.AssertExpectations(s.T()) // GetAttr .Once(): no extra stat beyond the reply attrs
}

func (s *RpcServerTestSuite) TestReadlink() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Readlink("/link", mock.Anything).Return("real.txt", fuse.OK)

	reply, err := s.server.Readlink(context.Background(), &proto.ReadlinkRequest{
		Volume: "testVolume",
		Caller: CreateCaller(0, 0, 0),
		Path:   "/link",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal("real.txt", reply.Target)
}

func (s *RpcServerTestSuite) TestSymlink() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().Symlink("../target", "/link", mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/link", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	reply, err := s.server.Symlink(testAuthedCtx("test-user"), &proto.SymlinkRequest{
		Volume:    "testVolume",
		Caller:    CreateCaller(0, 0, 0),
		Target:    "../target",
		LinkPath:  "/link",
		SessionId: s.sessionID,
		RequestId: "test-req-symlink",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestSetXAttr_Happy() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().SetXAttr("/f", "user.k", []byte("v"), 0, mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1}, fuse.OK).Maybe()

	// Subscribe to assert a mutation event is emitted on success.
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	reply, err := s.server.SetXAttr(testAuthedCtx("test-user"), &proto.SetXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "user.k", Data: []byte("v"),
		SessionId: s.sessionID, RequestId: "req-setx-1",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)

	select {
	case ev := <-events:
		s.Assert().Equal(serverio.KindMutated, ev.Kind)
		s.Assert().Equal("/f", ev.Path)
	case <-time.After(time.Second):
		s.FailNow("SetXAttr did not emit a KindMutated event within 1s")
	}
}

func (s *RpcServerTestSuite) TestSetXAttr_DisallowedNamespace_EPERM() {
	// No BindIdentity expectation: policy must reject BEFORE touching the FS.
	reply, err := s.server.SetXAttr(testAuthedCtx("test-user"), &proto.SetXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "trusted.evil", Data: []byte("v"),
		SessionId: s.sessionID, RequestId: "req-setx-2",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EPERM), reply.Status)
}

func (s *RpcServerTestSuite) TestSetXAttr_IdempotentReplay() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().SetXAttr("/f", "user.k", []byte("v"), 0, mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1}, fuse.OK).Maybe()

	req := &proto.SetXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "user.k", Data: []byte("v"),
		SessionId: s.sessionID, RequestId: "req-setx-replay",
	}
	for i := 0; i < 2; i++ {
		reply, err := s.server.SetXAttr(testAuthedCtx("test-user"), req)
		s.Require().NoError(err)
		s.Equal(int32(fuse.OK), reply.Status)
	}
	mockFs.AssertExpectations(s.T()) // .Once() proves the replay was deduped
}

func (s *RpcServerTestSuite) TestRemoveXAttr_Happy() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().RemoveXAttr("/f", "user.k", mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1}, fuse.OK).Maybe()

	reply, err := s.server.RemoveXAttr(testAuthedCtx("test-user"), &proto.RemoveXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "user.k",
		SessionId: s.sessionID, RequestId: "req-rmx-1",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestRemoveXAttr_DisallowedNamespace_EPERM() {
	// No BindIdentity expectation: policy must reject BEFORE touching the FS.
	reply, err := s.server.RemoveXAttr(testAuthedCtx("test-user"), &proto.RemoveXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "security.capability",
		SessionId: s.sessionID, RequestId: "req-rmx-2",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EPERM), reply.Status)
}

func (s *RpcServerTestSuite) TestListXAttr_Happy() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().ListXAttr("/f", mock.Anything).Return([]string{"user.a", "user.b"}, fuse.OK)

	reply, err := s.server.ListXAttr(testAuthedCtx("test-user"), &proto.ListXAttrRequest{
		Volume: "testVolume", Path: "/f",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal([]string{"user.a", "user.b"}, reply.Attributes)
}

func TestXattrWriteAllowed(t *testing.T) {
	cases := map[string]bool{
		"user.foo":                  true,
		"user.":                     true,
		"system.posix_acl_access":   true,
		"system.posix_acl_default":  true,
		"trusted.foo":               false,
		"security.capability":       false,
		"security.selinux":          false,
		"system.other":              false,
		"":                          false,
		"USER.foo":                  false, // case-sensitive
		"user":                      false, // no dot
		"system.posix_acl_accessX":  false, // near-miss on exact match
		"system.posix_acl_default ": false, // trailing whitespace
	}
	for attr, want := range cases {
		if got := xattrWriteAllowed(attr); got != want {
			t.Errorf("xattrWriteAllowed(%q) = %v, want %v", attr, got, want)
		}
	}
}

func TestRpcServerTestSuite(t *testing.T) {
	suite.Run(t, new(RpcServerTestSuite))
}
