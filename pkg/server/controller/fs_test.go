package controller

import (
	"context"
	"gmountie/internal/mocks/pkg/server/service"
	"testing"

	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

type RpcServerTestSuite struct {
	suite.Suite
	server    *RpcServerImpl
	fsService *service.MockVolumeService
}

func (s *RpcServerTestSuite) SetupTest() {
	s.fsService = new(service.MockVolumeService)
	s.server = NewGrpcServer(s.fsService)
}

func (s *RpcServerTestSuite) TestGetAttr() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
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
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Mkdir("/test/path", uint32(0), mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.MkdirRequest{Volume: "testVolume", Path: "/test/path", Mode: 0, Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Mkdir(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestRmdir() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Rmdir("/test/path", mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.RmdirRequest{Volume: "testVolume", Path: "/test/path", Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Rmdir(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestRename() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Rename("/old/path", "/new/path", mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.RenameRequest{Volume: "testVolume", OldName: "/old/path", NewName: "/new/path", Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Rename(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestOpenDir() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
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
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
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
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
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
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Unlink("/test/path", mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.UnlinkRequest{Volume: "testVolume", Path: "/test/path", Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Unlink(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestAccess() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
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

func (s *RpcServerTestSuite) TestTruncate() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Truncate("/test/path", uint64(0), mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.TruncateRequest{Volume: "testVolume", Path: "/test/path", Size: 0, Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Truncate(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestChmod() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Chmod("/test/path", uint32(0), mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.ChmodRequest{Volume: "testVolume", Path: "/test/path", Mode: 0, Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Chmod(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestChown() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Chown("/test/path", uint32(0), uint32(0), mock.Anything).Return(fuse.OK)

	// Test.
	request := &proto.ChownRequest{Volume: "testVolume", Path: "/test/path", Uid: 0, Gid: 0, Caller: CreateCaller(0, 0, 0)}
	reply, err := s.server.Chown(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestGetXAttr() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
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

func TestRpcServerTestSuite(t *testing.T) {
	suite.Run(t, new(RpcServerTestSuite))
}
