package controller

import (
	"context"
	mockservice "gmountie/internal/mocks/pkg/server/service"
	"gmountie/pkg/server/service"
	"testing"

	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	nodefs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/nodefs"
	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RpcFileServerTestSuite struct {
	suite.Suite
	server     *RpcFileServerImpl
	fsService  *mockservice.MockVolumeService
	sessionMgr service.SessionManager
	sessionID  string
}

func (s *RpcFileServerTestSuite) SetupTest() {
	s.fsService = new(mockservice.MockVolumeService)
	s.sessionMgr = service.NewSessionManager(service.SessionManagerOptions{})
	sid, err := s.sessionMgr.Create()
	s.Require().NoError(err)
	s.sessionID = sid
	s.server = NewRpcFileServer(s.fsService, s.sessionMgr)
}

func (s *RpcFileServerTestSuite) TearDownTest() {
	_ = s.sessionMgr.Stop(context.Background())
}

func (s *RpcFileServerTestSuite) TestOpen() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Open("/test/path", uint32(0), mock.Anything).Return(nodefs.NewDefaultFile(), fuse.OK)

	// Test.
	request := &proto.OpenRequest{Volume: "testVolume", Path: "/test/path", Flags: 0, Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID}
	reply, err := s.server.Open(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestCreate() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Create("/test/path", uint32(0), uint32(0), mock.Anything).Return(nodefs.NewDefaultFile(), fuse.OK)

	// Test.
	request := &proto.CreateRequest{Volume: "testVolume", Path: "/test/path", Flags: 0, Mode: 0, Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID}
	reply, err := s.server.Create(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestRead() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("/test/path", mockFile)
	ctx := context.Background()
	mockFile.EXPECT().Read(mock.Anything, int64(0)).Return(fuse.ReadResultData([]byte("test data")), fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.ReadRequest{Fd: fd, Size: 1024, Offset: 0, SessionId: s.sessionID}
	reply, err := s.server.Read(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcFileServerTestSuite) TestWrite() {
	// Setup.
	mockFs := new(pathfs2.MockFileSystem)
	mockFile := new(nodefs2.MockFile)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	sess, _ := s.sessionMgr.Get(s.sessionID)
	fd := sess.RegisterFile("/test/path", mockFile)
	ctx := context.Background()
	mockFile.EXPECT().Write([]byte("test data"), int64(0)).Return(uint32(len("test data")), fuse.OK)
	mockFile.EXPECT().Release().Return().Maybe()

	// Test.
	request := &proto.WriteRequest{Fd: fd, Bytes: []byte("test data"), Offset: 0, SessionId: s.sessionID}
	reply, err := s.server.Write(ctx, request)

	// Verify.
	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
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
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	// Open returns a non-OK status.
	mockFs.EXPECT().Open("/test/path", uint32(0), mock.Anything).
		Return(nil, fuse.ENOENT)

	request := &proto.OpenRequest{
		Volume: "testVolume", Path: "/test/path", Flags: 0,
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
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
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	mockFs.EXPECT().Create("/p", uint32(0), uint32(0), mock.Anything).
		Return(nil, fuse.EACCES)

	request := &proto.CreateRequest{
		Volume: "testVolume", Path: "/p",
		Caller: CreateCaller(0, 0, 0), SessionId: s.sessionID,
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
	_, err := s.server.Read(context.Background(), request)
	s.Require().Error(err)
	st, ok := status.FromError(err)
	s.Require().True(ok)
	s.Assert().Equal(codes.NotFound, st.Code())
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
