package io

import (
	"context"
	"gmountie/internal/mocks/pkg/client/grpc"
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

type LocalFileSystemTestSuite struct {
	suite.Suite
	client     *grpc.MockClient
	fs         *LocalFileSystem
	fsClient   *mockProto.MockRpcFsClient
	fileClient *mockProto.MockRpcFileClient
}

func (s *LocalFileSystemTestSuite) SetupTest() {
	s.client = grpc.NewMockClient(s.T())
	s.fsClient = mockProto.NewMockRpcFsClient(s.T())
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.client.EXPECT().Fs().Return(s.fsClient).Maybe()
	s.client.EXPECT().File().Return(s.fileClient).Maybe()
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
	s.fs = NewLocalFileSystem(s.client, "testVolume").(*LocalFileSystem)
}

func (s *LocalFileSystemTestSuite) TestGetAttr() {
	// Setup
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
	s.fsClient.EXPECT().GetAttr(mock.Anything, &proto.GetAttrRequest{
		Volume: "testVolume",
		Path:   "/test",
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.GetAttrReply{
		Status:     int32(fuse.OK),
		Attributes: testAttr,
	}, nil)

	// Test
	attr, status := s.fs.GetAttr("/test", &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
	s.Assert().Equal(testAttr.Size, attr.Size)
	s.Assert().Equal(testAttr.Mode, attr.Mode)
}

func (s *LocalFileSystemTestSuite) TestMkdir() {
	// Setup
	s.fsClient.EXPECT().Mkdir(mock.Anything, &proto.MkdirRequest{
		Volume: "testVolume",
		Path:   "/test",
		Mode:   0755,
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.MkdirReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Mkdir("/test", 0755, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestRmdir() {
	// Setup
	s.fsClient.EXPECT().Rmdir(mock.Anything, &proto.RmdirRequest{
		Volume: "testVolume",
		Path:   "/test",
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.RmdirReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Rmdir("/test", &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestRename() {
	// Setup
	s.fsClient.EXPECT().Rename(mock.Anything, &proto.RenameRequest{
		Volume:  "testVolume",
		OldName: "/old",
		NewName: "/new",
		Caller:  &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.RenameReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Rename("/old", "/new", &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestOpenDir() {
	// Setup
	entries := []*proto.DirEntry{
		{Name: "file1", Mode: 0644, Ino: 1},
		{Name: "file2", Mode: 0644, Ino: 2},
	}
	s.fsClient.EXPECT().OpenDir(mock.Anything, &proto.OpenDirRequest{
		Volume: "testVolume",
		Path:   "/test",
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.OpenDirReply{
		Status:  int32(fuse.OK),
		Entries: entries,
	}, nil)

	// Test
	result, status := s.fs.OpenDir("/test", &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
	s.Assert().Len(result, 2)
	s.Assert().Equal("file1", result[0].Name)
	s.Assert().Equal("file2", result[1].Name)
}

func (s *LocalFileSystemTestSuite) TestOpen() {
	// Setup
	s.fileClient.EXPECT().Open(mock.Anything, &proto.OpenRequest{
		Volume:    "testVolume",
		Path:      "/test",
		Flags:     0,
		Caller:    &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
		SessionId: "test-session",
	}).Return(&proto.OpenReply{
		Status: int32(fuse.OK),
		Fd:     1,
	}, nil)

	// Test
	file, status := s.fs.Open("/test", 0, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
	s.Assert().NotNil(file)
	s.Assert().IsType(&GrpcFile{}, file)
}

func (s *LocalFileSystemTestSuite) TestOpenStampsSessionID() {
	s.fileClient.EXPECT().Open(mock.Anything, mock.MatchedBy(func(req *proto.OpenRequest) bool {
		return req.SessionId == "test-session"
	})).Return(&proto.OpenReply{Status: int32(fuse.OK), Fd: 1}, nil).Once()

	file, st := s.fs.Open("/test", 0, &fuse.Context{
		Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
		Cancel: nil,
	})
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(file)
}

func (s *LocalFileSystemTestSuite) TestCreate() {
	// Setup
	s.fileClient.EXPECT().Create(mock.Anything, &proto.CreateRequest{
		Volume:    "testVolume",
		Path:      "/test",
		Flags:     0,
		Mode:      0644,
		Caller:    &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
		SessionId: "test-session",
	}).Return(&proto.CreateReply{
		Status: int32(fuse.OK),
		Fd:     1,
	}, nil)

	// Test
	file, status := s.fs.Create("/test", 0, 0644, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
	s.Assert().NotNil(file)
	s.Assert().IsType(&GrpcFile{}, file)
}

func (s *LocalFileSystemTestSuite) TestUnlink() {
	// Setup
	s.fsClient.EXPECT().Unlink(mock.Anything, &proto.UnlinkRequest{
		Volume: "testVolume",
		Path:   "/test",
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.UnlinkReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Unlink("/test", &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestStatFs() {
	// Setup
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

	// Test
	stats := s.fs.StatFs("/test")

	// Verify
	s.Assert().NotNil(stats)
	s.Assert().Equal(uint64(1000), stats.Blocks)
	s.Assert().Equal(uint64(500), stats.Bfree)
}

func (s *LocalFileSystemTestSuite) TestChmod() {
	// Setup
	s.fsClient.EXPECT().Chmod(mock.Anything, &proto.ChmodRequest{
		Volume: "testVolume",
		Path:   "/test",
		Mode:   0644,
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.ChmodReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Chmod("/test", 0644, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestChown() {
	// Setup
	s.fsClient.EXPECT().Chown(mock.Anything, &proto.ChownRequest{
		Volume: "testVolume",
		Path:   "/test",
		Uid:    1001,
		Gid:    1001,
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.ChownReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Chown("/test", 1001, 1001, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestAccess() {
	// Setup
	s.fsClient.EXPECT().Access(mock.Anything, &proto.AccessRequest{
		Volume: "testVolume",
		Path:   "/test",
		Mode:   0444,
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.AccessReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Access("/test", 0444, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestTruncate() {
	// Setup
	s.fsClient.EXPECT().Truncate(mock.Anything, &proto.TruncateRequest{
		Volume: "testVolume",
		Path:   "/test",
		Size:   1024,
		Caller: &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.TruncateReply{Status: int32(fuse.OK)}, nil)

	// Test
	status := s.fs.Truncate("/test", 1024, &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
}

func (s *LocalFileSystemTestSuite) TestGetXAttr() {
	// Setup
	expectedData := []byte("xattr_value")
	s.fsClient.EXPECT().GetXAttr(mock.Anything, &proto.GetXAttrRequest{
		Volume:    "testVolume",
		Path:      "/test",
		Attribute: "user.test",
		Caller:    &proto.Caller{Owner: &proto.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
	}).Return(&proto.GetXAttrReply{
		Status: int32(fuse.OK),
		Data:   expectedData,
	}, nil)

	// Test
	data, status := s.fs.GetXAttr("/test", "user.test", &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{Uid: 1000, Gid: 1000},
			Pid:   1000,
		},
		Cancel: nil,
	})

	// Verify
	s.Assert().Equal(fuse.OK, status)
	s.Assert().Equal(expectedData, data)
}

// Error cases
func (s *LocalFileSystemTestSuite) TestGetAttr_Error() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	attr, status := s.fs.GetAttr("/test", &fuse.Context{})
	s.Assert().Equal(fuse.EIO, status)
	s.Assert().NotNil(attr)
}

func (s *LocalFileSystemTestSuite) TestOpen_Error() {
	s.fileClient.EXPECT().Open(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	file, status := s.fs.Open("/test", 0, &fuse.Context{})
	s.Assert().Equal(fuse.EIO, status)
	s.Assert().Nil(file)
}

func (s *LocalFileSystemTestSuite) TestChmod_Error() {
	s.fsClient.EXPECT().Chmod(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	status := s.fs.Chmod("/test", 0644, &fuse.Context{})
	s.Assert().Equal(fuse.EIO, status)
}

func (s *LocalFileSystemTestSuite) TestChown_Error() {
	s.fsClient.EXPECT().Chown(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	status := s.fs.Chown("/test", 1001, 1001, &fuse.Context{})
	s.Assert().Equal(fuse.EIO, status)
}

func (s *LocalFileSystemTestSuite) TestAccess_Error() {
	s.fsClient.EXPECT().Access(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	status := s.fs.Access("/test", 0444, &fuse.Context{})
	s.Assert().Equal(fuse.EIO, status)
}

func (s *LocalFileSystemTestSuite) TestGetXAttr_Error() {
	s.fsClient.EXPECT().GetXAttr(mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	data, status := s.fs.GetXAttr("/test", "user.test", &fuse.Context{})
	s.Assert().Equal(fuse.EIO, status)
	s.Assert().Nil(data)
}

// TestGetAttr_RetriesOnUnavailable verifies that an idempotent metadata
// call survives a single transient Unavailable error via the retry wrapper.
func (s *LocalFileSystemTestSuite) TestGetAttr_RetriesOnUnavailable() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.Anything).
		Return(&proto.GetAttrReply{
			Status:     int32(fuse.OK),
			Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
		}, nil).Once()

	attr, st := s.fs.GetAttr("file", &fuse.Context{
		Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
		Cancel: make(chan struct{}),
	})

	s.Require().Equal(fuse.OK, st)
	s.NotNil(attr)
	s.fsClient.AssertNumberOfCalls(s.T(), "GetAttr", 2)
}

func TestLocalFileSystemTestSuite(t *testing.T) {
	suite.Run(t, new(LocalFileSystemTestSuite))
}
