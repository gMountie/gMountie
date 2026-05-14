package middleware

import (
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	pathfs2 "gmountie/internal/mocks/github.com/hanwen/go-fuse/v2/fuse/pathfs"
)

// Mock syscall functions
type mockSetFs struct {
	mock.Mock
}

// Setfsuid is a mock function for syscall.Setfsuid
func (m *mockSetFs) Setfsuid(uid int) error {
	args := m.Called(uid)
	return args.Error(0)
}

// Setfsgid is a mock function for syscall.Setfsgid
func (m *mockSetFs) Setfsgid(gid int) error {
	args := m.Called(gid)
	return args.Error(0)
}

type AssumeUserMiddlewareTestSuite struct {
	suite.Suite
	fs         *pathfs2.MockFileSystem
	setfs      *mockSetFs
	middleware pathfs.FileSystem
}

func (s *AssumeUserMiddlewareTestSuite) SetupTest() {
	s.fs = new(pathfs2.MockFileSystem)
	s.setfs = new(mockSetFs)
	setfsgid = s.setfs.Setfsgid
	setfsuid = s.setfs.Setfsuid
	s.middleware = AssumeUserMiddleware(s.fs)
}

func (s *AssumeUserMiddlewareTestSuite) TearDownTest() {
	// Restore original syscall functions
	setfsuid = syscall.Setfsuid
	setfsgid = syscall.Setfsgid
}

func (s *AssumeUserMiddlewareTestSuite) TestGetAttr() {
	// Setup
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	expectedAttr := &fuse.Attr{}
	s.fs.EXPECT().GetAttr("testfile", ctx).Return(expectedAttr, fuse.OK)
	s.setUpChangeUserMocks()

	// Test
	attr, status := s.middleware.GetAttr("testfile", ctx)

	// Verify
	s.Equal(fuse.OK, status)
	s.Equal(expectedAttr, attr)
	s.fs.AssertExpectations(s.T())
	s.setfs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestChmod() {
	// Setup
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	s.fs.EXPECT().Chmod("testfile", uint32(0644), ctx).Return(fuse.OK)
	s.setUpChangeUserMocks()

	// Test
	status := s.middleware.Chmod("testfile", 0644, ctx)

	// Verify
	s.Equal(fuse.OK, status)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestChown() {
	// Setup
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	s.fs.EXPECT().Chown("testfile", uint32(1001), uint32(1001), ctx).Return(fuse.OK)
	s.setUpChangeUserMocks()

	// Test
	status := s.middleware.Chown("testfile", 1001, 1001, ctx)

	// Verify
	s.Equal(fuse.OK, status)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestUtimens() {
	// Setup
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	now := time.Now()
	s.fs.EXPECT().Utimens("testfile", &now, &now, ctx).Return(fuse.OK)
	s.setUpChangeUserMocks()

	// Test
	status := s.middleware.Utimens("testfile", &now, &now, ctx)

	// Verify
	s.Equal(fuse.OK, status)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestOpen() {
	// Setup
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	expectedFile := nodefs.NewDefaultFile()
	s.fs.EXPECT().Open("testfile", uint32(0), ctx).Return(expectedFile, fuse.OK)
	s.setUpChangeUserMocks()

	// Test
	file, status := s.middleware.Open("testfile", 0, ctx)

	// Verify
	s.Equal(fuse.OK, status)
	s.Equal(expectedFile, file)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestCreate() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	expectedFile := nodefs.NewDefaultFile()
	s.fs.EXPECT().Create("testfile", uint32(0644), uint32(0644), ctx).Return(expectedFile, fuse.OK)
	s.setUpChangeUserMocks()

	file, status := s.middleware.Create("testfile", 0644, 0644, ctx)

	s.Equal(fuse.OK, status)
	s.Equal(expectedFile, file)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestOpenDir() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	expectedEntries := []fuse.DirEntry{{Name: "test", Mode: 0644}}
	s.fs.EXPECT().OpenDir("testdir", ctx).Return(expectedEntries, fuse.OK)
	s.setUpChangeUserMocks()

	entries, status := s.middleware.OpenDir("testdir", ctx)

	s.Equal(fuse.OK, status)
	s.Equal(expectedEntries, entries)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestSymlink() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	s.fs.EXPECT().Symlink("target", "link", ctx).Return(fuse.OK)
	s.setUpChangeUserMocks()

	status := s.middleware.Symlink("target", "link", ctx)

	s.Equal(fuse.OK, status)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestReadlink() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}
	s.fs.EXPECT().Readlink("link", ctx).Return("target", fuse.OK)
	s.setUpChangeUserMocks()

	target, status := s.middleware.Readlink("link", ctx)

	s.Equal(fuse.OK, status)
	s.Equal("target", target)
	s.fs.AssertExpectations(s.T())
}

func (s *AssumeUserMiddlewareTestSuite) TestXAttrOperations() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{
			Owner: fuse.Owner{
				Uid: 1000,
				Gid: 1000,
			},
		},
	}

	// Test GetXAttr
	expectedData := []byte("xattr-data")
	s.fs.EXPECT().GetXAttr("testfile", "user.test", ctx).Return(expectedData, fuse.OK)
	s.setUpChangeUserMocks()

	data, status := s.middleware.GetXAttr("testfile", "user.test", ctx)
	s.Equal(fuse.OK, status)
	s.Equal(expectedData, data)

	// Test ListXAttr
	expectedAttrs := []string{"user.test"}
	s.fs.EXPECT().ListXAttr("testfile", ctx).Return(expectedAttrs, fuse.OK)

	attrs, status := s.middleware.ListXAttr("testfile", ctx)
	s.Equal(fuse.OK, status)
	s.Equal(expectedAttrs, attrs)

	// Test SetXAttr
	s.fs.EXPECT().SetXAttr("testfile", "user.test", []byte("new-data"), 0, ctx).Return(fuse.OK)

	status = s.middleware.SetXAttr("testfile", "user.test", []byte("new-data"), 0, ctx)
	s.Equal(fuse.OK, status)

	// Test RemoveXAttr
	s.fs.EXPECT().RemoveXAttr("testfile", "user.test", ctx).Return(fuse.OK)

	status = s.middleware.RemoveXAttr("testfile", "user.test", ctx)
	s.Equal(fuse.OK, status)

	s.fs.AssertExpectations(s.T())
}

// TestGetAttr_SetfsuidFailureReturnsEPERM verifies that when setfsuid fails,
// the wrapper returns fuse.EPERM rather than killing the process.
func (s *AssumeUserMiddlewareTestSuite) TestGetAttr_SetfsuidFailureReturnsEPERM() {
	ctx := &fuse.Context{
		Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}},
	}
	// setfsuid returns an error; the wrapper must NOT invoke setfsgid or the
	// underlying FileSystem, and must return EPERM.
	s.setfs.On("Setfsuid", 1000).Return(syscall.EPERM).Once()

	attr, status := s.middleware.GetAttr("testfile", ctx)

	s.Nil(attr)
	s.Equal(fuse.EPERM, status)
	s.setfs.AssertExpectations(s.T())
	// fs should NOT have been called.
	s.fs.AssertNotCalled(s.T(), "GetAttr", mock.Anything, mock.Anything)
}

func (s *AssumeUserMiddlewareTestSuite) setUpChangeUserMocks() {
	s.setfs.On("Setfsuid", 1000).Return(nil)
	s.setfs.On("Setfsuid", syscall.Geteuid()).Return(nil)
	s.setfs.On("Setfsgid", 1000).Return(nil)
	s.setfs.On("Setfsgid", syscall.Getegid()).Return(nil)
}

func TestAssumeUserMiddlewareTestSuite(t *testing.T) {
	suite.Run(t, new(AssumeUserMiddlewareTestSuite))
}
