package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ConfinedStatSuite struct {
	suite.Suite
	rootDir string
	fs      *ConfinedLoopbackFileSystem
}

func TestConfinedStatSuite(t *testing.T) { suite.Run(t, new(ConfinedStatSuite)) }

func (s *ConfinedStatSuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.Mkdir(filepath.Join(s.rootDir, "sub"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "ok.txt"), []byte("hello"), 0o644))
	// in-tree relative symlink: link -> sub/ok.txt (sub doesn't need ok.txt for Readlink)
	s.Require().NoError(os.Symlink("sub/ok.txt", filepath.Join(s.rootDir, "link")))
	// escape symlink: evil -> /etc/passwd
	s.Require().NoError(os.Symlink("/etc/passwd", filepath.Join(s.rootDir, "evil")))

	var err error
	s.fs, err = NewConfinedLoopbackFileSystem(s.rootDir)
	s.Require().NoError(err)
}

func (s *ConfinedStatSuite) TearDownTest() {
	if s.fs != nil {
		unix.Close(s.fs.rootFd)
	}
}

// TestGetAttrInTree: stat a plain in-tree file returns OK + regular file mode.
func (s *ConfinedStatSuite) TestGetAttrInTree() {
	attr, st := s.fs.GetAttr("ok.txt", nil)
	s.Equal(fuse.OK, st)
	s.Require().NotNil(attr)
	s.True(attr.Mode&0o170000 == 0o100000, "expected regular file mode, got %o", attr.Mode)
}

// TestGetAttrEscape: traversal past root returns EACCES.
func (s *ConfinedStatSuite) TestGetAttrEscape() {
	_, st := s.fs.GetAttr("../../etc/passwd", nil)
	s.Equal(fuse.EACCES, st)
}

// TestReadlinkInTree: reading an in-tree relative symlink returns the link target string.
func (s *ConfinedStatSuite) TestReadlinkInTree() {
	target, st := s.fs.Readlink("link", nil)
	s.Equal(fuse.OK, st)
	s.Equal("sub/ok.txt", target)
}

// TestReadlinkEscapeBlocked: resolving the symlink NAME past root returns EACCES.
func (s *ConfinedStatSuite) TestReadlinkEscapeBlocked() {
	_, st := s.fs.Readlink("../../etc/passwd", nil)
	s.Equal(fuse.EACCES, st)
}

// TestAccessInTree: R_OK on a world-readable file returns OK.
func (s *ConfinedStatSuite) TestAccessInTree() {
	st := s.fs.Access("ok.txt", unix.R_OK, nil)
	s.Equal(fuse.OK, st)
}

// TestAccessEscape: traversal past root returns EACCES.
func (s *ConfinedStatSuite) TestAccessEscape() {
	st := s.fs.Access("../../etc/passwd", 0, nil)
	s.Equal(fuse.EACCES, st)
}

// TestStatFsRoot: StatFs on the root returns a non-nil result with non-zero Bsize.
func (s *ConfinedStatSuite) TestStatFsRoot() {
	out := s.fs.StatFs("")
	s.Require().NotNil(out)
	s.NotZero(out.Bsize)
}
