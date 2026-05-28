package io

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ConfinedDirSuite struct {
	suite.Suite
	rootDir string
	fs      *ConfinedLoopbackFileSystem
}

func TestConfinedDirSuite(t *testing.T) { suite.Run(t, new(ConfinedDirSuite)) }

func (s *ConfinedDirSuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "ok.txt"), []byte("hi"), 0o644))
	s.Require().NoError(os.Mkdir(filepath.Join(s.rootDir, "to-remove"), 0o755))

	var err error
	s.fs, err = NewConfinedLoopbackFileSystem(s.rootDir)
	s.Require().NoError(err)
}

func (s *ConfinedDirSuite) TearDownTest() {
	if s.fs != nil {
		unix.Close(s.fs.rootFd)
	}
}

// TestMkdirInTree: creating a directory within the tree succeeds and is visible on disk.
func (s *ConfinedDirSuite) TestMkdirInTree() {
	st := s.fs.Mkdir("newdir", 0o755, nil)
	s.Equal(fuse.OK, st)
	info, err := os.Stat(filepath.Join(s.rootDir, "newdir"))
	s.Require().NoError(err)
	s.True(info.IsDir())
}

// TestMkdirEscape: traversal past root returns EACCES.
func (s *ConfinedDirSuite) TestMkdirEscape() {
	st := s.fs.Mkdir("../../tmp/evil", 0o755, nil)
	s.Equal(fuse.EACCES, st)
}

// TestMknodFifoInTree: creating a named pipe within the tree succeeds and has FIFO mode.
// FIFOs don't require root, unlike block/char devices.
func (s *ConfinedDirSuite) TestMknodFifoInTree() {
	st := s.fs.Mknod("fifo", syscall.S_IFIFO|0o644, 0, nil)
	s.Equal(fuse.OK, st)
	info, err := os.Stat(filepath.Join(s.rootDir, "fifo"))
	s.Require().NoError(err)
	s.NotZero(info.Mode()&os.ModeNamedPipe, "expected FIFO mode bit set")
}

// TestMknodEscape: traversal past root returns EACCES.
func (s *ConfinedDirSuite) TestMknodEscape() {
	st := s.fs.Mknod("../../tmp/x", syscall.S_IFIFO|0o644, 0, nil)
	s.Equal(fuse.EACCES, st)
}

// TestRmdirInTree: removing an in-tree directory succeeds and it disappears from disk.
func (s *ConfinedDirSuite) TestRmdirInTree() {
	st := s.fs.Rmdir("to-remove", nil)
	s.Equal(fuse.OK, st)
	_, err := os.Stat(filepath.Join(s.rootDir, "to-remove"))
	s.True(os.IsNotExist(err))
}

// TestRmdirEscape: traversal past root returns EACCES.
func (s *ConfinedDirSuite) TestRmdirEscape() {
	st := s.fs.Rmdir("../../tmp/evil", nil)
	s.Equal(fuse.EACCES, st)
}

// TestRmdirOnFileReturnsENOTDIR: Rmdir on a regular file returns ENOTDIR, confirming
// errnoToStatus passes plain errnos through unchanged.
func (s *ConfinedDirSuite) TestRmdirOnFileReturnsENOTDIR() {
	st := s.fs.Rmdir("ok.txt", nil)
	s.Equal(fuse.Status(syscall.ENOTDIR), st)
}

// TestUnlinkInTree: unlinking an in-tree file succeeds and the file is gone.
func (s *ConfinedDirSuite) TestUnlinkInTree() {
	st := s.fs.Unlink("ok.txt", nil)
	s.Equal(fuse.OK, st)
	_, err := os.Stat(filepath.Join(s.rootDir, "ok.txt"))
	s.True(os.IsNotExist(err))
}

// TestUnlinkEscape: traversal past root returns EACCES.
func (s *ConfinedDirSuite) TestUnlinkEscape() {
	st := s.fs.Unlink("../../etc/passwd", nil)
	s.Equal(fuse.EACCES, st)
}

// TestRenameInTree: renaming ok.txt → renamed.txt succeeds; old gone, new present.
func (s *ConfinedDirSuite) TestRenameInTree() {
	st := s.fs.Rename("ok.txt", "renamed.txt", nil)
	s.Equal(fuse.OK, st)
	_, err := os.Stat(filepath.Join(s.rootDir, "ok.txt"))
	s.True(os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(s.rootDir, "renamed.txt"))
	s.Require().NoError(err)
}

// TestRenameOldEscape: escape in the source path returns EACCES.
func (s *ConfinedDirSuite) TestRenameOldEscape() {
	st := s.fs.Rename("../../etc/passwd", "x", nil)
	s.Equal(fuse.EACCES, st)
}

// TestRenameNewEscape: escape in the destination path returns EACCES.
func (s *ConfinedDirSuite) TestRenameNewEscape() {
	st := s.fs.Rename("ok.txt", "../../tmp/evil", nil)
	s.Equal(fuse.EACCES, st)
}

// TestSymlinkInTree: creating an in-tree symlink succeeds; Readlink returns the target.
func (s *ConfinedDirSuite) TestSymlinkInTree() {
	st := s.fs.Symlink("ok.txt", "link", nil)
	s.Equal(fuse.OK, st)
	target, err := os.Readlink(filepath.Join(s.rootDir, "link"))
	s.Require().NoError(err)
	s.Equal("ok.txt", target)
}

// TestSymlinkAbsoluteTargetStored: an absolute target value is stored verbatim.
// Confinement is enforced at resolve time, not at creation time — this documents
// the design intentionally.
func (s *ConfinedDirSuite) TestSymlinkAbsoluteTargetStored() {
	st := s.fs.Symlink("/etc/passwd", "evil", nil)
	s.Equal(fuse.OK, st)
	target, err := os.Readlink(filepath.Join(s.rootDir, "evil"))
	s.Require().NoError(err)
	s.Equal("/etc/passwd", target)
}

// TestSymlinkLinkNameEscape: escape in the link name returns EACCES.
func (s *ConfinedDirSuite) TestSymlinkLinkNameEscape() {
	st := s.fs.Symlink("x", "../../tmp/evil", nil)
	s.Equal(fuse.EACCES, st)
}

// TestLinkInTree: hard-linking ok.txt → ok2.txt succeeds; both exist and share an inode.
func (s *ConfinedDirSuite) TestLinkInTree() {
	st := s.fs.Link("ok.txt", "ok2.txt", nil)
	s.Equal(fuse.OK, st)
	info1, err := os.Stat(filepath.Join(s.rootDir, "ok.txt"))
	s.Require().NoError(err)
	info2, err := os.Stat(filepath.Join(s.rootDir, "ok2.txt"))
	s.Require().NoError(err)
	ino1 := info1.Sys().(*syscall.Stat_t).Ino
	ino2 := info2.Sys().(*syscall.Stat_t).Ino
	s.Equal(ino1, ino2, "hard-linked files must share the same inode")
}

// TestLinkOldEscape: escape in the source path returns EACCES.
func (s *ConfinedDirSuite) TestLinkOldEscape() {
	st := s.fs.Link("../../etc/passwd", "x", nil)
	s.Equal(fuse.EACCES, st)
}

// TestLinkNewEscape: escape in the destination path returns EACCES.
func (s *ConfinedDirSuite) TestLinkNewEscape() {
	st := s.fs.Link("ok.txt", "../../tmp/evil", nil)
	s.Equal(fuse.EACCES, st)
}
