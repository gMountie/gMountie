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

type ConfinedXAttrSuite struct {
	suite.Suite
	rootDir string
	fs      *ConfinedLoopbackFileSystem
}

func TestConfinedXAttrSuite(t *testing.T) { suite.Run(t, new(ConfinedXAttrSuite)) }

// setXAttrOrSkip probes whether the underlying filesystem supports user.*
// xattrs. If not, the entire suite is skipped — this is expected on some
// tmpfs configurations and NFS mounts.
func (s *ConfinedXAttrSuite) setXAttrOrSkip() {
	probe := filepath.Join(s.rootDir, ".xattr-probe")
	s.Require().NoError(os.WriteFile(probe, nil, 0o600))
	defer os.Remove(probe)
	if err := unix.Setxattr(probe, "user.probe", []byte("1"), 0); err != nil {
		s.T().Skipf("xattr not supported on this FS: %v", err)
	}
}

func (s *ConfinedXAttrSuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "ok.txt"), []byte("hello"), 0o644))

	s.setXAttrOrSkip()

	var err error
	s.fs, err = NewConfinedLoopbackFileSystem(s.rootDir)
	s.Require().NoError(err)
}

func (s *ConfinedXAttrSuite) TearDownTest() {
	if s.fs != nil {
		unix.Close(s.fs.rootFd)
	}
}

// TestSetThenGet: set a user.* xattr then read it back.
func (s *ConfinedXAttrSuite) TestSetThenGet() {
	st := s.fs.SetXAttr("ok.txt", "user.test", []byte("hello"), 0, nil)
	s.Equal(fuse.OK, st)

	data, st := s.fs.GetXAttr("ok.txt", "user.test", nil)
	s.Equal(fuse.OK, st)
	s.Equal([]byte("hello"), data)
}

// TestGetMissing: reading a non-existent xattr returns ENODATA.
func (s *ConfinedXAttrSuite) TestGetMissing() {
	_, st := s.fs.GetXAttr("ok.txt", "user.nonexistent", nil)
	s.Equal(fuse.Status(syscall.ENODATA), st)
}

// TestList: set two attrs and confirm ListXAttr returns both names.
func (s *ConfinedXAttrSuite) TestList() {
	s.Equal(fuse.OK, s.fs.SetXAttr("ok.txt", "user.a", []byte("1"), 0, nil))
	s.Equal(fuse.OK, s.fs.SetXAttr("ok.txt", "user.b", []byte("2"), 0, nil))

	attrs, st := s.fs.ListXAttr("ok.txt", nil)
	s.Equal(fuse.OK, st)
	s.Contains(attrs, "user.a")
	s.Contains(attrs, "user.b")
}

// TestRemove: set, remove, then get returns ENODATA.
func (s *ConfinedXAttrSuite) TestRemove() {
	s.Equal(fuse.OK, s.fs.SetXAttr("ok.txt", "user.test", []byte("hello"), 0, nil))
	s.Equal(fuse.OK, s.fs.RemoveXAttr("ok.txt", "user.test", nil))

	_, st := s.fs.GetXAttr("ok.txt", "user.test", nil)
	s.Equal(fuse.Status(syscall.ENODATA), st)
}

// TestGetEscape: path escape attempt returns EACCES.
func (s *ConfinedXAttrSuite) TestGetEscape() {
	_, st := s.fs.GetXAttr("../../etc/passwd", "user.test", nil)
	s.Equal(fuse.EACCES, st)
}

// TestSetEscape: path escape attempt returns EACCES.
func (s *ConfinedXAttrSuite) TestSetEscape() {
	st := s.fs.SetXAttr("../../etc/passwd", "user.test", []byte("x"), 0, nil)
	s.Equal(fuse.EACCES, st)
}

// TestListEscape: path escape attempt returns EACCES.
func (s *ConfinedXAttrSuite) TestListEscape() {
	_, st := s.fs.ListXAttr("../../etc/passwd", nil)
	s.Equal(fuse.EACCES, st)
}

// TestRemoveEscape: path escape attempt returns EACCES.
func (s *ConfinedXAttrSuite) TestRemoveEscape() {
	st := s.fs.RemoveXAttr("../../etc/passwd", "user.test", nil)
	s.Equal(fuse.EACCES, st)
}
