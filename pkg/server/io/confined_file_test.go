package io

import (
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// ConfinedFileSuite tests Open, Create, and OpenDir on ConfinedLoopbackFileSystem.
type ConfinedFileSuite struct {
	suite.Suite
	dir string
	fs  *ConfinedLoopbackFileSystem
}

func TestConfinedFileSuite(t *testing.T) {
	suite.Run(t, new(ConfinedFileSuite))
}

func (s *ConfinedFileSuite) SetupTest() {
	dir, err := os.MkdirTemp("", "confined-file-*")
	s.Require().NoError(err)
	s.dir = dir

	// r.txt = "hello"
	s.Require().NoError(os.WriteFile(dir+"/r.txt", []byte("hello"), 0o644))
	// sub/inner.txt = "nested"
	s.Require().NoError(os.Mkdir(dir+"/sub", 0o755))
	s.Require().NoError(os.WriteFile(dir+"/sub/inner.txt", []byte("nested"), 0o644))

	fs, err := NewConfinedLoopbackFileSystem(dir)
	s.Require().NoError(err)
	s.fs = fs
}

func (s *ConfinedFileSuite) TearDownTest() {
	if s.fs != nil {
		unix.Close(s.fs.rootFd)
	}
	if s.dir != "" {
		os.RemoveAll(s.dir)
	}
}

// TestOpenReadInTree opens r.txt and reads its contents via the nodefs.File interface.
func (s *ConfinedFileSuite) TestOpenReadInTree() {
	f, st := s.fs.Open("r.txt", uint32(os.O_RDONLY), nil)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(f)
	defer func() {
		f.Flush()
		f.Release()
	}()

	buf := make([]byte, 5)
	result, rst := f.Read(buf, 0)
	s.Require().Equal(fuse.OK, rst)
	data, bst := result.Bytes(buf)
	s.Require().Equal(fuse.OK, bst)
	s.Equal("hello", string(data))
}

// TestOpenEscape verifies that traversal attempts are denied.
func (s *ConfinedFileSuite) TestOpenEscape() {
	f, st := s.fs.Open("../../etc/passwd", uint32(os.O_RDONLY), nil)
	s.Nil(f)
	s.Equal(fuse.EACCES, st)
}

// TestCreateInTree creates a new file and verifies it exists on disk.
func (s *ConfinedFileSuite) TestCreateInTree() {
	f, st := s.fs.Create("new.txt", uint32(os.O_WRONLY), 0o644, nil)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(f)
	f.Flush()
	f.Release()

	info, err := os.Stat(s.dir + "/new.txt")
	s.Require().NoError(err)
	// Check that permission bits are roughly right (mask off umask effects —
	// just verify the file exists and is a regular file).
	s.True(info.Mode().IsRegular())
}

// TestCreateEscape verifies that escape paths are denied on Create.
func (s *ConfinedFileSuite) TestCreateEscape() {
	f, st := s.fs.Create("../../tmp/evil.txt", uint32(os.O_WRONLY), 0o644, nil)
	s.Nil(f)
	s.Equal(fuse.EACCES, st)
}

// TestOpenDirInTree lists the root and checks expected names and type bits.
func (s *ConfinedFileSuite) TestOpenDirInTree() {
	entries, st := s.fs.OpenDir("", nil)
	s.Require().Equal(fuse.OK, st)

	names := make(map[string]fuse.DirEntry, len(entries))
	for _, e := range entries {
		names[e.Name] = e
	}

	s.Contains(names, "r.txt")
	s.Contains(names, "sub")

	// r.txt must have S_IFREG type bit.
	s.Equal(uint32(syscall.S_IFREG), names["r.txt"].Mode&syscall.S_IFMT,
		"r.txt should be a regular file")
	// sub must have S_IFDIR type bit.
	s.Equal(uint32(syscall.S_IFDIR), names["sub"].Mode&syscall.S_IFMT,
		"sub should be a directory")
}

// TestOpenDirSubdir lists sub/ and finds inner.txt.
func (s *ConfinedFileSuite) TestOpenDirSubdir() {
	entries, st := s.fs.OpenDir("sub", nil)
	s.Require().Equal(fuse.OK, st)

	names := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		names[e.Name] = struct{}{}
	}
	s.Contains(names, "inner.txt")
}

// TestOpenDirEscape verifies that escape paths are denied on OpenDir.
func (s *ConfinedFileSuite) TestOpenDirEscape() {
	entries, st := s.fs.OpenDir("../../etc", nil)
	s.Nil(entries)
	s.Equal(fuse.EACCES, st)
}

// TestOpenDirNotDir opens a regular file as a directory; expect ENOTDIR.
func (s *ConfinedFileSuite) TestOpenDirNotDir() {
	entries, st := s.fs.OpenDir("r.txt", nil)
	s.Nil(entries)
	s.Equal(fuse.Status(syscall.ENOTDIR), st)
}
