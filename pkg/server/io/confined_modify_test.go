package io

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ConfinedModifySuite struct {
	suite.Suite
	rootDir string
	fs      *ConfinedLoopbackFileSystem
}

func TestConfinedModifySuite(t *testing.T) { suite.Run(t, new(ConfinedModifySuite)) }

func (s *ConfinedModifySuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "ok.txt"), []byte("hi"), 0o644))

	var err error
	s.fs, err = NewConfinedLoopbackFileSystem(s.rootDir)
	s.Require().NoError(err)
}

func (s *ConfinedModifySuite) TearDownTest() {
	if s.fs != nil {
		unix.Close(s.fs.rootFd)
	}
}

// TestChmodInTree: chmod within tree sets mode.
func (s *ConfinedModifySuite) TestChmodInTree() {
	st := s.fs.Chmod("ok.txt", 0o600, nil)
	s.Equal(fuse.OK, st)
	info, err := os.Stat(filepath.Join(s.rootDir, "ok.txt"))
	s.Require().NoError(err)
	s.Equal(os.FileMode(0o600), info.Mode()&0o777)
}

// TestChmodEscape: traversal past root returns EACCES.
func (s *ConfinedModifySuite) TestChmodEscape() {
	st := s.fs.Chmod("../../etc/passwd", 0o600, nil)
	s.Equal(fuse.EACCES, st)
}

// TestChownInTreeSelfNoop: chown to self succeeds for non-root.
func (s *ConfinedModifySuite) TestChownInTreeSelfNoop() {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	st := s.fs.Chown("ok.txt", uid, gid, nil)
	s.Equal(fuse.OK, st)
}

// TestChownEscape: traversal past root returns EACCES.
func (s *ConfinedModifySuite) TestChownEscape() {
	st := s.fs.Chown("../../etc/passwd", 0, 0, nil)
	s.Equal(fuse.EACCES, st)
}

// TestUtimensInTree: setting explicit timestamps on in-tree file succeeds.
func (s *ConfinedModifySuite) TestUtimensInTree() {
	target := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	st := s.fs.Utimens("ok.txt", &target, &target, nil)
	s.Equal(fuse.OK, st)
	info, err := os.Stat(filepath.Join(s.rootDir, "ok.txt"))
	s.Require().NoError(err)
	s.True(info.ModTime().UTC().Equal(target), "mtime mismatch: got %v want %v", info.ModTime().UTC(), target)
}

// TestUtimensNilOmits: nil timestamps use UTIME_OMIT — no panic, returns OK.
func (s *ConfinedModifySuite) TestUtimensNilOmits() {
	st := s.fs.Utimens("ok.txt", nil, nil, nil)
	s.Equal(fuse.OK, st)
}

// TestUtimensEscape: traversal past root returns EACCES.
func (s *ConfinedModifySuite) TestUtimensEscape() {
	st := s.fs.Utimens("../../etc/passwd", nil, nil, nil)
	s.Equal(fuse.EACCES, st)
}

// TestTruncateGrow: truncating to a larger size zero-pads.
func (s *ConfinedModifySuite) TestTruncateGrow() {
	st := s.fs.Truncate("ok.txt", 10, nil)
	s.Equal(fuse.OK, st)
	data, err := os.ReadFile(filepath.Join(s.rootDir, "ok.txt"))
	s.Require().NoError(err)
	s.Equal(10, len(data))
	s.Equal([]byte("hi"), data[:2])
	s.Equal(make([]byte, 8), data[2:])
}

// TestTruncateShrink: truncating to a smaller size discards trailing bytes.
func (s *ConfinedModifySuite) TestTruncateShrink() {
	st := s.fs.Truncate("ok.txt", 1, nil)
	s.Equal(fuse.OK, st)
	data, err := os.ReadFile(filepath.Join(s.rootDir, "ok.txt"))
	s.Require().NoError(err)
	s.Equal("h", string(data))
}

// TestTruncateEscape: traversal past root returns EACCES.
func (s *ConfinedModifySuite) TestTruncateEscape() {
	st := s.fs.Truncate("../../etc/passwd", 0, nil)
	s.Equal(fuse.EACCES, st)
}
