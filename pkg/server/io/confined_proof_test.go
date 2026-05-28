package io

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ConfinedProofSuite struct{ suite.Suite }

func TestConfinedProofSuite(t *testing.T) { suite.Run(t, new(ConfinedProofSuite)) }

func (s *ConfinedProofSuite) openat2Beneath(rootFd int, name string, flags uint64) (int, error) {
	how := &unix.OpenHow{
		Flags:   flags,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
	}
	return unix.Openat2(rootFd, name, how)
}

func (s *ConfinedProofSuite) openRoot(p string) int {
	fd, err := unix.Open(p, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	s.Require().NoError(err)
	return fd
}

// TestInTreeFileResolves: a plain file inside the volume root resolves cleanly.
func (s *ConfinedProofSuite) TestInTreeFileResolves() {
	root := s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(root, "ok.txt"), []byte("hi"), 0o644))
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	fd, err := s.openat2Beneath(rootFd, "ok.txt", unix.O_RDONLY)
	s.Require().NoError(err)
	unix.Close(fd)
}

// TestAbsoluteSymlinkEscapeBlocked: a symlink that points to an absolute path
// outside the root must fail (EXDEV or ELOOP). This is the core escape attack.
func (s *ConfinedProofSuite) TestAbsoluteSymlinkEscapeBlocked() {
	root := s.T().TempDir()
	s.Require().NoError(os.Symlink("/etc/passwd", filepath.Join(root, "evil")))
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	_, err := s.openat2Beneath(rootFd, "evil", unix.O_RDONLY)
	s.Require().Error(err, "absolute symlink escape must fail")
	s.True(errors.Is(err, unix.EXDEV) || errors.Is(err, unix.ELOOP),
		"expected EXDEV or ELOOP, got %v", err)
}

// TestDotDotTraversalBlocked: ../ that exits the root must fail.
func (s *ConfinedProofSuite) TestDotDotTraversalBlocked() {
	root := s.T().TempDir()
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	_, err := s.openat2Beneath(rootFd, "../../etc/passwd", unix.O_RDONLY)
	s.Require().Error(err)
	s.True(errors.Is(err, unix.EXDEV), "expected EXDEV, got %v", err)
}

// TestRelativeSymlinkWithinTreeWorks: a relative symlink that stays in-tree
// must still resolve (we are not banning all symlinks).
func (s *ConfinedProofSuite) TestRelativeSymlinkWithinTreeWorks() {
	root := s.T().TempDir()
	s.Require().NoError(os.Mkdir(filepath.Join(root, "sub"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(root, "sub", "ok.txt"), []byte("hi"), 0o644))
	s.Require().NoError(os.Symlink("sub/ok.txt", filepath.Join(root, "link")))
	rootFd := s.openRoot(root)
	defer unix.Close(rootFd)

	fd, err := s.openat2Beneath(rootFd, "link", unix.O_RDONLY)
	s.Require().NoError(err)
	unix.Close(fd)
}
