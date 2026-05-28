package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type ResolveBeneathSuite struct {
	suite.Suite
	rootDir string
	rootFd  int
}

func TestResolveBeneathSuite(t *testing.T) { suite.Run(t, new(ResolveBeneathSuite)) }

func (s *ResolveBeneathSuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.Mkdir(filepath.Join(s.rootDir, "sub"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "sub", "f.txt"), []byte("hi"), 0o644))
	fd, err := unix.Open(s.rootDir, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	s.Require().NoError(err)
	s.rootFd = fd
}

func (s *ResolveBeneathSuite) TearDownTest() { unix.Close(s.rootFd) }

func (s *ResolveBeneathSuite) TestResolvesTopLevel() {
	parentFd, leaf, err := resolveBeneath(s.rootFd, "sub")
	s.Require().NoError(err)
	defer unix.Close(parentFd)
	s.Equal("sub", leaf)
}

func (s *ResolveBeneathSuite) TestResolvesNestedReturnsParent() {
	parentFd, leaf, err := resolveBeneath(s.rootFd, "sub/f.txt")
	s.Require().NoError(err)
	defer unix.Close(parentFd)
	s.Equal("f.txt", leaf)
}

func (s *ResolveBeneathSuite) TestRejectsDotDotEscape() {
	_, _, err := resolveBeneath(s.rootFd, "../../etc/passwd")
	s.Require().ErrorIs(err, unix.EXDEV)
}

func (s *ResolveBeneathSuite) TestEmptyNameMeansRoot() {
	parentFd, leaf, err := resolveBeneath(s.rootFd, "")
	s.Require().NoError(err)
	defer unix.Close(parentFd)
	s.Equal(".", leaf, "empty name addresses the root itself")
}
