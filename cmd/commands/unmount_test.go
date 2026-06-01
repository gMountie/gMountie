//go:build linux || darwin

package commands

import (
	"bytes"
	"errors"
	"testing"

	"github.com/adrg/xdg"
	"github.com/stretchr/testify/suite"
)

type UnmountSuite struct {
	suite.Suite
	restoreAlive  func(int) bool
	restoreSignal func(int) error
	restoreFuse   func(string) error

	signalled  []int
	fuseCalled []string
}

func TestUnmountSuite(t *testing.T) { suite.Run(t, new(UnmountSuite)) }

func (s *UnmountSuite) SetupTest() {
	s.T().Setenv("XDG_STATE_HOME", s.T().TempDir())
	xdg.Reload()
	s.restoreAlive, s.restoreSignal, s.restoreFuse = processAlive, signalMount, fuseUnmount
	s.signalled, s.fuseCalled = nil, nil
	processAlive = func(int) bool { return true }
	signalMount = func(pid int) error { s.signalled = append(s.signalled, pid); return nil }
	fuseUnmount = func(path string) error { s.fuseCalled = append(s.fuseCalled, path); return nil }
}

func (s *UnmountSuite) TearDownTest() {
	processAlive, signalMount, fuseUnmount = s.restoreAlive, s.restoreSignal, s.restoreFuse
}

// A gMountie-managed, live mount is stopped by signalling its process, not by
// shelling out to fusermount.
func (s *UnmountSuite) TestManagedMountIsSignalled() {
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/managed", Volume: "v", PID: 4242}))
	var buf bytes.Buffer
	s.Require().NoError(unmountTarget(&buf, "/mnt/managed"))

	s.Equal([]int{4242}, s.signalled)
	s.Empty(s.fuseCalled)

	_, ok, err := findMountState("/mnt/managed")
	s.Require().NoError(err)
	s.False(ok, "state should be cleared after unmount")
}

// A path with no gMountie state (mounted some other way, or state lost) falls
// back to FUSE unmount tooling.
func (s *UnmountSuite) TestUnmanagedMountFallsBackToFuse() {
	var buf bytes.Buffer
	s.Require().NoError(unmountTarget(&buf, "/mnt/external"))

	s.Empty(s.signalled)
	s.Require().Len(s.fuseCalled, 1)
}

// A recorded mount whose process is already dead also falls back to FUSE
// unmount (the kernel mount may still be present even if the process is gone).
func (s *UnmountSuite) TestDeadProcessFallsBackToFuse() {
	processAlive = func(int) bool { return false }
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/dead", PID: 999}))

	var buf bytes.Buffer
	s.Require().NoError(unmountTarget(&buf, "/mnt/dead"))
	s.Empty(s.signalled)
	s.Require().Len(s.fuseCalled, 1)
}

// A signalling failure surfaces as an error.
func (s *UnmountSuite) TestSignalErrorSurfaces() {
	signalMount = func(int) error { return errors.New("no such process") }
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/x", PID: 5}))

	var buf bytes.Buffer
	err := unmountTarget(&buf, "/mnt/x")
	s.Require().Error(err)
	s.Contains(err.Error(), "no such process")
}
