//go:build linux || darwin

package commands

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adrg/xdg"
	"github.com/stretchr/testify/suite"
)

type UnmountSuite struct {
	suite.Suite
	restoreAlive   func(int) bool
	restoreSignal  func(int) error
	restoreFuse    func(string) error
	restoreTimeout time.Duration
	restorePoll    time.Duration

	signalled  []int
	fuseCalled []string
}

func TestUnmountSuite(t *testing.T) { suite.Run(t, new(UnmountSuite)) }

func (s *UnmountSuite) SetupTest() {
	s.T().Setenv("XDG_STATE_HOME", s.T().TempDir())
	xdg.Reload()
	s.restoreAlive, s.restoreSignal, s.restoreFuse = processAlive, signalMount, fuseUnmount
	s.restoreTimeout, s.restorePoll = unmountWaitTimeout, unmountPollInterval
	unmountWaitTimeout, unmountPollInterval = 250*time.Millisecond, 5*time.Millisecond
	s.signalled, s.fuseCalled = nil, nil
	processAlive = func(int) bool { return true }
	// The default signal stub also flips processAlive to "gone", modelling a
	// mount process that exits promptly after SIGTERM so waitMountExit returns.
	signalMount = func(pid int) error {
		s.signalled = append(s.signalled, pid)
		processAlive = func(int) bool { return false }
		return nil
	}
	fuseUnmount = func(path string) error { s.fuseCalled = append(s.fuseCalled, path); return nil }
}

func (s *UnmountSuite) TearDownTest() {
	processAlive, signalMount, fuseUnmount = s.restoreAlive, s.restoreSignal, s.restoreFuse
	unmountWaitTimeout, unmountPollInterval = s.restoreTimeout, s.restorePoll
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

// Success is only reported once the signalled process actually exits: the
// mount process tears the FUSE mount down before exiting, so process-gone is
// the "mount detached" signal. Here the process needs a few polls to die.
func (s *UnmountSuite) TestManagedMountWaitsForProcessExit() {
	var polls atomic.Int32
	signalMount = func(pid int) error {
		s.signalled = append(s.signalled, pid)
		processAlive = func(int) bool { return polls.Add(1) < 4 } // alive for 3 polls
		return nil
	}
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/slow", PID: 7}))

	var buf bytes.Buffer
	s.Require().NoError(unmountTarget(&buf, "/mnt/slow"))
	s.GreaterOrEqual(polls.Load(), int32(4), "unmount must poll until the process is gone")
	s.Contains(buf.String(), "Unmounted")
}

// A process that never exits within the wait budget is an error, not a false
// "Unmounted" success; the state record is kept so a later retry still finds it.
func (s *UnmountSuite) TestManagedMountTimesOutWhenProcessHangs() {
	signalMount = func(pid int) error { s.signalled = append(s.signalled, pid); return nil } // stays alive
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/stuck", PID: 8}))

	var buf bytes.Buffer
	err := unmountTarget(&buf, "/mnt/stuck")
	s.Require().Error(err)
	s.Contains(err.Error(), "still running")
	s.NotContains(buf.String(), "Unmounted")

	_, ok, ferr := findMountState("/mnt/stuck")
	s.Require().NoError(ferr)
	s.True(ok, "state record must survive a failed unmount")
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
