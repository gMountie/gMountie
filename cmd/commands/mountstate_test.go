//go:build linux || darwin

package commands

import (
	"testing"

	"github.com/adrg/xdg"
	"github.com/stretchr/testify/suite"
)

type MountStateSuite struct {
	suite.Suite
	restoreAlive func(int) bool
}

func TestMountStateSuite(t *testing.T) { suite.Run(t, new(MountStateSuite)) }

func (s *MountStateSuite) SetupTest() {
	s.T().Setenv("XDG_STATE_HOME", s.T().TempDir())
	xdg.Reload()
	s.restoreAlive = processAlive
	processAlive = func(int) bool { return true } // default: everything alive
}

func (s *MountStateSuite) TearDownTest() {
	processAlive = s.restoreAlive
}

func (s *MountStateSuite) TestWriteListRemoveRoundTrip() {
	a := mountState{Mountpoint: "/mnt/a", Server: "h:9449", Volume: "shared", PID: 111}
	b := mountState{Mountpoint: "/mnt/b", Server: "h:9449", Volume: "docs", PID: 222}
	s.Require().NoError(writeMountState(a))
	s.Require().NoError(writeMountState(b))

	got, err := listMountStates()
	s.Require().NoError(err)
	s.Len(got, 2)

	s.Require().NoError(removeMountState("/mnt/a"))
	got, err = listMountStates()
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("/mnt/b", got[0].Mountpoint)
	s.Equal("docs", got[0].Volume)
}

func (s *MountStateSuite) TestListPrunesDeadProcesses() {
	processAlive = func(pid int) bool { return pid == 111 }
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/alive", PID: 111}))
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/dead", PID: 999}))

	got, err := listMountStates()
	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("/mnt/alive", got[0].Mountpoint)

	// The dead entry's file must be pruned from disk, not just filtered.
	processAlive = func(int) bool { return true }
	got, err = listMountStates()
	s.Require().NoError(err)
	s.Len(got, 1)
}

func (s *MountStateSuite) TestRemoveMissingIsNoError() {
	s.NoError(removeMountState("/mnt/never-mounted"))
}

func (s *MountStateSuite) TestListEmptyWhenNothingMounted() {
	got, err := listMountStates()
	s.Require().NoError(err)
	s.Empty(got)
}
