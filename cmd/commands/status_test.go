//go:build linux || darwin

package commands

import (
	"bytes"
	"testing"
	"time"

	"github.com/adrg/xdg"
	"github.com/stretchr/testify/suite"
)

type StatusSuite struct {
	suite.Suite
	restoreAlive func(int) bool
}

func TestStatusSuite(t *testing.T) { suite.Run(t, new(StatusSuite)) }

func (s *StatusSuite) SetupTest() {
	s.T().Setenv("XDG_STATE_HOME", s.T().TempDir())
	xdg.Reload()
	s.restoreAlive = processAlive
	processAlive = func(int) bool { return true }
}

func (s *StatusSuite) TearDownTest() { processAlive = s.restoreAlive }

func (s *StatusSuite) TestRenderEmptyIsFriendly() {
	var buf bytes.Buffer
	renderMountStates(&buf, nil)
	s.Contains(buf.String(), "No active gMountie mounts")
}

func (s *StatusSuite) TestRenderShowsKeyFields() {
	var buf bytes.Buffer
	renderMountStates(&buf, []mountState{{
		Mountpoint: "/mnt/shared",
		Server:     "host:9449",
		Volume:     "shared",
		PID:        4242,
		StartedAt:  time.Now().Add(-90 * time.Second),
	}})
	out := buf.String()
	s.Contains(out, "/mnt/shared")
	s.Contains(out, "shared")
	s.Contains(out, "host:9449")
	s.Contains(out, "4242")
}

func (s *StatusSuite) TestFindMountStateByPath() {
	s.Require().NoError(writeMountState(mountState{Mountpoint: "/mnt/x", Volume: "v", PID: 7}))
	got, ok, err := findMountState("/mnt/x")
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Equal("v", got.Volume)
	s.Equal(7, got.PID)
}

func (s *StatusSuite) TestFindMountStateMissing() {
	_, ok, err := findMountState("/mnt/nope")
	s.Require().NoError(err)
	s.False(ok)
}
