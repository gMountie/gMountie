//go:build linux || darwin

package commands

import (
	"bytes"
	"sync/atomic"
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

// mountStatusOf classifies session health for the STATUS column (issue #112):
// a process can be alive while its session is locked out (revoked cert), which
// used to read as "active".
func (s *StatusSuite) TestMountStatusOf() {
	now := time.Now()

	// Pre-heartbeat record (zero HeartbeatAt) → active/unknown, never guessed degraded.
	s.Equal(mountStatusActive, mountStatusOf(mountState{}, now))

	// Fresh heartbeat, healthy → active.
	s.Equal(mountStatusActive, mountStatusOf(
		mountState{Healthy: true, HeartbeatAt: now.Add(-mountHeartbeatInterval)}, now))

	// Fresh heartbeat, not healthy (zombie: process alive, session locked out) → degraded.
	s.Equal(mountStatusDegraded, mountStatusOf(
		mountState{Healthy: false, HeartbeatAt: now.Add(-mountHeartbeatInterval)}, now))

	// Heartbeat stopped updating (daemon wedged) → stale, regardless of last health.
	s.Equal(mountStatusStale, mountStatusOf(
		mountState{Healthy: true, HeartbeatAt: now.Add(-2 * mountHeartbeatStaleWindow())}, now))
}

// The zombie mount is the headline #112 case: a revoked/locked-out session
// must not render as "active".
func (s *StatusSuite) TestRenderShowsDegradedForZombie() {
	var buf bytes.Buffer
	renderMountStates(&buf, []mountState{{
		Mountpoint:  "/mnt/zombie",
		Volume:      "v",
		PID:         4242,
		StartedAt:   time.Now().Add(-3 * time.Minute),
		Healthy:     false,
		HeartbeatAt: time.Now(),
	}})
	out := buf.String()
	s.Contains(out, "STATUS")
	s.Contains(out, mountStatusDegraded)
	s.NotContains(out, mountStatusActive)
}

// runMountHeartbeat refreshes the record with live health and stops cleanly,
// leaving no further writes after the stop signal (the resurrection guard).
func (s *StatusSuite) TestHeartbeatWritesHealthAndStops() {
	saved := mountHeartbeatInterval
	mountHeartbeatInterval = 10 * time.Millisecond
	defer func() { mountHeartbeatInterval = saved }()

	// atomic: the heartbeat goroutine reads health while the test flips it.
	var healthy atomic.Bool
	healthy.Store(true)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runMountHeartbeat(mountState{Mountpoint: "/mnt/hb", Volume: "v", PID: 7}, healthy.Load, stop)
	}()

	// Let a few beats land, then flip health and confirm it propagates.
	time.Sleep(50 * time.Millisecond)
	healthy.Store(false)
	time.Sleep(50 * time.Millisecond)
	close(stop)
	<-done

	st, ok, err := findMountState("/mnt/hb")
	s.Require().NoError(err)
	s.Require().True(ok)
	s.False(st.Healthy, "heartbeat must reflect the latest session health")
	s.False(st.HeartbeatAt.IsZero(), "heartbeat must stamp the record")
}
