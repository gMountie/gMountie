//go:build linux

package commands

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type DaemonSuite struct{ suite.Suite }

func TestDaemonSuite(t *testing.T) { suite.Run(t, new(DaemonSuite)) }

func (s *DaemonSuite) TestStripsDaemonFlagAndKeepsRest() {
	args := buildDaemonChildArgs([]string{"mount", "admin@host/shared", "/mnt", "--daemon"})
	s.NotContains(args, "--daemon")
	s.Contains(args, "mount")
	s.Contains(args, "admin@host/shared")
	s.Contains(args, "/mnt")
}

func (s *DaemonSuite) TestStripsDaemonEqualsForm() {
	args := buildDaemonChildArgs([]string{"mount", "/mnt", "--daemon=true"})
	for _, a := range args {
		s.NotContains(a, "--daemon")
	}
}

func (s *DaemonSuite) TestParentWaitsForReadyViaSeam() {
	fake := &fakeDaemonizer{ready: true}
	err := daemonize(fake, []string{"gmountie", "mount", "/mnt", "--daemon"})
	s.Require().NoError(err)
	s.True(fake.spawned)
}

func (s *DaemonSuite) TestParentReportsChildFailure() {
	fake := &fakeDaemonizer{ready: false}
	err := daemonize(fake, []string{"gmountie", "mount", "/mnt", "--daemon"})
	s.Require().Error(err)
}

type fakeDaemonizer struct {
	ready   bool
	spawned bool
}

func (f *fakeDaemonizer) spawnAndAwaitReady(childArgs []string) error {
	f.spawned = true
	if !f.ready {
		return errReady
	}
	return nil
}

// The ready pipe now carries the child's real failure reason so the parent can
// report it instead of a generic timeout (issue #114). interpretReadyMsg is the
// pure parser the parent applies to whatever the child wrote.
func (s *DaemonSuite) TestReadyMsgSuccess() {
	s.Require().NoError(interpretReadyMsg(daemonReadyMsg, "/tmp/log"))
}

func (s *DaemonSuite) TestReadyMsgPropagatesChildError() {
	err := interpretReadyMsg(daemonErrPrefix+"open cache persist: cache directory is locked by another process", "/tmp/log")
	s.Require().Error(err)
	s.Contains(err.Error(), "cache directory is locked", "the real cause must reach the parent")
	s.Contains(err.Error(), "/tmp/log", "the log path is still pointed to")
	s.NotContains(err.Error(), "timed out", "a signalled error is not a timeout")
}

func (s *DaemonSuite) TestReadyMsgEmptyIsTimeout() {
	err := interpretReadyMsg("", "/tmp/log")
	s.Require().ErrorIs(err, errReady)
	s.Contains(err.Error(), "timed out or child exited")
}

// signalDaemonError is a no-op outside a daemon child (no readyFD), so it must
// not panic and must ignore a nil error.
func (s *DaemonSuite) TestSignalDaemonErrorNoopOutsideChild() {
	s.NotPanics(func() { signalDaemonError(nil) })
	s.NotPanics(func() { signalDaemonError(errReady) })
}
