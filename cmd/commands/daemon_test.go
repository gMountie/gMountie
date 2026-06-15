//go:build linux

package commands

import (
	"os"
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
	err := daemonize(fake, []string{"gmountie", "mount", "/mnt", "--daemon"}, "secret")
	s.Require().NoError(err)
	s.True(fake.spawned)
}

func (s *DaemonSuite) TestParentReportsChildFailure() {
	fake := &fakeDaemonizer{ready: false}
	err := daemonize(fake, []string{"gmountie", "mount", "/mnt", "--daemon"}, "secret")
	s.Require().Error(err)
}

// TestDaemonizePassesPasswordToSeam proves the resolved password reaches the
// daemonizer seam (so it can be handed over the pipe) rather than the
// environment (CQ-L2).
func (s *DaemonSuite) TestDaemonizePassesPasswordToSeam() {
	fake := &fakeDaemonizer{ready: true}
	err := daemonize(fake, []string{"gmountie", "mount", "/mnt", "--daemon"}, "s3cr3t")
	s.Require().NoError(err)
	s.Equal("s3cr3t", fake.gotPassword)
}

type fakeDaemonizer struct {
	ready       bool
	spawned     bool
	gotPassword string
}

func (f *fakeDaemonizer) spawnAndAwaitReady(childArgs []string, password string) error {
	f.spawned = true
	f.gotPassword = password
	if !f.ready {
		return errReady
	}
	return nil
}

// TestReadDaemonPasswordRoundTrip proves the child-side reader recovers exactly
// what the parent wrote to the password pipe — the out-of-band hand-off that
// keeps the secret out of the child's environment (CQ-L2).
func (s *DaemonSuite) TestReadDaemonPasswordRoundTrip() {
	pr, pw, err := os.Pipe()
	s.Require().NoError(err)
	go func() {
		_, _ = pw.WriteString("hunter2")
		_ = pw.Close()
	}()
	s.Equal("hunter2", readDaemonPassword(pr))
}

// TestReadDaemonPasswordEmpty proves an EOF-only pipe (no secret sent, e.g. an
// mtls daemon mount) yields "" rather than blocking or erroring.
func (s *DaemonSuite) TestReadDaemonPasswordEmpty() {
	pr, pw, err := os.Pipe()
	s.Require().NoError(err)
	s.Require().NoError(pw.Close())
	s.Equal("", readDaemonPassword(pr))
}

// TestReadDaemonPasswordNilFile proves a missing fd (not a daemon child) is a
// safe no-op.
func (s *DaemonSuite) TestReadDaemonPasswordNilFile() {
	s.Equal("", readDaemonPassword(nil))
}

// TestApplyDaemonPasswordNoopOutsideChild proves the child-side apply is inert
// when not a daemon child (no fd 4, no env mutation, no panic).
func (s *DaemonSuite) TestApplyDaemonPasswordNoopOutsideChild() {
	s.T().Setenv(passwordEnvVar, "")
	s.NotPanics(applyDaemonPassword)
	s.Equal("", os.Getenv(passwordEnvVar))
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
