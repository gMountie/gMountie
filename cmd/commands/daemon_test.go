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
