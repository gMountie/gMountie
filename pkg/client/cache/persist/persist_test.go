package persist_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type PersistOpenSuite struct {
	suite.Suite
	dir string
}

func (s *PersistOpenSuite) SetupTest() {
	s.dir = s.T().TempDir()
}

func (s *PersistOpenSuite) TestOpenCreatesLayout() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	s.Require().FileExists(filepath.Join(s.dir, "LOCK"))
	s.Require().FileExists(filepath.Join(s.dir, "meta.db"))
	st, err := os.Stat(filepath.Join(s.dir, "chunks"))
	s.Require().NoError(err)
	s.Require().True(st.IsDir())
}

func (s *PersistOpenSuite) TestDualOpenFailsWithErrCacheLocked() {
	p1, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p1.Close()

	// Negative timeout = single attempt, no retry. Keeps this case
	// the fast-fail assertion it used to be even though Open now
	// defaults to a multi-second retry budget.
	_, err = persist.Open(persist.Options{Root: s.dir, LockAcquireTimeout: -1})
	s.Require().Error(err)
	s.Assert().ErrorIs(err, persist.ErrCacheLocked, "want ErrCacheLocked, got %v", err)
}

// TestOpenRetriesUntilPriorOwnerReleases proves the unmount-then-remount
// race is no longer a hard failure: a second Open with a non-zero
// timeout sits in the retry loop and succeeds as soon as the prior
// owner closes the bbolt handle and drops the flock.
func (s *PersistOpenSuite) TestOpenRetriesUntilPriorOwnerReleases() {
	p1, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)

	// Release the lock partway through the second Open's retry budget.
	released := make(chan struct{})
	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = p1.Close()
		close(released)
	}()

	start := time.Now()
	p2, err := persist.Open(persist.Options{Root: s.dir, LockAcquireTimeout: 2 * time.Second})
	elapsed := time.Since(start)
	s.Require().NoError(err)
	defer p2.Close()
	<-released
	s.Assert().GreaterOrEqual(elapsed, 100*time.Millisecond,
		"expected to have actually waited for the lock, got %s", elapsed)
	s.Assert().Less(elapsed, 2*time.Second,
		"expected to acquire well before the timeout, got %s", elapsed)
}

// TestOpenRetryGivesUpAfterTimeout makes sure the retry loop is bounded:
// when the prior owner never releases, Open eventually surfaces
// ErrCacheLocked within the configured budget.
func (s *PersistOpenSuite) TestOpenRetryGivesUpAfterTimeout() {
	p1, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p1.Close()

	start := time.Now()
	_, err = persist.Open(persist.Options{Root: s.dir, LockAcquireTimeout: 250 * time.Millisecond})
	elapsed := time.Since(start)
	s.Require().ErrorIs(err, persist.ErrCacheLocked)
	s.Assert().GreaterOrEqual(elapsed, 250*time.Millisecond)
	s.Assert().Less(elapsed, time.Second, "retry loop should respect the timeout, got %s", elapsed)
}

func (s *PersistOpenSuite) TestCloseReleasesLock() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.Require().NoError(p.Close())

	p2, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.Require().NoError(p2.Close())
}

func (s *PersistOpenSuite) TestFormatMismatchTriggersWipe() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	sentinel := filepath.Join(s.dir, "chunks", "sentinel")
	s.Require().NoError(os.WriteFile(sentinel, []byte("x"), 0o644))
	s.Require().NoError(p.Close())

	persist.TestingForceFormatVersion(s.T(), filepath.Join(s.dir, "meta.db"), 99)

	p2, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p2.Close()
	_, err = os.Stat(sentinel)
	s.Assert().True(os.IsNotExist(err), "wipe must have removed chunks/sentinel")
}

func TestPersistOpenSuite(t *testing.T) { suite.Run(t, new(PersistOpenSuite)) }
