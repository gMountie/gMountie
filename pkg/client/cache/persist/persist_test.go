package persist_test

import (
	"os"
	"path/filepath"
	"testing"

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

	_, err = persist.Open(persist.Options{Root: s.dir})
	s.Require().Error(err)
	s.Assert().ErrorIs(err, persist.ErrCacheLocked, "want ErrCacheLocked, got %v", err)
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
