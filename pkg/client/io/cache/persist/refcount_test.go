package persist_test

import (
	"os"
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/io/cache/persist"

	"github.com/stretchr/testify/suite"
)

type RefcountSuite struct {
	suite.Suite
	p   *persist.Persist
	dir string
}

func (s *RefcountSuite) SetupTest() {
	s.dir = s.T().TempDir()
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.p = p
}

func (s *RefcountSuite) TearDownTest() {
	if s.p != nil {
		_ = s.p.Close()
	}
}

func (s *RefcountSuite) TestIncDecLifecycle() {
	data := []byte("ref-test")
	hash, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)

	s.Require().NoError(s.p.IncChunkRef(hash))
	s.Require().NoError(s.p.IncChunkRef(hash))
	count, err := s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(2), count)

	remaining, err := s.p.DecChunkRef(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(1), remaining)
	// File still on disk.
	_, err = os.Stat(persist.TestingChunkPath(s.p, hash))
	s.Require().NoError(err)

	remaining, err = s.p.DecChunkRef(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(0), remaining)
	// File unlinked once refcount reaches zero.
	_, err = os.Stat(persist.TestingChunkPath(s.p, hash))
	s.Assert().True(os.IsNotExist(err), "chunk file must be removed when refcount hits 0")
}

func (s *RefcountSuite) TestDecBelowZeroStaysAtZero() {
	var h [16]byte
	remaining, err := s.p.DecChunkRef(h)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(0), remaining, "decrementing absent refcount is a no-op returning 0")
}

func (s *RefcountSuite) TestDoubleDecrementIsRecorded() {
	data := []byte("refcounted")
	hash, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	s.Require().NoError(s.p.IncChunkRef(hash)) // refcount = 1

	s.Require().Equal(int64(0), persist.TestingRefUnderflows(s.p))
	_, err = s.p.DecChunkRef(hash) // -> 0, deletes key (legitimate)
	s.Require().NoError(err)
	s.Assert().Equal(int64(0), persist.TestingRefUnderflows(s.p))

	_, err = s.p.DecChunkRef(hash) // decrement on an absent key = underflow
	s.Require().NoError(err)
	s.Assert().Equal(int64(1), persist.TestingRefUnderflows(s.p),
		"a decrement on an absent refcount must be recorded as an underflow")
}

func TestRefcountSuite(t *testing.T) { suite.Run(t, new(RefcountSuite)) }
