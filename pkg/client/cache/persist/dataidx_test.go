package persist_test

import (
	"testing"

	"gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type DataIdxSuite struct {
	suite.Suite
	p   *persist.Persist
	dir string
}

func (s *DataIdxSuite) SetupTest() {
	s.dir = s.T().TempDir()
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.p = p
}
func (s *DataIdxSuite) TearDownTest() { _ = s.p.Close() }

func (s *DataIdxSuite) TestPutGetRoundTrip() {
	ref := persist.ChunkRef{Hash: [16]byte{1, 2, 3}, Size: 1024, Version: 7}
	s.Require().NoError(s.p.PutChunkRef("foo/bar", 3, ref))
	got, ok, err := s.p.GetChunkRef("foo/bar", 3)
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Assert().Equal(ref, got)
}

func (s *DataIdxSuite) TestGetMissingReturnsFalse() {
	_, ok, err := s.p.GetChunkRef("nope", 0)
	s.Require().NoError(err)
	s.Assert().False(ok)
}

func (s *DataIdxSuite) TestInvalidatePathChunksDropsAllForPath() {
	for i := 0; i < 5; i++ {
		s.Require().NoError(s.p.PutChunkRef("a/b", i, persist.ChunkRef{Size: 1}))
	}
	s.Require().NoError(s.p.PutChunkRef("a/c", 0, persist.ChunkRef{Size: 1}))
	s.Require().NoError(s.p.InvalidatePathChunks("a/b"))
	for i := 0; i < 5; i++ {
		_, ok, err := s.p.GetChunkRef("a/b", i)
		s.Require().NoError(err)
		s.Assert().False(ok, "a/b chunk %d must be gone", i)
	}
	_, ok, err := s.p.GetChunkRef("a/c", 0)
	s.Require().NoError(err)
	s.Assert().True(ok, "sibling path must not be invalidated")
}

func (s *DataIdxSuite) TestPutAndInvalidateUpdateRefcounts() {
	data := []byte("indexed-chunk")
	hash, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	ref := persist.ChunkRef{Hash: hash, Size: uint32(len(data))}
	s.Require().NoError(s.p.PutChunkRef("p", 0, ref))
	count, err := s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(1), count, "PutChunkRef must IncRef in the same txn")

	s.Require().NoError(s.p.InvalidatePathChunks("p"))
	count, err = s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(0), count, "InvalidatePathChunks must DecRef")
}

func TestDataIdxSuite(t *testing.T) { suite.Run(t, new(DataIdxSuite)) }
