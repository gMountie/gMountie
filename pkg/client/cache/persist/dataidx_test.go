package persist_test

import (
	"os"
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

func (s *DataIdxSuite) TestInvalidateChunkRangeDecRefsAndUnlinks() {
	data := []byte("range-cleanup")
	hash, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	s.Require().NoError(s.p.PutChunkRef("path", 0, persist.ChunkRef{Hash: hash, Size: uint32(len(data))}))
	s.Require().NoError(s.p.PutChunkRef("path", 1, persist.ChunkRef{Hash: hash, Size: uint32(len(data))}))

	// Refcount = 2.
	count, err := s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(2), count)

	// Invalidate range [0, 0] — drops one of the two refs.
	s.Require().NoError(s.p.InvalidateChunkRange("path", 0, 0))
	_, ok, err := s.p.GetChunkRef("path", 0)
	s.Require().NoError(err)
	s.Assert().False(ok)
	count, err = s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(1), count)

	// Range [0, 1] would now only catch index 1 (0 already gone).
	s.Require().NoError(s.p.InvalidateChunkRange("path", 0, 1))
	count, err = s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(0), count)
}

func (s *DataIdxSuite) TestPutChunkRefUnlinksOverwrittenChunk() {
	d1 := []byte("first")
	d2 := []byte("second")
	h1, _, err := s.p.WriteChunk(d1)
	s.Require().NoError(err)
	h2, _, err := s.p.WriteChunk(d2)
	s.Require().NoError(err)

	s.Require().NoError(s.p.PutChunkRef("p", 0, persist.ChunkRef{Hash: h1, Size: uint32(len(d1))}))
	// Overwrite — h1's refcount should hit zero and the file should be unlinked.
	s.Require().NoError(s.p.PutChunkRef("p", 0, persist.ChunkRef{Hash: h2, Size: uint32(len(d2))}))

	// h1's chunk file must be gone.
	_, err = os.Stat(persist.TestingChunkPath(s.p, h1))
	s.Assert().True(os.IsNotExist(err), "overwritten chunk file must be unlinked")
}

// TestPutChunkRefSameHashIsNoOp regression-guards the memory-tier
// re-promotion path: store.get on a memory miss + disk hit calls
// store.put, which calls the disk putter (WriteChunk + PutChunkRef).
// WriteChunk dedupes (file already exists, same content); PutChunkRef
// must NOT then schedule a self-unlink of that file.
func (s *DataIdxSuite) TestPutChunkRefSameHashIsNoOp() {
	data := []byte("re-promoted chunk bytes")
	hash, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	ref := persist.ChunkRef{Hash: hash, Size: uint32(len(data))}
	s.Require().NoError(s.p.PutChunkRef("p", 0, ref))
	// Refcount = 1, chunk file exists.

	// Re-put with the SAME (path, idx, hash). Simulates memory-tier
	// eviction followed by disk-tier re-promotion of the same chunk.
	s.Require().NoError(s.p.PutChunkRef("p", 0, ref))

	// Chunk file must still exist (no self-unlink) and the refcount
	// must still be exactly 1 (no transient drop to 0).
	_, statErr := os.Stat(persist.TestingChunkPath(s.p, hash))
	s.Require().NoError(statErr, "same-hash re-put must not unlink the chunk file")
	count, err := s.p.ChunkRefCount(hash)
	s.Require().NoError(err)
	s.Assert().Equal(uint64(1), count, "same-hash re-put must leave refcount unchanged")

	// And the chunk must still be readable via the loader path.
	got, err := s.p.ReadChunk(hash)
	s.Require().NoError(err)
	s.Assert().Equal(data, got)
}

func TestDataIdxSuite(t *testing.T) { suite.Run(t, new(DataIdxSuite)) }
