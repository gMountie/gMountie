package cache

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
)

type DataCacheTestSuite struct {
	suite.Suite
	c *dataCache
}

func (s *DataCacheTestSuite) SetupTest() {
	s.c = newDataCache(newAccountant(0, 0), 1024) // 1 KiB chunks for arithmetic clarity
}

func (s *DataCacheTestSuite) TestMissAndPut() {
	s.Assert().Nil(s.c.get("/f", 0))
	s.c.put("/f", 0, []byte("abcd"))
	s.Assert().Equal([]byte("abcd"), s.c.get("/f", 0))
}

func (s *DataCacheTestSuite) TestDifferentPathsDisjoint() {
	s.c.put("/a", 0, []byte("1111"))
	s.c.put("/b", 0, []byte("2222"))
	s.Assert().Equal([]byte("1111"), s.c.get("/a", 0))
	s.Assert().Equal([]byte("2222"), s.c.get("/b", 0))
}

func (s *DataCacheTestSuite) TestInvalidatePathDropsAllChunks() {
	s.c.put("/f", 0, []byte("c0"))
	s.c.put("/f", 1, []byte("c1"))
	s.c.put("/f", 5, []byte("c5"))
	s.c.put("/other", 0, []byte("xx"))
	s.c.invalidatePath("/f")
	s.Assert().Nil(s.c.get("/f", 0))
	s.Assert().Nil(s.c.get("/f", 1))
	s.Assert().Nil(s.c.get("/f", 5))
	s.Assert().Equal([]byte("xx"), s.c.get("/other", 0))
}

func (s *DataCacheTestSuite) TestInvalidateRange() {
	// chunks: 0 = [0, 1023], 1 = [1024, 2047], 2 = [2048, 3071]
	s.c.put("/f", 0, make([]byte, 1024))
	s.c.put("/f", 1, make([]byte, 1024))
	s.c.put("/f", 2, make([]byte, 1024))
	// Write to bytes [500, 1500) touches chunks 0 and 1
	s.c.invalidateRange("/f", 500, 1000)
	s.Assert().Nil(s.c.get("/f", 0))
	s.Assert().Nil(s.c.get("/f", 1))
	s.Assert().NotNil(s.c.get("/f", 2))
}

func (s *DataCacheTestSuite) TestInvalidateRangeZeroSize() {
	s.c.put("/f", 0, make([]byte, 1024))
	s.c.invalidateRange("/f", 0, 0) // no-op
	s.Assert().NotNil(s.c.get("/f", 0))
}

func (s *DataCacheTestSuite) TestInvalidatePathDoesNotMatchPrefixOfOtherPath() {
	// /foo and /foobar both have a chunk; invalidatePath("/foo") must
	// only drop /foo's, not /foobar's. The "\x00" separator after
	// the path guarantees this.
	s.c.put("/foo", 0, []byte("a"))
	s.c.put("/foobar", 0, []byte("b"))
	s.c.invalidatePath("/foo")
	s.Assert().Nil(s.c.get("/foo", 0))
	s.Assert().NotNil(s.c.get("/foobar", 0))
}

func TestDataCacheTestSuite(t *testing.T) {
	suite.Run(t, new(DataCacheTestSuite))
}

// stalePersist is a fake chunkPersist that always serves a single stale chunk
// for (path,0) and fails invalidation. It lets us exercise the data cache's
// cleaner-error → poison-path wiring deterministically: a real *persist.Persist
// cannot both error on invalidation AND keep serving the stale ref (the index
// delete commits before the failable file op), so the persist is faked.
type stalePersist struct {
	stale         []byte
	invalidateErr error
}

func (s *stalePersist) GetChunkRef(_ string, idx int) (persist.ChunkRef, bool, error) {
	if idx != 0 {
		return persist.ChunkRef{}, false, nil
	}
	return persist.ChunkRef{Size: uint32(len(s.stale))}, true, nil
}
func (s *stalePersist) ReadChunk(_ [16]byte) ([]byte, error)                  { return s.stale, nil }
func (s *stalePersist) WriteChunk(_ []byte) ([16]byte, bool, error)           { return [16]byte{}, false, nil }
func (s *stalePersist) PutChunkRef(_ string, _ int, _ persist.ChunkRef) error { return nil }
func (s *stalePersist) InvalidatePathChunks(_ string) error                   { return s.invalidateErr }
func (s *stalePersist) InvalidateChunkRange(_ string, _, _ int) error         { return s.invalidateErr }

// TestInvalidatePathPoisonsOnPersistError proves that when the persist
// invalidation fails, the loader refuses to serve the stale disk chunk for that
// path (the #113-family serve-stale bug). After a failed invalidatePath, a get
// must miss rather than return the pre-write disk bytes.
func (s *DataCacheTestSuite) TestInvalidatePathPoisonsOnPersistError() {
	fake := &stalePersist{stale: []byte("stale-disk-bytes"), invalidateErr: errors.New("boom")}
	c := newDataCacheWithChunkPersist(newAccountant(1<<20, 0), 1024, fake)

	// Prime: the disk tier holds the stale chunk; a get loads it into memory.
	s.Require().Equal([]byte("stale-disk-bytes"), c.get("/f", 0), "loader serves disk chunk before invalidation")

	// A write invalidates the path, but persist invalidation fails.
	c.invalidatePath("/f")

	// The memory tier was cleared and the path is poisoned, so the loader must
	// NOT re-serve the stale disk chunk.
	s.Assert().Nil(c.get("/f", 0), "poisoned path must not serve stale disk chunk after failed invalidation")
}

// TestInvalidateRangePoisonsOnPersistError is the same guard for the range
// cleaner.
func (s *DataCacheTestSuite) TestInvalidateRangePoisonsOnPersistError() {
	fake := &stalePersist{stale: []byte("stale-disk-bytes"), invalidateErr: errors.New("boom")}
	c := newDataCacheWithChunkPersist(newAccountant(1<<20, 0), 1024, fake)

	s.Require().Equal([]byte("stale-disk-bytes"), c.get("/f", 0))
	c.invalidateRange("/f", 0, 1) // touches chunk 0
	s.Assert().Nil(c.get("/f", 0), "poisoned path must not serve stale disk chunk after failed range invalidation")
}
