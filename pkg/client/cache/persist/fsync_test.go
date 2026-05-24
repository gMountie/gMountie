package persist_test

import (
	"testing"

	"gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type PersistFsyncSuite struct {
	suite.Suite
	dir string
}

func (s *PersistFsyncSuite) SetupTest() { s.dir = s.T().TempDir() }

// A range invalidation over a path that was never cached must not open a
// writable transaction (which would commit + fsync for nothing).
func (s *PersistFsyncSuite) TestInvalidateChunkRangeNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidateChunkRange("/never/written", 0, 4))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "no-op range invalidation must not open a writable txn")
}

// A range invalidation that actually has an entry must still commit and
// remove it.
func (s *PersistFsyncSuite) TestInvalidateChunkRangeRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	hash, _, err := p.WriteChunk([]byte("hello"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/f", 0, persist.ChunkRef{Hash: hash, Size: 5}))

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidateChunkRange("/f", 0, 0))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "real invalidation must commit a writable txn")

	_, ok, err := p.GetChunkRef("/f", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "entry must be gone after invalidation")
}

func (s *PersistFsyncSuite) TestInvalidatePathChunksNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidatePathChunks("/never/written"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "no-op path invalidation must not open a writable txn")
}

func (s *PersistFsyncSuite) TestInvalidatePathChunksRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	hash, _, err := p.WriteChunk([]byte("hello"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/f", 0, persist.ChunkRef{Hash: hash, Size: 5}))

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidatePathChunks("/f"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "real path invalidation must commit a writable txn")

	_, ok, err := p.GetChunkRef("/f", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "entry must be gone after invalidation")
}

func (s *PersistFsyncSuite) TestDeleteAttrBytesNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/absent"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "deleting an absent attr key must not open a writable txn")
}

func (s *PersistFsyncSuite) TestDeleteAttrBytesRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	s.Require().NoError(p.PutAttrBytes("/a", []byte("x")))
	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/a"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "deleting a present key must commit a writable txn")

	_, ok, err := p.GetAttrBytes("/a")
	s.Require().NoError(err)
	s.Assert().False(ok, "key must be gone after delete")
}

func TestPersistFsyncSuite(t *testing.T) { suite.Run(t, new(PersistFsyncSuite)) }
