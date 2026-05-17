package persist_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type SweepSuite struct {
	suite.Suite
	dir string
}

func (s *SweepSuite) SetupTest() { s.dir = s.T().TempDir() }

func (s *SweepSuite) TestOrphanSweepRemovesUnreferencedChunks() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	// Drop a chunk file with no refcount entry.
	orphanHex := "aabb0000000000000000000000000000"
	shard := filepath.Join(s.dir, "chunks", "aa", "bb")
	s.Require().NoError(os.MkdirAll(shard, 0o755))
	orphan := filepath.Join(shard, orphanHex)
	s.Require().NoError(os.WriteFile(orphan, []byte("orphan"), 0o644))

	persist.TestingRunOrphanSweep(s.T(), p)
	_, err = os.Stat(orphan)
	s.Assert().True(os.IsNotExist(err), "orphan chunk must be removed")
	s.Require().NoError(p.Close())
}

func (s *SweepSuite) TestGhostSweepDropsIndexEntriesWithMissingChunks() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)

	var fake [16]byte
	fake[0] = 0x99
	s.Require().NoError(p.PutChunkRef("ghost/path", 0, persist.ChunkRef{Hash: fake, Size: 1}))

	persist.TestingRunGhostSweep(s.T(), p, 1.0)
	_, ok, err := p.GetChunkRef("ghost/path", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "ghost index entry must be dropped")
	s.Require().NoError(p.Close())
}

func (s *SweepSuite) TestDiskAccountantTracksChunkBytes() {
	p, err := persist.Open(persist.Options{Root: s.dir, DiskMaxBytes: 100})
	s.Require().NoError(err)
	_, _, err = p.WriteChunk(make([]byte, 30))
	s.Require().NoError(err)
	bytes := p.DiskBytesUsed()
	s.Assert().GreaterOrEqual(bytes, int64(30))
	time.Sleep(50 * time.Millisecond)
	s.Require().NoError(p.Close())
}

func (s *SweepSuite) TestWriteChunkEvictsUnderDiskCap() {
	p, err := persist.Open(persist.Options{Root: s.dir, DiskMaxBytes: 64})
	s.Require().NoError(err)
	defer p.Close()

	// Write 8 chunks of 16 bytes each = 128 bytes total against a 64-byte cap.
	// Each chunk is referenced once via PutChunkRef so it is eligible for eviction.
	for i := 0; i < 8; i++ {
		data := make([]byte, 16)
		data[0] = byte(i) // ensure distinct hashes so dedup does not short-circuit
		hash, _, err := p.WriteChunk(data)
		s.Require().NoError(err)
		s.Require().NoError(p.PutChunkRef("f", i, persist.ChunkRef{Hash: hash, Size: uint32(len(data))}))
	}

	// After all writes, on-disk total must be at or under the cap.
	// 16-byte chunks vs 64-byte cap means at most 4 chunks should remain.
	s.Assert().LessOrEqual(p.DiskBytesUsed(), int64(64), "disk cap must hold after eviction")
}

func TestSweepSuite(t *testing.T) { suite.Run(t, new(SweepSuite)) }
