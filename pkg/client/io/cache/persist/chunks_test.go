package persist_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/io/cache/persist"

	"github.com/stretchr/testify/suite"
)

type ChunkIOSuite struct {
	suite.Suite
	p   *persist.Persist
	dir string
}

func (s *ChunkIOSuite) SetupTest() {
	s.dir = s.T().TempDir()
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.p = p
}

func (s *ChunkIOSuite) TearDownTest() {
	if s.p != nil {
		_ = s.p.Close()
	}
}

func (s *ChunkIOSuite) TestWriteReadRoundTrip() {
	data := bytes.Repeat([]byte("xyz"), 1000)
	hash, dedup, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	s.Assert().False(dedup, "first write of a chunk is not a dedupe hit")

	got, err := s.p.ReadChunk(hash)
	s.Require().NoError(err)
	s.Assert().True(bytes.Equal(data, got), "round-trip bytes must match")
}

func (s *ChunkIOSuite) TestWriteIsContentAddressable() {
	data := []byte("hello world")
	h1, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	h2, dedup, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	s.Assert().Equal(h1, h2, "same bytes must hash to same address")
	s.Assert().True(dedup, "second write of identical bytes must report dedupe")
}

func (s *ChunkIOSuite) TestChunkPathIsSharded() {
	data := []byte("path-shard-test")
	hash, _, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	hex := persist.TestingHashHex(hash)
	want := filepath.Join(s.dir, "chunks", hex[:2], hex[2:4], hex)
	_, err = os.Stat(want)
	s.Require().NoError(err, "expected chunk at %s", want)
}

func (s *ChunkIOSuite) TestReadMissingReturnsErr() {
	var h [16]byte
	for i := range h {
		h[i] = 0xff
	}
	_, err := s.p.ReadChunk(h)
	s.Require().Error(err)
}

func (s *ChunkIOSuite) TestWriteChunkRepairsTornDedup() {
	data := []byte("durable chunk payload")
	hash, dedup, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	s.Require().False(dedup)

	// Simulate a torn/partial chunk left by a crash mid-write.
	path := persist.TestingChunkPath(s.p, hash)
	s.Require().NoError(os.Truncate(path, 4))

	// A re-write with the SAME bytes must repair (rewrite), not dedup-skip.
	gotHash, dedup, err := s.p.WriteChunk(data)
	s.Require().NoError(err)
	s.Require().Equal(hash, gotHash)
	s.Assert().False(dedup, "torn existing chunk must be rewritten, not treated as a dedup hit")

	got, err := s.p.ReadChunk(hash)
	s.Require().NoError(err)
	s.Assert().Equal(data, got, "chunk must be fully repaired")
}

func TestChunkIOSuite(t *testing.T) { suite.Run(t, new(ChunkIOSuite)) }
