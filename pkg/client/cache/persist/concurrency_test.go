package persist_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
)

type ConcurrencySuite struct {
	suite.Suite
	dir string
	p   *persist.Persist
}

func (s *ConcurrencySuite) SetupTest() {
	s.dir = s.T().TempDir()
	p, err := persist.Open(persist.Options{Root: s.dir, DiskMaxBytes: 64 * 1024})
	s.Require().NoError(err)
	s.p = p
}

func (s *ConcurrencySuite) TearDownTest() { _ = s.p.Close() }

// TestSweepRacesWritesPreservesInvariant runs writers, ref churn, and the
// ghost sweep concurrently; the invariant is that every live data_idx entry
// has a refcount >= 1 and a present chunk file, and no underflow is observed.
func (s *ConcurrencySuite) TestSweepRacesWritesPreservesInvariant() {
	const writers, iters = 4, 200
	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				data := []byte(fmt.Sprintf("chunk-%d-%d", w, i%7)) // 7 distinct hashes/writer
				hash, _, err := s.p.WriteChunk(data)
				if err != nil {
					s.T().Errorf("WriteChunk: %v", err)
					return
				}
				if err := s.p.PutChunkRef(fmt.Sprintf("p%d", w), i%7,
					persist.ChunkRef{Hash: hash, Size: uint32(len(data))}); err != nil {
					s.T().Errorf("PutChunkRef: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			persist.TestingRunGhostSweep(s.T(), s.p, 1.0)
		}
	}()
	wg.Wait()

	s.Assert().Equal(int64(0), persist.TestingRefUnderflows(s.p), "no refcount underflow under concurrency")

	// Every currently-referenced key must resolve to a readable, correctly sized chunk.
	for w := 0; w < writers; w++ {
		for idx := 0; idx < 7; idx++ {
			ref, ok, err := s.p.GetChunkRef(fmt.Sprintf("p%d", w), idx)
			s.Require().NoError(err)
			if !ok {
				continue
			}
			data, err := s.p.ReadChunk(ref.Hash)
			s.Require().NoError(err, "live index entry must have a present chunk file")
			s.Assert().Len(data, int(ref.Size), "chunk length must match its ref")
			cnt, err := s.p.ChunkRefCount(ref.Hash)
			s.Require().NoError(err)
			s.Assert().GreaterOrEqual(cnt, uint64(1), "live entry's hash must keep refcount >= 1")
		}
	}
}

func TestConcurrencySuite(t *testing.T) { suite.Run(t, new(ConcurrencySuite)) }
