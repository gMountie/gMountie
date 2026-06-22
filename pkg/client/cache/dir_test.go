package cache

import (
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/stretchr/testify/suite"
)

type DirCacheTestSuite struct {
	suite.Suite
	now   time.Time
	clock func() time.Time
	c     *dirCache
}

func (s *DirCacheTestSuite) SetupTest() {
	s.now = time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return s.now }
	s.c = newDirCache(newAccountant(0, 0), 5*time.Second, s.clock, metrics.NopRecorder{})
}

func (s *DirCacheTestSuite) advance(d time.Duration) { s.now = s.now.Add(d) }

func (s *DirCacheTestSuite) TestMiss() {
	_, hit := s.c.get("/d")
	s.Assert().False(hit)
}

func (s *DirCacheTestSuite) TestHitReturnsCopy() {
	entries := []io.DirEntry{{Name: "a", Mode: 0o644, Ino: 1}, {Name: "b", Mode: 0o644, Ino: 2}}
	s.c.put("/d", entries)
	got, hit := s.c.get("/d")
	s.Require().True(hit)
	s.Require().Len(got, 2)
	got[0].Name = "MUTATED"
	got2, _ := s.c.get("/d")
	s.Assert().Equal("a", got2[0].Name, "cache must return defensive copies")
}

func (s *DirCacheTestSuite) TestPutCopiesInput() {
	entries := []io.DirEntry{{Name: "a"}}
	s.c.put("/d", entries)
	entries[0].Name = "MUTATED_INPUT"
	got, _ := s.c.get("/d")
	s.Assert().Equal("a", got[0].Name, "put must copy the input slice so caller mutations are isolated")
}

func (s *DirCacheTestSuite) TestExpiry() {
	s.c.put("/d", []io.DirEntry{{Name: "a"}})
	s.advance(6 * time.Second)
	_, hit := s.c.get("/d")
	s.Assert().False(hit)
}

func (s *DirCacheTestSuite) TestInvalidate() {
	s.c.put("/d", []io.DirEntry{{Name: "a"}})
	s.c.invalidate("/d")
	_, hit := s.c.get("/d")
	s.Assert().False(hit)
}

func TestDirCacheTestSuite(t *testing.T) {
	suite.Run(t, new(DirCacheTestSuite))
}
