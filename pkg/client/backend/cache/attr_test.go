package cache

import (
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/metrics"

	"github.com/stretchr/testify/suite"
)

type AttrCacheTestSuite struct {
	suite.Suite
	now   time.Time
	clock func() time.Time
	c     *attrCache
}

func (s *AttrCacheTestSuite) SetupTest() {
	s.now = time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return s.now }
	s.c = newAttrCache(newAccountant(0, 0), 5*time.Second, 2*time.Second, s.clock, metrics.NopRecorder{})
}

func (s *AttrCacheTestSuite) advance(d time.Duration) { s.now = s.now.Add(d) }

func (s *AttrCacheTestSuite) TestMiss() {
	a, hit, pos := s.c.get("/x")
	s.Assert().False(hit)
	s.Assert().False(pos)
	s.Assert().Nil(a)
}

func (s *AttrCacheTestSuite) TestPositiveHit() {
	s.c.putPositive("/x", &backend.Attr{Ino: 7, Size: 100})
	a, hit, pos := s.c.get("/x")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(7), a.Ino)
}

func (s *AttrCacheTestSuite) TestNegativeHit() {
	s.c.putNegative("/missing")
	a, hit, pos := s.c.get("/missing")
	s.Require().True(hit)
	s.Assert().False(pos)
	s.Assert().Nil(a)
}

func (s *AttrCacheTestSuite) TestPositiveExpiry() {
	s.c.putPositive("/x", &backend.Attr{Ino: 1})
	s.advance(6 * time.Second) // > AttrTTL (5s)
	_, hit, _ := s.c.get("/x")
	s.Assert().False(hit)
}

func (s *AttrCacheTestSuite) TestNegativeExpiry() {
	s.c.putNegative("/missing")
	s.advance(3 * time.Second) // > NegativeTTL (2s)
	_, hit, _ := s.c.get("/missing")
	s.Assert().False(hit)
}

func (s *AttrCacheTestSuite) TestInvalidate() {
	s.c.putPositive("/x", &backend.Attr{Ino: 1})
	s.c.invalidate("/x")
	_, hit, _ := s.c.get("/x")
	s.Assert().False(hit)
}

func (s *AttrCacheTestSuite) TestPutPositiveNilIsNoOp() {
	s.c.putPositive("/x", nil)
	_, hit, _ := s.c.get("/x")
	s.Assert().False(hit, "putPositive(nil) must not insert a cache entry")
}

func TestAttrCacheTestSuite(t *testing.T) {
	suite.Run(t, new(AttrCacheTestSuite))
}
