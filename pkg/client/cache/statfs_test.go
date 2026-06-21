package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/io"
)

type StatfsCacheSuite struct{ suite.Suite }

func TestStatfsCacheSuite(t *testing.T) { suite.Run(t, new(StatfsCacheSuite)) }

func (s *StatfsCacheSuite) TestHitMissAndExpiry() {
	now := time.Unix(1000, 0)
	c := newStatfsCache(time.Second, func() time.Time { return now })
	_, ok := c.get("/")
	s.Assert().False(ok, "cold get is a miss")
	c.put("/", &io.StatFs{Bfree: 7})
	v, ok := c.get("/")
	s.Require().True(ok)
	s.Assert().Equal(uint64(7), v.Bfree)
	now = now.Add(2 * time.Second) // past TTL
	_, ok = c.get("/")
	s.Assert().False(ok, "expired entry is a miss")
}

func (s *StatfsCacheSuite) TestZeroTTLDisables() {
	c := newStatfsCache(0, nil)
	c.put("/", &io.StatFs{Bfree: 7})
	_, ok := c.get("/")
	s.Assert().False(ok, "ttl<=0 disables caching")
}
