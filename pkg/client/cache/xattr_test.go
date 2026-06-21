package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type XAttrCacheSuite struct{ suite.Suite }

func TestXAttrCacheSuite(t *testing.T) { suite.Run(t, new(XAttrCacheSuite)) }

func (s *XAttrCacheSuite) newCache(ttl time.Duration, now func() time.Time) *xattrCache {
	return newXAttrCache(newAccountant(0, 0), ttl, now)
}

func (s *XAttrCacheSuite) TestPutGetHit() {
	c := s.newCache(time.Minute, nil)
	c.put("a/b", []string{"user.x"})
	got, hit := c.get("a/b")
	s.True(hit)
	s.Equal([]string{"user.x"}, got)
}

func (s *XAttrCacheSuite) TestEmptyListIsPositiveHit() {
	c := s.newCache(time.Minute, nil)
	c.put("a/b", []string{}) // "no xattrs" is a cacheable fact
	got, hit := c.get("a/b")
	s.True(hit)
	s.Empty(got)
}

func (s *XAttrCacheSuite) TestMiss() {
	c := s.newCache(time.Minute, nil)
	_, hit := c.get("nope")
	s.False(hit)
}

func (s *XAttrCacheSuite) TestTTLExpiry() {
	t0 := time.Unix(1000, 0)
	cur := t0
	c := s.newCache(time.Minute, func() time.Time { return cur })
	c.put("a/b", []string{"user.x"})
	cur = t0.Add(2 * time.Minute)
	_, hit := c.get("a/b")
	s.False(hit)
}

func (s *XAttrCacheSuite) TestZeroTTLNeverExpiresOnTime() {
	t0 := time.Unix(1000, 0)
	cur := t0
	c := s.newCache(0, func() time.Time { return cur })
	c.put("a/b", []string{"user.x"})
	cur = t0.Add(99 * time.Hour)
	_, hit := c.get("a/b")
	s.True(hit)
}

func (s *XAttrCacheSuite) TestInvalidateDrops() {
	c := s.newCache(time.Minute, nil)
	c.put("a/b", []string{"user.x"})
	c.invalidate("a/b")
	_, hit := c.get("a/b")
	s.False(hit)
}
