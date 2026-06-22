package cache

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/proto"
)

type GetXAttrCacheSuite struct{ suite.Suite }

func TestGetXAttrCacheSuite(t *testing.T) { suite.Run(t, new(GetXAttrCacheSuite)) }

func (s *GetXAttrCacheSuite) TestPositiveHitMissAndCopy() {
	now := time.Unix(1000, 0)
	c := newGetXAttrCache(time.Second, func() time.Time { return now })

	_, _, ok := c.get("/f", "user.foo")
	s.Assert().False(ok, "cold get is a miss")

	c.put("/f", "user.foo", []byte("bar"), proto.FsError_FS_OK)
	data, st, ok := c.get("/f", "user.foo")
	s.Require().True(ok)
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal([]byte("bar"), data)

	// returned slice is a copy: mutating it must not corrupt the cached value
	data[0] = 'X'
	data2, _, _ := c.get("/f", "user.foo")
	s.Equal([]byte("bar"), data2)

	// a different attr on the same path is a separate entry
	_, _, ok = c.get("/f", "user.other")
	s.Assert().False(ok)
}

// TestNegativeCached is the headline behaviour: an "attribute absent" answer
// (ENO_XATTR) is cached so the kernel's repeated security.capability /
// posix_acl probes don't each become a WAN RPC.
func (s *GetXAttrCacheSuite) TestNegativeCached() {
	now := time.Unix(1000, 0)
	c := newGetXAttrCache(time.Second, func() time.Time { return now })

	c.put("/f", "security.capability", nil, proto.FsError_FS_ENO_XATTR)
	data, st, ok := c.get("/f", "security.capability")
	s.Require().True(ok)
	s.Equal(proto.FsError_FS_ENO_XATTR, st)
	s.Empty(data)
}

func (s *GetXAttrCacheSuite) TestExpiryAndInvalidate() {
	now := time.Unix(1000, 0)
	c := newGetXAttrCache(time.Second, func() time.Time { return now })

	c.put("/f", "user.a", []byte("1"), proto.FsError_FS_OK)
	c.put("/f", "user.b", []byte("2"), proto.FsError_FS_OK)

	now = now.Add(2 * time.Second) // past TTL
	_, _, ok := c.get("/f", "user.a")
	s.Assert().False(ok, "entry past TTL is a miss")

	now = time.Unix(1000, 0)
	c.put("/f", "user.a", []byte("1"), proto.FsError_FS_OK)
	c.invalidate("/f")
	_, _, ok = c.get("/f", "user.a")
	s.Assert().False(ok, "invalidate drops every attr for the path")
}

func (s *GetXAttrCacheSuite) TestLargePositiveValueNotCached() {
	now := time.Unix(1000, 0)
	c := newGetXAttrCache(time.Second, func() time.Time { return now })

	big := bytes.Repeat([]byte{'z'}, maxCachedXAttrValueBytes+1)
	c.put("/f", "user.big", big, proto.FsError_FS_OK)
	_, _, ok := c.get("/f", "user.big")
	s.Assert().False(ok, "oversize positive value is served through, not cached")
}

func (s *GetXAttrCacheSuite) TestZeroTTLDisables() {
	c := newGetXAttrCache(0, nil)
	c.put("/f", "user.a", []byte("1"), proto.FsError_FS_OK)
	_, _, ok := c.get("/f", "user.a")
	s.Assert().False(ok, "ttl<=0 disables the cache")
}
