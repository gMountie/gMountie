package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/proto"
)

type AccessCacheSuite struct{ suite.Suite }

func TestAccessCacheSuite(t *testing.T) { suite.Run(t, new(AccessCacheSuite)) }

func (s *AccessCacheSuite) TestHitMissExpiryAndInvalidate() {
	now := time.Unix(1000, 0)
	c := newAccessCache(time.Second, func() time.Time { return now })

	_, ok := c.get("/f", 4)
	s.Assert().False(ok, "cold get is a miss")

	c.put("/f", 4, proto.FsError_FS_OK)
	r, ok := c.get("/f", 4)
	s.Require().True(ok)
	s.Equal(proto.FsError_FS_OK, r)

	// different mask is a separate entry
	_, ok = c.get("/f", 2)
	s.Assert().False(ok)

	// negative (denied) results cache too
	c.put("/f", 2, proto.FsError_FS_EACCES)
	r, ok = c.get("/f", 2)
	s.Require().True(ok)
	s.Equal(proto.FsError_FS_EACCES, r)

	// invalidate drops all masks for the path
	c.invalidate("/f")
	_, ok = c.get("/f", 4)
	s.Assert().False(ok, "invalidate clears all masks")

	// TTL expiry
	c.put("/f", 4, proto.FsError_FS_OK)
	now = now.Add(2 * time.Second)
	_, ok = c.get("/f", 4)
	s.Assert().False(ok, "expired entry is a miss")
}

func (s *AccessCacheSuite) TestZeroTTLDisables() {
	c := newAccessCache(0, nil)
	c.put("/f", 4, proto.FsError_FS_OK)
	_, ok := c.get("/f", 4)
	s.Assert().False(ok, "ttl<=0 disables caching")
}
