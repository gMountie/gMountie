package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type IdentityCacheSuite struct{ suite.Suite }

func TestIdentityCacheSuite(t *testing.T) { suite.Run(t, new(IdentityCacheSuite)) }

type countingResolver struct{ calls atomic.Int64 }

func (c *countingResolver) Resolve(p string) (Identity, error) {
	c.calls.Add(1)
	return Identity{Principal: p, Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, nil
}

func (s *IdentityCacheSuite) TestCachesWithinTTL() {
	cr := &countingResolver{}
	c := NewCachedResolver(cr, time.Minute)
	for i := 0; i < 3; i++ {
		_, err := c.Resolve("alice")
		s.Require().NoError(err)
	}
	s.Equal(int64(1), cr.calls.Load())
}

func (s *IdentityCacheSuite) TestDoesNotCacheErrors() {
	failing := resolverFunc(func(string) (Identity, error) { return Identity{}, ErrPrincipalNotFound })
	c := NewCachedResolver(failing, time.Minute)
	_, e1 := c.Resolve("x")
	_, e2 := c.Resolve("x")
	s.Require().ErrorIs(e1, ErrPrincipalNotFound)
	s.Require().ErrorIs(e2, ErrPrincipalNotFound)
}

type resolverFunc func(string) (Identity, error)

func (f resolverFunc) Resolve(p string) (Identity, error) { return f(p) }
