package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
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

// TestNotFoundIsNegativeCached: a definitive not-found is served from the
// negative cache within negativeTTL — exactly one subprocess-backed resolve
// runs no matter how often an unknown principal is retried.
func (s *IdentityCacheSuite) TestNotFoundIsNegativeCached() {
	var calls atomic.Int64
	failing := resolverFunc(func(string) (Identity, error) {
		calls.Add(1)
		return Identity{}, ErrPrincipalNotFound
	})
	c := NewCachedResolver(failing, time.Minute)
	for i := 0; i < 3; i++ {
		_, err := c.Resolve("x")
		s.Require().ErrorIs(err, ErrPrincipalNotFound, "negative entries must keep failing closed")
	}
	s.Equal(int64(1), calls.Load(), "not-found must be negative-cached, not re-resolved per call")
}

// TestTransientErrorsAreNotCached: anything other than ErrPrincipalNotFound
// (a flaky NSS source, a timeout) must NOT pin a denial — the next call
// re-resolves.
func (s *IdentityCacheSuite) TestTransientErrorsAreNotCached() {
	var calls atomic.Int64
	flaky := resolverFunc(func(p string) (Identity, error) {
		if calls.Add(1) == 1 {
			return Identity{}, errTransient
		}
		return Identity{Principal: p, Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, nil
	})
	c := NewCachedResolver(flaky, time.Minute)
	_, e1 := c.Resolve("x")
	s.Require().Error(e1)
	id, e2 := c.Resolve("x")
	s.Require().NoError(e2, "transient failure must not be cached")
	s.Equal(uint32(1001), id.Uid)
}

var errTransient = errors.New("nss timeout")

type resolverFunc func(string) (Identity, error)

func (f resolverFunc) Resolve(p string) (Identity, error) { return f(p) }
