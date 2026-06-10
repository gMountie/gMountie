package service

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

type cacheEntry struct {
	id      Identity
	err     error // non-nil for a negative (not-found) entry
	expires time.Time
}

// negativeTTL bounds how long a definitive ErrPrincipalNotFound is cached.
// Short on purpose: it only needs to stop a misbehaving (or malicious) client
// from forking one `id` subprocess per RPC for a nonexistent principal, while
// a freshly-created system account becomes resolvable within seconds.
const negativeTTL = 5 * time.Second

// cachedResolver wraps a resolver with a per-principal TTL cache. Transient
// errors are never cached (a flaky resolver must not pin a denial); a
// definitive ErrPrincipalNotFound IS cached for negativeTTL so unknown
// principals can't drive a subprocess fork per request. One cachedResolver
// exists per volume, so the volume is implicit.
//
// Concurrent cache misses for the same principal are collapsed via singleflight
// so at most one subprocess (id/getent) is forked per principal at a time.
type cachedResolver struct {
	inner IdentityResolver
	ttl   time.Duration
	mu    sync.Mutex
	store map[string]cacheEntry
	sf    singleflight.Group
}

func NewCachedResolver(inner IdentityResolver, ttl time.Duration) IdentityResolver {
	return &cachedResolver{inner: inner, ttl: ttl, store: make(map[string]cacheEntry)}
}

// ConstantIdentity forwards the inner resolver's answer: a TTL cache never
// changes whether the underlying resolution is time-invariant.
func (c *cachedResolver) ConstantIdentity() bool { return resolverIsConstant(c.inner) }

func (c *cachedResolver) Resolve(principal string) (Identity, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.store[principal]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.id, e.err
	}
	c.mu.Unlock()

	// Collapse concurrent misses for the same principal into one resolve call.
	val, err, _ := c.sf.Do(principal, func() (any, error) {
		// Re-check under the singleflight barrier: another goroutine may have
		// already populated the cache before we were dispatched.
		now2 := time.Now()
		c.mu.Lock()
		if e, ok := c.store[principal]; ok && now2.Before(e.expires) {
			c.mu.Unlock()
			return e.id, e.err
		}
		c.mu.Unlock()

		id, err := c.inner.Resolve(principal)
		if err != nil {
			if errors.Is(err, ErrPrincipalNotFound) {
				// Definitive not-found: negative-cache it briefly.
				c.mu.Lock()
				c.store[principal] = cacheEntry{err: err, expires: now2.Add(negativeTTL)}
				c.mu.Unlock()
			}
			return Identity{}, err
		}
		c.mu.Lock()
		c.store[principal] = cacheEntry{id: id, expires: now2.Add(c.ttl)}
		c.mu.Unlock()
		return id, nil
	})
	if err != nil {
		return Identity{}, err
	}
	id, _ := val.(Identity)
	return id, nil
}
