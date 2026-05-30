package service

import (
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type cacheEntry struct {
	id      Identity
	expires time.Time
}

// cachedResolver wraps a resolver with a per-principal TTL cache. Errors are
// never cached (a transient resolver failure must not pin a denial). One
// cachedResolver exists per volume, so the volume is implicit.
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

func (c *cachedResolver) Resolve(principal string) (Identity, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.store[principal]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.id, nil
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
			return e.id, nil
		}
		c.mu.Unlock()

		id, err := c.inner.Resolve(principal)
		if err != nil {
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
