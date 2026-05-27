package service

import (
	"sync"
	"time"
)

type cacheEntry struct {
	id      Identity
	expires time.Time
}

// cachedResolver wraps a resolver with a per-principal TTL cache. Errors are
// never cached (a transient resolver failure must not pin a denial). One
// cachedResolver exists per volume, so the volume is implicit.
type cachedResolver struct {
	inner IdentityResolver
	ttl   time.Duration
	mu    sync.Mutex
	store map[string]cacheEntry
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

	id, err := c.inner.Resolve(principal)
	if err != nil {
		return Identity{}, err
	}
	c.mu.Lock()
	c.store[principal] = cacheEntry{id: id, expires: now.Add(c.ttl)}
	c.mu.Unlock()
	return id, nil
}
