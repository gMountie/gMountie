package cache

import (
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
)

// statfsEntry is a cached StatFs result with an absolute expiry.
type statfsEntry struct {
	val       *backend.StatFs
	expiresAt time.Time
}

// statfsCache is a tiny TTL cache for StatFs (free-space) results. macOS/FUSE-T
// issues a statfs alongside almost every operation, so without caching StatFs is
// a full WAN round-trip on each — the dominant cost of browsing (26 of 39 RPCs
// in a single `ls`). Free space changes slowly, so a short TTL collapses the
// flood while keeping the displayed value fresh. ttl<=0 disables the cache.
//
// It is keyed by path (statfs is volume-global in practice, but keying is cheap
// and avoids assuming a single query path). Entries are TTL-only — no explicit
// invalidation — so a large write's effect on free space lags by at most ttl.
type statfsCache struct {
	mu  sync.Mutex
	m   map[string]statfsEntry
	now func() time.Time
	ttl time.Duration
}

func newStatfsCache(ttl time.Duration, now func() time.Time) *statfsCache {
	if now == nil {
		now = time.Now
	}
	return &statfsCache{m: make(map[string]statfsEntry), now: now, ttl: ttl}
}

// get returns the cached StatFs for path when present and unexpired.
func (c *statfsCache) get(path string) (*backend.StatFs, bool) {
	if c.ttl <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[path]
	if !ok || c.now().After(e.expiresAt) {
		return nil, false
	}
	return e.val, true
}

// put caches a successful StatFs result for path.
func (c *statfsCache) put(path string, v *backend.StatFs) {
	if c.ttl <= 0 || v == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[path] = statfsEntry{val: v, expiresAt: c.now().Add(c.ttl)}
}
