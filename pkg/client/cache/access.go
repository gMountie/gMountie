package cache

import (
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
)

// accessEntry is a cached Access result for one (path, mask) with an expiry.
type accessEntry struct {
	result    proto.FsError
	expiresAt time.Time
}

// accessCache caches Access(path, mask) results so a flood of permission checks
// (notably GNOME/Nautilus access()-probing every entry, and the kernel's
// per-component X-bit checks during path resolution) does not become one WAN
// RPC each. It is only consulted on mounts that forward Access — i.e. non-squash
// modes where the kernel can't enforce locally (squash mounts use the kernel's
// default_permissions instead, so Access never reaches the backend).
//
// Correctness: an Access answer depends on the path's perms (its attr), so the
// cache is invalidated in lockstep with the attr cache on every perm/existence
// change (SetAttr, create/delete/rename, Subscribe push). It is also bounded by
// the TTL. Crucially, Access is advisory — the server still enforces every real
// operation — so a stale entry yields at most a wrong access() answer, never
// unauthorized access. Keyed (path -> mask -> entry) so invalidate(path) drops
// all masks for a path in one step. ttl<=0 disables the cache.
type accessCache struct {
	mu  sync.Mutex
	m   map[string]map[uint32]accessEntry
	now func() time.Time
	ttl time.Duration
}

func newAccessCache(ttl time.Duration, now func() time.Time) *accessCache {
	if now == nil {
		now = time.Now
	}
	return &accessCache{m: make(map[string]map[uint32]accessEntry), now: now, ttl: ttl}
}

// get returns the cached result for (path, mask) when present and unexpired.
func (c *accessCache) get(path string, mask uint32) (proto.FsError, bool) {
	if c.ttl <= 0 {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	byMask, ok := c.m[path]
	if !ok {
		return 0, false
	}
	e, ok := byMask[mask]
	if !ok {
		return 0, false
	}
	if c.now().After(e.expiresAt) {
		delete(byMask, mask)
		if len(byMask) == 0 {
			delete(c.m, path)
		}
		return 0, false
	}
	return e.result, true
}

// put caches a stable Access result (granted/denied). Transient errors are not
// cached by the caller.
func (c *accessCache) put(path string, mask uint32, result proto.FsError) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	byMask := c.m[path]
	if byMask == nil {
		byMask = make(map[uint32]accessEntry)
		c.m[path] = byMask
	}
	byMask[mask] = accessEntry{result: result, expiresAt: c.now().Add(c.ttl)}
}

// invalidate drops all cached masks for a path. Called wherever the path's attr
// is invalidated (its perms or existence may have changed).
func (c *accessCache) invalidate(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, path)
}
