package cache

import (
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
)

// maxCachedXAttrValueBytes bounds the size of a positive xattr value we will
// cache. Values above it are served straight through (re-fetched each time) so
// a few large xattrs can't balloon the cache. The hot case this cache targets —
// the kernel/npm probing security.capability, system.posix_acl_* which don't
// exist — caches a nil-data negative, well under the cap.
const maxCachedXAttrValueBytes = 4096

// getXAttrEntry is a cached GetXAttr(path, attr) result with an expiry.
type getXAttrEntry struct {
	data      []byte
	status    proto.FsError
	expiresAt time.Time
}

// getXAttrCache caches per-attribute GetXAttr results so a flood of identical
// probes (notably the kernel's security.capability / system.posix_acl_* lookups
// before reads/writes and tar/npm metadata churn) does not become one WAN RPC
// each. Crucially it caches NEGATIVE answers (ENO_XATTR / ENOTSUP / ENOSYS):
// those are exactly the repeated "attribute absent" probes that dominate the
// GetXAttr flood, and there is no other negative-caching layer for them.
//
// Like xattrCache (the names list) this is advisory/display-only — every real
// operation is still enforced server-side, so a stale entry yields at most a
// wrong getxattr() answer, never an enforcement bypass. Freshness is bounded by
// TTL plus invalidation in lockstep with the names cache (SetXAttr/RemoveXAttr,
// path lifecycle, Subscribe push). Keyed (path -> attr -> entry) so
// invalidate(path) drops every attr for a path in one step. ttl<=0 disables it.
type getXAttrCache struct {
	mu  sync.Mutex
	m   map[string]map[string]getXAttrEntry
	now func() time.Time
	ttl time.Duration
}

func newGetXAttrCache(ttl time.Duration, now func() time.Time) *getXAttrCache {
	if now == nil {
		now = time.Now
	}
	return &getXAttrCache{m: make(map[string]map[string]getXAttrEntry), now: now, ttl: ttl}
}

// get returns (data, status, true) on a fresh hit. The returned slice is a copy
// the caller may retain. (nil, 0, false) on miss or expiry.
func (c *getXAttrCache) get(path, attr string) ([]byte, proto.FsError, bool) {
	if c.ttl <= 0 {
		return nil, 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	byAttr, ok := c.m[path]
	if !ok {
		return nil, 0, false
	}
	e, ok := byAttr[attr]
	if !ok {
		return nil, 0, false
	}
	if c.now().After(e.expiresAt) {
		delete(byAttr, attr)
		if len(byAttr) == 0 {
			delete(c.m, path)
		}
		return nil, 0, false
	}
	out := make([]byte, len(e.data))
	copy(out, e.data)
	return out, e.status, true
}

// put caches a stable GetXAttr result. Positive values over
// maxCachedXAttrValueBytes are skipped (served through each time); negatives
// (nil data) are always small. The caller filters to stable statuses.
func (c *getXAttrCache) put(path, attr string, data []byte, status proto.FsError) {
	if c.ttl <= 0 {
		return
	}
	if status == proto.FsError_FS_OK && len(data) > maxCachedXAttrValueBytes {
		return
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	c.mu.Lock()
	defer c.mu.Unlock()
	byAttr := c.m[path]
	if byAttr == nil {
		byAttr = make(map[string]getXAttrEntry)
		c.m[path] = byAttr
	}
	byAttr[attr] = getXAttrEntry{data: stored, status: status, expiresAt: c.now().Add(c.ttl)}
}

// invalidate drops all cached attributes for a path. Called wherever the path's
// xattrs may have changed (SetXAttr/RemoveXAttr, delete/rename, Subscribe push).
func (c *getXAttrCache) invalidate(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, path)
}
