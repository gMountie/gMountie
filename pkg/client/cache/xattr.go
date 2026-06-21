package cache

import (
	"time"
)

// xattrEntry is the value stored in xattrCache. An empty (non-nil) names
// slice is a valid positive entry meaning "this path has no xattrs".
type xattrEntry struct {
	names     []string
	expiresAt time.Time
}

// xattrCache is a TTL wrapper over a store holding per-path xattr-name lists.
// It is advisory/display-only (ACL enforcement is server-side), so it carries
// no negative entries and no validity-tracker gating — TTL plus explicit/Subscribe
// invalidation are the only freshness signals.
type xattrCache struct {
	st  *store
	now func() time.Time
	ttl time.Duration
}

func newXAttrCache(acct *accountant, ttl time.Duration, now func() time.Time) *xattrCache {
	if now == nil {
		now = time.Now
	}
	return &xattrCache{st: newStore(acct, "xattr"), now: now, ttl: ttl}
}

// get returns (names, true) on a fresh hit, (nil, false) on miss or expiry.
// The returned slice is a copy; callers may not mutate the cached view.
func (c *xattrCache) get(path string) ([]string, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false
	}
	xe, _ := e.value.(*xattrEntry) // xattr store only holds *xattrEntry
	// ttl=0 means "never expire on time alone"; invalidation is the only signal.
	if c.ttl > 0 && c.now().After(xe.expiresAt) {
		c.st.remove(path)
		return nil, false
	}
	out := make([]string, len(xe.names))
	copy(out, xe.names)
	return out, true
}

// put stores names for path (copying the slice). A nil names slice is stored
// as an empty list so it still reads back as a positive "no xattrs" hit.
func (c *xattrCache) put(path string, names []string) {
	stored := make([]string, len(names))
	copy(stored, names)
	xe := &xattrEntry{names: stored, expiresAt: c.now().Add(c.ttl)}
	c.st.put(path, xe, xattrEntrySize(path, xe))
}

func (c *xattrCache) invalidate(path string) { c.st.remove(path) }

// xattrEntrySize estimates the in-memory footprint: the path (stored twice —
// map key + entry key copy) plus struct overhead plus each name's bytes.
func xattrEntrySize(path string, xe *xattrEntry) int {
	n := 2*len(path) + 64
	for i := range xe.names {
		n += len(xe.names[i]) + 16
	}
	return n
}
