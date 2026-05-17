package cache

import (
	"time"

	"gmountie/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// attrEntry is the value stored in attrCache. negative=true means a
// prior Lookup returned ENOENT; attr is then nil. expiresAt is the
// absolute deadline at which the entry should be treated as a miss.
type attrEntry struct {
	attr      *io.Attr
	negative  bool
	expiresAt time.Time
}

// attrCache is a thin TTL wrapper over a store. attrTTL and negativeTTL
// are taken from CacheConfig at construction.
type attrCache struct {
	st          *store
	now         func() time.Time
	attrTTL     time.Duration
	negativeTTL time.Duration
}

func newAttrCache(acct *accountant, attrTTL, negativeTTL time.Duration, now func() time.Time) *attrCache {
	if now == nil {
		now = time.Now
	}
	return &attrCache{
		st:          newStore(acct),
		now:         now,
		attrTTL:     attrTTL,
		negativeTTL: negativeTTL,
	}
}

// get returns (attr, true, true) on a positive hit, (nil, true, false)
// on a negative hit (ENOENT cached), or (nil, false, false) on a miss
// or expired entry. Two booleans (hit, positive) make the call sites
// readable without a third type.
func (c *attrCache) get(path string) (*io.Attr, bool, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false, false
	}
	ae := e.value.(*attrEntry)
	if c.now().After(ae.expiresAt) {
		c.st.remove(path)
		return nil, false, false
	}
	if ae.negative {
		return nil, true, false
	}
	return ae.attr, true, true
}

// putPositive caches a successful Stat result.
func (c *attrCache) putPositive(path string, attr *io.Attr) {
	if attr == nil {
		return
	}
	ae := &attrEntry{attr: attr, expiresAt: c.now().Add(c.attrTTL)}
	c.st.put(path, ae, attrEntrySize(ae))
}

// putNegative caches an ENOENT result.
func (c *attrCache) putNegative(path string) {
	ae := &attrEntry{negative: true, expiresAt: c.now().Add(c.negativeTTL)}
	c.st.put(path, ae, attrEntrySize(ae))
}

// invalidate drops the cached entry for path (positive or negative).
func (c *attrCache) invalidate(path string) {
	c.st.remove(path)
}

// attrEntrySize estimates the in-memory footprint of an attrEntry.
// Used for accountant bookkeeping; small and approximate is fine.
func attrEntrySize(_ *attrEntry) int {
	// 16-ish fields × 8 bytes + struct overhead. 256 is a generous
	// rounded estimate that absorbs the negative variant too.
	return 256
}

// attrStatus returns the appropriate FUSE status for a cache hit.
// Convenience for backend.go's Stat/Lookup handlers.
func attrStatus(positive bool) fuse.Status {
	if positive {
		return fuse.OK
	}
	return fuse.ENOENT
}
