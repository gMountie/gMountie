package cache

import (
	"bytes"
	"encoding/gob"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/io"
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
		st:          newStore(acct, "attr"),
		now:         now,
		attrTTL:     attrTTL,
		negativeTTL: negativeTTL,
	}
}

// get returns (attr, true, true) on a positive hit, (nil, true, false)
// on a negative hit (ENOENT cached), or (nil, false, false) on a miss
// or expired entry. Two booleans (hit, positive) make the call sites
// readable without a third type.
//
// The returned *io.Attr is the cached pointer (not a copy). Callers
// MUST NOT mutate it — a future handler that wrote through this
// pointer would silently corrupt the cache entry for every subsequent
// reader. dirCache copies the slice it returns for the same reason;
// attr is uncopied because real call sites today only read fields,
// but the no-mutate rule is contract, not enforcement.
func (c *attrCache) get(path string) (*io.Attr, bool, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false, false
	}
	// attr store only holds *attrEntry; the assertion can't fail.
	ae, _ := e.value.(*attrEntry)
	// TTL=0 means "never expire on time alone"; expiry is driven solely
	// by Subscribe push invalidation or explicit cache ops.
	if ae.negative {
		if c.negativeTTL > 0 && c.now().After(ae.expiresAt) {
			c.st.remove(path)
			return nil, false, false
		}
		return nil, true, false
	}
	if c.attrTTL > 0 && c.now().After(ae.expiresAt) {
		c.st.remove(path)
		return nil, false, false
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

// persistedAttr is the on-disk shape. Negative entries persist false
// for attr; gob-zero for missing fields is fine.
type persistedAttr struct {
	Attr      *io.Attr
	Negative  bool
	ExpiresAt int64 // unix nanos
	Version   uint64
}

// newAttrCacheWithPersist constructs an attrCache that fronts the
// persist tier when p is non-nil. Loader gob-decodes attr bytes from
// the attr bucket; Putter gob-encodes and writes through; Remover
// deletes. nil p falls back to newAttrCache (memory-only).
func newAttrCacheWithPersist(acct *accountant, attrTTL, negativeTTL time.Duration, now func() time.Time, p *persist.Persist) *attrCache {
	if now == nil {
		now = time.Now
	}
	if p == nil {
		return newAttrCache(acct, attrTTL, negativeTTL, now)
	}
	c := &attrCache{
		now:         now,
		attrTTL:     attrTTL,
		negativeTTL: negativeTTL,
	}
	loader := func(key string) (any, int, bool) {
		raw, ok, err := p.GetAttrBytes(key)
		if err != nil || !ok {
			return nil, 0, false
		}
		var pa persistedAttr
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&pa); err != nil {
			return nil, 0, false
		}
		ae := &attrEntry{
			attr:      pa.Attr,
			negative:  pa.Negative,
			expiresAt: time.Unix(0, pa.ExpiresAt),
		}
		return ae, attrEntrySize(ae), true
	}
	putter := func(key string, value any, _ int) {
		ae, _ := value.(*attrEntry) // store only holds *attrEntry

		pa := persistedAttr{Attr: ae.attr, Negative: ae.negative, ExpiresAt: ae.expiresAt.UnixNano()}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(pa); err != nil {
			return
		}
		_ = p.PutAttrBytes(key, buf.Bytes())
	}
	remover := func(key string) { _ = p.DeleteAttrBytes(key) }
	c.st = newStoreWithPersist(acct, loader, putter, remover, "attr")
	return c
}
