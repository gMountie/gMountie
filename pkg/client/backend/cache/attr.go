package cache

import (
	"bytes"
	"encoding/gob"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
)

// attrEntry is the value stored in attrCache. negative=true means a
// prior Lookup returned ENOENT; attr is then nil. expiresAt is the
// absolute deadline at which the entry should be treated as a miss.
type attrEntry struct {
	attr      *backend.Attr
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
	// prefixRemover, when non-nil, deletes a whole path subtree from the disk
	// tier (set only for the persist-backed cache). The memory tier is handled
	// by st.removeMatching; the disk tier needs a separate prefix delete because
	// removeMatching only sees memory-resident keys — a descendant evicted to
	// disk would otherwise be loaded back stale after an ancestor rename.
	prefixRemover func(prefix string)
}

func newAttrCache(acct *accountant, attrTTL, negativeTTL time.Duration, now func() time.Time, rec metrics.Recorder) *attrCache {
	if now == nil {
		now = time.Now
	}
	return &attrCache{
		st:          newStore(acct, "attr", rec),
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
// The returned *backend.Attr is the cached pointer (not a copy). Callers
// MUST NOT mutate it — a future handler that wrote through this
// pointer would silently corrupt the cache entry for every subsequent
// reader. dirCache copies the slice it returns for the same reason;
// attr is uncopied because real call sites today only read fields,
// but the no-mutate rule is contract, not enforcement.
func (c *attrCache) get(path string) (*backend.Attr, bool, bool) {
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
func (c *attrCache) putPositive(path string, attr *backend.Attr) {
	if attr == nil {
		return
	}
	ae := &attrEntry{attr: attr, expiresAt: c.now().Add(c.attrTTL)}
	c.st.put(path, ae, attrEntrySize(path, ae))
}

// putNegative caches an ENOENT result.
func (c *attrCache) putNegative(path string) {
	ae := &attrEntry{negative: true, expiresAt: c.now().Add(c.negativeTTL)}
	c.st.put(path, ae, attrEntrySize(path, ae))
}

// invalidate drops the cached entry for path (positive or negative).
func (c *attrCache) invalidate(path string) {
	c.st.remove(path)
}

// invalidatePrefix drops the cached entry for path AND every descendant
// (path + "/..."), across both tiers. Used when a directory is moved/removed
// as one operation (rename, recursive delete): the per-key invalidate would
// leave descendant entries behind, which then resurface as phantom
// "already exists" results when a directory of the same name is recreated.
func (c *attrCache) invalidatePrefix(path string) {
	c.st.removeMatching(subtreePred(path))
	if c.prefixRemover != nil {
		c.prefixRemover(path)
	}
}

// bumpParentEntryAdded refreshes a cached parent directory's attr after a child
// was created under it, instead of evicting it. Adding an entry advances the
// directory's mtime and ctime but leaves mode/uid/gid (what default_permissions
// checks) and nlink (regular-file/symlink children don't change a dir's link
// count) untouched — so the cached entry stays correct and the kernel's
// permission checks + the app's dir stats keep hitting cache instead of taking
// a full GetAttr per child. No-op when no positive entry is cached (the next
// Stat populates it). NOTE: only safe for file/symlink creates; a Mkdir bumps
// the parent's nlink, so callers keep evicting the parent there.
func (c *attrCache) bumpParentEntryAdded(path string) {
	e := c.st.get(path)
	if e == nil {
		return
	}
	ae, _ := e.value.(*attrEntry)
	if ae == nil || ae.negative || ae.attr == nil {
		return
	}
	updated := *ae.attr // copy; never mutate the pointer get() shares with readers
	now := c.now()
	updated.Mtime = uint64(now.Unix())
	updated.Mtimensec = uint32(now.Nanosecond())
	updated.Ctime = uint64(now.Unix())
	updated.Ctimensec = uint32(now.Nanosecond())
	ne := &attrEntry{attr: &updated, expiresAt: now.Add(c.attrTTL)}
	c.st.put(path, ne, attrEntrySize(path, ne))
}

// bumpSize optimistically updates a cached positive entry after a local write
// whose highest touched offset is writtenEnd (off+n): it grows Size to at least
// writtenEnd and advances Mtime to now, WITHOUT an RPC — the writer is
// authoritative on the file's new minimum size. It writes a fresh entry rather
// than mutating the shared *backend.Attr that get() hands out (whose contract forbids
// mutation). It is a no-op (returns false) when no positive entry is cached; the
// next Stat then populates it and later writes bump that. Returns true when an
// entry was updated, so the caller can stamp the path verified to keep the
// post-write GetAttr served from cache.
func (c *attrCache) bumpSize(path string, writtenEnd int64) bool {
	e := c.st.get(path)
	if e == nil {
		return false
	}
	ae, _ := e.value.(*attrEntry)
	if ae == nil || ae.negative || ae.attr == nil {
		return false
	}
	updated := *ae.attr // copy; never mutate the pointer get() shares with readers
	if writtenEnd > 0 && uint64(writtenEnd) > updated.Size {
		updated.Size = uint64(writtenEnd)
	}
	now := c.now()
	updated.Mtime = uint64(now.Unix())
	updated.Mtimensec = uint32(now.Nanosecond())
	ne := &attrEntry{attr: &updated, expiresAt: now.Add(c.attrTTL)}
	c.st.put(path, ne, attrEntrySize(path, ne))
	return true
}

// attrFixedOverheadBytes approximates the per-entry resident cost that does
// NOT scale with the path: the backend.Attr value, the attrEntry struct, the store
// entry struct, the container/list node, and amortised Go map bucket overhead.
const attrFixedOverheadBytes = 300

// attrEntrySize estimates the real in-memory footprint of a cached attr entry.
// The path string is stored TWICE (the map key and the store entry's key
// copy), so it dominates the cost for the long, distinct paths that drive the
// leak (git objects, sqlite temp files). The old flat 256 B ignored the key
// entirely and systematically under-counted, letting the maps grow in real
// heap far past the byte budget without eviction (issue #118).
func attrEntrySize(path string, _ *attrEntry) int {
	return 2*len(path) + attrFixedOverheadBytes
}

// persistedAttr is the on-disk shape. Negative entries persist false
// for attr; gob-zero for missing fields is fine.
type persistedAttr struct {
	Attr      *backend.Attr
	Negative  bool
	ExpiresAt int64 // unix nanos
	Version   uint64
}

// newAttrCacheWithPersist constructs an attrCache that fronts the
// persist tier when p is non-nil. Loader gob-decodes attr bytes from
// the attr bucket; Putter gob-encodes and writes through; Remover
// deletes. nil p falls back to newAttrCache (memory-only).
func newAttrCacheWithPersist(acct *accountant, attrTTL, negativeTTL time.Duration, now func() time.Time, p *persist.Persist, rec metrics.Recorder) *attrCache {
	if now == nil {
		now = time.Now
	}
	if p == nil {
		return newAttrCache(acct, attrTTL, negativeTTL, now, rec)
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
		return ae, attrEntrySize(key, ae), true
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
	c.st = newStoreWithPersist(acct, loader, putter, remover, "attr", rec)
	c.prefixRemover = func(prefix string) { _ = p.DeleteAttrPrefix(prefix) }
	return c
}
