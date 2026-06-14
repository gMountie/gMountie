package cache

import (
	"bytes"
	"encoding/gob"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

type dirEntry struct {
	entries   []io.DirEntry
	expiresAt time.Time
}

type dirCache struct {
	st     *store
	now    func() time.Time
	dirTTL time.Duration
}

func newDirCache(acct *accountant, dirTTL time.Duration, now func() time.Time) *dirCache {
	if now == nil {
		now = time.Now
	}
	return &dirCache{st: newStore(acct, "dir"), now: now, dirTTL: dirTTL}
}

// get returns (entries, true) on a fresh hit, (nil, false) on miss or
// expiry. Returned slice is a copy; callers may not mutate the cached
// view.
func (c *dirCache) get(path string) ([]io.DirEntry, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false
	}
	de, _ := e.value.(*dirEntry) // dir store only holds *dirEntry
	// dirTTL=0 means "never expire on time alone"; Subscribe push or
	// explicit invalidation are the only eviction signals in that mode.
	if c.dirTTL > 0 && c.now().After(de.expiresAt) {
		c.st.remove(path)
		return nil, false
	}
	out := make([]io.DirEntry, len(de.entries))
	copy(out, de.entries)
	return out, true
}

func (c *dirCache) put(path string, entries []io.DirEntry) {
	stored := make([]io.DirEntry, len(entries))
	copy(stored, entries)
	de := &dirEntry{entries: stored, expiresAt: c.now().Add(c.dirTTL)}
	c.st.put(path, de, dirEntrySize(path, de))
}

func (c *dirCache) invalidate(path string) { c.st.remove(path) }

// dirPerEntryOverheadBytes is the non-name resident cost of one cached
// io.DirEntry (struct fields + the name string header).
const dirPerEntryOverheadBytes = 64

// dirEntrySize estimates the real in-memory footprint of a cached listing.
// It accounts for the keyed path (stored twice — map key + entry key copy)
// and the actual entry-name bytes, not just a flat per-entry constant, so a
// directory of many long names is sized close to its true heap cost (#118).
func dirEntrySize(path string, de *dirEntry) int {
	n := 2*len(path) + 64 // path stored twice + dirEntry/store struct overhead
	for i := range de.entries {
		n += len(de.entries[i].Name) + dirPerEntryOverheadBytes
	}
	return n
}

// persistedDir is the on-disk shape of a cached listing.
type persistedDir struct {
	Entries   []io.DirEntry
	ExpiresAt int64 // unix nanos
}

// newDirCacheWithPersist constructs a dirCache that fronts the persist
// tier when p is non-nil. nil p falls back to newDirCache.
func newDirCacheWithPersist(acct *accountant, dirTTL time.Duration, now func() time.Time, p *persist.Persist) *dirCache {
	if now == nil {
		now = time.Now
	}
	if p == nil {
		return newDirCache(acct, dirTTL, now)
	}
	c := &dirCache{now: now, dirTTL: dirTTL}
	loader := func(key string) (any, int, bool) {
		raw, ok, err := p.GetDirBytes(key)
		if err != nil || !ok {
			return nil, 0, false
		}
		var pd persistedDir
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&pd); err != nil {
			return nil, 0, false
		}
		de := &dirEntry{entries: pd.Entries, expiresAt: time.Unix(0, pd.ExpiresAt)}
		return de, dirEntrySize(key, de), true
	}
	putter := func(key string, value any, _ int) {
		de, _ := value.(*dirEntry) // dir store only holds *dirEntry
		pd := persistedDir{Entries: de.entries, ExpiresAt: de.expiresAt.UnixNano()}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(pd); err != nil {
			return
		}
		_ = p.PutDirBytes(key, buf.Bytes())
	}
	remover := func(key string) { _ = p.DeleteDirBytes(key) }
	c.st = newStoreWithPersist(acct, loader, putter, remover, "dir")
	return c
}
