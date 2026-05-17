package cache

import (
	"time"

	"gmountie/pkg/client/io"
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
	return &dirCache{st: newStore(acct), now: now, dirTTL: dirTTL}
}

// get returns (entries, true) on a fresh hit, (nil, false) on miss or
// expiry. Returned slice is a copy; callers may not mutate the cached
// view.
func (c *dirCache) get(path string) ([]io.DirEntry, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false
	}
	de := e.value.(*dirEntry)
	if c.now().After(de.expiresAt) {
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
	c.st.put(path, de, dirEntrySize(de))
}

func (c *dirCache) invalidate(path string) { c.st.remove(path) }

// dirEntrySize estimates the in-memory footprint of a cached listing.
// 64 bytes overhead per DirEntry is a generous round figure that
// covers the struct + name string header.
func dirEntrySize(de *dirEntry) int { return 32 + 64*len(de.entries) }
