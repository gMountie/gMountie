package cache

import "strings"

// subtreePred reports whether a path-keyed cache key is `path` itself or a
// descendant (`path + "/..."`). It deliberately excludes siblings that merely
// share a string prefix — "a" must not match "ab", only "a" and "a/...". Used
// for subtree invalidation on a directory rename / recursive delete.
func subtreePred(path string) func(key string) bool {
	slash := path + "/"
	return func(k string) bool {
		return k == path || strings.HasPrefix(k, slash)
	}
}

// invalidatePrefix removes every cached chunk under path and its descendants.
// Data keys are chunkKey(p, idx) == p + "\x00" + idx, so the path's own chunks
// match the "\x00" boundary and descendants match the "/" boundary — neither
// matches a sibling like "ab". The disk twins of memory-resident chunks are
// dropped via the store's remover; disk-only chunks under the subtree are a
// known residual (a rewrite of a recreated path self-heals via the per-path
// generation; a read-without-rewrite of a recreated path is the uncovered case).
func (c *dataCache) invalidatePrefix(path string) {
	ownChunks := path + "\x00"
	descendants := path + "/"
	c.st.removeMatching(func(k string) bool {
		return strings.HasPrefix(k, ownChunks) || strings.HasPrefix(k, descendants)
	})
}

// invalidatePrefix drops the xattr-name lists for path and its descendants
// (memory-only cache — no disk tier).
func (c *xattrCache) invalidatePrefix(path string) {
	c.st.removeMatching(subtreePred(path))
}

// invalidatePrefix drops cached getxattr values for path and its descendants.
func (c *getXAttrCache) invalidatePrefix(path string) {
	pred := subtreePred(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.m {
		if pred(k) {
			delete(c.m, k)
		}
	}
}

// invalidatePrefix drops cached Access decisions for path and its descendants.
func (c *accessCache) invalidatePrefix(path string) {
	pred := subtreePred(path)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.m {
		if pred(k) {
			delete(c.m, k)
		}
	}
}
