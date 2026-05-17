package cache

import (
	"fmt"
	"strings"
)

// dataCache stores file content as fixed-size chunks keyed by
// (path, chunkIndex). No TTL: entries are valid until explicitly
// invalidated by a Write/Truncate/Unlink/Rename on the path, or
// evicted under the global byte cap.
type dataCache struct {
	st             *store
	chunkSizeBytes int
}

func newDataCache(acct *accountant, chunkSizeBytes int) *dataCache {
	return &dataCache{st: newStore(acct), chunkSizeBytes: chunkSizeBytes}
}

// ChunkSize returns the configured chunk size in bytes.
func (c *dataCache) ChunkSize() int { return c.chunkSizeBytes }

// chunkKey returns the cache key for (path, chunkIndex). path is
// the FUSE-side path; chunkIndex is the zero-based chunk number.
// The "\x00" separator is impossible in valid file paths and so
// is a safe delimiter.
func chunkKey(path string, chunkIndex int) string {
	return fmt.Sprintf("%s\x00%d", path, chunkIndex)
}

// get returns the cached chunk for (path, chunkIndex), or nil on miss.
// Returned slice is the cached buffer; callers MUST NOT mutate it.
// (Read-through composition in backend.go always copies into the
// destination buffer before returning to the kernel.)
func (c *dataCache) get(path string, chunkIndex int) []byte {
	e := c.st.get(chunkKey(path, chunkIndex))
	if e == nil {
		return nil
	}
	return e.value.([]byte)
}

// put stores chunk bytes under (path, chunkIndex). data is stored by
// reference — caller is responsible for not mutating it afterwards.
func (c *dataCache) put(path string, chunkIndex int, data []byte) {
	c.st.put(chunkKey(path, chunkIndex), data, len(data))
}

// invalidatePath removes every chunk cached under any chunkIndex for
// the given path. Called by Write/Truncate/Unlink/Rename in backend.go.
func (c *dataCache) invalidatePath(path string) {
	prefix := path + "\x00"
	c.st.removeMatching(func(k string) bool {
		return strings.HasPrefix(k, prefix)
	})
}

// invalidateRange removes chunks overlapping [off, off+size) for path.
// Used by Write (it only needs to invalidate chunks the write touches)
// and Truncate (chunks past the new size).
func (c *dataCache) invalidateRange(path string, off, size int64) {
	if size <= 0 {
		return
	}
	first := int(off / int64(c.chunkSizeBytes))
	last := int((off + size - 1) / int64(c.chunkSizeBytes))
	for i := first; i <= last; i++ {
		c.st.remove(chunkKey(path, i))
	}
}
