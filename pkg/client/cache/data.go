package cache

import (
	"strconv"
	"strings"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
)

// dataCache stores file content as fixed-size chunks keyed by
// (path, chunkIndex). No TTL: entries are valid until explicitly
// invalidated by a Write/SetAttr(size)/Unlink/Rename on the path, or
// evicted under the global byte cap.
type dataCache struct {
	st                  *store
	chunkSizeBytes      int
	persistCleaner      func(path string)
	persistRangeCleaner func(path string, firstIdx, lastIdx int)
}

func newDataCache(acct *accountant, chunkSizeBytes int) *dataCache {
	return &dataCache{st: newStore(acct, "data"), chunkSizeBytes: chunkSizeBytes}
}

// ChunkSize returns the configured chunk size in bytes.
func (c *dataCache) ChunkSize() int { return c.chunkSizeBytes }

// chunkKey returns the cache key for (path, chunkIndex). path is
// the FUSE-side path; chunkIndex is the zero-based chunk number.
// The "\x00" separator is impossible in valid file paths and so
// is a safe delimiter. Uses strconv.AppendInt to avoid the
// fmt.Sprintf allocation on the hot data-cache access path.
func chunkKey(path string, chunkIndex int) string {
	b := make([]byte, 0, len(path)+1+10)
	b = append(b, path...)
	b = append(b, 0)
	b = strconv.AppendInt(b, int64(chunkIndex), 10)
	return string(b)
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
	v, _ := e.value.([]byte) // data store only holds []byte
	return v
}

// put stores chunk bytes under (path, chunkIndex). data is stored by
// reference — caller is responsible for not mutating it afterwards.
func (c *dataCache) put(path string, chunkIndex int, data []byte) {
	c.st.put(chunkKey(path, chunkIndex), data, len(data))
}

// invalidatePath removes every chunk cached under any chunkIndex for
// the given path. Called by Write/SetAttr(size)/Unlink/Rename in backend.go.
func (c *dataCache) invalidatePath(path string) {
	prefix := path + "\x00"
	c.st.removeMatching(func(k string) bool {
		return strings.HasPrefix(k, prefix)
	})
	if c.persistCleaner != nil {
		c.persistCleaner(path)
	}
}

// invalidateRange removes chunks overlapping [off, off+size) for path.
// Used by Write, Allocate and CopyFileRange — they only need to
// invalidate the chunks the mutation touches.
func (c *dataCache) invalidateRange(path string, off, size int64) {
	if size <= 0 {
		return
	}
	first := int(off / int64(c.chunkSizeBytes))
	last := int((off + size - 1) / int64(c.chunkSizeBytes))
	for i := first; i <= last; i++ {
		c.st.remove(chunkKey(path, i))
	}
	if c.persistRangeCleaner != nil {
		c.persistRangeCleaner(path, first, last)
	}
}

// newDataCacheWithPersist constructs a dataCache that fronts persist's
// content-addressable chunk store. nil p falls back to newDataCache.
//
// Chunks go through WriteChunk/ReadChunk for bytes and
// PutChunkRef/GetChunkRef for index. invalidatePath uses the bulk
// persist.InvalidatePathChunks path rather than the per-key Remover
// (one bbolt cursor walk vs one txn per chunk).
func newDataCacheWithPersist(acct *accountant, chunkSizeBytes int, p *persist.Persist) *dataCache {
	if p == nil {
		return newDataCache(acct, chunkSizeBytes)
	}
	c := &dataCache{chunkSizeBytes: chunkSizeBytes}
	loader := func(key string) (any, int, bool) {
		path, idx, ok := parseChunkKey(key)
		if !ok {
			return nil, 0, false
		}
		ref, ok, err := p.GetChunkRef(path, idx)
		if err != nil || !ok {
			return nil, 0, false
		}
		data, err := p.ReadChunk(ref.Hash)
		if err != nil {
			return nil, 0, false
		}
		return data, len(data), true
	}
	putter := func(key string, value any, _ int) {
		path, idx, ok := parseChunkKey(key)
		if !ok {
			return
		}
		data, _ := value.([]byte) // data store only holds []byte
		hash, dedup, err := p.WriteChunk(data)
		if err != nil {
			return
		}
		if dedup {
			metrics.CacheDedupeHit()
		}
		_ = p.PutChunkRef(path, idx, persist.ChunkRef{Hash: hash, Size: uint32(len(data))})
	}
	// Per-key Remover is a no-op for data: the bulk persistCleaner and
	// persistRangeCleaner drive index+refcount invalidation efficiently.
	// The memory tier's per-key remove is still cheap (map delete).
	c.st = newStoreWithPersist(acct, loader, putter, func(string) {}, "data")
	c.persistCleaner = func(path string) { _ = p.InvalidatePathChunks(path) }
	c.persistRangeCleaner = func(path string, firstIdx, lastIdx int) {
		_ = p.InvalidateChunkRange(path, firstIdx, lastIdx)
	}
	return c
}

// parseChunkKey is the inverse of chunkKey: splits "path\x00idx" back
// into (path, idx). Returns ok=false on malformed keys.
func parseChunkKey(key string) (string, int, bool) {
	i := strings.IndexByte(key, 0)
	if i < 0 {
		return "", 0, false
	}
	idx, err := strconv.Atoi(key[i+1:])
	if err != nil {
		return "", 0, false
	}
	return key[:i], idx, true
}
