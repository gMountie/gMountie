package cache

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"go.gmountie.dev/gmountie/pkg/client/backend/cache/persist"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// dataPersistQueueDepth bounds the async write-back queue for chunk persist.
// Each queued job references a chunk already held in the memory tier (no extra
// copy), so the cost is ~queue-of-pointers. Deep enough to absorb readahead
// bursts; when it overflows (disk can't keep up with streaming) the persist is
// dropped — the chunk stays in memory, a future cache miss at worst. See
// store.startAsyncPersist.
const dataPersistQueueDepth = 256

// dataCache stores file content as fixed-size chunks keyed by
// (path, chunkIndex). No TTL: entries are valid until explicitly
// invalidated by a Write/SetAttr(size)/Unlink/Rename on the path, or
// evicted under the global byte cap.
type dataCache struct {
	st                  *store
	chunkSizeBytes      int
	persistCleaner      func(path string)
	persistRangeCleaner func(path string, firstIdx, lastIdx int)
	// curGenFn returns a path's current invalidation generation (nil for a
	// memory-only data cache). Callers that sample bytes from a lower tier
	// capture it BEFORE the sample and thread it into putGen so a deferred
	// persist whose path was invalidated since the sample is dropped.
	curGenFn func(path string) uint64
	// rec is the injected metrics sink (never nil — NewCachedBackend defaults it
	// to metrics.NopRecorder{}). Used to record the content-addressable dedupe hit
	// in the persist write-through.
	rec metrics.Recorder
}

func newDataCache(acct *accountant, chunkSizeBytes int, rec metrics.Recorder) *dataCache {
	return &dataCache{st: newStore(acct, "data", rec), chunkSizeBytes: chunkSizeBytes, rec: rec}
}

// ChunkSize returns the configured chunk size in bytes.
func (c *dataCache) ChunkSize() int { return c.chunkSizeBytes }

// Close flushes and stops the async persist worker (if started). Idempotent.
// cachedBackend.Close must call this before closing the persist tier so the
// worker's in-flight WriteChunk/PutChunkRef finish against an open DB.
func (c *dataCache) Close() {
	if c.st != nil {
		c.st.Close()
	}
}

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
	c.putGen(path, chunkIndex, data, c.currentGen(path))
}

// putGen caches a chunk, stamping the persist job with gen — the path's
// invalidation generation captured by the caller at byte-sample time. The async
// write-through (onPersist) drops or undoes the job if the generation has since
// advanced, so a chunk sampled before an invalidation cannot be resurrected on
// disk after it.
func (c *dataCache) putGen(path string, chunkIndex int, data []byte, gen uint64) {
	c.st.putGen(chunkKey(path, chunkIndex), data, len(data), gen)
}

// currentGen returns path's current invalidation generation, or 0 for a
// memory-only data cache (no persist tier, so no resurrection to guard against).
func (c *dataCache) currentGen(path string) uint64 {
	if c.curGenFn == nil {
		return 0
	}
	return c.curGenFn(path)
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
func newDataCacheWithPersist(acct *accountant, chunkSizeBytes int, p *persist.Persist, rec metrics.Recorder) *dataCache {
	if p == nil {
		return newDataCache(acct, chunkSizeBytes, rec)
	}
	return newDataCacheWithChunkPersist(acct, chunkSizeBytes, p, rec)
}

// chunkPersist is the persist surface the data cache fronts. *persist.Persist
// satisfies it; tests inject fakes (e.g. one whose invalidation fails) to
// exercise the poison path deterministically.
type chunkPersist interface {
	GetChunkRef(path string, chunkIndex int) (persist.ChunkRef, bool, error)
	ReadChunk(hash [16]byte) ([]byte, error)
	WriteChunk(data []byte) (hash [16]byte, dedup bool, err error)
	PutChunkRef(path string, chunkIndex int, ref persist.ChunkRef) error
	InvalidatePathChunks(path string) error
	InvalidateChunkRange(path string, firstIdx, lastIdx int) error
}

// newDataCacheWithChunkPersist builds the persist-backed data cache over the
// chunkPersist seam. p must be non-nil.
func newDataCacheWithChunkPersist(acct *accountant, chunkSizeBytes int, p chunkPersist, rec metrics.Recorder) *dataCache {
	c := &dataCache{chunkSizeBytes: chunkSizeBytes, rec: rec}
	// poisoned tracks paths whose persist invalidation failed: while a path is
	// poisoned the disk tier is not authoritative for it, so the loader refuses
	// to serve disk chunks (avoids serving a stale hit after a write whose
	// invalidation was lost — the #113 family). Only a later SUCCESSFUL
	// invalidation clears the poison; write-through must not (persist is async,
	// so a deferred or disk-promoted write-through could otherwise resurrect
	// stale-serving — see the putter's NOTE).
	var poisoned sync.Map // path -> struct{}
	// gen tracks each path's invalidation generation. The cleaners below bump it
	// BEFORE clearing the disk tier; a queued persist carries the generation it
	// saw at enqueue (persistJob.gen), and the write-through (onPersist) drops or
	// undoes a write whose generation has since advanced. This closes the
	// async-persist stale-read race: a deferred persist can no longer land a
	// chunk on disk after its invalidation already ran.
	// gens maps path -> *atomic.Uint64 generation counter. globalGen is a
	// process-wide monotonic source: a bump stamps the path's counter with the
	// next global value rather than a per-path +1, so generations are strictly
	// increasing across the whole cache. That is what makes PRUNING a path's
	// entry sound (the cleaners delete it to bound the map, see persistCleaner):
	// a counter re-created after a delete is seeded from the current high-water
	// (globalGen.Load()), so it can never alias a generation an in-flight
	// persistJob captured before the delete. onPersist compares generations by
	// EQUALITY only, and a re-created counter can equal an old job.gen ONLY when
	// no bump occurred between that job's enqueue and its persist — exactly when
	// writing through is correct. Any intervening invalidation advanced globalGen
	// strictly past the job's value, so the reseed mismatches and the stale write
	// is dropped (served from memory — safe). A stale chunk can therefore never be
	// resurrected on disk (the #141 invariant) even though the map is now pruned.
	var (
		gens      sync.Map // path -> *atomic.Uint64, lock-free on the read hot path
		globalGen atomic.Uint64
	)
	genCounter := func(path string) *atomic.Uint64 {
		if v, ok := gens.Load(path); ok { // hot path: no allocation, no lock
			c, _ := v.(*atomic.Uint64)
			return c
		}
		// First touch (or first after a prune): seed from the high-water so a
		// re-created entry cannot collide with a pre-delete in-flight job's gen.
		seed := new(atomic.Uint64)
		seed.Store(globalGen.Load())
		actual, _ := gens.LoadOrStore(path, seed) // concurrent first-touchers converge
		c, _ := actual.(*atomic.Uint64)
		return c
	}
	curGen := func(path string) uint64 { return genCounter(path).Load() }
	bumpGen := func(path string) { genCounter(path).Store(globalGen.Add(1)) }
	loader := func(key string) (any, int, bool) {
		path, idx, ok := parseChunkKey(key)
		if !ok {
			return nil, 0, false
		}
		if _, bad := poisoned.Load(path); bad {
			return nil, 0, false // a prior invalidation failed; never serve disk for this path
		}
		ref, ok, err := p.GetChunkRef(path, idx)
		if err != nil || !ok {
			return nil, 0, false
		}
		data, err := p.ReadChunk(ref.Hash)
		if err != nil {
			return nil, 0, false
		}
		if len(data) != int(ref.Size) {
			// Torn/partial chunk (crash between data write and index sync).
			// Treat as a miss so the caller refetches; the subsequent WriteChunk
			// with correct bytes repairs the file (Task 1 dedup-size check).
			return nil, 0, false
		}
		return data, len(data), true
	}
	// onPersist is the invalidation-aware write-through: the store calls it from
	// the async worker with the full job (including the generation captured at
	// enqueue). It drops a job whose path was invalidated since enqueue, and
	// undoes a write that raced an invalidation, so a deferred persist can never
	// resurrect a stale chunk on disk.
	onPersist := func(job persistJob) {
		path, idx, ok := parseChunkKey(job.key)
		if !ok {
			return
		}
		// Drop if the path was invalidated after this job was queued: writing now
		// would re-add a chunk the invalidation already removed from disk.
		if curGen(path) != job.gen {
			return
		}
		data, _ := job.value.([]byte) // data store only holds []byte
		hash, dedup, err := p.WriteChunk(data)
		if err != nil {
			log.Log.Warn("persist chunk write-through failed; serving from memory only",
				zap.String("path", path), zap.Int("idx", idx), zap.Error(err))
			return
		}
		if dedup {
			c.rec.CacheDedupeHitInc()
		}
		if err := p.PutChunkRef(path, idx, persist.ChunkRef{Hash: hash, Size: uint32(len(data))}); err != nil {
			log.Log.Warn("persist chunk-ref write-through failed; serving from memory only",
				zap.String("path", path), zap.Int("idx", idx), zap.Error(err))
			return
		}
		// TOCTOU close: an invalidation may have run between the generation check
		// above and the ref write just now — its disk clean would have executed
		// before our PutChunkRef, leaving our (now stale) chunk behind. If the
		// generation moved, undo our write so the disk tier cannot serve it; if
		// the undo itself fails, poison the path so the loader refuses the disk
		// tier until a later successful invalidation.
		//
		// NOTE: a successful write-through never CLEARS poison — only a
		// successful invalidation (persistCleaner/persistRangeCleaner) does, the
		// only event that definitively removes stale disk data.
		if curGen(path) != job.gen {
			if err := p.InvalidateChunkRange(path, idx, idx); err != nil {
				poisoned.Store(path, struct{}{})
				log.Log.Warn("persist write-vs-invalidation race undo failed; poisoning path",
					zap.String("path", path), zap.Int("idx", idx), zap.Error(err))
			}
		}
	}
	// Per-key Remover is a no-op for data: the bulk persistCleaner and
	// persistRangeCleaner drive index+refcount invalidation efficiently.
	// The memory tier's per-key remove is still cheap (map delete). The putter
	// is nil because data write-through goes through onPersist so it can honour
	// the per-path generation.
	c.st = newStoreWithPersist(acct, loader, nil, func(string) {}, "data", rec)
	c.curGenFn = curGen
	c.st.genOf = func(key string) uint64 {
		path, _, ok := parseChunkKey(key)
		if !ok {
			return 0
		}
		return curGen(path)
	}
	c.st.onPersist = onPersist
	// Chunk persist does a per-chunk fsync (persist.WriteChunk); run it on a
	// background worker so the read path never blocks on disk. cachedBackend.Close
	// flushes this before closing the persist tier.
	c.st.startAsyncPersist(dataPersistQueueDepth)
	c.persistCleaner = func(path string) {
		// Advance the generation BEFORE clearing disk so a persist queued with the
		// old generation is dropped (or, if mid-flight, undone) by onPersist.
		bumpGen(path)
		if err := p.InvalidatePathChunks(path); err != nil {
			poisoned.Store(path, struct{}{})
			log.Log.Warn("persist path invalidation failed; poisoning path to avoid serving stale cache",
				zap.String("path", path), zap.Error(err))
			return
		}
		poisoned.Delete(path)
		// Prune the path's generation entry now that its whole disk tier is
		// cleared: this reclaims the entry on full-path invalidation, bounding the
		// write/delete-churn growth (the #118 shape — paths repeatedly written and
		// invalidated). Sound because the bumpGen above already advanced globalGen
		// past any in-flight job's captured gen, so a later re-create seeds high
		// and those jobs still drop. Success path only — a poisoned path keeps its
		// counter (the error branch returned above). NOTE: a read-only walk over
		// many distinct, never-invalidated paths still accumulates one entry each
		// (genCounter creates them on read-miss); fully bounding that needs an
		// eviction-time prune (sound under the same high-water seed) and is a
		// follow-up.
		gens.Delete(path)
	}
	c.persistRangeCleaner = func(path string, firstIdx, lastIdx int) {
		// Advance the generation BEFORE clearing disk (see persistCleaner).
		bumpGen(path)
		if err := p.InvalidateChunkRange(path, firstIdx, lastIdx); err != nil {
			poisoned.Store(path, struct{}{})
			log.Log.Warn("persist range invalidation failed; poisoning path to avoid serving stale cache",
				zap.String("path", path), zap.Int("first", firstIdx), zap.Int("last", lastIdx), zap.Error(err))
			return
		}
		poisoned.Delete(path)
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
