package cache

import (
	"sync"

	"go.gmountie.dev/gmountie/pkg/client/metrics"
)

// store is a generic key→entry map that defers byte-accounting and
// eviction to a shared accountant. The store's API is intentionally
// thin; callers (attrCache, dirCache, dataCache) wrap it with their
// own TTL / type semantics.
//
// Concurrency: store.mu protects the map; accountant.mu protects the
// global LRU + byte budget. When both are held, the canonical order is
// accountant.mu OUTER, store.mu INNER — set by the eviction callback
// (accountant.evictLocked invokes store.removeKey while holding
// accountant.mu). Mutating-store paths (put, remove, removeMatching)
// therefore release store.mu BEFORE calling into the accountant; the
// 201a53b fix in store.put's replace path locks that invariant in.
//
// Persistence hooks (Loader, Putter, Remover) added in Sub-spec C let
// the store front a persistent backing tier. They are all optional —
// nil means "memory-only", matching the Sub-spec B behaviour.
//
// The eviction callback (removeKey) does NOT call Remover: memory
// eviction must leave on-disk entries alone per the Sub-spec C tier
// contract.
//
// cacheType is one of "attr", "dir", "data" — used only for metrics labels.
type store struct {
	mu        sync.RWMutex
	entries   map[string]*entry
	acct      *accountant
	loader    Loader
	putter    Putter
	remover   Remover
	cacheType string

	// Async write-back persist (enabled per-store via startAsyncPersist;
	// used by the data store, whose putter does per-chunk fsync). When
	// asyncCh is non-nil, put() enqueues the putter to persistWorker
	// instead of calling it inline, so a slow disk write never blocks the
	// read path. nil => synchronous putter (attr/dir, and memory-only).
	asyncCh   chan persistJob
	asyncStop chan struct{}
	asyncDone chan struct{}
	closeOnce sync.Once

	// genOf and onPersist make async write-back invalidation-aware (set by the
	// data cache; nil for synchronous stores). genOf returns the current
	// invalidation generation for a key's path, captured into persistJob.gen at
	// enqueue. onPersist, when set, replaces the plain putter in the worker: it
	// receives the full job (including gen) so it can drop or undo a write whose
	// path was invalidated after the job was queued.
	genOf     func(key string) uint64
	onPersist func(job persistJob)
}

// persistJob is one deferred write-through enqueued by put when async
// persist is enabled. gen is the path's invalidation generation captured at
// enqueue time (via store.genOf): the worker compares it against the current
// generation and drops the job if the path was invalidated since enqueue, so a
// deferred persist cannot resurrect a chunk on disk after its invalidation
// already ran (the async-persist stale-read race).
type persistJob struct {
	key   string
	value any
	size  int
	gen   uint64
}

// Loader returns a value loaded from a lower tier (disk persist).
// hit=false means the lower tier didn't have it.
type Loader func(key string) (value any, size int, hit bool)

// Putter writes through to a lower tier. Called synchronously after a
// store.put. nil putter disables write-through.
type Putter func(key string, value any, size int)

// Remover invalidates a key in the lower tier. Called by remove and
// removeMatching. nil remover disables persistence-side invalidation.
// NOT called by the accountant's eviction path — memory eviction
// must leave the on-disk entry intact.
type Remover func(key string)

func newStore(acct *accountant, cacheType string) *store {
	return &store{entries: make(map[string]*entry), acct: acct, cacheType: cacheType}
}

func newStoreWithPersist(acct *accountant, loader Loader, putter Putter, remover Remover, cacheType string) *store {
	return &store{entries: make(map[string]*entry), acct: acct, loader: loader, putter: putter, remover: remover, cacheType: cacheType}
}

// get returns the entry for key (or nil if absent). Promotes to MRU
// on hit. On memory miss with a loader configured, falls through:
// asks the loader, and on hit promotes the loaded value into the
// memory tier via put (which write-throughs to the putter — that's
// fine; promotion is structurally identical to insertion from the
// loader's point of view).
//
// Fires metrics.CacheHit("memory", ...) on a memory tier hit,
// metrics.CacheHit("disk", ...) on a loader (disk) hit, and
// metrics.CacheMiss(...) when both tiers missed.
func (s *store) get(key string) *entry {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if ok {
		s.acct.touch(e)
		metrics.CacheHit("memory", s.cacheType)
		return e
	}
	if s.loader == nil {
		// Memory-only store: a miss here is the final answer.
		// Misses for memory-only stores are reported by the sub-cache
		// callers in backend.go (at the fall-through-to-inner sites)
		// so that expiry-invalidated entries also count correctly.
		return nil
	}
	value, size, hit := s.loader(key)
	if !hit {
		// Both tiers missed; caller (backend.go) bumps CacheMiss.
		return nil
	}
	metrics.CacheHit("disk", s.cacheType)
	s.put(key, value, size)
	s.mu.RLock()
	e = s.entries[key]
	s.mu.RUnlock()
	return e
}

// put inserts or replaces the entry for key. If a prior entry existed,
// it is removed from the accountant first (so bytes don't double-count).
// Write-throughs to the putter after the memory work, outside the
// store lock so a slow disk write does not block reads.
func (s *store) put(key string, value any, size int) {
	e := &entry{key: key, value: value, size: size, remove: s.removeKey}
	s.mu.Lock()
	prior, hadPrior := s.entries[key]
	s.entries[key] = e
	s.mu.Unlock()
	if hadPrior {
		s.acct.remove(prior)
	}
	s.acct.insert(e)
	switch {
	case s.asyncCh != nil:
		// Async write-back: hand the (fsync-heavy) putter to the background
		// worker so the read path never blocks on disk. Drop on a full queue
		// — the entry stays in the memory tier, so losing the disk copy is
		// just a future cache miss, never data loss. asyncCh is never closed
		// (Close signals via asyncStop), so this send can't panic.
		job := persistJob{key: key, value: value, size: size}
		if s.genOf != nil {
			// Stamp the path's current invalidation generation so the worker can
			// drop this job if an invalidation lands before it runs.
			job.gen = s.genOf(key)
		}
		select {
		case s.asyncCh <- job:
		default:
			metrics.CachePersistDropped()
		}
	case s.putter != nil:
		s.putter(key, value, size)
	}
}

// startAsyncPersist enables async write-back for this store: put() enqueues the
// putter onto a bounded background worker instead of calling it inline, so a
// slow (fsync-heavy) disk write never blocks the read path. depth bounds the
// queue; an overflowing queue drops the persist (the entry stays in the memory
// tier — a lost disk copy is just a future miss). No-op if the store has no
// putter or async is already started. Call Close to flush+stop the worker.
func (s *store) startAsyncPersist(depth int) {
	if (s.putter == nil && s.onPersist == nil) || s.asyncCh != nil {
		return
	}
	if depth < 1 {
		depth = 1
	}
	s.asyncCh = make(chan persistJob, depth)
	s.asyncStop = make(chan struct{})
	s.asyncDone = make(chan struct{})
	go s.persistWorker()
}

// persistWorker drains the async persist queue, calling the putter for each
// job. On stop it drains whatever is still buffered (flush) and exits.
func (s *store) persistWorker() {
	defer close(s.asyncDone)
	for {
		select {
		case job := <-s.asyncCh:
			s.runPersist(job)
		case <-s.asyncStop:
			for {
				select {
				case job := <-s.asyncCh:
					s.runPersist(job)
				default:
					return
				}
			}
		}
	}
}

// runPersist executes one queued persist job. When onPersist is set (the data
// cache's invalidation-aware write-through), it receives the full job so it can
// honour persistJob.gen; otherwise the plain putter is called.
func (s *store) runPersist(job persistJob) {
	if s.onPersist != nil {
		s.onPersist(job)
		return
	}
	s.putter(job.key, job.value, job.size)
}

// Close stops the async persist worker (if started) after flushing buffered
// jobs. Idempotent; a no-op for synchronous (memory-only or inline-putter)
// stores. Must be called before the underlying persist tier is closed.
func (s *store) Close() {
	s.closeOnce.Do(func() {
		if s.asyncStop != nil {
			close(s.asyncStop)
			<-s.asyncDone
		}
	})
}

// remove deletes the entry for key from the memory tier and forwards
// to the configured Remover (which clears the persisted entry).
// Idempotent at both tiers.
func (s *store) remove(key string) {
	s.mu.Lock()
	e, ok := s.entries[key]
	if ok {
		delete(s.entries, key)
	}
	s.mu.Unlock()
	if ok {
		s.acct.remove(e)
	}
	if s.remover != nil {
		s.remover(key)
	}
}

// removeKey is the eviction callback invoked by the accountant while
// holding accountant.mu. By design it does NOT call the Remover —
// memory eviction must leave the on-disk entry intact (the tier
// contract from Sub-spec C).
func (s *store) removeKey(key string) {
	s.mu.Lock()
	delete(s.entries, key)
	s.mu.Unlock()
}

// removeMatching removes every entry whose key satisfies pred. Used
// by data cache's invalidate-by-path and dir's parent invalidations.
// Forwards each matched key to the Remover after the accountant work.
func (s *store) removeMatching(pred func(key string) bool) {
	s.mu.Lock()
	matched := make([]*entry, 0)
	for k, e := range s.entries {
		if pred(k) {
			matched = append(matched, e)
			delete(s.entries, k)
		}
	}
	s.mu.Unlock()
	for _, e := range matched {
		s.acct.remove(e)
		if s.remover != nil {
			s.remover(e.key)
		}
	}
}

// size returns the number of entries (for tests).
func (s *store) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
