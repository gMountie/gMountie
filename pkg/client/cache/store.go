package cache

import "sync"

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
type store struct {
	mu      sync.RWMutex
	entries map[string]*entry
	acct    *accountant
	loader  Loader
	putter  Putter
	remover Remover
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

func newStore(acct *accountant) *store {
	return &store{entries: make(map[string]*entry), acct: acct}
}

func newStoreWithLoader(acct *accountant, loader Loader, putter Putter) *store {
	return &store{entries: make(map[string]*entry), acct: acct, loader: loader, putter: putter}
}

func newStoreWithPersist(acct *accountant, loader Loader, putter Putter, remover Remover) *store {
	return &store{entries: make(map[string]*entry), acct: acct, loader: loader, putter: putter, remover: remover}
}

// get returns the entry for key (or nil if absent). Promotes to MRU
// on hit. On memory miss with a loader configured, falls through:
// asks the loader, and on hit promotes the loaded value into the
// memory tier via put (which write-throughs to the putter — that's
// fine; promotion is structurally identical to insertion from the
// loader's point of view).
func (s *store) get(key string) *entry {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if ok {
		s.acct.touch(e)
		return e
	}
	if s.loader == nil {
		return nil
	}
	value, size, hit := s.loader(key)
	if !hit {
		return nil
	}
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
	if s.putter != nil {
		s.putter(key, value, size)
	}
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
