package cache

import "sync"

// store is a generic key→entry map that defers byte-accounting and
// eviction to a shared accountant. The store's API is intentionally
// thin; callers (attrCache, dirCache, dataCache) wrap it with their
// own TTL / type semantics.
//
// Concurrency: store.mu protects the map. Operations that mutate the
// accountant DO NOT hold store.mu while doing so (different locks
// in different orders would deadlock).
type store struct {
	mu      sync.RWMutex
	entries map[string]*entry
	acct    *accountant
}

func newStore(acct *accountant) *store {
	return &store{
		entries: make(map[string]*entry),
		acct:    acct,
	}
}

// get returns the entry for key (or nil if absent). Promotes to MRU
// on hit. Callers MUST cast e.value to the typed value.
func (s *store) get(key string) *entry {
	s.mu.RLock()
	e, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return nil
	}
	s.acct.touch(e)
	return e
}

// put inserts or replaces the entry for key. If a prior entry existed,
// it is removed from the accountant first (so bytes don't double-count).
func (s *store) put(key string, value any, size int) {
	e := &entry{key: key, value: value, size: size, remove: s.removeKey}
	// Snapshot any prior entry under the store lock, but release the
	// store lock BEFORE touching the accountant. accountant.evictLocked
	// invokes the eviction callback (store.removeKey) while holding
	// accountant.mu; if we still held store.mu here we'd hit a
	// lock-order inversion with that path and deadlock.
	s.mu.Lock()
	prior, hadPrior := s.entries[key]
	s.entries[key] = e
	s.mu.Unlock()
	if hadPrior {
		s.acct.remove(prior)
	}
	s.acct.insert(e)
}

// remove deletes the entry for key. Idempotent.
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
}

// removeKey is the callback the accountant invokes during eviction.
// Distinct from remove so the accountant's lock is held during the
// call (we only mutate the store's map, not the accountant).
func (s *store) removeKey(key string) {
	s.mu.Lock()
	delete(s.entries, key)
	s.mu.Unlock()
}

// removeMatching removes every entry whose key satisfies pred. Used
// by data cache's invalidate-by-path and dir's parent invalidations.
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
	}
}

// size returns the number of entries (for tests).
func (s *store) size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
