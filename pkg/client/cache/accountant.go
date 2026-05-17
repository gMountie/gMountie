// Package cache implements the in-memory client-side cache decorator
// for FileSystemBackend (defined in pkg/client/io). The cache is
// disabled by default; see CacheConfig in pkg/client/config.
//
// accountant.go owns the shared byte-cap tracker and global LRU list
// across all stores in the cache package. Eviction picks the globally
// least-recently-used entry regardless of which store it lives in,
// so one cache type (e.g. data) cannot starve another (e.g. attr).
package cache

import (
	"container/list"
	"sync"
)

// entry is the generic LRU-tracked record. Stores own the typed value
// (attr, dir entries, or a chunk []byte); the accountant owns the
// list element and the bytes accounting.
type entry struct {
	key     string
	value   any
	size    int
	element *list.Element    // pointer to its node in accountant.lru
	remove  func(key string) // called by accountant on eviction; store passes its own removeKey
}

// accountant tracks total bytes across registered stores and runs the
// global LRU eviction loop when an insertion would push the total over
// the configured budget. budget == 0 disables eviction (used in tests
// that exercise pure semantics without cap behaviour).
type accountant struct {
	mu     sync.Mutex
	budget int
	used   int
	lru    *list.List // tail = LRU (evicted first), head = MRU
}

// newAccountant constructs an accountant with the given byte budget.
// budget <= 0 disables eviction.
func newAccountant(budget int) *accountant {
	return &accountant{budget: budget, lru: list.New()}
}

// insert registers a new entry, accounts its bytes, and evicts LRU
// entries until used <= budget. Caller must NOT already hold the
// accountant lock. The entry's element field is populated on success.
func (a *accountant) insert(e *entry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e.element = a.lru.PushFront(e)
	a.used += e.size
	a.evictLocked()
}

// touch promotes an existing entry to MRU. Called by stores on a cache
// hit. Caller must NOT already hold the accountant lock.
func (a *accountant) touch(e *entry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if e.element != nil {
		a.lru.MoveToFront(e.element)
	}
}

// remove deregisters an entry and refunds its bytes. Idempotent.
// Caller must NOT already hold the accountant lock.
func (a *accountant) remove(e *entry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.removeLocked(e)
}

func (a *accountant) removeLocked(e *entry) {
	if e.element == nil {
		return
	}
	a.lru.Remove(e.element)
	e.element = nil
	a.used -= e.size
	if a.used < 0 {
		a.used = 0
	}
}

// evictLocked drains the LRU tail until used <= budget. Called with
// accountant lock held.
func (a *accountant) evictLocked() {
	if a.budget <= 0 {
		return
	}
	for a.used > a.budget {
		back := a.lru.Back()
		if back == nil {
			return
		}
		e := back.Value.(*entry)
		a.removeLocked(e)
		if e.remove != nil {
			e.remove(e.key)
		}
	}
}

// Used returns the current accounted bytes. Exposed for testing /
// observability.
func (a *accountant) Used() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}
