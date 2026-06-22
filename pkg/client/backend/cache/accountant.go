// Package cache implements the in-memory client-side cache decorator
// for FileSystemBackend (defined in pkg/client/backend). The cache is
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

// accountant tracks total bytes AND total entry count across registered
// stores and runs the global LRU eviction loop when an insertion would push
// either over its configured limit. budget == 0 disables the byte cap and
// maxEntries == 0 disables the count cap (both used in tests that exercise
// pure semantics without cap behaviour).
//
// The entry-count cap is a hard backstop: per-entry byte sizes are estimates,
// and if they ever under-count (as they did before this cap — issue #118), a
// workload touching many distinct paths could grow the maps in real heap far
// past the byte budget without the byte-based eviction ever firing. The count
// cap bounds the map cardinality regardless of sizing accuracy, so a sizing
// error can never leak unboundedly.
type accountant struct {
	mu         sync.Mutex
	budget     int // byte budget; <=0 disables the byte cap
	maxEntries int // entry-count backstop; <=0 disables the count cap
	used       int
	lru        *list.List // tail = LRU (evicted first), head = MRU
}

// newAccountant constructs an accountant with the given byte budget and
// entry-count cap. A non-positive value disables that cap.
func newAccountant(budget, maxEntries int) *accountant {
	return &accountant{budget: budget, maxEntries: maxEntries, lru: list.New()}
}

// minEntryFootprintBytes is a conservative floor on a real cached entry's
// resident cost. The derived entry-count cap is budget ÷ this, so it sits
// well above the count the byte budget allows at realistic (accurately-sized)
// entries and never fires in normal use — it only bounds the maps if the byte
// estimates ever under-count again. Smaller than attrFixedOverheadBytes on
// purpose, to keep the cap a loose backstop rather than a second active limit.
const minEntryFootprintBytes = 128

// deriveMaxEntries returns the entry-count backstop for a byte budget. A
// non-positive budget disables both caps (memory tier is unbounded — the
// caller opted out of eviction entirely).
func deriveMaxEntries(memoryMaxBytes int) int {
	if memoryMaxBytes <= 0 {
		return 0
	}
	return memoryMaxBytes / minEntryFootprintBytes
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

// evictLocked drains the LRU tail until the accountant is within BOTH the
// byte budget and the entry-count cap. Called with accountant lock held.
func (a *accountant) evictLocked() {
	for a.overLimitLocked() {
		back := a.lru.Back()
		if back == nil {
			return
		}
		// accountant.lru only ever stores *entry; the assertion can't fail.
		e, _ := back.Value.(*entry)
		a.removeLocked(e)
		if e.remove != nil {
			e.remove(e.key)
		}
	}
}

// overLimitLocked reports whether the accountant exceeds either active cap.
// A cap of <=0 is disabled. Called with the lock held.
func (a *accountant) overLimitLocked() bool {
	if a.budget > 0 && a.used > a.budget {
		return true
	}
	if a.maxEntries > 0 && a.lru.Len() > a.maxEntries {
		return true
	}
	return false
}

// Used returns the current accounted bytes. Exposed for testing /
// observability.
func (a *accountant) Used() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}

// Len returns the current number of tracked entries. Exposed for testing /
// observability.
func (a *accountant) Len() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lru.Len()
}
