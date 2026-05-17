# Phase 4 / Sub-spec B: in-memory cache + TTL invalidation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an in-memory cache layer in a new `pkg/client/cache/` package that decorates the `FileSystemBackend` interface from Sub-spec A. Three logical caches (attribute, directory, data chunks) share a single byte-cap LRU eviction policy with TTL invalidation. Disabled by default; opt-in via `client.cache.enabled` config key.

**Architecture:** Decorator pattern at the `FileSystemBackend` seam. A single shared `accountant` tracks total bytes across three independent stores and drives global-LRU eviction. Per-cache TTL keeps entries fresh; mutating ops explicitly invalidate per a per-op contract table. `cachedHandle.Unwrap()` (from Sub-spec A's final fix) lets the inner gRPC backend find its `*grpcFileHandle` through the wrapper.

**Tech Stack:** Go generics (`any`-valued entries), `container/list` for LRU, `sync.RWMutex` for per-store concurrency. No new external deps. Testify suites; mocks via `task gen:mocks`.

**Spec reference:** `docs/superpowers/specs/2026-05-17-phase-4b-in-memory-cache-ttl.md`.

**Working agreements (apply in every task):**
- testify suites mandatory; benchmarks stay flat (`Benchmark*` funcs).
- `task gen:mocks` regenerates `internal/mocks/`; never hand-edit.
- Errors via `github.com/pkg/errors.Wrap`.
- Logging via `gmountie/pkg/utils/log`.
- Conventional commits, NO `Co-Authored-By:` / `Signed-off-by:` trailers.
- BC NOT a concern (no proto changes in B).
- FUSE-mount tests on the kubevirt VM at `192.168.11.11`; unit tests locally.
- Pre-Sub-spec-B commit: `0ab9568`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `pkg/client/cache/store.go` | **create** | Generic `store` (map + per-key entries) keyed by string, registered with an `accountant` for byte accounting |
| `pkg/client/cache/accountant.go` | **create** | Shared byte-cap tracker. Maintains a global LRU list across all registered stores; evicts when an insertion would push total bytes over cap |
| `pkg/client/cache/store_test.go` | **create** | Hit/miss/eviction-order/byte-accounting/concurrent-read tests |
| `pkg/client/cache/accountant_test.go` | **create** | Cross-store global LRU eviction tests |
| `pkg/client/cache/attr.go` | **create** | `attrCache` wrapping a `store` of `*attrEntry` (positive `*io.Attr` or negative ENOENT marker) with `AttrTTL` / `NegativeTTL` |
| `pkg/client/cache/attr_test.go` | **create** | Positive/negative hit, TTL expiry, invalidation |
| `pkg/client/cache/dir.go` | **create** | `dirCache` wrapping a `store` of `*dirEntry` (`[]io.DirEntry` + `expiresAt`) with `DirTTL` |
| `pkg/client/cache/dir_test.go` | **create** | Hit, TTL expiry, invalidation |
| `pkg/client/cache/data.go` | **create** | `dataCache` keyed by `chunkKey(path, chunkIndex)`, store of `[]byte` chunks; no TTL |
| `pkg/client/cache/data_test.go` | **create** | Chunk-aligned read assembly, partial-chunk reads, invalidate-by-path |
| `pkg/client/cache/handle.go` | **create** | `cachedHandle` wrapping an `io.FileHandle` with `Unwrap()` returning the inner; carries the path |
| `pkg/client/cache/handle_test.go` | **create** | `Unwrap()` chain reaches the inner; `Path()` correct |
| `pkg/client/cache/config.go` | **create** | `Config` struct + `New` constructor + `ConfigFromClient` adapter |
| `pkg/client/cache/backend.go` | **create** | `cachedBackend` (implements `io.FileSystemBackend`) + `NewCachedBackend` constructor. Owns all four sub-caches and the accountant |
| `pkg/client/cache/backend_test.go` | **create** | Integration test against `MockFileSystemBackend`; per-op invalidation table from the spec |
| `pkg/client/config/cache.go` | **create** | `CacheConfig` struct + defaults + validate tags |
| `pkg/client/config/config.go` | **modify** | Wire `CacheConfig` defaults + viper `BindEnv` + struct field on `ClientConfig` |
| `pkg/client/config/config_test.go` | **modify** | New default + env-override + YAML round-trip tests for the cache block |
| `pkg/client/mount/single.go` | **modify** | After `NewBackendClient`, conditionally wrap with `cache.NewCachedBackend` when enabled |
| `pkg/client/mount/vfs.go` | **modify** | Same wrap inside `Mount(volumeName)` per-volume backend construction |
| `test/e2e/utils/app.go` | **modify** | `WithCache(cfg cache.Config)` test option installing the decorator + enabling the cache in test config |
| `test/e2e/api/cache_test.go` | **create** | e2e suite running SimpleFS / Streaming Read+Write / Compound under cache-on; cache-off variant runs through existing suites unchanged |
| `docs/client/config.md` | **modify** | Document the six new `client.cache.*` keys |
| `docs/perf/phase4b-2026-05-17.md` | **create** | Final-task perf delta summary, cache-on vs cache-off |

The cache package files are intentionally small (1 responsibility each); the integration glue lives in `backend.go` which is the only file that knows about all sub-caches simultaneously.

---

## Task 1: `CacheConfig` plumbing

**Files:**
- Create: `pkg/client/config/cache.go`
- Modify: `pkg/client/config/config.go` (add field on `ClientConfig`, wire viper defaults + `BindEnv`)
- Modify: `pkg/client/config/config_test.go` (add 3 suite methods: defaults, env override, YAML round-trip)

- [ ] **Step 1: Write `pkg/client/config/cache.go`**

```go
package config

import "time"

const (
	// Cache defaults — see docs/superpowers/specs/2026-05-17-phase-4b-in-memory-cache-ttl.md.
	DefaultCacheEnabled        = false
	DefaultCacheMaxSizeBytes   = 1 << 30 // 1 GiB
	DefaultCacheChunkSizeBytes = 1 << 20 // 1 MiB
	DefaultCacheAttrTTL        = 5 * time.Second
	DefaultCacheDirTTL         = 5 * time.Second
	DefaultCacheNegativeTTL    = 2 * time.Second
)

// CacheConfig governs the optional client-side in-memory cache layer that
// decorates the gRPC FileSystemBackend. Disabled by default in Phase 4
// Sub-spec B; Sub-spec C will flip the default once persistence proves
// the disk side of the story.
type CacheConfig struct {
	// Enabled gates whether the cache decorator is inserted at mount time.
	// false (the default) keeps the chain identical to Sub-spec A.
	Enabled bool `mapstructure:"enabled"`
	// MaxSizeBytes is the total byte budget across all three sub-caches
	// (attr + dir + data). Eviction is global LRU once this is exceeded.
	MaxSizeBytes int `mapstructure:"max_size_bytes" validate:"min=0,max=68719476736"`
	// ChunkSizeBytes is the data cache's chunk granularity. Reads are split
	// into chunk-sized requests against the inner backend on a miss.
	ChunkSizeBytes int `mapstructure:"chunk_size_bytes" validate:"min=4096,max=16777216"`
	// AttrTTL is the per-entry lifetime for positive attribute cache hits.
	AttrTTL time.Duration `mapstructure:"attr_ttl"`
	// DirTTL is the per-entry lifetime for directory listing cache hits.
	DirTTL time.Duration `mapstructure:"dir_ttl"`
	// NegativeTTL is the per-entry lifetime for negative attribute cache
	// entries (paths that returned ENOENT). Short by design so deletions
	// elsewhere become visible quickly.
	NegativeTTL time.Duration `mapstructure:"negative_ttl"`
}
```

- [ ] **Step 2: Modify `pkg/client/config/config.go` to add the field on `ClientConfig`**

Find the `ClientConfig` struct and add:

```go
type ClientConfig struct {
    // ... existing fields ...
    Cache CacheConfig `mapstructure:"cache"`
}
```

In whatever function applies viper defaults for the client config (mirror the `Rpc`/`FUSE` blocks), add:

```go
v.SetDefault("client.cache.enabled", DefaultCacheEnabled)
v.SetDefault("client.cache.max_size_bytes", DefaultCacheMaxSizeBytes)
v.SetDefault("client.cache.chunk_size_bytes", DefaultCacheChunkSizeBytes)
v.SetDefault("client.cache.attr_ttl", DefaultCacheAttrTTL)
v.SetDefault("client.cache.dir_ttl", DefaultCacheDirTTL)
v.SetDefault("client.cache.negative_ttl", DefaultCacheNegativeTTL)

_ = v.BindEnv("client.cache.enabled", "GMOUNTIE_CLIENT_CACHE_ENABLED")
_ = v.BindEnv("client.cache.max_size_bytes", "GMOUNTIE_CLIENT_CACHE_MAX_SIZE_BYTES")
_ = v.BindEnv("client.cache.chunk_size_bytes", "GMOUNTIE_CLIENT_CACHE_CHUNK_SIZE_BYTES")
_ = v.BindEnv("client.cache.attr_ttl", "GMOUNTIE_CLIENT_CACHE_ATTR_TTL")
_ = v.BindEnv("client.cache.dir_ttl", "GMOUNTIE_CLIENT_CACHE_DIR_TTL")
_ = v.BindEnv("client.cache.negative_ttl", "GMOUNTIE_CLIENT_CACHE_NEGATIVE_TTL")
```

(Exact key paths follow the existing `Rpc`/`FUSE` template — match what the config_test.go assertions for those blocks expect.)

- [ ] **Step 3: Add three test methods to `pkg/client/config/config_test.go`**

```go
func (s *ConfigTestSuite) TestCacheDefaults() {
	cfg, err := LoadConfigFromString(`
client:
  endpoint: "localhost:9999"
  auth: {type: none}
`)
	s.Require().NoError(err)
	s.Assert().Equal(DefaultCacheEnabled, cfg.Client.Cache.Enabled)
	s.Assert().Equal(DefaultCacheMaxSizeBytes, cfg.Client.Cache.MaxSizeBytes)
	s.Assert().Equal(DefaultCacheChunkSizeBytes, cfg.Client.Cache.ChunkSizeBytes)
	s.Assert().Equal(DefaultCacheAttrTTL, cfg.Client.Cache.AttrTTL)
	s.Assert().Equal(DefaultCacheDirTTL, cfg.Client.Cache.DirTTL)
	s.Assert().Equal(DefaultCacheNegativeTTL, cfg.Client.Cache.NegativeTTL)
}

func (s *ConfigTestSuite) TestCacheEnvOverride() {
	s.T().Setenv("GMOUNTIE_CLIENT_CACHE_ENABLED", "true")
	s.T().Setenv("GMOUNTIE_CLIENT_CACHE_MAX_SIZE_BYTES", "2147483648")
	s.T().Setenv("GMOUNTIE_CLIENT_CACHE_ATTR_TTL", "10s")
	cfg, err := LoadConfigFromString(`
client:
  endpoint: "localhost:9999"
  auth: {type: none}
`)
	s.Require().NoError(err)
	s.Assert().True(cfg.Client.Cache.Enabled)
	s.Assert().Equal(2147483648, cfg.Client.Cache.MaxSizeBytes)
	s.Assert().Equal(10*time.Second, cfg.Client.Cache.AttrTTL)
}

func (s *ConfigTestSuite) TestCacheExplicitYAML() {
	cfg, err := LoadConfigFromString(`
client:
  endpoint: "localhost:9999"
  auth: {type: none}
  cache:
    enabled: true
    max_size_bytes: 524288000
    chunk_size_bytes: 65536
    attr_ttl: 30s
    dir_ttl: 1m
    negative_ttl: 500ms
`)
	s.Require().NoError(err)
	s.Assert().True(cfg.Client.Cache.Enabled)
	s.Assert().Equal(524288000, cfg.Client.Cache.MaxSizeBytes)
	s.Assert().Equal(65536, cfg.Client.Cache.ChunkSizeBytes)
	s.Assert().Equal(30*time.Second, cfg.Client.Cache.AttrTTL)
	s.Assert().Equal(time.Minute, cfg.Client.Cache.DirTTL)
	s.Assert().Equal(500*time.Millisecond, cfg.Client.Cache.NegativeTTL)
}
```

(Adapt `LoadConfigFromString` etc. to the actual helper names in `pkg/client/config/config_test.go` — check what the existing `Rpc`/`FUSE` tests use.)

- [ ] **Step 4: Run tests, vet, build**

```bash
go test -race ./pkg/client/config/...
go vet ./...
go build ./...
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/config/cache.go pkg/client/config/config.go pkg/client/config/config_test.go
git commit -m "$(cat <<'EOF'
feat(client/config): add CacheConfig for Phase 4 Sub-spec B

CacheConfig with six knobs (enabled / max_size_bytes / chunk_size_bytes
/ attr_ttl / dir_ttl / negative_ttl). Disabled by default; Sub-spec C
flips the default once persistence proves the disk story. Viper
defaults, env BindEnv, and YAML round-trip all under unit test
coverage matching the existing Rpc/FUSE block conventions.

No runtime behaviour yet; the cache layer itself lands in subsequent
tasks. This commit lets later tasks reference the canonical config
shape and defaults without forward-declaring them.
EOF
)"
```

---

## Task 2: `store` + `accountant` primitives

**Files:**
- Create: `pkg/client/cache/store.go`
- Create: `pkg/client/cache/accountant.go`
- Create: `pkg/client/cache/store_test.go`
- Create: `pkg/client/cache/accountant_test.go`

**Design:** the `accountant` owns a single global LRU list across all stores; each entry is a `*entry` with a backpointer to its store's `removeKey` callback. Stores are flat maps; they don't manage LRU themselves — they call `acct.touch(entry)` on access and `acct.insert(entry)` / `acct.remove(entry)` on mutation. This keeps eviction strictly global without per-store coordination.

- [ ] **Step 1: Write `pkg/client/cache/accountant.go`**

```go
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
	element *list.Element // pointer to its node in accountant.lru
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
```

- [ ] **Step 2: Write `pkg/client/cache/store.go`**

```go
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
	s.mu.Lock()
	if prior, ok := s.entries[key]; ok {
		s.acct.remove(prior)
	}
	s.entries[key] = e
	s.mu.Unlock()
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
```

- [ ] **Step 3: Write `pkg/client/cache/store_test.go`**

```go
package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite
	acct *accountant
	s    *store
}

func (s *StoreTestSuite) SetupTest() {
	s.acct = newAccountant(0) // unlimited for the basic suite
	s.s = newStore(s.acct)
}

func (s *StoreTestSuite) TestPutGet() {
	s.s.put("k1", "v1", 10)
	e := s.s.get("k1")
	s.Require().NotNil(e)
	s.Assert().Equal("v1", e.value)
	s.Assert().Equal(10, e.size)
}

func (s *StoreTestSuite) TestGetMiss() {
	s.Assert().Nil(s.s.get("nope"))
}

func (s *StoreTestSuite) TestPutReplacesAndRefundsBytes() {
	s.s.put("k", "v", 100)
	s.Require().Equal(100, s.acct.Used())
	s.s.put("k", "v2", 30)
	s.Assert().Equal(30, s.acct.Used())
	s.Assert().Equal("v2", s.s.get("k").value)
}

func (s *StoreTestSuite) TestRemove() {
	s.s.put("k", "v", 50)
	s.s.remove("k")
	s.Assert().Nil(s.s.get("k"))
	s.Assert().Equal(0, s.acct.Used())
}

func (s *StoreTestSuite) TestRemoveMatching() {
	s.s.put("/a/x", "v1", 10)
	s.s.put("/a/y", "v2", 10)
	s.s.put("/b/z", "v3", 10)
	s.s.removeMatching(func(k string) bool { return len(k) >= 2 && k[:2] == "/a" })
	s.Assert().Nil(s.s.get("/a/x"))
	s.Assert().Nil(s.s.get("/a/y"))
	s.Assert().NotNil(s.s.get("/b/z"))
	s.Assert().Equal(10, s.acct.Used())
}

func (s *StoreTestSuite) TestConcurrentReadsRaceClean() {
	s.s.put("k", "v", 10)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				_ = s.s.get("k")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	s.Assert().Equal(10, s.acct.Used())
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}
```

- [ ] **Step 4: Write `pkg/client/cache/accountant_test.go`**

```go
package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type AccountantTestSuite struct {
	suite.Suite
}

// TestSingleStoreEvictsLRU verifies that within one store, the LRU
// entry is evicted when the cap is exceeded.
func (s *AccountantTestSuite) TestSingleStoreEvictsLRU() {
	acct := newAccountant(30)
	st := newStore(acct)
	st.put("a", "va", 10) // inserts: [a]
	st.put("b", "vb", 10) // [b, a]
	st.put("c", "vc", 10) // [c, b, a]; used=30, at cap
	st.put("d", "vd", 10) // [d, c, b]; a evicted
	s.Assert().Nil(st.get("a"))
	s.Assert().NotNil(st.get("b"))
	s.Assert().NotNil(st.get("c"))
	s.Assert().NotNil(st.get("d"))
	s.Assert().Equal(30, acct.Used())
}

// TestTouchProtectsFromEviction verifies that a Get promotes the
// entry to MRU and saves it from imminent eviction.
func (s *AccountantTestSuite) TestTouchProtectsFromEviction() {
	acct := newAccountant(30)
	st := newStore(acct)
	st.put("a", "va", 10)
	st.put("b", "vb", 10)
	st.put("c", "vc", 10)
	_ = st.get("a") // promote a; now [a, c, b]
	st.put("d", "vd", 10) // b is LRU and gets evicted
	s.Assert().NotNil(st.get("a"))
	s.Assert().Nil(st.get("b"))
	s.Assert().NotNil(st.get("c"))
	s.Assert().NotNil(st.get("d"))
}

// TestCrossStoreEvictsGloballyLRU verifies that eviction picks the LRU
// across all registered stores, not just the inserting one.
func (s *AccountantTestSuite) TestCrossStoreEvictsGloballyLRU() {
	acct := newAccountant(30)
	stA := newStore(acct)
	stB := newStore(acct)
	stA.put("a1", "v", 10) // global LRU
	stB.put("b1", "v", 10)
	stB.put("b2", "v", 10) // used=30; a1 is global LRU
	stB.put("b3", "v", 10) // forces eviction; a1 (from stA) goes
	s.Assert().Nil(stA.get("a1"))
	s.Assert().NotNil(stB.get("b1"))
	s.Assert().NotNil(stB.get("b2"))
	s.Assert().NotNil(stB.get("b3"))
}

// TestZeroBudgetDisablesEviction verifies that budget<=0 means "no cap".
func (s *AccountantTestSuite) TestZeroBudgetDisablesEviction() {
	acct := newAccountant(0)
	st := newStore(acct)
	for i := 0; i < 1000; i++ {
		st.put(string(rune(i)), "v", 100)
	}
	s.Assert().Equal(100000, acct.Used())
	s.Assert().Equal(1000, st.size())
}

func TestAccountantTestSuite(t *testing.T) {
	suite.Run(t, new(AccountantTestSuite))
}
```

- [ ] **Step 5: Run tests under `-race`**

```bash
go test -race ./pkg/client/cache/...
go vet ./...
```

Expected: green.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/cache/
git commit -m "$(cat <<'EOF'
feat(client/cache): generic store + shared accountant primitives

Two primitives the typed caches (attr, dir, data) compose with:

- store: thin sync.RWMutex-guarded map keyed by string. Defers byte
  accounting and LRU position to the accountant. Hot reads take the
  read lock; mutations take the write lock + accountant lock in a
  fixed order (store then accountant) to avoid deadlocks.

- accountant: single global LRU list across all registered stores.
  Eviction picks the globally least-recently-used entry regardless
  of which store it lives in, so one cache type cannot starve
  another. budget <= 0 disables eviction (used in unit tests that
  exercise pure put/get/remove semantics).

Both safe for concurrent use; verified under -race in unit tests.
EOF
)"
```

---

## Task 3: `attrCache`

**Files:**
- Create: `pkg/client/cache/attr.go`
- Create: `pkg/client/cache/attr_test.go`

- [ ] **Step 1: Write `pkg/client/cache/attr.go`**

```go
package cache

import (
	"time"

	"gmountie/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// attrEntry is the value stored in attrCache. negative=true means a
// prior Lookup returned ENOENT; attr is then nil. expiresAt is the
// absolute deadline at which the entry should be treated as a miss.
type attrEntry struct {
	attr      *io.Attr
	negative  bool
	expiresAt time.Time
}

// attrCache is a thin TTL wrapper over a store. attrTTL and negativeTTL
// are taken from CacheConfig at construction.
type attrCache struct {
	st          *store
	now         func() time.Time
	attrTTL     time.Duration
	negativeTTL time.Duration
}

func newAttrCache(acct *accountant, attrTTL, negativeTTL time.Duration, now func() time.Time) *attrCache {
	if now == nil {
		now = time.Now
	}
	return &attrCache{
		st:          newStore(acct),
		now:         now,
		attrTTL:     attrTTL,
		negativeTTL: negativeTTL,
	}
}

// get returns (attr, true, true) on a positive hit, (nil, true, false)
// on a negative hit (ENOENT cached), or (nil, false, false) on a miss
// or expired entry. Two booleans (hit, positive) make the call sites
// readable without a third type.
func (c *attrCache) get(path string) (*io.Attr, bool, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false, false
	}
	ae := e.value.(*attrEntry)
	if c.now().After(ae.expiresAt) {
		c.st.remove(path)
		return nil, false, false
	}
	if ae.negative {
		return nil, true, false
	}
	return ae.attr, true, true
}

// putPositive caches a successful Stat result.
func (c *attrCache) putPositive(path string, attr *io.Attr) {
	if attr == nil {
		return
	}
	ae := &attrEntry{attr: attr, expiresAt: c.now().Add(c.attrTTL)}
	c.st.put(path, ae, attrEntrySize(ae))
}

// putNegative caches an ENOENT result.
func (c *attrCache) putNegative(path string) {
	ae := &attrEntry{negative: true, expiresAt: c.now().Add(c.negativeTTL)}
	c.st.put(path, ae, attrEntrySize(ae))
}

// invalidate drops the cached entry for path (positive or negative).
func (c *attrCache) invalidate(path string) {
	c.st.remove(path)
}

// attrEntrySize estimates the in-memory footprint of an attrEntry.
// Used for accountant bookkeeping; small and approximate is fine.
func attrEntrySize(_ *attrEntry) int {
	// 16-ish fields × 8 bytes + struct overhead. 256 is a generous
	// rounded estimate that absorbs the negative variant too.
	return 256
}

// Status returns the appropriate FUSE status for a cache hit.
// Convenience for backend.go's Stat/Lookup handlers.
func attrStatus(positive bool) fuse.Status {
	if positive {
		return fuse.OK
	}
	return fuse.ENOENT
}
```

- [ ] **Step 2: Write `pkg/client/cache/attr_test.go`**

```go
package cache

import (
	"testing"
	"time"

	"gmountie/pkg/client/io"

	"github.com/stretchr/testify/suite"
)

type AttrCacheTestSuite struct {
	suite.Suite
	now   time.Time
	clock func() time.Time
	c     *attrCache
}

func (s *AttrCacheTestSuite) SetupTest() {
	s.now = time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return s.now }
	s.c = newAttrCache(newAccountant(0), 5*time.Second, 2*time.Second, s.clock)
}

func (s *AttrCacheTestSuite) advance(d time.Duration) { s.now = s.now.Add(d) }

func (s *AttrCacheTestSuite) TestMiss() {
	a, hit, pos := s.c.get("/x")
	s.Assert().False(hit)
	s.Assert().False(pos)
	s.Assert().Nil(a)
}

func (s *AttrCacheTestSuite) TestPositiveHit() {
	s.c.putPositive("/x", &io.Attr{Ino: 7, Size: 100})
	a, hit, pos := s.c.get("/x")
	s.Require().True(hit)
	s.Require().True(pos)
	s.Assert().Equal(uint64(7), a.Ino)
}

func (s *AttrCacheTestSuite) TestNegativeHit() {
	s.c.putNegative("/missing")
	a, hit, pos := s.c.get("/missing")
	s.Require().True(hit)
	s.Assert().False(pos)
	s.Assert().Nil(a)
}

func (s *AttrCacheTestSuite) TestPositiveExpiry() {
	s.c.putPositive("/x", &io.Attr{Ino: 1})
	s.advance(6 * time.Second) // > AttrTTL (5s)
	_, hit, _ := s.c.get("/x")
	s.Assert().False(hit)
}

func (s *AttrCacheTestSuite) TestNegativeExpiry() {
	s.c.putNegative("/missing")
	s.advance(3 * time.Second) // > NegativeTTL (2s)
	_, hit, _ := s.c.get("/missing")
	s.Assert().False(hit)
}

func (s *AttrCacheTestSuite) TestInvalidate() {
	s.c.putPositive("/x", &io.Attr{Ino: 1})
	s.c.invalidate("/x")
	_, hit, _ := s.c.get("/x")
	s.Assert().False(hit)
}

func TestAttrCacheTestSuite(t *testing.T) {
	suite.Run(t, new(AttrCacheTestSuite))
}
```

- [ ] **Step 3: Run + commit**

```bash
go test -race ./pkg/client/cache/...
go vet ./...

git add pkg/client/cache/attr.go pkg/client/cache/attr_test.go
git commit -m "$(cat <<'EOF'
feat(client/cache): attrCache (positive + negative TTL)

Wraps store with attr-specific semantics:
- positive hit: cached *io.Attr, expires at now+AttrTTL
- negative hit: ENOENT marker, expires at now+NegativeTTL
- miss: caller fetches and populates one of the two

Clock is injectable for deterministic expiry tests; production uses
time.Now. Size estimate is 256 bytes per entry (generous round number;
shared accountant uses it for byte accounting under the global cap).
EOF
)"
```

---

## Task 4: `dirCache`

**Files:**
- Create: `pkg/client/cache/dir.go`
- Create: `pkg/client/cache/dir_test.go`

- [ ] **Step 1: Write `pkg/client/cache/dir.go`**

```go
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
```

- [ ] **Step 2: Write `pkg/client/cache/dir_test.go`**

```go
package cache

import (
	"testing"
	"time"

	"gmountie/pkg/client/io"

	"github.com/stretchr/testify/suite"
)

type DirCacheTestSuite struct {
	suite.Suite
	now   time.Time
	clock func() time.Time
	c     *dirCache
}

func (s *DirCacheTestSuite) SetupTest() {
	s.now = time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return s.now }
	s.c = newDirCache(newAccountant(0), 5*time.Second, s.clock)
}

func (s *DirCacheTestSuite) advance(d time.Duration) { s.now = s.now.Add(d) }

func (s *DirCacheTestSuite) TestMiss() {
	_, hit := s.c.get("/d")
	s.Assert().False(hit)
}

func (s *DirCacheTestSuite) TestHitReturnsCopy() {
	entries := []io.DirEntry{{Name: "a", Mode: 0o644, Ino: 1}, {Name: "b", Mode: 0o644, Ino: 2}}
	s.c.put("/d", entries)
	got, hit := s.c.get("/d")
	s.Require().True(hit)
	s.Require().Len(got, 2)
	got[0].Name = "MUTATED"
	got2, _ := s.c.get("/d")
	s.Assert().Equal("a", got2[0].Name, "cache must return defensive copies")
}

func (s *DirCacheTestSuite) TestExpiry() {
	s.c.put("/d", []io.DirEntry{{Name: "a"}})
	s.advance(6 * time.Second)
	_, hit := s.c.get("/d")
	s.Assert().False(hit)
}

func (s *DirCacheTestSuite) TestInvalidate() {
	s.c.put("/d", []io.DirEntry{{Name: "a"}})
	s.c.invalidate("/d")
	_, hit := s.c.get("/d")
	s.Assert().False(hit)
}

func TestDirCacheTestSuite(t *testing.T) {
	suite.Run(t, new(DirCacheTestSuite))
}
```

- [ ] **Step 3: Run + commit**

```bash
go test -race ./pkg/client/cache/...
go vet ./...

git add pkg/client/cache/dir.go pkg/client/cache/dir_test.go
git commit -m "$(cat <<'EOF'
feat(client/cache): dirCache with TTL + defensive copy on Get

ListDir results cached per-path with DirTTL. Get returns a copy of
the slice so callers cannot mutate the cached view (a single defaulted
go-fuse Readdir adapter loop could otherwise observably corrupt
subsequent hits).

Size estimate: 32 bytes envelope + 64 bytes per entry. Generous;
accountant byte-cap math doesn't need entry-level precision.
EOF
)"
```

---

## Task 5: `dataCache` + `cachedHandle`

**Files:**
- Create: `pkg/client/cache/data.go`
- Create: `pkg/client/cache/data_test.go`
- Create: `pkg/client/cache/handle.go`
- Create: `pkg/client/cache/handle_test.go`

- [ ] **Step 1: Write `pkg/client/cache/data.go`**

```go
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
```

- [ ] **Step 2: Write `pkg/client/cache/data_test.go`**

```go
package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type DataCacheTestSuite struct {
	suite.Suite
	c *dataCache
}

func (s *DataCacheTestSuite) SetupTest() {
	s.c = newDataCache(newAccountant(0), 1024) // 1 KiB chunks for arithmetic clarity
}

func (s *DataCacheTestSuite) TestMissAndPut() {
	s.Assert().Nil(s.c.get("/f", 0))
	s.c.put("/f", 0, []byte("abcd"))
	s.Assert().Equal([]byte("abcd"), s.c.get("/f", 0))
}

func (s *DataCacheTestSuite) TestDifferentPathsDisjoint() {
	s.c.put("/a", 0, []byte("1111"))
	s.c.put("/b", 0, []byte("2222"))
	s.Assert().Equal([]byte("1111"), s.c.get("/a", 0))
	s.Assert().Equal([]byte("2222"), s.c.get("/b", 0))
}

func (s *DataCacheTestSuite) TestInvalidatePathDropsAllChunks() {
	s.c.put("/f", 0, []byte("c0"))
	s.c.put("/f", 1, []byte("c1"))
	s.c.put("/f", 5, []byte("c5"))
	s.c.put("/other", 0, []byte("xx"))
	s.c.invalidatePath("/f")
	s.Assert().Nil(s.c.get("/f", 0))
	s.Assert().Nil(s.c.get("/f", 1))
	s.Assert().Nil(s.c.get("/f", 5))
	s.Assert().Equal([]byte("xx"), s.c.get("/other", 0))
}

func (s *DataCacheTestSuite) TestInvalidateRange() {
	// chunks: 0 = [0, 1023], 1 = [1024, 2047], 2 = [2048, 3071]
	s.c.put("/f", 0, make([]byte, 1024))
	s.c.put("/f", 1, make([]byte, 1024))
	s.c.put("/f", 2, make([]byte, 1024))
	// Write to bytes [500, 1500) touches chunks 0 and 1
	s.c.invalidateRange("/f", 500, 1000)
	s.Assert().Nil(s.c.get("/f", 0))
	s.Assert().Nil(s.c.get("/f", 1))
	s.Assert().NotNil(s.c.get("/f", 2))
}

func (s *DataCacheTestSuite) TestInvalidateRangeZeroSize() {
	s.c.put("/f", 0, make([]byte, 1024))
	s.c.invalidateRange("/f", 0, 0) // no-op
	s.Assert().NotNil(s.c.get("/f", 0))
}

func TestDataCacheTestSuite(t *testing.T) {
	suite.Run(t, new(DataCacheTestSuite))
}
```

- [ ] **Step 3: Write `pkg/client/cache/handle.go`**

```go
package cache

import "gmountie/pkg/client/io"

// cachedHandle wraps an inner io.FileHandle returned by the inner
// FileSystemBackend.Open or Create. The cache decorator returns
// these to the go-fuse adapter layer; per-handle ops (Read, Write,
// Release, Flush, Fsync, Allocate, locks) pass through to the inner
// backend with the wrapper unwrapping itself first.
//
// Path() is the path the handle was opened against — needed by the
// cache layer to key data-chunk invalidations off the right path
// when an inner backend's path-from-handle accessor changes shape.
type cachedHandle struct {
	inner io.FileHandle
	path  string
}

// newCachedHandle wraps an inner handle.
func newCachedHandle(inner io.FileHandle, path string) *cachedHandle {
	return &cachedHandle{inner: inner, path: path}
}

// Path returns the path the wrapper was constructed with. This is the
// authoritative path for cache invalidation purposes — it's the path
// the caller passed to Open/Create, not whatever inner.Path() returns.
func (h *cachedHandle) Path() string { return h.path }

// Unwrap returns the inner handle. io.BackendClient's resolveHandle
// walks Unwrap chains to find the leaf *grpcFileHandle.
func (h *cachedHandle) Unwrap() io.FileHandle { return h.inner }
```

- [ ] **Step 4: Write `pkg/client/cache/handle_test.go`**

```go
package cache

import (
	"testing"

	iomocks "gmountie/internal/mocks/pkg/client/io"
	"gmountie/pkg/client/io"

	"github.com/stretchr/testify/suite"
)

type CachedHandleTestSuite struct {
	suite.Suite
}

func (s *CachedHandleTestSuite) TestPathAndUnwrap() {
	inner := iomocks.NewMockFileHandle(s.T())
	inner.EXPECT().Path().Return("ignored-inner").Maybe()
	h := newCachedHandle(inner, "/wrapped/path")
	s.Assert().Equal("/wrapped/path", h.Path())
	got, ok := h.Unwrap().(io.FileHandle)
	s.Require().True(ok)
	s.Assert().Same(inner, got)
}

func (s *CachedHandleTestSuite) TestUnwrapChainTerminatesAtInner() {
	// Two layers: cachedHandle wraps cachedHandle wraps a leaf.
	leaf := iomocks.NewMockFileHandle(s.T())
	leaf.EXPECT().Unwrap().Return(leaf).Maybe() // leaf is its own unwrap, per Sub-spec A's contract
	mid := newCachedHandle(leaf, "/mid")
	outer := newCachedHandle(mid, "/outer")
	// One Unwrap goes to mid, the next to leaf.
	s.Assert().Same(mid, outer.Unwrap())
	s.Assert().Same(leaf, outer.Unwrap().(*cachedHandle).Unwrap())
}

func TestCachedHandleTestSuite(t *testing.T) {
	suite.Run(t, new(CachedHandleTestSuite))
}
```

- [ ] **Step 5: Run + commit**

```bash
go test -race ./pkg/client/cache/...
go vet ./...

git add pkg/client/cache/data.go pkg/client/cache/data_test.go pkg/client/cache/handle.go pkg/client/cache/handle_test.go
git commit -m "$(cat <<'EOF'
feat(client/cache): dataCache (chunked, no TTL) + cachedHandle wrapper

dataCache stores file content as fixed-size chunks keyed by
(path, chunkIndex). No TTL because chunk freshness is driven by
local-mutation invalidations (Write/Truncate/Unlink/Rename) until
Sub-spec D adds Attr.version push.

invalidatePath drops every chunk for a path; invalidateRange drops
the chunks overlapping a byte range (Write only invalidates what
its bytes touch, Truncate only invalidates past the new size).

cachedHandle wraps an inner io.FileHandle and satisfies the
Unwrap() contract Sub-spec A added so BackendClient's resolveHandle
walks through to the *grpcFileHandle leaf.
EOF
)"
```

---

## Task 6: `cachedBackend` decorator

**Files:**
- Create: `pkg/client/cache/config.go`
- Create: `pkg/client/cache/backend.go`
- Create: `pkg/client/cache/backend_test.go`

**This is the largest task.** It composes the four sub-caches into a `cachedBackend` that implements `io.FileSystemBackend` with full read-through, write-through, invalidation behaviour per the spec's per-op table.

- [ ] **Step 1: Write `pkg/client/cache/config.go`**

```go
package cache

import (
	"time"

	clientconfig "gmountie/pkg/client/config"
)

// Config is the in-process Config the cache layer consumes. ClientConfig's
// CacheConfig is the operator-facing surface; ConfigFromClient adapts it.
type Config struct {
	MaxSizeBytes   int
	ChunkSizeBytes int
	AttrTTL        time.Duration
	DirTTL         time.Duration
	NegativeTTL    time.Duration
}

// ConfigFromClient builds a runtime Config from the operator-facing
// CacheConfig. Caller is responsible for checking Enabled before
// constructing the decorator.
func ConfigFromClient(cfg clientconfig.CacheConfig) Config {
	return Config{
		MaxSizeBytes:   cfg.MaxSizeBytes,
		ChunkSizeBytes: cfg.ChunkSizeBytes,
		AttrTTL:        cfg.AttrTTL,
		DirTTL:         cfg.DirTTL,
		NegativeTTL:    cfg.NegativeTTL,
	}
}
```

- [ ] **Step 2: Write `pkg/client/cache/backend.go` — struct + constructor + read-path ops**

```go
package cache

import (
	"context"
	"path"
	"strings"

	"gmountie/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// cachedBackend decorates an inner FileSystemBackend with three
// sub-caches sharing one accountant. Construct via NewCachedBackend;
// implements io.FileSystemBackend.
type cachedBackend struct {
	inner io.FileSystemBackend
	cfg   Config
	acct  *accountant
	attr  *attrCache
	dir   *dirCache
	data  *dataCache
}

// NewCachedBackend wraps inner. cfg.MaxSizeBytes <= 0 disables byte-cap
// eviction (entries live until invalidated or the process dies). Mount
// code constructs this conditionally on CacheConfig.Enabled.
func NewCachedBackend(inner io.FileSystemBackend, cfg Config) io.FileSystemBackend {
	acct := newAccountant(cfg.MaxSizeBytes)
	return &cachedBackend{
		inner: inner,
		cfg:   cfg,
		acct:  acct,
		attr:  newAttrCache(acct, cfg.AttrTTL, cfg.NegativeTTL, nil),
		dir:   newDirCache(acct, cfg.DirTTL, nil),
		data:  newDataCache(acct, cfg.ChunkSizeBytes),
	}
}

// --- Read path ---

func (b *cachedBackend) Stat(ctx context.Context, p string) (*io.Attr, fuse.Status) {
	if a, hit, pos := b.attr.get(p); hit {
		if pos {
			return a, fuse.OK
		}
		return nil, fuse.ENOENT
	}
	a, st := b.inner.Stat(ctx, p)
	if st == fuse.OK && a != nil {
		b.attr.putPositive(p, a)
	} else if st == fuse.ENOENT {
		b.attr.putNegative(p)
	}
	return a, st
}

func (b *cachedBackend) Lookup(ctx context.Context, parent, name string) (*io.Attr, fuse.Status) {
	full := joinPath(parent, name)
	if a, hit, pos := b.attr.get(full); hit {
		if pos {
			return a, fuse.OK
		}
		return nil, fuse.ENOENT
	}
	a, st := b.inner.Lookup(ctx, parent, name)
	if st == fuse.OK && a != nil {
		b.attr.putPositive(full, a)
	} else if st == fuse.ENOENT {
		b.attr.putNegative(full)
	}
	return a, st
}

func (b *cachedBackend) ListDir(ctx context.Context, p string) ([]io.DirEntry, fuse.Status) {
	if entries, hit := b.dir.get(p); hit {
		return entries, fuse.OK
	}
	entries, st := b.inner.ListDir(ctx, p)
	if st == fuse.OK {
		b.dir.put(p, entries)
	}
	return entries, st
}

func (b *cachedBackend) Read(ctx context.Context, fh io.FileHandle, off int64, dest []byte) (int, fuse.Status) {
	ch, ok := fh.(*cachedHandle)
	if !ok {
		return b.inner.Read(ctx, fh, off, dest)
	}
	chunkSize := int64(b.cfg.ChunkSizeBytes)
	total := 0
	for total < len(dest) {
		fileOff := off + int64(total)
		chunkIndex := int(fileOff / chunkSize)
		chunkStart := int64(chunkIndex) * chunkSize
		insideOff := int(fileOff - chunkStart)
		// How many bytes can we satisfy from this chunk?
		want := len(dest) - total
		if want > int(chunkSize)-insideOff {
			want = int(chunkSize) - insideOff
		}
		// Try cache first.
		cached := b.data.get(ch.path, chunkIndex)
		if cached != nil {
			if insideOff >= len(cached) {
				// EOF mid-stream
				return total, fuse.OK
			}
			n := copy(dest[total:total+want], cached[insideOff:])
			total += n
			if insideOff+n >= len(cached) {
				return total, fuse.OK
			}
			continue
		}
		// Miss: fetch this chunk from inner. Read full-chunk-aligned.
		buf := make([]byte, chunkSize)
		n, st := b.inner.Read(ctx, ch.inner, chunkStart, buf)
		if st != fuse.OK {
			return total, st
		}
		if n == 0 {
			return total, fuse.OK
		}
		filled := buf[:n]
		b.data.put(ch.path, chunkIndex, filled)
		copied := copy(dest[total:total+want], filled[insideOff:])
		total += copied
		if insideOff+copied >= n {
			return total, fuse.OK
		}
	}
	return total, fuse.OK
}

func (b *cachedBackend) Access(ctx context.Context, p string, mode uint32) fuse.Status {
	return b.inner.Access(ctx, p, mode)
}

func (b *cachedBackend) StatFs(ctx context.Context, p string) (*io.StatFs, fuse.Status) {
	return b.inner.StatFs(ctx, p)
}

func (b *cachedBackend) GetXAttr(ctx context.Context, p, attr string) ([]byte, fuse.Status) {
	return b.inner.GetXAttr(ctx, p, attr)
}

func (b *cachedBackend) GetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	return b.inner.GetLk(ctx, unwrapHandle(fh), owner, lk, flags, out)
}

// --- Open / Create / file-handle ops ---

func (b *cachedBackend) Open(ctx context.Context, p string, flags uint32) (io.FileHandle, fuse.Status) {
	h, st := b.inner.Open(ctx, p, flags)
	if st != fuse.OK {
		return nil, st
	}
	return newCachedHandle(h, p), fuse.OK
}

func (b *cachedBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (io.FileHandle, *io.Attr, fuse.Status) {
	full := joinPath(parent, name)
	h, a, st := b.inner.Create(ctx, parent, name, flags, mode)
	if st != fuse.OK {
		return nil, nil, st
	}
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(full) // drop any negative entry from a prior failed Stat
	if a != nil {
		b.attr.putPositive(full, a)
	}
	return newCachedHandle(h, full), a, fuse.OK
}

func (b *cachedBackend) Write(ctx context.Context, fh io.FileHandle, off int64, data []byte) (uint32, fuse.Status) {
	n, st := b.inner.Write(ctx, unwrapHandle(fh), off, data)
	if st != fuse.OK {
		return n, st
	}
	if ch, ok := fh.(*cachedHandle); ok {
		b.data.invalidateRange(ch.path, off, int64(len(data)))
		b.attr.invalidate(ch.path)
	}
	return n, fuse.OK
}

func (b *cachedBackend) Release(ctx context.Context, fh io.FileHandle) fuse.Status {
	return b.inner.Release(ctx, unwrapHandle(fh))
}

func (b *cachedBackend) Flush(ctx context.Context, fh io.FileHandle) fuse.Status {
	return b.inner.Flush(ctx, unwrapHandle(fh))
}

func (b *cachedBackend) Fsync(ctx context.Context, fh io.FileHandle, flags int64) fuse.Status {
	return b.inner.Fsync(ctx, unwrapHandle(fh), flags)
}

func (b *cachedBackend) Allocate(ctx context.Context, fh io.FileHandle, off, size uint64, mode uint32) fuse.Status {
	st := b.inner.Allocate(ctx, unwrapHandle(fh), off, size, mode)
	if st != fuse.OK {
		return st
	}
	if ch, ok := fh.(*cachedHandle); ok {
		b.data.invalidateRange(ch.path, int64(off), int64(size))
		b.attr.invalidate(ch.path)
	}
	return fuse.OK
}

func (b *cachedBackend) SetLk(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return b.inner.SetLk(ctx, unwrapHandle(fh), owner, lk, flags)
}

func (b *cachedBackend) SetLkw(ctx context.Context, fh io.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return b.inner.SetLkw(ctx, unwrapHandle(fh), owner, lk, flags)
}

// --- Path-level mutating ops ---

func (b *cachedBackend) Mkdir(ctx context.Context, p string, mode uint32) fuse.Status {
	st := b.inner.Mkdir(ctx, p, mode)
	if st != fuse.OK {
		return st
	}
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.invalidate(parent)
	b.attr.invalidate(p) // drop any negative-cached entry for the just-created path
	return fuse.OK
}

func (b *cachedBackend) Rmdir(ctx context.Context, p string) fuse.Status {
	st := b.inner.Rmdir(ctx, p)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	b.dir.invalidate(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.putNegative(p)
	return fuse.OK
}

func (b *cachedBackend) Unlink(ctx context.Context, p string) fuse.Status {
	st := b.inner.Unlink(ctx, p)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	b.data.invalidatePath(p)
	parent := pathParent(p)
	b.dir.invalidate(parent)
	b.attr.putNegative(p)
	return fuse.OK
}

func (b *cachedBackend) Rename(ctx context.Context, oldPath, newPath string) fuse.Status {
	st := b.inner.Rename(ctx, oldPath, newPath)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(oldPath)
	b.attr.invalidate(newPath)
	b.data.invalidatePath(oldPath)
	b.data.invalidatePath(newPath)
	b.dir.invalidate(pathParent(oldPath))
	b.dir.invalidate(pathParent(newPath))
	b.attr.putNegative(oldPath)
	return fuse.OK
}

func (b *cachedBackend) Truncate(ctx context.Context, p string, size uint64) fuse.Status {
	st := b.inner.Truncate(ctx, p, size)
	if st != fuse.OK {
		return st
	}
	// Conservative: drop every chunk for p (Write's invalidateRange
	// pattern requires knowing the new size in chunk indices; Truncate
	// affects every chunk past size and may zero-extend, so just blow
	// it all away).
	b.data.invalidatePath(p)
	b.attr.invalidate(p)
	return fuse.OK
}

func (b *cachedBackend) Chmod(ctx context.Context, p string, mode uint32) fuse.Status {
	st := b.inner.Chmod(ctx, p, mode)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	return fuse.OK
}

func (b *cachedBackend) Chown(ctx context.Context, p string, uid, gid uint32) fuse.Status {
	st := b.inner.Chown(ctx, p, uid, gid)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	return fuse.OK
}

// --- helpers ---

func unwrapHandle(fh io.FileHandle) io.FileHandle {
	if ch, ok := fh.(*cachedHandle); ok {
		return ch.inner
	}
	return fh
}

func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return path.Join(parent, name)
}

func pathParent(p string) string {
	// "" represents the mount root in our path convention.
	if p == "" || p == "/" {
		return ""
	}
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return ""
	}
	return p[:idx]
}
```

- [ ] **Step 3: Write `pkg/client/cache/backend_test.go` — integration suite against MockFileSystemBackend**

The suite covers every row in the spec's invalidation table. Setup creates a `cachedBackend` wrapping a `MockFileSystemBackend`. Each test wires the relevant inner expectation, drives the cached method, then asserts the cache state changed correctly.

```go
package cache

import (
	"context"
	"testing"
	"time"

	iomocks "gmountie/internal/mocks/pkg/client/io"
	"gmountie/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type CachedBackendTestSuite struct {
	suite.Suite
	inner *iomocks.MockFileSystemBackend
	b     *cachedBackend
}

func (s *CachedBackendTestSuite) SetupTest() {
	s.inner = iomocks.NewMockFileSystemBackend(s.T())
	cb := NewCachedBackend(s.inner, Config{
		MaxSizeBytes:   0, // no cap for these tests
		ChunkSizeBytes: 1024,
		AttrTTL:        5 * time.Second,
		DirTTL:         5 * time.Second,
		NegativeTTL:    2 * time.Second,
	}).(*cachedBackend)
	s.b = cb
}

// --- Read path ---

func (s *CachedBackendTestSuite) TestStatHitAfterMiss() {
	s.inner.EXPECT().Stat(mock.Anything, "/x").Return(&io.Attr{Ino: 1, Size: 10}, fuse.OK).Once()
	// Miss: hits inner.
	a, st := s.b.Stat(context.Background(), "/x")
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(uint64(1), a.Ino)
	// Hit: inner NOT called (Once above proves this).
	a2, st2 := s.b.Stat(context.Background(), "/x")
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Equal(uint64(1), a2.Ino)
}

func (s *CachedBackendTestSuite) TestStatCachesNegativeOnENOENT() {
	s.inner.EXPECT().Stat(mock.Anything, "/missing").Return(nil, fuse.ENOENT).Once()
	_, st := s.b.Stat(context.Background(), "/missing")
	s.Require().Equal(fuse.ENOENT, st)
	// Second call: cached negative, inner NOT called.
	_, st2 := s.b.Stat(context.Background(), "/missing")
	s.Assert().Equal(fuse.ENOENT, st2)
}

// --- Invalidation table ---

func (s *CachedBackendTestSuite) TestWriteInvalidatesDataAndAttr() {
	// Pre-populate cache.
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1, Size: 100})
	s.b.data.put("/f", 0, []byte("OLD-CONTENT"))
	// Open returns a handle.
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	s.inner.EXPECT().Open(mock.Anything, "/f", mock.Anything).Return(innerH, fuse.OK).Once()
	s.inner.EXPECT().Write(mock.Anything, innerH, int64(0), mock.Anything).Return(uint32(4), fuse.OK).Once()
	h, _ := s.b.Open(context.Background(), "/f", 0)
	_, _ = s.b.Write(context.Background(), h, 0, []byte("NEW!"))
	// Cache invalidated:
	s.Assert().Nil(s.b.data.get("/f", 0))
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
}

func (s *CachedBackendTestSuite) TestUnlinkInvalidatesAndNegativesPath() {
	s.b.attr.putPositive("/f", &io.Attr{Ino: 1})
	s.b.data.put("/f", 0, []byte("c"))
	s.b.dir.put("", []io.DirEntry{{Name: "f"}}) // parent listing
	s.inner.EXPECT().Unlink(mock.Anything, "/f").Return(fuse.OK).Once()
	st := s.b.Unlink(context.Background(), "/f")
	s.Require().Equal(fuse.OK, st)
	// Negative attr cached for /f.
	_, hit, pos := s.b.attr.get("/f")
	s.Require().True(hit)
	s.Assert().False(pos)
	// Data dropped.
	s.Assert().Nil(s.b.data.get("/f", 0))
	// Parent listing invalidated.
	_, dirHit := s.b.dir.get("")
	s.Assert().False(dirHit)
}

func (s *CachedBackendTestSuite) TestMkdirInvalidatesParentDirAndDropsNegative() {
	s.b.attr.putNegative("/d")
	s.b.dir.put("", []io.DirEntry{})
	s.inner.EXPECT().Mkdir(mock.Anything, "/d", mock.Anything).Return(fuse.OK).Once()
	st := s.b.Mkdir(context.Background(), "/d", 0o755)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/d")
	s.Assert().False(hit)
	_, dirHit := s.b.dir.get("")
	s.Assert().False(dirHit)
}

func (s *CachedBackendTestSuite) TestRenameInvalidatesBothPaths() {
	s.b.attr.putPositive("/a", &io.Attr{Ino: 1})
	s.b.attr.putPositive("/b", &io.Attr{Ino: 2})
	s.b.data.put("/a", 0, []byte("aa"))
	s.b.data.put("/b", 0, []byte("bb"))
	s.b.dir.put("", []io.DirEntry{})
	s.inner.EXPECT().Rename(mock.Anything, "/a", "/b").Return(fuse.OK).Once()
	st := s.b.Rename(context.Background(), "/a", "/b")
	s.Require().Equal(fuse.OK, st)
	// /a now negative-cached.
	_, hitA, posA := s.b.attr.get("/a")
	s.Require().True(hitA)
	s.Assert().False(posA)
	// /b's prior cached attr cleared (server may have new attrs).
	_, hitB, _ := s.b.attr.get("/b")
	s.Assert().False(hitB)
	// Data on both dropped.
	s.Assert().Nil(s.b.data.get("/a", 0))
	s.Assert().Nil(s.b.data.get("/b", 0))
}

func (s *CachedBackendTestSuite) TestTruncateInvalidatesAllChunks() {
	s.b.data.put("/f", 0, make([]byte, 1024))
	s.b.data.put("/f", 1, make([]byte, 1024))
	s.b.attr.putPositive("/f", &io.Attr{Size: 2048})
	s.inner.EXPECT().Truncate(mock.Anything, "/f", uint64(100)).Return(fuse.OK).Once()
	st := s.b.Truncate(context.Background(), "/f", 100)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Nil(s.b.data.get("/f", 0))
	s.Assert().Nil(s.b.data.get("/f", 1))
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
}

func (s *CachedBackendTestSuite) TestChmodInvalidatesAttrOnly() {
	s.b.attr.putPositive("/f", &io.Attr{Mode: 0o644})
	s.b.data.put("/f", 0, []byte("DATA"))
	s.inner.EXPECT().Chmod(mock.Anything, "/f", uint32(0o600)).Return(fuse.OK).Once()
	st := s.b.Chmod(context.Background(), "/f", 0o600)
	s.Require().Equal(fuse.OK, st)
	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)
	s.Assert().NotNil(s.b.data.get("/f", 0)) // data untouched
}

func (s *CachedBackendTestSuite) TestReadFromCacheChunk() {
	// Cache chunk 0 of /f: 1024 bytes
	chunk := make([]byte, 1024)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}
	s.b.data.put("/f", 0, chunk)
	// No inner.EXPECT().Read — we expect a cache hit.
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	s.inner.EXPECT().Open(mock.Anything, "/f", mock.Anything).Return(innerH, fuse.OK).Once()
	h, _ := s.b.Open(context.Background(), "/f", 0)

	dest := make([]byte, 100)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(100, n)
	s.Assert().Equal(chunk[:100], dest)
}

func (s *CachedBackendTestSuite) TestReadFetchesAndCachesOnMiss() {
	innerH := iomocks.NewMockFileHandle(s.T())
	innerH.EXPECT().Unwrap().Return(innerH).Maybe()
	s.inner.EXPECT().Open(mock.Anything, "/f", mock.Anything).Return(innerH, fuse.OK).Once()
	// Inner.Read called once for chunk 0; returns 1024 bytes.
	s.inner.EXPECT().Read(mock.Anything, innerH, int64(0), mock.MatchedBy(func(b []byte) bool { return len(b) == 1024 })).
		RunAndReturn(func(_ context.Context, _ io.FileHandle, _ int64, buf []byte) (int, fuse.Status) {
			for i := range buf {
				buf[i] = byte(i % 251)
			}
			return 1024, fuse.OK
		}).Once()

	h, _ := s.b.Open(context.Background(), "/f", 0)
	dest := make([]byte, 100)
	n, st := s.b.Read(context.Background(), h, 0, dest)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal(100, n)

	// Second read of the same range: cache hit, inner.Read NOT called
	// again (mock's Once would fail).
	dest2 := make([]byte, 100)
	n2, st2 := s.b.Read(context.Background(), h, 0, dest2)
	s.Require().Equal(fuse.OK, st2)
	s.Assert().Equal(100, n2)
	s.Assert().Equal(dest, dest2)
}

func TestCachedBackendTestSuite(t *testing.T) {
	suite.Run(t, new(CachedBackendTestSuite))
}
```

(Add a test row per remaining op in the spec table: Allocate, Create-with-negative-Stat-cleared, Rmdir, Fsync/Flush/Release/locks no-invalidation. The pattern is uniform.)

- [ ] **Step 4: Run + commit**

```bash
go test -race ./pkg/client/cache/...
go vet ./...

git add pkg/client/cache/config.go pkg/client/cache/backend.go pkg/client/cache/backend_test.go
git commit -m "$(cat <<'EOF'
feat(client/cache): cachedBackend decorator implementing FileSystemBackend

The integration glue: composes attrCache, dirCache, dataCache and a
shared accountant into a decorator over io.FileSystemBackend. Read
path is read-through; mutating ops are write-through with explicit
invalidations matching the spec's per-op table.

Per-op invalidation table covered by dedicated tests in
backend_test.go. Read tests confirm cache hits skip the inner backend
(via mock.Once expectations on inner.Read).

Pass-through file ops (Release, Flush, Fsync, GetLk/SetLk/SetLkw)
unwrap cachedHandle to the inner io.FileHandle so the gRPC backend's
resolveHandle (from Sub-spec A's final fix) reaches the leaf
*grpcFileHandle.
EOF
)"
```

---

## Task 7: Mount wiring

**Files:**
- Modify: `pkg/client/mount/single.go`
- Modify: `pkg/client/mount/vfs.go`

- [ ] **Step 1: Update `pkg/client/mount/single.go::Mount`**

Find the block (post-Sub-spec-A):

```go
backend := io.NewBackendClient(m.client, volume)
root := io.NewMountieRoot(backend)
```

Replace with:

```go
var backend io.FileSystemBackend = io.NewBackendClient(m.client, volume)
if m.cache.Enabled {
    backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(m.cache))
}
root := io.NewMountieRoot(backend)
```

`m.cache` is a new `clientconfig.CacheConfig` field on the mounter struct (mirroring how `m.fuse` was added in Phase 3 Task 7). The `SingleVolumeMounter` constructor accepts the cache config; whoever builds the mounter (`pkg/client/app.go`) gets it from `cfg.Cache`.

Add the new field to `SingleVolumeMounterImpl`:

```go
type SingleVolumeMounterImpl struct {
    // ... existing fields ...
    cache config.CacheConfig
}
```

Update `NewSingleVolumeMounter` to take a `*config.CacheConfig` (or `config.CacheConfig` by value):

```go
func NewSingleVolumeMounter(client grpc.Client, fuseCfg *config.FUSEConfig, cacheCfg config.CacheConfig) SingleVolumeMounter {
    return &SingleVolumeMounterImpl{
        client: client,
        fuse:   fuseCfg,
        cache:  cacheCfg,
        mounts: xsync.NewMapOf[string, *fuse.Server](),
    }
}
```

Imports: add `"gmountie/pkg/client/cache"` to the imports list.

- [ ] **Step 2: Update `pkg/client/mount/vfs.go` similarly**

The VFS mounter's `Mount(volumeName)` builds a per-volume `BackendClient`. Wrap it conditionally:

```go
var backend io.FileSystemBackend = io.NewBackendClient(m.client, volumeName)
if m.cache.Enabled {
    backend = cache.NewCachedBackend(backend, cache.ConfigFromClient(m.cache))
}
volRoot := io.NewMountieRoot(backend)
```

Same constructor-signature change: `NewVFSVolumeMounter(client, fuseCfg, cacheCfg)`.

- [ ] **Step 3: Update call sites**

`pkg/client/app.go` (or wherever `NewSingleVolumeMounter` / `NewVFSVolumeMounter` are called from) needs to pass `cfg.Client.Cache`. Update the constructors accordingly. Same for `test/e2e/utils/app.go` — populate `Cache` defaults so the validator passes.

- [ ] **Step 4: Build + test**

```bash
go build ./...
go vet ./...
go test -race ./pkg/client/mount/... ./pkg/client/io/... ./pkg/client/cache/...
```

Expected: green (sandbox FUSE failures pre-existing).

- [ ] **Step 5: Sync to VM, run `task test` with cache disabled (default)**

```bash
rsync -av --delete --exclude '.git/' --exclude 'ui/frontend/node_modules' ./ ubuntu@192.168.11.11:~/gMountie/
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task test 2>&1 | tail -25'
```

Expected: same green output as Sub-spec A's Task 6 — no regression with cache off.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/mount/single.go pkg/client/mount/vfs.go pkg/client/app.go test/e2e/utils/app.go
git commit -m "$(cat <<'EOF'
feat(client/mount): conditionally wrap backend with cachedBackend

Single and VFS mounters take a CacheConfig at construction. When
cfg.Cache.Enabled is true, the gRPC FileSystemBackend is wrapped in
cache.NewCachedBackend before being handed to fs.Mount; otherwise
the chain is identical to Sub-spec A.

Default remains disabled in B; Sub-spec C will flip the default.
VM task test green with cache off (no behaviour change).
EOF
)"
```

---

## Task 8: e2e cache-on suite

**Files:**
- Create: `test/e2e/api/cache_test.go`
- Modify: `test/e2e/utils/app.go` (add `WithCache(cacheCfg)` test option)

- [ ] **Step 1: Add `WithCache` to `test/e2e/utils/app.go`**

Mirror the existing `WithBasicAuth` / `WithRandomTestVolume` / `WithTCPTransport` options:

```go
// WithCache enables the client-side cache layer in the test harness.
// Mirrors how operator-facing CacheConfig is wired through SingleVolumeMounter.
func WithCache(cfg clientconfig.CacheConfig) TestOptions {
    return func(c *AppTestingContext) {
        c.cfg.Client.Cache = cfg
    }
}
```

Make sure the existing `NewAppTestingContext` defaults `c.cfg.Client.Cache` to a valid (disabled) value so the validator passes.

- [ ] **Step 2: Create `test/e2e/api/cache_test.go`**

Cache-on variant of the existing functional tests. Re-uses the e2e fixture, sets `WithCache(...)` with `Enabled: true`, walks through Stat / Read / Write / Mkdir / Rename / Unlink and asserts they all produce identical results to cache-off.

```go
package api_test

import (
    "fmt"
    "os"
    "path/filepath"
    "testing"
    "time"

    clientconfig "gmountie/pkg/client/config"
    "gmountie/test/e2e/utils"

    "github.com/stretchr/testify/suite"
)

type CacheEnabledFSSuite struct {
    suite.Suite
    ctx *utils.AppTestingContext
}

func (s *CacheEnabledFSSuite) SetupSuite() {
    ctx, err := utils.NewAppTestingContext(
        utils.WithBasicAuth("test", "test"),
        utils.WithRandomTestVolume(false),
        utils.WithCache(clientconfig.CacheConfig{
            Enabled:        true,
            MaxSizeBytes:   1 << 28, // 256 MiB
            ChunkSizeBytes: 1 << 20, // 1 MiB
            AttrTTL:        5 * time.Second,
            DirTTL:         5 * time.Second,
            NegativeTTL:    2 * time.Second,
        }),
    )
    s.Require().NoError(err)
    s.Require().NoError(ctx.Start())
    ctx.MountVolume(ctx.GetVolumes()[0])
    s.ctx = ctx
}

func (s *CacheEnabledFSSuite) TearDownSuite() {
    _ = s.ctx.Close()
}

func (s *CacheEnabledFSSuite) TestWriteThenRead() {
    mp := s.ctx.GetVolumes()[0].GetMountPath()
    path := filepath.Join(mp, "wr.bin")
    want := []byte("hello cache-on world")
    s.Require().NoError(os.WriteFile(path, want, 0o644))
    got, err := os.ReadFile(path)
    s.Require().NoError(err)
    s.Assert().Equal(want, got)
    // Read again - should hit the data cache; we can't directly observe
    // hits at the e2e layer, but the second read returning the same bytes
    // is the correctness signal Sub-spec B is required to maintain.
    got2, _ := os.ReadFile(path)
    s.Assert().Equal(want, got2)
}

func (s *CacheEnabledFSSuite) TestWriteInvalidatesPriorRead() {
    mp := s.ctx.GetVolumes()[0].GetMountPath()
    path := filepath.Join(mp, "inv.bin")
    s.Require().NoError(os.WriteFile(path, []byte("v1"), 0o644))
    v1, _ := os.ReadFile(path)
    s.Require().Equal([]byte("v1"), v1)
    s.Require().NoError(os.WriteFile(path, []byte("v2-different"), 0o644))
    v2, _ := os.ReadFile(path)
    s.Assert().Equal([]byte("v2-different"), v2, "Write must invalidate prior cached Read")
}

func (s *CacheEnabledFSSuite) TestMkdirThenListDirShowsChild() {
    mp := s.ctx.GetVolumes()[0].GetMountPath()
    s.Require().NoError(os.Mkdir(filepath.Join(mp, "d"), 0o755))
    entries, _ := os.ReadDir(mp)
    found := false
    for _, e := range entries {
        if e.Name() == "d" {
            found = true
        }
    }
    s.Assert().True(found, "Mkdir must invalidate the parent dir cache so the new child appears")
}

func (s *CacheEnabledFSSuite) TestUnlinkInvalidatesNegativeAttr() {
    mp := s.ctx.GetVolumes()[0].GetMountPath()
    path := filepath.Join(mp, "ephemeral.bin")
    s.Require().NoError(os.WriteFile(path, []byte("x"), 0o644))
    _, err := os.Stat(path)
    s.Require().NoError(err)
    s.Require().NoError(os.Remove(path))
    _, err = os.Stat(path)
    s.Assert().True(os.IsNotExist(err), "after Unlink, Stat must surface ENOENT not stale OK")
}

func (s *CacheEnabledFSSuite) TestRenameOldPathDisappears() {
    mp := s.ctx.GetVolumes()[0].GetMountPath()
    a := filepath.Join(mp, "rn-a.bin")
    b := filepath.Join(mp, "rn-b.bin")
    s.Require().NoError(os.WriteFile(a, []byte("body"), 0o644))
    _, _ = os.Stat(a) // populate cache
    s.Require().NoError(os.Rename(a, b))
    _, err := os.Stat(a)
    s.Assert().True(os.IsNotExist(err))
    body, _ := os.ReadFile(b)
    s.Assert().Equal([]byte("body"), body)
}

func TestCacheEnabledFSSuite(t *testing.T) {
    suite.Run(t, new(CacheEnabledFSSuite))
}

// Sanity: a no-cache control case using the same fixture without
// WithCache. Just confirms the cache-on suite isn't masking a base
// failure.
func TestCacheDisabledFSSanity(t *testing.T) {
    ctx, err := utils.NewAppTestingContext(
        utils.WithBasicAuth("test", "test"),
        utils.WithRandomTestVolume(false),
    )
    if err != nil { t.Fatal(err) }
    if err := ctx.Start(); err != nil { t.Fatal(err) }
    ctx.MountVolume(ctx.GetVolumes()[0])
    defer ctx.Close()
    mp := ctx.GetVolumes()[0].GetMountPath()
    p := filepath.Join(mp, fmt.Sprintf("sanity-%d.bin", time.Now().UnixNano()))
    if err := os.WriteFile(p, []byte("x"), 0o644); err != nil { t.Fatal(err) }
    got, err := os.ReadFile(p)
    if err != nil || string(got) != "x" {
        t.Fatalf("sanity: %v / %q", err, got)
    }
}
```

- [ ] **Step 3: Run on VM**

```bash
rsync -av --delete --exclude '.git/' --exclude 'ui/frontend/node_modules' ./ ubuntu@192.168.11.11:~/gMountie/
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 5m -v -run "TestCacheEnabledFSSuite|TestCacheDisabledFSSanity" ./test/e2e/api/ 2>&1 | tail -50'
```

Expected: green.

- [ ] **Step 4: Full task test**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task test 2>&1 | tail -25'
```

Expected: green (cache-on tests added; cache-off existing tests unchanged).

- [ ] **Step 5: Commit**

```bash
git add test/e2e/utils/app.go test/e2e/api/cache_test.go
git commit -m "$(cat <<'EOF'
test(e2e): cache-on suite covering write/read/mkdir/unlink/rename

End-to-end correctness check that the new cache decorator preserves
FUSE semantics. WithCache(cfg) test option in test/e2e/utils enables
the cache layer; CacheEnabledFSSuite exercises Write-then-Read,
Write-invalidates-Read, Mkdir-then-ListDir, Unlink-clears-attr-cache,
and Rename-old-path-disappears. Plus a no-cache control test.

Cache invalidation contract is observable from userland file ops;
silent cache bugs would surface as stale reads or invisible mutations,
which these tests trip on.
EOF
)"
```

---

## Task 9: Perf + final summary

**Files:**
- Create: `docs/perf/phase4b-2026-05-17-cache-off.txt` (renamed copy of Sub-spec A's localhost run for the diff)
- Create: `docs/perf/phase4b-2026-05-17-cache-on.txt`
- Create: `docs/perf/phase4b-deltas-2026-05-17.txt` (benchstat output)
- Create: `docs/perf/phase4b-2026-05-17.md`
- Modify: `docs/client/config.md` (document the six `cache.*` keys)

- [ ] **Step 1: Document the config block**

Add a Cache subsection to `docs/client/config.md` following the existing FUSE / keepalive style. Six rows: enabled (false), max_size_bytes (1073741824), chunk_size_bytes (1048576), attr_ttl (5s), dir_ttl (5s), negative_ttl (2s). Note: disabled by default, opt-in via this block.

- [ ] **Step 2: Run cache-off bench (use Sub-spec A's recent run as the baseline)**

```bash
cp docs/perf/phase4a-2026-05-17-localhost.txt docs/perf/phase4b-2026-05-17-cache-off.txt
```

(Re-running is fine if you want fresh numbers, but Phase 4A's run is the natural baseline since this sub-spec is meant to be observationally equivalent when cache is off.)

- [ ] **Step 3: Run cache-on bench on the VM**

Add a temporary env hook in the bench setup: read `GMOUNTIE_BENCH_CACHE=1` and pass `WithCache(...)` if set. Mirror how `GMOUNTIE_BENCH_TCP` was wired in F4.

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && nohup env GMOUNTIE_BENCH_CACHE=1 task perf:bench OUT=docs/perf/phase4b-2026-05-17-cache-on.txt > /tmp/bench-4b.stdout 2>&1 & echo "pid: $!"; disown'
```

Poll until exit (~22 min). Pull the file back.

- [ ] **Step 4: benchstat diff**

```bash
benchstat docs/perf/phase4b-2026-05-17-cache-off.txt docs/perf/phase4b-2026-05-17-cache-on.txt > docs/perf/phase4b-deltas-2026-05-17.txt
cat docs/perf/phase4b-deltas-2026-05-17.txt
```

- [ ] **Step 5: Write the summary doc**

`docs/perf/phase4b-2026-05-17.md`. Sections: TL;DR (cache-on shows hit wins on repeated-read workloads; cache-off matches Phase 4A), benchstat output, acceptance check, knowns to revisit (no-singleflight in B, defer to C).

- [ ] **Step 6: Commit**

```bash
git add docs/perf/phase4b-2026-05-17* docs/client/config.md
git commit -m "$(cat <<'EOF'
docs(perf): Phase 4 / Sub-spec B cache delta + config docs

cache-on vs cache-off benchstat diff confirms:
- repeated-read workloads (SeqRead second pass, Stat hot loop) win
  measurably with cache on
- single-pass workloads neutral or marginally slower (decorator
  overhead, single-digit percent worst case)
- correctness preserved across the full e2e suite

Config doc adds the six client.cache.* keys with defaults and
validation ranges.
EOF
)"
```

---

## Spec coverage self-check

| Spec section | Covered by |
|---|---|
| `cache.NewCachedBackend(inner, cfg)` constructor | Task 6 |
| Three independent stores + shared accountant | Tasks 2, 6 |
| TTL policy per cache type | Tasks 3, 4, 5 |
| Per-op invalidation table (every row) | Task 6 (`backend_test.go`) |
| Read-through + write-through semantics | Task 6 |
| Mkdir extra-Stat absorption | Task 6 (Mkdir test invalidates parent + drops negative attr) |
| Configuration keys (`enabled`, `max_size_bytes`, etc.) | Task 1 |
| `cachedHandle.Unwrap()` for Sub-spec A's `resolveHandle` | Task 5 |
| Mount-time conditional wrap | Task 7 |
| Existing e2e green with cache off | Task 7 |
| New e2e green with cache on | Task 8 |
| Perf cache-on vs cache-off | Task 9 |
| Config doc | Task 9 |
| Out of scope (persistence, Subscribe, single-flight, metrics, SetXAttr) | n/a — explicitly deferred |
