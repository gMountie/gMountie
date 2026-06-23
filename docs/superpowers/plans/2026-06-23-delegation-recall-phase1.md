# Write-Delegation + Recall (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the delegation + recall *coherence layer* — a client takes a write-delegation rooted at a subtree, the server arbitrates non-overlapping grants and recalls a holder when another client touches its subtree — with `close()` still flushing durably (no WAL, nothing deferred, so it cannot lose data).

**Architecture:** A self-contained server `delegation` package (arbiter + delegation table + cooldown table + recall registry) is injected via `AppContext` and consulted by the mutating gRPC handlers; recall rides a new dedicated bidi stream on `RpcFs`. On the client a self-contained `delegation` package provides a `Manager` (owns grants, the recall goroutine, and the `IsDelegated` oracle) wired as a **named backend layer at the reserved `posWritePath` slot**; the cache consults the oracle in its revalidation fast-path, and the transport piggybacks delegation requests/grants on existing mutating RPCs via a `DelegationHook`. No backward compatibility is required — the proto is changed freely.

**Tech Stack:** Go, gRPC (`pkg/proto`, regenerated via `task gen:grpc`), `protoc` + `protoc-gen-go`/`-go-grpc` (go.mod `tool` directives), `puzpuzpuz/xsync/v3` for concurrent maps, `testify/suite` for tests, go-fuse v2 on the client FUSE adapters. Module path `go.gmountie.dev/gmountie`.

## Global Constraints

- **No backward compatibility.** Change the proto freely; clients and servers upgrade together (there is already a "BREAKING CHANGE: clients and servers must upgrade together" note in `api/proto/fs.proto`).
- **Phase 1 defers NOTHING.** `close()`/`Flush`/`Release` stay durable exactly as today. No WAL, no write buffering, no deferred close. The delegation only governs *coherence* (skip-revalidation + recall), never durability.
- **Recall rule (one principle):** recall a held write-delegation only when continuing to hold it would expose incoherent state. In Phase 1 that means **remote WRITE (any mutating op) recalls; remote READ does not** (there is no deferred data for a reader to miss). Only mutating handlers consult the arbiter.
- **Handoff barrier is a lock obligation, never transport ordering.** The server must not let a contender's mutating op proceed against a delegated subtree until the holder's recall is acked (or times out). Enforce at the arbiter's per-region state machine — NOT by stream/message ordering.
- **Never hold the arbiter table mutex across a recall round-trip.** Detect contention under the lock, transition the region to `RECALLING`, release the lock, perform the recall RPC + ack wait, re-acquire, transition to `FREE`. Holding the lock across the RTT deadlocks against ack finalization.
- **Thrash prevention is SERVER-SIDE** (cooldown table, exponential + capped). The client re-requests freely (piggybacked); correctness never depends on the client honoring `retry_after`.
- **Logging** via `go.gmountie.dev/gmountie/pkg/utils/log` (`log.Log`); **errors** wrapped with `github.com/pkg/errors`.
- **Tests are testify suites** (methods on a `suite.Suite`), not standalone `func TestX`. Root-gated server FS tests skip when not root (mirror existing `pkg/server/io` suites).
- **Never hand-edit `internal/mocks/`** — regenerate via `task gen:mocks` after any interface change.
- **Mutating ops carry `session_id` + `request_id` already**; recall-on-contention keys on the **session_id** of the op (the contender) vs the delegation's owner session.

---

## File Structure

**New — server (`pkg/server/delegation/`, self-contained package):**
- `table.go` — `delegationTable`: path-prefix containment index (root → owner). Overlap/containment queries.
- `cooldown.go` — `cooldownTable`: recently-recalled roots → expiry, exponential + capped, TTL'd + LRU-capped.
- `arbiter.go` — `Arbiter`: composes table + cooldown + the per-region state machine; `Request`, `OnMutation`, `ReleaseSession`.
- `recall.go` — `RecallRegistry`: per-session recall stream registration + `Recall(ownerSession, root)` push-and-await-ack with timeout.
- `*_test.go` — suites for each.

**New — server controller:**
- `pkg/server/controller/recall.go` — the `Recall` bidi stream handler (registers/deregisters the stream in `RecallRegistry`, pumps acks).

**New — client (`pkg/client/backend/delegation/`, self-contained package):**
- `writeset.go` — `writeSet`: tracks recently-written paths, computes the lowest-common-ancestor subtree to request, promotes upward when writes scatter.
- `manager.go` — `Manager`: holds active grants, the `IsDelegated(path)` oracle, the recall-stream goroutine, the cache-invalidator ref, `Close`. Implements `transport.DelegationHook`.
- `layer.go` — `NewLayer(inner, mgr)`: the `posWritePath` backend layer (records write-set on mutating ops, forces cross-subtree rename synchronous, owns `mgr` lifecycle via `Close`).
- `*_test.go` — suites.

**Modified — proto:**
- `api/proto/fs.proto` — add `DelegationRequest`, `DelegationGrant`, `RecallMsg`, `RecallAck` messages; add a `DelegationRequest delegation` field to mutating *requests* and a `DelegationGrant grant` field to their *replies*; add `rpc Recall (stream RecallAck) returns (stream RecallMsg)` to `RpcFs`.
- `api/proto/file.proto` — same piggyback fields on `CreateRequest/Reply`, the first `WriteFrame`/`WriteReply`, `AllocateRequest/Reply`.

**Modified — server wiring:**
- `pkg/server/app.go` — construct `Arbiter` + `RecallRegistry`, add to `AppContext`, pass to controllers, wire `SessionManager` `OnReap → arbiter.ReleaseSession`.
- `pkg/server/controller/fs.go` + `file.go` — `NewGrpcServer`/`NewRpcFileServer` gain an `*delegation.Arbiter`; mutating handlers call `arbiter.OnMutation` (recall-on-contention) before the op and `arbiter.Request` (grant) after, stamping the reply.
- `pkg/server/service/session.go` — `SessionManagerOptions.OnReap func(sessionID string)`; reap paths (`MarkDisconnected` timer, `ReapIf`, `Stop`) invoke it.
- `pkg/server/metrics/metrics.go` — `DelegationGrantsActive` (gauge), `DelegationRecalls` (counter), `DelegationCooldownTrips` (counter).

**Modified — client wiring:**
- `pkg/client/mount/single.go` — build the `delegation.Manager`, add a `posWritePath` layer, pass the oracle into `NewCachedBackend`, give the transport the `DelegationHook`.
- `pkg/client/backend/cache/backend.go` — `NewCachedBackend` accepts an optional `DelegationOracle`; the revalidation fast-path also short-circuits when `oracle.IsDelegated(path)`.
- `pkg/client/backend/transport/backend_grpc.go` — optional `DelegationHook`: stamp `delegation` on mutating requests, deliver `grant` from replies back to the hook.

---

## Task 1: Proto — delegation messages, piggyback fields, Recall stream

**Files:**
- Modify: `api/proto/fs.proto`
- Modify: `api/proto/file.proto`
- Regenerate: `pkg/proto/*.pb.go` via `task gen:grpc`

**Interfaces:**
- Produces (wire): `DelegationRequest{ string root }`, `DelegationGrant{ string granted_root; repeated string excluded_paths; uint64 retry_after_ms }`, `RecallMsg{ string root; uint64 recall_id }`, `RecallAck{ uint64 recall_id; bool done }`; field `DelegationRequest delegation` on mutating requests; field `DelegationGrant grant` on their replies; `rpc Recall (stream RecallAck) returns (stream RecallMsg)` on `RpcFs`.

- [ ] **Step 1: Add the four messages + the Recall RPC to `fs.proto`.**

In `api/proto/fs.proto`, add near the top (after `FileTime`):

```proto
// DelegationRequest is piggybacked on a mutating op: "while you're processing
// this, please grant me a write-delegation rooted here." root is a volume-
// relative path. Empty/absent = no request (the op is still arbitrated for
// recall-on-contention regardless).
message DelegationRequest {
  string root = 1;
}

// DelegationGrant is piggybacked on a mutating reply. granted_root may be
// SHORTER than the requested root (server carved around cooling/!owned paths);
// empty granted_root = denied. excluded_paths are sub-paths under granted_root
// the client must treat as undelegated. retry_after_ms hints when to re-request
// a denied/cooling root (advisory only).
message DelegationGrant {
  string          granted_root   = 1;
  repeated string excluded_paths = 2;
  uint64          retry_after_ms = 3;
}

// RecallMsg is pushed server->client on the Recall stream: "release your
// delegation covering root; flush+invalidate, then ack." recall_id correlates
// the ack.
message RecallMsg {
  string root      = 1;
  uint64 recall_id = 2;
}

// RecallAck is the client's confirmation that the recall is complete (cache
// invalidated, delegation dropped). done is always true in Phase 1 (no partial
// flush); it exists for Phase 2 progress reporting.
message RecallAck {
  uint64 recall_id = 1;
  bool   done      = 2;
}
```

Add the RPC inside `service RpcFs { ... }`:

```proto
  // Recall is the coherence control plane: the client opens this bidi stream
  // once per mount; the server pushes RecallMsg when another client contends a
  // delegated subtree; the client replies RecallAck when it has released it.
  rpc Recall (stream RecallAck) returns (stream RecallMsg);
```

- [ ] **Step 2: Add piggyback fields to the mutating fs requests/replies.**

For each of these `fs.proto` messages, append the field at the next free tag number (shown):

- `MkdirRequest`: `DelegationRequest delegation = 7;` · `MkdirReply`: `DelegationGrant grant = 3;`
- `RmdirRequest`: `DelegationRequest delegation = 6;` · `RmdirReply`: `DelegationGrant grant = 2;`
- `RenameRequest`: `DelegationRequest delegation = 7;` · `RenameReply`: `DelegationGrant grant = 2;`
- `UnlinkRequest`: `DelegationRequest delegation = 6;` · `UnlinkReply`: `DelegationGrant grant = 2;`
- `SetAttrRequest`: `DelegationRequest delegation = 13;` · `SetAttrReply`: `DelegationGrant grant = 3;`
- `SymlinkRequest`: `DelegationRequest delegation = 7;` · `SymlinkReply`: `DelegationGrant grant = 3;`
- `SetXAttrRequest`: `DelegationRequest delegation = 9;` · `SetXAttrReply`: `DelegationGrant grant = 2;`
- `RemoveXAttrRequest`: `DelegationRequest delegation = 7;` · `RemoveXAttrReply`: `DelegationGrant grant = 2;`

- [ ] **Step 3: Add piggyback fields to the mutating file requests/replies.**

In `api/proto/file.proto`, append at the next free tag:
- `CreateRequest`: `DelegationRequest delegation = <next>;` · `CreateReply`: `DelegationGrant grant = <next>;`
- `WriteFrame` (first frame only carries it): `DelegationRequest delegation = <next>;` · `WriteReply`: `DelegationGrant grant = <next>;`
- `AllocateRequest`: `DelegationRequest delegation = <next>;` · `AllocateReply`: `DelegationGrant grant = <next>;`

(Read each message first to pick the correct next tag — do NOT reuse an existing tag. `file.proto` imports `common.proto`; `DelegationRequest`/`DelegationGrant` live in `fs.proto` which is the same `package gmountie`, so no extra import is needed — they share the generated `pkg/proto` package.)

- [ ] **Step 4: Regenerate stubs and build.**

Run: `cd <repo> && task gen:grpc && go build ./...`
Expected: regenerates `pkg/proto/fs.pb.go`, `fs_grpc.pb.go`, `file.pb.go`; build succeeds (new `RpcFs_RecallServer`/`RpcFs_RecallClient` stream types exist). `RpcServerImpl` will now FAIL to satisfy `proto.RpcFsServer` until Task 6 adds `Recall` — that's expected; if `go build ./pkg/server/...` breaks here, add a temporary `func (r *RpcServerImpl) Recall(proto.RpcFs_RecallServer) error { return status.Error(codes.Unimplemented, "wip") }` and delete it in Task 6.

- [ ] **Step 5: Commit.**

```bash
git add api/proto/fs.proto api/proto/file.proto pkg/proto/
git commit -m "feat(proto): add delegation request/grant fields and Recall bidi stream"
```

---

## Task 2a: Server delegation table — containment + narrower-grant carving

**Files:**
- Create: `pkg/server/delegation/table.go`
- Test: `pkg/server/delegation/table_test.go`

**Interfaces:**
- Produces: `type ownerKey = string` (the owner's session_id); `delegationTable` with
  `grant(owner ownerKey, root string) (granted string, excluded []string, ok bool)`,
  `ownerOf(path string) (owner ownerKey, root string, ok bool)`,
  `releaseOwner(owner ownerKey)`, `release(root string)`.
- Used by Task 2b/3/4 (the `Arbiter`).

Path model: volume-relative, `/`-joined, no trailing slash, `""` = volume root. "A contains B" ⇔ `B == A || strings.HasPrefix(B, A+"/")` (with `""` containing everything).

- [ ] **Step 1: Write the failing test.**

```go
package delegation

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type TableSuite struct{ suite.Suite }

func TestTableSuite(t *testing.T) { suite.Run(t, new(TableSuite)) }

func (s *TableSuite) TestGrantDisjointSucceeds() {
	tbl := newDelegationTable()
	g1, _, ok1 := tbl.grant("sessA", "teamA")
	g2, _, ok2 := tbl.grant("sessB", "teamB")
	s.True(ok1)
	s.True(ok2)
	s.Equal("teamA", g1)
	s.Equal("teamB", g2)
}

func (s *TableSuite) TestGrantNarrowsAroundForeignSubtree() {
	tbl := newDelegationTable()
	tbl.grant("sessB", "proj/vendor") // B owns a sub-path
	// A asks for the parent; it must be carved to exclude B's subtree.
	granted, excluded, ok := tbl.grant("sessA", "proj")
	s.True(ok)
	s.Equal("proj", granted)
	s.Equal([]string{"proj/vendor"}, excluded)
}

func (s *TableSuite) TestGrantDeniedWhenContainedByForeign() {
	tbl := newDelegationTable()
	tbl.grant("sessB", "proj")
	_, _, ok := tbl.grant("sessA", "proj/src") // fully inside B's root
	s.False(ok)
}

func (s *TableSuite) TestOwnerOfFindsCoveringRoot() {
	tbl := newDelegationTable()
	tbl.grant("sessA", "proj/src")
	owner, root, ok := tbl.ownerOf("proj/src/main.go")
	s.True(ok)
	s.Equal("sessA", owner)
	s.Equal("proj/src", root)
}

func (s *TableSuite) TestReleaseOwnerClearsAll() {
	tbl := newDelegationTable()
	tbl.grant("sessA", "x")
	tbl.grant("sessA", "y")
	tbl.releaseOwner("sessA")
	_, _, ok := tbl.ownerOf("x/file")
	s.False(ok)
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/server/delegation/ -run TestTableSuite -v`
Expected: FAIL — `newDelegationTable` undefined.

- [ ] **Step 3: Implement `table.go`.**

```go
// Package delegation implements the server-side write-delegation arbiter:
// it tracks which session holds a write-delegation over which subtree, grants
// non-overlapping roots (carving around foreign subtrees), and drives recalls
// on contention. Phase 1 governs coherence only — no durability semantics.
package delegation

import (
	"sort"
	"strings"
)

// contains reports whether root a contains path b (a==b, b under a/, or a=="").
func contains(a, b string) bool {
	if a == "" {
		return true
	}
	return a == b || strings.HasPrefix(b, a+"/")
}

type entry struct {
	owner string
	root  string
}

// delegationTable is the containment index. Not safe for concurrent use; the
// Arbiter serializes access under its own mutex.
type delegationTable struct {
	entries []entry // invariant: roots are pairwise non-overlapping
}

func newDelegationTable() *delegationTable { return &delegationTable{} }

// ownerOf returns the entry whose root contains path, if any.
func (t *delegationTable) ownerOf(path string) (owner, root string, ok bool) {
	for _, e := range t.entries {
		if contains(e.root, path) {
			return e.owner, e.root, true
		}
	}
	return "", "", false
}

// grant tries to grant owner a delegation rooted at root. Rules:
//   - if root is contained by a *foreign* root -> denied (ok=false).
//   - if root contains foreign roots -> granted, with those foreign roots
//     returned as excluded (carve around them).
//   - roots owned by the SAME owner under root are absorbed (re-rooted upward).
func (t *delegationTable) grant(owner, root string) (granted string, excluded []string, ok bool) {
	var kept []entry
	for _, e := range t.entries {
		switch {
		case e.owner == owner && contains(root, e.root):
			// absorbed into the wider same-owner grant; drop it.
			continue
		case e.owner != owner && contains(e.root, root):
			// requested root sits inside a foreign delegation -> deny.
			return "", nil, false
		case e.owner != owner && contains(root, e.root):
			// foreign delegation sits inside the requested root -> carve.
			excluded = append(excluded, e.root)
			kept = append(kept, e)
		default:
			kept = append(kept, e)
		}
	}
	kept = append(kept, entry{owner: owner, root: root})
	t.entries = kept
	sort.Strings(excluded)
	return root, excluded, true
}

// release drops the entry with exactly this root (any owner).
func (t *delegationTable) release(root string) {
	kept := t.entries[:0]
	for _, e := range t.entries {
		if e.root != root {
			kept = append(kept, e)
		}
	}
	t.entries = kept
}

// releaseOwner drops every entry owned by owner (session reap).
func (t *delegationTable) releaseOwner(owner string) {
	kept := t.entries[:0]
	for _, e := range t.entries {
		if e.owner != owner {
			kept = append(kept, e)
		}
	}
	t.entries = kept
}
```

- [ ] **Step 4: Run the tests to verify they pass.**

Run: `go test ./pkg/server/delegation/ -run TestTableSuite -v`
Expected: PASS (all 5).

- [ ] **Step 5: Commit.**

```bash
git add pkg/server/delegation/table.go pkg/server/delegation/table_test.go
git commit -m "feat(delegation): server delegation table with containment + carving"
```

---

## Task 2b: Server cooldown table — exponential, capped, TTL'd

**Files:**
- Create: `pkg/server/delegation/cooldown.go`
- Test: `pkg/server/delegation/cooldown_test.go`

**Interfaces:**
- Produces: `cooldownTable` with `trip(root string, now time.Time)`, `cooling(root string, now time.Time) bool`, `sweep(now time.Time)`. Time is injected (no `time.Now()` inside — keeps tests deterministic and matches the no-`Date.now` discipline).

- [ ] **Step 1: Write the failing test.**

```go
package delegation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type CooldownSuite struct{ suite.Suite }

func TestCooldownSuite(t *testing.T) { suite.Run(t, new(CooldownSuite)) }

func (s *CooldownSuite) TestTripThenCoolingThenExpires() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("hot", t0)
	s.True(c.cooling("hot", t0.Add(500*time.Millisecond)))   // inside window
	s.False(c.cooling("hot", t0.Add(2*time.Second)))         // window passed
	s.False(c.cooling("cold", t0))                           // never tripped
}

func (s *CooldownSuite) TestExponentialBackoffCaps() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: 4 * time.Second, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("p", t0)                       // window 1s
	c.trip("p", t0.Add(time.Second))      // -> 2s
	c.trip("p", t0.Add(3*time.Second))    // -> 4s
	c.trip("p", t0.Add(7*time.Second))    // -> capped at 4s
	base := t0.Add(7 * time.Second)
	s.True(c.cooling("p", base.Add(3*time.Second)))  // still inside 4s
	s.False(c.cooling("p", base.Add(5*time.Second))) // past capped 4s
}

func (s *CooldownSuite) TestSweepEvictsExpired() {
	c := newCooldownTable(cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 1024})
	t0 := time.Unix(0, 0)
	c.trip("a", t0)
	c.sweep(t0.Add(time.Hour))
	s.Equal(0, c.len())
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/server/delegation/ -run TestCooldownSuite -v`
Expected: FAIL — `newCooldownTable` undefined.

- [ ] **Step 3: Implement `cooldown.go`.**

```go
package delegation

import "time"

type cooldownConfig struct {
	Base time.Duration // first cooldown window
	Max  time.Duration // cap on the window
	Cap  int           // max tracked roots (LRU-ish eviction via sweep + cap)
}

type coolEntry struct {
	until  time.Time
	window time.Duration
}

// cooldownTable records recently-recalled roots so the arbiter denies re-grant
// within a growing window. Not safe for concurrent use (Arbiter serializes).
type cooldownTable struct {
	cfg     cooldownConfig
	entries map[string]coolEntry
}

func newCooldownTable(cfg cooldownConfig) *cooldownTable {
	return &cooldownTable{cfg: cfg, entries: make(map[string]coolEntry)}
}

func (c *cooldownTable) len() int { return len(c.entries) }

// trip starts (or extends, exponentially) the cooldown for root.
func (c *cooldownTable) trip(root string, now time.Time) {
	w := c.cfg.Base
	if e, ok := c.entries[root]; ok {
		w = e.window * 2
		if w > c.cfg.Max {
			w = c.cfg.Max
		}
	}
	if len(c.entries) >= c.cfg.Cap {
		c.evictOldest()
	}
	c.entries[root] = coolEntry{until: now.Add(w), window: w}
}

// cooling reports whether root is still within its cooldown window.
func (c *cooldownTable) cooling(root string, now time.Time) bool {
	e, ok := c.entries[root]
	if !ok {
		return false
	}
	return now.Before(e.until)
}

// sweep drops entries whose window has fully elapsed.
func (c *cooldownTable) sweep(now time.Time) {
	for k, e := range c.entries {
		if !now.Before(e.until) {
			delete(c.entries, k)
		}
	}
}

func (c *cooldownTable) evictOldest() {
	var oldestKey string
	var oldest time.Time
	first := true
	for k, e := range c.entries {
		if first || e.until.Before(oldest) {
			oldestKey, oldest, first = k, e.until, false
		}
	}
	if !first {
		delete(c.entries, oldestKey)
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass.**

Run: `go test ./pkg/server/delegation/ -run TestCooldownSuite -v`
Expected: PASS (all 3).

- [ ] **Step 5: Commit.**

```bash
git add pkg/server/delegation/cooldown.go pkg/server/delegation/cooldown_test.go
git commit -m "feat(delegation): server cooldown table (exponential, capped, TTL'd)"
```

---

## Task 3: Server recall registry + per-region handoff state machine

**Files:**
- Create: `pkg/server/delegation/recall.go`
- Test: `pkg/server/delegation/recall_test.go`

**Interfaces:**
- Produces: `type Recaller interface { Recall(ownerSession, root string) error }`; `RecallRegistry` implementing it with `Register(sessionID string, send func(*proto.RecallMsg) error) (release func())` and an `Ack(sessionID string, recallID uint64)` method. `Recall` pushes a `RecallMsg` to that session's registered stream and blocks until the matching `Ack` or `timeout` (returns error on timeout / no-stream). recall_id is a per-registry atomic counter (NOT time/random — resume-safe).
- Consumed by Task 4 (handlers) and Task 6 (the Recall stream controller calls `Register`/`Ack`).

- [ ] **Step 1: Write the failing test.**

```go
package delegation

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type RecallSuite struct{ suite.Suite }

func TestRecallSuite(t *testing.T) { suite.Run(t, new(RecallSuite)) }

func (s *RecallSuite) TestRecallSucceedsOnAck() {
	reg := NewRecallRegistry(time.Second)
	var got *proto.RecallMsg
	release := reg.Register("sessA", func(m *proto.RecallMsg) error { got = m; return nil })
	defer release()

	done := make(chan error, 1)
	go func() { done <- reg.Recall("sessA", "proj/src") }()

	s.Eventually(func() bool { return got != nil }, time.Second, time.Millisecond)
	reg.Ack("sessA", got.RecallId)
	s.NoError(<-done)
	s.Equal("proj/src", got.Root)
}

func (s *RecallSuite) TestRecallTimesOutWithoutAck() {
	reg := NewRecallRegistry(50 * time.Millisecond)
	release := reg.Register("sessA", func(m *proto.RecallMsg) error { return nil })
	defer release()
	s.Error(reg.Recall("sessA", "x"))
}

func (s *RecallSuite) TestRecallNoStreamIsError() {
	reg := NewRecallRegistry(time.Second)
	s.Error(reg.Recall("ghost", "x")) // never registered -> treat as released
}

func (s *RecallSuite) TestConcurrentRecallsDistinctIDs() {
	reg := NewRecallRegistry(time.Second)
	var mu sync.Mutex
	ids := map[uint64]bool{}
	release := reg.Register("sessA", func(m *proto.RecallMsg) error {
		mu.Lock(); ids[m.RecallId] = true; mu.Unlock()
		go reg.Ack("sessA", m.RecallId)
		return nil
	})
	defer release()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = reg.Recall("sessA", "r") }()
	}
	wg.Wait()
	s.Len(ids, 8)
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/server/delegation/ -run TestRecallSuite -v`
Expected: FAIL — `NewRecallRegistry` undefined.

- [ ] **Step 3: Implement `recall.go`.**

```go
package delegation

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// Recaller is the arbiter's view of the recall transport.
type Recaller interface {
	Recall(ownerSession, root string) error
}

type pending struct {
	ackCh chan struct{}
}

type streamSlot struct {
	send func(*proto.RecallMsg) error
}

// RecallRegistry maps owner session_id -> its open Recall stream and correlates
// RecallMsg/RecallAck by recall_id. Safe for concurrent use.
type RecallRegistry struct {
	timeout time.Duration
	nextID  atomic.Uint64

	mu       sync.Mutex
	streams  map[string]*streamSlot
	inflight map[uint64]*pending
}

func NewRecallRegistry(timeout time.Duration) *RecallRegistry {
	return &RecallRegistry{
		timeout:  timeout,
		streams:  make(map[string]*streamSlot),
		inflight: make(map[uint64]*pending),
	}
}

// Register installs the send-fn for a session's Recall stream and returns a
// release closure to call when the stream ends (deregister).
func (r *RecallRegistry) Register(sessionID string, send func(*proto.RecallMsg) error) (release func()) {
	slot := &streamSlot{send: send}
	r.mu.Lock()
	r.streams[sessionID] = slot
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		if r.streams[sessionID] == slot {
			delete(r.streams, sessionID)
		}
		r.mu.Unlock()
	}
}

// Ack completes the in-flight recall for recallID, if any.
func (r *RecallRegistry) Ack(sessionID string, recallID uint64) {
	r.mu.Lock()
	p := r.inflight[recallID]
	delete(r.inflight, recallID)
	r.mu.Unlock()
	if p != nil {
		close(p.ackCh)
	}
}

// Recall pushes a RecallMsg to ownerSession and blocks until the matching Ack
// or the timeout. Never holds any caller lock — the arbiter calls this AFTER
// releasing its table mutex.
func (r *RecallRegistry) Recall(ownerSession, root string) error {
	id := r.nextID.Add(1)
	p := &pending{ackCh: make(chan struct{})}

	r.mu.Lock()
	slot := r.streams[ownerSession]
	if slot == nil {
		r.mu.Unlock()
		return errors.Errorf("recall: no stream for session %s", ownerSession)
	}
	r.inflight[id] = p
	r.mu.Unlock()

	if err := slot.send(&proto.RecallMsg{Root: root, RecallId: id}); err != nil {
		r.mu.Lock()
		delete(r.inflight, id)
		r.mu.Unlock()
		return errors.Wrap(err, "recall: send")
	}

	select {
	case <-p.ackCh:
		return nil
	case <-time.After(r.timeout):
		r.mu.Lock()
		delete(r.inflight, id)
		r.mu.Unlock()
		return errors.Errorf("recall: timed out after %s waiting for ack from %s", r.timeout, ownerSession)
	}
}
```

- [ ] **Step 4: Run the tests to verify they pass.**

Run: `go test ./pkg/server/delegation/ -run TestRecallSuite -v`
Expected: PASS (all 4).

- [ ] **Step 5: Commit.**

```bash
git add pkg/server/delegation/recall.go pkg/server/delegation/recall_test.go
git commit -m "feat(delegation): recall registry with push+ack and timeout"
```

---

## Task 4: Server Arbiter — compose table+cooldown+recall, no-lock-across-RTT

**Files:**
- Create: `pkg/server/delegation/arbiter.go`
- Test: `pkg/server/delegation/arbiter_test.go`

**Interfaces:**
- Consumes: `delegationTable`, `cooldownTable`, `Recaller`.
- Produces:
  - `func NewArbiter(r Recaller, cfg Config, now func() time.Time) *Arbiter`
  - `func (a *Arbiter) OnMutation(contenderSession, path string) error` — recall-on-contention: if a *foreign* delegation covers path, recall it (lock released across the RTT), trip its cooldown, drop it; returns the recall error (handler maps to FS_EAGAIN). Self-owned coverage is a no-op (self-access never recalls).
  - `func (a *Arbiter) Request(owner, root string) *proto.DelegationGrant` — grant after carving + cooldown filtering; nil-safe (returns a grant with empty granted_root when denied/cooling).
  - `func (a *Arbiter) ReleaseSession(sessionID string)` — reap hook.
- `Config{ RecallTimeout, Cooldown cooldownConfig }`. `now` injectable for tests (`time.Now` in prod).

- [ ] **Step 1: Write the failing test (fake Recaller, deterministic clock).**

```go
package delegation

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type fakeRecaller struct {
	mu     sync.Mutex
	calls  []string // "owner:root"
	failOn map[string]bool
}

func (f *fakeRecaller) Recall(owner, root string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, owner+":"+root)
	if f.failOn[owner] {
		return assertErr
	}
	return nil
}

var assertErr = errInfo("recall failed")
type errInfo string
func (e errInfo) Error() string { return string(e) }

type ArbiterSuite struct {
	suite.Suite
	clock time.Time
}

func TestArbiterSuite(t *testing.T) { suite.Run(t, new(ArbiterSuite)) }

func (s *ArbiterSuite) now() time.Time { return s.clock }

func (s *ArbiterSuite) newArbiter(r Recaller) *Arbiter {
	s.clock = time.Unix(1000, 0)
	return NewArbiter(r, Config{
		RecallTimeout: time.Second,
		Cooldown:      cooldownConfig{Base: time.Second, Max: time.Minute, Cap: 256},
	}, s.now)
}

func (s *ArbiterSuite) TestGrantThenForeignMutationRecalls() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	g := a.Request("sessA", "proj")
	s.Equal("proj", g.GrantedRoot)

	// B mutates inside A's subtree -> A recalled, A's grant dropped.
	s.NoError(a.OnMutation("sessB", "proj/file"))
	s.Equal([]string{"sessA:proj"}, fr.calls)

	// A's delegation is gone now; B mutating again must NOT recall (no owner).
	fr.calls = nil
	s.NoError(a.OnMutation("sessB", "proj/file"))
	s.Empty(fr.calls)
}

func (s *ArbiterSuite) TestSelfMutationNeverRecalls() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	s.NoError(a.OnMutation("sessA", "proj/file")) // own subtree
	s.Empty(fr.calls)
}

func (s *ArbiterSuite) TestCooldownBlocksImmediateRegrant() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	s.NoError(a.OnMutation("sessB", "proj/file")) // recall + trip cooldown on "proj"
	// A re-requests immediately -> denied (cooling).
	g := a.Request("sessA", "proj")
	s.Equal("", g.GrantedRoot)
	s.Greater(g.RetryAfterMs, uint64(0))
}

func (s *ArbiterSuite) TestReleaseSessionFreesSubtree() {
	fr := &fakeRecaller{}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	a.ReleaseSession("sessA")
	// No owner now -> B's mutation recalls nothing; B can take it.
	s.NoError(a.OnMutation("sessB", "proj/x"))
	s.Empty(fr.calls)
	g := a.Request("sessB", "proj")
	s.Equal("proj", g.GrantedRoot)
}

func (s *ArbiterSuite) TestRecallFailurePropagates() {
	fr := &fakeRecaller{failOn: map[string]bool{"sessA": true}}
	a := s.newArbiter(fr)
	a.Request("sessA", "proj")
	s.Error(a.OnMutation("sessB", "proj/file")) // handler maps to FS_EAGAIN
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/server/delegation/ -run TestArbiterSuite -v`
Expected: FAIL — `NewArbiter` undefined.

- [ ] **Step 3: Implement `arbiter.go` — the state machine, lock released across the recall.**

```go
package delegation

import (
	"sync"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"
)

type Config struct {
	RecallTimeout time.Duration
	Cooldown      cooldownConfig
}

// regionState tracks an in-flight recall so concurrent contenders coalesce
// onto one recall instead of stampeding the holder.
type regionState struct {
	recalling bool
	done      chan struct{} // closed when the in-flight recall finishes
}

type Arbiter struct {
	recaller Recaller
	now      func() time.Time

	mu       sync.Mutex
	table    *delegationTable
	cooldown *cooldownTable
	regions  map[string]*regionState // keyed by delegated root being recalled
}

func NewArbiter(r Recaller, cfg Config, now func() time.Time) *Arbiter {
	return &Arbiter{
		recaller: r,
		now:      now,
		table:    newDelegationTable(),
		cooldown: newCooldownTable(cfg.Cooldown),
		regions:  make(map[string]*regionState),
	}
}

// Request grants owner a delegation rooted at root, carving around foreign
// subtrees and refusing cooling roots. Returns a grant (empty GrantedRoot =
// denied). root=="" (no piggyback) returns an empty grant without touching the
// table.
func (a *Arbiter) Request(owner, root string) *proto.DelegationGrant {
	if root == "" {
		return &proto.DelegationGrant{}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	a.cooldown.sweep(now)
	if a.cooldown.cooling(root, now) {
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	granted, excluded, ok := a.table.grant(owner, root)
	if !ok {
		return &proto.DelegationGrant{RetryAfterMs: uint64(a.cfgRetryMs())}
	}
	return &proto.DelegationGrant{GrantedRoot: granted, ExcludedPaths: excluded}
}

func (a *Arbiter) cfgRetryMs() int64 { return a.cooldown.cfg.Base.Milliseconds() }

// OnMutation enforces recall-on-contention. If a FOREIGN delegation covers
// path, recall it (releasing the lock across the RPC), trip its cooldown, and
// drop it. Self-owned coverage is a no-op. Returns the recall error so the
// caller can fail the contender's op closed (map to FS_EAGAIN).
func (a *Arbiter) OnMutation(contender, path string) error {
	a.mu.Lock()
	owner, root, ok := a.table.ownerOf(path)
	if !ok || owner == contender {
		a.mu.Unlock()
		return nil // free, or self-access -> never recall
	}
	// Coalesce: if this root is already being recalled, wait for that recall.
	if rs := a.regions[root]; rs != nil && rs.recalling {
		done := rs.done
		a.mu.Unlock()
		<-done
		return nil // the in-flight recall already freed (or cooled) the root
	}
	rs := &regionState{recalling: true, done: make(chan struct{})}
	a.regions[root] = rs
	a.mu.Unlock()

	// ---- recall RTT happens with NO lock held (barrier = this handoff) ----
	err := a.recaller.Recall(owner, root)

	a.mu.Lock()
	if err == nil {
		a.table.release(root)
		a.cooldown.trip(root, a.now())
	}
	delete(a.regions, root)
	close(rs.done)
	a.mu.Unlock()
	return err
}

// ReleaseSession drops all delegations owned by a reaped session.
func (a *Arbiter) ReleaseSession(sessionID string) {
	a.mu.Lock()
	a.table.releaseOwner(sessionID)
	a.mu.Unlock()
}
```

- [ ] **Step 4: Run the tests to verify they pass.**

Run: `go test ./pkg/server/delegation/ -run TestArbiterSuite -v`
Expected: PASS (all 5). Also run the whole package with the race detector:
`go test -race ./pkg/server/delegation/...` → PASS.

- [ ] **Step 5: Commit.**

```bash
git add pkg/server/delegation/arbiter.go pkg/server/delegation/arbiter_test.go
git commit -m "feat(delegation): arbiter composing table+cooldown+recall (no lock across RTT)"
```

---

## Task 5: Server — Recall stream controller + session-reap hook + metrics + wiring

**Files:**
- Create: `pkg/server/controller/recall.go`
- Test: `pkg/server/controller/recall_test.go`
- Modify: `pkg/server/service/session.go` (add `OnReap` option; invoke from all reap paths)
- Modify: `pkg/server/metrics/metrics.go` (3 collectors)
- Modify: `pkg/server/app.go` (construct + wire)

**Interfaces:**
- Consumes: `*delegation.RecallRegistry`, `service.SessionManager`.
- Produces: `Recall(stream proto.RpcFs_RecallServer) error` method on `RpcServerImpl`; `SessionManagerOptions.OnReap func(sessionID string)`.

- [ ] **Step 1: Write the failing reap-hook test (session package).**

Add to a new `pkg/server/service/session_reap_hook_test.go` (testify suite, mirrors `session_test.go` style):

```go
func (s *SessionManagerSuite) TestOnReapCalledOnGraceExpiry() {
	var mu sync.Mutex
	var reaped []string
	mgr := NewSessionManager(SessionManagerOptions{
		GracePeriod: 10 * time.Millisecond,
		OnReap:      func(id string) { mu.Lock(); reaped = append(reaped, id); mu.Unlock() },
	})
	id, _ := mgr.Create("alice", "")
	mgr.MarkDisconnected(id)
	s.Eventually(func() bool {
		mu.Lock(); defer mu.Unlock(); return len(reaped) == 1 && reaped[0] == id
	}, time.Second, 5*time.Millisecond)
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/server/service/ -run TestSessionManagerSuite/TestOnReapCalledOnGraceExpiry -v`
Expected: FAIL — `SessionManagerOptions` has no `OnReap` field.

- [ ] **Step 3: Add `OnReap` to SessionManager and call it from every reap path.**

Read `pkg/server/service/session.go` around the `SessionManagerOptions` struct, the `managerImpl` `reap(...)` helper, the grace timer in `MarkDisconnected`, `ReapIf`, and `Stop`. Add the field:

```go
type SessionManagerOptions struct {
	// ... existing fields ...
	// OnReap, if set, is called with the session id whenever a session is
	// removed (grace expiry, revocation ReapIf, or shutdown Stop) AFTER its
	// fds are released. Used to drop the session's write-delegations so a dead
	// holder does not orphan a subtree. Must be non-blocking / fast.
	OnReap func(sessionID string)
}
```

In the single internal `reap(sess Session, reason string)` helper (the one `MarkDisconnected`'s timer, `ReapIf`, and `Stop` all funnel through — confirm by reading; if they do NOT share one, add the call to each), after `sess.ReleaseAll()` and the metrics decrement:

```go
	if m.onReap != nil {
		m.onReap(sess.ID())
	}
```

Store `onReap: opts.OnReap` in the constructor.

- [ ] **Step 4: Run the reap test to verify it passes.**

Run: `go test ./pkg/server/service/ -run TestSessionManagerSuite/TestOnReapCalledOnGraceExpiry -v`
Expected: PASS. Then `go test ./pkg/server/service/...` → PASS (no regressions).

- [ ] **Step 5: Write the Recall-stream controller test.**

`pkg/server/controller/recall_test.go` (testify suite). Use a fake `proto.RpcFs_RecallServer` that scripts a single `RecallAck` then blocks on ctx. Assert the controller `Register`s the stream (a subsequent `registry.Recall(sessionID, "x")` from another goroutine returns nil once the fake forwards the ack via `controller`), and that returning from the stream deregisters (a later `Recall` errors). Key behaviors to assert:
- The session id is taken from the stream context (`sessionIDFromContext`).
- `Recv()` of a `RecallAck` calls `registry.Ack(sessionID, ack.RecallId)`.
- Stream return (EOF/ctx cancel) calls the `release` from `Register`.

- [ ] **Step 6: Implement `pkg/server/controller/recall.go`.**

```go
package controller

import (
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/delegation"
)

// Recall is the server side of the coherence control stream. The client opens
// it once per mount; the server pushes RecallMsg (via the RecallRegistry) and
// reads RecallAck. Registration is keyed by the session id from the stream ctx.
func (r *RpcServerImpl) Recall(stream proto.RpcFs_RecallServer) error {
	sessionID := sessionIDFromContext(stream.Context())
	if sessionID == "" {
		return errMissingSession // existing helper used elsewhere; reuse the same status
	}
	release := r.recalls.Register(sessionID, stream.Send)
	defer release()
	for {
		ack, err := stream.Recv()
		if err != nil {
			return err // EOF / cancel -> defer release() deregisters
		}
		r.recalls.Ack(sessionID, ack.RecallId)
	}
}
```

(Add a `recalls *delegation.RecallRegistry` field to `RpcServerImpl` and the `NewGrpcServer` signature — see Step 8. If `errMissingSession` doesn't exist, return `status.Error(codes.Unauthenticated, "recall: no session")`.)

- [ ] **Step 7: Add the three metrics collectors.**

In `pkg/server/metrics/metrics.go`, mirror the existing `Subscribe*` collectors: a `DelegationGrantsActive` gauge (`Inc`/`Dec`), `DelegationRecalls` counter (`Inc`), `DelegationCooldownTrips` counter (`Inc`). Register them in `Register`.

- [ ] **Step 8: Wire everything in `app.go` + the controller constructors.**

In `pkg/server/controller/fs.go` change the struct + constructor:

```go
type RpcServerImpl struct {
	fsService service.VolumeService
	sessions  service.SessionManager
	bus       serverio.EventBus
	metrics   *metrics.Metrics
	arbiter   *delegation.Arbiter
	recalls   *delegation.RecallRegistry
	proto.UnimplementedRpcFsServer
}

func NewGrpcServer(fsService service.VolumeService, sessions service.SessionManager, bus serverio.EventBus, m *metrics.Metrics, arbiter *delegation.Arbiter, recalls *delegation.RecallRegistry) *RpcServerImpl {
	return &RpcServerImpl{fsService: fsService, sessions: sessions, bus: bus, metrics: m, arbiter: arbiter, recalls: recalls}
}
```

Add `arbiter *delegation.Arbiter` to `RpcFileServerImpl` + `NewRpcFileServer` likewise.

In `pkg/server/app.go` `NewServerAppContext`: construct the registry + arbiter and wire the reap hook **before** building the SessionManager so the option can reference the arbiter:

```go
recalls := delegation.NewRecallRegistry(cfg.Server.Session.GracePeriod) // ≤ grace; the registry owns the recall timeout
arbiter := delegation.NewArbiter(recalls, delegation.Config{
	Cooldown: delegation.CooldownConfigDefault(), // Base 1s, Max 60s, Cap 4096
}, time.Now)
sessionMgr := service.NewSessionManager(service.SessionManagerOptions{
	Metrics:              m,
	GracePeriod:          cfg.Server.Session.GracePeriod,
	IdempotencyCacheSize: cfg.Server.Session.IdempotencyCacheSize,
	OnReap:               arbiter.ReleaseSession,
})
```

Add `Delegation *delegation.Arbiter` and `Recalls *delegation.RecallRegistry` to `AppContext`, set them, and update `GetGrpcServices`:

```go
controller.NewGrpcServer(c.VolumeService, c.SessionManager, c.Bus, c.Metrics, c.Delegation, c.Recalls),
controller.NewRpcFileServer(c.VolumeService, c.SessionManager, c.Metrics, c.Config.Server.FrameSizeBytes, c.Bus, c.Delegation),
```

(Export a `CooldownConfigDefault()` from the delegation package returning the default `cooldownConfig`. Also delete the temporary `Recall` stub from Task 1 Step 4 if you added one — the real method from Step 6 replaces it.)

- [ ] **Step 9: Build + test the touched packages.**

Run: `go build ./... && go test ./pkg/server/delegation/... ./pkg/server/service/... ./pkg/server/controller/... -v`
Expected: PASS. (Handlers don't call the arbiter yet — that's Task 6 — but everything compiles and the Recall stream + reap hook work.)

- [ ] **Step 10: Commit.**

```bash
git add pkg/server/controller/recall.go pkg/server/controller/recall_test.go pkg/server/service/session.go pkg/server/service/session_reap_hook_test.go pkg/server/metrics/metrics.go pkg/server/app.go pkg/server/controller/fs.go pkg/server/controller/file.go
git commit -m "feat(delegation): recall stream controller, session-reap release hook, wiring + metrics"
```

---

## Task 6: Server — arbitrate the mutating handlers (recall-on-contention + grant)

**Files:**
- Modify: `pkg/server/controller/fs.go` (Mkdir, Rmdir, Rename, Unlink, SetAttr, Symlink, SetXAttr, RemoveXAttr)
- Modify: `pkg/server/controller/file.go` (Create, Write, Allocate)
- Create: `pkg/server/controller/delegation_handler_test.go`

**Interfaces:**
- Consumes: `r.arbiter.OnMutation(sessionID, path) error`, `r.arbiter.Request(sessionID, req.Delegation.GetRoot()) *proto.DelegationGrant`.
- Produces: a small helper `arbitrate` so every handler reads identically.

- [ ] **Step 1: Write the failing handler test.**

In `delegation_handler_test.go` (testify suite using the existing controller test harness — copy setup from `fs_test.go`): build an `RpcServerImpl` with a *real* arbiter whose `Recaller` is a fake that records calls. Two sessions, sessA + sessB.

```go
func (s *DelegationHandlerSuite) TestForeignMkdirRecallsHolder() {
	// sessA holds a delegation on "d" (granted via a prior piggybacked op).
	s.srv.arbiter.Request("sessA", "d")
	// sessB does Mkdir under "d" -> handler calls OnMutation -> recall fired.
	_, err := s.srv.Mkdir(s.ctxFor("sessB"), &proto.MkdirRequest{
		Volume: s.vol, Caller: s.caller, Path: "d/sub", SessionId: "sessB", RequestId: "r1",
	})
	s.NoError(err)
	s.Equal([]string{"sessA:d"}, s.recaller.calls)
}

func (s *DelegationHandlerSuite) TestPiggybackedRequestReturnsGrant() {
	reply, err := s.srv.Mkdir(s.ctxFor("sessA"), &proto.MkdirRequest{
		Volume: s.vol, Caller: s.caller, Path: "proj/x", SessionId: "sessA", RequestId: "r2",
		Delegation: &proto.DelegationRequest{Root: "proj"},
	})
	s.NoError(err)
	s.Equal("proj", reply.Grant.GetGrantedRoot())
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/server/controller/ -run TestDelegationHandlerSuite -v`
Expected: FAIL — `MkdirRequest` has `Delegation`/`MkdirReply` has `Grant` (from Task 1) but the handler ignores them and never recalls.

- [ ] **Step 3: Add the `arbitrate` helper and call it in each mutating handler.**

Add to `pkg/server/controller/fs.go` (or a new `delegation_handler.go` in the same package):

```go
// arbitrateContention enforces recall-on-contention for a mutating op at path.
// Returns a non-nil reply-status mapping when the contender must back off
// (recall failed/timed out -> FS_EAGAIN); nil means "proceed".
func (r *RpcServerImpl) arbitrateContention(sessionID, path string) proto.FsError {
	if r.arbiter == nil {
		return proto.FsError_FS_OK
	}
	if err := r.arbiter.OnMutation(sessionID, path); err != nil {
		log.Log.Warn("delegation recall failed; failing op closed",
			zap.String("path", path), zap.Error(err))
		return proto.FsError_FS_EAGAIN
	}
	return proto.FsError_FS_OK
}

// grantFor evaluates a piggybacked delegation request (nil-safe).
func (r *RpcServerImpl) grantFor(sessionID string, req *proto.DelegationRequest) *proto.DelegationGrant {
	if r.arbiter == nil || req.GetRoot() == "" {
		return nil
	}
	return r.arbiter.Request(sessionID, req.GetRoot())
}
```

Then in **Mkdir** (and structurally identically in Rmdir/Rename/Unlink/SetAttr/Symlink/SetXAttr/RemoveXAttr), insert the contention check at the top of the `withIdempotency` closure and stamp the grant on success. Mkdir becomes:

```go
return withIdempotency(sess, request.RequestId, func() (*proto.MkdirReply, error) {
	if st := r.arbitrateContention(request.SessionId, request.Path); st != proto.FsError_FS_OK {
		return &proto.MkdirReply{Status: st}, nil
	}
	fctx := createContext(ctx, request.Caller)
	if s := fs.Mkdir(request.Path, request.Mode, fctx); s != fuse.OK {
		return &proto.MkdirReply{Status: fserr.FromErrno(syscall.Errno(s))}, nil
	}
	reply := &proto.MkdirReply{
		Status:     proto.FsError_FS_OK,
		Attributes: r.statAttrsAndEmit(ctx, fs, request.Volume, request.Path, &id, fctx),
		Grant:      r.grantFor(request.SessionId, request.Delegation),
	}
	return reply, nil
})
```

For **Rename** (cross-subtree): arbitrate BOTH `request.OldName` and `request.NewName` (a rename can contend two delegations). Call `arbitrateContention` for each; if either returns non-OK, return that status. This is the server half of "cross-subtree rename forced synchronous" — the client half is Task 9.

For **file.go** Create/Write/Allocate: arbitrate on the path/handle path. `Write` only carries the delegation request on the first frame; evaluate the grant once and stamp it on the final `WriteReply`. `Create`'s path is `joinPath(parent, name)`.

- [ ] **Step 4: Run the handler tests + full server suites.**

Run: `go test ./pkg/server/controller/... -v` then `go test -race ./pkg/server/...`
Expected: PASS, no regressions.

- [ ] **Step 5: Commit.**

```bash
git add pkg/server/controller/fs.go pkg/server/controller/file.go pkg/server/controller/delegation_handler.go pkg/server/controller/delegation_handler_test.go
git commit -m "feat(delegation): arbitrate mutating handlers (recall-on-contention + piggybacked grant)"
```

---

## Task 7: Client — write-set / LCA tracker

**Files:**
- Create: `pkg/client/backend/delegation/writeset.go`
- Test: `pkg/client/backend/delegation/writeset_test.go`

**Interfaces:**
- Produces: `writeSet` with `record(path string)` and `root() string` (lowest common ancestor dir of recent writes; promotes upward as writes scatter; `""` = mount root). Bounded (ring of last N write paths).

- [ ] **Step 1: Write the failing test.**

```go
package delegation

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type WriteSetSuite struct{ suite.Suite }

func TestWriteSetSuite(t *testing.T) { suite.Run(t, new(WriteSetSuite)) }

func (s *WriteSetSuite) TestSingleDirIsThatDir() {
	w := newWriteSet(16)
	w.record("proj/src/a.go")
	w.record("proj/src/b.go")
	s.Equal("proj/src", w.root())
}

func (s *WriteSetSuite) TestScatterPromotesToCommonAncestor() {
	w := newWriteSet(16)
	w.record("proj/src/a.go")
	w.record("proj/test/b.go")
	s.Equal("proj", w.root())
}

func (s *WriteSetSuite) TestFullScatterIsMountRoot() {
	w := newWriteSet(16)
	w.record("a/x")
	w.record("b/y")
	s.Equal("", w.root())
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/client/backend/delegation/ -run TestWriteSetSuite -v`
Expected: FAIL — `newWriteSet` undefined.

- [ ] **Step 3: Implement `writeset.go`.**

```go
package delegation

import (
	"path"
	"strings"
	"sync"
)

// writeSet tracks recent write paths and yields the lowest common ancestor
// directory to request a delegation over. Safe for concurrent use.
type writeSet struct {
	mu   sync.Mutex
	ring []string
	n    int
	cap  int
}

func newWriteSet(capacity int) *writeSet { return &writeSet{cap: capacity} }

func (w *writeSet) record(p string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ring) < w.cap {
		w.ring = append(w.ring, p)
	} else {
		w.ring[w.n%w.cap] = p
	}
	w.n++
}

// root returns the LCA *directory* of the recorded paths ("" == mount root).
func (w *writeSet) root() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.ring) == 0 {
		return ""
	}
	lca := path.Dir(w.ring[0])
	for _, p := range w.ring[1:] {
		lca = commonDir(lca, path.Dir(p))
		if lca == "." || lca == "" {
			return ""
		}
	}
	if lca == "." {
		return ""
	}
	return lca
}

// commonDir returns the longest shared leading path segment sequence of a,b.
func commonDir(a, b string) string {
	if a == b {
		return a
	}
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	i := 0
	for i < len(as) && i < len(bs) && as[i] == bs[i] {
		i++
	}
	if i == 0 {
		return ""
	}
	return strings.Join(as[:i], "/")
}
```

- [ ] **Step 4: Run the tests to verify they pass.**

Run: `go test ./pkg/client/backend/delegation/ -run TestWriteSetSuite -v`
Expected: PASS (all 3).

- [ ] **Step 5: Commit.**

```bash
git add pkg/client/backend/delegation/writeset.go pkg/client/backend/delegation/writeset_test.go
git commit -m "feat(delegation): client write-set / LCA tracker"
```

---

## Task 8: Client — Manager (grants + oracle + recall goroutine) and the posWritePath Layer

**Files:**
- Create: `pkg/client/backend/delegation/manager.go`
- Create: `pkg/client/backend/delegation/layer.go`
- Test: `pkg/client/backend/delegation/manager_test.go`

**Interfaces:**
- Produces:
  - `type CacheInvalidator interface { InvalidateSubtree(path string) }` (the Manager calls this on recall; the cache adapter implements it).
  - `type Manager` with:
    - `NewManager(inv CacheInvalidator) *Manager`
    - `IsDelegated(path string) bool` — the cache oracle (a held grant whose granted_root contains path, minus excluded sub-paths).
    - `Apply(grant *proto.DelegationGrant)` — record a grant returned by the transport.
    - `RequestedRoot() string` — the write-set LCA the transport should piggyback (empty if nothing to request / cooling-suppressed).
    - `Record(path string)` — feed the write-set (called by the Layer on mutating ops).
    - `OnRecall(root string)` — drop grants under root + `inv.InvalidateSubtree(root)` (the recall handler calls this, then acks).
    - `Close()` — stop the recall goroutine.
  - `func NewLayer(inner backend.FileSystemBackend, m *Manager) backend.FileSystemBackend` — embeds `backend.PassthroughBackend`; overrides the mutating ops to `m.Record(path)`; forces cross-subtree rename synchronous; `Close()` calls `m.Close()` then `inner.Close()`.
- Consumes: `transport.DelegationHook` is satisfied by `*Manager` (`RequestedRoot`/`Apply`).

- [ ] **Step 1: Write the failing Manager test.**

```go
package delegation

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type fakeInv struct{ subtrees []string }

func (f *fakeInv) InvalidateSubtree(p string) { f.subtrees = append(f.subtrees, p) }

type ManagerSuite struct{ suite.Suite }

func TestManagerSuite(t *testing.T) { suite.Run(t, new(ManagerSuite)) }

func (s *ManagerSuite) TestApplyThenIsDelegated() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})
	s.True(m.IsDelegated("proj/src/a.go"))
	s.False(m.IsDelegated("other/x"))
}

func (s *ManagerSuite) TestExcludedPathNotDelegated() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", ExcludedPaths: []string{"proj/vendor"}})
	s.True(m.IsDelegated("proj/src/a.go"))
	s.False(m.IsDelegated("proj/vendor/dep/x"))
}

func (s *ManagerSuite) TestOnRecallDropsAndInvalidates() {
	inv := &fakeInv{}
	m := NewManager(inv)
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})
	m.OnRecall("proj")
	s.False(m.IsDelegated("proj/src/a.go"))
	s.Equal([]string{"proj"}, inv.subtrees)
}

func (s *ManagerSuite) TestEmptyGrantNoOp() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{}) // denied
	s.False(m.IsDelegated("anything"))
}
```

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/client/backend/delegation/ -run TestManagerSuite -v`
Expected: FAIL — `NewManager` undefined.

- [ ] **Step 3: Implement `manager.go`.**

Write `Manager` holding a `*writeSet`, a `sync.RWMutex`-guarded set of active grants (`map[string]grantState` keyed by granted_root, each with its excluded list), the `CacheInvalidator`, and the recall goroutine plumbing (a `stop chan struct{}` for `Close`; the actual stream pump lands in Task 10 wiring, but `OnRecall` is the unit-testable core here). `IsDelegated(path)` = exists a grant whose root `contains` path AND no excluded entry `contains` path. `RequestedRoot()` returns `writeSet.root()` unless that root is already fully delegated (then `""`). `Apply` records non-empty grants. `OnRecall(root)` deletes grants whose root is covered by (or covers) `root` and calls `inv.InvalidateSubtree(root)`.

(Reuse a local `contains` helper identical to the server's path containment.)

- [ ] **Step 4: Implement `layer.go`.**

```go
package delegation

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type layer struct {
	backend.PassthroughBackend
	inner backend.FileSystemBackend
	mgr   *Manager
}

// NewLayer returns the posWritePath delegation layer. It records the write-set
// on mutating ops (so the Manager can pick a delegation root) and forces a
// cross-subtree rename down the synchronous path (it cannot be covered by a
// single delegation). All durability still happens in the transport leaf.
func NewLayer(inner backend.FileSystemBackend, m *Manager) backend.FileSystemBackend {
	return &layer{PassthroughBackend: backend.NewPassthrough(inner), inner: inner, mgr: m}
}

func (l *layer) Create(ctx context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	l.mgr.Record(joinPath(parent, name))
	return l.inner.Create(ctx, parent, name, flags, mode)
}

func (l *layer) Mkdir(ctx context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
	l.mgr.Record(path)
	return l.inner.Mkdir(ctx, path, mode)
}

// ... Write (record fh.Path()), SetAttr, Symlink, Unlink, Rmdir, SetXAttr,
// RemoveXAttr: record the path then delegate ...

func (l *layer) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	// Cross-subtree rename can't be covered by one delegation: record both ends
	// so the write-set may promote, but otherwise just delegate (the server
	// arbitrates both paths and recalls as needed — Task 6).
	l.mgr.Record(oldPath)
	l.mgr.Record(newPath)
	return l.inner.Rename(ctx, oldPath, newPath)
}

func (l *layer) Close() error {
	l.mgr.Close()
	return l.inner.Close()
}
```

(Confirm the `PassthroughBackend` constructor name by reading `pkg/client/backend/passthrough.go`; the explorer reported layers embed it. `joinPath` mirrors the transport's helper — copy the one-liner.)

- [ ] **Step 5: Run + race.**

Run: `go test -race ./pkg/client/backend/delegation/... -v`
Expected: PASS. Also `go build ./...` to confirm `NewLayer` satisfies `FileSystemBackend` (or `TestFileSystemBackendMethodSet` in `pkg/client/backend` still passes — the Layer embeds Passthrough so it does).

- [ ] **Step 6: Commit.**

```bash
git add pkg/client/backend/delegation/manager.go pkg/client/backend/delegation/layer.go pkg/client/backend/delegation/manager_test.go
git commit -m "feat(delegation): client Manager (grants+oracle+recall) and posWritePath layer"
```

---

## Task 9: Client — cache skip-revalidation oracle (+ the two discriminating tests)

**Files:**
- Modify: `pkg/client/backend/cache/backend.go` (`NewCachedBackend` gains an optional `DelegationOracle`; fast-path consults it)
- Test: `pkg/client/backend/cache/delegation_skip_test.go`

**Interfaces:**
- Consumes: `type DelegationOracle interface { IsDelegated(path string) bool }` (satisfied by `*delegation.Manager`; declared in the cache package to avoid an import cycle — the cache must not import delegation).
- Produces: `NewCachedBackend(..., oracle DelegationOracle)` (nil = today's behavior).

- [ ] **Step 1: Write the two failing tests (the proof Phase 1 delivers value).**

`delegation_skip_test.go` (testify suite, reuse the cache test harness in the package):

```go
// PERF: during an UNVERIFIED window, a delegated path skips GetAttrIfChanged
// while a non-delegated path still revalidates. This is the reconnect-window
// bonus and the regression guard for it.
func (s *DelegationSkipSuite) TestDelegatedPathSkipsRevalidationWhenUnverified() {
	s.oracle.delegated["proj/a"] = true
	s.backend.validity.markGlobalUnverified() // force the revalidation path
	s.primeAttr("proj/a", 7)
	s.primeAttr("other/b", 9)

	_, _ = s.backend.Stat(s.ctx, "proj/a")
	_, _ = s.backend.Stat(s.ctx, "other/b")

	s.Equal(0, s.inner.getAttrIfChangedCalls["proj/a"]) // skipped (delegated)
	s.Equal(1, s.inner.getAttrIfChangedCalls["other/b"]) // revalidated (not delegated)
}

// CORRECTNESS (the headline): a remote write whose Subscribe event is delayed
// must never be served stale for a delegated subtree, because the holder is
// guaranteed a recall first. Model: while delegated + a recall has fired,
// IsDelegated flips false and the cache revalidates -> fresh attrs.
func (s *DelegationSkipSuite) TestRecallRestoresRevalidationSoNoStale() {
	s.oracle.delegated["proj/a"] = true
	s.backend.validity.markGlobalUnverified()
	s.primeAttr("proj/a", 7)         // cached version 7
	s.inner.setVersion("proj/a", 8)  // server moved on (remote write)

	// Before recall: delegated -> skip -> would serve stale 7. That's SAFE only
	// because a recall is guaranteed; simulate it:
	s.oracle.delegated["proj/a"] = false // recall dropped the delegation

	attr, _ := s.backend.Stat(s.ctx, "proj/a")
	s.Equal(uint64(8), attr.Version) // revalidated to fresh
}
```

- [ ] **Step 2: Run them to verify they fail.**

Run: `go test ./pkg/client/backend/cache/ -run TestDelegationSkipSuite -v`
Expected: FAIL — `NewCachedBackend` has no oracle param / fast-path ignores delegation.

- [ ] **Step 3: Thread the oracle into the fast-path.**

Read the revalidation fast-path in `pkg/client/backend/cache/backend.go` (the `cachedAttrLookup`/`Stat` region that checks `b.validity.globalState()==stateVerified || b.validity.isPathVerified(key)`). Add the oracle:

```go
// in cachedBackend struct:
//   oracle DelegationOracle // optional; nil = no delegation

// in the fast-path predicate:
if b.validity.globalState() == stateVerified ||
	b.validity.isPathVerified(key) ||
	(b.oracle != nil && b.oracle.IsDelegated(key)) {
	// serve cached attrs without a GetAttrIfChanged RTT
}
```

Add the `DelegationOracle` interface + the constructor param (append it to `NewCachedBackend`’s signature; update the call site in Task 10). Declare the interface in the cache package.

- [ ] **Step 4: Run both tests + the full cache suite.**

Run: `go test ./pkg/client/backend/cache/... -v`
Expected: PASS (both new tests + no regressions). Run with `-race`.

- [ ] **Step 5: Commit.**

```bash
git add pkg/client/backend/cache/backend.go pkg/client/backend/cache/delegation_skip_test.go
git commit -m "feat(delegation): cache consults delegation oracle to skip revalidation (coherence + reconnect bonus)"
```

---

## Task 10: Client — transport DelegationHook + mount wiring (compose at posWritePath)

**Files:**
- Modify: `pkg/client/backend/transport/backend_grpc.go` (optional `DelegationHook`; stamp request / deliver grant on the mutating ops)
- Modify: `pkg/client/mount/single.go` (build Manager, recall stream goroutine, wire oracle + hook + posWritePath layer)
- Test: `pkg/client/backend/transport/delegation_hook_test.go`

**Interfaces:**
- Produces: `type DelegationHook interface { RequestedRoot() string; Apply(*proto.DelegationGrant) }`; `transport.WithDelegationHook(h DelegationHook) BackendOption`.

- [ ] **Step 1: Write the failing transport test.**

Assert that when a hook is set, a mutating RPC (e.g. `Mkdir`) carries `Delegation.Root == hook.RequestedRoot()` and that a non-empty `reply.Grant` is delivered to `hook.Apply`. Use the existing transport test harness with a stub gRPC `RpcFsClient`.

- [ ] **Step 2: Run it to verify it fails.**

Run: `go test ./pkg/client/backend/transport/ -run TestDelegationHookSuite -v`
Expected: FAIL — no `WithDelegationHook` / requests omit `Delegation`.

- [ ] **Step 3: Implement the hook in the transport.**

Add a `delegation DelegationHook` field (nil by default) + `WithDelegationHook` option. In each mutating request builder, when `b.delegation != nil`, set `Delegation: &proto.DelegationRequest{Root: b.delegation.RequestedRoot()}`; after a successful reply, `if g := reply.GetGrant(); g != nil { b.delegation.Apply(g) }`. For `Write`, stamp on the first frame and `Apply` the `WriteReply.Grant`.

- [ ] **Step 4: Wire it all in `single.go`.**

In `Mount`, after the cache layer block and before the observer/identity layers, build the Manager and add the posWritePath layer. Order matters: the Manager is the cache's oracle AND the transport's hook, so construct it first:

```go
// Delegation manager: shared by the cache (oracle) and transport (hook).
delMgr := delegation.NewManager(cacheInvalidatorFor(/* the cache layer */))
backendOpts = append(backendOpts, transport.WithDelegationHook(delMgr))
// cache layer: pass delMgr as the DelegationOracle (extra arg to NewCachedBackend)
// ... layers = append(..., posWritePath, build: NewLayer(inner, delMgr)) ...
// Start the recall stream pump: open client.Fs().Recall(ctx), loop Recv(RecallMsg)
//   -> delMgr.OnRecall(msg.Root) -> stream.Send(&proto.RecallAck{RecallId: msg.RecallId, Done: true})
```

Because the cache is built via a layer closure and the Manager needs the cache's invalidator, build the cache first (capturing the constructed `*cachedBackend`'s invalidator surface — expose a small `InvalidateSubtree(path)` method on the cache that the Manager's `CacheInvalidator` adapts), then build the Manager, then append the posWritePath layer and set the transport hook. Add the recall-stream goroutine lifecycle to the Manager (started here, stopped on `Close`). Keep the wiring in a focused helper (`buildDelegation(...)`) so `Mount` stays readable — mirrors the existing per-layer blocks.

- [ ] **Step 5: Build + targeted tests.**

Run: `go test ./pkg/client/... -run 'Transport|Delegation|Mount|Compose' -v && go build ./...`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
git add pkg/client/backend/transport/backend_grpc.go pkg/client/backend/transport/delegation_hook_test.go pkg/client/mount/single.go
git commit -m "feat(delegation): transport piggyback hook + mount wiring at posWritePath"
```

---

## Task 11: End-to-end integration test (two in-process mounts)

**Files:**
- Create: `test/e2e/delegation/delegation_test.go` (follow `test/e2e/utils` harness; gate per `feedback_ci_has_fuse` — CI has /dev/fuse so a real mount runs, but the two-client coherence assertions can run over **bufconn with two BackendClients sharing one server** without FUSE, which is more deterministic — prefer that for the coherence/recall assertions and reserve a single real FUSE mount for a smoke check).

**Interfaces:**
- Consumes: the whole stack (real server `AppContext` + two client backends).

- [ ] **Step 1: Write the four scenario tests (failing until the stack is wired end-to-end).**

```go
// 1. GRANT + COHERENCE: clientA writes under /d with a piggybacked request,
//    gets a grant, then a delayed/suppressed remote change is never served
//    stale because clientB's write recalls A first.
// 2. RECALL: clientA holds /d; clientB Mkdir /d/x -> server recalls A ->
//    A's Manager.OnRecall fires (assert via a hook/metric) -> B's op succeeds.
// 3. CROSS-SUBTREE RENAME: clientA holds /a and /b disjoint; a rename a/x -> b/y
//    arbitrates BOTH -> both recalled / synchronous; no panic, correct result.
// 4. THRASH: alternating writes by A and B to /d -> cooldown makes the region
//    stay synchronous (assert DelegationCooldownTrips counter increments and
//    re-grants are denied within the window).
```

- [ ] **Step 2: Run to verify they fail / drive remaining gaps.**

Run: `go test ./test/e2e/delegation/ -v`
Expected: initially FAIL; iterate until green. Investigate any FUSE-env failures per `feedback_fuse_test_env` (use the VM if local FUSE is unavailable; bufconn variants run anywhere).

- [ ] **Step 3: Make them pass; run with race.**

Run: `go test -race ./test/e2e/delegation/ -v`
Expected: PASS.

- [ ] **Step 4: Commit.**

```bash
git add test/e2e/delegation/
git commit -m "test(delegation): e2e grant/recall/cross-rename/thrash coherence suite"
```

---

## Task 12: Docs fold + full gate

**Files:**
- Create: `docs/design/delegation-recall.md` (distilled from the spec; the durable design home)
- Modify: `website/sidebars.js` (add the new design doc)
- Delete: `docs/superpowers/specs/2026-06-23-delegation-recall-wal-design.md` and `docs/superpowers/plans/2026-06-23-delegation-recall-phase1.md` (fold into `docs/design/`, per `feedback_fold_superpowers_docs`)

- [ ] **Step 1: Write `docs/design/delegation-recall.md`.**

Distill: the model (delegation + recall, subtree granularity), the one-principle recall rule, server-mediated handoff, server-side cooldown/carving, the Phase-1 scope (coherence only, no deferral) and the **honest value framing** (provable coherence closing the known memory-tier stale race + reconnect-window perf bonus; steady-state read perf is already delivered by the verified cache), and the **Phase-2 gates** (WAL, fencing-by-delegation-gen — call out that boot-epoch reclaim is the hole, SQL watermark, per-fd handle seam). Keep the Phase-2 detail as a clearly-labelled "deferred" section so the next cycle has the gates written down.

- [ ] **Step 2: Add to `website/sidebars.js`** under the design docs section.

- [ ] **Step 3: `git rm` the superpowers SPEC; commit the fold.** (The plan file is pruned in Task 13's final step, after the benchmark has been run from it.)

```bash
git rm docs/superpowers/specs/2026-06-23-delegation-recall-wal-design.md
git add docs/design/delegation-recall.md website/sidebars.js
git commit -m "docs(delegation): fold Phase 1 design into docs/design, prune superpowers spec"
```

- [ ] **Step 4: Full local gate over the touched packages.**

Per `feedback_gate_touched_packages`, test the UNION of touched packages (can't run `./...` locally because of FUSE):

```bash
go build ./... && \
go vet ./pkg/server/delegation/... ./pkg/server/controller/... ./pkg/server/service/... \
       ./pkg/client/backend/delegation/... ./pkg/client/backend/cache/... \
       ./pkg/client/backend/transport/... ./pkg/client/mount/... && \
task lint && \
go test -race ./pkg/server/delegation/... ./pkg/server/controller/... ./pkg/server/service/... \
       ./pkg/client/backend/delegation/... ./pkg/client/backend/cache/... \
       ./pkg/client/backend/transport/...
```

Expected: all PASS. Run `task gen:mocks` if any mock-backed interface changed signature, then re-run. The e2e FUSE suite (`test/e2e/...`) runs in CI (which has /dev/fuse) and on the VM.

- [ ] **Step 5: (PR is opened in Task 13 Step 5, after the perf gate.)**

---

## Task 13: Performance regression benchmark (before/after on the perf VM)

**Goal:** Prove Phase 1 added no throughput/latency regression. Phase 1 adds an arbiter `OnMutation` call + optional grant on each mutating handler, a per-mount recall bidi stream, and one extra predicate (`oracle.IsDelegated(path)`) in the cache revalidation fast-path. None should move steady-state numbers; this measures it.

**Controller-run, not delegated** — uses the Multipass perf VM, which wedges under concurrent/backgrounded `multipass exec` (`feedback_multipass_exec_wedges`): run ONE `timeout 60 multipass exec` at a time, `multipass transfer` for files, never `pkill -f "multipass exec"`. Use the dedicated **`gmountie-perf`** VM, NEVER `gmountie-qa` (`feedback_dedicated_perf_vm`).

**Method (A/B):**
- **BASE (before):** `git -C <worktree> worktree add` (or a clone) at `08e81ff` (branch base / master).
- **HEAD (after):** the delegation branch tip.
- For each, run `scripts/perf/run.sh` on `gmountie-perf` with the SAME env (LAN + WAN netem profiles, COUNT=5, cache on AND off), capturing the bench output.
- Compare seq read/write throughput, readdir, random read/write, and metadata RTT. Phase 1 is **coherence-only**; expectation = within noise (±~20% VM jitter — trust deterministic deltas over single-run swings, per `project_fio_flaky_rootcause`).

- [ ] **Step 1:** Ensure `gmountie-perf` is up; build the `gmountie` binary at BASE and at HEAD (two binaries), `multipass transfer` both to the VM.
- [ ] **Step 2:** Run the perf harness for BASE (LAN+WAN × cache on/off, COUNT=5); save `perf-before.txt`.
- [ ] **Step 3:** Run the perf harness for HEAD with identical env; save `perf-after.txt`.
- [ ] **Step 4:** Diff the two; record a short before/after table in the PR body. If any metric regresses beyond VM noise, investigate (likely the mutating-path arbiter call or the recall-stream goroutine) before merge.
- [ ] **Step 5: Prune the plan + open the PR.**

```bash
git -C <worktree> rm docs/superpowers/plans/2026-06-23-delegation-recall-phase1.md
git -C <worktree> commit -m "docs(delegation): prune Phase 1 implementation plan post-merge-prep"
```

Single PR (design + implementation together, per `feedback_consolidate_related_prs`). Conventional-commit title, descriptive body **including the before/after perf table**, NO AI attribution. Title: `feat(delegation): Phase 1 — write-delegation + recall coherence layer`.

---

## Self-Review

**Spec coverage** — every settled spec item maps to a task:
- Delegation table + containment + narrower-grant carving → Task 2a. Cooldown (exp/capped/TTL) → Task 2b. Arbiter + no-lock-across-RTT + recall coalescing → Task 4. Recall registry/transport (dedicated bidi stream, ack/timeout) → Tasks 1, 3, 5. Recall-on-contention + grant piggyback in handlers → Task 6. **Session-reap → ReleaseSession** → Task 5. Self-access-never-recall → Tasks 4, 6. Cross-subtree rename forced synchronous → Tasks 6 (server arbitrates both ends) + 8 (client). Client write-set/LCA → Task 7. Manager/oracle/recall handler + clean posWritePath layer → Tasks 8, 10. Cache-invalidate-on-recall → Task 8 (`OnRecall` → `InvalidateSubtree`). Skip-reval oracle + the two discriminating tests (correctness never-stale + perf reconnect-window) → Task 9. Demand-driven negotiation (no handshake) → Task 10 (piggyback only; no capability exchange). Metrics → Task 5. Docs fold → Task 12.
- **Phase-2 gates explicitly OUT:** WAL, deferred close, fencing-by-delegation-gen, SQL watermark, per-fd handle seam — recorded in Task 12's deferred section, not implemented.

**Placeholder scan** — integration tasks (5 Step 3, 6 Step 3, 8 Step 4, 9 Step 3, 10 Step 4) say "read region X first" because they touch large existing files; each gives the concrete anchor (symbol + the exact predicate/handler to change) and the real surrounding signatures from first-hand reads, not vague "add handling." The pure-logic tasks (2a, 2b, 3, 4, 7) carry complete code.

**Type consistency** — `OnMutation(session, path) error`, `Request(owner, root) *proto.DelegationGrant`, `ReleaseSession(id)`, `IsDelegated(path) bool`, `Apply(*proto.DelegationGrant)`, `OnRecall(root)`, `RequestedRoot() string`, `InvalidateSubtree(path)` are used identically across server (Tasks 4/5/6) and client (Tasks 8/9/10). The wire types `DelegationRequest{root}` / `DelegationGrant{granted_root,excluded_paths,retry_after_ms}` / `RecallMsg{root,recall_id}` / `RecallAck{recall_id,done}` are defined once (Task 1) and referenced unchanged everywhere.
