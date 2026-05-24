# Cache fsync reduction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the client cache's per-write `fdatasync` cost — both the no-op invalidation commits and the per-commit fsync on the cache index — to close the LAN write-throughput gap.

**Architecture:** Two independent, behavior-safe changes to `pkg/client/cache/persist`. (A) Invalidation methods probe with a read-only txn and skip the writable txn when nothing would be deleted. (B) `meta.db` opens with `NoSync`, a 1s background goroutine calls `db.Sync()`, and `Close` does a final sync. The cache is write-through to the server and reconstructable, so relaxing index durability loses at most cache entries (→ re-fetch), never user data.

**Tech Stack:** Go, `go.etcd.io/bbolt` v1.4.3, testify suites, `task` (go-task), golangci-lint.

**Spec:** `docs/superpowers/specs/2026-05-24-cache-fsync-reduction-design.md`

---

## File structure

- `pkg/client/cache/persist/dataidx.go` — add read-probe skips to `InvalidateChunkRange` and `InvalidatePathChunks`.
- `pkg/client/cache/persist/kv.go` — add read-probe skip to `kvDelete` (covers `DeleteAttrBytes` + `DeleteDirBytes`).
- `pkg/client/cache/persist/persist.go` — `NoSync` on open; syncer goroutine + lifecycle fields; final sync in `Close`; `metaSyncInterval` const.
- `pkg/client/cache/persist/testing_helpers_test.go` — `TestingMetaWriteCount`, `TestingNoSync` helpers (internal `package persist`).
- `pkg/client/cache/persist/fsync_test.go` — new external (`package persist_test`) suite + regression benchmark.

---

## Task 1: Test helpers to observe bbolt internals

**Files:**
- Modify: `pkg/client/cache/persist/testing_helpers_test.go`

These let the external test package assert on `p.db` internals (it can't reach the unexported field directly). Mirrors the existing `TestingChunkPath`/`TestingForceFormatVersion` pattern.

- [ ] **Step 1: Add the helpers**

Append to `pkg/client/cache/persist/testing_helpers_test.go` (it is `package persist`):

```go
// TestingMetaWriteCount returns bbolt's cumulative count of page writes.
// A committed writable transaction increments it — even under NoSync,
// which skips the fsync but still performs the write — while a skipped
// transaction does not. Used to assert no-op invalidations open no
// writable txn.
func TestingMetaWriteCount(p *Persist) int64 { return p.db.Stats().TxStats.GetWrite() }

// TestingNoSync reports whether meta.db was opened with fsync-on-commit
// disabled.
func TestingNoSync(p *Persist) bool { return p.db.NoSync }
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./pkg/client/cache/persist/ && go vet ./pkg/client/cache/persist/`
Expected: no output, exit 0. (`go vet` compiles test files.)

- [ ] **Step 3: Commit**

```bash
git add pkg/client/cache/persist/testing_helpers_test.go
git commit -m "test(client/cache/persist): helpers to observe bbolt write count + NoSync"
```

---

## Task 2: Skip no-op `InvalidateChunkRange`

**Files:**
- Modify: `pkg/client/cache/persist/dataidx.go:164` (`InvalidateChunkRange`)
- Test: `pkg/client/cache/persist/fsync_test.go` (new)

- [ ] **Step 1: Write the failing tests**

Create `pkg/client/cache/persist/fsync_test.go`:

```go
package persist_test

import (
	"testing"

	"gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type PersistFsyncSuite struct {
	suite.Suite
	dir string
}

func (s *PersistFsyncSuite) SetupTest() { s.dir = s.T().TempDir() }

// A range invalidation over a path that was never cached must not open a
// writable transaction (which would commit + fsync for nothing).
func (s *PersistFsyncSuite) TestInvalidateChunkRangeNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidateChunkRange("/never/written", 0, 4))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "no-op range invalidation must not open a writable txn")
}

// A range invalidation that actually has an entry must still commit and
// remove it.
func (s *PersistFsyncSuite) TestInvalidateChunkRangeRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	hash, _, err := p.WriteChunk([]byte("hello"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/f", 0, persist.ChunkRef{Hash: hash, Size: 5}))

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidateChunkRange("/f", 0, 0))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "real invalidation must commit a writable txn")

	_, ok, err := p.GetChunkRef("/f", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "entry must be gone after invalidation")
}

func TestPersistFsyncSuite(t *testing.T) { suite.Run(t, new(PersistFsyncSuite)) }
```

- [ ] **Step 2: Run the tests to verify the no-op test fails**

Run: `go test -run 'TestPersistFsyncSuite/TestInvalidateChunkRangeNoOpSkipsTxn' -v ./pkg/client/cache/persist/`
Expected: FAIL — `after` is greater than `before` because the current `InvalidateChunkRange` always opens `db.Update`.

- [ ] **Step 3: Add the read-probe skip**

In `pkg/client/cache/persist/dataidx.go`, at the very top of `InvalidateChunkRange` (before `var toUnlink ...` / the `p.db.Update` call), insert:

```go
	// Probe first: a writable txn commits (and, absent NoSync, fsyncs)
	// even when it deletes nothing. The hot write path invalidates ranges
	// that were never cached, so skip the txn when no entry exists in range.
	var hasAny bool
	if err := p.db.View(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		for i := firstIdx; i <= lastIdx; i++ {
			if idx.Get(dataIdxKey(path, i)) != nil {
				hasAny = true
				return nil
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if !hasAny {
		return nil
	}
```

- [ ] **Step 4: Run both tests to verify they pass**

Run: `go test -run 'TestPersistFsyncSuite/TestInvalidateChunkRange' -v ./pkg/client/cache/persist/`
Expected: PASS (both `NoOpSkipsTxn` and `RealStillDeletes`).

- [ ] **Step 5: Run the full persist suite (no regression in refcount/dataidx)**

Run: `go test ./pkg/client/cache/persist/`
Expected: `ok`.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/cache/persist/dataidx.go pkg/client/cache/persist/fsync_test.go
git commit -m "perf(client/cache/persist): skip no-op InvalidateChunkRange txn

A range invalidation over a never-cached path opened a writable bbolt
txn that committed + fsynced while deleting nothing. Probe with a read
txn first; only open the writable txn when an entry exists in range."
```

---

## Task 3: Skip no-op `InvalidatePathChunks`

**Files:**
- Modify: `pkg/client/cache/persist/dataidx.go:118` (`InvalidatePathChunks`)
- Test: `pkg/client/cache/persist/fsync_test.go`

- [ ] **Step 1: Add the failing tests**

Append to the `PersistFsyncSuite` in `pkg/client/cache/persist/fsync_test.go`:

```go
func (s *PersistFsyncSuite) TestInvalidatePathChunksNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidatePathChunks("/never/written"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "no-op path invalidation must not open a writable txn")
}

func (s *PersistFsyncSuite) TestInvalidatePathChunksRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	hash, _, err := p.WriteChunk([]byte("hello"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/f", 0, persist.ChunkRef{Hash: hash, Size: 5}))

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidatePathChunks("/f"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "real path invalidation must commit a writable txn")

	_, ok, err := p.GetChunkRef("/f", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "entry must be gone after invalidation")
}
```

- [ ] **Step 2: Run to verify the no-op test fails**

Run: `go test -run 'TestPersistFsyncSuite/TestInvalidatePathChunksNoOpSkipsTxn' -v ./pkg/client/cache/persist/`
Expected: FAIL — current code always opens `db.Update`.

- [ ] **Step 3: Add the read-probe skip**

In `pkg/client/cache/persist/dataidx.go`, at the very top of `InvalidatePathChunks` (before the `p.db.Update` call), insert:

```go
	// Probe first: skip the writable txn when the path has no cached
	// chunks (same rationale as InvalidateChunkRange).
	var hasAny bool
	if err := p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketDataIdx).Cursor()
		prefix := dataIdxPathPrefix(path)
		if k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix) {
			hasAny = true
		}
		return nil
	}); err != nil {
		return err
	}
	if !hasAny {
		return nil
	}
```

(`bytes` is already imported in `dataidx.go`.)

- [ ] **Step 4: Run both tests to verify they pass**

Run: `go test -run 'TestPersistFsyncSuite/TestInvalidatePathChunks' -v ./pkg/client/cache/persist/`
Expected: PASS.

- [ ] **Step 5: Run the full persist suite**

Run: `go test ./pkg/client/cache/persist/`
Expected: `ok`.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/cache/persist/dataidx.go pkg/client/cache/persist/fsync_test.go
git commit -m "perf(client/cache/persist): skip no-op InvalidatePathChunks txn"
```

---

## Task 4: Skip no-op `kvDelete` (attr + dir removers)

**Files:**
- Modify: `pkg/client/cache/persist/kv.go:31` (`kvDelete`)
- Test: `pkg/client/cache/persist/fsync_test.go`

`DeleteAttrBytes` and `DeleteDirBytes` both route through `kvDelete`, so one fix covers the attr remover (the second per-write fsync on the bench) and the dir remover.

- [ ] **Step 1: Add the failing tests**

Append to `PersistFsyncSuite` in `pkg/client/cache/persist/fsync_test.go`:

```go
func (s *PersistFsyncSuite) TestDeleteAttrBytesNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/absent"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "deleting an absent attr key must not open a writable txn")
}

func (s *PersistFsyncSuite) TestDeleteAttrBytesRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	s.Require().NoError(p.PutAttrBytes("/a", []byte("x")))
	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/a"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "deleting a present key must commit a writable txn")

	_, ok, err := p.GetAttrBytes("/a")
	s.Require().NoError(err)
	s.Assert().False(ok, "key must be gone after delete")
}
```

- [ ] **Step 2: Run to verify the no-op test fails**

Run: `go test -run 'TestPersistFsyncSuite/TestDeleteAttrBytesNoOpSkipsTxn' -v ./pkg/client/cache/persist/`
Expected: FAIL.

- [ ] **Step 3: Add the read-probe skip to `kvDelete`**

Replace the body of `kvDelete` in `pkg/client/cache/persist/kv.go`:

```go
func (p *Persist) kvDelete(bucket []byte, key string) error {
	// Probe first: deleting an absent key still commits + fsyncs a txn
	// that changed nothing. Skip the writable txn when the key is absent.
	_, ok, err := p.kvGet(bucket, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	return p.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Delete([]byte(key))
	})
}
```

- [ ] **Step 4: Run the new tests + existing kv tests**

Run: `go test -run 'TestPersistFsyncSuite/TestDeleteAttrBytes' -v ./pkg/client/cache/persist/ && go test -run 'Kv|KV' -v ./pkg/client/cache/persist/`
Expected: PASS for the new tests; existing kv tests still pass.

- [ ] **Step 5: Run the full persist suite**

Run: `go test ./pkg/client/cache/persist/`
Expected: `ok`.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/cache/persist/kv.go pkg/client/cache/persist/fsync_test.go
git commit -m "perf(client/cache/persist): skip no-op kvDelete txn (attr + dir removers)"
```

---

## Task 5: `NoSync` meta.db + periodic syncer + final sync on Close

**Files:**
- Modify: `pkg/client/cache/persist/persist.go` (struct fields, `Open`, `Close`, new `metaSyncInterval` const + `startMetaSyncer`)
- Test: `pkg/client/cache/persist/fsync_test.go`

- [ ] **Step 1: Add the failing tests**

Append to `PersistFsyncSuite` in `pkg/client/cache/persist/fsync_test.go`:

```go
func (s *PersistFsyncSuite) TestOpenEnablesNoSync() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()
	s.Assert().True(persist.TestingNoSync(p), "meta.db should open with NoSync enabled")
}

// Close must stop the syncer and perform a final sync; data written
// before Close must be readable after reopening the same directory.
func (s *PersistFsyncSuite) TestCloseSyncsAndDataSurvivesReopen() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.Require().NoError(p.PutAttrBytes("/a", []byte("durable")))
	s.Require().NoError(p.Close())

	p2, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p2.Close()
	v, ok, err := p2.GetAttrBytes("/a")
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Assert().Equal([]byte("durable"), v)
}
```

- [ ] **Step 2: Run to verify `TestOpenEnablesNoSync` fails**

Run: `go test -run 'TestPersistFsyncSuite/TestOpenEnablesNoSync' -v ./pkg/client/cache/persist/`
Expected: FAIL — `NoSync` is false (not yet set).

- [ ] **Step 3: Add the `sync` import and lifecycle fields**

In `pkg/client/cache/persist/persist.go`, add `"sync"` to the import block (alongside `"os"`, `"path/filepath"`, `"time"`).

Extend the `Persist` struct:

```go
type Persist struct {
	root     string
	db       *bolt.DB
	lock     *lockHandle
	disk     *diskAccountant
	syncStop chan struct{}
	syncWG   sync.WaitGroup
}
```

- [ ] **Step 4: Add the interval const and the syncer**

Add near the other consts (after `lockRetryInterval`):

```go
// metaSyncInterval bounds how stale the on-disk meta.db can be after an
// unclean crash. meta.db opens NoSync (no per-commit fsync) because the
// cache index is reconstructable — a lost entry costs a cache miss, never
// data. This ticker flushes it durably in the background.
const metaSyncInterval = time.Second
```

Add the syncer method:

```go
// startMetaSyncer launches the background goroutine that periodically
// fsyncs the NoSync meta.db. Stopped by Close before db.Close().
func (p *Persist) startMetaSyncer() {
	p.syncStop = make(chan struct{})
	p.syncWG.Add(1)
	go func() {
		defer p.syncWG.Done()
		t := time.NewTicker(metaSyncInterval)
		defer t.Stop()
		for {
			select {
			case <-p.syncStop:
				return
			case <-t.C:
				_ = p.db.Sync()
			}
		}
	}()
}
```

- [ ] **Step 5: Set `NoSync` on open and start the syncer**

In `Open`, change the `bolt.Open` call to enable `NoSync`:

```go
	db, err := bolt.Open(filepath.Join(opts.Root, "meta.db"), 0o600, &bolt.Options{Timeout: time.Second, NoSync: true})
```

Then, after `p.startBackgroundSweeps()` and before `return p, nil`, add:

```go
	p.startMetaSyncer()
```

- [ ] **Step 6: Stop the syncer and final-sync in `Close`**

Replace `Close` in `pkg/client/cache/persist/persist.go`:

```go
// Close stops the background syncer, flushes bbolt durably, releases the
// lock file, and frees OS resources.
func (p *Persist) Close() error {
	if p.syncStop != nil {
		close(p.syncStop)
		p.syncWG.Wait()
		p.syncStop = nil
	}
	// Final durable flush: NoSync means db.Close() won't fsync pending
	// commits for us.
	_ = p.db.Sync()
	if err := p.db.Close(); err != nil {
		_ = p.lock.release()
		return errors.Wrap(err, "close meta.db")
	}
	return p.lock.release()
}
```

(Setting `p.syncStop = nil` keeps `Close` idempotent — a second call won't double-close the channel.)

- [ ] **Step 7: Run the new tests**

Run: `go test -run 'TestPersistFsyncSuite/(TestOpenEnablesNoSync|TestCloseSyncsAndDataSurvivesReopen)' -v ./pkg/client/cache/persist/`
Expected: PASS.

- [ ] **Step 8: Run the full persist suite (incl. the lock/close tests)**

Run: `go test -count=1 ./pkg/client/cache/persist/`
Expected: `ok`. (Confirms `TestCloseReleasesLock` and the LOCK-retry tests still pass with the new `Close`.)

- [ ] **Step 9: Commit**

```bash
git add pkg/client/cache/persist/persist.go pkg/client/cache/persist/fsync_test.go
git commit -m "perf(client/cache/persist): open meta.db NoSync with periodic + close sync

meta.db now opens NoSync (no per-commit fdatasync); a 1s background
ticker and a final sync on Close bound durability. The cache index is
reconstructable — a lost entry is a cache miss, never data loss (writes
are write-through to the server). Behavior change: cache index durability
is best-effort, not per-commit."
```

---

## Task 6: Regression benchmark for the no-op path

**Files:**
- Modify: `pkg/client/cache/persist/fsync_test.go`

- [ ] **Step 1: Add the benchmark**

Append to `pkg/client/cache/persist/fsync_test.go`:

```go
// BenchmarkInvalidateChunkRangeNoOp guards the Task 2 optimisation: a
// range invalidation over a never-cached path must not pay for a bbolt
// commit. A regression (re-introducing the writable txn) shows up as a
// large ns/op jump on slow-fsync storage.
func BenchmarkInvalidateChunkRangeNoOp(b *testing.B) {
	p, err := persist.Open(persist.Options{Root: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	for b.Loop() {
		if err := p.InvalidateChunkRange("/never/written", 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}
```

- [ ] **Step 2: Run the benchmark**

Run: `go test -run '^$' -bench BenchmarkInvalidateChunkRangeNoOp -benchtime=1000x ./pkg/client/cache/persist/`
Expected: a result line `BenchmarkInvalidateChunkRangeNoOp-... <n> ns/op`, exit 0.

- [ ] **Step 3: Commit**

```bash
git add pkg/client/cache/persist/fsync_test.go
git commit -m "test(client/cache/persist): regression benchmark for no-op invalidation"
```

---

## Task 7: Verify the stale-invalidation safety dependency

The spec relies on the validity layer resetting to `stateUnverified` on a fresh backend so a stale chunk that survives a crash (lost NoSync invalidation) is revalidated, not served. This task verifies that and adds the fallback only if needed.

**Files:**
- Read: `pkg/client/cache/validity.go`, `pkg/client/cache/subscriber.go`, `pkg/client/cache/backend.go` (validity tracker construction + read-path gating)
- Conditionally modify: `pkg/client/cache/persist/dataidx.go`, `pkg/client/cache/persist/kv.go`

- [ ] **Step 1: Trace the validity gate**

Read how `validityTracker` is constructed on a fresh `cachedBackend`, what flips it to `stateVerified`, and whether any code path serves cached bytes as verified **without** a subscribe stream ever delivering a HEARTBEAT. Specifically answer: *if the persistent cache is enabled but subscribe is disabled/unavailable, can a read serve a cached chunk without revalidation?*

Run (orientation): `grep -rn 'globalState\|markGlobalVerified\|stateVerified\|newValidityTracker\|validity' pkg/client/cache/ | grep -v _test`

- [ ] **Step 2: Decide and record the branch**

- **If reads always revalidate when unverified, and unverified is the startup default with no subscribe-independent path to verified:** the dependency holds. Add a one-paragraph note to the spec's "stale-invalidation dependency" section stating it was verified, citing the file:line that gates reads. No code change. Skip to Step 4.
- **If a trusted-without-subscribe read path exists:** implement the fallback in Step 3.

- [ ] **Step 3 (only if needed): Add sync-after-real-invalidation**

When an invalidation *actually* removed an entry, fsync immediately so the removal can't be lost on a crash. In `pkg/client/cache/persist/dataidx.go`, after the `p.db.Update(...)` returns successfully in both `InvalidateChunkRange` and `InvalidatePathChunks` (i.e. the probe found entries and the txn ran), add before the post-commit unlink loop:

```go
	_ = p.db.Sync() // durably persist the removal; a lost invalidation could serve stale data
```

And in `pkg/client/cache/persist/kv.go` `kvDelete`, change the final return to:

```go
	if err := p.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Delete([]byte(key))
	}); err != nil {
		return err
	}
	return p.db.Sync()
```

Add a test to `PersistFsyncSuite` asserting a real invalidation is durable (write entry, invalidate, reopen, assert absent) and update the spec note to record that the fallback was taken.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs(spec): record stale-invalidation safety verification"
# or, if the fallback was implemented:
# git commit -m "fix(client/cache/persist): sync after real invalidations to prevent post-crash stale reads"
```

---

## Task 8: Full gate + VM acceptance re-bench

**Files:**
- Modify: `docs/superpowers/specs/2026-05-24-cache-fsync-reduction-design.md` (status → implemented, record numbers)

- [ ] **Step 1: Lint + full test suite**

Run: `task lint && task test`
Expected: `0 issues.` from lint; all packages `ok` / `PASS` from test.

- [ ] **Step 2: Build a client binary for the VM**

Run: `task build`
Expected: `goreleaser` snapshot build succeeds; binaries under `dist/`.

- [ ] **Step 3: VM re-bench (blocking acceptance — see spec acceptance criterion #4)**

On the kubevirt VM in `testing/scratch/` (the FUSE/`local-path` reference env from saved memory `feedback_fuse_test_env`): start `gMountie serve`, mount a volume with the persistent cache **enabled** (`cache.enabled: true`, a `cache.path` on the `local-path` PVC), and run the 1 MiB sequential-write fio job used previously. Record before (pre-change binary / `v0.2.0-alpha.0`) vs after MiB/s.

Acceptance: after-change throughput is materially above the 31.6 MiB/s baseline. If it is not, stop and re-profile on the VM — the host's fast fsync cannot reproduce the win, so the VM number is the source of truth.

- [ ] **Step 4: Record results + flip spec status**

Edit `docs/superpowers/specs/2026-05-24-cache-fsync-reduction-design.md`: set `Status:` to `implemented`, and append the VM before/after MiB/s under acceptance criterion #4.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-05-24-cache-fsync-reduction-design.md
git commit -m "docs(spec): mark cache fsync reduction implemented; record VM bench"
```

---

## Self-review notes

- **Spec coverage:** A (no-op skip) → Tasks 2–4; B (NoSync + periodic/close sync) → Task 5; crash-consistency stale-invalidation decision → Task 7; testing (unit + benchmark + VM re-bench) → Tasks 2–6, 8; `NoSync`-unconditional + behavior-change documentation → Task 5 commit body + Task 8 spec status. All spec acceptance criteria map to a task.
- **Type/name consistency:** `TestingMetaWriteCount`/`TestingNoSync` (Task 1) used in Tasks 2–5; `PersistFsyncSuite`/`s.dir` consistent across Tasks 2–6; `metaSyncInterval`, `syncStop`, `syncWG`, `startMetaSyncer` consistent within Task 5; existing exported API used (`WriteChunk`, `PutChunkRef`, `GetChunkRef`, `PutAttrBytes`, `GetAttrBytes`, `DeleteAttrBytes`, `InvalidateChunkRange`, `InvalidatePathChunks`) matches the read source.
- **No placeholders:** every code step contains the full code; the only conditional is Task 7 Step 3, gated on an explicit verification outcome with both branches specified.
