# SP5 — Partial-consume Pipelined Readahead Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make client readahead effective for sequential reads on high-RTT links by redesigning the readahead engine for partial-consume, cross-chunk, full-or-miss serving with a deep window of large in-flight fetches.

**Architecture:** Client-only. Evolve the existing chunk-slot `Readahead` (`pkg/client/io/readahead.go`): add a per-chunk consumed cursor so partially-read chunks are retained; rewrite `Serve` to copy across contiguous ready chunks (full-or-miss); drop `Observe`'s `n > chunkSize` no-op guard so big reads arm a deep window; make eviction respect partial consume. Then bump the default chunk size (→ 1 MiB) and window (→ 4). No wire/server/cache/fd change; `doPrefetch`'s drive in `backend_grpc.go` is unchanged.

**Tech Stack:** Go, testify suites, go-fuse, gRPC. Unit tests run anywhere; the WAN-netem read bench runs on the kubevirt VM (`192.168.11.11`).

**Spec:** `docs/superpowers/specs/2026-05-27-sp5-partial-consume-readahead-design.md`

**Conventions (from CLAUDE.md + memory):** module path `gmountie`; tests are methods on a testify `suite`; conventional-commit subject + short body, NO `Co-Authored-By`/`Signed-off-by`; logging via `gmountie/pkg/utils/log`; FUSE-mount tests run on the VM, not the sandbox.

**Run a unit suite:** `go test -v -run TestReadaheadSuite ./pkg/client/io/`
**Lint:** `task lint`

---

## Current code (read before editing)

`pkg/client/io/readahead.go` today:
- `raChunk{ off int64; data []byte }` — `data == nil` while in flight.
- `Observe(off, n)`: bumps the sequential-run counter; evicts chunks with `c.off+chunkSize <= off+n`; **returns `nil` when `n > chunkSize`** (the read-path-allocs no-op guard); else arms up to `window` chunks ahead.
- `Serve(dest, off)`: hits only if a single ready chunk fully covers `dest` (`len(dest) > end-off` → miss) and **discards the whole chunk on a hit** (one-shot consume).
- `Store(off, data)`: marks a slot ready.

SP5 reverses the two guards (the no-op skip and one-shot consume), so four existing tests in `readahead_test.go` that pin the old behavior must be updated (Task 1, Step 1):
- `TestServe_FullRangeHitReturnsBytesAndConsumesRing` (one-shot consume)
- `TestServeMissWhenDestLargerThanChunk` (kept, but a cross-chunk hit test is added)
- `TestObserve_ReadLargerThanChunkArmsNothing` (the `n > chunkSize` no-op — reversed)
- `TestObserve_PrefetchResumesWhenReadsShrinkBelowChunk` (built on that no-op — reversed)

---

## Task 1: Redesign the `Readahead` engine (partial-consume, cross-chunk, deep window)

**Files:**
- Modify: `pkg/client/io/readahead.go` (`raChunk`, `Serve`, `Observe`)
- Test: `pkg/client/io/readahead_test.go`

Use TDD: write/adjust the tests first, watch them fail, implement, watch them pass.

- [ ] **Step 1: Update the four tests that pin the old behavior, and add the new-behavior tests (all should now fail/compile-fail)**

In `pkg/client/io/readahead_test.go`:

(a) **Replace** `TestServe_FullRangeHitReturnsBytesAndConsumesRing` with a retention test:

```go
func (s *ReadaheadTestSuite) TestServe_PartialConsumeRetainsChunk() {
	r := NewReadahead(4096, 3, 1)
	stored := make([]byte, 4096)
	for i := range stored {
		stored[i] = byte(i % 251)
	}
	r.Observe(0, 4096)
	r.Observe(4096, 4096)
	arm := r.Observe(8192, 4096)
	s.Require().Equal([]int64{12288}, arm)
	r.Store(12288, stored)

	// First read consumes the front 1024 bytes; the chunk is retained.
	dest := make([]byte, 1024)
	n, hit := r.Serve(dest, 12288)
	s.Require().True(hit)
	s.Require().Equal(1024, n)
	s.Assert().Equal(stored[:1024], dest)

	// Next sequential read hits the retained tail of the SAME chunk.
	dest2 := make([]byte, 1024)
	n, hit = r.Serve(dest2, 12288+1024)
	s.Require().True(hit, "retained tail must still serve")
	s.Require().Equal(1024, n)
	s.Assert().Equal(stored[1024:2048], dest2)

	// Drain the rest (2048..4096); chunk then drops and a re-serve misses.
	rest := make([]byte, 2048)
	_, hit = r.Serve(rest, 12288+2048)
	s.Require().True(hit)
	_, hit = r.Serve(make([]byte, 1), 12288)
	s.Assert().False(hit, "fully drained chunk must be gone")
}
```

(b) **Replace** `TestObserve_ReadLargerThanChunkArmsNothing` (premise reversed — big reads now arm):

```go
func (s *ReadaheadTestSuite) TestObserve_ReadLargerThanChunkArmsWindow() {
	// chunk=4096, window=2: a read larger than one chunk must now arm the
	// window (SP5 makes it servable via cross-chunk serve), not skip.
	r := NewReadahead(4096, 1, 2)
	arm := r.Observe(0, 8192)
	s.Require().Len(arm, 2, "large read must arm a deep window now")
	s.Assert().Equal([]int64{8192, 12288}, arm, "arm contiguous chunks ahead of cursor")
}
```

(c) **Replace** `TestObserve_PrefetchResumesWhenReadsShrinkBelowChunk` (no longer about a skip; large reads arm immediately):

```go
func (s *ReadaheadTestSuite) TestObserve_DeepWindowArmsForLargeReads() {
	r := NewReadahead(1<<20, 1, 4) // 1 MiB chunk, window 4
	arm := r.Observe(0, 1<<20)     // one full-chunk read
	s.Require().Len(arm, 4, "deep window armed ahead of the cursor")
	s.Assert().Equal(int64(1<<20), arm[0])
	s.Assert().Equal(int64(4<<20), arm[3])
}
```

(d) **Keep** `TestServeMissWhenDestLargerThanChunk` (with one ready chunk a too-large dest still misses — full-or-miss), and **add** a cross-chunk hit:

```go
func (s *ReadaheadTestSuite) TestServe_CrossChunkHitSpansContiguousChunks() {
	// Observe(0, 4096) arms prefetch slots starting at 4096 (the read at 0 is
	// not a slot). Store the two contiguous armed slots 4096 and 8192.
	r := NewReadahead(4096, 1, 4)
	arm := r.Observe(0, 4096)
	s.Require().Contains(arm, int64(4096))
	s.Require().Contains(arm, int64(8192))
	c0 := make([]byte, 4096)
	c1 := make([]byte, 4096)
	for i := range c0 {
		c0[i] = byte(i % 251)
		c1[i] = byte((i + 7) % 251)
	}
	r.Store(4096, c0)
	r.Store(8192, c1)

	// Read [6144, 10240) spans the 4096|8192 boundary: 2048 bytes from each.
	dest := make([]byte, 4096)
	n, hit := r.Serve(dest, 6144)
	s.Require().True(hit, "read spanning two contiguous ready chunks must hit")
	s.Require().Equal(4096, n)
	s.Assert().Equal(c0[2048:], dest[:2048])
	s.Assert().Equal(c1[:2048], dest[2048:])
}

func (s *ReadaheadTestSuite) TestServe_MissWhenNextChunkNotReady() {
	r := NewReadahead(4096, 1, 4)
	r.Observe(0, 4096)                // arms 4096, 8192, ...
	r.Store(4096, make([]byte, 4096)) // only the first armed slot is ready
	// A read past the one ready chunk misses (full-or-miss) and must not
	// mutate it.
	dest := make([]byte, 4096+10)
	n, hit := r.Serve(dest, 4096)
	s.Assert().False(hit)
	s.Assert().Equal(0, n)
	// The ready chunk still serves a fitting read afterwards.
	d2 := make([]byte, 4096)
	_, hit = r.Serve(d2, 4096)
	s.Assert().True(hit, "a miss must leave the ready chunk intact")
}
```

Note for the implementer: in step (a), `Observe` is called with the arming you need (`NewReadahead(4096,3,1)`); in (d) the construction uses `window>=2` so two chunks can coexist. If an arming call returns offsets you then `Store`, store exactly those offsets. Read the existing kept tests (`TestObserve_SequentialOffsetsTriggerPrefetch`, `TestWindowFillsAheadAndSlides`, `TestObserve_SingleInFlightNeverExceedsWindowOne`, `TestDoesNotReArmInflight`, `TestObserve_BackwardSeekResetsState`) — those stay and must still pass.

- [ ] **Step 2: Run the suite to confirm the new/updated tests fail (and the kept ones still pass)**

Run: `go test -v -run TestReadaheadSuite ./pkg/client/io/`
Expected: the new partial-consume / cross-chunk / large-read-arm tests FAIL (old `Serve` one-shot-consumes and old `Observe` skips `n>chunkSize`); kept tests pass.

- [ ] **Step 3: Add the consumed cursor to `raChunk`**

In `pkg/client/io/readahead.go`:

```go
type raChunk struct {
	off      int64  // file offset where this fetch was issued
	data     []byte // nil while in flight; bytes once Stored
	consumed int    // bytes already served from the front of data (partial consume)
}
```

The live (servable) range of a ready chunk is `[off+consumed, off+len(data))`.

- [ ] **Step 4: Rewrite `Serve` — partial-consume, cross-chunk, full-or-miss, side-effect-free on a miss**

Replace the `Serve` body with:

```go
// Serve satisfies Read(dest, off) from the ready window when the full range
// [off, off+len(dest)) is covered by one or more contiguous ready chunks.
// It copies across chunk boundaries, advances each touched chunk's consumed
// cursor, and drops any chunk that becomes fully drained. Partially-consumed
// chunks are retained so the next sequential read hits their tail. On any miss
// (no covering chunk, or a gap/not-ready chunk before the range ends) it
// returns (0, false) and mutates nothing — the caller's synchronous Read then
// fetches the whole dest. Stale eviction is Observe's job, not Serve's.
func (r *Readahead) Serve(dest []byte, off int64) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	need := int64(len(dest))
	if need == 0 {
		return 0, true
	}
	end := off + need

	// Locate the ready chunk whose live range contains off.
	start := -1
	for i := range r.chunks {
		c := r.chunks[i]
		if c.data == nil {
			continue
		}
		if c.off+int64(c.consumed) <= off && off < c.off+int64(len(c.data)) {
			start = i
			break
		}
	}
	if start == -1 {
		return 0, false
	}

	// Verify contiguous ready coverage to end WITHOUT mutating.
	covEnd := r.chunks[start].off + int64(len(r.chunks[start].data))
	last := start
	for covEnd < end {
		next := last + 1
		if next >= len(r.chunks) {
			return 0, false
		}
		c := r.chunks[next]
		if c.data == nil || c.off != covEnd {
			return 0, false
		}
		covEnd = c.off + int64(len(c.data))
		last = next
	}

	// Coverage confirmed — copy and advance consumed cursors.
	written := int64(0)
	for i := start; i <= last; i++ {
		c := &r.chunks[i]
		srcStart := int64(0)
		if i == start {
			srcStart = off - c.off
		}
		take := int64(len(c.data)) - srcStart
		if take > need-written {
			take = need - written
		}
		copy(dest[written:written+take], c.data[srcStart:srcStart+take])
		written += take
		if int(srcStart+take) > c.consumed {
			c.consumed = int(srcStart + take)
		}
	}

	// Drop fully-drained chunks.
	kept := r.chunks[:0]
	for _, c := range r.chunks {
		if c.data != nil && c.consumed >= len(c.data) {
			continue
		}
		kept = append(kept, c)
	}
	r.chunks = kept
	return int(written), true
}
```

- [ ] **Step 5: Rewrite `Observe` — drop the no-op guard; eviction respects consumed/ready length; arm for any read size**

In `Observe`, **delete** the block:

```go
	if n > r.chunkSize {
		return nil
	}
```

and replace the eviction loop with one that uses each chunk's real end (ready = `len(data)`, in-flight = `chunkSize`):

```go
	next := off + int64(n)
	kept := r.chunks[:0]
	for _, c := range r.chunks {
		endOff := c.off + int64(r.chunkSize) // in-flight: assume full size
		if c.data != nil {
			endOff = c.off + int64(len(c.data))
		}
		if endOff > next {
			kept = append(kept, c)
		}
	}
	r.chunks = kept
```

Leave the threshold gate and the arming loop (`armFrom` … `for len(r.chunks) < r.window`) exactly as they are — they already arm contiguous chunks ahead and cap at `window`.

- [ ] **Step 6: Run the suite to verify all readahead tests pass**

Run: `go test -v -run TestReadaheadSuite ./pkg/client/io/`
Expected: PASS — the new partial-consume/cross-chunk/large-read tests and every kept test. Then run the whole package: `go test ./pkg/client/io/` → PASS (the Read drive in `backend_grpc.go` is source-compatible; `Serve`/`Observe` signatures are unchanged).

- [ ] **Step 7: Commit**

```bash
git add pkg/client/io/readahead.go pkg/client/io/readahead_test.go
git commit -m "feat(client/io): partial-consume, cross-chunk, deep-window readahead

Serve now copies across contiguous ready chunks and retains partially
consumed chunks (full-or-miss; side-effect-free on miss). Observe drops
the read-path-allocs n>chunkSize no-op guard so large reads arm the
window, and eviction respects the consumed cursor. This is what lets a
deep window actually pipeline sequential reads on a high-RTT link."
```

---

## Task 2: Bump readahead default tuning (chunk → 1 MiB, window → 4)

**Files:**
- Modify: `pkg/client/config/rpc.go` (`DefaultReadaheadChunkBytes`, `DefaultReadaheadWindow`)
- Test: `pkg/client/config/config_test.go`

- [ ] **Step 1: Update the failing-expectation test first**

Find the config defaults test in `pkg/client/config/config_test.go` that asserts readahead defaults (search for `ReadaheadChunkBytes` / `ReadaheadWindow`). Update the expected values to the new defaults (and add assertions if missing):

```go
	s.Assert().Equal(1<<20, result.Rpc.ReadaheadChunkBytes)
	s.Assert().Equal(4, result.Rpc.ReadaheadWindow)
	s.Assert().Equal(3, result.Rpc.ReadaheadThreshold) // unchanged
```

If no such assertions exist yet, add them to the "defaults" test case alongside the existing `DefaultFUSEMaxWriteBytes`-style assertions.

- [ ] **Step 2: Run to confirm it fails**

Run: `go test -v -run TestConfig ./pkg/client/config/` (use the actual suite/test name in that file)
Expected: FAIL — current defaults are 64 KiB / window 1.

- [ ] **Step 3: Bump the default constants**

In `pkg/client/config/rpc.go`:

```go
	// DefaultReadaheadChunkBytes is the default size of a single readahead
	// fetch. 1 MiB matches the default FUSE max-write / server frame size, so
	// each prefetch is one server frame and the server's frame-buffer pool is
	// sized for it. Capped down by the Version handshake if the server
	// advertises a smaller frame.
	DefaultReadaheadChunkBytes = 1 << 20
	// DefaultReadaheadThreshold is the number of strictly-sequential reads
	// required before the client arms prefetches.
	DefaultReadaheadThreshold = 3
	// DefaultReadaheadWindow is the number of readahead chunks kept in flight
	// ahead of the cursor. 4 is a bandwidth-delay-product start for ~50 ms RTT
	// / 100 Mbit; the knob ranges [1,64] so operators on longer/fatter pipes
	// raise it. Each in-flight chunk is one concurrent Read RPC.
	DefaultReadaheadWindow = 4
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -v -run TestConfig ./pkg/client/config/` then `go test ./pkg/client/config/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/config/rpc.go pkg/client/config/config_test.go
git commit -m "feat(client/config): readahead defaults — 1 MiB chunk, window 4

With the partial-consume readahead engine, a deep window of frame-sized
fetches is the WAN read win. Default chunk to 1 MiB (one server frame)
and window to 4 (a BDP start for ~50 ms / 100 Mbit). Knobs unchanged."
```

---

## Task 3: Validate on the VM (unit gate + WAN-netem read bench)

**Run location:** the kubevirt VM (`192.168.11.11`) — real FUSE + `tc netem`. The sandbox cannot mount FUSE. Sync the worktree to an isolated path (e.g. `rsync` to `~ubuntu/gmountie-sp5`) and run there.

**No code changes** unless the bench reveals a regression; this task is the acceptance gate.

- [ ] **Step 1: Full unit suite + lint on the VM**

Run (on the VM): `go test -count=1 ./pkg/client/... ./pkg/server/... ./pkg/proto/... ./internal/...` and `task lint`
Expected: all green (the `pkg/client/mount` real-FUSE suite passes on the VM).

- [ ] **Step 2: WAN-netem SeqRead bench, window=4 vs window=1**

On the VM, apply the WAN profile and run the read benches over TCP transport, comparing the new default (window 4) against a window=1 baseline:

```bash
sudo scripts/perf/profile.sh apply wan
# baseline (window 1) and new default (window 4); GMOUNTIE_BENCH_TCP=1 so netem bites
GMOUNTIE_BENCH_TCP=1 go test -run=^$ -bench 'BenchmarkSeqRead' -benchmem \
  -count=5 -benchtime=10s ./test/e2e/perf/ | tee perf-out/sp5-read.txt
sudo scripts/perf/profile.sh clear
```
(The bench reads through the client; `readahead_window`/`readahead_chunk_bytes` come from the bench harness config. Confirm the perf harness uses the config defaults or set them explicitly via the harness's options — check `test/e2e/perf` setup; if the harness pins readahead, run once with window=1 and once with window=4 to get the comparison.)

Expected: `SeqRead{16,64}MiB/wan` throughput materially higher with window=4 than window=1, climbing toward the ~11.9 MiB/s link ceiling; no regression at window=1 or on LAN.

- [ ] **Step 3: Record the outcome (no committed numbers)**

Note in the eventual PR description whether the read win materialized and at what window depth. Do NOT commit benchmark numbers to the repo (Bencher is the system of record). If the win did not materialize, STOP and report — the design assumption (HTTP/2 multiplexing pipelines the deep window once chunks are servable) would need re-examination before shipping.

- [ ] **Step 4: Commit (only if Step 2 required a code tweak)**

If the bench was clean, nothing to commit here. If a tweak was needed (e.g. the harness needed a readahead option), commit it with a `test(e2e):`-prefixed message.

---

## Task 4: Fold durable docs + prune the spec (on ship, after merge decision)

**Files:**
- Modify: `docs/design/performance.md`, `docs/roadmap.md`
- Delete: the SP5 spec + this plan (transient working docs)

> Do this as the final step once the implementation is reviewed and the bench confirms the win — it is the "on ship" durable-docs fold from the spec. Kept as a task so it isn't forgotten.

- [ ] **Step 1: Rewrite `docs/design/performance.md` §5.1** from "deferred SP5" to the implemented design: partial-consume + cross-chunk full-or-miss `Serve` with retention, deep window, eviction-with-retention. Update §2.5 (readahead description) and the §6 config table (chunk → 1 MiB, window → 4). Add the deferred buffer-pool follow-up note (`window × chunkSize`/fd, pool if Bencher flags).

- [ ] **Step 2: Update `docs/roadmap.md`** — under "Near-term deferred performance levers", mark **SP5 — Partial-consume readahead** as Done (link to `design/performance.md`), leaving the CodecV2 lever.

- [ ] **Step 3: Delete the transient working docs** and commit:

```bash
git rm docs/superpowers/specs/2026-05-27-sp5-partial-consume-readahead-design.md \
       docs/superpowers/plans/2026-05-27-sp5-partial-consume-readahead.md
git add docs/design/performance.md docs/roadmap.md
git commit -m "docs: fold SP5 readahead into design/performance.md; prune spec+plan"
```

---

## Self-review notes (plan vs spec)

- **Spec coverage:** `Serve` redesign (Task 1, Steps 3-4), `Observe` redesign (Task 1, Step 5), config defaults (Task 2), VM bench win (Task 3), durable-doc fold (Task 4), the four reversed tests handled (Task 1, Step 1). All spec sections mapped.
- **Type consistency:** `raChunk{off, data, consumed}` introduced in Task 1 Step 3 and used by both `Serve` (Step 4) and `Observe` (Step 5). `Serve`/`Observe`/`Store` signatures unchanged, so `backend_grpc.go` and tests compile without drive changes. Config constants `DefaultReadaheadChunkBytes`/`DefaultReadaheadWindow` are the only Task 2 symbols, wired via `factory.go` `WithReadahead` (unchanged).
- **No placeholders:** every code step shows the actual code; the one investigate-then-act spot (Task 3 Step 2, whether the perf harness pins readahead) is flagged with the concrete check, not left vague.
- **Memory footprint:** deferred per spec; surfaced in the Task 4 doc note, not built.
