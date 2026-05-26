# WAN link saturation (SP4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Saturate the WAN link on sustained sequential I/O by deepening readahead into an N-wide prefetch window (config-knobbed) and validating/hardening the opt-in FUSE writeback cache — all within the existing durable-fd + retryable-RPC model (no protocol/fd change).

**Architecture:** Reads gain an N-deep prefetch window (`Readahead` keeps up to `ReadaheadWindow` short-lived Read RPCs in flight ahead of the cursor) instead of today's single-chunk prefetch. Writes are pipelined by the kernel when `writeback_cache` is enabled (already wired; default stays off) — SP4 proves it composes with the cache/subscribe layers and the FUSE writeback contract. Resilience is untouched: every fetch/write is a retryable RPC against a session-held fd.

**Tech Stack:** Go, go-fuse v2.10.1 (FUSE), gRPC, testify suites, the `test/e2e` harness, Bencher (perf via `perf.yml`).

**Spec:** `docs/superpowers/specs/2026-05-26-wan-link-saturation-design.md`

---

## File structure

- `pkg/client/config/rpc.go` — add `ReadaheadWindow` (default `1`, validated `1..64`).
- `pkg/client/io/readahead.go` — single-slot prefetch → ordered N-deep window.
- `pkg/client/io/backend_grpc.go` — construct `Readahead` with the window; drive up to `ReadaheadWindow` concurrent prefetch Read RPCs from `Observe`'s returned offsets.
- `pkg/client/config/fuse.go`, `pkg/client/mount/common.go` — refresh the stale "pending Phase 4" comments (no default change).
- `pkg/client/io/node.go` / server `pkg/server/...` — only if writeback validation surfaces a `setattr`/size gap.
- Tests beside each; a writeback-consistency e2e under `test/e2e`.

> **Read-half first (Tasks 1–4), then write-half validation (Tasks 5–6), then acceptance (Task 7).** The read half is self-contained and independently shippable; the write half is mostly validation of an existing opt-in.

---

## Task 1: `ReadaheadWindow` config knob

**Files:**
- Modify: `pkg/client/config/rpc.go`
- Test: `pkg/client/config/config_test.go`

- [ ] **Step 1: Write the failing test**

Append to `pkg/client/config/config_test.go` (testify suite `ConfigTestSuite`, mirror the existing `TestParse_RpcDefaults`/`TestParse_RpcOverride`):

```go
func (s *ConfigTestSuite) TestParse_ReadaheadWindowDefault() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(DefaultReadaheadWindow, result.Rpc.ReadaheadWindow)
	s.Assert().Equal(1, result.Rpc.ReadaheadWindow) // default = today's single-chunk behavior
}

func (s *ConfigTestSuite) TestParse_ReadaheadWindowOverride() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
rpc:
  readahead_window: 12
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(12, result.Rpc.ReadaheadWindow)
}

func (s *ConfigTestSuite) TestParse_ReadaheadWindowRejectsOutOfRange() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
rpc:
  readahead_window: 0
`
	_, err := LoadConfigFromString(conf)
	s.Require().Error(err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -run 'TestConfigTestSuite/TestParse_ReadaheadWindow' ./pkg/client/config/`
Expected: FAIL — `DefaultReadaheadWindow` / `ReadaheadWindow` undefined.

- [ ] **Step 3: Implement**

In `pkg/client/config/rpc.go`, add the const (next to `DefaultReadaheadThreshold`):

```go
	// DefaultReadaheadWindow is the number of readahead chunks kept in flight
	// ahead of the cursor. 1 = today's single-chunk prefetch (LAN-tuned, no
	// regression). WAN users raise it so window*chunk covers the
	// bandwidth-delay product (e.g. 8-16 at ~50ms RTT / 100 Mbit).
	DefaultReadaheadWindow = 1
```

Add the field to `RpcConfig` (next to `ReadaheadThreshold`):

```go
	// ReadaheadWindow is how many ReadaheadChunkBytes chunks to keep
	// prefetched/in-flight ahead of a sequential reader. 1 preserves the
	// legacy single-chunk behaviour.
	ReadaheadWindow int `mapstructure:"readahead_window" validate:"min=1,max=64"`
```

In `NewRpcConfig`, set the struct default (next to `ReadaheadThreshold: DefaultReadaheadThreshold,`):

```go
		ReadaheadWindow: DefaultReadaheadWindow,
```

and the viper default (next to the `readahead_threshold` SetDefault):

```go
	v.SetDefault("readahead_window", DefaultReadaheadWindow)
```

- [ ] **Step 4: Run to verify pass**

Run: `go test -count=1 ./pkg/client/config/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/config/rpc.go pkg/client/config/config_test.go
git commit -m "feat(client/config): add readahead_window knob (default 1)"
```

---

## Task 2: `Readahead` — single slot → ordered N-deep window

**Files:**
- Modify: `pkg/client/io/readahead.go`
- Test: `pkg/client/io/readahead_test.go`

The new contract (the tests are authoritative): `NewReadahead(chunkSize, threshold, window)`. `Observe(off, n)` returns a slice of chunk offsets to prefetch now (0..window of them) to keep the window full ahead of `off+n`; it tracks each as in-flight so it isn't re-armed. `Serve(dest, off)` returns `(len(dest), true)` from whichever ready window chunk covers `off`, consumes it, and drops chunks now behind the cursor. `Store(off, data)` marks an in-flight chunk ready. `window==1` reproduces today's behavior exactly. A non-sequential `Observe` drops the whole window.

- [ ] **Step 1: Write the failing tests**

Replace/extend `pkg/client/io/readahead_test.go`'s suite with these (keep existing single-slot tests but update `NewReadahead` calls to pass a window; add the window cases):

```go
func (s *ReadaheadSuite) TestWindowFillsAheadAndSlides() {
	r := NewReadahead(100, 1, 4) // chunk=100, threshold=1, window=4
	// First sequential read at 0 arms a full window ahead (offsets 100,200,300,400... up to 4 chunks).
	arm := r.Observe(0, 100)
	s.Require().Len(arm, 4)
	s.Assert().Equal([]int64{100, 200, 300, 400}, arm)
	// Fulfil the in-flight chunks.
	for _, off := range arm {
		r.Store(off, make([]byte, 100))
	}
	// Serve the next sequential read from the window; it consumes chunk@100 and
	// the window tops back up to 4 ahead (arms 500).
	n, hit := r.Serve(make([]byte, 100), 100)
	s.Require().True(hit)
	s.Assert().Equal(100, n)
	arm2 := r.Observe(100, 100)
	s.Assert().Equal([]int64{500}, arm2) // only the newly-vacated slot is re-armed
}

func (s *ReadaheadSuite) TestWindowOneEqualsLegacy() {
	r := NewReadahead(100, 1, 1)
	arm := r.Observe(0, 100)
	s.Require().Len(arm, 1)
	s.Assert().Equal([]int64{100}, arm)
	r.Store(100, make([]byte, 100))
	// No second prefetch armed while the single chunk is outstanding.
	arm2 := r.Observe(0, 100) // re-observe same (non-advancing) -> still just keeps 1 ahead
	s.Assert().Empty(arm2)
}

func (s *ReadaheadSuite) TestNonSequentialDropsWindow() {
	r := NewReadahead(100, 1, 4)
	for _, off := range r.Observe(0, 100) {
		r.Store(off, make([]byte, 100))
	}
	// Backwards seek -> window dropped, re-arm fresh from the new position.
	arm := r.Observe(1000, 100)
	s.Assert().Equal([]int64{1100, 1200, 1300, 1400}, arm)
	// The old chunk@100 must no longer Serve.
	_, hit := r.Serve(make([]byte, 100), 100)
	s.Assert().False(hit)
}

func (s *ReadaheadSuite) TestServeMissWhenDestLargerThanChunk() {
	r := NewReadahead(100, 1, 4)
	for _, off := range r.Observe(0, 100) {
		r.Store(off, make([]byte, 100))
	}
	_, hit := r.Serve(make([]byte, 150), 100) // dest 150 > chunk 100
	s.Assert().False(hit)
}

func (s *ReadaheadSuite) TestDoesNotReArmInflight() {
	r := NewReadahead(100, 1, 4)
	r.Observe(0, 100) // arms 100..400 as in-flight (not yet Stored)
	arm := r.Observe(0, 100) // same cursor, window already full of in-flight -> nothing new
	s.Assert().Empty(arm)
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test -run 'TestReadaheadSuite/TestWindow' ./pkg/client/io/`
Expected: FAIL — `NewReadahead` arity / `Observe` return type changed.

- [ ] **Step 3: Implement the window**

Rewrite `pkg/client/io/readahead.go`'s state + methods. Reference implementation:

```go
// raChunk is one window slot. data == nil means the prefetch is in flight;
// non-nil means it's fetched and ready to Serve.
type raChunk struct {
	off  int64
	data []byte
}

type Readahead struct {
	mu        sync.Mutex
	chunkSize int
	threshold int
	window    int // max chunks tracked ahead (in-flight + ready)
	seqHits   int
	lastOff   int64
	lastSize  int
	chunks    []raChunk // ascending by off; len <= window
}

func NewReadahead(chunkSize, threshold, window int) *Readahead {
	if window < 1 {
		window = 1
	}
	return &Readahead{chunkSize: chunkSize, threshold: threshold, window: window}
}

// Observe updates the sequential-run counter after a Read of n bytes at off and
// returns the chunk offsets to prefetch now to top the window up to `window`
// chunks ahead of off+n. Each returned offset is recorded as in-flight so a
// later Observe before its Store won't re-arm it.
func (r *Readahead) Observe(off int64, n int) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	sequential := off == r.lastOff+int64(r.lastSize)
	if sequential {
		r.seqHits++
	} else {
		r.seqHits = 1
		r.chunks = nil
	}
	r.lastOff = off
	r.lastSize = n
	if r.seqHits < r.threshold {
		return nil
	}

	next := off + int64(n)
	// Drop chunks fully behind the cursor.
	kept := r.chunks[:0]
	for _, c := range r.chunks {
		if c.off+int64(r.chunkSize) > next {
			kept = append(kept, c)
		}
	}
	r.chunks = kept

	// Next offset to arm: one chunk past the highest tracked, else `next`.
	armFrom := next
	if len(r.chunks) > 0 {
		armFrom = r.chunks[len(r.chunks)-1].off + int64(r.chunkSize)
	}
	var arm []int64
	for len(r.chunks) < r.window {
		r.chunks = append(r.chunks, raChunk{off: armFrom}) // in-flight (data nil)
		arm = append(arm, armFrom)
		armFrom += int64(r.chunkSize)
	}
	return arm
}

// Store marks the in-flight chunk at off ready with data. No-op if the window
// dropped it (intervening non-sequential Observe).
func (r *Readahead) Store(off int64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.chunks {
		if r.chunks[i].off == off {
			r.chunks[i].data = data
			return
		}
	}
}

// Serve satisfies Read(dest, off) from a ready window chunk covering off,
// one-shot consuming it. Miss (returns 0,false, window untouched) when no ready
// chunk covers off or dest is larger than the chunk.
func (r *Readahead) Serve(dest []byte, off int64) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.chunks {
		c := r.chunks[i]
		if c.data == nil {
			continue
		}
		end := c.off + int64(len(c.data))
		if off < c.off || off >= end {
			continue
		}
		if int64(len(dest)) > end-off {
			return 0, false
		}
		copy(dest, c.data[off-c.off:off-c.off+int64(len(dest))])
		r.chunks = append(r.chunks[:i], r.chunks[i+1:]...) // consume
		return len(dest), true
	}
	return 0, false
}
```

(Adjust the test expectations if your final algorithm legitimately arms a different but equivalent offset set — the *contract* is "≤ window chunks ahead, in-flight not re-armed, slides on consume, dropped on non-sequential." Make the tests and code agree.)

- [ ] **Step 4: Run to verify pass**

Run: `go test -count=1 ./pkg/client/io/ -run TestReadaheadSuite`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/readahead.go pkg/client/io/readahead_test.go
git commit -m "feat(client/io): N-deep readahead window (Observe returns arm offsets)"
```

---

## Task 3: Drive the window from the backend (concurrent prefetch)

**Files:**
- Modify: `pkg/client/io/backend_grpc.go` (`Read` path callers of `Observe`; `NewReadahead` construction)
- Test: `pkg/client/io/backend_grpc_test.go`

- [ ] **Step 1: Write the failing test**

In `backend_grpc_test.go` (mirror the existing readahead-prefetch test setup; the suite already builds a `BackendClient` over a mocked `RpcFileClient`):

```go
func (s *BackendGrpcSuite) TestReadFillsPrefetchWindow() {
	// Open a handle with a window of 3 and a small chunk. After the threshold
	// sequential reads, the backend issues up to 3 concurrent prefetch Read RPCs.
	h := s.openReadableHandle("/big", PerFileConfig{ReadaheadChunkBytes: 64, ReadaheadThreshold: 1, ReadaheadWindow: 3})
	// mock Read to return a 64-byte chunk for any prefetch offset.
	s.fileMock.EXPECT().Read(mock.Anything, mock.Anything).Return(s.readStream(make([]byte, 64)), nil).Maybe()
	_, _ = s.backend.Read(s.ctx, h, 0, make([]byte, 64))
	// allow prefetch goroutines to run
	s.Eventually(func() bool { return s.readCallCount() >= 3 }, time.Second, 5*time.Millisecond)
}
```

(Use the suite's existing mock-Read helper and call-counter names; if absent, count via a `mock.Anything` `.Run` counter. The assertion is "≥ window prefetch Reads issued.")

- [ ] **Step 2: Run to verify it fails / compiles against the new API**

Run: `go test -run 'TestReadFillsPrefetchWindow' ./pkg/client/io/`
Expected: FAIL (window not driven; `ReadaheadWindow` not threaded to `NewReadahead`).

- [ ] **Step 3: Implement**

In `backend_grpc.go`:
- Thread the window into construction (the `cfg.ReadaheadChunkBytes > 0 && cfg.ReadaheadThreshold > 0` branch near line 1031):

```go
	if cfg.ReadaheadChunkBytes > 0 && cfg.ReadaheadThreshold > 0 {
		h.readahead = NewReadahead(cfg.ReadaheadChunkBytes, cfg.ReadaheadThreshold, cfg.ReadaheadWindow)
	}
```

- At the two `Observe` call sites (≈ lines 504 and 556), `Observe` now returns `[]int64`; fan out a bounded prefetch per offset:

```go
	if h.readahead != nil {
		for _, prefetchOff := range h.readahead.Observe(off, res.written) {
			go b.doPrefetch(h, prefetchOff)
		}
	}
```

(Do the same at the Serve-hit site. `doPrefetch` is unchanged — it fetches one chunk and `Store`s it under `h.lifeCtx`, so window prefetches are still cancelled on Release. The window bound caps concurrency to `ReadaheadWindow` because `Observe` only arms vacant slots.)

- Add `ReadaheadWindow int` to the `PerFileConfig` struct and populate it where `PerFileConfig` is built from `RpcConfig` (find the builder — same place `ReadaheadChunkBytes`/`ReadaheadThreshold` are copied).

- [ ] **Step 4: Run to verify pass + no regression**

Run: `go test -count=1 -race ./pkg/client/io/`
Expected: `ok` (window test passes; existing readahead tests with window defaulting/explicit still pass; `-race` clean — multiple prefetch goroutines + `Serve`/`Store` under `r.mu`).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/backend_grpc_test.go
git commit -m "feat(client/io): drive N concurrent prefetch Reads from the readahead window"
```

---

## Task 4: Refresh the stale writeback comments (no behavior change)

**Files:**
- Modify: `pkg/client/config/fuse.go`, `pkg/client/mount/common.go`

- [ ] **Step 1: Update the comments**

In `fuse.go`, the `DefaultFUSEWritebackCache` doc and the `WritebackCache` field doc say "pending Phase 4's cache layer." The cache layer exists now. Reword to: writeback is an opt-in WAN-throughput mode (default off) that lets the kernel pipeline writes asynchronously; enable with `writeback_cache: true`. Same for the `common.go` comment near the `CAP_WRITEBACK_CACHE` toggle. **Do not change the default or the toggle logic.**

- [ ] **Step 2: Verify no behavior change**

Run: `go build ./pkg/client/... && go test -count=1 ./pkg/client/config/ ./pkg/client/mount/ 2>&1 | grep -vE 'fusermount|not a socket'`
Expected: builds; config tests pass (mount FUSE tests fail only in a sandbox — ignore those, run on the VM).

- [ ] **Step 3: Commit**

```bash
git add pkg/client/config/fuse.go pkg/client/mount/common.go
git commit -m "docs(client): writeback_cache is a validated opt-in WAN mode, not 'pending'"
```

---

## Task 5: Validate the writeback contract (investigation + e2e)

**Files:**
- Read: `pkg/client/io/node.go` (`Setattr`, `Write`, `Getattr`), the server `Setattr`/size handling, `pkg/client/cache/backend.go` (`Write` write-through/invalidate).
- Create/Modify: `test/e2e/` writeback-consistency test (mirror an existing e2e suite in `test/e2e/fs` that mounts a real server+FUSE).

- [ ] **Step 1: Trace the writeback contract**

With `CAP_WRITEBACK_CACHE` the kernel owns size/mtime for cached writes and supplies them on `setattr`/flush. Read `node.go` `Setattr` and the server's size/mtime handling and answer: does a `writeback_cache: true` mount correctly (a) extend file size on buffered append, (b) honor a kernel-supplied size on `setattr` (truncate), (c) flush mtime? Note any gap. Also confirm `cachedBackend.Write` (invalidate-on-write) fires on the kernel's async flush, not the app `write()`, and that this doesn't break the subscribe/validity revalidation.

- [ ] **Step 2: Write the failing e2e test**

Add to `test/e2e/fs` (mirror an existing mount-based suite; it spins up a server + FUSE mount in-process). Mount with `writeback_cache: true`:

```go
func (s *WritebackSuite) TestWriteThenReadBack() {
	// write a file through the writeback mount, then read it back via a fresh
	// open — bytes must match (close-to-open).
	path := filepath.Join(s.mnt, "wb.bin")
	want := bytes.Repeat([]byte("x"), 3<<20) // 3 MiB > one max_write
	s.Require().NoError(os.WriteFile(path, want, 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Assert().Equal(want, got)
}

func (s *WritebackSuite) TestTruncateUnderWriteback() {
	path := filepath.Join(s.mnt, "wb-trunc.bin")
	s.Require().NoError(os.WriteFile(path, bytes.Repeat([]byte("y"), 1<<20), 0o644))
	s.Require().NoError(os.Truncate(path, 4096))
	fi, err := os.Stat(path)
	s.Require().NoError(err)
	s.Assert().Equal(int64(4096), fi.Size())
}
```

(Use the e2e harness's mount helper, passing `FUSEConfig{WritebackCache: true, MaxBackground: 64, MaxWriteBytes: 1<<20}`. Look at `test/e2e/utils/app.go` for how the mount is built and add a writeback variant.)

- [ ] **Step 3: Run on the VM (FUSE)**

These need a real FUSE mount — run on the kubevirt VM (sandbox can't mount):
`task -t testing/scratch/Taskfile.yml test:e2e-fs` (or the equivalent that runs `./test/e2e/fs/...`), or sync + `go test ./test/e2e/fs/...` on the VM.
Expected: PASS. If Step 1 found a `setattr`/size gap, fix it in `node.go`/server until these pass.

- [ ] **Step 4: Commit**

```bash
git add pkg/client/io/node.go test/e2e/ # + server files if a gap was fixed
git commit -m "test(e2e): writeback_cache write-then-read + truncate correctness"
# If a fix was needed, use: "fix(client/io): honor kernel size/mtime under writeback_cache"
```

---

## Task 6: Writeback error + multi-client invalidation behavior

**Files:**
- Modify: `test/e2e/` (extend the writeback suite)

- [ ] **Step 1: Write the tests**

```go
func (s *WritebackSuite) TestWriteErrorSurfacesAtClose() {
	// Cause the server write to fail (e.g. write into a read-only volume or a
	// path the server rejects), then assert the error reaches the app at
	// Close()/Sync, not silently swallowed. (Mirror how other e2e error tests
	// induce a server-side failure.)
}

func (s *WritebackSuite) TestSecondClientSeesWriteAfterClose() {
	// With subscribe enabled, write+close on mount A, then read on mount B and
	// confirm the new bytes are visible (close-to-open) and the validity layer
	// revalidated rather than serving stale.
}
```

(Fill the failure-induction and second-mount setup from the existing e2e patterns — `test/e2e/fs` already has subscribe + error-path tests to copy.)

- [ ] **Step 2: Run on the VM**

Run the e2e fs suite on the VM (as Task 5 Step 3).
Expected: PASS — write errors surface at close (SP3's `WriteAndFlush` path), close-to-open visibility holds.

- [ ] **Step 3: Commit**

```bash
git add test/e2e/
git commit -m "test(e2e): writeback_cache error-at-close + close-to-open multi-client"
```

---

## Task 7: Bencher acceptance — does A reach the ceiling?

**Files:** none (measurement); record results in the spec.

- [ ] **Step 1: Full gate**

Run: `task lint && go test -count=1 ./pkg/client/... ./pkg/server/...` (ignore the sandbox FUSE `pkg/client/mount` failures).
Expected: lint 0 issues; tests pass.

- [ ] **Step 2: Push the branch + dispatch perf on it**

Push the SP4 branch, then dispatch the on-demand perf workflow against it with a deep readahead + writeback on (configure via the perf harness's client config / env if it exposes them; otherwise the bench's default mount opts must be set to `WritebackCache: true`, `ReadaheadWindow` deep for this run):

```bash
gh workflow run perf.yml --ref <sp4-branch> -f bencher_branch=sp4 -f count=6 -f benchtime=3s
```

- [ ] **Step 3: Compare to the master baseline in Bencher**

Open the Bencher project; compare branch `sp4` vs `master` for WAN `SeqRead*` and `SeqWrite*`.
- **Pass:** WAN SeqRead/SeqWrite climb materially toward ~11.9 MiB/s; LAN unregressed.
- **Fail-to-saturate:** record the gap — this is the documented trigger to escalate to streams-per-fd (option B) in a new spec.

- [ ] **Step 4: Record + flip spec status**

Edit `docs/superpowers/specs/2026-05-26-wan-link-saturation-design.md`: set status to `implemented`, append the Bencher before/after WAN numbers under acceptance criterion #3. Commit.

---

## Self-review notes

- **Spec coverage:** read-half window → Tasks 1–3; readahead-window config knob (default 1, no regression) → Task 1; writeback opt-in validation (FUSE contract, cache/subscribe, error-at-close) → Tasks 4–6; resilience-preserved (no protocol/fd change) → holds by construction (Tasks touch only readahead + config + e2e); Bencher acceptance + escalation trigger → Task 7. All spec acceptance criteria map to a task.
- **Type/name consistency:** `ReadaheadWindow` (config, Task 1) flows into `PerFileConfig`/`NewReadahead(chunkSize, threshold, window)` (Tasks 2–3); `Observe` returns `[]int64` consistently in Tasks 2 and 3; `raChunk`/`chunks` window state is internal to Task 2.
- **No placeholders:** config + window + driving are concrete code; the window tests are the authoritative contract (with a note to keep code/tests in agreement on equivalent arm-offset sets). The write-half tasks (5–6) are validation: they contain concrete e2e test skeletons against the existing `test/e2e/fs` harness, with the failure-induction/second-mount specifics drawn from existing patterns there — the one genuinely investigation-shaped step (Task 5 Step 1) feeds a conditional fix that the e2e tests gate.
