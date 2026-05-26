# Read-Path Allocation Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the wasted-prefetch over-issue at the default `readahead_window=1` and pool the server-side per-RPC frame buffer, returning baseline `SeqRead*` allocations to ≤ the pre-rewrite (v1) level.

**Architecture:** Two independent, small changes. Client: `Readahead.Observe` skips arming a prefetch when the FUSE read size exceeds the chunk size (a chunk can never satisfy a larger read, so `Serve` can never hit and the prefetch is pure waste). Server: `ReadStreamer` recycles frame-sized read buffers through a `sync.Pool` instead of `make`-ing one per RPC. No wire-protocol, fd-model, or config change.

**Tech Stack:** Go, `github.com/hanwen/go-fuse/v2/fuse`, `github.com/stretchr/testify/suite`, `go test -bench`/`benchstat`/`pprof`. Spec: `docs/superpowers/specs/2026-05-27-read-path-allocs-design.md` (`88ac90e`).

**Worktree:** `.claude/worktrees/read-path-allocs`, branch `worktree-read-path-allocs`, off `origin/master` `b5dc764`. Run all commands from the worktree root.

---

## File map

- **Modify** `pkg/client/io/readahead.go` — add the unservable-skip guard in `Observe`.
- **Modify** `pkg/client/io/readahead_test.go` — add two suite methods (unservable-skip; single-in-flight invariant).
- **Modify** `pkg/server/service/file_streaming.go` — add `sync.Pool` of frame buffers to `ReadStreamer`.
- **Create** `pkg/server/service/file_streaming_test.go` — `AllocsPerRun` test that the frame buffer is pooled.
- **Modify (Task 5)** `docs/design/performance.md` — durable consolidation + Bencher slug fix.

---

## Task 1: Readahead unservable-skip (Fix B)

**Files:**
- Modify: `pkg/client/io/readahead.go` (`Observe`, after the threshold check)
- Test: `pkg/client/io/readahead_test.go` (append methods to existing `ReadaheadTestSuite`, `package io`, white-box)

- [ ] **Step 1: Write the failing test**

Append to `pkg/client/io/readahead_test.go` (the suite and its `TestReadaheadSuite` runner already exist at the bottom of the file):

```go
func (s *ReadaheadTestSuite) TestObserve_ReadLargerThanChunkArmsNothing() {
	// chunk=4096, threshold=3, window=1. Each read is 8192 bytes — larger than
	// one chunk — so Serve can never hit (Serve misses when len(dest) > avail,
	// and avail <= chunkSize). Observe must therefore never arm a wasted
	// prefetch, even once the sequential threshold is met.
	r := NewReadahead(4096, 3, 1)

	s.Require().Empty(r.Observe(0, 8192))
	s.Require().Empty(r.Observe(8192, 8192))
	arm := r.Observe(16384, 8192)
	s.Assert().Empty(arm, "a read larger than one chunk must not arm a prefetch")
	s.Assert().Empty(r.chunks, "no chunk should be left armed for an unservable read size")
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/client/io/ -run 'TestReadaheadSuite/TestObserve_ReadLargerThanChunkArmsNothing' -v`
Expected: FAIL — on current code the third (threshold-meeting) `Observe` arms `[24576]`, so `arm` is non-empty and `r.chunks` is non-empty.

- [ ] **Step 3: Add the unservable-skip guard**

In `pkg/client/io/readahead.go`, inside `Observe`, locate:

```go
	if r.seqHits < r.threshold {
		return nil
	}

	// Arm new slots until the window is full.
```

Insert the guard between the threshold check and the arm comment so the block reads:

```go
	if r.seqHits < r.threshold {
		return nil
	}

	// A single chunk can never satisfy a read larger than itself, so Serve can
	// never hit for this access pattern. Arming a prefetch here is pure waste:
	// the chunk would be evicted on the next read before it is ever served,
	// then re-armed — repeating every read. Leave the window empty until the
	// reads fit within one chunk. The real win for large-buffer readers is the
	// SP5 partial-consume redesign, not this prefetch.
	if n > r.chunkSize {
		return nil
	}

	// Arm new slots until the window is full.
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test ./pkg/client/io/ -run 'TestReadaheadSuite/TestObserve_ReadLargerThanChunkArmsNothing' -v`
Expected: PASS.

- [ ] **Step 5: Add the single-in-flight characterization test**

Append to `pkg/client/io/readahead_test.go`:

```go
func (s *ReadaheadTestSuite) TestObserve_SingleInFlightNeverExceedsWindowOne() {
	// Guards an existing invariant the fix must not break: at window=1, across a
	// sequential servable run, Observe arms at most one chunk per call and the
	// window never holds more than one chunk. Reads are 4096 (== chunk), so
	// they are servable and the unservable-skip does not apply.
	r := NewReadahead(4096, 1, 1)

	off := int64(0)
	for i := 0; i < 8; i++ {
		arm := r.Observe(off, 4096)
		s.Require().LessOrEqual(len(arm), 1, "window=1 must arm at most one prefetch per Observe")
		s.Require().LessOrEqual(len(r.chunks), 1, "window=1 must never hold more than one chunk")
		off += 4096
	}
}
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `go test ./pkg/client/io/ -run 'TestReadaheadSuite/TestObserve_SingleInFlightNeverExceedsWindowOne' -v`
Expected: PASS (this invariant holds on both old and new code — it is a guard, not a red→green driver).

- [ ] **Step 7: Run the whole readahead suite**

Run: `go test ./pkg/client/io/ -run TestReadaheadSuite -v`
Expected: PASS — including the pre-existing `TestObserve_SequentialOffsetsTriggerPrefetch` (4096-byte reads with a 4096 chunk still arm, proving the guard does not break the servable path).

- [ ] **Step 8: Commit**

```bash
git add pkg/client/io/readahead.go pkg/client/io/readahead_test.go
git commit -m "fix(client/io): readahead skips arming when read size exceeds chunk

At window=1 a read larger than readahead_chunk_bytes can never be served
from a single chunk, so the prefetch is pure waste: the chunk is evicted on
the next read before it is served, then re-armed every read. Observe now
returns no arm offsets when n > chunkSize, eliminating the wasted prefetch
RPCs that regressed the default-config sequential-read path. The servable
path (read <= chunk) is unchanged."
```

---

## Task 2: Server frame-buffer pool (Fix A)

**Files:**
- Modify: `pkg/server/service/file_streaming.go` (import, `ReadStreamer` struct, `NewReadStreamer`, `Stream`)
- Test: `pkg/server/service/file_streaming_test.go` (new file)

- [ ] **Step 1: Write the failing test**

Create `pkg/server/service/file_streaming_test.go`:

```go
package service

import (
	"context"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type FileStreamingTestSuite struct {
	suite.Suite
}

// TestStream_FrameBufferIsPooledNotAllocatedPerCall asserts that repeated
// Stream calls do not each allocate a fresh frame buffer. Without pooling,
// every call allocates one frameSize buffer (allocs/call == 1); with the pool
// the buffer is recycled and the per-call allocation amortizes below 1.
func (s *FileStreamingTestSuite) TestStream_FrameBufferIsPooledNotAllocatedPerCall() {
	const frameSize = 4096
	streamer := NewReadStreamer(frameSize)
	ctx := context.Background()

	// Alloc-free fileRead (reports a full frame, copies nothing) and emit.
	fileRead := func(buf []byte, off int64) (int, fuse.Status) {
		return len(buf), fuse.OK
	}
	emit := func(data []byte, st fuse.Status) error { return nil }

	run := func() {
		_ = streamer.Stream(ctx, frameSize, 0, fileRead, emit)
	}

	// Warm the pool so the one-time New allocation is not counted.
	run()

	allocs := testing.AllocsPerRun(200, run)
	s.Assert().Less(allocs, 1.0,
		"frame buffer must come from the pool, not a per-call make([]byte, frameSize)")
}

func TestFileStreamingSuite(t *testing.T) {
	suite.Run(t, new(FileStreamingTestSuite))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test ./pkg/server/service/ -run 'TestFileStreamingSuite/TestStream_FrameBufferIsPooledNotAllocatedPerCall' -v`
Expected: FAIL — current `Stream` does `make([]byte, s.frameSize)` every call, so `allocs == 1.0`, which is not `< 1.0`.

- [ ] **Step 3: Add the `sync` import**

In `pkg/server/service/file_streaming.go`, change the import block:

```go
import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
)
```

to:

```go
import (
	"context"
	"sync"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
)
```

- [ ] **Step 4: Add the pool field to the struct**

Change:

```go
type ReadStreamer struct {
	frameSize int
}
```

to:

```go
type ReadStreamer struct {
	frameSize int
	// bufPool recycles frame-sized read buffers across Stream calls. It holds
	// *[]byte (not []byte) so Put does not box a slice header onto the heap
	// (staticcheck SA6002). frameSize is fixed per server, so every pooled
	// buffer is the same length.
	bufPool sync.Pool
}
```

- [ ] **Step 5: Initialise the pool in the constructor**

Change:

```go
func NewReadStreamer(frameSize int) *ReadStreamer {
	return &ReadStreamer{frameSize: frameSize}
}
```

to:

```go
func NewReadStreamer(frameSize int) *ReadStreamer {
	return &ReadStreamer{
		frameSize: frameSize,
		bufPool: sync.Pool{
			New: func() any {
				b := make([]byte, frameSize)
				return &b
			},
		},
	}
}
```

- [ ] **Step 6: Borrow the buffer from the pool in `Stream`**

In `Stream`, change:

```go
	remaining := totalSize
	off := startOffset
	// One buffer reused across frames. emit must consume data synchronously
	// (gRPC stream.Send marshals before returning), which the controller
	// closure does — so overwriting the buffer for the next read is safe.
	buf := make([]byte, s.frameSize)
```

to:

```go
	remaining := totalSize
	off := startOffset
	// Borrow a frame-sized buffer from the pool and return it on exit. emit
	// consumes data synchronously (gRPC stream.Send marshals before
	// returning), so the buffer is safe to reuse across frames within this
	// call and to return to the pool once the final emit completes.
	bufp := s.bufPool.Get().(*[]byte)
	defer s.bufPool.Put(bufp)
	buf := *bufp
```

The rest of `Stream` (the `buf[:chunk]` / `buf[:n]` slicing) is unchanged.

- [ ] **Step 7: Run the test to verify it passes**

Run: `go test ./pkg/server/service/ -run 'TestFileStreamingSuite/TestStream_FrameBufferIsPooledNotAllocatedPerCall' -v`
Expected: PASS — with a warm pool the per-call buffer allocation amortizes to ~0.

- [ ] **Step 8: Run the full service package to confirm no regression**

Run: `go test ./pkg/server/service/ -v`
Expected: PASS — existing `compound`, `session`, `auth`, `volume` suites unaffected; the new streaming suite passes.

- [ ] **Step 9: Commit**

```bash
git add pkg/server/service/file_streaming.go pkg/server/service/file_streaming_test.go
git commit -m "perf(server/service): pool ReadStreamer frame buffers across RPCs

ReadStreamer.Stream allocated a fresh make([]byte, frameSize) per Read RPC —
the dominant read-path allocator (a 64 MiB read allocated ~64 frame buffers).
Recycle frame-sized buffers through a sync.Pool on the shared ReadStreamer.
Safe because emit consumes synchronously (gRPC Send marshals before
returning), the same invariant the in-call reuse already relied on. The pool
holds *[]byte to avoid the SA6002 slice-boxing allocation on Put."
```

---

## Task 3: Lint + full test gate

**Files:** none (verification only)

- [ ] **Step 1: Lint**

Run: `task lint`
Expected: PASS — in particular no `SA6002` (the pool holds `*[]byte`, not `[]byte`).

- [ ] **Step 2: Full test suite**

Run: `task test`
Expected: PASS (non-FUSE suites; the FUSE-mount e2e suites are exercised on the VM in Task 4).

- [ ] **Step 3: Commit (only if lint auto-fixed anything)**

```bash
git add -A
git commit -m "chore: lint fixups for read-path allocation changes"
```

(Skip this step if `git status` is clean.)

---

## Task 4: VM allocation confirmation (integration)

**Files:** none (runs on the kubevirt VM `192.168.11.11`; allocations are deterministic so this is the authoritative check). Requires the user-approved VM-execution path.

- [ ] **Step 1: Sync the fixed worktree to the VM**

Run from the worktree root:

```bash
rsync -az --exclude=.git -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null" \
  ./ ubuntu@192.168.11.11:/home/ubuntu/gm-perf-fix/
```

Expected: completes without error. (`~/gm-perf-v1` from the earlier investigation — the pre-rewrite `7df048e` baseline — is already on the VM.)

- [ ] **Step 2: Memory-profile SeqRead64MiB on the VM (fix build)**

Run:

```bash
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@192.168.11.11 '
  cd ~/gm-perf-fix && go test -c -o ~/perf-fix.test ./test/e2e/perf/ &&
  GMOUNTIE_BENCH_TCP=1 ~/perf-fix.test -test.run="^$" -test.bench="SeqRead64MiB$" \
    -test.benchmem -test.count=1 -test.benchtime=20x -test.memprofile=$HOME/mem-fix.out \
    | tail -3'
```

Expected: a `BenchmarkSeqRead64MiB` line. **Record its `B/op` and `allocs/op`.**

- [ ] **Step 3: Compare allocations to v1**

Run:

```bash
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@192.168.11.11 '
  go tool pprof -top -alloc_space -nodecount=8 ~/perf-fix.test ~/mem-fix.out 2>/dev/null | sed -n "1,14p"'
```

Expected: `service.(*ReadStreamer).Stream` is no longer a dominant flat allocator (the pool removed its per-RPC `make`), and the `SeqRead64MiB` `B/op` from Step 2 is **≤ v1's ~808 MiB** (v2/v3 were ~1045 MiB). Acceptance criterion 1 is met when `B/op ≤ v1`.

- [ ] **Step 4: (Optional) throughput direction on the fix build**

Run:

```bash
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ubuntu@192.168.11.11 '
  cd ~/gm-perf-fix &&
  GMOUNTIE_BENCH_TCP=1 go test -run="^$" -bench="SeqRead(1|16|64)MiB$" -benchmem \
    -count=6 -benchtime=3s ./test/e2e/perf/'
```

Expected (informational, not a hard gate — the VM's ±14–38% throughput noise cannot resolve a ~14% effect): the `SeqRead*/lan` MB/s should sit at or above the recorded v1 baseline (v1 ≈ `SeqRead64MiB` 114.5, `SeqRead16MiB` 108.1, `SeqRead1MiB` 85.7 MB/s; v2/v3 regressed to ~100/95/80). The deterministic acceptance gate is the allocation check in Step 3; the authoritative throughput confirmation is a dedicated-runner Bencher run dispatched via the release perf workflow, out of scope for plan execution.

---

## Task 5: Durable docs + Bencher slug fix

**Files:**
- Modify: `docs/design/performance.md`

- [ ] **Step 1: Record the readahead behaviour in §2.5**

In `docs/design/performance.md` §2.5, after the paragraph describing the `readahead_window` knob, add:

```markdown
At `window=1` the client keeps at most one prefetch in flight and does not arm
a prefetch when the FUSE read size exceeds `readahead_chunk_bytes`: a single
chunk can never satisfy a larger read, so `Serve` cannot hit and the prefetch
would be pure waste. Consequently readahead is a no-op for readers whose buffer
exceeds the chunk size — making it effective for those readers is the SP5
partial-consume redesign (§5.1), not this path.
```

- [ ] **Step 2: Record the frame-buffer pool in §3.1**

In `docs/design/performance.md` §3.1, after the per-frame copy budget block, add:

```markdown
The server recycles the frame-sized read buffer through a `sync.Pool` on the
shared `ReadStreamer` rather than allocating one per Read RPC, so a large
sequential read reuses a single buffer instead of one per frame. This is the
dominant read-path allocation removed; it is safe because `emit` consumes each
frame synchronously (gRPC `Send` marshals before returning).
```

- [ ] **Step 3: Fix the Bencher project slug**

In `docs/design/performance.md`, change the Bencher project identifier from `gmountie` to the actual queryable slug `gmountie-tfkojd8g` in two places: §4.4 ("**Project:** `gmountie`") and the §7 glossary "Bencher" entry. (The `bencher` CLI and API reject `gmountie` — that slug belongs to a different org.)

- [ ] **Step 4: Commit**

```bash
git add docs/design/performance.md
git commit -m "docs(perf): record readahead unservable-skip + frame-buffer pooling

Document the window=1 single-in-flight + unservable-skip behaviour (§2.5) and
the ReadStreamer frame-buffer pool (§3.1), and correct the Bencher project
slug to gmountie-tfkojd8g (§4.4 + glossary)."
```

- [ ] **Step 5: Prune the transient spec (at merge time)**

When this branch merges to `master`, delete `docs/superpowers/specs/2026-05-27-read-path-allocs-design.md` and this plan — the durable record now lives in `docs/design/performance.md`, per the working agreement (transient specs/plans are consolidated then pruned). Do this as part of the merge, not during task execution.
