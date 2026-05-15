# Phase 3 Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lift gMountie performance above the 4 MiB unary ceiling and prepare the streaming + batched RPCs that Phase 4's cache will consume — without regressing the reliability invariants from Phases 1 and 2.

**Architecture:** Convert `Read` to server-streaming and `Write` to client-streaming so per-frame sizes are tunable; add a `Compound` metadata RPC for `1+N` directory walks; restrict Snappy to the streaming body RPCs; tune FUSE mount options, gRPC keepalive, message sizes; add client-side readahead and small-write coalescing per fd. Idempotency tokens (Phase 1d) and per-session fd tables (Phase 1c) carry forward unchanged — request_id is sent on the first frame of streaming Write to keep retry safe.

**Tech Stack:** gRPC streaming (server-streaming + client-streaming), protobuf, go-fuse v2.10.1 (still `pathfs` in this phase — the `pathfs`→`fs` migration is Phase 4 scope), `golang.org/x/sync/singleflight` for any compound de-duping, existing `gmountie/pkg/utils/log` zap logger, testify suites.

**Spec reference:** `docs/superpowers/specs/2026-05-13-roadmap-reliability-and-performance.md` lines 143–176 (Phase 3 — Performance: streaming, batching, tuning).

**Working agreements that apply in every task:**
- Tests are testify suites (methods on `suite.Suite`), never standalone `func TestX`.
- Mocks under `internal/mocks/` are generated — never hand-edit. Regenerate with `task gen:mocks` after changing any interface that has a mock.
- Errors wrap with `github.com/pkg/errors.Wrap`.
- Logging goes through `gmountie/pkg/utils/log` (use `log.Log`), never `slog` or `fmt`.
- Commits are conventional: `type(scope): subject` + short body explaining *why*. No `Co-Authored-By:` / `Signed-off-by:` trailers.
- Backwards compatibility is not a concern. Wire format and config keys can change; release notes document the break.
- All commands run from the repo root unless otherwise stated. FUSE mount tests must run on the kubevirt VM at `192.168.11.11` (the Claude sandbox can't open `/dev/fuse`). Plain `go test` of non-FUSE packages can run locally.
- The perf harness built in Task 1 is available throughout. Any intermediate task can spot-check its own delta with `task perf:bench OUT=docs/perf/task<N>.txt` then `task perf:diff BEFORE=docs/perf/baseline-2026-05-15-localhost.txt AFTER=docs/perf/task<N>.txt` — the result doesn't have to be committed (these `task*.txt` files are gitignored unless explicitly added) but is useful as a sanity check before moving on. Task 10 runs the load-bearing comparison.

---

## File Structure

**Proto changes:**
- Modify `api/proto/file.proto` — `Read` returns `stream ReadFrame`; `Write` takes `stream WriteFrame`; add new frame messages.
- Modify `api/proto/fs.proto` — add `Compound` RPC with `CompoundRequest` / `CompoundReply` lists.
- Regenerate `pkg/proto/*.pb.go` via `task gen:grpc`.

**Server changes (new + modified):**
- Modify `pkg/server/controller/file.go` — `Read` becomes streaming handler; `Write` becomes streaming handler; both keep the existing fd-table + idempotency wiring.
- Create `pkg/server/service/streaming.go` — `StreamingService` interface (`ReadInto(ctx, fd, off, buf) (int, status)` already exists in pathfs, but expose the frame-iteration logic here so the controller stays thin per the layering skill).
- Modify `pkg/server/controller/fs.go` — register Compound; new method dispatches each sub-op.
- Create `pkg/server/service/compound.go` — `CompoundService` runs an op-list, returning a parallel list of replies, with bounded fan-out.
- Modify `pkg/server/grpc/snappy/` — make the codec name explicit (`snappy`) and registered only on the streaming Read/Write call sites, not on the default `DefaultCallOptions`.
- Modify `pkg/server/grpc/server.go` — set `MaxRecvMsgSize`, `MaxSendMsgSize`, `KeepaliveParams`, `KeepaliveEnforcementPolicy`; remove the global Snappy default; honour `server.frame_size_bytes`.
- Modify `pkg/server/config/server.go` — add `FrameSizeBytes`, `Keepalive` (with `Time`, `Timeout`, `MinTime`, `PermitWithoutStream`), `MaxMessageBytes`.

**Client changes (new + modified):**
- Modify `pkg/client/io/file.go` — `Read` consumes a server-streaming response into the caller's buffer; `Write` opens a client-streaming RPC and sends frames. Retry semantics keep the same shape (one request_id for the full op, sent on the first frame for writes).
- Create `pkg/client/io/readahead.go` — `Readahead` struct tracks last-offset per fd, predicts sequential pattern, prefetches the next chunk into an in-memory ring.
- Create `pkg/client/io/coalesce.go` — `WriteCoalescer` per fd, buffers small contiguous writes up to a flush threshold, then sends one streaming Write.
- Modify `pkg/client/io/fs.go` — optional Compound path for `OpenDir`-with-stat (mark as a Phase 3.5 optimisation; flag-gated, default off until Phase 4 needs it).
- Modify `pkg/client/grpc/factory.go` — `DialOption` set: keepalive, max message sizes, Snappy per-call instead of global.
- Modify `pkg/client/mount/single.go` and `pkg/client/mount/vfs.go` — set `MountOptions.MaxRead`, `MaxWrite`, `MaxBackground`, `CongestionThreshold`, `EnableWritebackCache`.
- Modify `pkg/client/config/client.go` — add `ReadaheadChunkBytes`, `WriteCoalesceBytes`, `FUSE.MaxRead`, `FUSE.MaxWrite`, `FUSE.WritebackCache`, keepalive params.

**Test changes:**
- New unit test suites: `pkg/server/controller/file_streaming_test.go`, `pkg/server/service/compound_test.go`, `pkg/client/io/readahead_test.go`, `pkg/client/io/coalesce_test.go`.
- New e2e: `test/e2e/api/streaming_test.go` (4 GiB bidirectional copy, single mount), `test/e2e/api/compound_test.go` (Compound returns N replies in one RTT).
- Modify existing benchmarks in `test/e2e/fs/io_bench_test.go` only if needed to expose the new frame size knob; otherwise leave alone — they're the measurement instrument.

**Performance harness:**
- Create `test/e2e/perf/` — Go `BenchmarkXxx` functions in benchstat-compatible form (seq I/O, random I/O, metadata). Reused by Task 1 (baseline), every intermediate task that wants to spot-check progress, and Task 10 (final delta).
- Modify `Taskfile.yaml` — `perf:install`, `perf:bench`, `perf:diff` targets wrapping `go test -bench` and `benchstat`.
- Create `docs/perf/README.md` — workflow doc.

**Documentation:**
- Create `docs/perf/baseline-2026-05-15.*` (Task 1) and `docs/perf/phase3-final-2026-05-15.*` + `docs/perf/phase3-deltas-2026-05-15-*.txt` (Task 10).
- Modify `docs/server/config.md` and `docs/client/config.md` for the new knobs.

---

## Task 1: Build perf harness and capture baseline

**Files:**
- Create: `test/e2e/perf/perf_suite_test.go` — shared FUSE-mount setup helpers
- Create: `test/e2e/perf/seq_io_bench_test.go` — `BenchmarkSeqRead*`, `BenchmarkSeqWrite*`
- Create: `test/e2e/perf/random_io_bench_test.go` — `BenchmarkRandomRead4KiB`, `BenchmarkRandomWrite4KiB`
- Create: `test/e2e/perf/metadata_bench_test.go` — `BenchmarkOpenStatClose`, `BenchmarkReaddir100`
- Modify: `Taskfile.yaml` — add `perf:install`, `perf:bench`, `perf:diff` targets
- Create: `docs/perf/README.md` — workflow doc
- Create: `docs/perf/baseline-2026-05-15-localhost.txt` — raw bench output, benchstat-compatible
- Create: `docs/perf/baseline-2026-05-15-slow30ms.txt` — same, with 30 ms loopback latency
- Create: `docs/perf/baseline-2026-05-15.md` — environment summary + commit hash

**Why this is task 1:** The spec mandates a baseline before any perf work so each later task can report a delta. We use Go-standard `benchstat` (`golang.org/x/perf/cmd/benchstat`) which compares two runs with **statistical significance** (geomean + p-value) rather than eyeballing markdown tables — that catches the 5–10% regressions that hand-rolled tables miss. The same harness drives baseline capture here, optional progress checks during intermediate tasks, and the final comparison in Task 10.

**Where to run:** On the kubevirt VM (`192.168.11.11`). The sandbox can't open `/dev/fuse`.

**Design notes for the benchmarks:**
- Each `Benchmark*` builds its *own* server + FUSE mount via `test/e2e/utils` and tears it down on cleanup. Setup cost is amortised when `b.N` is large or `-benchtime=10s` is used.
- Throughput benchmarks call `b.SetBytes(n)` — `benchstat` then reports MB/s automatically.
- Metadata benchmarks rely on `b.N` round-trips → benchstat shows ns/op + ops/s.
- `b.ResetTimer()` is called after setup so the timer measures only the op, not the mount handshake.
- Existing `test/e2e/fs/io_bench_test.go` (fio-driven) is left untouched — it's useful as a smoke test but its output isn't structured. The new harness under `test/e2e/perf/` is the load-bearing one.

- [ ] **Step 1: Sync the repo to the VM**

Run on the host:
```bash
ssh ubuntu@192.168.11.11 'mkdir -p ~/gMountie'
rsync -av --delete --exclude '.git/' --exclude 'ui/frontend/node_modules' \
  ./ ubuntu@192.168.11.11:~/gMountie/
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task --list' | head -5
```

Expected: rsync succeeds, `task --list` prints targets.

- [ ] **Step 2: Install benchstat on the VM**

```bash
ssh ubuntu@192.168.11.11 'go install golang.org/x/perf/cmd/benchstat@latest'
ssh ubuntu@192.168.11.11 'ls -l $HOME/go/bin/benchstat && $HOME/go/bin/benchstat -h 2>&1 | head -3'
```

Expected: binary present, help text prints. If `~/go/bin` isn't on PATH inside `task`, the Taskfile targets below use the absolute path or rely on the shell's PATH including `$HOME/go/bin` (the VM's cloud-init does this).

- [ ] **Step 3: Write the shared bench fixture**

Create `test/e2e/perf/perf_suite_test.go`:

```go
package perf

import (
	"os"
	"path/filepath"
	"testing"

	"gmountie/test/e2e/utils"
)

// benchEnv holds the server + mount + temp dirs each benchmark needs.
type benchEnv struct {
	app        *utils.TestApp
	mountPoint string
	dataDir    string
}

func setupBenchEnv(b *testing.B) *benchEnv {
	b.Helper()
	tmp := b.TempDir()
	dataDir := filepath.Join(tmp, "data")
	mountPoint := filepath.Join(tmp, "mnt")
	for _, d := range []string{dataDir, mountPoint} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			b.Fatalf("mkdir %s: %v", d, err)
		}
	}
	app, err := utils.StartTestApp(utils.TestConfig{
		VolumePath: dataDir,
		MountPoint: mountPoint,
	})
	if err != nil {
		b.Fatalf("start test app: %v", err)
	}
	b.Cleanup(func() {
		if err := app.Stop(); err != nil {
			b.Logf("stop test app: %v", err)
		}
	})
	return &benchEnv{app: app, mountPoint: mountPoint, dataDir: dataDir}
}
```

(Adapt struct/field names to the real `test/e2e/utils` API — that package is the reference; the existing `test/e2e/api/*_test.go` files show the calling convention.)

- [ ] **Step 4: Write the sequential I/O benchmarks**

Create `test/e2e/perf/seq_io_bench_test.go`:

```go
package perf

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func benchSeqRead(b *testing.B, size int64) {
	env := setupBenchEnv(b)

	src := filepath.Join(env.dataDir, "seq.bin")
	f, err := os.Create(src)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	if _, err := io.CopyN(f, rand.Reader, size); err != nil {
		b.Fatalf("fill: %v", err)
	}
	f.Close()

	path := filepath.Join(env.mountPoint, "seq.bin")
	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rf, err := os.Open(path)
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		if _, err := io.Copy(io.Discard, rf); err != nil {
			b.Fatalf("read: %v", err)
		}
		rf.Close()
	}
}

func BenchmarkSeqRead1MiB(b *testing.B)  { benchSeqRead(b, 1<<20) }
func BenchmarkSeqRead16MiB(b *testing.B) { benchSeqRead(b, 16<<20) }
func BenchmarkSeqRead1GiB(b *testing.B)  { benchSeqRead(b, 1<<30) }

func benchSeqWrite(b *testing.B, size int64) {
	env := setupBenchEnv(b)

	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}

	b.SetBytes(size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := filepath.Join(env.mountPoint, "w.bin")
		if err := os.WriteFile(path, payload, 0o644); err != nil {
			b.Fatalf("write: %v", err)
		}
		os.Remove(path)
	}
}

func BenchmarkSeqWrite1MiB(b *testing.B)  { benchSeqWrite(b, 1<<20) }
func BenchmarkSeqWrite16MiB(b *testing.B) { benchSeqWrite(b, 16<<20) }
func BenchmarkSeqWrite1GiB(b *testing.B)  { benchSeqWrite(b, 1<<30) }
```

- [ ] **Step 5: Write the random I/O benchmarks**

Create `test/e2e/perf/random_io_bench_test.go`:

```go
package perf

import (
	"crypto/rand"
	"io"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkRandomRead4KiB(b *testing.B) {
	env := setupBenchEnv(b)

	const fileSize int64 = 256 << 20 // 256 MiB
	src := filepath.Join(env.dataDir, "rand.bin")
	f, err := os.Create(src)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	if _, err := io.CopyN(f, rand.Reader, fileSize); err != nil {
		b.Fatalf("fill: %v", err)
	}
	f.Close()

	rf, err := os.Open(filepath.Join(env.mountPoint, "rand.bin"))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer rf.Close()

	buf := make([]byte, 4096)
	r := mathrand.New(mathrand.NewSource(1))
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := r.Int63n(fileSize - 4096)
		if _, err := rf.ReadAt(buf, off); err != nil {
			b.Fatalf("readat off=%d: %v", off, err)
		}
	}
}

func BenchmarkRandomWrite4KiB(b *testing.B) {
	env := setupBenchEnv(b)

	const fileSize int64 = 256 << 20
	src := filepath.Join(env.dataDir, "rand.bin")
	f, err := os.Create(src)
	if err != nil {
		b.Fatalf("create: %v", err)
	}
	if err := f.Truncate(fileSize); err != nil {
		b.Fatalf("truncate: %v", err)
	}
	f.Close()

	wf, err := os.OpenFile(filepath.Join(env.mountPoint, "rand.bin"), os.O_RDWR, 0o644)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer wf.Close()

	buf := make([]byte, 4096)
	rand.Read(buf)
	r := mathrand.New(mathrand.NewSource(2))
	b.SetBytes(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := r.Int63n(fileSize - 4096)
		if _, err := wf.WriteAt(buf, off); err != nil {
			b.Fatalf("writeat: %v", err)
		}
	}
}
```

- [ ] **Step 6: Write the metadata benchmarks**

Create `test/e2e/perf/metadata_bench_test.go`:

```go
package perf

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func BenchmarkOpenStatClose(b *testing.B) {
	env := setupBenchEnv(b)

	path := filepath.Join(env.mountPoint, "m.bin")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fi, err := os.Stat(path)
		if err != nil || fi.Size() != 1 {
			b.Fatalf("stat: %v %v", fi, err)
		}
	}
}

func BenchmarkReaddir100(b *testing.B) {
	env := setupBenchEnv(b)

	for i := 0; i < 100; i++ {
		if err := os.WriteFile(filepath.Join(env.mountPoint, fmt.Sprintf("f%03d", i)), []byte{}, 0o644); err != nil {
			b.Fatalf("seed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries, err := os.ReadDir(env.mountPoint)
		if err != nil || len(entries) != 100 {
			b.Fatalf("readdir: %v %d", err, len(entries))
		}
	}
}

func BenchmarkLookup(b *testing.B) {
	env := setupBenchEnv(b)

	path := filepath.Join(env.mountPoint, "l.bin")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		b.Fatalf("seed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := os.Lstat(path); err != nil {
			b.Fatalf("lstat: %v", err)
		}
	}
}
```

- [ ] **Step 7: Add Taskfile targets**

Edit the root `Taskfile.yaml`. Add under the existing tasks:

```yaml
  perf:install:
    desc: Install benchstat
    cmds:
      - go install golang.org/x/perf/cmd/benchstat@latest

  perf:bench:
    desc: Run perf benchmarks. Vars - OUT (output file), COUNT (default 5), BENCHTIME (default 10s)
    vars:
      OUT: '{{.OUT | default (printf "docs/perf/bench-%s.txt" (now | date "2006-01-02T15-04-05"))}}'
      COUNT: '{{.COUNT | default "5"}}'
      BENCHTIME: '{{.BENCHTIME | default "10s"}}'
    cmds:
      - mkdir -p docs/perf
      - go test -run=^$ -bench=. -benchmem -count={{.COUNT}} -benchtime={{.BENCHTIME}} ./test/e2e/perf/ | tee {{.OUT}}
      - echo "wrote {{.OUT}}"

  perf:diff:
    desc: Compare two bench outputs with benchstat. Vars - BEFORE, AFTER
    cmds:
      - benchstat {{.BEFORE}} {{.AFTER}}
```

(`task` template syntax uses `{{.NAME}}`; defaults via `| default`. The `now | date` builtin generates the timestamp.)

- [ ] **Step 8: Smoke-run the harness**

On the VM:

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task perf:bench COUNT=1 BENCHTIME=1s OUT=/tmp/smoke.txt'
ssh ubuntu@192.168.11.11 'cat /tmp/smoke.txt'
```

Expected: header lines (`goos:`, `goarch:`, `pkg:`) followed by one `BenchmarkXxx-N ... B/op` line per benchmark. Every benchmark should appear at least once.

- [ ] **Step 9: Capture the baseline (localhost)**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task perf:bench OUT=docs/perf/baseline-2026-05-15-localhost.txt'
```

Runtime: ~30–60 minutes (default COUNT=5 × BENCHTIME=10s × ~10 benchmarks + setup overhead). Inspect the file when done — `benchstat docs/perf/baseline-2026-05-15-localhost.txt` should print a single-column table.

- [ ] **Step 10: Capture the baseline under 30 ms loopback latency**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && sudo bash scripts/start-slow-loopback.sh 30ms 0%'
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task perf:bench OUT=docs/perf/baseline-2026-05-15-slow30ms.txt COUNT=3 BENCHTIME=20s'
ssh ubuntu@192.168.11.11 'cd ~/gMountie && sudo bash scripts/stop-slow-loopback.sh'
```

(COUNT is dropped because each iteration takes longer; BENCHTIME is raised so the sample is statistically meaningful.)

- [ ] **Step 11: Pull outputs back to the host and document the environment**

```bash
mkdir -p docs/perf
scp ubuntu@192.168.11.11:~/gMountie/docs/perf/baseline-2026-05-15-*.txt docs/perf/
ssh ubuntu@192.168.11.11 'uname -a; nproc; free -h; go version; grep "model name" /proc/cpuinfo | head -1'
```

Create `docs/perf/README.md`:

```markdown
# Performance harness

Go benchmarks under `test/e2e/perf/` drive a real server + FUSE mount and
emit `benchstat`-compatible output. The harness only runs on FUSE-capable
hosts — use the kubevirt VM (see `testing/scratch/`).

## Workflow

```bash
# Baseline (before the change)
task perf:bench OUT=docs/perf/before.txt

# Make the change, then measure again
task perf:bench OUT=docs/perf/after.txt

# Compare
task perf:diff BEFORE=docs/perf/before.txt AFTER=docs/perf/after.txt
```

`benchstat` reports deltas with **statistical significance** — `~` means no
significant change at the configured confidence level. Numeric deltas show
geomean and p-value per benchmark.

## Variables

- `OUT=<path>` — output file (default: timestamped under `docs/perf/`)
- `COUNT=<n>` — repetitions per benchmark (default 5; lower for very slow runs)
- `BENCHTIME=<dur>` — per-iteration budget (default `10s`)
```

Create `docs/perf/baseline-2026-05-15.md`:

```markdown
# Phase 3 baseline — 2026-05-15

**Commit:** `<git rev-parse HEAD on develop>`
**Host:** kubevirt VM `192.168.11.11`
**Kernel:** <uname -a>
**CPUs:** <nproc> × <model name from /proc/cpuinfo>
**Memory:** <free -h>
**Go:** <go version>

## Files

- `baseline-2026-05-15-localhost.txt` — `task perf:bench` raw output
- `baseline-2026-05-15-slow30ms.txt` — same, with `tc netem 30ms` on loopback

## Pre-Phase-3 state (will change as the phase lands)

- Snappy applied globally (changes in Task 5).
- Read/Write are unary RPCs subject to the 4 MiB ceiling (changes in Tasks 2–3).
- FUSE `MaxRead`/`MaxWrite` at go-fuse defaults, 128 KiB (changes in Task 7).
- No client readahead or write coalescing.
```

- [ ] **Step 12: Commit**

```bash
git add test/e2e/perf/ Taskfile.yaml docs/perf/README.md \
        docs/perf/baseline-2026-05-15-localhost.txt \
        docs/perf/baseline-2026-05-15-slow30ms.txt \
        docs/perf/baseline-2026-05-15.md
git commit -m "$(cat <<'EOF'
test(perf): benchmark harness + Phase 3 baseline

Adds Go-bench-shaped harness under test/e2e/perf/ (seq, random,
metadata workloads), Taskfile targets wrapping go test -bench and
benchstat, and a docs/perf/ workflow doc. Captures pre-Phase-3
numbers on localhost and 30ms loopback for later delta reporting.
EOF
)"
```

---

## Task 2: Streaming Read RPC

**Files:**
- Modify: `api/proto/file.proto`
- Regenerate: `pkg/proto/file.pb.go`, `pkg/proto/file_grpc.pb.go` (via `task gen:grpc`)
- Modify: `pkg/server/controller/file.go` (Read handler)
- Create: `pkg/server/service/file_streaming.go` (ReadFrameStream helper)
- Modify: `pkg/server/config/server.go` (FrameSizeBytes field)
- Modify: `pkg/client/io/file.go` (Read consumer)
- Modify: `pkg/client/config/client.go` (ReadaheadChunkBytes for future use, just the field for now)
- Create: `pkg/server/controller/file_streaming_test.go`
- Modify: `test/e2e/api/file_test.go` (existing tests must still pass; add large-read coverage)
- Regenerate mocks if any service interface changed: `task gen:mocks`

**Design notes:**
- `ReadRequest` keeps its current shape but `size` becomes the *total* requested byte count (it already is — semantics unchanged).
- New `ReadFrame { bytes data = 1; int32 status = 2; }`. The server writes one frame per chunk; the final frame carries the terminal status (typically `OK` or an `errno`). On EOF mid-stream, the server closes the stream after the last data frame and the final status frame.
- Frame size comes from `ServerConfig.FrameSizeBytes` (default 1 MiB). Configurable so internet links with high BDP can be tuned.
- Snappy is *not* removed in this task — it stays globally registered until Task 5. Streaming through Snappy is fine for now.
- Read is naturally idempotent (no side effects), so no `request_id`. Retry on the client side is a single fresh streaming call.

- [ ] **Step 1: Write the failing unit test for streaming Read**

Create `pkg/server/controller/file_streaming_test.go`:

```go
package controller_test

import (
	"context"
	"io"
	"testing"

	"gmountie/internal/mocks"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/controller"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type StreamingReadSuite struct {
	suite.Suite
	// fixtures: AppContext, FileController, in-memory test volume with a known file
}

func (s *StreamingReadSuite) SetupTest() {
	// Build an AppContext with a tmpfs-backed volume.
	// Pre-write a 5 MiB known-pattern file to the volume.
	// Open it via the existing Open RPC to obtain an fd and session_id.
}

func (s *StreamingReadSuite) TestRead_DeliversFullPayloadInMultipleFrames() {
	// Given: file of 5 MiB, server FrameSizeBytes = 1 MiB
	// When: client sends Read(volume, fd, offset=0, size=5MiB)
	// Then: 5 frames returned, concatenated bytes equal the file content,
	//       the last frame carries status OK and no further frames arrive.
}

func (s *StreamingReadSuite) TestRead_EOFReturnsShortFinalFrame() {
	// Given: file of 1.5 MiB
	// When: client requests 5 MiB starting at offset 0
	// Then: first frame is 1 MiB, second frame is 0.5 MiB with status OK, stream closes.
}

func (s *StreamingReadSuite) TestRead_ReturnsErrnoOnBadFd() {
	// Given: fd unknown to session
	// When: Read called with bogus fd
	// Then: a single frame with non-OK status is sent, stream closes.
}

func TestStreamingReadSuite(t *testing.T) {
	suite.Run(t, new(StreamingReadSuite))
}
```

- [ ] **Step 2: Run the test, confirm it fails to compile**

```bash
go test -v -run TestStreamingReadSuite ./pkg/server/controller/
```

Expected: compile error — `Read` signature doesn't match streaming yet. Good.

- [ ] **Step 3: Update `api/proto/file.proto` to stream Read**

Apply this diff to `api/proto/file.proto`:

```diff
 message ReadRequest {
   string volume = 1;
   uint64 fd = 2;
   int64 offset = 3;
   uint32 size = 4;
   string session_id = 5;
 }

-message ReadReply {
-  bytes bytes = 1;
-  int64 size = 2;
-  int32 status = 3;
+message ReadFrame {
+  bytes data = 1;
+  int32 status = 2;
 }

 service RpcFile {
   rpc Open (OpenRequest) returns (OpenReply);
   rpc Create (CreateRequest) returns (CreateReply);
-  rpc Read (ReadRequest) returns (ReadReply);
+  rpc Read (ReadRequest) returns (stream ReadFrame);
```

- [ ] **Step 4: Regenerate Go stubs**

```bash
task gen:grpc
```

Expected: `pkg/proto/file.pb.go` and `pkg/proto/file_grpc.pb.go` updated. No mock regen needed yet (the controller is the implementor of the generated server interface, not something we mock).

- [ ] **Step 5: Add `FrameSizeBytes` to server config**

Edit `pkg/server/config/server.go`:

```go
const (
	DefaultAddress = "0.0.0.0"
	DefaultPort = 9449
	DefaultMetricsAddr = ":9090"
	DefaultFrameSizeBytes = 1 << 20 // 1 MiB
)

type ServerConfig struct {
	Address     string `validate:"required,ip"`
	Port        uint   `validate:"required"`
	Metrics     bool
	MetricsAddr string `validate:"hostname_port" mapstructure:"metrics_addr"`
	// FrameSizeBytes is the maximum payload size of a single Read/Write streaming frame.
	FrameSizeBytes int `validate:"min=4096,max=16777216" mapstructure:"frame_size_bytes"`
}
```

Wire the default in `pkg/server/config/load.go` (or wherever the server config defaults are populated; check the existing pattern for `DefaultMetricsAddr`).

- [ ] **Step 6: Create the streaming-read service helper**

Create `pkg/server/service/file_streaming.go`:

```go
package service

import (
	"context"
	"io"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
)

// ReadStreamer reads bytes from an open file in chunks of frameSize and
// invokes emit for each chunk. The final emit call carries the terminal
// fuse.Status (OK on clean EOF or full payload, non-OK on error).
// Returns the first transport error from emit, otherwise nil.
type ReadStreamer struct {
	frameSize int
}

func NewReadStreamer(frameSize int) *ReadStreamer {
	return &ReadStreamer{frameSize: frameSize}
}

// Stream issues ReadAt-style reads against the FUSE file and invokes
// emit for each frame. It assumes fileRead returns (n, status) following
// the go-fuse pathfs convention (status fuse.OK with n bytes; or fuse.EIO etc.).
func (s *ReadStreamer) Stream(
	ctx context.Context,
	totalSize int,
	startOffset int64,
	fileRead func(buf []byte, off int64) (n int, status fuse.Status),
	emit func(data []byte, status fuse.Status) error,
) error {
	remaining := totalSize
	off := startOffset
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return errors.Wrap(err, "read stream cancelled")
		}
		chunk := remaining
		if chunk > s.frameSize {
			chunk = s.frameSize
		}
		buf := make([]byte, chunk)
		n, st := fileRead(buf, off)
		if !st.Ok() {
			return emit(nil, st)
		}
		if n == 0 {
			// EOF
			return emit(nil, fuse.OK)
		}
		if err := emit(buf[:n], fuse.OK); err != nil {
			return err
		}
		off += int64(n)
		remaining -= n
		if n < chunk {
			// short read — treat as EOF, terminate cleanly
			return nil
		}
	}
	return nil
}
```

(Layering reminder: the controller will be a thin handler — request validation, fd lookup, delegate to `ReadStreamer.Stream`, return.)

- [ ] **Step 7: Update server controller `Read` to streaming**

Edit `pkg/server/controller/file.go`. Replace the unary `Read` method with a streaming handler. The exact signature comes from the regenerated `pkg/proto/file_grpc.pb.go` — it will look like `Read(*proto.ReadRequest, proto.RpcFile_ReadServer) error`.

Key responsibilities of the new handler:
1. Resolve session + volume + fd via the existing helpers used by the unary version.
2. Build a `*ReadStreamer` with `frameSize := c.cfg.FrameSizeBytes`.
3. Call `streamer.Stream(stream.Context(), int(req.Size), req.Offset, file.Read, func(data, status) { return stream.Send(&proto.ReadFrame{Data: data, Status: int32(status)}) })`.
4. Return the error from Stream wrapped via `errors.Wrap`.

Pseudo-shape:

```go
func (c *FileController) Read(req *proto.ReadRequest, stream proto.RpcFile_ReadServer) error {
	ctx := stream.Context()
	vol, err := c.app.Volumes.Get(req.GetVolume())
	if err != nil {
		return status.Errorf(codes.NotFound, "volume %q: %v", req.GetVolume(), err)
	}
	sess, err := c.app.Sessions.Get(req.GetSessionId())
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "session: %v", err)
	}
	file, ok := sess.GetFile(req.GetFd())
	if !ok {
		// match existing behaviour — single frame, non-OK
		return stream.Send(&proto.ReadFrame{Status: int32(fuse.EBADF)})
	}
	streamer := service.NewReadStreamer(c.cfg.FrameSizeBytes)
	return streamer.Stream(ctx, int(req.GetSize()), req.GetOffset(),
		func(buf []byte, off int64) (int, fuse.Status) {
			res, st := file.Read(buf, off)
			if !st.Ok() {
				return 0, st
			}
			n, err := res.Bytes(buf)
			if err != nil {
				return 0, fuse.EIO
			}
			return len(n), fuse.OK
		},
		func(data []byte, st fuse.Status) error {
			return stream.Send(&proto.ReadFrame{Data: data, Status: int32(st)})
		},
	)
}
```

(The exact shape of `file.Read` is the existing pathfs ReadResult API used in the current unary handler — copy from the previous implementation.)

- [ ] **Step 8: Update client `Read` to consume the streaming response**

Edit `pkg/client/io/file.go`. The current unary call site becomes:

```go
func (f *GrpcFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.IOTimeout)
	defer cancel()

	req := &proto.ReadRequest{
		Volume:    f.volume,
		Fd:        f.fd,
		Offset:    off,
		Size:      uint32(len(dest)),
		SessionId: f.sessionID(),
	}

	var stream proto.RpcFile_ReadClient
	op := func() error {
		var err error
		stream, err = f.client.Read(ctx, req)
		if err != nil {
			return err
		}
		// Reset buffer pointer per attempt.
		written := 0
		var finalStatus fuse.Status = fuse.OK
		for {
			frame, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return recvErr
			}
			if st := fuse.Status(frame.GetStatus()); !st.Ok() {
				finalStatus = st
				break
			}
			data := frame.GetData()
			if len(data) == 0 {
				continue
			}
			if written+len(data) > len(dest) {
				return errors.New("server sent more bytes than requested")
			}
			copy(dest[written:], data)
			written += len(data)
		}
		f.lastReadResult = readResult{n: written, status: finalStatus}
		return nil
	}

	if err := f.retry(ctx, op); err != nil {
		// classify err into a fuse.Status as the existing unary code does
		return nil, classifyReadErr(err)
	}
	return fuse.ReadResultData(dest[:f.lastReadResult.n]), f.lastReadResult.status
}
```

Adapt to the actual struct fields / retry helper that already exist on `GrpcFile` — the shape above is the contract, not a literal patch. The key behavioural requirements are: (a) each retry attempt opens a fresh stream, (b) bytes are accumulated into the caller-supplied buffer in order, (c) the terminal status from the server is propagated.

- [ ] **Step 9: Run the unit tests; iterate until green**

```bash
go test -v -run TestStreamingReadSuite ./pkg/server/controller/
```

Expected: all three suite methods pass.

- [ ] **Step 10: Add an e2e test for large reads on the VM**

Create `test/e2e/api/streaming_test.go` (or extend the closest existing file):

```go
type StreamingReadE2ESuite struct {
	suite.Suite
	app *utils.TestApp
	// ...
}

func (s *StreamingReadE2ESuite) TestRead16MiB() {
	// Write a 16 MiB known-pattern file directly to the server-side data dir.
	// Mount the volume via gMountie.
	// Read the file back via the mount, verify byte-for-byte equality.
}

func (s *StreamingReadE2ESuite) TestRead1GiB_DoesNotHitMessageSizeLimit() {
	// Write 1 GiB random bytes (sha256 known), read back, verify hash.
	// Failure mode being guarded against: pre-Phase-3 this would have
	// failed with ResourceExhausted at the 4 MiB grpc default cap.
}
```

Run on the VM (FUSE required):

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 10m -v -run TestStreamingReadE2ESuite ./test/e2e/api/'
```

Expected: green.

- [ ] **Step 11: Run the full test suite to catch regressions**

On the VM:

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task test'
```

Expected: green (or only pre-existing flakes — record those in the commit body if any).

- [ ] **Step 12: Commit**

```bash
git add api/proto/file.proto pkg/proto/file.pb.go pkg/proto/file_grpc.pb.go \
        pkg/server/controller/file.go pkg/server/service/file_streaming.go \
        pkg/server/controller/file_streaming_test.go pkg/server/config/server.go \
        pkg/client/io/file.go test/e2e/api/streaming_test.go
git commit -m "$(cat <<'EOF'
feat(proto/file): convert Read to server-streaming

Lifts the 4 MiB unary ceiling on reads. ReadFrame replaces ReadReply;
server emits one frame per FrameSizeBytes chunk; client accumulates
into the caller buffer. Read is naturally idempotent so retry remains
a fresh stream (no request_id needed).
EOF
)"
```

---

## Task 3: Streaming Write RPC

**Files:**
- Modify: `api/proto/file.proto`
- Regenerate: `pkg/proto/file.pb.go`, `pkg/proto/file_grpc.pb.go`
- Modify: `pkg/server/controller/file.go` (Write handler — streaming receive)
- Create: `pkg/server/service/file_streaming_write.go` (WriteFrameDrain helper)
- Modify: `pkg/client/io/file.go` (Write becomes a streaming send)
- Modify: `pkg/server/service/session.go` — if the `DoOnce` LRU key currently composes `session_id+request_id` and stores `(int, fuse.Status)` for unary Write, keep the same shape. (Phase 1d already did this; just verify the existing wiring still applies after the proto change.)
- Create: `pkg/server/controller/file_streaming_write_test.go`
- Modify: `test/e2e/api/streaming_test.go` (add Write coverage)

**Design notes:**
- New message `WriteFrame { string volume=1; uint64 fd=2; string session_id=3; string request_id=4; int64 offset=5; bytes data=6; }`. The *first* frame carries volume/fd/session_id/request_id/offset; subsequent frames carry only `data` (zero-valued header fields are ignored — the server pins them from frame 1 and rejects mismatches as `InvalidArgument`).
- `WriteReply` is unary: `{ uint32 written=1; int32 status=2; }`. Sent once, after the stream closes.
- Idempotency: server consults `(session_id, request_id)` in the existing per-session LRU *after* fully draining the first frame. If a cached reply exists, server returns it without re-applying.
- Retry: client sends the entire stream again with the same `request_id`. Server idempotency cache returns the cached reply.
- Max in-flight write size for a single op is bounded by `MaxRecvMsgSize × (frames received before completion)`. There is no aggregate cap — large writes naturally chunk.

- [ ] **Step 1: Write the failing unit test**

Create `pkg/server/controller/file_streaming_write_test.go` with a suite covering:

```go
func (s *StreamingWriteSuite) TestWrite_SingleFrameAppendsAndReturnsByteCount()
func (s *StreamingWriteSuite) TestWrite_MultipleFramesContiguousOffsetsAppend()
func (s *StreamingWriteSuite) TestWrite_DuplicateRequestIDReturnsCachedReply()
func (s *StreamingWriteSuite) TestWrite_FrameMissingFirstHeaderFails()
```

(Use the in-process server bootstrap pattern from `Task 2 Step 1`.)

- [ ] **Step 2: Run the test, confirm compile failure**

```bash
go test -v -run TestStreamingWriteSuite ./pkg/server/controller/
```

Expected: compile failure (Write signature mismatch).

- [ ] **Step 3: Update the proto**

Diff to `api/proto/file.proto`:

```diff
-message WriteRequest {
-  string volume = 1;
-  uint64 fd = 2;
-  bytes bytes = 3;
-  int64 offset = 4;
-  string session_id = 5;
-  string request_id = 6;
-}
+message WriteFrame {
+  // First frame must set volume, fd, session_id, request_id, offset.
+  // Subsequent frames need only data; non-data fields are ignored if set.
+  string volume = 1;
+  uint64 fd = 2;
+  string session_id = 3;
+  string request_id = 4;
+  int64 offset = 5;
+  bytes data = 6;
+}

 message WriteReply {
   uint32 written = 1;
   int32 status = 2;
 }

 service RpcFile {
   rpc Open (OpenRequest) returns (OpenReply);
   rpc Create (CreateRequest) returns (CreateReply);
   rpc Read (ReadRequest) returns (stream ReadFrame);
-  rpc Write (WriteRequest) returns (WriteReply);
+  rpc Write (stream WriteFrame) returns (WriteReply);
```

- [ ] **Step 4: Regenerate stubs**

```bash
task gen:grpc
```

- [ ] **Step 5: Implement the server streaming Write handler**

Edit `pkg/server/controller/file.go`. The new generated signature is `Write(proto.RpcFile_WriteServer) error`. Handler responsibilities:

1. Receive frame 1.
2. Validate session, volume, fd, request_id (all required).
3. Check `(session_id, request_id)` in the idempotency LRU. If a hit, drain the stream (until EOF) without applying, then send the cached reply.
4. Otherwise: write frame-1's data at frame-1's offset, then loop on `stream.Recv()`, writing each frame's data at `cursorOffset` (advancing by `len(data)` each frame).
5. On EOF, build a `WriteReply{ Written: totalWritten, Status: int32(finalStatus) }`, store in LRU, send.
6. On any error mid-stream, abort the stream with a non-OK status return (the gRPC layer will propagate it).

Use a small helper in `pkg/server/service/file_streaming_write.go`:

```go
package service

import (
	"github.com/hanwen/go-fuse/v2/fuse"
)

// WriteFrameSink writes incoming frames sequentially to an open file.
// Total returns the cumulative bytes written; FirstStatusError returns the
// first non-OK status encountered (if any), short-circuiting further writes.
type WriteFrameSink struct {
	file   FuseFileWriter // narrow interface: Write([]byte, int64) (uint32, fuse.Status)
	offset int64
	total  uint32
}

type FuseFileWriter interface {
	Write(data []byte, off int64) (uint32, fuse.Status)
}

func NewWriteFrameSink(f FuseFileWriter, startOffset int64) *WriteFrameSink {
	return &WriteFrameSink{file: f, offset: startOffset}
}

func (w *WriteFrameSink) WriteFrame(data []byte) (uint32, fuse.Status) {
	if len(data) == 0 {
		return 0, fuse.OK
	}
	n, st := w.file.Write(data, w.offset)
	if !st.Ok() {
		return n, st
	}
	w.offset += int64(n)
	w.total += n
	return n, fuse.OK
}

func (w *WriteFrameSink) Total() uint32 { return w.total }
```

(The narrow `FuseFileWriter` interface makes the sink easily unit-testable.)

- [ ] **Step 6: Implement the client streaming Write**

Edit `pkg/client/io/file.go`. Replace the unary Write call site with:

```go
func (f *GrpcFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	ctx, cancel := context.WithTimeout(context.Background(), f.cfg.IOTimeout)
	defer cancel()

	requestID := uuid.NewString()
	op := func() error {
		stream, err := f.client.Write(ctx)
		if err != nil {
			return err
		}
		// First frame — full header + first chunk.
		frameSize := f.cfg.FrameSizeBytes
		if frameSize <= 0 {
			frameSize = 1 << 20
		}
		cursor := 0
		first := true
		for cursor < len(data) {
			chunk := frameSize
			if cursor+chunk > len(data) {
				chunk = len(data) - cursor
			}
			frame := &proto.WriteFrame{Data: data[cursor : cursor+chunk]}
			if first {
				frame.Volume = f.volume
				frame.Fd = f.fd
				frame.SessionId = f.sessionID()
				frame.RequestId = requestID
				frame.Offset = off
				first = false
			}
			if err := stream.Send(frame); err != nil {
				return err
			}
			cursor += chunk
		}
		reply, err := stream.CloseAndRecv()
		if err != nil {
			return err
		}
		f.lastWriteResult = writeResult{n: reply.GetWritten(), status: fuse.Status(reply.GetStatus())}
		return nil
	}

	if err := f.retry(ctx, op); err != nil {
		return 0, classifyWriteErr(err)
	}
	return f.lastWriteResult.n, f.lastWriteResult.status
}
```

(The `requestID` is generated *outside* the retry closure — Phase 1d invariant. Each retry attempt re-sends the same id so the server's idempotency cache can short-circuit.)

- [ ] **Step 7: Run the unit tests**

```bash
go test -v -run TestStreamingWriteSuite ./pkg/server/controller/
```

Expected: all four suite methods pass.

- [ ] **Step 8: Add e2e write coverage**

Extend `test/e2e/api/streaming_test.go`:

```go
func (s *StreamingWriteE2ESuite) TestWrite16MiB() { /* known-pattern, verify on disk */ }
func (s *StreamingWriteE2ESuite) TestBidirectional4GiB() {
	// Write 4 GiB random (sha256 known), read 4 GiB back, verify hash.
	// This is the spec's DoD test for Phase 3.
}
```

Run on the VM:

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 30m -v -run TestStreamingWriteE2ESuite ./test/e2e/api/'
```

Expected: green. The 4 GiB test is slow — that's normal.

- [ ] **Step 9: Full suite**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task test'
```

Expected: green.

- [ ] **Step 10: Commit**

```bash
git add api/proto/file.proto pkg/proto/file.pb.go pkg/proto/file_grpc.pb.go \
        pkg/server/controller/file.go pkg/server/service/file_streaming_write.go \
        pkg/server/controller/file_streaming_write_test.go pkg/client/io/file.go \
        test/e2e/api/streaming_test.go
git commit -m "$(cat <<'EOF'
feat(proto/file): convert Write to client-streaming

Lifts the 4 MiB unary ceiling on writes. First frame carries the
session_id/request_id/fd/offset header; subsequent frames carry only
data. Idempotency cache is consulted once frame 1 is parsed so retries
of the entire stream are safe.
EOF
)"
```

---

## Task 4: Compound metadata RPC

**Files:**
- Modify: `api/proto/fs.proto`
- Regenerate: `pkg/proto/fs.pb.go`, `pkg/proto/fs_grpc.pb.go`
- Modify: `pkg/server/controller/fs.go` (register Compound)
- Create: `pkg/server/service/compound.go` + `compound_test.go`
- Modify: `pkg/server/controller/fs_test.go` (or wherever the FsController tests live — add Compound coverage)
- Create: `pkg/client/io/compound.go` (helper for batched metadata sends; intentionally unused by default — `pkg/client/io/fs.go` is **not** changed in this task)
- Add: e2e `test/e2e/api/compound_test.go`

**Design notes:**
- The shape mirrors NFSv4 Compound at a small scale: one outer RPC carries `repeated CompoundOp ops`; the server returns `repeated CompoundReply replies` in the same order. If any op fails fatally (transport-class error), the server may return fewer replies than ops were sent — the spec wording is "returns a list of replies"; we choose **best-effort: server always returns N replies, errors are per-op in the reply**.
- We only batch read-only metadata ops in Phase 3 (`GetAttr`, `OpenDir`, `Access`, `StatFs`, `GetXAttr`). Mutating ops are out of scope (idempotency interactions, deferred to a later need).
- The proto uses a `oneof` to express the per-op payload, so it's strongly typed at the wire.

- [ ] **Step 1: Write the failing service test**

Create `pkg/server/service/compound_test.go`:

```go
package service_test

type CompoundSuite struct {
	suite.Suite
	// fixtures: in-memory volume with 3 files at known paths
}

func (s *CompoundSuite) TestDispatch_GetAttr_OpenDir_GetAttr_ReturnsThreeReplies() {
	// Given an op list with [GetAttr(/a.txt), OpenDir(/), GetAttr(/b.txt)]
	// When the service dispatches the list
	// Then 3 replies come back, in order, each with the expected payload
}

func (s *CompoundSuite) TestDispatch_PerOpErrorDoesNotAbortBatch() {
	// Given [GetAttr(/missing), GetAttr(/exists)]
	// When dispatched
	// Then reply[0].Status is ENOENT, reply[1] succeeds
}

func (s *CompoundSuite) TestDispatch_RespectsParallelismCap() {
	// Given 100 GetAttr ops and a service configured with maxParallel=4
	// Spy on the underlying handler to count concurrent in-flight ops; assert <= 4
}
```

- [ ] **Step 2: Update `api/proto/fs.proto`**

```diff
+message CompoundOp {
+  oneof op {
+    GetAttrRequest get_attr = 1;
+    StatFsRequest stat_fs = 2;
+    OpenDirRequest open_dir = 3;
+    AccessRequest access = 4;
+    GetXAttrRequest get_xattr = 5;
+  }
+}
+
+message CompoundReply {
+  oneof reply {
+    GetAttrReply get_attr = 1;
+    StatFsReply stat_fs = 2;
+    OpenDirReply open_dir = 3;
+    AccessReply access = 4;
+    GetXAttrReply get_xattr = 5;
+    int32 status = 6;  // set if a per-op error prevented producing a typed reply
+  }
+}
+
+message CompoundRequest {
+  repeated CompoundOp ops = 1;
+}
+
+message CompoundBatch {
+  repeated CompoundReply replies = 1;
+}
+
 service RpcFs {
   ...
   rpc GetXAttr (GetXAttrRequest) returns (GetXAttrReply);
+  rpc Compound (CompoundRequest) returns (CompoundBatch);
 }
```

Regenerate: `task gen:grpc`.

- [ ] **Step 3: Implement the service**

Create `pkg/server/service/compound.go`:

```go
package service

import (
	"context"
	"sync"

	"gmountie/pkg/proto"
)

type CompoundDispatcher struct {
	fs          FsHandlers   // narrow interface — exposes the per-op handler methods
	maxParallel int
}

// FsHandlers is the minimal set of operations Compound dispatches to.
// Implemented by the existing FsController.
type FsHandlers interface {
	GetAttr(context.Context, *proto.GetAttrRequest) (*proto.GetAttrReply, error)
	StatFs(context.Context, *proto.StatFsRequest) (*proto.StatFsReply, error)
	OpenDir(context.Context, *proto.OpenDirRequest) (*proto.OpenDirReply, error)
	Access(context.Context, *proto.AccessRequest) (*proto.AccessReply, error)
	GetXAttr(context.Context, *proto.GetXAttrRequest) (*proto.GetXAttrReply, error)
}

func NewCompoundDispatcher(fs FsHandlers, maxParallel int) *CompoundDispatcher {
	if maxParallel <= 0 {
		maxParallel = 8
	}
	return &CompoundDispatcher{fs: fs, maxParallel: maxParallel}
}

func (d *CompoundDispatcher) Dispatch(ctx context.Context, ops []*proto.CompoundOp) []*proto.CompoundReply {
	replies := make([]*proto.CompoundReply, len(ops))
	sem := make(chan struct{}, d.maxParallel)
	var wg sync.WaitGroup
	for i, op := range ops {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, op *proto.CompoundOp) {
			defer wg.Done()
			defer func() { <-sem }()
			replies[i] = d.dispatchOne(ctx, op)
		}(i, op)
	}
	wg.Wait()
	return replies
}

func (d *CompoundDispatcher) dispatchOne(ctx context.Context, op *proto.CompoundOp) *proto.CompoundReply {
	switch v := op.Op.(type) {
	case *proto.CompoundOp_GetAttr:
		reply, err := d.fs.GetAttr(ctx, v.GetAttr)
		if err != nil {
			return &proto.CompoundReply{Reply: &proto.CompoundReply_Status{Status: int32(grpcErrToFuseStatus(err))}}
		}
		return &proto.CompoundReply{Reply: &proto.CompoundReply_GetAttr{GetAttr: reply}}
	// ... mirror for StatFs, OpenDir, Access, GetXAttr
	default:
		return &proto.CompoundReply{Reply: &proto.CompoundReply_Status{Status: int32(fuse.EINVAL)}}
	}
}
```

(`grpcErrToFuseStatus` is a small mapper — keep it in `pkg/server/service/compound.go` as an unexported helper.)

- [ ] **Step 4: Register Compound on the controller**

Edit `pkg/server/controller/fs.go` to add a `Compound` method that delegates to `CompoundDispatcher.Dispatch`. The controller stays thin — it wires the dispatcher (built once at AppContext init) into the gRPC handler signature.

```go
func (c *FsController) Compound(ctx context.Context, req *proto.CompoundRequest) (*proto.CompoundBatch, error) {
	replies := c.compound.Dispatch(ctx, req.GetOps())
	return &proto.CompoundBatch{Replies: replies}, nil
}
```

Wire the dispatcher in `pkg/server/app.go` (or wherever `FsController` is constructed). `maxParallel` comes from server config (`ServerConfig.CompoundMaxParallel int`, default 8).

- [ ] **Step 5: Run service tests**

```bash
go test -v -run TestCompoundSuite ./pkg/server/service/
```

Expected: green.

- [ ] **Step 6: Add an e2e test for round-trip count**

Create `test/e2e/api/compound_test.go`:

```go
func (s *CompoundE2ESuite) Test100GetAttrInOneRTT() {
	// Pre-create 100 files in the volume.
	// Build CompoundRequest with 100 GetAttrRequest ops.
	// Use the *raw gRPC client* (not the mount) to send Compound directly.
	// Assert: 100 replies returned, all OK, all attributes populated.
	// Measurement: wrap the call in time.Now()/time.Since and assert < 50ms on localhost
	// (latency-sensitive — adjust threshold based on baseline doc if necessary).
}
```

Run on the VM:

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 5m -v -run TestCompoundE2ESuite ./test/e2e/api/'
```

Expected: green.

- [ ] **Step 7: Add a client helper (not yet wired into fs.go)**

Create `pkg/client/io/compound.go`:

```go
package io

import (
	"context"

	"gmountie/pkg/proto"
)

// CompoundBatcher is a building block used by future readdir-with-stat
// optimisations. It accumulates ops, then issues a single Compound RPC.
// Phase 3 only ships the helper; Phase 4 (cache) decides where to wire it in.
type CompoundBatcher struct {
	client proto.RpcFsClient
	ops    []*proto.CompoundOp
}

func NewCompoundBatcher(c proto.RpcFsClient) *CompoundBatcher {
	return &CompoundBatcher{client: c}
}

func (b *CompoundBatcher) AddGetAttr(volume, path string) { /* ... */ }
func (b *CompoundBatcher) AddOpenDir(volume, path string) { /* ... */ }

func (b *CompoundBatcher) Send(ctx context.Context) ([]*proto.CompoundReply, error) {
	reply, err := b.client.Compound(ctx, &proto.CompoundRequest{Ops: b.ops})
	if err != nil {
		return nil, err
	}
	b.ops = b.ops[:0]
	return reply.GetReplies(), nil
}
```

- [ ] **Step 8: Full suite**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task test'
```

Expected: green.

- [ ] **Step 9: Commit**

```bash
git add api/proto/fs.proto pkg/proto/fs.pb.go pkg/proto/fs_grpc.pb.go \
        pkg/server/controller/fs.go pkg/server/service/compound.go \
        pkg/server/service/compound_test.go pkg/server/config/server.go \
        pkg/server/app.go pkg/client/io/compound.go test/e2e/api/compound_test.go
git commit -m "$(cat <<'EOF'
feat(proto/fs): add Compound RPC for batched metadata ops

NFSv4-style: one RPC carries an op list (GetAttr/StatFs/OpenDir/Access/
GetXAttr), server returns N replies in order. Per-op errors don't abort
the batch. Client helper is shipped but not yet wired into the FUSE
handlers — Phase 4 cache will adopt it for readdir-with-stat.
EOF
)"
```

---

## Task 5: Per-call compression policy

**Files:**
- Modify: `pkg/server/grpc/server.go` — remove the global Snappy default codec.
- Modify: `pkg/client/grpc/factory.go` — remove the global Snappy default; apply Snappy only on Read/Write streaming calls.
- Modify: `pkg/server/grpc/snappy/` — confirm the codec is registered under `"snappy"` name and selectable via `grpc.UseCompressor("snappy")` per call.
- Add: `pkg/client/io/file.go` — `grpc.UseCompressor("snappy")` `CallOption` on the Read/Write streaming client invocations.
- Add: a brief decision note `docs/perf/compression-2026-05-15.md` recording the Snappy-vs-zstd experiment result.

**Why:** Metadata RPCs (GetAttr, OpenDir, Access, ...) don't compress meaningfully but pay CPU. Restrict compression to bulk payload streams. Then run a one-off experiment comparing Snappy vs zstd level-1; record which is kept.

- [ ] **Step 1: Make Snappy opt-in per call**

Find the current registration site for Snappy. In `pkg/server/grpc/server.go` and `pkg/client/grpc/factory.go`, look for `grpc.RPCCompressor` / `grpc.UseCompressor` / `encoding.RegisterCompressor` calls. Ensure:

- `encoding.RegisterCompressor(snappy.New())` runs at package init (this can stay global — it just registers a name).
- Remove any `grpc.DefaultCallOptions(grpc.UseCompressor("snappy"))` on the dial / server options. Compression must now be opt-in per call.

- [ ] **Step 2: Add `grpc.UseCompressor("snappy")` to Read + Write client calls only**

In `pkg/client/io/file.go`, the streaming Read and Write calls pass an extra `CallOption`:

```go
stream, err := f.client.Read(ctx, req, grpc.UseCompressor("snappy"))
// ...
stream, err := f.client.Write(ctx, grpc.UseCompressor("snappy"))
```

Other client RPCs do not pass `UseCompressor` — those calls flow uncompressed.

- [ ] **Step 3: Update test expectations**

If any existing test inspects the response compression header, update it. Run:

```bash
go test ./pkg/server/grpc/... ./pkg/client/grpc/...
```

Expected: green.

- [ ] **Step 4: Run the benchmark to validate the policy**

On the VM:

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 30m -v -run TestIoBench ./test/e2e/fs/' | tee /tmp/bench-task5.log
```

Compare metadata-op latency vs `docs/perf/baseline-2026-05-15.md`. Expect a 10–30% latency improvement on `OpenDir` and `GetAttr` heavy workloads, neutral on bulk reads.

- [ ] **Step 5: Run the zstd experiment**

Add a temporary feature flag (`GMOUNTIE_EXPERIMENT_ZSTD=1`) gated in `pkg/client/io/file.go` so the streaming Read/Write switch from `snappy` to `zstd` (level 1). Run the bulk-read benchmark with and without and record:

- Throughput (MB/s) at each setting.
- CPU% on both client and server (use `top -b -n 5 -p $(pidof gMountie)` during a sustained read).
- Compression ratio if available (peek at `payload bytes sent` from a tcpdump if needed; otherwise rely on observed bandwidth).

Write the result to `docs/perf/compression-2026-05-15.md`:

```markdown
# Compression decision — 2026-05-15

## Test workload
- 1 GiB sequential read over 30 ms loopback latency
- Random-looking content (cat /dev/urandom > /data/test.bin)

## Snappy (level fixed)
- Throughput: ...
- Client CPU: ...
- Server CPU: ...

## zstd level 1
- Throughput: ...
- Client CPU: ...
- Server CPU: ...

## Decision
Keep Snappy / Switch to zstd. Reasoning: ...
```

If zstd wins decisively, integrate it (register `grpc.UseCompressor("zstd")` via `github.com/mostynb/go-grpc-compression/zstd` or write a thin wrapper analogous to `pkg/server/grpc/snappy/`). If not, document the result and remove the temporary flag.

- [ ] **Step 6: Remove the experiment flag**

Whichever way the decision goes, remove `GMOUNTIE_EXPERIMENT_ZSTD` so the code is back to a single chosen codec.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/grpc/server.go pkg/client/grpc/factory.go pkg/client/io/file.go \
        docs/perf/compression-2026-05-15.md
git commit -m "$(cat <<'EOF'
feat(grpc): apply compression only to Read/Write streaming RPCs

Metadata RPCs no longer pay Snappy's CPU cost. Compression is opt-in
per call via grpc.UseCompressor. zstd was evaluated but not adopted
(decision logged under docs/perf/).
EOF
)"
```

(Adjust the commit body to match the decision actually made.)

---

## Task 6: gRPC keepalive + message size tuning

**Files:**
- Modify: `pkg/server/grpc/server.go` — `keepalive.ServerParameters` + `keepalive.EnforcementPolicy`, `MaxRecvMsgSize`, `MaxSendMsgSize`.
- Modify: `pkg/client/grpc/factory.go` — `keepalive.ClientParameters`, matching message sizes.
- Modify: `pkg/server/config/server.go` and `pkg/client/config/client.go` — config knobs.
- Modify: `docs/server/config.md` and `docs/client/config.md`.

**Defaults:**
- Server `KeepaliveParams{ Time: 30s, Timeout: 10s, MaxConnectionIdle: 0, MaxConnectionAge: 0 }`
- Server `KeepaliveEnforcementPolicy{ MinTime: 10s, PermitWithoutStream: true }`
- Client `keepalive.ClientParameters{ Time: 30s, Timeout: 10s, PermitWithoutStream: true }`
- `MaxRecvMsgSize = MaxSendMsgSize = 16 MiB` (well above any single streaming frame; mostly a safety cap)

- [ ] **Step 1: Add config fields**

`pkg/server/config/server.go`:

```go
type ServerConfig struct {
	// ... existing ...
	MaxMessageBytes int                  `validate:"min=65536,max=67108864" mapstructure:"max_message_bytes"`
	Keepalive       ServerKeepaliveConfig `mapstructure:"keepalive"`
}

type ServerKeepaliveConfig struct {
	Time                time.Duration `mapstructure:"time"`
	Timeout             time.Duration `mapstructure:"timeout"`
	MinTime             time.Duration `mapstructure:"min_time"`
	PermitWithoutStream bool          `mapstructure:"permit_without_stream"`
}
```

Defaults: `MaxMessageBytes=16<<20`, `Keepalive.Time=30*time.Second`, `Timeout=10*time.Second`, `MinTime=10*time.Second`, `PermitWithoutStream=true`.

Mirror in `pkg/client/config/client.go` (drop `MinTime`; add nothing else — `ClientParameters` is a subset).

- [ ] **Step 2: Wire into server**

`pkg/server/grpc/server.go` server option list:

```go
opts := []grpc.ServerOption{
	grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    cfg.Keepalive.Time,
		Timeout: cfg.Keepalive.Timeout,
	}),
	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             cfg.Keepalive.MinTime,
		PermitWithoutStream: cfg.Keepalive.PermitWithoutStream,
	}),
	grpc.MaxRecvMsgSize(cfg.MaxMessageBytes),
	grpc.MaxSendMsgSize(cfg.MaxMessageBytes),
	// ... existing interceptors etc ...
}
```

- [ ] **Step 3: Wire into client**

`pkg/client/grpc/factory.go`:

```go
dialOpts := []grpc.DialOption{
	grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                cfg.Keepalive.Time,
		Timeout:             cfg.Keepalive.Timeout,
		PermitWithoutStream: cfg.Keepalive.PermitWithoutStream,
	}),
	grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(cfg.MaxMessageBytes),
		grpc.MaxCallSendMsgSize(cfg.MaxMessageBytes),
	),
	// ... existing creds, interceptors ...
}
```

- [ ] **Step 4: Write a test that verifies dead-connection detection**

Add a suite method to `pkg/client/grpc/factory_test.go` (create if not present):

```go
func (s *FactorySuite) TestKeepalive_DetectsDeadConnectionWithinTimeoutBudget() {
	// Start a bufconn server with our server-side keepalive params.
	// Dial with the client factory.
	// Drop the underlying conn via the bufconn pipe.
	// Issue a call; expect Unavailable within ~ Time + Timeout = 40s.
	// (Use t.Parallel() and a custom shorter Time for the test, e.g. 200ms/100ms.)
}
```

- [ ] **Step 5: Run tests**

```bash
go test ./pkg/server/grpc/... ./pkg/client/grpc/...
```

Expected: green.

- [ ] **Step 6: Update docs**

`docs/server/config.md` and `docs/client/config.md`: document the new `keepalive.*` and `max_message_bytes` keys with the defaults above.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/grpc/server.go pkg/client/grpc/factory.go \
        pkg/server/config/server.go pkg/client/config/client.go \
        pkg/client/grpc/factory_test.go docs/server/config.md docs/client/config.md
git commit -m "$(cat <<'EOF'
feat(grpc): configurable keepalive and max message size

Server pings idle conns every 30s; client matches. Dead connections
now surface as Unavailable within 40s instead of hanging, hooking into
the Phase 1 retry path. MaxMessageBytes raised to 16 MiB as a safety
cap above the streaming frame size.
EOF
)"
```

---

## Task 7: FUSE mount-option tuning

**Files:**
- Modify: `pkg/client/mount/single.go` — set `MountOptions`.
- Modify: `pkg/client/mount/vfs.go` — same.
- Modify: `pkg/client/config/client.go` — add `FUSE` config block.
- Modify: `docs/client/config.md`.

**Defaults to start with:**
- `MaxRead = 1 MiB` (matches server `FrameSizeBytes`)
- `MaxWrite = 1 MiB`
- `MaxBackground = 64`
- `CongestionThreshold = 48`
- `EnableWritebackCache = false` (default off — Phase 4 will revisit; turning it on opens a consistency window we don't want without the cache layer below it)

- [ ] **Step 1: Add the config block**

`pkg/client/config/client.go`:

```go
type FUSEConfig struct {
	MaxReadBytes        int  `validate:"min=4096,max=16777216" mapstructure:"max_read_bytes"`
	MaxWriteBytes       int  `validate:"min=4096,max=16777216" mapstructure:"max_write_bytes"`
	MaxBackground       int  `validate:"min=1,max=1024"       mapstructure:"max_background"`
	CongestionThreshold int  `validate:"min=1,max=1024"       mapstructure:"congestion_threshold"`
	WritebackCache      bool                                  `mapstructure:"writeback_cache"`
}
```

Default constants in `pkg/client/config/client.go` next to the others.

- [ ] **Step 2: Apply in `single.go`**

In `pkg/client/mount/single.go` where the existing `fuse.MountOptions` is built:

```go
mountOpts := fuse.MountOptions{
	// ... existing fields ...
	MaxRead:             cfg.FUSE.MaxReadBytes,
	MaxWrite:            cfg.FUSE.MaxWriteBytes,
	MaxBackground:       cfg.FUSE.MaxBackground,
	CongestionThreshold: cfg.FUSE.CongestionThreshold,
	EnableWriteback:     cfg.FUSE.WritebackCache,
}
```

(Field names follow `go-fuse v2.10.1`. If a name differs, use the `godoc` for `fuse.MountOptions`.)

Mirror in `pkg/client/mount/vfs.go`.

- [ ] **Step 3: Negotiate `MaxRead`/`MaxWrite` with the server**

The server's `FrameSizeBytes` is the natural upper bound. Before mounting, the client calls a lightweight RPC to learn it. Re-use the existing Version RPC by adding `frame_size_bytes` to `VersionReply`:

Diff `api/proto/version.proto`:

```diff
 message VersionReply {
   string version = 1;
   string commit = 2;
   string date = 3;
+  int32 frame_size_bytes = 4;
 }
```

Regenerate: `task gen:grpc`.

In `pkg/server/controller/version.go`, fill `FrameSizeBytes` from server config.

In the client mount path (whatever calls `NewClientFromConfig` before `Mount`):

```go
v, err := client.Version.Get(ctx, &proto.VersionRequest{})
if err == nil && v.GetFrameSizeBytes() > 0 {
	if cfg.FUSE.MaxReadBytes > int(v.GetFrameSizeBytes()) {
		cfg.FUSE.MaxReadBytes = int(v.GetFrameSizeBytes())
	}
	if cfg.FUSE.MaxWriteBytes > int(v.GetFrameSizeBytes()) {
		cfg.FUSE.MaxWriteBytes = int(v.GetFrameSizeBytes())
	}
}
```

(If the Version call fails — old server, network glitch — keep the configured values. Don't gate the mount on this.)

- [ ] **Step 4: Manual test on the VM**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 10m -v -run TestStreamingReadE2E ./test/e2e/api/'
```

Expected: green. The new `MaxRead`/`MaxWrite` enable larger FUSE-kernel-side transfers; verify nothing breaks.

- [ ] **Step 5: Re-run benchmarks**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 30m -v -run TestIoBench ./test/e2e/fs/' | tee /tmp/bench-task7.log
```

Expect throughput improvement on sequential workloads. Note the numbers; they go into the final perf doc in Task 10.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/mount/single.go pkg/client/mount/vfs.go \
        pkg/client/config/client.go api/proto/version.proto \
        pkg/proto/version.pb.go pkg/proto/version_grpc.pb.go \
        pkg/server/controller/version.go docs/client/config.md
git commit -m "$(cat <<'EOF'
feat(client/fuse): tunable MaxRead/MaxWrite + background depth

Client now negotiates frame size with the server (via Version RPC)
and caps FUSE MaxRead/MaxWrite at that ceiling. Background depth and
congestion threshold are configurable; writeback cache stays off
pending Phase 4.
EOF
)"
```

---

## Task 8: Client-side readahead

**Files:**
- Create: `pkg/client/io/readahead.go`
- Create: `pkg/client/io/readahead_test.go`
- Modify: `pkg/client/io/file.go` — consult readahead state on `Read`.
- Modify: `pkg/client/config/client.go` — `ReadaheadChunkBytes`, `ReadaheadWindow`.

**Design:**
- Per-`GrpcFile` state: `lastOffset`, `lastSize`, `nextPrefetchOffset`.
- On each `Read(buf, off)`:
  - If `off == lastOffset + lastSize` (perfectly sequential), increment a `seqHits` counter.
  - When `seqHits >= 3`, mark the file as in "sequential mode" and kick off a goroutine that issues a streaming Read for `[nextPrefetchOffset, nextPrefetchOffset + ReadaheadChunkBytes)` into an in-memory ring buffer.
  - On the next `Read`, check the ring first; copy from there and only fall back to a network Read if the ring is empty or the requested range doesn't match.
- On a non-sequential `Read` (offset jump backwards or large skip), drop the ring and reset `seqHits`.
- One outstanding prefetch at a time per file. Cancel it on `Release`.

- [ ] **Step 1: Write the failing readahead unit test**

`pkg/client/io/readahead_test.go`:

```go
type ReadaheadSuite struct {
	suite.Suite
}

func (s *ReadaheadSuite) TestObserve_SequentialOffsetsTriggerPrefetch() {
	// Given a Readahead with chunkSize=4KiB, prefetchThreshold=3
	// Feed it offsets 0, 4096, 8192 — after the third, shouldPrefetch() returns true,
	// and the next prefetch offset is 12288.
}

func (s *ReadaheadSuite) TestObserve_BackwardSeekResetsState() {
	// Sequential 0,4096,8192. Then offset 0 again. shouldPrefetch() should be false.
}

func (s *ReadaheadSuite) TestServe_PrefilledBufferReturnsHit() {
	// Inject a prefetched [12288, 16384) buffer into Readahead.
	// Calling Serve(12288, 1024) returns the right bytes and hit=true.
	// Calling Serve(12288, 8192) returns hit=false (overflows the buffer; let the network handle it).
}

func TestReadaheadSuite(t *testing.T) { suite.Run(t, new(ReadaheadSuite)) }
```

- [ ] **Step 2: Implement `Readahead`**

```go
package io

import (
	"sync"
)

type Readahead struct {
	mu               sync.Mutex
	chunkSize        int
	threshold        int
	seqHits          int
	lastOffset       int64
	lastSize         int
	prefetched       []byte
	prefetchedOffset int64
}

func NewReadahead(chunkSize, threshold int) *Readahead {
	return &Readahead{chunkSize: chunkSize, threshold: threshold}
}

// Observe records that a read of size n at offset off was just served.
// Returns true if the caller should kick off a prefetch.
func (r *Readahead) Observe(off int64, n int) (prefetchOffset int64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	expected := r.lastOffset + int64(r.lastSize)
	if off == expected {
		r.seqHits++
	} else {
		r.seqHits = 0
		r.prefetched = nil
	}
	r.lastOffset = off
	r.lastSize = n
	if r.seqHits >= r.threshold && r.prefetched == nil {
		return off + int64(n), true
	}
	return 0, false
}

func (r *Readahead) Store(off int64, data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.prefetched = data
	r.prefetchedOffset = off
}

// Serve attempts to satisfy a read from the prefetched buffer.
// Returns (n, true) on hit and zeroes the prefetched buffer; (0, false) on miss.
func (r *Readahead) Serve(dest []byte, off int64) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.prefetched == nil {
		return 0, false
	}
	if off < r.prefetchedOffset || off >= r.prefetchedOffset+int64(len(r.prefetched)) {
		return 0, false
	}
	start := int(off - r.prefetchedOffset)
	end := start + len(dest)
	if end > len(r.prefetched) {
		return 0, false
	}
	copy(dest, r.prefetched[start:end])
	r.prefetched = nil // consume
	return len(dest), true
}
```

- [ ] **Step 3: Run readahead unit tests**

```bash
go test -v -run TestReadaheadSuite ./pkg/client/io/
```

Expected: green.

- [ ] **Step 4: Wire into `Read`**

In `pkg/client/io/file.go`, around the existing streaming Read implementation:

```go
func (f *GrpcFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	if n, hit := f.readahead.Serve(dest, off); hit {
		f.readahead.Observe(off, n)
		return fuse.ReadResultData(dest[:n]), fuse.OK
	}
	// ... existing streaming read (Task 2 implementation) ...
	res, st := f.doStreamingRead(dest, off)
	if st.Ok() {
		if prefetchOff, shouldPrefetch := f.readahead.Observe(off, len(dest)); shouldPrefetch {
			go f.doPrefetch(prefetchOff)
		}
	}
	return res, st
}

func (f *GrpcFile) doPrefetch(off int64) {
	buf := make([]byte, f.cfg.ReadaheadChunkBytes)
	// Same as doStreamingRead but discards on error and stores on success.
	res, st := f.doStreamingRead(buf, off)
	if !st.Ok() {
		return
	}
	// res is a ReadResult; convert it back to bytes.
	data, _ := res.Bytes(buf)
	f.readahead.Store(off, data)
}
```

- [ ] **Step 5: Run benchmarks**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 30m -v -run TestIoBench ./test/e2e/fs/' | tee /tmp/bench-task8.log
```

Expect a sequential-read throughput improvement on the 30 ms loopback run (the latency-sensitive case where prefetch matters most). Localhost should be neutral or slightly worse — that's fine; the goal is the wide-area case.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/io/readahead.go pkg/client/io/readahead_test.go \
        pkg/client/io/file.go pkg/client/config/client.go
git commit -m "$(cat <<'EOF'
feat(client/io): sequential readahead with single-chunk window

Detects 3-in-a-row sequential reads, prefetches the next chunk into
an in-memory ring, serves the next Read from there if it lines up.
Targets the high-RTT sequential read case where each round-trip costs.
EOF
)"
```

---

## Task 9: Client-side write coalescing

**Files:**
- Create: `pkg/client/io/coalesce.go`
- Create: `pkg/client/io/coalesce_test.go`
- Modify: `pkg/client/io/file.go` — route small writes through the coalescer.

**Design:**
- Per-`GrpcFile` `WriteCoalescer` holds an in-memory append buffer + start offset.
- On `Write(data, off)`:
  - If `coalescer.empty()`, set `coalescer.startOffset = off` and append.
  - If `off == coalescer.endOffset()`, append to the buffer.
  - If the appended size reaches `WriteCoalesceBytes`, flush the buffer with a single streaming Write.
  - If `off` is *not* contiguous, flush the existing buffer, then start a new one at `off`.
- On `Flush` (FUSE-level): drain any outstanding bytes.
- On `Release`: same — drain then release.
- Bounded by `WriteCoalesceBytes` (default 1 MiB — same as a streaming frame). Anything larger goes direct.

- [ ] **Step 1: Failing test**

`pkg/client/io/coalesce_test.go`:

```go
type CoalesceSuite struct {
	suite.Suite
}

func (s *CoalesceSuite) TestAppend_ContiguousSmallWritesAccumulate() {
	c := NewWriteCoalescer(1024)
	c.Append([]byte("aaaa"), 0)
	c.Append([]byte("bbbb"), 4)
	c.Append([]byte("cccc"), 8)
	pending := c.Drain()
	s.Equal(int64(0), pending.Offset)
	s.Equal([]byte("aaaabbbbcccc"), pending.Data)
}

func (s *CoalesceSuite) TestAppend_DiscontiguousWriteForcesFlush() {
	c := NewWriteCoalescer(1024)
	c.Append([]byte("aaaa"), 0)
	flush1 := c.Append([]byte("bbbb"), 1000) // non-contiguous: returns the prior batch
	s.NotNil(flush1)
	s.Equal(int64(0), flush1.Offset)
	s.Equal([]byte("aaaa"), flush1.Data)
	// the b's are now the new buffer
	pending := c.Drain()
	s.Equal(int64(1000), pending.Offset)
}

func (s *CoalesceSuite) TestAppend_ReachingThresholdForcesFlush() {
	c := NewWriteCoalescer(8)
	flush := c.Append([]byte("12345678"), 0)
	s.NotNil(flush) // buffer hit threshold, must flush before returning
}

func TestCoalesceSuite(t *testing.T) { suite.Run(t, new(CoalesceSuite)) }
```

- [ ] **Step 2: Implement**

```go
package io

type CoalescedWrite struct {
	Offset int64
	Data   []byte
}

type WriteCoalescer struct {
	threshold int
	buf       []byte
	startOff  int64
	hasData   bool
}

func NewWriteCoalescer(threshold int) *WriteCoalescer {
	return &WriteCoalescer{threshold: threshold}
}

// Append registers a write; returns a non-nil CoalescedWrite if the caller
// must flush it to the server before continuing.
func (w *WriteCoalescer) Append(data []byte, off int64) *CoalescedWrite {
	if !w.hasData {
		w.startOff = off
		w.buf = append(w.buf[:0], data...)
		w.hasData = true
		if len(w.buf) >= w.threshold {
			return w.Drain()
		}
		return nil
	}
	endOff := w.startOff + int64(len(w.buf))
	if off != endOff {
		flushed := w.Drain()
		w.startOff = off
		w.buf = append(w.buf[:0], data...)
		w.hasData = true
		return flushed
	}
	w.buf = append(w.buf, data...)
	if len(w.buf) >= w.threshold {
		return w.Drain()
	}
	return nil
}

func (w *WriteCoalescer) Drain() *CoalescedWrite {
	if !w.hasData {
		return nil
	}
	out := &CoalescedWrite{Offset: w.startOff, Data: append([]byte(nil), w.buf...)}
	w.buf = w.buf[:0]
	w.hasData = false
	return out
}
```

- [ ] **Step 3: Wire into Write/Flush/Release**

In `pkg/client/io/file.go`:

```go
func (f *GrpcFile) Write(data []byte, off int64) (uint32, fuse.Status) {
	if len(data) >= f.cfg.WriteCoalesceBytes {
		// Big enough that coalescing buys nothing — go direct.
		if pending := f.coalescer.Drain(); pending != nil {
			if _, st := f.streamingWrite(pending.Data, pending.Offset); !st.Ok() {
				return 0, st
			}
		}
		return f.streamingWrite(data, off)
	}
	if pending := f.coalescer.Append(data, off); pending != nil {
		if _, st := f.streamingWrite(pending.Data, pending.Offset); !st.Ok() {
			return 0, st
		}
	}
	// Lie to the kernel about the write being durable — Flush will reconcile.
	// This is safe: FUSE writeback semantics already assume Flush/fsync are the durability boundary.
	return uint32(len(data)), fuse.OK
}

func (f *GrpcFile) Flush() fuse.Status {
	if pending := f.coalescer.Drain(); pending != nil {
		if _, st := f.streamingWrite(pending.Data, pending.Offset); !st.Ok() {
			return st
		}
	}
	return f.doFlushRPC() // existing flush RPC
}

func (f *GrpcFile) Release() {
	if pending := f.coalescer.Drain(); pending != nil {
		_, _ = f.streamingWrite(pending.Data, pending.Offset)
	}
	f.doReleaseRPC()
}
```

(`streamingWrite` is a helper that wraps the streaming Write client logic from Task 3 into a reusable function.)

- [ ] **Step 4: Run unit tests**

```bash
go test -v -run TestCoalesceSuite ./pkg/client/io/
```

Expected: green.

- [ ] **Step 5: Run an e2e test that hits the coalesce path**

Add to `test/e2e/api/streaming_test.go`:

```go
func (s *StreamingWriteE2ESuite) TestManySmallWritesCoalesce() {
	// Open a file via mount, write 4096 records of 8 bytes each at consecutive offsets,
	// Flush, close, then verify on the server side that:
	//   - the file content is correct
	//   - the server saw < 4096 Write RPCs (use the metrics endpoint or wrap the controller)
}
```

The metrics check is the load-bearing assertion: pre-Phase-3 this would be 4096 unary RPCs; post-coalescing it should be a small number (≤ 32 for 1 MiB threshold).

- [ ] **Step 6: Benchmark**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -timeout 30m -v -run TestIoBench ./test/e2e/fs/' | tee /tmp/bench-task9.log
```

Expect a clear win on small-write workloads at 30 ms latency.

- [ ] **Step 7: Commit**

```bash
git add pkg/client/io/coalesce.go pkg/client/io/coalesce_test.go pkg/client/io/file.go \
        test/e2e/api/streaming_test.go
git commit -m "$(cat <<'EOF'
feat(client/io): coalesce small contiguous writes per fd

Buffers writes up to WriteCoalesceBytes (1 MiB default), flushes on
threshold, discontiguous offset, Flush or Release. Cuts the per-write
RTT cost on small-write workloads. Big writes pass through directly.
EOF
)"
```

---

## Task 10: Re-measure and compare against baseline

**Files:**
- Create: `docs/perf/phase3-final-2026-05-15-localhost.txt`
- Create: `docs/perf/phase3-final-2026-05-15-slow30ms.txt`
- Create: `docs/perf/phase3-deltas-2026-05-15-localhost.txt` — `benchstat` output diff
- Create: `docs/perf/phase3-deltas-2026-05-15-slow30ms.txt` — same, for the latency-injected run
- Create: `docs/perf/phase3-final-2026-05-15.md` — human summary + DoD checklist

This task uses the harness built in Task 1. No new code — just measurement, diff, and write-up.

- [ ] **Step 1: Sync the post-Phase-3 tree to the VM**

```bash
rsync -av --delete --exclude '.git/' --exclude 'ui/frontend/node_modules' \
  ./ ubuntu@192.168.11.11:~/gMountie/
```

- [ ] **Step 2: Final benchmark — localhost**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task perf:bench OUT=docs/perf/phase3-final-2026-05-15-localhost.txt'
```

(Same `COUNT`/`BENCHTIME` defaults as the baseline so benchstat's comparison is apples-to-apples.)

- [ ] **Step 3: Final benchmark — 30 ms loopback**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && sudo bash scripts/start-slow-loopback.sh 30ms 0%'
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task perf:bench OUT=docs/perf/phase3-final-2026-05-15-slow30ms.txt COUNT=3 BENCHTIME=20s'
ssh ubuntu@192.168.11.11 'cd ~/gMountie && sudo bash scripts/stop-slow-loopback.sh'
```

(Match baseline's COUNT=3 / BENCHTIME=20s so benchstat doesn't complain about mismatched iteration budgets.)

- [ ] **Step 4: Pull outputs back and run benchstat**

```bash
scp ubuntu@192.168.11.11:~/gMountie/docs/perf/phase3-final-2026-05-15-*.txt docs/perf/

task perf:diff BEFORE=docs/perf/baseline-2026-05-15-localhost.txt \
               AFTER=docs/perf/phase3-final-2026-05-15-localhost.txt \
  | tee docs/perf/phase3-deltas-2026-05-15-localhost.txt

task perf:diff BEFORE=docs/perf/baseline-2026-05-15-slow30ms.txt \
               AFTER=docs/perf/phase3-final-2026-05-15-slow30ms.txt \
  | tee docs/perf/phase3-deltas-2026-05-15-slow30ms.txt
```

`benchstat` prints a delta column per benchmark with geomean and p-value. `~` = no statistically significant change at the default 95% confidence. A negative `sec/op` delta or positive `B/s` delta is an improvement.

- [ ] **Step 5: Verify the DoD inline against the delta files**

Read each delta file. For the spec's success criteria:

- **Sequential read of 1 GiB ≥ 70% of raw loopback FUSE throughput on localhost.** Look at `BenchmarkSeqRead1GiB-N` MB/s in `phase3-final-2026-05-15-localhost.txt`. Compare to a raw-loopback baseline by running `dd if=/dev/zero of=/tmp/x bs=1M count=1024` on the VM's tmpfs and computing its throughput; the ratio must be ≥ 0.70.
- **Write of 1 GiB completes without OOM and without hitting the unary cap.** Pre-Phase-3 this benchmark would fail with `ResourceExhausted`; if it runs at all post-streaming, this is satisfied.
- **Metadata ops latency does not regress more than 10% vs baseline.** Check the deltas for `BenchmarkOpenStatClose`, `BenchmarkReaddir100`, `BenchmarkLookup` — `ns/op` delta must be ≤ +10%.
- **4 GiB bidirectional copy verified bit-exact.** The e2e test from Task 3 (`TestBidirectional4GiB`) must be green; record `task test` output line.
- **Compound of 100 GetAttrs completes in one RTT.** The e2e test from Task 4 (`Test100GetAttrInOneRTT`) must be green and report < 50ms on localhost / < 100ms on 30ms loopback.

- [ ] **Step 6: Write the final summary**

Create `docs/perf/phase3-final-2026-05-15.md`:

```markdown
# Phase 3 performance — final 2026-05-15

**Commit:** `<git rev-parse HEAD on develop after Task 9>`
**Compared against:** `docs/perf/baseline-2026-05-15.md`

## Deltas (benchstat)

### Localhost

```
<paste contents of docs/perf/phase3-deltas-2026-05-15-localhost.txt verbatim>
```

### 30 ms loopback

```
<paste contents of docs/perf/phase3-deltas-2026-05-15-slow30ms.txt verbatim>
```

## DoD verification (spec lines 169–175)

- [ ] Sequential read of 1 GiB ≥ 70% of raw loopback FUSE throughput on localhost — measured ratio: <X>
- [ ] Write of 1 GiB completes without OOM and without hitting the unary cap — confirmed by `BenchmarkSeqWrite1GiB`
- [ ] Metadata ops latency does not regress more than 10% vs baseline — worst delta: <op name> +<X%>
- [ ] 4 GiB bidirectional copy verified bit-exact — `TestBidirectional4GiB` PASS
- [ ] Compound of 100 GetAttrs completes in one RTT — `Test100GetAttrInOneRTT` PASS, <Xms> on localhost

## Compression decision

(One-line summary from `docs/perf/compression-2026-05-15.md`.)

## Knowns to revisit

- Writeback cache stays off — revisit when Phase 4 cache lands.
- QUIC transport — reassess once `grpc-go` has stable HTTP/3 support (Appendix B item 7).
- Aggregate compound across mutating ops — deferred; this phase only batches read-only metadata.
```

- [ ] **Step 7: Commit**

```bash
git add docs/perf/phase3-final-2026-05-15-localhost.txt \
        docs/perf/phase3-final-2026-05-15-slow30ms.txt \
        docs/perf/phase3-deltas-2026-05-15-localhost.txt \
        docs/perf/phase3-deltas-2026-05-15-slow30ms.txt \
        docs/perf/phase3-final-2026-05-15.md
git commit -m "$(cat <<'EOF'
docs(perf): Phase 3 final benchmarks + delta vs baseline

benchstat deltas attached for localhost and 30ms loopback runs.
DoD items from the spec verified inline. Phase ready for review.
EOF
)"
```

---

## Spec coverage self-check

| Spec item (lines 149–162) | Plan task |
|---|---|
| 1. Streaming reads | Task 2 |
| 1. Streaming writes | Task 3 |
| 1. session_id/request_id retained on streaming | Task 3 (request_id on first frame; Read remains naturally idempotent) |
| 2. Compound metadata RPC | Task 4 |
| 3. Tune compression / per-call codec | Task 5 |
| 3. Evaluate zstd | Task 5 step 5 |
| 4. FUSE mount-option tuning (MaxRead/MaxWrite/etc.) | Task 7 |
| 4. Negotiate values with server | Task 7 step 3 |
| 5. Client-side readahead | Task 8 |
| 5. Client-side write coalescing | Task 9 |
| 6. gRPC keepalive (`KeepaliveParams` + `EnforcementPolicy`) | Task 6 |
| Baseline first | Task 1 |
| DoD verification + deltas | Task 10 |

Out-of-scope items confirmed:
- Server-side caching of file content: explicitly out per spec line 167.
- Multi-server / replication: out per spec line 168.
- Cache layer itself: Phase 4.
- Shared `ClientConn` across mounts in one process: deferred to Phase 8 per spec line 161.
