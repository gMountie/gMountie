// Package wal_e2e verifies the full WAL stack (coordinator + layer + Apply RPC)
// over an in-process bufconn server with no FUSE mount. Five scenarios are
// covered:
//
//  1. RYOW (read-your-own-writes): a delegated Create+Write+Read returns
//     pending bytes BEFORE any flush; after an explicit Fsync the server
//     has the file.
//
//  2. RTT collapse: N files created under a delegation issues far fewer
//     Apply RPC streams than N (the batching win).
//
//  3. Recall-flush coherence: client A holds delegation + has deferred WAL
//     ops; client B writes into A's subtree → server recalls A → A flushes
//     its WAL → B sees A's writes.
//
//  4. Replay dedup: a second Replay of already-committed seqs is silently
//     skipped by the server (seq ≤ watermark = no-op), leaving the FS intact.
//
//  5. Loss logging: a forced server Apply failure (OpWrite to a path that has
//     no preceding Create) fires the default logDataLost hook, printing all
//     lost paths, and increments the WalDataLost metric.
//
// Stack assembly (no FUSE):
//
//	transport.NewBackendClient(cl, vol,
//	    WithDelegationHook(mgr),
//	    WithWriteDrain(coord))
//	→ delegation.NewLayer(transport, mgr)      // write-set recorder
//	→ wal.NewLayer(delegation, mgr, coord)     // RYOW seam
package wal_e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	clientdelegation "go.gmountie.dev/gmountie/pkg/client/backend/delegation"
	"go.gmountie.dev/gmountie/pkg/client/backend/transport"
	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
	clientmetrics "go.gmountie.dev/gmountie/pkg/client/metrics"
	grpcclient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	serverapp "go.gmountie.dev/gmountie/pkg/server"
	"go.gmountie.dev/gmountie/pkg/server/watermark"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.gmountie.dev/gmountie/test/e2e/utils"
)

// ─── in-memory watermark store ───────────────────────────────────────────────

// memWatermarkStore is a thread-safe, in-memory watermark.Store for tests.
// It mirrors the fakeWatermarkStore used in apply_test.go but is exported so
// the e2e harness can inject it via WithServerAppContextOption.
type memWatermarkStore struct {
	mu      sync.Mutex
	records map[watermark.Key]watermark.Record
}

func newMemWatermarkStore() *memWatermarkStore {
	return &memWatermarkStore{records: make(map[watermark.Key]watermark.Record)}
}

func (m *memWatermarkStore) Get(k watermark.Key) (watermark.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[k], nil
}

func (m *memWatermarkStore) Advance(k watermark.Key, wm uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[k]
	if wm > r.Watermark {
		r.Watermark = wm
		m.records[k] = r
	}
	return nil
}

func (m *memWatermarkStore) RevokeGen(k watermark.Key, gen uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[k]
	r.RevokedGens = append(r.RevokedGens, gen)
	m.records[k] = r
	return nil
}

func (m *memWatermarkStore) NextGen(k watermark.Key) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.records[k]
	r.GenHi++
	gen := r.GenHi
	m.records[k] = r
	return gen, nil
}

func (m *memWatermarkStore) watermarkFor(k watermark.Key) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[k].Watermark
}

func (m *memWatermarkStore) Close() error { return nil }

// ─── trackingInvalidator ─────────────────────────────────────────────────────

type trackingInvalidator struct {
	mu    sync.Mutex
	roots []string
}

func (t *trackingInvalidator) InvalidateSubtree(root string) {
	t.mu.Lock()
	t.roots = append(t.roots, root)
	t.mu.Unlock()
}

func (t *trackingInvalidator) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.roots)
}

// ─── walClientStack ───────────────────────────────────────────────────────────

// walClientStack is a manually-wired client with delegation + WAL layers but
// no FUSE mount.  Stack order (outermost → inner):
//
//	walBe → delegationBe → transportBe (gRPC leaf)
//
// The recall pump goroutine runs in the background, driving mgr.OnRecall.
type walClientStack struct {
	cl           grpcclient.Client
	mgr          *clientdelegation.Manager
	inv          *trackingInvalidator
	coord        *wal.Coordinator
	walLog       *wal.BboltLog
	be           backend.FileSystemBackend
	cancelRecall context.CancelFunc

	mkdirSeq atomic.Uint64
}

// newWALStack builds a WAL-enabled client stack backed by the given AppTestingContext.
func newWALStack(t *testing.T, tc *utils.AppTestingContext, user, pass, volName string) *walClientStack {
	t.Helper()

	cl, err := tc.NewClientAs(user, pass)
	require.NoError(t, err, "NewClientAs")

	walPath := filepath.Join(t.TempDir(), "wal.db")
	walLog, err := wal.Open(walPath)
	require.NoError(t, err, "wal.Open")

	overlay := wal.NewOverlay()
	inv := &trackingInvalidator{}
	mgr := clientdelegation.NewManager(inv)

	coord := wal.NewCoordinator(mgr, walLog, overlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
	)

	transportBe := transport.NewBackendClient(cl, volName,
		transport.WithDelegationHook(mgr),
		transport.WithWriteDrain(coord),
	)
	delegationBe := clientdelegation.NewLayer(transportBe, mgr)
	walBe := wal.NewLayer(delegationBe, mgr, coord)

	recallCtx, cancelRecall := context.WithCancel(context.Background())
	mgr.SetCancel(cancelRecall)
	mgr.SetRecallFlusher(coord)

	cs := &walClientStack{
		cl:           cl,
		mgr:          mgr,
		inv:          inv,
		coord:        coord,
		walLog:       walLog,
		be:           walBe,
		cancelRecall: cancelRecall,
	}
	go runRecallPump(recallCtx, cl, mgr)
	return cs
}

// MkdirFresh creates a uniquely-named child under parent.
func (cs *walClientStack) MkdirFresh(ctx context.Context, parent string) (proto.FsError, string) {
	name := fmt.Sprintf("sub%d", cs.mkdirSeq.Add(1))
	path := parent + "/" + name
	_, st := cs.be.Mkdir(ctx, path, 0o755)
	return st, path
}

func (cs *walClientStack) Close() {
	cs.cancelRecall()
	cs.mgr.Close()
	_ = cs.coord.Close()
	_ = cs.be.Close()
	_ = cs.cl.Close()
}

// runRecallPump mirrors mount.runRecallLoop: opens the bidi Recall stream,
// delivers OnRecall to the manager for each message, and sends the ack.
func runRecallPump(ctx context.Context, cl grpcclient.Client, mgr *clientdelegation.Manager) {
	backoff := 50 * time.Millisecond
	const maxBackoff = 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		stream, err := cl.Fs().Recall(ctx, grpc.WaitForReady(true))
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
		}
		backoff = 50 * time.Millisecond
		for {
			msg, err := stream.Recv()
			if err != nil {
				break
			}
			if recallErr := mgr.OnRecall(ctx, msg.Root); recallErr != nil {
				continue
			}
			_ = stream.Send(&proto.RecallAck{RecallId: msg.RecallId, Done: true})
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

// bg returns a plain background context for backend calls.
func bg() context.Context { return context.Background() }

// ─── acquireGrant helper ──────────────────────────────────────────────────────

// acquireGrant seeds the write-set and issues Mkdir calls until the manager
// holds a delegation grant covering parent.
func acquireGrant(t *testing.T, cs *walClientStack, parent string, deadline time.Duration) {
	t.Helper()
	cs.mgr.Record(parent + "/seed1")
	cs.mgr.Record(parent + "/seed2")
	require.Eventually(t, func() bool {
		st, _ := cs.MkdirFresh(bg(), parent)
		return st == proto.FsError_FS_OK && cs.mgr.IsDelegated(parent)
	}, deadline, 20*time.Millisecond, "must obtain delegation grant for %q", parent)
}

// ─── WAL e2e suite ────────────────────────────────────────────────────────────

// WalE2ESuite exercises the full WAL stack over bufconn (no FUSE).
// Each scenario spins up its own AppTestingContext with its own in-memory
// watermark store so watermark state does not leak between scenarios.
type WalE2ESuite struct {
	suite.Suite
}

// newScenarioCtx creates an isolated AppTestingContext for a single scenario.
// Each scenario owns its own server + watermark store so WAL seq counters
// and dedup state do not interfere across tests.
//
// Returns (tc, wm, srcDir, volName). The caller must call tc.Close() when
// done (register via t.Cleanup or defer).
func (s *WalE2ESuite) newScenarioCtx(extraOpts ...utils.TestOptions) (
	tc *utils.AppTestingContext, wm *memWatermarkStore, srcDir, volName string,
) {
	s.T().Helper()
	srcDir = s.T().TempDir()
	volName = "testvol"
	wm = newMemWatermarkStore()

	opts := []utils.TestOptions{
		utils.WithBasicAuth("user", "pass"),
		utils.WithExistingVolume(volName, srcDir),
		utils.WithSessionGracePeriod(5 * time.Second),
		utils.WithServerAppContextOption(serverapp.WithWatermarkStore(wm)),
	}
	opts = append(opts, extraOpts...)

	var err error
	tc, err = utils.NewAppTestingContext(opts...)
	s.Require().NoError(err, "build AppTestingContext")
	s.Require().NoError(tc.Start(), "start server")
	s.T().Cleanup(func() { _ = tc.Close() })
	return tc, wm, srcDir, volName
}

// ─── Scenario 1: RYOW (Read-Your-Own-Writes) ─────────────────────────────────
// A delegated Create+Write+Read returns the pending bytes BEFORE any flush.
// After Fsync the server has the file and its bytes.

func (s *WalE2ESuite) TestRYOW() {
	r := s.Require()
	ctx := bg()

	tc, _, srcDir, volName := s.newScenarioCtx()
	srcSub := filepath.Join(srcDir, "ryow")
	r.NoError(os.Mkdir(srcSub, 0o755))

	cs := newWALStack(s.T(), tc, "user", "pass", volName)
	defer cs.Close()

	// Acquire delegation for "ryow".
	acquireGrant(s.T(), cs, "ryow", 5*time.Second)
	r.True(cs.mgr.IsDelegated("ryow"))

	// Create a new file under the delegation.
	fh, _, fst := cs.be.Create(ctx, "ryow", "hello.txt", uint32(os.O_RDWR|os.O_CREATE), 0o644)
	r.Equal(proto.FsError_FS_OK, fst, "Create must succeed on delegated path")
	defer cs.be.Release(ctx, fh)

	want := []byte("hello WAL world")

	// Write bytes — goes to the overlay; NOT yet on the server.
	_, wst := cs.be.Write(ctx, fh, 0, want)
	r.Equal(proto.FsError_FS_OK, wst, "Write must succeed")

	// RYOW: read back from the same handle (overlay-sourced) BEFORE any flush.
	got := make([]byte, len(want))
	n, rst := cs.be.Read(ctx, fh, 0, got)
	r.Equal(proto.FsError_FS_OK, rst, "Read must succeed before flush")
	r.Equal(len(want), n, "Read must return full byte count")
	r.Equal(want, got[:n], "read-your-own-writes must return the pending bytes")

	// Server must NOT have the file yet.
	_, statErr := os.Stat(filepath.Join(srcSub, "hello.txt"))
	r.True(os.IsNotExist(statErr), "server must not have the file before flush")

	// Flush via Fsync — drives the Apply RPC.
	r.Equal(proto.FsError_FS_OK, cs.coord.Fsync(ctx), "Fsync must succeed")

	// Server now has the file and its bytes.
	serverBytes, readErr := os.ReadFile(filepath.Join(srcSub, "hello.txt"))
	r.NoError(readErr, "server must have the file after flush")
	r.Equal(want, serverBytes, "server bytes must match what was written")
}

// ─── Scenario 2: RTT collapse (batching win) ─────────────────────────────────
// N files created under a delegation → far fewer Apply streams than N.

func (s *WalE2ESuite) TestRTTCollapse() {
	r := s.Require()

	const N = 20

	// Count Apply streams via a server interceptor.
	var applyCount atomic.Int64
	interceptor := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if info.FullMethod == applyFullMethod {
			applyCount.Add(1)
		}
		return handler(srv, ss)
	}

	tc, _, srcDir, volName := s.newScenarioCtx(utils.WithServerStreamInterceptors(interceptor))

	cs := newWALStack(s.T(), tc, "user", "pass", volName)
	defer cs.Close()

	// Create the "rtt" subtree root on the server before acquiring delegation.
	r.NoError(os.Mkdir(filepath.Join(srcDir, "rtt"), 0o755))

	// Track the srcSub for the file stat check.
	srcSub := srcDir

	// Acquire delegation for "rtt".
	acquireGrant(s.T(), cs, "rtt", 5*time.Second)

	// Create N files under the delegation — all writes go to the WAL / overlay.
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("f%03d.txt", i)
		fh, _, fst := cs.be.Create(bg(), "rtt", name, uint32(os.O_RDWR|os.O_CREATE), 0o644)
		r.Equal(proto.FsError_FS_OK, fst, "Create rtt/%s must succeed", name)
		data := []byte(fmt.Sprintf("content-%d", i))
		_, wst := cs.be.Write(bg(), fh, 0, data)
		r.Equal(proto.FsError_FS_OK, wst)
		_ = cs.be.Release(bg(), fh)
	}

	// One explicit flush — all N ops in a single Apply stream.
	applyBefore := applyCount.Load()
	r.Equal(proto.FsError_FS_OK, cs.coord.Fsync(bg()), "Fsync must succeed")
	applyAfter := applyCount.Load()

	applyDelta := applyAfter - applyBefore
	r.Less(applyDelta, int64(N),
		"WAL must batch: %d Apply streams for %d files (must be < N)", applyDelta, N)
	r.GreaterOrEqual(applyDelta, int64(1),
		"at least one Apply stream must fire on flush")

	// Sanity: server received all N files.
	for i := 0; i < N; i++ {
		_, statErr := os.Stat(filepath.Join(srcSub, fmt.Sprintf("rtt/f%03d.txt", i)))
		r.NoError(statErr, "server must have file %d after flush", i)
	}
}

// ─── Scenario 3: Recall-flush coherence ──────────────────────────────────────
// A holds delegation + deferred WAL ops; B writes into A's subtree → server
// recalls A → A flushes WAL → B sees A's writes (no stale state for B).

func (s *WalE2ESuite) TestRecallFlushCoherence() {
	r := s.Require()

	tc, _, srcDir, volName := s.newScenarioCtx()
	srcSub := filepath.Join(srcDir, "coherence")
	r.NoError(os.Mkdir(srcSub, 0o755))

	csA := newWALStack(s.T(), tc, "user", "pass", volName)
	defer csA.Close()
	csB := newWALStack(s.T(), tc, "user", "pass", volName)
	defer csB.Close()

	// A acquires delegation for "coherence".
	acquireGrant(s.T(), csA, "coherence", 5*time.Second)
	r.True(csA.mgr.IsDelegated("coherence"))

	// A creates a file inside the delegation (deferred in WAL, not yet on server).
	fh, _, fst := csA.be.Create(bg(), "coherence", "a-file.txt", uint32(os.O_RDWR|os.O_CREATE), 0o644)
	r.Equal(proto.FsError_FS_OK, fst, "A Create must succeed")
	wantBytes := []byte("written by A pre-recall")
	_, wst := csA.be.Write(bg(), fh, 0, wantBytes)
	r.Equal(proto.FsError_FS_OK, wst)
	_ = csA.be.Release(bg(), fh)

	// Confirm file is NOT on the server yet (still in A's overlay).
	_, statErr := os.Stat(filepath.Join(srcSub, "a-file.txt"))
	r.True(os.IsNotExist(statErr), "file must not be on server before recall")

	// B contends under "coherence" → triggers recall of A.
	// setRecallFlusher was called in newWALStack, so OnRecall calls coord.FlushForRecall.
	invBefore := csA.inv.count()
	r.Eventually(func() bool {
		st, _ := csB.MkdirFresh(bg(), "coherence")
		return st == proto.FsError_FS_OK
	}, 5*time.Second, 50*time.Millisecond, "B must succeed after A is recalled")

	// A's grant must be revoked.
	r.Eventually(func() bool {
		return !csA.mgr.IsDelegated("coherence")
	}, 5*time.Second, 50*time.Millisecond, "A must lose grant after recall")

	// A's invalidator must have fired (OnRecall ran fully).
	r.Eventually(func() bool {
		return csA.inv.count() > invBefore
	}, 5*time.Second, 50*time.Millisecond, "A's recall must complete (invalidator fired)")

	// A's deferred write must now be on the server (flushed by FlushForRecall).
	serverBytes, readErr := os.ReadFile(filepath.Join(srcSub, "a-file.txt"))
	r.NoError(readErr, "a-file.txt must be on the server after recall-flush")
	r.Equal(wantBytes, serverBytes, "server must have A's pre-recall bytes after flush")
}

// ─── Scenario 4: Replay dedup ─────────────────────────────────────────────────
// A WAL that has already been applied is replayed again (simulating a client
// reconnect). The server's seq-watermark deduplicates every op — no double-apply.

func (s *WalE2ESuite) TestReplayDedup() {
	r := s.Require()

	tc, wm, srcDir, volName := s.newScenarioCtx()
	srcSub := filepath.Join(srcDir, "dedup")
	r.NoError(os.Mkdir(srcSub, 0o755))

	cs := newWALStack(s.T(), tc, "user", "pass", volName)
	defer cs.Close()

	// Acquire delegation and create a directory via the WAL.
	acquireGrant(s.T(), cs, "dedup", 5*time.Second)
	_, mst := cs.be.Mkdir(bg(), "dedup/replay-dir", 0o755)
	r.Equal(proto.FsError_FS_OK, mst, "Mkdir must succeed")

	// Flush to the server (first apply — watermark advances to N).
	r.Equal(proto.FsError_FS_OK, cs.coord.Fsync(bg()), "first Fsync must succeed")

	// Server must have the directory.
	_, statErr := os.Stat(filepath.Join(srcSub, "replay-dir"))
	r.NoError(statErr, "dedup/replay-dir must exist after first flush")

	// Record the server-side watermark AFTER first apply.
	// The passthrough volume resolves caller nil → Identity{} → Principal="",
	// so the watermark key uses an empty Identity string.
	wmKey := watermark.Key{Identity: "", Volume: volName}
	wmAfterFirst := wm.watermarkFor(wmKey)
	r.Greater(wmAfterFirst, uint64(0), "server watermark must advance after first flush")

	// Build a SECOND WAL log at a different path and append the same operations
	// (fresh seqs, but the server's watermark is already past them when we pass
	// resumeWatermark=0 and the server sees seq ≤ wmAfterFirst).
	//
	// Dedup proof: the ops contain a Mkdir for a path that already exists.
	// If the server re-applies them it would EEXIST; dedup means it skips silently.
	replayLog, err := wal.Open(filepath.Join(s.T().TempDir(), "replay.db"))
	r.NoError(err, "open replay WAL")
	defer replayLog.Close()

	// Append the same ops that were already applied — fresh seqs (1, 2, ...).
	replayOverlay := wal.NewOverlay()
	replayMgr := clientdelegation.NewManager(&trackingInvalidator{})
	replayCoord := wal.NewCoordinator(replayMgr, replayLog, replayOverlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cs.cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
	)
	defer replayCoord.Close()

	// Re-record the same Mkdir op.
	r.NoError(replayCoord.RecordOp(wal.Op{
		Kind: wal.OpMkdir,
		Path: "dedup/replay-dir",
		Mode: 0o755,
	}))

	// Replay from watermark=0 → server sees seq=1, but its current watermark is
	// wmAfterFirst (which is the seq that was applied the first time). Since the
	// server's watermark ≥ 1 after the first apply, seq=1 is deduplicated.
	//
	// Note: the server-side watermark is keyed on (identity, volume). The identity
	// for squash mode is "" (passthrough resolves caller nil → Identity{} →
	// Principal=""); dedup triggers when incoming seq ≤ stored watermark.
	replayErr := replayCoord.Replay(bg(), 0)
	// Replay must succeed without error: all seqs are deduplicated (no ordered-halt,
	// no data loss). An EEXIST from a re-applied Mkdir would surface as FS_EEXIST
	// → ordered-halt → non-OK FsError return.
	r.NoError(replayErr, "Replay must not return an error when ops are deduplicated")

	// UNAMBIGUOUS DEDUP PROOF: the server-side watermark must not have advanced
	// during the replay. seq ≤ stored watermark ⇒ the server skipped the op entirely
	// without executing it. Combined with replayErr==nil (no halt) this proves the
	// coordinator's dedup path fired, not a silent ignore of an EEXIST.
	wmAfterReplay := wm.watermarkFor(wmKey)
	r.Equal(wmAfterFirst, wmAfterReplay,
		"server watermark must not advance on replay of already-committed seqs (dedup proof)")

	// Directory must still exist exactly once (no double-apply artifacts).
	entries, readErr := os.ReadDir(srcSub)
	r.NoError(readErr, "dedup/ must be readable")
	var replayDirs int
	for _, e := range entries {
		if e.Name() == "replay-dir" {
			replayDirs++
		}
	}
	r.Equal(1, replayDirs, "replay-dir must exist exactly once (no double-apply)")
}

// ─── Scenario 5: Loss logging ─────────────────────────────────────────────────
// Force a server Apply ordered-halt (OpWrite to a non-existent path → ENOENT).
// Assert: the DEFAULT onLoss hook (logDataLost) fires — no WithOnLoss override —
// incrementing the WalDataLost metric via wal.SetMetrics and emitting an ERROR
// log via log.Log with a "lost_paths" field enumerating every lost file path.

func (s *WalE2ESuite) TestLossLogging() {
	r := s.Require()

	// ── Install zap observer BEFORE the server starts ──────────────────────────
	// The observer captures ERROR entries from log.Log regardless of which
	// goroutine emits them. We must set log.Log before any goroutines start and
	// restore it AFTER all goroutines stop. t.Cleanup uses LIFO ordering, so we
	// register the restore NOW (first → runs last), then call newScenarioCtx
	// (second → tc.Close() runs first, stopping server goroutines before restore).
	observerCore, observed := observer.New(zapcore.ErrorLevel)
	origLog := log.Log
	log.Log = zap.New(observerCore)
	s.T().Cleanup(func() { log.Log = origLog }) // runs LAST (LIFO)

	// ── Wire a fresh metrics instance via wal.SetMetrics ───────────────────────
	// logDataLost increments walMetrics when non-nil. We inject a fresh registry
	// so counters are isolated. LIFO: restore runs after coordinator.Close() (defer).
	m := clientmetrics.NewMetrics()
	wal.SetMetrics(m)
	s.T().Cleanup(func() { wal.SetMetrics(nil) }) // runs LAST (LIFO)

	// ── Start server (registers tc.Close() Cleanup: runs FIRST via LIFO) ───────
	tc, _, _, volName := s.newScenarioCtx()
	_ = tc

	// ── Build coordinator WITHOUT WithOnLoss — default hook = logDataLost ──────
	walLog, err := wal.Open(filepath.Join(s.T().TempDir(), "loss.db"))
	r.NoError(err)

	overlay := wal.NewOverlay()
	lossMgr := clientdelegation.NewManager(&trackingInvalidator{})
	cl, err := tc.NewClientAs("user", "pass")
	r.NoError(err)
	defer cl.Close()

	lossCoord := wal.NewCoordinator(lossMgr, walLog, overlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
		// No WithOnLoss — the coordinator installs defaultOnLoss → logDataLost.
	)
	defer lossCoord.Close()

	// ── Write ops for paths that do NOT exist on the server ────────────────────
	// OpWrite to a non-existent path → server Open(O_WRONLY, no O_CREAT) →
	// ENOENT → ordered-halt at seq=1 → logDataLost fires via onLoss.
	lostPath1 := "ghost1.bin"
	lostPath2 := "ghost2.bin"
	r.NoError(lossCoord.RecordOp(wal.Op{
		Kind:   wal.OpWrite,
		Path:   lostPath1,
		Offset: 0,
		Data:   []byte("phantom bytes 1"),
	}))
	r.NoError(lossCoord.RecordOp(wal.Op{
		Kind:   wal.OpWrite,
		Path:   lostPath2,
		Offset: 0,
		Data:   []byte("phantom bytes 2"),
	}))

	// ── Flush → server ordered-halt → default hook fires ───────────────────────
	fsyncFerr := lossCoord.Fsync(bg())
	r.NotEqual(proto.FsError_FS_OK, fsyncFerr, "Fsync must return FS_EIO on ordered-halt")

	// ── Assert WalDataLost metric incremented by logDataLost ───────────────────
	// logDataLost calls walMetrics.WalDataLostEventInc("apply-failure") (+1 event)
	// and walMetrics.WalDataLostFilesAdd("apply-failure", distinctPaths) (+2 files).
	eventsVal := testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "events"))
	r.Equal(1.0, eventsVal, "WalDataLost events must be 1 (default hook increments once per halt)")
	filesVal := testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "files"))
	r.Equal(2.0, filesVal, "WalDataLost files must equal distinct lost-path count (2)")

	// ── Assert ERROR log emitted by logDataLost enumerates both ghost paths ─────
	// Filter by message so other server ERROR logs don't interfere.
	lossEntries := observed.FilterMessage("WAL data loss: un-flushed ops discarded without reaching the server")
	r.Len(lossEntries.All(), 1, "exactly one WAL data-loss ERROR log must be emitted by logDataLost")

	entry := lossEntries.All()[0]
	r.Equal(zapcore.ErrorLevel, entry.Level, "loss log must be at ERROR level")

	// zap.Strings("lost_paths") is encoded by zapcore.MapObjectEncoder as
	// []interface{} (each element is a string). Extract and assert both ghost paths.
	rawPaths, ok := entry.ContextMap()["lost_paths"]
	r.True(ok, "log entry must contain a 'lost_paths' field")
	ifacePaths, ok := rawPaths.([]interface{})
	r.True(ok, "lost_paths must be a []interface{} (zap.Strings encoding)")
	var loggedPaths []string
	for _, v := range ifacePaths {
		str, isStr := v.(string)
		r.True(isStr, "each element of lost_paths must be a string")
		loggedPaths = append(loggedPaths, str)
	}
	r.Contains(loggedPaths, lostPath1, "lost_paths must include ghost1.bin")
	r.Contains(loggedPaths, lostPath2, "lost_paths must include ghost2.bin")
}

// ─── Scenario 6: Unmount flush — data reaches server on coord.Close() ────────
// Proves (a) the apply-factory/volume/caller options are wired as production
// single.go does it, and (b) coord.Close() flushes pending ops instead of
// silently discarding them.  The coordinator is built with the same options
// single.go uses: a real Apply factory from a bufconn RpcFsClient, WithVolume,
// and WithCaller carrying the mounting process's own uid/gid/pid.

func (s *WalE2ESuite) TestUnmountFlush_DataReachesServer() {
	r := s.Require()
	ctx := bg()

	tc, _, srcDir, volName := s.newScenarioCtx()
	srcSub := filepath.Join(srcDir, "unmount")
	r.NoError(os.Mkdir(srcSub, 0o755))

	cl, err := tc.NewClientAs("user", "pass")
	r.NoError(err)
	defer cl.Close()

	walPath := filepath.Join(s.T().TempDir(), "wal.db")
	walLog, err := wal.Open(walPath)
	r.NoError(err)

	overlay := wal.NewOverlay()
	inv := &trackingInvalidator{}
	mgr := clientdelegation.NewManager(inv)

	// Build coordinator with SAME options as single.go: factory from real client,
	// volume name, and mount-level caller (uid/gid/pid of this process).
	mountCaller := &proto.Caller{
		Owner: &proto.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())},
		Pid:   uint32(os.Getpid()),
	}
	coord := wal.NewCoordinator(mgr, walLog, overlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
		wal.WithCaller(mountCaller),
	)

	transportBe := transport.NewBackendClient(cl, volName,
		transport.WithDelegationHook(mgr),
		transport.WithWriteDrain(coord),
	)
	delegationBe := clientdelegation.NewLayer(transportBe, mgr)
	walBe := wal.NewLayer(delegationBe, mgr, coord)

	mgr.SetRecallFlusher(coord)

	// Acquire delegation for the "unmount" subtree.
	cs := &walClientStack{
		cl:    cl,
		mgr:   mgr,
		inv:   inv,
		coord: coord,
		be:    walBe,
	}
	acquireGrant(s.T(), cs, "unmount", 5*time.Second)
	r.True(mgr.IsDelegated("unmount"))

	// Create + write a file — deferred in the WAL (not yet on server).
	fh, _, fst := walBe.Create(ctx, "unmount", "close-flush.txt", uint32(os.O_RDWR|os.O_CREATE), 0o644)
	r.Equal(proto.FsError_FS_OK, fst, "Create must succeed on delegated path")
	want := []byte("data flushed on unmount")
	_, wst := walBe.Write(ctx, fh, 0, want)
	r.Equal(proto.FsError_FS_OK, wst)
	_ = walBe.Release(ctx, fh)

	// Server must NOT have the file yet.
	_, statErr := os.Stat(filepath.Join(srcSub, "close-flush.txt"))
	r.True(os.IsNotExist(statErr), "server must not have the file before Close")

	// coord.Close() simulates unmount: must flush pending ops to the server.
	r.NoError(coord.Close(), "coord.Close() must succeed (data reaches server)")

	// Server now has the file — flush happened on Close, not discarded.
	serverBytes, readErr := os.ReadFile(filepath.Join(srcSub, "close-flush.txt"))
	r.NoError(readErr, "server must have the file after coord.Close()")
	r.Equal(want, serverBytes, "server bytes must match what was written before Close")

	// Second Close is a safe no-op (log already closed, no panic).
	_ = coord.Close()
}

// ─── Scenario 7: Flush failure on Close fires the §5.1 loud loss log ─────────
// Force a server ordered-halt during coord.Close(): write ops for paths that
// do not exist on the server (ENOENT → ordered-halt). Assert:
//   - coord.Close() returns a non-nil error (not silent).
//   - The §5.1 logDataLost hook fires: WalDataLost events == 1, ERROR log
//     emitted with lost_paths.
//   - events == 1 proves Close did not double-flush (which would produce events==2).

func (s *WalE2ESuite) TestCloseFlushFailure_LoudLossLog() {
	r := s.Require()

	// Install zap observer BEFORE server starts; restore AFTER all goroutines
	// stop (LIFO t.Cleanup: restore runs last, tc.Close() runs first).
	observerCore, observed := observer.New(zapcore.ErrorLevel)
	origLog := log.Log
	log.Log = zap.New(observerCore)
	s.T().Cleanup(func() { log.Log = origLog })

	// Wire a fresh metrics instance; restore after (LIFO).
	m := clientmetrics.NewMetrics()
	wal.SetMetrics(m)
	s.T().Cleanup(func() { wal.SetMetrics(nil) })

	// Start server (tc.Close() registered as first Cleanup → runs before log/metrics restore).
	tc, _, _, volName := s.newScenarioCtx()
	_ = tc

	cl, err := tc.NewClientAs("user", "pass")
	r.NoError(err)
	defer cl.Close()

	walLog, err := wal.Open(filepath.Join(s.T().TempDir(), "close-loss.db"))
	r.NoError(err)

	overlay := wal.NewOverlay()
	lossMgr := clientdelegation.NewManager(&trackingInvalidator{})
	mountCaller := &proto.Caller{
		Owner: &proto.Owner{Uid: uint32(os.Getuid()), Gid: uint32(os.Getgid())},
		Pid:   uint32(os.Getpid()),
	}

	// Build coordinator with production-equivalent options; default onLoss = logDataLost.
	lossCoord := wal.NewCoordinator(lossMgr, walLog, overlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
		wal.WithCaller(mountCaller),
		// No WithOnLoss — coordinator installs defaultOnLoss → logDataLost.
	)

	// Record ops for paths that do NOT exist on the server: OpWrite with no
	// preceding Create → server ENOENT → ordered-halt at seq=1.
	r.NoError(lossCoord.RecordOp(wal.Op{
		Kind:   wal.OpWrite,
		Path:   "ghost-close1.bin",
		Offset: 0,
		Data:   []byte("phantom bytes on close 1"),
	}))
	r.NoError(lossCoord.RecordOp(wal.Op{
		Kind:   wal.OpWrite,
		Path:   "ghost-close2.bin",
		Offset: 0,
		Data:   []byte("phantom bytes on close 2"),
	}))

	// coord.Close() must trigger the final flush → ordered-halt → onLoss fires.
	closeErr := lossCoord.Close()
	r.Error(closeErr, "Close must return non-nil error when flush fails (not silent)")

	// WalDataLost must be incremented by logDataLost (default hook).
	eventsVal := testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "events"))
	r.Equal(1.0, eventsVal, "WalDataLost events must be exactly 1 (not 0=silent, not 2=double-flush)")
	filesVal := testutil.ToFloat64(m.WalDataLost.WithLabelValues("apply-failure", "files"))
	r.Equal(2.0, filesVal, "WalDataLost files must equal distinct lost-path count (2)")

	// ERROR log must enumerate both ghost paths.
	lossEntries := observed.FilterMessage("WAL data loss: un-flushed ops discarded without reaching the server")
	r.Len(lossEntries.All(), 1, "exactly one WAL data-loss ERROR log must be emitted")
	entry := lossEntries.All()[0]
	r.Equal(zapcore.ErrorLevel, entry.Level)
	rawPaths, ok := entry.ContextMap()["lost_paths"]
	r.True(ok, "log entry must contain a 'lost_paths' field")
	ifacePaths, ok := rawPaths.([]interface{})
	r.True(ok, "lost_paths must be a []interface{} (zap.Strings encoding)")
	var loggedPaths []string
	for _, v := range ifacePaths {
		str, isStr := v.(string)
		r.True(isStr, "each element of lost_paths must be a string")
		loggedPaths = append(loggedPaths, str)
	}
	r.Contains(loggedPaths, "ghost-close1.bin")
	r.Contains(loggedPaths, "ghost-close2.bin")

	// Second Close is safe no-op (log already closed).
	_ = lossCoord.Close()
}

// ─── Scenario 8: Startup overlay rebuild (dead-process recovery) ─────────────
// After a process crash with un-acked WAL ops, opening the same wal.db on a
// fresh coordinator and calling RebuildOverlay must:
//   (a) populate the overlay immediately (RYOW before Replay),
//   (b) leave the log intact (RebuildOverlay is read-only).
// After Replay(ctx, 0) the ops reach the server (data durability).

func (s *WalE2ESuite) TestStartupRecovery_RebuildAndReplay() {
	r := s.Require()
	ctx := bg()

	tc, _, srcDir, volName := s.newScenarioCtx()
	srcSub := filepath.Join(srcDir, "recovery")
	r.NoError(os.Mkdir(srcSub, 0o755))

	cl, err := tc.NewClientAs("user", "pass")
	r.NoError(err)
	defer cl.Close()

	// ── Phase 1: seed un-acked ops in wal.db (simulate a crash) ────────────────
	// Record ops into a log WITHOUT calling coord.Close() — Close flushes pending
	// ops, which would empty the log and undermine the recovery scenario.
	walPath := filepath.Join(s.T().TempDir(), "recovery.db")
	seedLog, err := wal.Open(walPath)
	r.NoError(err, "open seed log")

	seedMgr := clientdelegation.NewManager(&trackingInvalidator{})
	seedOverlay := wal.NewOverlay()
	seedCoord := wal.NewCoordinator(seedMgr, seedLog, seedOverlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
	)

	// Record a Mkdir and a Create+Write — replayable ops with a parent that
	// exists on the server (srcSub = srcDir/recovery, already created above).
	r.NoError(seedCoord.RecordOp(wal.Op{Kind: wal.OpMkdir, Path: "recovery/crash-dir", Mode: 0o40755}))
	r.NoError(seedCoord.RecordOp(wal.Op{Kind: wal.OpCreate, Path: "recovery/crash-dir/data.txt", Mode: 0o644}))
	r.NoError(seedCoord.RecordOp(wal.Op{Kind: wal.OpWrite, Path: "recovery/crash-dir/data.txt", Offset: 0, Data: []byte("survivor")}))

	// Simulate crash: close ONLY the log (release bbolt file lock), NOT the coordinator.
	// coord.Close() would flush the ops — we want them to remain un-acked in wal.db.
	r.NoError(seedLog.Close())

	// ── Phase 2: fresh process mounts — opens same wal.db ──────────────────────
	freshLog, err := wal.Open(walPath)
	r.NoError(err, "open fresh log on same file")

	freshOverlay := wal.NewOverlay()
	freshMgr := clientdelegation.NewManager(&trackingInvalidator{})
	freshCoord := wal.NewCoordinator(freshMgr, freshLog, freshOverlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
	)

	// ── Phase 3(a): RebuildOverlay — RYOW correct BEFORE Replay ────────────────
	r.NoError(freshCoord.RebuildOverlay(), "RebuildOverlay must succeed")

	// Assert (a): overlay reflects the seeded ops immediately, before any server flush.
	r.True(freshOverlay.Has("recovery/crash-dir"), "overlay must have crashed mkdir")
	r.True(freshOverlay.Has("recovery/crash-dir/data.txt"), "overlay must have crashed create")
	merged := freshOverlay.ReadMerge("recovery/crash-dir/data.txt", 0, nil)
	r.Equal([]byte("survivor"), merged, "overlay ReadMerge must return the crashed write bytes")

	// Server must NOT have the directory yet — ops are still only in the overlay.
	_, statErr := os.Stat(filepath.Join(srcSub, "crash-dir"))
	r.True(os.IsNotExist(statErr), "server must not have crash-dir before Replay")

	// ── Phase 3(b): Replay — sends ops to server (data durability) ──────────────
	r.NoError(freshCoord.Replay(ctx, 0), "Replay must succeed")

	// Assert (b): data reached the server.
	serverBytes, readErr := os.ReadFile(filepath.Join(srcSub, "crash-dir/data.txt"))
	r.NoError(readErr, "server must have data.txt after Replay")
	r.Equal([]byte("survivor"), serverBytes, "server bytes must match the crashed write")

	// Cleanup: close the fresh coordinator (which closes the log).
	r.NoError(freshCoord.Close())
}

// ─── Scenario 9: Empty WAL — RebuildOverlay is a no-op ───────────────────────
// On a fresh mount with no leftover wal.db ops, RebuildOverlay must return nil
// and leave the overlay empty. Replay on the empty log must also return nil
// without opening an Apply stream.

func (s *WalE2ESuite) TestStartupRecovery_EmptyWAL_NoOp() {
	r := s.Require()
	ctx := bg()

	tc, _, _, volName := s.newScenarioCtx()

	cl, err := tc.NewClientAs("user", "pass")
	r.NoError(err)
	defer cl.Close()

	walPath := filepath.Join(s.T().TempDir(), "empty.db")
	freshLog, err := wal.Open(walPath)
	r.NoError(err)

	freshOverlay := wal.NewOverlay()
	freshMgr := clientdelegation.NewManager(&trackingInvalidator{})
	freshCoord := wal.NewCoordinator(freshMgr, freshLog, freshOverlay,
		wal.WithApplyFactory(func(ctx context.Context) (proto.RpcFs_ApplyClient, error) {
			return cl.Fs().Apply(ctx)
		}),
		wal.WithVolume(volName),
	)
	defer freshCoord.Close()

	// RebuildOverlay on an empty log must be a no-op.
	r.NoError(freshCoord.RebuildOverlay(), "RebuildOverlay on empty WAL must return nil")

	// Overlay must be empty: no paths were seeded.
	r.False(freshOverlay.Has("any/path"), "overlay must be empty after no-op rebuild")

	// Replay on an empty log must return nil without error.
	r.NoError(freshCoord.Replay(ctx, 0), "Replay on empty WAL must return nil")
}

// ─── test entry-point ─────────────────────────────────────────────────────────

func TestWalE2ESuite(t *testing.T) {
	// Guard: do not run FUSE-dependent tests in environments without /dev/fuse.
	// These tests are bufconn-only so no guard is needed — but we do require
	// a writable temp dir (always available in CI and locally).
	suite.Run(t, new(WalE2ESuite))
}

// ─── compile-time interface assertion ────────────────────────────────────────

var _ watermark.Store = (*memWatermarkStore)(nil)

// ─── verify Apply method full path (documentation) ───────────────────────────

// ApplyFullMethod is the gRPC full method name for Apply (client-streaming).
// Referenced here to catch proto renames at compile time.
const applyFullMethod = proto.RpcFs_Apply_FullMethodName // "/gmountie.RpcFs/Apply"

// Suppress unused import warning.
var _ = status.New(codes.OK, "")
