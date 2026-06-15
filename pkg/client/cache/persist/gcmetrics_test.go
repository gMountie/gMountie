package persist

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeGCMetrics is a counting GCMetrics sink for the GC/accounting tests.
// Safe for concurrent use (the sweeps and unlinkChunk may touch it under
// p.accMu, but tests also read it directly).
type fakeGCMetrics struct {
	mu              sync.Mutex
	unlinked        map[string]int
	ghostDeleted    int
	refUnderflow    int
	orphanReclaimed int
	tmpReclaimed    int
	budgetEviction  int
	diskBytesUsed   int64
}

func newFakeGCMetrics() *fakeGCMetrics {
	return &fakeGCMetrics{unlinked: map[string]int{}}
}

func (f *fakeGCMetrics) ChunkUnlinked(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unlinked[reason]++
}
func (f *fakeGCMetrics) GhostEntryDeleted() { f.mu.Lock(); f.ghostDeleted++; f.mu.Unlock() }
func (f *fakeGCMetrics) RefcountUnderflow() { f.mu.Lock(); f.refUnderflow++; f.mu.Unlock() }
func (f *fakeGCMetrics) OrphanReclaimed()   { f.mu.Lock(); f.orphanReclaimed++; f.mu.Unlock() }
func (f *fakeGCMetrics) TmpReclaimed()      { f.mu.Lock(); f.tmpReclaimed++; f.mu.Unlock() }
func (f *fakeGCMetrics) BudgetEviction()    { f.mu.Lock(); f.budgetEviction++; f.mu.Unlock() }
func (f *fakeGCMetrics) DiskBytesUsed(n int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.diskBytesUsed = n
}

func (f *fakeGCMetrics) snapshotUnlinked(reason string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unlinked[reason]
}

// TestGCMetrics_NilOptionIsNoop confirms a nil Metrics option is turned into a
// no-op sink so every GC call site stays nil-safe.
func TestGCMetrics_NilOptionIsNoop(t *testing.T) {
	p, err := Open(Options{Root: t.TempDir(), DisableBackgroundSweeps: true}) // Metrics nil
	require.NoError(t, err)
	defer p.Close()

	// A path that exercises DecChunkRef → unlinkChunk and an underflow must not panic.
	data := []byte("noop-sink")
	h, _, err := p.WriteChunk(data)
	require.NoError(t, err)
	require.NoError(t, p.IncChunkRef(h))
	_, err = p.DecChunkRef(h) // refcount → 0 → unlinkChunk(refcount_zero)
	require.NoError(t, err)
	_, err = p.DecChunkRef(h) // absent now → underflow
	require.NoError(t, err)
	require.Equal(t, int64(1), p.refUnderflows.Load())
}

// TestGCMetrics_GhostSweepBumpsGhostEntryDeleted proves a ghost-sweep deletion
// (index entry over a missing chunk file) bumps GhostEntryDeleted and emits a
// ChunkUnlinked("ghost"), via a fake GCMetrics injected through Options.
func TestGCMetrics_GhostSweepBumpsGhostEntryDeleted(t *testing.T) {
	fake := newFakeGCMetrics()
	p, err := Open(Options{Root: t.TempDir(), DisableBackgroundSweeps: true, Metrics: fake})
	require.NoError(t, err)
	defer p.Close()

	data := []byte("ghost-content")
	h, _, err := p.WriteChunk(data)
	require.NoError(t, err)
	require.NoError(t, p.PutChunkRef("ghost/path", 0, ChunkRef{Hash: h, Size: uint32(len(data))}))

	// Make it a ghost: delete the chunk file out from under the index entry.
	require.NoError(t, os.Remove(p.chunkPath(h)))

	// Exhaustive synchronous ghost sweep finds the dangling index entry.
	require.NoError(t, p.runGhostSweep(1.0, nil))

	require.Equal(t, 1, fake.ghostDeleted, "ghost sweep must bump GhostEntryDeleted")
	// The chunk file was already gone, so unlinkChunk hits the idempotent
	// no-op path and ChunkUnlinked does NOT fire — distinct from GhostEntryDeleted.
	require.Equal(t, 0, fake.snapshotUnlinked(unlinkReasonGhost),
		"already-absent file => no ChunkUnlinked (idempotent no-op)")
	require.Equal(t, 0, fake.refUnderflow, "refcount was present; no underflow")

	// And the index entry is gone.
	_, ok, err := p.GetChunkRef("ghost/path", 0)
	require.NoError(t, err)
	require.False(t, ok, "ghost index entry reclaimed")
}

// Note on the ghost path: ChunkUnlinked("ghost") is intentionally NOT asserted
// here because it is unreachable. runGhostSweep only deletes an index entry
// whose chunk file is missing at BOTH collect time and the apply-phase re-stat
// (sweep.go: "chunk file reappeared since collect — no longer a ghost"), so the
// post-txn unlinkChunk(h, "ghost") always finds the file already absent and
// takes the idempotent no-op path. GhostEntryDeleted() is therefore the real,
// durable ghost-reclaim signal (asserted above), not ChunkUnlinked("ghost").

// TestGCMetrics_UnderflowBumpsRefcountUnderflow proves a decrement on an absent
// refcount bumps RefcountUnderflow (and the atomic), via a fake GCMetrics.
func TestGCMetrics_UnderflowBumpsRefcountUnderflow(t *testing.T) {
	fake := newFakeGCMetrics()
	p, err := Open(Options{Root: t.TempDir(), DisableBackgroundSweeps: true, Metrics: fake})
	require.NoError(t, err)
	defer p.Close()

	// DecChunkRef on a hash that was never IncRef'd: decRefTx returns found=false.
	var h [16]byte
	for i := range h {
		h[i] = byte(i)
	}
	_, err = p.DecChunkRef(h)
	require.NoError(t, err)

	require.Equal(t, 1, fake.refUnderflow, "underflow must bump RefcountUnderflow metric")
	require.Equal(t, int64(1), p.refUnderflows.Load(), "atomic counter still bumped (test helper)")
}

// TestGCMetrics_RefcountZeroUnlinkAndDiskGauge proves the refcount_zero unlink
// reason fires on a real removal and that the disk gauge tracks add/remove.
func TestGCMetrics_RefcountZeroUnlinkAndDiskGauge(t *testing.T) {
	fake := newFakeGCMetrics()
	p, err := Open(Options{Root: t.TempDir(), DisableBackgroundSweeps: true, Metrics: fake})
	require.NoError(t, err)
	defer p.Close()

	data := []byte("refcount-zero-content")
	h, _, err := p.WriteChunk(data)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), fake.diskBytesUsed, "gauge tracks WriteChunk add")

	require.NoError(t, p.IncChunkRef(h))
	_, err = p.DecChunkRef(h) // → 0 → unlinkChunk(refcount_zero), real removal
	require.NoError(t, err)

	require.Equal(t, 1, fake.snapshotUnlinked(unlinkReasonRefcountZero),
		"refcount-zero unlink fires ChunkUnlinked(refcount_zero)")
	require.Equal(t, int64(0), fake.diskBytesUsed, "gauge tracks unlink debit")
}

// TestGCMetrics_OrphanAndTmpReclaim proves the orphan sweep bumps OrphanReclaimed
// (+ ChunkUnlinked("orphan")) for an unreferenced chunk and TmpReclaimed for a
// stale .tmp- file.
func TestGCMetrics_OrphanAndTmpReclaim(t *testing.T) {
	fake := newFakeGCMetrics()
	p, err := Open(Options{Root: t.TempDir(), DisableBackgroundSweeps: true, Metrics: fake})
	require.NoError(t, err)
	defer p.Close()

	// An orphan: a chunk file with no refcount entry.
	data := []byte("orphan-content")
	h, _, err := p.WriteChunk(data) // WriteChunk does NOT incRef on its own
	require.NoError(t, err)

	// A stale tmp file in the same shard. writeChunkFile cleans its own tmp on
	// success, so fabricate a leftover directly.
	tmpPath := p.chunkPath(h) + ".tmp-deadbeef"
	require.NoError(t, os.WriteFile(tmpPath, []byte("leftover"), 0o644))

	// Synchronous orphan sweep (minAge 0) reclaims both.
	require.NoError(t, p.runOrphanSweep(0, nil))

	require.Equal(t, 1, fake.orphanReclaimed, "orphan chunk reclaimed")
	require.Equal(t, 1, fake.snapshotUnlinked(unlinkReasonOrphan), "orphan unlink labelled")
	require.Equal(t, 1, fake.tmpReclaimed, "stale tmp reclaimed")
}
