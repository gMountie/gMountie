package persist

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPersistCrossFileDivergenceIsBenign proves the hypothesised persist-layer
// cross-file divergence (#1) is REACHABLE through the real code, then proves it
// is BENIGN (transient miss that self-heals), never SEVERE (wrong data served as
// a trusted hit).
//
// The bad interleave, driven deterministically via the testHookBeforeUnlink seam:
//
//	A: InvalidatePathChunks(P1) — P1 is the sole referent of H. decRefTx(H)→0
//	   commits inside A's Update txn (refcount.go:89-90). A is now in the
//	   post-commit window, holding H in its toUnlink slice, about to delete the
//	   chunk file.
//	B (fires inside the seam, BEFORE A's unlink): WriteChunk(H) sees the file
//	   still present → dedup short-circuit, no rewrite (chunks.go:38-41); then
//	   PutChunkRef(P2,_,H) incRefs H→1 and commits (dataidx.go:87).
//	A: resumes, unlinkChunk(H) deletes the chunk file.
//
// End state: a committed data_idx entry P2→H with refcount 1, over a DELETED
// chunk file. This is exactly the divergence the hypothesis describes. We assert
// it exists (reachability), then assert it self-heals on read (benign).
func TestPersistCrossFileDivergenceIsBenign(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(Options{Root: dir, DisableBackgroundSweeps: true})
	require.NoError(t, err)
	defer p.Close()
	defer func() { testHookBeforeUnlink = nil }()

	// Identical bytes for two distinct paths => same content-addressed hash H.
	data := []byte("shared-content-for-P1-and-P2")

	// P1 is the SOLE referent of H. WriteChunk creates the file; PutChunkRef
	// incRefs H to 1.
	h, _, err := p.WriteChunk(data)
	require.NoError(t, err)
	require.NoError(t, p.PutChunkRef("P1", 0, ChunkRef{Hash: h, Size: uint32(len(data))}))

	cnt, err := p.ChunkRefCount(h)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cnt, "precondition: H referenced once (by P1)")
	_, statErr := os.Stat(p.chunkPath(h))
	require.NoError(t, statErr, "precondition: chunk file present")

	// Arm the seam: B's write lands AFTER A's decRef-to-zero committed+synced
	// but BEFORE A's post-commit unlinkChunk. Fires once.
	fired := false
	testHookBeforeUnlink = func() {
		if fired {
			return
		}
		fired = true
		// B writes the same bytes: WriteChunk hits the dedup short-circuit
		// (file still present here), no rewrite.
		hb, dedup, werr := p.WriteChunk(data)
		require.NoError(t, werr)
		require.Equal(t, h, hb, "same bytes => same hash")
		require.True(t, dedup, "file still present => dedup short-circuit (no rewrite)")
		// B references H for P2: incRefs H 0->1 and commits.
		require.NoError(t, p.PutChunkRef("P2", 0, ChunkRef{Hash: h, Size: uint32(len(data))}))
	}

	// A invalidates P1 — its sole referent. decRef H->0, then the seam runs B,
	// then A unlinks the (now refcount-1-for-P2) chunk file.
	require.NoError(t, p.InvalidatePathChunks("P1"))
	require.True(t, fired, "seam must have fired (proves the window was entered)")

	// --- REACHABILITY: the divergent end-state exists ---
	ref, ok, err := p.GetChunkRef("P2", 0)
	require.NoError(t, err)
	require.True(t, ok, "P2's index entry is committed")
	require.Equal(t, h, ref.Hash)

	cnt, err = p.ChunkRefCount(h)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cnt, "refcount for H is 1 (P2 references it)")

	_, statErr = os.Stat(p.chunkPath(h))
	require.True(t, os.IsNotExist(statErr),
		"DIVERGENCE: committed refcount-1 index entry P2->H over a DELETED chunk file")

	require.Equal(t, int64(0), p.refUnderflows.Load(),
		"the bookkeeping is internally consistent — no underflow; only the file is gone")

	// --- HARM ASSESSMENT: this is BENIGN, not SEVERE ---
	// The loader (cache/data.go:142-145) maps a failed ReadChunk to a MISS, never
	// to a trusted hit. Prove ReadChunk fails here.
	_, rerr := p.ReadChunk(ref.Hash)
	require.Error(t, rerr, "ReadChunk over the deleted file fails => loader returns a miss, NOT wrong data")

	// Content-addressed invariant guarantees no wrong-data hit is even possible:
	// chunkPath(H) is only ever written with bytes that hash to H. So the file is
	// either absent (this case => miss) or correct. The loader's length-check
	// (data.go:146-151) closes the only other gap (torn write). There is no
	// reachable state where this race yields (data, len, true) with wrong bytes.

	// --- SELF-HEALING: the next read refetches and repairs ---
	// Simulate the refetch the loader triggers on a miss: server returns the
	// bytes, write-through re-stores them.
	hh, _, err := p.WriteChunk(data)
	require.NoError(t, err)
	require.Equal(t, h, hh)
	require.NoError(t, p.PutChunkRef("P2", 0, ChunkRef{Hash: h, Size: uint32(len(data))}))

	// The chunk file is restored, bytes are correct.
	restored, err := p.ReadChunk(h)
	require.NoError(t, err, "chunk file recreated by refetch write-through")
	require.Equal(t, data, restored, "served bytes are correct after heal")

	// The same-hash re-put is a no-op (dataidx.go:65-67), so the refetch's
	// PutChunkRef does NOT double-increment: refcount stays 1.
	cnt, err = p.ChunkRefCount(h)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cnt, "same-hash re-put is a no-op; refcount stays 1 (no leak)")

	require.Equal(t, int64(0), p.refUnderflows.Load(), "no underflow across the whole sequence")
}
