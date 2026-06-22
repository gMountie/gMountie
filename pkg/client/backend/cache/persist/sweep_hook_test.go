package persist

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGhostSweepReValidatesKeyChangedBetweenPhases(t *testing.T) {
	dir := t.TempDir()
	p, err := Open(Options{Root: dir, DisableBackgroundSweeps: true})
	require.NoError(t, err)
	defer p.Close()

	// Key (p,0) -> hashA whose FILE is missing => a ghost at collect time.
	a := []byte("ghost-A")
	ha, _, err := p.WriteChunk(a)
	require.NoError(t, err)
	require.NoError(t, p.PutChunkRef("p", 0, ChunkRef{Hash: ha, Size: uint32(len(a))}))
	require.NoError(t, os.Remove(p.chunkPath(ha)))

	// Between collect and apply, a writer re-references the key with a healthy hashB.
	b := []byte("healthy-B")
	hb, _, err := p.WriteChunk(b)
	require.NoError(t, err)
	testHookAfterCollect = func() {
		testHookAfterCollect = nil // fire once
		require.NoError(t, p.PutChunkRef("p", 0, ChunkRef{Hash: hb, Size: uint32(len(b))}))
	}
	defer func() { testHookAfterCollect = nil }()

	require.NoError(t, p.runGhostSweep(1.0, nil))

	// With the re-Get fix the healthy hashB entry survives and keeps refcount 1.
	got, ok, err := p.GetChunkRef("p", 0)
	require.NoError(t, err)
	require.True(t, ok, "sweep must not delete a key overwritten with a healthy chunk between phases")
	require.Equal(t, hb, got.Hash)
	cnt, err := p.ChunkRefCount(hb)
	require.NoError(t, err)
	require.Equal(t, uint64(1), cnt)
}
