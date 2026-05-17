package persist

import (
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// orphanSweepBgAge is the minimum file age used when the orphan sweep
// runs in the background (via startBackgroundSweeps). Fresh chunks
// have no refcount yet because WriteChunk and IncRef/PutChunkRef are
// two separate operations — skipping young files avoids a race where
// the sweep removes a chunk before its refcount is recorded.
const orphanSweepBgAge = 60 * time.Second

// runOrphanSweep walks chunks/ and unlinks any file whose hash is not
// present in the chunk_refs bucket. minAge > 0 causes files newer than
// that age to be skipped (safe for background use; pass 0 in tests to
// sweep freshly injected orphans immediately).
func (p *Persist) runOrphanSweep(minAge time.Duration) error {
	cutoff := time.Now().Add(-minAge)
	chunksRoot := filepath.Join(p.root, "chunks")
	return filepath.WalkDir(chunksRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if len(name) != 32 { // hex of 16 bytes
			return nil
		}
		if minAge > 0 {
			info, err := d.Info()
			if err != nil {
				return err
			}
			if info.ModTime().After(cutoff) {
				return nil // too fresh; IncRef may still be in flight
			}
		}
		raw, err := hex.DecodeString(name)
		if err != nil {
			return nil
		}
		var h [16]byte
		copy(h[:], raw)
		count, err := p.ChunkRefCount(h)
		if err != nil {
			return err
		}
		if count == 0 {
			if err := p.unlinkChunk(h); err != nil {
				return err
			}
		}
		return nil
	})
}

// runGhostSweep samples (path, idx) entries from data_idx; for each,
// it checks the chunk file exists on disk. Missing files mean the
// index entry is a ghost — delete it and decrement the refcount.
// sampleFraction in [0, 1]; 1.0 = exhaustive.
func (p *Persist) runGhostSweep(sampleFraction float64) error {
	if sampleFraction <= 0 {
		return nil
	}
	var toDelete [][]byte
	var toDecRef [][16]byte
	err := p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketDataIdx).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if sampleFraction < 1.0 && rand.Float64() > sampleFraction {
				continue
			}
			ref, err := decodeChunkRef(v)
			if err != nil {
				return err
			}
			if _, err := os.Stat(p.chunkPath(ref.Hash)); os.IsNotExist(err) {
				ks := make([]byte, len(k))
				copy(ks, k)
				toDelete = append(toDelete, ks)
				toDecRef = append(toDecRef, ref.Hash)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "ghost sweep scan")
	}
	if len(toDelete) == 0 {
		return nil
	}
	var unlinks [][16]byte
	err = p.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		for i, k := range toDelete {
			if err := idx.Delete(k); err != nil {
				return err
			}
			remaining, err := decRefTx(tx, toDecRef[i])
			if err != nil {
				return err
			}
			if remaining == 0 {
				unlinks = append(unlinks, toDecRef[i])
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "ghost sweep delete")
	}
	for _, h := range unlinks {
		_ = p.unlinkChunk(h)
	}
	return nil
}

// startBackgroundSweeps kicks off the async orphan sweep + the initial
// sampled ghost sweep. Called from Open. Errors are swallowed (cache
// is usable during the sweep) — no return.
func (p *Persist) startBackgroundSweeps() {
	go func() { _ = p.runGhostSweep(0.01) }()
	go func() { _ = p.runOrphanSweep(orphanSweepBgAge) }()
}
