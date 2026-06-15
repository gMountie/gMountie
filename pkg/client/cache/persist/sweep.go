package persist

import (
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
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

// testHookAfterCollect, when non-nil, runs between a sweeper's View-collect and
// its Update-apply phases. Test-only (nil in production) — it lets a test land a
// concurrent overwrite in the exact lost-update window.
var testHookAfterCollect func()

// stopRequested reports whether the background-goroutine stop signal has
// fired. A nil channel never fires (the select falls through to default),
// so callers that don't participate in lifecycle (tests) pass nil.
func stopRequested(stop <-chan struct{}) bool {
	select {
	case <-stop:
		return true
	default:
		return false
	}
}

// runOrphanSweep walks chunks/ and unlinks any file whose hash is not
// present in the chunk_refs bucket. minAge > 0 causes files newer than
// that age to be skipped (safe for background use; pass 0 in tests to
// sweep freshly injected orphans immediately). stop aborts the walk
// cooperatively when Close signals shutdown (nil disables cancellation).
func (p *Persist) runOrphanSweep(minAge time.Duration, stop <-chan struct{}) error {
	cutoff := time.Now().Add(-minAge)
	chunksRoot := filepath.Join(p.root, "chunks")
	return filepath.WalkDir(chunksRoot, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		// Bail promptly on shutdown so Close doesn't block on a long walk
		// and we never touch p.db after it's closed.
		if stopRequested(stop) {
			return filepath.SkipAll
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if idx := strings.Index(name, ".tmp-"); idx == 32 {
			// crash-leftover temp chunk from an interrupted WriteChunk
			if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
				return errors.Wrap(rmErr, "remove stale tmp chunk")
			}
			return nil
		}
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
			// Filename isn't valid hex — not one of our chunk files.
			// Skip silently so unrelated files in chunks/ don't abort
			// the sweep (orphan-cleanup is best-effort).
			return nil //nolint:nilerr // intentional: skip non-gMountie files
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
func (p *Persist) runGhostSweep(sampleFraction float64, stop <-chan struct{}) error {
	if sampleFraction <= 0 || stopRequested(stop) {
		return nil
	}
	var toDelete [][]byte
	var toDecRef [][16]byte
	var stopped bool
	err := p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketDataIdx).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if stopRequested(stop) {
				stopped = true
				return nil
			}
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
	if testHookAfterCollect != nil {
		testHookAfterCollect()
	}
	// Shutdown signalled mid-scan: drop partial results rather than open a
	// writable txn (Close is waiting on us, and db may close next).
	if stopped || len(toDelete) == 0 {
		return nil
	}
	var unlinks [][16]byte
	err = p.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		for i, k := range toDelete {
			cur := idx.Get(k)
			if cur == nil {
				continue // key already deleted by a concurrent writer
			}
			curRef, derr := decodeChunkRef(cur)
			if derr != nil {
				return derr
			}
			if curRef.Hash != toDecRef[i] {
				continue // key was overwritten since collect — not the ghost we saw
			}
			if _, statErr := os.Stat(p.chunkPath(curRef.Hash)); statErr == nil {
				continue // chunk file reappeared since collect — no longer a ghost
			}
			if derr := idx.Delete(k); derr != nil {
				return derr
			}
			remaining, found, derr := decRefTx(tx, curRef.Hash)
			if derr != nil {
				return derr
			}
			if !found {
				p.refUnderflows.Add(1)
			}
			if remaining == 0 {
				unlinks = append(unlinks, curRef.Hash)
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
// is usable during the sweep) — no return. Both goroutines join p.bgWG
// and watch p.stopCh so Close cancels and waits for them before closing
// the db (otherwise they'd touch a closed handle).
func (p *Persist) startBackgroundSweeps() {
	p.bgWG.Add(2)
	go func() { defer p.bgWG.Done(); _ = p.runGhostSweep(0.01, p.stopCh) }()
	go func() { defer p.bgWG.Done(); _ = p.runOrphanSweep(orphanSweepBgAge, p.stopCh) }()
}

// enforceDiskBudget evicts the lexically-oldest data_idx entries until
// disk usage falls under the budget. All entries to evict are collected
// in a single bbolt View walk and then deleted in a single Update
// transaction, avoiding the 2-txn-per-chunk pattern (one View + one
// Update per iteration). After the batch Update, chunk files whose
// refcount hit zero are unlinked — which debits the disk accountant.
//
// FIFO-of-path semantics (lexical ordering, not strict access LRU).
// A future task may add proper access-time ordering via the lru /
// lru_pos buckets that schema.go already provisions.
//
// Terminates when Over()==0 or data_idx is empty (any remaining
// over-budget bytes must be orphan chunks reachable only by the
// background orphan sweep).
func (p *Persist) enforceDiskBudget() error {
	if p.disk.Over() == 0 {
		return nil
	}

	// Collect keys and refs to evict in a single read-only scan. We walk
	// lexically from First() and accumulate entries until the sum of their
	// sizes would cover the over-budget amount. Refcounting may mean some
	// chunks are shared and won't actually be unlinked, causing a second
	// call to enforce the budget on the remainder — that is acceptable.
	overBy := p.disk.Over()
	var toDeleteKeys [][]byte
	var toDeleteRefs []ChunkRef
	var collected int64
	err := p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketDataIdx).Cursor()
		for k, v := c.First(); k != nil && collected < overBy; k, v = c.Next() {
			r, derr := decodeChunkRef(v)
			if derr != nil {
				return derr
			}
			ks := make([]byte, len(k))
			copy(ks, k)
			toDeleteKeys = append(toDeleteKeys, ks)
			toDeleteRefs = append(toDeleteRefs, r)
			collected += int64(r.Size)
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "evict scan")
	}
	if testHookAfterCollect != nil {
		testHookAfterCollect()
	}
	if len(toDeleteKeys) == 0 {
		// data_idx can't free more; remaining over-budget bytes are orphan or
		// crash-leftover tmp chunks. Reclaim them synchronously (age 0) so the
		// budget self-heals instead of wedging and re-scanning on every write.
		return errors.Wrap(p.runOrphanSweep(0, nil), "orphan reclaim under budget pressure")
	}

	var toUnlink [][16]byte
	err = p.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		for i, k := range toDeleteKeys {
			cur := idx.Get(k)
			if cur == nil {
				continue
			}
			curRef, derr := decodeChunkRef(cur)
			if derr != nil {
				return derr
			}
			if curRef.Hash != toDeleteRefs[i].Hash {
				continue // overwritten since collect; don't evict the new chunk
			}
			if derr := idx.Delete(k); derr != nil {
				return derr
			}
			remaining, found, derr := decRefTx(tx, curRef.Hash)
			if derr != nil {
				return derr
			}
			if !found {
				p.refUnderflows.Add(1)
			}
			if remaining == 0 {
				toUnlink = append(toUnlink, curRef.Hash)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "evict delete")
	}
	for _, h := range toUnlink {
		if err := p.unlinkChunk(h); err != nil {
			return err
		}
	}
	return nil
}
