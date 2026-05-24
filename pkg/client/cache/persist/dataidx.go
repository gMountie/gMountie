package persist

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// ChunkRef is the value stored under data_idx[path\x00idx]. Sub-spec D
// will populate Version; Sub-spec C writes zero.
type ChunkRef struct {
	Hash    [16]byte
	Size    uint32
	Version uint64
}

// dataIdxKey encodes (path, chunkIndex) as the bytes path + 0x00 +
// uvarint(idx). Keeps prefix scans by path cheap for invalidation.
func dataIdxKey(path string, chunkIndex int) []byte {
	out := make([]byte, 0, len(path)+1+binary.MaxVarintLen64)
	out = append(out, []byte(path)...)
	out = append(out, 0)
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, uint64(chunkIndex))
	out = append(out, buf[:n]...)
	return out
}

func dataIdxPathPrefix(path string) []byte {
	out := make([]byte, 0, len(path)+1)
	out = append(out, []byte(path)...)
	out = append(out, 0)
	return out
}

// PutChunkRef writes ref under (path, chunkIndex) AND increments the
// refcount for ref.Hash, atomically in one txn. Overwriting an
// existing entry decrements the old ref's count first; if the old
// count reaches zero the prior chunk file is unlinked after the txn.
func (p *Persist) PutChunkRef(path string, chunkIndex int, ref ChunkRef) error {
	var priorHash [16]byte
	var unlinkPrior bool
	err := p.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		key := dataIdxKey(path, chunkIndex)
		if prior := idx.Get(key); prior != nil {
			old, err := decodeChunkRef(prior)
			if err != nil {
				return err
			}
			// Same-hash re-put is a no-op: the (path, idx) entry still
			// points at the same content. dec+inc would temporarily drop
			// the refcount to zero (when this was the last referent),
			// scheduling a post-commit unlinkChunk that then races with
			// the immediately-following inc. The chunk file we're trying
			// to refresh gets deleted out from under us. This path is
			// hit on memory-tier eviction + disk-tier re-promotion of a
			// previously cached chunk (store.get → store.put).
			if old.Hash == ref.Hash {
				return nil
			}
			remaining, err := decRefTx(tx, old.Hash)
			if err != nil {
				return err
			}
			if remaining == 0 {
				priorHash = old.Hash
				unlinkPrior = true
			}
		}
		enc, err := encodeChunkRef(ref)
		if err != nil {
			return err
		}
		if err := idx.Put(key, enc); err != nil {
			return err
		}
		return incRefTx(tx, ref.Hash)
	})
	if err != nil {
		return err
	}
	if unlinkPrior {
		if err := p.unlinkChunk(priorHash); err != nil {
			return err
		}
	}
	return nil
}

// GetChunkRef returns the ref at (path, chunkIndex). ok=false on
// absent (no error).
func (p *Persist) GetChunkRef(path string, chunkIndex int) (ChunkRef, bool, error) {
	var ref ChunkRef
	var ok bool
	err := p.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketDataIdx).Get(dataIdxKey(path, chunkIndex))
		if v == nil {
			return nil
		}
		r, err := decodeChunkRef(v)
		if err != nil {
			return err
		}
		ref = r
		ok = true
		return nil
	})
	return ref, ok, errors.Wrap(err, "GetChunkRef")
}

// InvalidatePathChunks removes every (path, *) entry from data_idx and
// decrements each removed entry's chunk refcount. Hashes whose refcount
// hits zero have their on-disk chunk files unlinked after the txn.
func (p *Persist) InvalidatePathChunks(path string) error {
	// Probe first: skip the writable txn when the path has no cached
	// chunks (same rationale as InvalidateChunkRange).
	var hasAny bool
	if err := p.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketDataIdx).Cursor()
		prefix := dataIdxPathPrefix(path)
		if k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix) {
			hasAny = true
		}
		return nil
	}); err != nil {
		return err
	}
	if !hasAny {
		return nil
	}

	var toUnlink [][16]byte
	err := p.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		c := idx.Cursor()
		prefix := dataIdxPathPrefix(path)
		var keys [][]byte
		var refs []ChunkRef
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			r, err := decodeChunkRef(v)
			if err != nil {
				return err
			}
			ks := make([]byte, len(k))
			copy(ks, k)
			keys = append(keys, ks)
			refs = append(refs, r)
		}
		for i, k := range keys {
			if err := idx.Delete(k); err != nil {
				return err
			}
			remaining, err := decRefTx(tx, refs[i].Hash)
			if err != nil {
				return err
			}
			if remaining == 0 {
				toUnlink = append(toUnlink, refs[i].Hash)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Belt-and-suspenders: make real invalidations durable immediately under
	// NoSync so that a stale chunk cannot survive a crash and be served as a
	// trusted hit on the next open. The no-op probe above guarantees Update
	// only ran because there were entries to delete, so this sync always
	// follows a real removal. The hot write path (no cached chunks) never
	// reaches here; cost is proportional to actual invalidation frequency.
	if err := p.db.Sync(); err != nil {
		return errors.Wrap(err, "sync after InvalidatePathChunks")
	}
	for _, h := range toUnlink {
		if err := p.unlinkChunk(h); err != nil {
			return err
		}
	}
	return nil
}

// InvalidateChunkRange removes data_idx entries for path at chunk
// indices [firstIdx, lastIdx]. Decrements each removed entry's
// refcount and unlinks chunk files whose count hits zero (post-commit).
func (p *Persist) InvalidateChunkRange(path string, firstIdx, lastIdx int) error {
	// Probe first: a writable txn commits (and, absent NoSync, fsyncs)
	// even when it deletes nothing. The hot write path invalidates ranges
	// that were never cached, so skip the txn when no entry exists in range.
	var hasAny bool
	if err := p.db.View(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		for i := firstIdx; i <= lastIdx; i++ {
			if idx.Get(dataIdxKey(path, i)) != nil {
				hasAny = true
				return nil
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if !hasAny {
		return nil
	}

	var toUnlink [][16]byte
	err := p.db.Update(func(tx *bolt.Tx) error {
		idx := tx.Bucket(bucketDataIdx)
		for i := firstIdx; i <= lastIdx; i++ {
			key := dataIdxKey(path, i)
			v := idx.Get(key)
			if v == nil {
				continue
			}
			ref, err := decodeChunkRef(v)
			if err != nil {
				return err
			}
			if err := idx.Delete(key); err != nil {
				return err
			}
			remaining, err := decRefTx(tx, ref.Hash)
			if err != nil {
				return err
			}
			if remaining == 0 {
				toUnlink = append(toUnlink, ref.Hash)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Belt-and-suspenders: make this real invalidation durable immediately
	// under NoSync. The no-op probe above guarantees Update only ran because
	// ≥1 entry existed, so a real removal just committed. See the comment in
	// InvalidatePathChunks for the full rationale.
	if err := p.db.Sync(); err != nil {
		return errors.Wrap(err, "sync after InvalidateChunkRange")
	}
	for _, h := range toUnlink {
		if err := p.unlinkChunk(h); err != nil {
			return err
		}
	}
	return nil
}

func encodeChunkRef(r ChunkRef) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		return nil, errors.Wrap(err, "encode ChunkRef")
	}
	return buf.Bytes(), nil
}

func decodeChunkRef(b []byte) (ChunkRef, error) {
	var r ChunkRef
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&r); err != nil {
		return r, errors.Wrap(err, "decode ChunkRef")
	}
	return r, nil
}
