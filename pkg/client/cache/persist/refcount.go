package persist

import (
	"encoding/binary"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// IncChunkRef increments the refcount for hash, creating the entry
// at 1 if absent. Public-API form runs its own bbolt txn; internal
// callers that want to compose with other writes use incRefTx.
func (p *Persist) IncChunkRef(hash [16]byte) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		return incRefTx(tx, hash)
	})
}

// DecChunkRef decrements the refcount. If the resulting count is 0,
// the corresponding chunk file is unlinked from disk (after txn
// commit so we don't roll back a successful unlink). Returns the
// post-decrement count.
func (p *Persist) DecChunkRef(hash [16]byte) (uint64, error) {
	var remaining uint64
	err := p.db.Update(func(tx *bolt.Tx) error {
		var (
			found bool
			err   error
		)
		remaining, found, err = decRefTx(tx, hash)
		if err != nil {
			return err
		}
		if !found {
			p.recordRefUnderflow(hash)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	if remaining == 0 {
		if err := p.unlinkChunk(hash, unlinkReasonRefcountZero); err != nil {
			return 0, err
		}
	}
	return remaining, nil
}

// ChunkRefCount reads the current refcount for hash. Returns 0 when
// absent (no error — absence is normal).
func (p *Persist) ChunkRefCount(hash [16]byte) (uint64, error) {
	var count uint64
	err := p.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChunkRefs)
		v := b.Get(hash[:])
		if v == nil {
			return nil
		}
		count, _ = binary.Uvarint(v)
		return nil
	})
	return count, errors.Wrap(err, "ChunkRefCount")
}

// incRefTx is the txn-bound increment, used internally when composing
// refcount changes with index updates.
func incRefTx(tx *bolt.Tx, hash [16]byte) error {
	b := tx.Bucket(bucketChunkRefs)
	cur, _ := binary.Uvarint(b.Get(hash[:]))
	cur++
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, cur)
	return b.Put(hash[:], buf[:n])
}

// decRefTx is the txn-bound decrement. found reports whether the key
// existed: found==false means a decrement landed on an absent refcount
// (double-decrement / underflow) — the caller records it. When the
// surviving count is 0 the entry key is removed from the bucket; the
// on-disk unlink happens outside the txn in DecChunkRef.
func decRefTx(tx *bolt.Tx, hash [16]byte) (remaining uint64, found bool, err error) {
	b := tx.Bucket(bucketChunkRefs)
	v := b.Get(hash[:])
	if v == nil {
		return 0, false, nil // absent: underflow — caller records it
	}
	cur, _ := binary.Uvarint(v)
	if cur <= 1 {
		return 0, true, b.Delete(hash[:])
	}
	cur--
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, cur)
	return cur, true, b.Put(hash[:], buf[:n])
}
