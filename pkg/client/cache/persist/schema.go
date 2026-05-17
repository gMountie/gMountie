package persist

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// formatVersion is bumped any time the on-disk layout or value gob
// shapes change. Mismatch triggers a wipe (no migration code; the
// project's no-BC stance applies — release notes document the wipe).
const formatVersion uint64 = 1

// Bucket name constants. Sibling files use these directly; external
// packages reach them via typed methods.
var (
	bucketMeta      = []byte("meta")
	bucketAttr      = []byte("attr")
	bucketDir       = []byte("dir")
	bucketDataIdx   = []byte("data_idx")
	bucketChunkRefs = []byte("chunk_refs")
	bucketLRU       = []byte("lru")
	bucketLRUPos    = []byte("lru_pos")
)

var keyFormatVersion = []byte("format_version")
var keyCreatedAt = []byte("created_at")

// ErrFormatMismatch is returned (in chained context only — Open
// handles it internally by wiping) when an existing meta.db has a
// format_version that doesn't match the running build.
var ErrFormatMismatch = errors.New("cache format_version mismatch")

func ensureSchema(db *bolt.DB) (bool, error) {
	wiped := false
	err := db.Update(func(tx *bolt.Tx) error {
		mb, err := tx.CreateBucketIfNotExists(bucketMeta)
		if err != nil {
			return err
		}
		if v := mb.Get(keyFormatVersion); v != nil {
			got, _ := binary.Uvarint(v)
			if got != formatVersion {
				wiped = true
				return nil
			}
		} else {
			buf := make([]byte, binary.MaxVarintLen64)
			n := binary.PutUvarint(buf, formatVersion)
			if err := mb.Put(keyFormatVersion, buf[:n]); err != nil {
				return err
			}
			tsBuf := make([]byte, 8)
			binary.BigEndian.PutUint64(tsBuf, uint64(time.Now().UnixNano()))
			if err := mb.Put(keyCreatedAt, tsBuf); err != nil {
				return err
			}
		}
		for _, b := range [][]byte{bucketAttr, bucketDir, bucketDataIdx, bucketChunkRefs, bucketLRU, bucketLRUPos} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.Wrap(err, "ensureSchema")
	}
	if wiped {
		if err := wipeAndRecreate(db); err != nil {
			return true, err
		}
	}
	return wiped, nil
}

// wipeAndRecreate drops every bucket (including meta) and rebuilds at
// the current formatVersion. The caller must also wipe chunks/ on
// disk; we surface that via wipeChunksFor(root).
func wipeAndRecreate(db *bolt.DB) error {
	err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketMeta, bucketAttr, bucketDir, bucketDataIdx, bucketChunkRefs, bucketLRU, bucketLRUPos} {
			if tx.Bucket(b) != nil {
				if err := tx.DeleteBucket(b); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "wipe buckets")
	}
	_, err = ensureSchema(db)
	return err
}

// wipeChunksFor removes the entire chunks/ tree under root.
// Called after wipeAndRecreate when format_version changed.
func wipeChunksFor(root string) error {
	chunks := filepath.Join(root, "chunks")
	if err := os.RemoveAll(chunks); err != nil {
		return errors.Wrap(err, "wipe chunks dir")
	}
	return os.MkdirAll(chunks, 0o755)
}
