// Package wal provides a durable, ordered, sequence-numbered op-log per
// (identity, volume). Append assigns the next monotone seq, persists durably
// before returning, and survives process restarts — the seq continues above the
// highest seq seen before the crash. Replay/Truncate/Prefix support WAL-replay
// and prefix-ack workflows.
//
// The backing store is an embedded bbolt database. One bucket ("ops") maps
// big-endian uint64 seq → JSON-encoded Op. A second internal bucket ("meta")
// persists the high-water seq so that a full-truncate-then-reopen cannot reset
// the sequence counter back to 1 and silently reuse low seqs.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// OpKind identifies the filesystem operation recorded in the WAL.
type OpKind uint8

const (
	OpWrite       OpKind = 1
	OpCreate      OpKind = 2
	OpSetAttr     OpKind = 3
	OpMkdir       OpKind = 4
	OpRename      OpKind = 5
	OpUnlink      OpKind = 6
	OpRmdir       OpKind = 7
	OpSymlink     OpKind = 8
	OpSetXAttr    OpKind = 9
	OpRemoveXAttr OpKind = 10
)

// Op is a single WAL entry. Seq is assigned by Append and must not be set by
// callers — Append overwrites it with the assigned monotone sequence number.
//
// SetAttr fields: Valid carries the FATTR_* bitmask (backend.FATTR_MODE,
// FATTR_UID, FATTR_GID, FATTR_SIZE, FATTR_ATIME, FATTR_MTIME). Only fields
// whose FATTR_* bit is set in Valid are meaningful; the rest carry zero.
// Flags is reserved for open-flags on OpCreate (kept separate from the
// FATTR_* valid mask to avoid conflation).
//
// XAttr fields: XattrName/XattrValue/XattrFlags are used by OpSetXAttr and
// OpRemoveXAttr. For OpRemoveXAttr, XattrValue is nil; XattrFlags is unused.
//
// These fields are JSON-encoded in bbolt (local only). No wire change, no proto
// regen — they round-trip through the existing Op encoding purely as local state.
type Op struct {
	Seq     uint64
	Gen     uint64
	Kind    OpKind
	Path    string
	NewPath string
	Offset  int64
	Data    []byte
	Mode    uint32
	Flags   uint32 // open-flags for OpCreate; NOT the FATTR_* mask

	// SetAttr valid-bitmask and per-field values (FATTR_* bits from backend).
	Valid     uint32
	UID       uint32
	GID       uint32
	Size      uint64
	AtimeSec  int64
	AtimeNsec uint32
	MtimeSec  int64
	MtimeNsec uint32

	// XAttr fields for OpSetXAttr / OpRemoveXAttr.
	XattrName  string
	XattrValue []byte
	XattrFlags uint32
}

var (
	bucketOps  = []byte("ops")
	bucketMeta = []byte("meta")
	keyHiSeq   = []byte("hi_seq")
	keyEpoch   = []byte("wal_epoch")
)

// BboltLog is a bbolt-backed WAL Log. Open creates or reopens one.
// BboltLog is safe for concurrent use; bbolt's single-writer serialization
// guarantees gap-free monotone seqs without an additional mutex.
type BboltLog struct {
	db    *bolt.DB
	epoch string // stable per-wal.db UUID, minted on first Open
}

// Epoch returns the stable per-wal.db UUID minted on first Open. The client
// stamps it on every WalOp and DelegationRequest so the server namespaces the
// dedup watermark and the revoked-gen fence per epoch — a fresh wal.db gets a
// fresh epoch and is never dedup-skipped against a prior epoch's watermark.
func (l *BboltLog) Epoch() string { return l.epoch }

// Open opens (or creates) a bbolt-backed WAL log at the given path.
// The database is opened WITHOUT NoSync so that every db.Update commits
// durably (msync+fsync) before returning — the persist-before-ack invariant.
func Open(path string) (*BboltLog, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, errors.Wrap(err, "open wal bbolt")
	}
	var epoch string
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(bucketOps); e != nil {
			return e
		}
		mb, e := tx.CreateBucketIfNotExists(bucketMeta)
		if e != nil {
			return e
		}
		// Mint a stable per-wal.db epoch on first open; reuse it forever after.
		// A fresh wal.db (new path / wiped cache) gets a fresh epoch so its seq
		// space never collides with a prior epoch's server-side watermark.
		if v := mb.Get(keyEpoch); len(v) > 0 {
			epoch = string(v)
			return nil
		}
		epoch = uuid.NewString()
		return mb.Put(keyEpoch, []byte(epoch))
	}); err != nil {
		_ = db.Close()
		return nil, errors.Wrap(err, "create wal buckets")
	}
	return &BboltLog{db: db, epoch: epoch}, nil
}

// Append assigns the next monotone seq to op, persists it durably, and
// returns the assigned seq. The seq is derived inside the writable txn
// (using the max of the last key in ops + the persisted hi_seq watermark),
// so bbolt's single-writer serialization guarantees gap-free monotone seqs
// under concurrent callers without an additional mutex.
func (l *BboltLog) Append(op Op) (uint64, error) {
	var assigned uint64
	err := l.db.Update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bucketOps)
		mb := tx.Bucket(bucketMeta)

		// Compute next seq as max(last-ops-key, persisted-hi_seq) + 1.
		next := uint64(1)
		if k, _ := ob.Cursor().Last(); k != nil {
			next = binary.BigEndian.Uint64(k) + 1
		}
		if hi := loadHiSeq(mb); hi+1 > next {
			next = hi + 1
		}

		op.Seq = next

		v, err := json.Marshal(op)
		if err != nil {
			return errors.Wrap(err, "marshal op")
		}
		if err := ob.Put(seqKey(next), v); err != nil {
			return err
		}
		// Update the persisted high-water seq so a full-truncate cannot
		// reset nextSeq on reopen.
		if err := storeHiSeq(mb, next); err != nil {
			return err
		}
		assigned = next
		return nil
	})
	if err != nil {
		return 0, errors.Wrap(err, "wal Append")
	}
	return assigned, nil
}

// Replay returns all ops with seq > fromSeq, in ascending seq order.
// fromSeq=0 returns all persisted ops.
func (l *BboltLog) Replay(fromSeq uint64) ([]Op, error) {
	var out []Op
	err := l.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketOps).Cursor()
		// Seek past fromSeq.
		var k, v []byte
		if fromSeq == 0 {
			k, v = c.First()
		} else {
			// Seek to fromSeq+1.
			k, v = c.Seek(seqKey(fromSeq + 1))
		}
		for ; k != nil; k, v = c.Next() {
			var op Op
			if err := json.Unmarshal(v, &op); err != nil {
				return errors.Wrap(err, "unmarshal op")
			}
			out = append(out, op)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "wal Replay")
	}
	return out, nil
}

// Truncate deletes all ops with seq ≤ throughSeq (the acked prefix).
// It does NOT reset the high-water seq, so reopening after a full truncate
// continues the seq above the previous maximum.
func (l *BboltLog) Truncate(throughSeq uint64) error {
	return errors.Wrap(l.db.Update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(bucketOps)
		c := ob.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if binary.BigEndian.Uint64(k) > throughSeq {
				break
			}
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	}), "wal Truncate")
}

// Prefix returns all persisted ops with seq ≤ throughSeq, in ascending order.
// Unmarshal errors are silently skipped (the entry is omitted from the result).
func (l *BboltLog) Prefix(throughSeq uint64) []Op {
	var out []Op
	_ = l.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketOps).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if binary.BigEndian.Uint64(k) > throughSeq {
				break
			}
			var op Op
			if err := json.Unmarshal(v, &op); err != nil {
				continue
			}
			out = append(out, op)
		}
		return nil
	})
	return out
}

// Close closes the underlying bbolt database.
func (l *BboltLog) Close() error {
	return errors.Wrap(l.db.Close(), "close wal bbolt")
}

// seqKey encodes seq as a big-endian 8-byte key so bbolt iterates in seq order.
func seqKey(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return b
}

// loadHiSeq reads the persisted hi_seq from the meta bucket. Returns 0 if absent.
func loadHiSeq(mb *bolt.Bucket) uint64 {
	v := mb.Get(keyHiSeq)
	if len(v) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}

// storeHiSeq persists seq as the new hi_seq in the meta bucket.
func storeHiSeq(mb *bolt.Bucket, seq uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return mb.Put(keyHiSeq, b)
}
