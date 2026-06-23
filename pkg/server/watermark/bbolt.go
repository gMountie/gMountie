package watermark

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

var bucketWM = []byte("wm")

type bboltStore struct {
	db *bolt.DB
}

// OpenBBolt opens (or creates) a bbolt-backed WatermarkStore at path.
// The database is opened WITHOUT NoSync so that every db.Update commits
// durably (msync+fsync) before returning — this is the persist-before-ack
// invariant required by Advance and RevokeGen.
func OpenBBolt(path string) (Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, errors.Wrap(err, "open watermark bbolt")
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucketWM)
		return e
	}); err != nil {
		_ = db.Close()
		return nil, errors.Wrap(err, "create wm bucket")
	}
	return &bboltStore{db: db}, nil
}

func keyBytes(k Key) []byte {
	b := make([]byte, len(k.Identity)+1+len(k.Volume))
	n := copy(b, k.Identity)
	b[n] = 0x00
	copy(b[n+1:], k.Volume)
	return b
}

// getRecord reads the Record for k inside an existing transaction.
// Returns a zero Record if the key is absent.
func getRecord(tx *bolt.Tx, k Key) (Record, error) {
	v := tx.Bucket(bucketWM).Get(keyBytes(k))
	if v == nil {
		return Record{}, nil
	}
	var r Record
	if err := json.Unmarshal(v, &r); err != nil {
		return Record{}, errors.Wrap(err, "unmarshal watermark record")
	}
	return r, nil
}

// putRecord encodes and stores r for k inside an existing transaction.
func putRecord(tx *bolt.Tx, k Key, r Record) error {
	v, err := json.Marshal(r)
	if err != nil {
		return errors.Wrap(err, "marshal watermark record")
	}
	return tx.Bucket(bucketWM).Put(keyBytes(k), v)
}

// Get returns the Record for k. Missing key returns a zero Record and nil error.
func (s *bboltStore) Get(k Key) (Record, error) {
	var r Record
	err := s.db.View(func(tx *bolt.Tx) error {
		var e error
		r, e = getRecord(tx, k)
		return e
	})
	return r, errors.Wrap(err, "Get watermark")
}

// Advance raises the watermark to max(current, watermark). The update is
// committed durably (bolt's default sync-on-commit, no NoSync) before return.
func (s *bboltStore) Advance(k Key, watermark uint64) error {
	return errors.Wrap(s.db.Update(func(tx *bolt.Tx) error {
		r, err := getRecord(tx, k)
		if err != nil {
			return err
		}
		if watermark <= r.Watermark {
			return nil // monotone: lower value ignored
		}
		r.Watermark = watermark
		return putRecord(tx, k, r)
	}), "Advance watermark")
}

// RevokeGen adds gen to the revoked-generation set (idempotent — no duplicates).
// The update is committed durably before return.
func (s *bboltStore) RevokeGen(k Key, gen uint64) error {
	return errors.Wrap(s.db.Update(func(tx *bolt.Tx) error {
		r, err := getRecord(tx, k)
		if err != nil {
			return err
		}
		for _, g := range r.RevokedGens {
			if g == gen {
				return nil // already present — idempotent
			}
		}
		r.RevokedGens = append(r.RevokedGens, gen)
		return putRecord(tx, k, r)
	}), "RevokeGen watermark")
}

// NextGen atomically increments GenHi for k and returns the new value.
// The update is committed durably before return.  gen=0 is never returned
// (GenHi starts at 0; first call returns 1).
func (s *bboltStore) NextGen(k Key) (uint64, error) {
	var next uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		r, err := getRecord(tx, k)
		if err != nil {
			return err
		}
		r.GenHi++
		next = r.GenHi
		return putRecord(tx, k, r)
	})
	if err != nil {
		return 0, errors.Wrap(err, "NextGen watermark")
	}
	return next, nil
}

// Close closes the underlying bbolt database.
func (s *bboltStore) Close() error {
	return errors.Wrap(s.db.Close(), "close watermark bbolt")
}
