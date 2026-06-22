package persist

import (
	"bytes"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// kvGet returns (value, true, nil) on hit, (nil, false, nil) on miss.
func (p *Persist) kvGet(bucket []byte, key string) ([]byte, bool, error) {
	var out []byte
	var ok bool
	err := p.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucket).Get([]byte(key))
		if v == nil {
			return nil
		}
		out = make([]byte, len(v))
		copy(out, v)
		ok = true
		return nil
	})
	return out, ok, errors.Wrap(err, "kvGet")
}

func (p *Persist) kvPut(bucket []byte, key string, value []byte) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Put([]byte(key), value)
	})
}

func (p *Persist) kvDelete(bucket []byte, key string) error {
	// Probe first: deleting an absent key still commits + fsyncs a txn
	// that changed nothing. Skip the writable txn when the key is absent.
	_, ok, err := p.kvGet(bucket, key)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := p.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Delete([]byte(key))
	}); err != nil {
		return err
	}
	// Belt-and-suspenders: make this real invalidation durable immediately
	// under NoSync. The probe above confirms the key was present, so the
	// Update just performed a real deletion. Syncing here prevents a stale
	// attr/dir entry from surviving a crash and being served as trusted on
	// the next open when SubscribeEnabled=false (verified at backend.go:52-57:
	// that branch calls markGlobalVerified() at construction, bypassing
	// the unverified-startup revalidation the subscribe path provides).
	return errors.Wrap(p.sync(), "sync after kvDelete")
}

// kvDeletePrefix deletes every key in bucket that is the path `prefix` itself
// or a descendant of it (prefix + "/..."). It deliberately does NOT delete
// sibling keys that merely share a string prefix (e.g. prefix "a" must not
// touch "ab"), so it matches `k == prefix || hasPrefix(k, prefix+"/")`. One
// writable cursor pass; syncs once at the end so a subtree invalidation
// (directory rename / recursive delete) is durable like kvDelete. Returns the
// count deleted so the caller can skip the sync when nothing matched.
func (p *Persist) kvDeletePrefix(bucket []byte, prefix string) error {
	pb := []byte(prefix)
	slash := append(append([]byte{}, pb...), '/')
	deleted := 0
	if err := p.db.Update(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucket).Cursor()
		// Seek to prefix; iterate while the key still shares the prefix bytes
		// (bounds the scan), deleting only exact-or-descendant keys.
		for k, _ := c.Seek(pb); k != nil && bytes.HasPrefix(k, pb); k, _ = c.Next() {
			if bytes.Equal(k, pb) || bytes.HasPrefix(k, slash) {
				if err := c.Delete(); err != nil {
					return err
				}
				deleted++
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if deleted == 0 {
		return nil
	}
	return errors.Wrap(p.sync(), "sync after kvDeletePrefix")
}

// DeleteAttrPrefix / DeleteDirPrefix delete a whole path subtree from the attr
// and dir buckets — used by the cache layer to invalidate descendants on a
// directory rename or recursive delete, which the per-key facades cannot do.
func (p *Persist) DeleteAttrPrefix(prefix string) error { return p.kvDeletePrefix(bucketAttr, prefix) }
func (p *Persist) DeleteDirPrefix(prefix string) error  { return p.kvDeletePrefix(bucketDir, prefix) }

// PutAttrBytes / GetAttrBytes / DeleteAttrBytes: attr bucket facade.
func (p *Persist) PutAttrBytes(key string, value []byte) error {
	return p.kvPut(bucketAttr, key, value)
}
func (p *Persist) GetAttrBytes(key string) ([]byte, bool, error) {
	return p.kvGet(bucketAttr, key)
}
func (p *Persist) DeleteAttrBytes(key string) error { return p.kvDelete(bucketAttr, key) }

// PutDirBytes / GetDirBytes / DeleteDirBytes: dir bucket facade.
func (p *Persist) PutDirBytes(key string, value []byte) error {
	return p.kvPut(bucketDir, key, value)
}
func (p *Persist) GetDirBytes(key string) ([]byte, bool, error) {
	return p.kvGet(bucketDir, key)
}
func (p *Persist) DeleteDirBytes(key string) error { return p.kvDelete(bucketDir, key) }
