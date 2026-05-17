package persist

import (
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
	return p.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Delete([]byte(key))
	})
}

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
