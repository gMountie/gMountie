package persist

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// TestingForceFormatVersion is a test-only helper that rewrites the
// meta bucket's format_version key. Use from external test packages
// when you need to simulate an out-of-date cache directory.
func TestingForceFormatVersion(t *testing.T, dbPath string, version uint64) {
	t.Helper()
	db, err := bolt.Open(dbPath, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open meta.db: %v", err)
	}
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, version)
		return tx.Bucket(bucketMeta).Put(keyFormatVersion, buf[:n])
	})
	if err != nil {
		t.Fatalf("rewrite format_version: %v", err)
	}
}

// TestingHashHex returns the hex form of a chunk hash. For test
// assertion convenience.
func TestingHashHex(hash [16]byte) string { return hex.EncodeToString(hash[:]) }
