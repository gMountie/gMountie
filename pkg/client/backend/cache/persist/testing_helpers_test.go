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

// TestingChunkPath returns the absolute path where hash would live on
// disk. Used by tests that assert presence/absence after refcount or
// sweep operations.
func TestingChunkPath(p *Persist, hash [16]byte) string {
	return p.chunkPath(hash)
}

// TestingRunOrphanSweep runs the orphan sweep synchronously with no
// minimum-age filter, so freshly injected orphan files are removed.
// Use in tests that pre-seed disk state and then assert the sweep
// cleaned it.
func TestingRunOrphanSweep(t *testing.T, p *Persist) {
	t.Helper()
	if err := p.runOrphanSweep(0, nil); err != nil {
		t.Fatalf("orphan sweep: %v", err)
	}
}

// TestingRunOrphanSweepStop runs the orphan sweep (minAge 0) with the given
// stop channel so tests can assert it bails cooperatively when signalled.
func TestingRunOrphanSweepStop(t *testing.T, p *Persist, stop <-chan struct{}) error {
	t.Helper()
	return p.runOrphanSweep(0, stop)
}

// TestingRunGhostSweep runs the ghost sweep synchronously at the given
// sample fraction.
func TestingRunGhostSweep(t *testing.T, p *Persist, fraction float64) {
	t.Helper()
	if err := p.runGhostSweep(fraction, nil); err != nil {
		t.Fatalf("ghost sweep: %v", err)
	}
}

// TestingMetaWriteCount returns bbolt's cumulative count of page writes.
// A committed writable transaction increments it — even under NoSync,
// which skips the fsync but still performs the write — while a skipped
// transaction does not. Used to assert no-op invalidations open no
// writable txn.
func TestingMetaWriteCount(p *Persist) int64 {
	s := p.db.Stats().TxStats
	return s.GetWrite()
}

// TestingNoSync reports whether meta.db was opened with fsync-on-commit
// disabled.
func TestingNoSync(p *Persist) bool { return p.db.NoSync }

// TestingSyncCount returns the cumulative number of explicit db.Sync()
// (fsync) calls Persist has made. Lets tests assert that a given op makes
// its mutation durable under NoSync rather than leaving it for Close.
func TestingSyncCount(p *Persist) int64 { return p.syncCount.Load() }

// TestingDiskUsed returns the disk-accountant's current byte total.
func TestingDiskUsed(p *Persist) int64 { return p.disk.Used() }

// TestingRefUnderflows returns the count of decrements observed on an absent
// refcount key (double-decrement / underflow signal).
func TestingRefUnderflows(p *Persist) int64 { return p.refUnderflows.Load() }
