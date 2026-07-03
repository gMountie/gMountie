package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	bolt "go.etcd.io/bbolt"
)

// CorruptEntrySuite tests WAL handling of corrupt (unmarshal-failing) entries.
type CorruptEntrySuite struct {
	suite.Suite
	dir string
}

func TestCorruptEntrySuite(t *testing.T) {
	suite.Run(t, new(CorruptEntrySuite))
}

func (s *CorruptEntrySuite) SetupTest() {
	dir, err := os.MkdirTemp("", "wal-corrupt-test-*")
	s.Require().NoError(err)
	s.dir = dir
}

func (s *CorruptEntrySuite) TearDownTest() {
	_ = os.RemoveAll(s.dir)
}

func (s *CorruptEntrySuite) openLog() *BboltLog {
	l, err := Open(filepath.Join(s.dir, "wal.db"))
	s.Require().NoError(err)
	return l
}

// TestCorruptEntryIsSkippedLoudlyNotFatal verifies that Replay and Prefix skip
// corrupt entries with a loud ERROR log instead of wedging or silently dropping.
func (s *CorruptEntrySuite) TestCorruptEntryIsSkippedLoudlyNotFatal() {
	l := s.openLog()
	defer l.Close()

	_, err := l.Append(Op{Kind: OpCreate, Path: "a"})
	s.Require().NoError(err)
	_, err = l.Append(Op{Kind: OpCreate, Path: "b"})
	s.Require().NoError(err)

	// Corrupt entry seq=1 in place by overwriting with invalid JSON.
	s.NoError(l.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOps).Put(seqKey(1), []byte("{not json"))
	}))

	// Replay must not error on the corrupt entry; it should skip it and return seq=2.
	ops, err := l.Replay(0)
	s.Require().NoError(err, "a single corrupt entry must not brick the whole WAL")
	s.Require().Len(ops, 1, "only seq=2 should be returned")
	s.Equal("b", ops[0].Path)

	// Prefix must also skip the corrupt entry (not return it, with a loud ERROR log).
	pre := l.Prefix(10)
	s.Len(pre, 1, "Prefix must skip corrupt entry and return only seq=2")
	s.Equal("b", pre[0].Path)
}
