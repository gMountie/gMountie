package wal_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/backend/wal"
)

// LogSuite is a testify suite covering the WAL Log contract.
type LogSuite struct {
	suite.Suite
	dir string
}

func TestLogSuite(t *testing.T) {
	suite.Run(t, new(LogSuite))
}

func (s *LogSuite) SetupTest() {
	dir, err := os.MkdirTemp("", "wal-test-*")
	s.Require().NoError(err)
	s.dir = dir
}

func (s *LogSuite) TearDownTest() {
	_ = os.RemoveAll(s.dir)
}

func (s *LogSuite) open() *wal.BboltLog {
	l, err := wal.Open(filepath.Join(s.dir, "wal.db"))
	s.Require().NoError(err)
	return l
}

// TestAppendMonotone verifies that Append assigns seq 1, 2, 3 in order.
func (s *LogSuite) TestAppendMonotone() {
	l := s.open()
	defer l.Close()

	op1 := wal.Op{Kind: wal.OpWrite, Path: "/a", Offset: 0, Data: []byte("hello")}
	op2 := wal.Op{Kind: wal.OpSetAttr, Path: "/b", Mode: 0o644}
	op3 := wal.Op{Kind: wal.OpCreate, Path: "/c", Flags: 0o1}

	seq1, err := l.Append(op1)
	s.Require().NoError(err)
	s.Equal(uint64(1), seq1)

	seq2, err := l.Append(op2)
	s.Require().NoError(err)
	s.Equal(uint64(2), seq2)

	seq3, err := l.Append(op3)
	s.Require().NoError(err)
	s.Equal(uint64(3), seq3)
}

// TestReopenContinuesSeq closes and reopens the log; Append must continue from
// the max persisted seq, not restart at 1. This is the crash-recovery property.
func (s *LogSuite) TestReopenContinuesSeq() {
	l := s.open()

	for i := 0; i < 3; i++ {
		_, err := l.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
		s.Require().NoError(err)
	}
	s.Require().NoError(l.Close())

	l2 := s.open()
	defer l2.Close()

	seq4, err := l2.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
	s.Require().NoError(err)
	s.Equal(uint64(4), seq4, "seq must continue from persisted max after reopen")
}

// TestReopenReplayOrder verifies that after reopen, Replay returns persisted
// ops in seq order with their payloads intact.
func (s *LogSuite) TestReopenReplayOrder() {
	l := s.open()

	ops := []wal.Op{
		{Kind: wal.OpWrite, Path: "/f1", Offset: 0, Data: []byte("alpha")},
		{Kind: wal.OpSetAttr, Path: "/f2", Mode: 0o755},
		{Kind: wal.OpCreate, Path: "/f3", Flags: 1},
	}
	for _, op := range ops {
		_, err := l.Append(op)
		s.Require().NoError(err)
	}
	s.Require().NoError(l.Close())

	l2 := s.open()
	defer l2.Close()

	replayed, err := l2.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(replayed, 3)

	for i, r := range replayed {
		s.Equal(uint64(i+1), r.Seq)
		s.Equal(ops[i].Kind, r.Kind)
		s.Equal(ops[i].Path, r.Path)
	}
	s.Equal([]byte("alpha"), replayed[0].Data)
	s.Equal(uint32(0o755), replayed[1].Mode)
}

// TestReplayFromSeq verifies that Replay(fromSeq) returns only ops with seq > fromSeq.
func (s *LogSuite) TestReplayFromSeq() {
	l := s.open()
	defer l.Close()

	for i := 0; i < 5; i++ {
		_, err := l.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
		s.Require().NoError(err)
	}

	// fromSeq=3 → only seq 4, 5
	replayed, err := l.Replay(3)
	s.Require().NoError(err)
	s.Require().Len(replayed, 2)
	s.Equal(uint64(4), replayed[0].Seq)
	s.Equal(uint64(5), replayed[1].Seq)

	// fromSeq=0 → all 5
	all, err := l.Replay(0)
	s.Require().NoError(err)
	s.Len(all, 5)

	// fromSeq=5 → empty
	none, err := l.Replay(5)
	s.Require().NoError(err)
	s.Empty(none)
}

// TestTruncateDropsPrefix verifies that Truncate removes acked ops.
func (s *LogSuite) TestTruncateDropsPrefix() {
	l := s.open()
	defer l.Close()

	for i := 0; i < 5; i++ {
		_, err := l.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
		s.Require().NoError(err)
	}

	// Truncate through seq 3 — seq 1,2,3 are dropped.
	s.Require().NoError(l.Truncate(3))

	remaining, err := l.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(remaining, 2)
	s.Equal(uint64(4), remaining[0].Seq)
	s.Equal(uint64(5), remaining[1].Seq)
}

// TestTruncateAllThenReopenMonotone is the load-bearing regression for the
// full-truncate-then-reopen case: Truncate(max) empties the data bucket, so
// naive last-key recovery would reset nextSeq to 1 — reusing low seqs and
// silently losing ops on the server (deduplicated by watermark ≤ old seq).
func (s *LogSuite) TestTruncateAllThenReopenMonotone() {
	l := s.open()

	for i := 0; i < 3; i++ {
		_, err := l.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
		s.Require().NoError(err)
	}
	// Truncate everything.
	s.Require().NoError(l.Truncate(3))
	s.Require().NoError(l.Close())

	// Reopen: nextSeq must continue above 3.
	l2 := s.open()
	defer l2.Close()

	seq, err := l2.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
	s.Require().NoError(err)
	s.Greater(seq, uint64(3), "seq after full-truncate reopen must exceed the previous high-water")
}

// TestPrefixReturnsLESeqs verifies Prefix(throughSeq) returns ops with seq ≤ throughSeq.
func (s *LogSuite) TestPrefixReturnsLESeqs() {
	l := s.open()
	defer l.Close()

	for i := 0; i < 5; i++ {
		_, err := l.Append(wal.Op{Kind: wal.OpWrite, Path: "/x"})
		s.Require().NoError(err)
	}

	prefix := l.Prefix(3)
	s.Require().Len(prefix, 3)
	s.Equal(uint64(1), prefix[0].Seq)
	s.Equal(uint64(2), prefix[1].Seq)
	s.Equal(uint64(3), prefix[2].Seq)

	// Prefix(0) → empty (no seq ≤ 0 exists since seqs start at 1).
	s.Empty(l.Prefix(0))

	// Prefix beyond max → all ops.
	all := l.Prefix(100)
	s.Len(all, 5)
}

// TestOpRoundTripSetAttrFields verifies that the new SetAttr fields (UID, GID,
// Size, AtimeSec/Nsec, MtimeSec/Nsec, Valid) survive an Append/Replay cycle.
func (s *LogSuite) TestOpRoundTripSetAttrFields() {
	l := s.open()
	defer l.Close()

	op := wal.Op{
		Kind:      wal.OpSetAttr,
		Path:      "/chown",
		Mode:      0o100755,
		Valid:     0b00111111, // all 6 FATTR bits
		UID:       1001,
		GID:       2002,
		Size:      4096,
		AtimeSec:  1000000,
		AtimeNsec: 123456789,
		MtimeSec:  2000000,
		MtimeNsec: 987654321,
	}
	_, err := l.Append(op)
	s.Require().NoError(err)

	replayed, err := l.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(replayed, 1)

	r := replayed[0]
	s.Equal(wal.OpSetAttr, r.Kind)
	s.Equal("/chown", r.Path)
	s.Equal(uint32(0o100755), r.Mode)
	s.Equal(uint32(0b00111111), r.Valid)
	s.Equal(uint32(1001), r.UID)
	s.Equal(uint32(2002), r.GID)
	s.Equal(uint64(4096), r.Size)
	s.Equal(int64(1000000), r.AtimeSec)
	s.Equal(uint32(123456789), r.AtimeNsec)
	s.Equal(int64(2000000), r.MtimeSec)
	s.Equal(uint32(987654321), r.MtimeNsec)
}

// TestOpRoundTripSetXAttr verifies that XattrName/XattrValue/XattrFlags survive
// an Append/Replay cycle for OpSetXAttr.
func (s *LogSuite) TestOpRoundTripSetXAttr() {
	l := s.open()
	defer l.Close()

	op := wal.Op{
		Kind:       wal.OpSetXAttr,
		Path:       "/f",
		XattrName:  "user.color",
		XattrValue: []byte("blue"),
		XattrFlags: 2,
	}
	_, err := l.Append(op)
	s.Require().NoError(err)

	replayed, err := l.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(replayed, 1)

	r := replayed[0]
	s.Equal(wal.OpSetXAttr, r.Kind)
	s.Equal("/f", r.Path)
	s.Equal("user.color", r.XattrName)
	s.Equal([]byte("blue"), r.XattrValue)
	s.Equal(uint32(2), r.XattrFlags)
}

// TestOpRoundTripRemoveXAttr verifies that OpRemoveXAttr survives Append/Replay.
func (s *LogSuite) TestOpRoundTripRemoveXAttr() {
	l := s.open()
	defer l.Close()

	op := wal.Op{
		Kind:      wal.OpRemoveXAttr,
		Path:      "/f",
		XattrName: "user.tag",
	}
	_, err := l.Append(op)
	s.Require().NoError(err)

	replayed, err := l.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(replayed, 1)

	r := replayed[0]
	s.Equal(wal.OpRemoveXAttr, r.Kind)
	s.Equal("/f", r.Path)
	s.Equal("user.tag", r.XattrName)
	s.Nil(r.XattrValue)
}

// TestFlagsNotClobberedByValid verifies that Op.Flags (open-flags for OpCreate)
// and Op.Valid (FATTR_* mask for OpSetAttr) are independent fields and both
// survive a round-trip.
func (s *LogSuite) TestFlagsNotClobberedByValid() {
	l := s.open()
	defer l.Close()

	// OpCreate with open-flags in Flags
	createOp := wal.Op{Kind: wal.OpCreate, Path: "/f", Mode: 0o100644, Flags: 0o102}
	_, err := l.Append(createOp)
	s.Require().NoError(err)

	// OpSetAttr with FATTR_* mask in Valid
	setAttrOp := wal.Op{
		Kind:  wal.OpSetAttr,
		Path:  "/f",
		Mode:  0o100755,
		Valid: 0b000001, // FATTR_MODE
		UID:   0,
	}
	_, err = l.Append(setAttrOp)
	s.Require().NoError(err)

	replayed, err := l.Replay(0)
	s.Require().NoError(err)
	s.Require().Len(replayed, 2)

	s.Equal(uint32(0o102), replayed[0].Flags, "Flags must survive round-trip")
	s.Equal(uint32(0), replayed[0].Valid, "Valid must be zero for create op")
	s.Equal(uint32(0b000001), replayed[1].Valid, "Valid must survive round-trip")
	s.Equal(uint32(0), replayed[1].Flags, "Flags must be zero for setattr op")
}
