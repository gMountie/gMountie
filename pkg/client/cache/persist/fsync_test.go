package persist_test

import (
	"testing"

	"gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type PersistFsyncSuite struct {
	suite.Suite
	dir string
}

func (s *PersistFsyncSuite) SetupTest() { s.dir = s.T().TempDir() }

// A range invalidation over a path that was never cached must not open a
// writable transaction (which would commit + fsync for nothing).
func (s *PersistFsyncSuite) TestInvalidateChunkRangeNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidateChunkRange("/never/written", 0, 4))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "no-op range invalidation must not open a writable txn")
}

// A range invalidation that actually has an entry must still commit and
// remove it.
func (s *PersistFsyncSuite) TestInvalidateChunkRangeRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	hash, _, err := p.WriteChunk([]byte("hello"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/f", 0, persist.ChunkRef{Hash: hash, Size: 5}))

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidateChunkRange("/f", 0, 0))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "real invalidation must commit a writable txn")

	_, ok, err := p.GetChunkRef("/f", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "entry must be gone after invalidation")
}

func (s *PersistFsyncSuite) TestInvalidatePathChunksNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidatePathChunks("/never/written"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "no-op path invalidation must not open a writable txn")
}

func (s *PersistFsyncSuite) TestInvalidatePathChunksRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	hash, _, err := p.WriteChunk([]byte("hello"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/f", 0, persist.ChunkRef{Hash: hash, Size: 5}))

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.InvalidatePathChunks("/f"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "real path invalidation must commit a writable txn")

	_, ok, err := p.GetChunkRef("/f", 0)
	s.Require().NoError(err)
	s.Assert().False(ok, "entry must be gone after invalidation")
}

func (s *PersistFsyncSuite) TestDeleteAttrBytesNoOpSkipsTxn() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/absent"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Equal(before, after, "deleting an absent attr key must not open a writable txn")
}

func (s *PersistFsyncSuite) TestDeleteAttrBytesRealStillDeletes() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()

	s.Require().NoError(p.PutAttrBytes("/a", []byte("x")))
	before := persist.TestingMetaWriteCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/a"))
	after := persist.TestingMetaWriteCount(p)
	s.Assert().Greater(after, before, "deleting a present key must commit a writable txn")

	_, ok, err := p.GetAttrBytes("/a")
	s.Require().NoError(err)
	s.Assert().False(ok, "key must be gone after delete")
}

func (s *PersistFsyncSuite) TestOpenEnablesNoSync() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close()
	s.Assert().True(persist.TestingNoSync(p), "meta.db should open with NoSync enabled")
}

// Close must stop the syncer and perform a final sync; data written
// before Close must be readable after reopening the same directory.
func (s *PersistFsyncSuite) TestCloseSyncsAndDataSurvivesReopen() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.Require().NoError(p.PutAttrBytes("/a", []byte("durable")))
	s.Require().NoError(p.Close())

	p2, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p2.Close()
	v, ok, err := p2.GetAttrBytes("/a")
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Assert().Equal([]byte("durable"), v)
}

// TestRealInvalidationDurableAfterReopen verifies that a real invalidation
// (attr delete + chunk range delete) is durable: the entries are absent when
// the same directory is reopened via a fresh Open call. This is the
// belt-and-suspenders guarantee that closes the SubscribeEnabled=false
// safety gap: when subscribe is off, NewCachedBackend calls
// markGlobalVerified() at construction (backend.go:52-57), so the validity
// layer's unverified-startup revalidation is bypassed. The only remaining
// defence is that the invalidation itself is fsynced before we return, which
// is what the db.Sync() calls added to kvDelete / InvalidateChunkRange /
// InvalidatePathChunks provide.
//
// Note: this test uses a normal Close→Reopen cycle, not a simulated crash.
// It cannot prove crash-survival directly (that would require OS-level fault
// injection), but it does confirm the durable state is written — a clean
// Close that skipped the sync would pass this test too, but the subsequent
// sync-count assertions in the "Real" tests above confirm the writable txn
// committed and the sync fired within the same Open session.
func (s *PersistFsyncSuite) TestRealInvalidationDurableAfterReopen() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)

	// Write an attr entry and a chunk ref, then invalidate both.
	s.Require().NoError(p.PutAttrBytes("/durability-test", []byte("cached-attrs")))
	hash, _, err := p.WriteChunk([]byte("cached-data"))
	s.Require().NoError(err)
	s.Require().NoError(p.PutChunkRef("/durability-test", 0, persist.ChunkRef{Hash: hash, Size: 11}))

	s.Require().NoError(p.DeleteAttrBytes("/durability-test"))
	s.Require().NoError(p.InvalidateChunkRange("/durability-test", 0, 0))

	// Close (which also syncs) then reopen.
	s.Require().NoError(p.Close())

	p2, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p2.Close()

	_, attrPresent, err := p2.GetAttrBytes("/durability-test")
	s.Require().NoError(err)
	s.Assert().False(attrPresent, "attr invalidation must be absent after reopen")

	_, chunkPresent, err := p2.GetChunkRef("/durability-test", 0)
	s.Require().NoError(err)
	s.Assert().False(chunkPresent, "chunk invalidation must be absent after reopen")
}

func TestPersistFsyncSuite(t *testing.T) { suite.Run(t, new(PersistFsyncSuite)) }

// BenchmarkInvalidateChunkRangeNoOp guards the no-op skip optimisation: a
// range invalidation over a never-cached path must not pay for a bbolt
// commit. A regression (re-introducing the writable txn) shows up as a
// large ns/op jump on slow-fsync storage.
func BenchmarkInvalidateChunkRangeNoOp(b *testing.B) {
	p, err := persist.Open(persist.Options{Root: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	for b.Loop() {
		if err := p.InvalidateChunkRange("/never/written", 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}
