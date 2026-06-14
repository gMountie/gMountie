package persist_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"
)

// CrashDurabilitySuite covers issue #113's crash-teardown gap. The existing
// busy-unmount regression test (test/e2e/api/delete_durability_test.go, PR
// #116) exercises a CLEAN signalled unmount, where persist.Close() runs a final
// db.Sync(). The open question was a CRASH/SIGKILL teardown that skips Close()
// entirely: meta.db opens NoSync, so a delete only survives such a crash if it
// is fsynced at delete time. These tests pin that "guard #1" — kvDelete fsyncs
// every real invalidation immediately — so a deleted attr/dir entry can never
// resurrect after a crash that never reached Close().
//
// (This proves the durability MECHANISM. It is not a reproduction of the
// original report, which was observed on a build that already had the guard;
// see the issue thread.)
type CrashDurabilitySuite struct {
	suite.Suite
	dir string
}

func (s *CrashDurabilitySuite) SetupTest() { s.dir = s.T().TempDir() }

// TestDeleteAttrBytesFsyncsImmediately asserts a real attr delete fsyncs at
// delete time, not just at Close. The measurement window is one DeleteAttrBytes
// call — orders of magnitude shorter than the 1s background-syncer tick — so an
// increase in the sync count is attributable to the delete's own fsync.
func (s *CrashDurabilitySuite) TestDeleteAttrBytesFsyncsImmediately() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close() //nolint:errcheck

	s.Require().NoError(p.PutAttrBytes("/probe/big.bin", []byte("listing")))

	before := persist.TestingSyncCount(p)
	s.Require().NoError(p.DeleteAttrBytes("/probe/big.bin"))
	s.Assert().Greater(persist.TestingSyncCount(p), before,
		"a real attr delete must fsync immediately so it survives a crash before Close() (#113)")
}

// TestDeleteDirBytesFsyncsImmediately is the same guard for the dir bucket —
// the stale directory listing whose survival drove the reported resurrection.
func (s *CrashDurabilitySuite) TestDeleteDirBytesFsyncsImmediately() {
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	defer p.Close() //nolint:errcheck

	s.Require().NoError(p.PutDirBytes("/parent", []byte("child-listing")))

	before := persist.TestingSyncCount(p)
	s.Require().NoError(p.DeleteDirBytes("/parent"))
	s.Assert().Greater(persist.TestingSyncCount(p), before,
		"a real dir-listing delete must fsync immediately so it survives a crash before Close() (#113)")
}

func TestCrashDurabilitySuite(t *testing.T) {
	suite.Run(t, new(CrashDurabilitySuite))
}
