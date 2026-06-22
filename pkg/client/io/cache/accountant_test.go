package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
)

type AccountantTestSuite struct {
	suite.Suite
}

// TestSingleStoreEvictsLRU verifies that within one store, the LRU
// entry is evicted when the cap is exceeded.
func (s *AccountantTestSuite) TestSingleStoreEvictsLRU() {
	acct := newAccountant(30, 0)
	st := newStore(acct, "attr", metrics.NopRecorder{})
	st.put("a", "va", 10) // inserts: [a]
	st.put("b", "vb", 10) // [b, a]
	st.put("c", "vc", 10) // [c, b, a]; used=30, at cap
	st.put("d", "vd", 10) // [d, c, b]; a evicted
	s.Assert().Nil(st.get("a"))
	s.Assert().NotNil(st.get("b"))
	s.Assert().NotNil(st.get("c"))
	s.Assert().NotNil(st.get("d"))
	s.Assert().Equal(30, acct.Used())
}

// TestTouchProtectsFromEviction verifies that a Get promotes the
// entry to MRU and saves it from imminent eviction.
func (s *AccountantTestSuite) TestTouchProtectsFromEviction() {
	acct := newAccountant(30, 0)
	st := newStore(acct, "attr", metrics.NopRecorder{})
	st.put("a", "va", 10)
	st.put("b", "vb", 10)
	st.put("c", "vc", 10)
	_ = st.get("a")       // promote a; now [a, c, b]
	st.put("d", "vd", 10) // b is LRU and gets evicted
	s.Assert().NotNil(st.get("a"))
	s.Assert().Nil(st.get("b"))
	s.Assert().NotNil(st.get("c"))
	s.Assert().NotNil(st.get("d"))
}

// TestCrossStoreEvictsGloballyLRU verifies that eviction picks the LRU
// across all registered stores, not just the inserting one.
func (s *AccountantTestSuite) TestCrossStoreEvictsGloballyLRU() {
	acct := newAccountant(30, 0)
	stA := newStore(acct, "attr", metrics.NopRecorder{})
	stB := newStore(acct, "dir", metrics.NopRecorder{})
	stA.put("a1", "v", 10) // global LRU
	stB.put("b1", "v", 10)
	stB.put("b2", "v", 10) // used=30; a1 is global LRU
	stB.put("b3", "v", 10) // forces eviction; a1 (from stA) goes
	s.Assert().Nil(stA.get("a1"))
	s.Assert().NotNil(stB.get("b1"))
	s.Assert().NotNil(stB.get("b2"))
	s.Assert().NotNil(stB.get("b3"))
}

// TestZeroBudgetDisablesEviction verifies that budget<=0 means "no cap".
func (s *AccountantTestSuite) TestZeroBudgetDisablesEviction() {
	acct := newAccountant(0, 0)
	st := newStore(acct, "attr", metrics.NopRecorder{})
	for i := 0; i < 1000; i++ {
		st.put(string(rune(i)), "v", 100)
	}
	s.Assert().Equal(100000, acct.Used())
	s.Assert().Equal(1000, st.size())
}

// TestEntryCountCapBoundsMaps is the issue #118 regression guard. Even when
// per-entry BYTE sizes badly under-count (here size=1, far below reality), the
// entry-count cap must bound the number of live map entries so the maps cannot
// grow unboundedly in real heap. With only a byte cap (the old behaviour) all
// 10_000 entries would persist, because used (10_000) never approaches a
// 256 MiB byte budget — exactly the leak this issue reported.
func (s *AccountantTestSuite) TestEntryCountCapBoundsMaps() {
	const maxEntries = 100
	acct := newAccountant(256<<20, maxEntries) // huge byte budget, small count cap
	st := newStore(acct, "attr", metrics.NopRecorder{})
	for i := 0; i < 10_000; i++ {
		st.put(fmt.Sprintf("path-%d", i), "v", 1) // size=1 → the byte cap never fires
	}
	s.Assert().Equal(maxEntries, acct.Len(),
		"entry count must stay at the count cap despite under-counted bytes")
	s.Assert().LessOrEqual(acct.Used(), maxEntries, "accounted bytes track the surviving entries")
}

// TestCountCapZeroDisablesCountEviction confirms the count cap honours the same
// "0 disables" contract as the byte budget: with a positive byte budget but a
// zero count cap, eviction is purely byte-driven.
func (s *AccountantTestSuite) TestCountCapZeroDisablesCountEviction() {
	acct := newAccountant(0, 0) // both disabled
	st := newStore(acct, "attr", metrics.NopRecorder{})
	for i := 0; i < 500; i++ {
		st.put(fmt.Sprintf("k-%d", i), "v", 10)
	}
	s.Assert().Equal(500, acct.Len(), "no count cap → all entries retained")
}

// TestDeriveMaxEntries pins the backstop derivation: disabled at budget 0,
// budget/minEntryFootprintBytes otherwise.
func (s *AccountantTestSuite) TestDeriveMaxEntries() {
	s.Assert().Equal(0, deriveMaxEntries(0), "zero budget disables the count cap")
	s.Assert().Equal(0, deriveMaxEntries(-1), "negative budget disables the count cap")
	s.Assert().Equal((256<<20)/minEntryFootprintBytes, deriveMaxEntries(256<<20))
}

// TestEntrySizeScalesWithKey is the other half of the #118 fix: the per-entry
// size estimate must grow with the path (and, for dir listings, the entry
// names), not be a flat constant that under-counts long distinct paths.
func (s *AccountantTestSuite) TestEntrySizeScalesWithKey() {
	short := attrEntrySize("/a", &attrEntry{})
	long := attrEntrySize("/very/long/path/.git/objects/ab/cdef0123456789", &attrEntry{})
	s.Assert().Greater(long, short, "a longer path must be sized larger (key is stored twice)")

	empty := dirEntrySize("/d", &dirEntry{})
	full := dirEntrySize("/d", &dirEntry{entries: []io.DirEntry{{Name: "a-fairly-long-entry-name.txt"}, {Name: "another-name"}}})
	s.Assert().Greater(full, empty, "a listing with named entries must be sized larger than an empty one")
}

func TestAccountantTestSuite(t *testing.T) {
	suite.Run(t, new(AccountantTestSuite))
}
