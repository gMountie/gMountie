package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type AccountantTestSuite struct {
	suite.Suite
}

// TestSingleStoreEvictsLRU verifies that within one store, the LRU
// entry is evicted when the cap is exceeded.
func (s *AccountantTestSuite) TestSingleStoreEvictsLRU() {
	acct := newAccountant(30)
	st := newStore(acct)
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
	acct := newAccountant(30)
	st := newStore(acct)
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
	acct := newAccountant(30)
	stA := newStore(acct)
	stB := newStore(acct)
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
	acct := newAccountant(0)
	st := newStore(acct)
	for i := 0; i < 1000; i++ {
		st.put(string(rune(i)), "v", 100)
	}
	s.Assert().Equal(100000, acct.Used())
	s.Assert().Equal(1000, st.size())
}

func TestAccountantTestSuite(t *testing.T) {
	suite.Run(t, new(AccountantTestSuite))
}
