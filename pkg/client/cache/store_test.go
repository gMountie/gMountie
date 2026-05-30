package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type StoreTestSuite struct {
	suite.Suite
	acct *accountant
	s    *store
}

func (s *StoreTestSuite) SetupTest() {
	s.acct = newAccountant(0) // unlimited for the basic suite
	s.s = newStore(s.acct, "attr")
}

func (s *StoreTestSuite) TestPutGet() {
	s.s.put("k1", "v1", 10)
	e := s.s.get("k1")
	s.Require().NotNil(e)
	s.Assert().Equal("v1", e.value)
	s.Assert().Equal(10, e.size)
}

func (s *StoreTestSuite) TestGetMiss() {
	s.Assert().Nil(s.s.get("nope"))
}

func (s *StoreTestSuite) TestPutReplacesAndRefundsBytes() {
	s.s.put("k", "v", 100)
	s.Require().Equal(100, s.acct.Used())
	s.s.put("k", "v2", 30)
	s.Assert().Equal(30, s.acct.Used())
	s.Assert().Equal("v2", s.s.get("k").value)
}

func (s *StoreTestSuite) TestRemove() {
	s.s.put("k", "v", 50)
	s.s.remove("k")
	s.Assert().Nil(s.s.get("k"))
	s.Assert().Equal(0, s.acct.Used())
}

func (s *StoreTestSuite) TestRemoveMatching() {
	s.s.put("/a/x", "v1", 10)
	s.s.put("/a/y", "v2", 10)
	s.s.put("/b/z", "v3", 10)
	s.s.removeMatching(func(k string) bool { return len(k) >= 2 && k[:2] == "/a" })
	s.Assert().Nil(s.s.get("/a/x"))
	s.Assert().Nil(s.s.get("/a/y"))
	s.Assert().NotNil(s.s.get("/b/z"))
	s.Assert().Equal(10, s.acct.Used())
}

func (s *StoreTestSuite) TestConcurrentReadsRaceClean() {
	s.s.put("k", "v", 10)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 1000; j++ {
				_ = s.s.get("k")
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	s.Assert().Equal(10, s.acct.Used())
}

func TestStoreTestSuite(t *testing.T) {
	suite.Run(t, new(StoreTestSuite))
}

// PersistedStoreSuite exercises the memory-tier-above-disk fallthrough
// added by Sub-spec C. Memory hit returns immediately. Memory miss
// falls through to disk via the configured Loader/Putter pair; a disk
// hit promotes the value back into the memory tier so subsequent gets
// short-circuit.
type PersistedStoreSuite struct {
	suite.Suite
}

func (s *PersistedStoreSuite) TestMemoryMissFallsThroughToLoader() {
	loaderCalls := 0
	loader := func(key string) (any, int, bool) {
		loaderCalls++
		if key == "k1" {
			return "from-disk", 9, true
		}
		return nil, 0, false
	}
	acct := newAccountant(0)
	st := newStoreWithPersist(acct, loader, func(string, any, int) {}, nil, "attr")

	e := st.get("k1")
	s.Require().NotNil(e)
	s.Assert().Equal("from-disk", e.value)
	s.Assert().Equal(1, loaderCalls)

	e2 := st.get("k1")
	s.Require().NotNil(e2)
	s.Assert().Equal(1, loaderCalls, "loader must not be called for memory hit")
}

func (s *PersistedStoreSuite) TestPutAlsoWritesThrough() {
	var putCalls int
	loader := func(string) (any, int, bool) { return nil, 0, false }
	putter := func(_ string, _ any, _ int) { putCalls++ }
	st := newStoreWithPersist(newAccountant(0), loader, putter, nil, "attr")
	st.put("k", "v", 1)
	s.Assert().Equal(1, putCalls, "write-through must call putter")
}

func (s *PersistedStoreSuite) TestRemoveForwardsToRemover() {
	var removerCalls int
	loader := func(string) (any, int, bool) { return nil, 0, false }
	putter := func(string, any, int) {}
	remover := func(string) { removerCalls++ }
	st := newStoreWithPersist(newAccountant(0), loader, putter, remover, "attr")
	st.put("k", "v", 1)
	st.remove("k")
	s.Assert().Equal(1, removerCalls)
}

func TestPersistedStoreSuite(t *testing.T) { suite.Run(t, new(PersistedStoreSuite)) }
