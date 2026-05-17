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
	s.s = newStore(s.acct)
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
