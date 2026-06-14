package cache

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type DataCacheTestSuite struct {
	suite.Suite
	c *dataCache
}

func (s *DataCacheTestSuite) SetupTest() {
	s.c = newDataCache(newAccountant(0, 0), 1024) // 1 KiB chunks for arithmetic clarity
}

func (s *DataCacheTestSuite) TestMissAndPut() {
	s.Assert().Nil(s.c.get("/f", 0))
	s.c.put("/f", 0, []byte("abcd"))
	s.Assert().Equal([]byte("abcd"), s.c.get("/f", 0))
}

func (s *DataCacheTestSuite) TestDifferentPathsDisjoint() {
	s.c.put("/a", 0, []byte("1111"))
	s.c.put("/b", 0, []byte("2222"))
	s.Assert().Equal([]byte("1111"), s.c.get("/a", 0))
	s.Assert().Equal([]byte("2222"), s.c.get("/b", 0))
}

func (s *DataCacheTestSuite) TestInvalidatePathDropsAllChunks() {
	s.c.put("/f", 0, []byte("c0"))
	s.c.put("/f", 1, []byte("c1"))
	s.c.put("/f", 5, []byte("c5"))
	s.c.put("/other", 0, []byte("xx"))
	s.c.invalidatePath("/f")
	s.Assert().Nil(s.c.get("/f", 0))
	s.Assert().Nil(s.c.get("/f", 1))
	s.Assert().Nil(s.c.get("/f", 5))
	s.Assert().Equal([]byte("xx"), s.c.get("/other", 0))
}

func (s *DataCacheTestSuite) TestInvalidateRange() {
	// chunks: 0 = [0, 1023], 1 = [1024, 2047], 2 = [2048, 3071]
	s.c.put("/f", 0, make([]byte, 1024))
	s.c.put("/f", 1, make([]byte, 1024))
	s.c.put("/f", 2, make([]byte, 1024))
	// Write to bytes [500, 1500) touches chunks 0 and 1
	s.c.invalidateRange("/f", 500, 1000)
	s.Assert().Nil(s.c.get("/f", 0))
	s.Assert().Nil(s.c.get("/f", 1))
	s.Assert().NotNil(s.c.get("/f", 2))
}

func (s *DataCacheTestSuite) TestInvalidateRangeZeroSize() {
	s.c.put("/f", 0, make([]byte, 1024))
	s.c.invalidateRange("/f", 0, 0) // no-op
	s.Assert().NotNil(s.c.get("/f", 0))
}

func (s *DataCacheTestSuite) TestInvalidatePathDoesNotMatchPrefixOfOtherPath() {
	// /foo and /foobar both have a chunk; invalidatePath("/foo") must
	// only drop /foo's, not /foobar's. The "\x00" separator after
	// the path guarantees this.
	s.c.put("/foo", 0, []byte("a"))
	s.c.put("/foobar", 0, []byte("b"))
	s.c.invalidatePath("/foo")
	s.Assert().Nil(s.c.get("/foo", 0))
	s.Assert().NotNil(s.c.get("/foobar", 0))
}

func TestDataCacheTestSuite(t *testing.T) {
	suite.Run(t, new(DataCacheTestSuite))
}
