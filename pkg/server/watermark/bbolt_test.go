package watermark

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type StoreSuite struct {
	suite.Suite
	dir string
}

func TestStoreSuite(t *testing.T) { suite.Run(t, new(StoreSuite)) }
func (s *StoreSuite) SetupTest() { s.dir = s.T().TempDir() }

func (s *StoreSuite) open() Store {
	st, err := OpenBBolt(filepath.Join(s.dir, "wm.db"))
	s.Require().NoError(err)
	return st
}

func (s *StoreSuite) TestAdvanceIsMonotoneAndDurable() {
	k := Key{Identity: "alice", Volume: "vol"}
	st := s.open()
	s.Require().NoError(st.Advance(k, 5))
	s.Require().NoError(st.Advance(k, 3)) // lower ignored
	s.Require().NoError(st.Close())

	st2 := s.open() // reopen → durability
	r, err := st2.Get(k)
	s.Require().NoError(err)
	s.Equal(uint64(5), r.Watermark)
	s.Require().NoError(st2.Close())
}

func (s *StoreSuite) TestRevokeGenPersists() {
	k := Key{Identity: "bob", Volume: "vol"}
	st := s.open()
	s.Require().NoError(st.RevokeGen(k, 7))
	s.Require().NoError(st.RevokeGen(k, 9))
	s.Require().NoError(st.Close())

	st2 := s.open()
	r, err := st2.Get(k)
	s.Require().NoError(err)
	s.ElementsMatch([]uint64{7, 9}, r.RevokedGens)
	s.Require().NoError(st2.Close())
}

func (s *StoreSuite) TestGetMissingIsZeroRecord() {
	st := s.open()
	defer func() { s.Require().NoError(st.Close()) }()
	r, err := st.Get(Key{Identity: "nobody", Volume: "v"})
	s.Require().NoError(err)
	s.Equal(uint64(0), r.Watermark)
	s.Empty(r.RevokedGens)
}
