package persist_test

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/cache/persist"

	"github.com/stretchr/testify/suite"
)

type KVSuite struct {
	suite.Suite
	p   *persist.Persist
	dir string
}

func (s *KVSuite) SetupTest() {
	s.dir = s.T().TempDir()
	p, err := persist.Open(persist.Options{Root: s.dir})
	s.Require().NoError(err)
	s.p = p
}
func (s *KVSuite) TearDownTest() { _ = s.p.Close() }

func (s *KVSuite) TestAttrPutGetDelete() {
	s.Require().NoError(s.p.PutAttrBytes("path/to/file", []byte("payload")))
	got, ok, err := s.p.GetAttrBytes("path/to/file")
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Assert().Equal([]byte("payload"), got)

	s.Require().NoError(s.p.DeleteAttrBytes("path/to/file"))
	_, ok, err = s.p.GetAttrBytes("path/to/file")
	s.Require().NoError(err)
	s.Assert().False(ok)
}

func (s *KVSuite) TestDirPutGetDelete() {
	s.Require().NoError(s.p.PutDirBytes("dir/a", []byte("entries")))
	got, ok, err := s.p.GetDirBytes("dir/a")
	s.Require().NoError(err)
	s.Require().True(ok)
	s.Assert().Equal([]byte("entries"), got)
	s.Require().NoError(s.p.DeleteDirBytes("dir/a"))
	_, ok, _ = s.p.GetDirBytes("dir/a")
	s.Assert().False(ok)
}

func TestKVSuite(t *testing.T) { suite.Run(t, new(KVSuite)) }
