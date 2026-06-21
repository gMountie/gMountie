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

// TestDeleteAttrPrefixSubtreeOnly: a prefix delete removes the directory itself
// and its descendants, but MUST NOT touch siblings that merely share a string
// prefix ("d" vs "d2", "d.bak") — the boundary that makes #159's subtree
// invalidation safe.
func (s *KVSuite) TestDeleteAttrPrefixSubtreeOnly() {
	keys := []string{"d", "d/.git", "d/.git/config", "d/src/main.go", "d2", "d.bak", "e"}
	for _, k := range keys {
		s.Require().NoError(s.p.PutAttrBytes(k, []byte("v")))
	}
	s.Require().NoError(s.p.DeleteAttrPrefix("d"))

	for _, k := range []string{"d", "d/.git", "d/.git/config", "d/src/main.go"} {
		_, ok, err := s.p.GetAttrBytes(k)
		s.Require().NoError(err)
		s.Assert().Falsef(ok, "%q should be deleted by the subtree prefix", k)
	}
	for _, k := range []string{"d2", "d.bak", "e"} {
		_, ok, err := s.p.GetAttrBytes(k)
		s.Require().NoError(err)
		s.Assert().Truef(ok, "sibling %q must survive a subtree prefix delete", k)
	}
}

func TestKVSuite(t *testing.T) { suite.Run(t, new(KVSuite)) }
