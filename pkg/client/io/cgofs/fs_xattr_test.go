//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type XattrSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestXattrSuite(t *testing.T) { suite.Run(t, new(XattrSuite)) }

func (s *XattrSuite) SetupTest() {
	s.be = &fakeBackend{}
	s.fs = New(s.be, nil)
}

func (s *XattrSuite) TestGetxattr() {
	s.be.xattrData = []byte("v")
	s.be.xattrGetSt = fuse.OK
	rc, data := s.fs.Getxattr("/f", "user.k")
	s.Equal(0, rc)
	s.Equal("v", string(data))
}

func (s *XattrSuite) TestListxattr() {
	s.be.xattrNames = []string{"user.a", "user.b"}
	s.be.xattrListSt = fuse.OK
	var got []string
	rc := s.fs.Listxattr("/f", func(name string) bool { got = append(got, name); return true })
	s.Equal(0, rc)
	s.Equal([]string{"user.a", "user.b"}, got)
}
