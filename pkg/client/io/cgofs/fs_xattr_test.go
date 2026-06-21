//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	proto "go.gmountie.dev/gmountie/pkg/proto"

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
	s.be.xattrGetSt = proto.FsError_FS_OK
	rc, data := s.fs.Getxattr("/f", "user.k")
	s.Equal(0, rc)
	s.Equal("v", string(data))
}

func (s *XattrSuite) TestListxattr() {
	s.be.xattrNames = []string{"user.a", "user.b"}
	s.be.xattrListSt = proto.FsError_FS_OK
	var got []string
	rc := s.fs.Listxattr("/f", func(name string) bool { got = append(got, name); return true })
	s.Equal(0, rc)
	s.Equal([]string{"user.a", "user.b"}, got)
}
