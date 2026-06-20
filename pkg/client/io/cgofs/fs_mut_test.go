//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

type MutSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestMutSuite(t *testing.T) { suite.Run(t, new(MutSuite)) }

func (s *MutSuite) SetupTest() {
	s.be = &fakeBackend{statAttr: &gio.Attr{}, statSt: fuse.OK}
	// rewriter: local uid 501 -> server uid 1000
	rw := gio.NewIDRewriter(&gio.Identity{Uid: 1000, Gid: 1000}, 501, 20)
	s.fs = New(s.be, rw)
}

func (s *MutSuite) TestChmodSetsModeBit() {
	rc := s.fs.Chmod("/f", 0o600)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_MODE))
	s.Equal(uint32(0o600), s.be.setAttrIn.Mode)
}

func (s *MutSuite) TestChownAppliesOutboundRewrite() {
	rc := s.fs.Chown("/f", 501, 20)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_UID))
	s.Equal(uint32(1000), s.be.setAttrIn.Uid) // 501 -> 1000 via Outbound
	s.Equal(uint32(1000), s.be.setAttrIn.Gid) // 20 -> 1000 via Outbound
}

func (s *MutSuite) TestUtimensSetsTimes() {
	tmsp := []cgofuse.Timespec{{Sec: 111, Nsec: 0}, {Sec: 222, Nsec: 0}}
	rc := s.fs.Utimens("/f", tmsp)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_ATIME))
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_MTIME))
	s.Require().NotNil(s.be.setAttrIn.Atime)
	s.Equal(int64(111), s.be.setAttrIn.Atime.Unix())
}

func (s *MutSuite) TestRename() {
	rc := s.fs.Rename("/a", "/b")
	s.Equal(0, rc)
}
