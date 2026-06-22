//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

type MutSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestMutSuite(t *testing.T) { suite.Run(t, new(MutSuite)) }

func (s *MutSuite) SetupTest() {
	s.be = &fakeBackend{statAttr: &gio.Attr{}, statSt: proto.FsError_FS_OK}
	// No rewriter here: the adapter no longer rewrites uid/gid — the identity
	// backend layer (tested in pkg/client/io/identity) does, composed outermost.
	s.fs = New(s.be)
}

func (s *MutSuite) TestChmodSetsModeBit() {
	rc := s.fs.Chmod("/f", 0o600)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_MODE))
	s.Equal(uint32(0o600), s.be.setAttrIn.Mode)
}

func (s *MutSuite) TestChownForwardsLocalIDsWithBothBits() {
	// The adapter no longer rewrites: it forwards the LOCAL ids with both valid
	// bits set, leaving the local→server Outbound to the identity layer. cgofuse
	// always supplies both ids, so both bits are always set.
	rc := s.fs.Chown("/f", 501, 20)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_UID))
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_GID))
	s.Equal(uint32(501), s.be.setAttrIn.Uid) // forwarded verbatim (no rewrite)
	s.Equal(uint32(20), s.be.setAttrIn.Gid)  // forwarded verbatim (no rewrite)
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
