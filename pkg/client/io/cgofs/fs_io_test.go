//go:build darwin || cgofuse

package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// recHandle is an io.FileHandle that records its path.
type recHandle struct{ p string }

func (h *recHandle) Path() string           { return h.p }
func (h *recHandle) Unwrap() gio.FileHandle { return h }

type IOSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestIOSuite(t *testing.T) { suite.Run(t, new(IOSuite)) }

func (s *IOSuite) SetupTest() {
	s.be = &fakeBackend{}
	s.fs = New(s.be, nil)
}

func (s *IOSuite) TestOpenReadRelease() {
	s.be.openFH = &recHandle{p: "f"}
	s.be.openSt = proto.FsError_FS_OK
	rc, fh := s.fs.Open("/f", 0)
	s.Equal(0, rc)
	s.NotZero(fh)

	s.be.readData = []byte("hello")
	s.be.readSt = proto.FsError_FS_OK
	buf := make([]byte, 5)
	n := s.fs.Read("/f", buf, 0, fh)
	s.Equal(5, n)
	s.Equal("hello", string(buf))

	rc = s.fs.Release("/f", fh)
	s.Equal(0, rc)
	// handle no longer resolvable -> EBADF-ish: a subsequent read returns -EBADF
	n = s.fs.Read("/f", buf, 0, fh)
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_EBADF)), n)
}

func (s *IOSuite) TestWrite() {
	s.be.openFH = &recHandle{p: "f"}
	s.be.openSt = proto.FsError_FS_OK
	_, fh := s.fs.Open("/f", 0)
	s.be.writeSt = proto.FsError_FS_OK
	n := s.fs.Write("/f", []byte("abc"), 0, fh)
	s.Equal(3, n)
	s.Equal("abc", string(s.be.wroteData))
}

func (s *IOSuite) TestTruncateMapsToSetAttrSize() {
	s.be.statAttr = &gio.Attr{}
	s.be.statSt = proto.FsError_FS_OK
	rc := s.fs.Truncate("/f", 128, ^uint64(0))
	s.Equal(0, rc)
	s.Equal(uint32(fuse.FATTR_SIZE), s.be.setAttrIn.Valid&uint32(fuse.FATTR_SIZE))
	s.Equal(uint64(128), s.be.setAttrIn.Size)
}
