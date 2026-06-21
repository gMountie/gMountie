package cgofs

import (
	"testing"

	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	proto "go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/suite"
)

type StatusSuite struct{ suite.Suite }

func TestStatusSuite(t *testing.T) { suite.Run(t, new(StatusSuite)) }

func (s *StatusSuite) TestOKMapsToZero() {
	s.Equal(0, errc(proto.FsError_FS_OK))
}

func (s *StatusSuite) TestErrnoMapsToNegative() {
	// errc routes through fserr.ToErrno, which is per-GOOS: the expected value
	// is the host errno for the canonical FsError on whichever platform the
	// test runs (Linux numbers on the cgofs-conformance lane, Darwin on macOS).
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_ENOENT)), errc(proto.FsError_FS_ENOENT))
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_EACCES)), errc(proto.FsError_FS_EACCES))
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_EIO)), errc(proto.FsError_FS_EIO))
}

// TestErrcMapsHostErrno covers the divergent-numbering cases that motivated the
// OS-neutral FsError on the wire: ENOTEMPTY and the missing-xattr code surface
// as the correct HOST errno through errc, not the server's Linux number. On a
// Darwin/macFUSE build fserr.ToErrno yields Darwin errnos; on Linux it yields
// Linux's. Either way errc(FS_OK) is 0 and a real error is negative.
func (s *StatusSuite) TestErrcMapsHostErrno() {
	s.Equal(0, errc(proto.FsError_FS_OK))
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_ENOTEMPTY)), errc(proto.FsError_FS_ENOTEMPTY))
	s.Negative(errc(proto.FsError_FS_ENO_XATTR))
	// ebadf() is the unknown/closed-handle return; it must equal errc(FS_EBADF).
	s.Equal(errc(proto.FsError_FS_EBADF), ebadf())
}
