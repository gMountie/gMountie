package fserr

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
	proto "go.gmountie.dev/gmountie/pkg/proto"
	"google.golang.org/grpc/codes"
)

type FserrSuite struct{ suite.Suite }

func TestFserrSuite(t *testing.T) { suite.Run(t, new(FserrSuite)) }

// allErrs is every FsError value the mapping must handle.
var allErrs = []proto.FsError{
	proto.FsError_FS_EPERM, proto.FsError_FS_ENOENT, proto.FsError_FS_EIO,
	proto.FsError_FS_ENXIO, proto.FsError_FS_EBADF, proto.FsError_FS_EAGAIN,
	proto.FsError_FS_EACCES, proto.FsError_FS_EBUSY, proto.FsError_FS_EEXIST,
	proto.FsError_FS_EXDEV, proto.FsError_FS_ENOTDIR, proto.FsError_FS_EISDIR,
	proto.FsError_FS_EINVAL, proto.FsError_FS_EMFILE, proto.FsError_FS_ENFILE,
	proto.FsError_FS_EFBIG, proto.FsError_FS_ENOSPC, proto.FsError_FS_EROFS,
	proto.FsError_FS_EMLINK, proto.FsError_FS_ERANGE, proto.FsError_FS_ENAMETOOLONG,
	proto.FsError_FS_ENOSYS, proto.FsError_FS_ENOTEMPTY, proto.FsError_FS_ELOOP,
	proto.FsError_FS_EOVERFLOW, proto.FsError_FS_EDQUOT, proto.FsError_FS_ESTALE,
	proto.FsError_FS_ENOTSUP, proto.FsError_FS_ENO_XATTR, proto.FsError_FS_EINTR,
	proto.FsError_FS_ETXTBSY, proto.FsError_FS_EDEADLK, proto.FsError_FS_ENOLCK,
}

func (s *FserrSuite) TestOKIsZero() {
	s.Equal(syscall.Errno(0), ToErrno(proto.FsError_FS_OK))
	s.Equal(proto.FsError_FS_OK, FromErrno(0))
}

func (s *FserrSuite) TestEveryErrorMapsNonZeroAndRoundTrips() {
	for _, e := range allErrs {
		errno := ToErrno(e)
		s.NotEqualf(syscall.Errno(0), errno, "%v mapped to 0", e)
		s.Equalf(e, FromErrno(errno), "round-trip failed for %v", e)
	}
}

// TestForwardMapIsInjective guards round-trip soundness: if two FsError values
// mapped to the same errno, the reverse map would collapse them and round-trip
// would silently break for one. len(fromErrno) must equal len(toErrno).
func (s *FserrSuite) TestForwardMapIsInjective() {
	s.Len(fromErrno, len(toErrno),
		"two FsError values map to the same errno (forward map not injective)")
}

func (s *FserrSuite) TestUnknownErrnoIsEIO() {
	s.Equal(proto.FsError_FS_EIO, FromErrno(syscall.Errno(0x7fff)))
}

func (s *FserrSuite) TestGRPCCodes() {
	s.Equal(proto.FsError_FS_ENOENT, FromGRPCCode(codes.NotFound))
	s.Equal(proto.FsError_FS_EACCES, FromGRPCCode(codes.PermissionDenied))
	s.Equal(proto.FsError_FS_EACCES, FromGRPCCode(codes.Unauthenticated))
	s.Equal(proto.FsError_FS_EIO, FromGRPCCode(codes.Internal))
}
