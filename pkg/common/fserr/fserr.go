// Package fserr maps the OS-neutral proto.FsError to and from the host kernel's
// errno. syscall.E* constants are per-GOOS, so ToErrno yields the correct number
// on whatever platform this is built for; the server uses FromErrno, each client
// adapter uses ToErrno. Codes whose NAME differs across OSes (the xattr case)
// live in the build-tagged fserr_<goos>.go files via toErrnoExtra.
package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
	"google.golang.org/grpc/codes"
)

// toErrno is the same-name table; platform files add toErrnoExtra in init().
var toErrno = map[proto.FsError]syscall.Errno{
	proto.FsError_FS_EPERM:        syscall.EPERM,
	proto.FsError_FS_ENOENT:       syscall.ENOENT,
	proto.FsError_FS_EIO:          syscall.EIO,
	proto.FsError_FS_ENXIO:        syscall.ENXIO,
	proto.FsError_FS_EBADF:        syscall.EBADF,
	proto.FsError_FS_EAGAIN:       syscall.EAGAIN,
	proto.FsError_FS_EACCES:       syscall.EACCES,
	proto.FsError_FS_EBUSY:        syscall.EBUSY,
	proto.FsError_FS_EEXIST:       syscall.EEXIST,
	proto.FsError_FS_EXDEV:        syscall.EXDEV,
	proto.FsError_FS_ENOTDIR:      syscall.ENOTDIR,
	proto.FsError_FS_EISDIR:       syscall.EISDIR,
	proto.FsError_FS_EINVAL:       syscall.EINVAL,
	proto.FsError_FS_EMFILE:       syscall.EMFILE,
	proto.FsError_FS_ENFILE:       syscall.ENFILE,
	proto.FsError_FS_EFBIG:        syscall.EFBIG,
	proto.FsError_FS_ENOSPC:       syscall.ENOSPC,
	proto.FsError_FS_EROFS:        syscall.EROFS,
	proto.FsError_FS_EMLINK:       syscall.EMLINK,
	proto.FsError_FS_ERANGE:       syscall.ERANGE,
	proto.FsError_FS_ENAMETOOLONG: syscall.ENAMETOOLONG,
	proto.FsError_FS_ENOSYS:       syscall.ENOSYS,
	proto.FsError_FS_ENOTEMPTY:    syscall.ENOTEMPTY,
	proto.FsError_FS_ELOOP:        syscall.ELOOP,
	proto.FsError_FS_EOVERFLOW:    syscall.EOVERFLOW,
	proto.FsError_FS_EDQUOT:       syscall.EDQUOT,
	proto.FsError_FS_ESTALE:       syscall.ESTALE,
	proto.FsError_FS_ENOTSUP:      syscall.ENOTSUP,
	proto.FsError_FS_EINTR:        syscall.EINTR,
	proto.FsError_FS_ETXTBSY:      syscall.ETXTBSY,
	proto.FsError_FS_EDEADLK:      syscall.EDEADLK,
	proto.FsError_FS_ENOLCK:       syscall.ENOLCK,
}

var fromErrno = map[syscall.Errno]proto.FsError{}

func init() {
	for fe, en := range toErrnoExtra { // platform-specific (xattr)
		toErrno[fe] = en
	}
	for fe, en := range toErrno {
		fromErrno[en] = fe
	}
}

// ToErrno maps a canonical FsError to the host kernel's errno. FS_OK -> 0.
func ToErrno(e proto.FsError) syscall.Errno {
	if e == proto.FsError_FS_OK {
		return 0
	}
	if en, ok := toErrno[e]; ok {
		return en
	}
	return syscall.EIO
}

// FromErrno maps a host errno to the canonical FsError. 0 -> FS_OK; unmapped -> FS_EIO.
func FromErrno(e syscall.Errno) proto.FsError {
	if e == 0 {
		return proto.FsError_FS_OK
	}
	if fe, ok := fromErrno[e]; ok {
		return fe
	}
	return proto.FsError_FS_EIO
}

// FromGRPCCode maps a transport-level gRPC code to FsError (mirrors what the
// server produces; see backend_grpc.go statusFromRPCError history).
func FromGRPCCode(c codes.Code) proto.FsError {
	switch c {
	case codes.NotFound:
		return proto.FsError_FS_ENOENT
	case codes.PermissionDenied, codes.Unauthenticated:
		return proto.FsError_FS_EACCES
	default:
		return proto.FsError_FS_EIO
	}
}
