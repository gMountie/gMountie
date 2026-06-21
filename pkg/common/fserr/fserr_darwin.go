//go:build darwin

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// Darwin reports a missing xattr as ENOATTR.
var toErrnoExtra = map[proto.FsError]syscall.Errno{
	proto.FsError_FS_ENO_XATTR: syscall.ENOATTR,
}
