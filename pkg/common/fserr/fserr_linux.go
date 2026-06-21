//go:build linux

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// Linux reports a missing xattr as ENODATA.
var toErrnoExtra = map[proto.FsError]syscall.Errno{
	proto.FsError_FS_ENO_XATTR: syscall.ENODATA,
}
