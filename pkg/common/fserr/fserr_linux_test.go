//go:build linux

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// TestLinuxNumbers pins the concrete Linux errno numbers. It lives in a
// linux-tagged file because syscall.ENODATA is Linux-only; keeping it out of the
// shared fserr_test.go lets the suite compile (and TestDarwinNumbers run) under
// GOOS=darwin.
func (s *FserrSuite) TestLinuxNumbers() {
	s.Equal(syscall.Errno(39), ToErrno(proto.FsError_FS_ENOTEMPTY)) // ENOTEMPTY=39 on Linux
	s.Equal(syscall.ENODATA, ToErrno(proto.FsError_FS_ENO_XATTR))
}
