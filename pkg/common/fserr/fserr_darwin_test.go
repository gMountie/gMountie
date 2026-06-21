//go:build darwin

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

func (s *FserrSuite) TestDarwinNumbers() {
	s.Equal(syscall.Errno(66), ToErrno(proto.FsError_FS_ENOTEMPTY)) // Darwin ENOTEMPTY
	s.Equal(syscall.ENOATTR, ToErrno(proto.FsError_FS_ENO_XATTR))
	s.Equal(syscall.Errno(35), ToErrno(proto.FsError_FS_EAGAIN)) // Darwin EAGAIN
}
