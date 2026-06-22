// Package cgofs adapts the gMountie io.FileSystemBackend to cgofuse's
// FileSystemInterface, so macOS can mount via macFUSE/FUSE-T (and Windows via
// WinFsp later) without the Linux-only go-fuse path. Files that import
// cgofuse/fuse are build-tagged "darwin || cgofuse"; status mapping and the
// handle table are pure Go and build everywhere.
package cgofs

import (
	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// errc converts a backend proto.FsError into cgofuse's return convention: 0 for
// success, negative host errno otherwise. fserr.ToErrno is per-GOOS, so on a
// darwin build this yields Darwin errno numbers (what macFUSE expects).
func errc(st proto.FsError) int {
	if st == proto.FsError_FS_OK {
		return 0
	}
	return -int(fserr.ToErrno(st))
}

// ebadf is the cgofuse return for an unknown/closed handle.
func ebadf() int { return -int(fserr.ToErrno(proto.FsError_FS_EBADF)) }
