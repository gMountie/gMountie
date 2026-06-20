// Package cgofs adapts the gMountie io.FileSystemBackend to cgofuse's
// FileSystemInterface, so macOS can mount via macFUSE/FUSE-T (and Windows via
// WinFsp later) without the Linux-only go-fuse path. Files that import
// cgofuse/fuse are build-tagged "darwin || cgofuse"; status mapping and the
// handle table are pure Go and build everywhere.
package cgofs

import "github.com/hanwen/go-fuse/v2/fuse"

// errc converts a FileSystemBackend fuse.Status into cgofuse's return
// convention: 0 for success, negative errno otherwise.
func errc(st fuse.Status) int {
	if st == fuse.OK {
		return 0
	}
	return -int(st)
}
