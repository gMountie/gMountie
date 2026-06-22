//go:build darwin || cgofuse

package cgofs

import (
	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// fillStat maps an io.Attr to a cgofuse Stat_t. It is a plain field copy —
// the backend (with the identity layer composed outermost) has already
// rewritten Uid/Gid to local display ids, so no rewrite happens here (mirrors
// setAttrFromBackend in node.go).
func fillStat(dst *cgofuse.Stat_t, a *gio.Attr) {
	dst.Ino = a.Ino
	dst.Size = int64(a.Size)
	dst.Blocks = int64(a.Blocks)
	dst.Mode = a.Mode
	dst.Nlink = a.Nlink
	dst.Rdev = uint64(a.Rdev)
	dst.Blksize = int64(a.Blksize)
	dst.Atim = cgofuse.Timespec{Sec: int64(a.Atime), Nsec: int64(a.Atimensec)}
	dst.Mtim = cgofuse.Timespec{Sec: int64(a.Mtime), Nsec: int64(a.Mtimensec)}
	dst.Ctim = cgofuse.Timespec{Sec: int64(a.Ctime), Nsec: int64(a.Ctimensec)}
	dst.Uid = a.Uid
	dst.Gid = a.Gid
}

// fillStatfs maps an io.StatFs to a cgofuse Statfs_t.
func fillStatfs(dst *cgofuse.Statfs_t, s *gio.StatFs) {
	dst.Bsize = uint64(s.Bsize)
	dst.Frsize = uint64(s.Frsize)
	dst.Blocks = s.Blocks
	dst.Bfree = s.Bfree
	dst.Bavail = s.Bavail
	dst.Files = s.Files
	dst.Ffree = s.Ffree
	dst.Namemax = uint64(s.Namelen)
}
