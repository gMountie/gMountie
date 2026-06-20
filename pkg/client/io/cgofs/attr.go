//go:build darwin || cgofuse

package cgofs

import (
	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// fillStat maps an io.Attr to a cgofuse Stat_t, applying the mount's
// IDRewriter so the caller sees local display uid/gid (mirrors
// setAttrFromBackend in node.go). rw may be nil (identity transform).
func fillStat(dst *cgofuse.Stat_t, a *gio.Attr, rw *gio.IDRewriter) {
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
	// rw.Inbound is nil-receiver-safe (nil rewriter = identity); no guard needed, mirroring node.go setAttrFromBackend.
	dst.Uid, dst.Gid = rw.Inbound(a.Uid, a.Gid)
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
