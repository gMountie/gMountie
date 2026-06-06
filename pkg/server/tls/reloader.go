package tls

import (
	"os"
	"time"
)

// stamp identifies a cert file's on-disk version. The inode catches the
// kubelet projected-volume rotation (an atomic ..data symlink swap = a new
// inode) and same-second mtime granularity; os.Stat follows symlinks so the
// projected layout is transparent.
type stamp struct {
	modTime time.Time
	size    int64
	inode   uint64
}

// equal compares stamps. modTime is compared with Equal (never ==) so a
// monotonic-clock component can't poison the comparison.
func (a stamp) equal(b stamp) bool {
	return a.modTime.Equal(b.modTime) && a.size == b.size && a.inode == b.inode
}

// statStamp reads the current stamp of path.
func statStamp(path string) (stamp, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return stamp{}, err
	}
	return stamp{modTime: fi.ModTime(), size: fi.Size(), inode: inodeOf(fi)}, nil
}
