//go:build unix

package tls

import (
	"os"
	"syscall"
)

// inodeOf extracts the file's inode where the platform exposes one.
func inodeOf(fi os.FileInfo) uint64 {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return st.Ino
	}
	return 0
}
