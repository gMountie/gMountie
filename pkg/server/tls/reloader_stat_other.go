//go:build !unix

package tls

import "os"

// inodeOf is unavailable off unix; the (mtime, size) pair still detects
// rotation in practice there.
func inodeOf(os.FileInfo) uint64 { return 0 }
