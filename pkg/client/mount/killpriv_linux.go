//go:build linux

package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// enableKillPrivCap advertises CAP_HANDLE_KILLPRIV_V2 on opts so the kernel
// marks files S_NOSEC and stops issuing a per-write security.capability
// getxattr (see createMountOptions for the rationale). The capability constant
// lives in go-fuse's types_linux.go, so the reference is confined to this
// Linux-only file; killpriv_other.go provides a no-op for the macOS/BSD CLI
// builds where the constant does not exist.
func enableKillPrivCap(opts *fuse.MountOptions) {
	opts.ExtraCapabilities |= fuse.CAP_HANDLE_KILLPRIV_V2
}
