//go:build !linux

package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// enableKillPrivCap is a no-op on non-Linux platforms. CAP_HANDLE_KILLPRIV_V2
// is a Linux-only capability in go-fuse (it advertises a Linux-kernel FUSE
// feature), so there is no constant to reference here. macFUSE / FUSE-T perform
// setuid/setgid stripping per their own platform semantics, so the
// fuse.handle_kill_priv knob simply has no effect on these builds.
func enableKillPrivCap(_ *fuse.MountOptions) {}
