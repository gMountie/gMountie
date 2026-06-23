//go:build darwin

package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// applyGoFusePlatformOptions tunes go-fuse mount options for macFUSE. DirectMount
// is a Linux-only fast path (mount(2) instead of mount_macfuse); macFUSE must go
// through mount_darwin.go's mount_macfuse exec, so disable it. The macFUSE Finder
// options go into MountOptions.Options (iosize already rides MaxWrite). autoXattr
// picks the auto_xattr vs noappledouble xattr-handling option.
func applyGoFusePlatformOptions(opts *fuse.MountOptions, volume string, autoXattr bool) {
	opts.DirectMount = false
	opts.Options = append(opts.Options, goFuseMacFUSEOptions(volume, autoXattr)...)
}
