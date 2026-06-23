//go:build !darwin

package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// applyGoFusePlatformOptions is a no-op off darwin: Linux keeps DirectMount and
// adds no Finder options.
func applyGoFusePlatformOptions(_ *fuse.MountOptions, _ string, _ bool) {}
