package mount

import (
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

// currentGOOS is the runtime OS the helper sees. It defaults to runtime.GOOS
// and is overridable so the table-driven test can exercise both branches from
// a single (Linux) CI host without build tags.
var currentGOOS = runtime.GOOS

// wrapMountError annotates a FUSE mount error on darwin when it looks like the
// host is missing a FUSE provider (macFUSE or FUSE-T). On any other platform —
// or when the error doesn't match the missing-driver pattern — the error is
// returned unchanged (pointer-identical for nil and non-matching cases).
func wrapMountError(err error) error {
	if err == nil || currentGOOS != "darwin" {
		return err
	}
	if !looksLikeMissingFUSE(err) {
		return err
	}
	return errors.Wrap(err,
		"FUSE driver missing — install macFUSE (https://macfuse.io) or "+
			"FUSE-T (https://www.fuse-t.org/) before mounting on macOS")
}

// looksLikeMissingFUSE returns true if the error text suggests the macOS FUSE
// provider (macFUSE's mount_macfuse helper, FUSE-T's /dev/macfuse, the older
// /dev/osxfuse* devices) isn't installed or reachable. Matching is
// case-insensitive substring across a small allow-list to keep false positives
// low.
func looksLikeMissingFUSE(err error) bool {
	s := strings.ToLower(err.Error())
	patterns := []string{
		"macfuse",
		"osxfuse",
		"mount_macfuse",
		"/dev/macfuse",
		"/dev/osxfuse",
		"no such file or directory",
		"executable file not found",
	}
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}
