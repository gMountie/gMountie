package mount

import (
	"fmt"
	"os"
)

type fuseProvider string

const (
	providerAuto    fuseProvider = "auto"
	providerMacFUSE fuseProvider = "macfuse"
	providerFuseT   fuseProvider = "fuse-t"

	// macFUSELibPath is the macFUSE userspace library install location.
	macFUSELibPath = "/usr/local/lib/libfuse.2.dylib"
	// fuseTLibPath is the FUSE-T userspace library install location.
	fuseTLibPath = "/usr/local/lib/libfuse-t.dylib"
)

// detectProvider resolves which FUSE provider to use. override wins unless it
// is providerAuto, in which case it probes install paths (macFUSE preferred,
// FUSE-T fallback) using exists (injected for testing; pass pathExists in
// production). Returns an error when auto finds neither.
func detectProvider(override fuseProvider, exists func(string) bool) (fuseProvider, error) {
	if override != providerAuto {
		return override, nil
	}
	if exists(macFUSELibPath) {
		return providerMacFUSE, nil
	}
	if exists(fuseTLibPath) {
		return providerFuseT, nil
	}
	return "", fmt.Errorf("no FUSE provider found: install macFUSE (https://macfuse.github.io) or FUSE-T (https://www.fuse-t.org)")
}

// pathExists is the production exists probe for detectProvider.
func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// macOSMountOptions builds the cgofuse mount option list. volname is always
// set (Finder needs a name). "local" is macFUSE-only — it makes Finder treat
// the mount as a browsable local device (fixes "terminal sees files, Finder
// doesn't"); FUSE-T rejects unknown options, so it is omitted there.
func macOSMountOptions(volume string, provider fuseProvider, maxWrite int) []string {
	opts := []string{
		"-o", "volname=" + volume,
		"-o", "noappledouble",
	}
	if provider == providerMacFUSE {
		opts = append(opts, "-o", "local")
		// macFUSE sizes I/O via the iosize mount option. Without a large value
		// the kernel fragments writes into small FUSE ops (each crossing cgo),
		// crippling write throughput — go-fuse's MaxWrite equivalent.
		if maxWrite > 0 {
			opts = append(opts, "-o", fmt.Sprintf("iosize=%d", maxWrite))
		}
	} else if maxWrite > 0 {
		// FUSE-T accepts libfuse-style max_write ("-o max_write=N: set maximum
		// size of write requests") — same write-fragmentation fix.
		opts = append(opts, "-o", fmt.Sprintf("max_write=%d", maxWrite))
	}
	return opts
}

// linuxCgofuseOptions returns the cgofuse mount options for the Linux libfuse
// backend (the cgofuse-on-linux benchmark and the future "unify Linux" path).
// The macOS options (volname/local/noappledouble) are macFUSE-specific and
// libfuse rejects them (e.g. "unknown option volname"), so they are omitted.
// big_writes + max_write are go-fuse's MountOptions.MaxWrite equivalent: without
// them libfuse fragments writes into tiny ops (each crossing cgo), which
// collapsed write throughput ~3-20x in benchmarks.
func linuxCgofuseOptions(maxWrite int) []string {
	if maxWrite <= 0 {
		return nil
	}
	return []string{"-o", "big_writes", "-o", fmt.Sprintf("max_write=%d", maxWrite)}
}
