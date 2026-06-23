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

// adapterKind names which FUSE adapter mounts a backend on darwin.
type adapterKind int

const (
	adapterGoFuse  adapterKind = iota // macFUSE: hanwen/go-fuse (cgo-free, full node.go features)
	adapterCgoFuse                    // FUSE-T: winfsp/cgofuse (kextless)
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
func macOSMountOptions(volume string, provider fuseProvider, maxWrite int, ftBackend string) []string {
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
	} else {
		// FUSE-T. "backend=fskit" selects the native Apple FSKit backend instead
		// of the default NFSv4 one (which amplifies metadata RPCs); omitted for
		// "nfs"/"" so FUSE-T uses its default backend.
		if ftBackend == "fskit" {
			opts = append(opts, "-o", "backend=fskit")
		}
		// FUSE-T accepts libfuse-style max_write ("-o max_write=N: set maximum
		// size of write requests") — same write-fragmentation fix.
		if maxWrite > 0 {
			opts = append(opts, "-o", fmt.Sprintf("max_write=%d", maxWrite))
		}
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

// adapterForProvider maps a resolved (non-auto) provider to its adapter.
// macFUSE speaks the FUSE kernel protocol go-fuse implements; FUSE-T is
// NFSv4-backed and only reachable through cgofuse's libfuse API.
func adapterForProvider(p fuseProvider) adapterKind {
	if p == providerFuseT {
		return adapterCgoFuse
	}
	return adapterGoFuse
}

// goFuseMacFUSEOptions returns the macFUSE mount options as bare strings for
// go-fuse's MountOptions.Options (it joins them as a single -o list and adds
// -o iosize=MaxWrite itself in mount_darwin.go). "local" makes Finder show a
// browsable volume (fixes "terminal sees files, Finder doesn't"); "volname"
// names it. go-fuse on darwin is only ever the macFUSE path.
//
// The last option toggles macOS xattr handling (see config.DefaultFUSEAutoXAttr):
//   - autoXattr true (default): "auto_xattr" — macFUSE stores xattrs/FinderInfo
//     in ._ AppleDouble files, so Finder copies work (Finder's
//     setattrlist(ATTR_CMN_FNDRINFO) would otherwise EINVAL → "error -50").
//   - autoXattr false: "noappledouble" — suppresses ._*/.DS_Store chatter and
//     routes xattrs server-side, but Finder copies that set FinderInfo fail.
func goFuseMacFUSEOptions(volume string, autoXattr bool) []string {
	appleXattr := "noappledouble"
	if autoXattr {
		appleXattr = "auto_xattr"
	}
	return []string{"volname=" + volume, "local", appleXattr}
}
