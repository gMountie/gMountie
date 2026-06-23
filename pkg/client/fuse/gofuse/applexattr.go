package gofuse

import "strings"

// macOS extended attributes live in the com.apple.* namespace (quarantine,
// FinderInfo, resource forks, Spotlight metadata, …). They cannot be stored on
// the server verbatim for two reasons: the server's wire allowlist
// (xattrWriteAllowed) only permits user.* and the POSIX-ACL pair, and the Linux
// backing store itself rejects xattr names outside the user./trusted./
// security./system. namespaces. A raw com.apple.* SetXAttr therefore fails with
// EPERM — and macOS copyfile() (used by cp, ditto and Finder) treats ANY
// fsetxattr failure as fatal once the filesystem advertises xattr support,
// which gMountie does (user.* works). The result is the "you don't have
// permission to access some of the items" copy failure.
//
// To make copies succeed while preserving the metadata, the macFUSE client
// remaps com.apple.* into the user.* namespace on the wire — the same
// convention Samba and Linux NFS use to serve macOS xattrs from a Linux store.
// The mapping is reversed on ListXAttr so Finder/copyfile see the real names.
//
// These helpers are GOOS-independent (and unit-tested on Linux); the wiring
// that calls them is darwin-only (see applexattr_darwin.go), so the Linux FUSE
// path is unaffected.
const (
	appleXattrPrefix = "com.apple."
	userXattrPrefix  = "user."
)

// appleXattrToBackend maps a kernel-supplied xattr name to the name used on the
// wire and backend: com.apple.* becomes user.com.apple.*. Any other name
// (including a genuine user.* attribute) is returned unchanged.
func appleXattrToBackend(name string) string {
	if strings.HasPrefix(name, appleXattrPrefix) {
		return userXattrPrefix + name
	}
	return name
}

// appleXattrFromBackend reverses appleXattrToBackend for names returned by the
// backend (ListXAttr): user.com.apple.* becomes com.apple.*. A plain user.*
// name is left untouched.
func appleXattrFromBackend(name string) string {
	if strings.HasPrefix(name, userXattrPrefix+appleXattrPrefix) {
		return strings.TrimPrefix(name, userXattrPrefix)
	}
	return name
}

// macOS <sys/xattr.h> defines different SETXATTR flag bits than Linux, and adds
// several Linux has no notion of. macFUSE forwards the macOS values verbatim, so
// they must be translated before reaching the Linux server's unix.Setxattr —
// otherwise an unknown bit (e.g. Finder's XATTR_NODEFAULT 0x10, sent on the
// setattrlist(ATTR_CMN_FNDRINFO) FinderInfo write) is rejected with EINVAL,
// which surfaces in Finder as the opaque "error code -50" on every copy.
const (
	macXattrCreate  = 0x0002 // macOS XATTR_CREATE  (Linux: 0x1)
	macXattrReplace = 0x0004 // macOS XATTR_REPLACE (Linux: 0x2)
	// macOS-only bits with no Linux meaning: XATTR_NOFOLLOW 0x1,
	// XATTR_NOSECURITY 0x8, XATTR_NODEFAULT 0x10, XATTR_SHOWCOMPRESSION 0x20.
	linuxXattrCreate  = 0x1 // unix.XATTR_CREATE
	linuxXattrReplace = 0x2 // unix.XATTR_REPLACE
)

// appleXattrFlagsToBackend translates macOS SETXATTR flags to the Linux flag set
// the server's unix.Setxattr understands: only XATTR_CREATE / XATTR_REPLACE
// carry over (with their differing values), and macOS-only bits are dropped.
func appleXattrFlagsToBackend(flags uint32) uint32 {
	var out uint32
	if flags&macXattrCreate != 0 {
		out |= linuxXattrCreate
	}
	if flags&macXattrReplace != 0 {
		out |= linuxXattrReplace
	}
	return out
}
