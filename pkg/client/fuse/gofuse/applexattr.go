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
