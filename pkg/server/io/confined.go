// Package io contains the server-side filesystem layer. The confined loopback
// FS lives here and replaces the unconfined pathfs.NewLoopbackFileSystem so
// every wire path is resolved beneath the volume root via openat2.
package io

import (
	"path"
	"strings"

	"golang.org/x/sys/unix"
)

// resolveHow is the openat2 resolve mask we apply to every wire path. We
// forbid every escape mechanism (mount-namespace, symlink, magic links,
// crossing devices) so a malicious or buggy path can never reach a file
// outside the volume root.
const resolveHow = unix.RESOLVE_BENEATH |
	unix.RESOLVE_NO_MAGICLINKS |
	unix.RESOLVE_NO_XDEV

// resolveBeneath translates a wire path `name` (relative to the volume root
// fd `rootFd`) into a (parentFd, leaf) pair suitable for any `*at` syscall.
// `parentFd` is an O_PATH dirfd the caller must close. `leaf` is the final
// path component, or "." when `name` addresses the root itself.
//
// All escape attempts (".." past root, absolute symlinks, magic links,
// crossing mount points) return unix.EXDEV or unix.ELOOP — never a real fd.
func resolveBeneath(rootFd int, name string) (parentFd int, leaf string, err error) {
	// Reject any path that attempts to escape via ".." before path.Clean has a
	// chance to swallow the traversal. Without this guard, "../../etc/passwd"
	// cleans to "etc/passwd" — a valid relative path the kernel would happily
	// resolve within the root, silently defeating the security boundary.
	if strings.HasPrefix(name, "..") || strings.Contains(name, "/../") || strings.HasPrefix(name, "/") {
		return -1, "", unix.EXDEV
	}

	clean := path.Clean("/" + name) // anchor at "/" so Clean can't yield ".."
	clean = strings.TrimPrefix(clean, "/")
	if clean == "" || clean == "." {
		fd, err := dupCloexec(rootFd)
		return fd, ".", err
	}
	dir, leaf := path.Split(clean)
	dir = strings.TrimSuffix(dir, "/")
	if dir == "" {
		fd, err := dupCloexec(rootFd)
		return fd, leaf, err
	}
	how := &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: resolveHow,
	}
	parentFd, err = unix.Openat2(rootFd, dir, how)
	return parentFd, leaf, err
}

// dupCloexec duplicates rootFd with CLOEXEC so the caller closing it doesn't
// affect the FS's long-lived root handle.
func dupCloexec(rootFd int) (int, error) {
	return unix.FcntlInt(uintptr(rootFd), unix.F_DUPFD_CLOEXEC, 0)
}
