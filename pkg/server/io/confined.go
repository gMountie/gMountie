// Package io contains the server-side filesystem layer. The confined loopback
// FS lives here and replaces the unconfined pathfs.NewLoopbackFileSystem so
// every wire path is resolved beneath the volume root via openat2.
package io

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
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

// ConfinedLoopbackFileSystem is a pathfs.FileSystem that translates every op
// to fd-relative *at syscalls anchored at a single openat2-resolved volume
// root dirfd. It composes safely under identityBoundFS.
type ConfinedLoopbackFileSystem struct {
	pathfs.FileSystem        // no-op String/SetDebug/OnMount/OnUnmount etc.
	rootFd            int
	rootPath          string // kept for log + StatFs
}

// NewConfinedLoopbackFileSystem opens path as the volume root. It must be a
// directory; ENOTDIR/ENOENT bubble up to the caller.
func NewConfinedLoopbackFileSystem(rootPath string) (*ConfinedLoopbackFileSystem, error) {
	fd, err := unix.Open(rootPath, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open volume root %q: %w", rootPath, err)
	}
	return &ConfinedLoopbackFileSystem{
		FileSystem: pathfs.NewDefaultFileSystem(),
		rootFd:     fd,
		rootPath:   rootPath,
	}, nil
}

// errnoToStatus maps a unix errno to a fuse.Status. EXDEV/ELOOP from
// resolveBeneath map to fuse.EACCES per spec §3.10 (the client sees a
// permission denial, not a system-internal cross-device hint).
func errnoToStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.Errno(unix.EXDEV), syscall.Errno(unix.ELOOP):
			return fuse.EACCES
		}
		return fuse.Status(errno)
	}
	return fuse.EIO
}

func (c *ConfinedLoopbackFileSystem) GetAttr(name string, _ *fuse.Context) (*fuse.Attr, fuse.Status) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	var st unix.Stat_t
	if err := unix.Fstatat(parentFd, leaf, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, errnoToStatus(err)
	}
	a := &fuse.Attr{}
	a.FromStat((*syscall.Stat_t)(unsafe.Pointer(&st)))
	return a, fuse.OK
}

func (c *ConfinedLoopbackFileSystem) StatFs(name string) *fuse.StatfsOut {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil
	}
	defer unix.Close(parentFd)
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil
	}
	defer unix.Close(fd)
	var sf unix.Statfs_t
	if err := unix.Fstatfs(fd, &sf); err != nil {
		return nil
	}
	out := &fuse.StatfsOut{}
	out.FromStatfsT((*syscall.Statfs_t)(unsafe.Pointer(&sf)))
	return out
}

func (c *ConfinedLoopbackFileSystem) Readlink(name string, _ *fuse.Context) (string, fuse.Status) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return "", errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	buf := make([]byte, 4096)
	n, err := unix.Readlinkat(parentFd, leaf, buf)
	if err != nil {
		return "", errnoToStatus(err)
	}
	return string(buf[:n]), fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Access(name string, mode uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	if err := unix.Faccessat(parentFd, leaf, mode, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}
