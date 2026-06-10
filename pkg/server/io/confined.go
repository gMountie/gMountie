// Package io contains the server-side filesystem layer. The confined loopback
// FS lives here and replaces the unconfined pathfs.NewLoopbackFileSystem so
// every wire path is resolved beneath the volume root via openat2.
package io

import (
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
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
	// gMountie's wire convention treats paths as relative to the volume root,
	// and the client may send them with or without a leading slash (the wire
	// RPCs send "/foo/bar", the FUSE pathfs hook sends "foo/bar"). Strip
	// the slash up front so both forms address the same in-tree file.
	name = strings.TrimPrefix(name, "/")

	// Reject any path that attempts to escape via ".." before path.Clean has a
	// chance to swallow the traversal. Without this guard, "../../etc/passwd"
	// cleans to "etc/passwd" — a valid relative path the kernel would happily
	// resolve within the root, silently defeating the security boundary.
	if strings.HasPrefix(name, "..") || strings.Contains(name, "/../") {
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
	pathfs.FileSystem // no-op String/SetDebug/OnMount/OnUnmount etc.
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
		case unix.EXDEV, unix.ELOOP:
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
	defer func() { _ = unix.Close(parentFd) }()
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
	defer func() { _ = unix.Close(parentFd) }()
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil
	}
	defer func() { _ = unix.Close(fd) }()
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
	defer func() { _ = unix.Close(parentFd) }()
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
	defer func() { _ = unix.Close(parentFd) }()
	// Faccessat with flags=0 follows symlinks the normal way — which would
	// happily resolve a symlink whose target lives OUTSIDE the volume root,
	// since the leaf op is not openat2-guarded. Resolve the (followed) target
	// through openat2 RESOLVE_BENEATH first; that rejects any cross-root
	// resolve as EXDEV/ELOOP before we ever check access on a real path.
	leafFd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(leafFd) }()
	if err := unix.Faccessat(unix.AT_FDCWD, procFdPath(leafFd), mode, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// procFdPath returns the /proc/self/fd/N path for an O_PATH fd. Used by ops
// whose syscall has no fd-only variant (faccessat, fchmodat, *xattr) so the
// kernel operates on the fd's referent without re-walking the path.
func procFdPath(fd int) string {
	return fmt.Sprintf("/proc/self/fd/%d", fd)
}

func (c *ConfinedLoopbackFileSystem) Chmod(name string, mode uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	// Same symlink-escape concern as Access: Fchmodat with flags=0 follows
	// links unguarded. Resolve via openat2 RESOLVE_BENEATH first so any
	// cross-root target trips EXDEV before chmod touches the inode.
	leafFd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(leafFd) }()
	if err := unix.Fchmodat(unix.AT_FDCWD, procFdPath(leafFd), mode, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Chown(name string, uid, gid uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	if err := unix.Fchownat(parentFd, leaf, int(uid), int(gid), unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// toTimespec maps a nil *time.Time to UTIME_OMIT (the FUSE convention for
// "don't touch this stamp") and a non-nil time to a normal sec/nsec pair.
func toTimespec(t *time.Time) unix.Timespec {
	if t == nil {
		return unix.Timespec{Sec: 0, Nsec: unix.UTIME_OMIT}
	}
	return unix.Timespec{Sec: t.Unix(), Nsec: int64(t.Nanosecond())}
}

func (c *ConfinedLoopbackFileSystem) Utimens(name string, atime, mtime *time.Time, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	ts := [2]unix.Timespec{toTimespec(atime), toTimespec(mtime)}
	if err := unix.UtimesNanoAt(parentFd, leaf, ts[:], unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Truncate(name string, size uint64, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_WRONLY | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Ftruncate(fd, int64(size)); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Mkdir(name string, mode uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	if err := unix.Mkdirat(parentFd, leaf, mode); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Mknod(name string, mode, dev uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	if err := unix.Mknodat(parentFd, leaf, mode, int(dev)); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Rmdir(name string, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	if err := unix.Unlinkat(parentFd, leaf, unix.AT_REMOVEDIR); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Unlink(name string, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	if err := unix.Unlinkat(parentFd, leaf, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Rename(oldName, newName string, _ *fuse.Context) fuse.Status {
	oldParent, oldLeaf, err := resolveBeneath(c.rootFd, oldName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(oldParent) }()
	newParent, newLeaf, err := resolveBeneath(c.rootFd, newName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(newParent) }()
	if err := unix.Renameat(oldParent, oldLeaf, newParent, newLeaf); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// Symlink creates a symlink at linkName pointing at value. The value string is
// stored verbatim — confinement enforcement happens at resolve time (any
// traversal through the link goes through resolveBeneath, which has
// RESOLVE_NO_MAGICLINKS + RESOLVE_BENEATH set). We do not validate the value
// here; an absolute or escaping value is just an inert string until something
// tries to follow it.
func (c *ConfinedLoopbackFileSystem) Symlink(value, linkName string, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, linkName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	if err := unix.Symlinkat(value, parentFd, leaf); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Link(oldName, newName string, _ *fuse.Context) fuse.Status {
	oldParent, oldLeaf, err := resolveBeneath(c.rootFd, oldName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(oldParent) }()
	newParent, newLeaf, err := resolveBeneath(c.rootFd, newName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(newParent) }()
	// flags=0: don't follow the source if it's a symlink (preserve loopback behavior).
	if err := unix.Linkat(oldParent, oldLeaf, newParent, newLeaf, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// Open opens an existing file for reading or writing. The fd is opened via
// openat2 with RESOLVE_BENEATH so the returned nodefs.File is confined to the
// volume root. O_APPEND is stripped: the kernel translates offsets for us.
func (c *ConfinedLoopbackFileSystem) Open(name string, flags uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	// Mirror loopback: strip O_APPEND (kernel handles offset translation).
	flags = flags &^ uint32(syscall.O_APPEND)
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   uint64(flags) | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, errnoToStatus(err)
	}
	// os.NewFile takes ownership of fd; the embedded LoopbackFile closes it
	// via Release. RawFdFile keeps the raw fd reachable for copy/lseek.
	return NewRawFdFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
}

// Create creates (or opens with O_CREAT) a file confined to the volume root.
// O_APPEND is stripped for the same reason as Open.
func (c *ConfinedLoopbackFileSystem) Create(name string, flags, mode uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	flags = flags &^ uint32(syscall.O_APPEND)
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	// openat2 strictly rejects file-type bits (S_IFMT) in how->mode — only the
	// 12 permission/sticky/setuid/setgid bits are allowed. FUSE forwards the
	// caller's full mode (often S_IFREG | 0o644 from libc), so we mask down
	// before handing it to the kernel.
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   uint64(flags) | unix.O_CREAT | unix.O_CLOEXEC,
		Mode:    uint64(mode) & 0o7777,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, errnoToStatus(err)
	}
	return NewRawFdFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
}

// openConfinedDir opens `name` as a confined directory via openat2 and reads
// all its entries. It returns the still-open *os.File together with the
// entries so callers that need the fd alive (ReadDirPlus, for per-entry
// Fstatat) can do so. Callers that only need the entry list (OpenDir) close
// the file immediately after the call returns. Status is fuse.OK on success;
// the file and entries are nil on any error.
func (c *ConfinedLoopbackFileSystem) openConfinedDir(name string) (*os.File, []os.DirEntry, fuse.Status) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, nil, errnoToStatus(err)
	}
	defer func() { _ = unix.Close(parentFd) }()
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, nil, errnoToStatus(err)
	}
	// os.NewFile takes ownership of the fd.
	f := os.NewFile(uintptr(fd), leaf)
	entries, err := f.ReadDir(-1)
	if err != nil {
		_ = f.Close()
		return nil, nil, errnoToStatus(err)
	}
	return f, entries, fuse.OK
}

// OpenDir reads directory entries beneath the volume root. Mode and Ino are
// sourced from the underlying syscall.Stat_t, mirroring the loopback's
// fuse.ToStatT conversion exactly.
func (c *ConfinedLoopbackFileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	f, entries, st := c.openConfinedDir(name)
	if st != fuse.OK {
		return nil, st
	}
	// OpenDir does not need the fd beyond this point; close it now.
	_ = f.Close()
	out := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		d := fuse.DirEntry{Name: e.Name()}
		// info.Sys() is *syscall.Stat_t on Linux; that carries the kernel
		// mode bits (S_IFREG/S_IFDIR/…) FUSE expects, not Go's os.FileMode
		// layout. Fall back to the dirent type if per-entry stat fails.
		if info, ierr := e.Info(); ierr == nil {
			if st, ok := info.Sys().(*syscall.Stat_t); ok {
				d.Mode = st.Mode
				d.Ino = st.Ino
			}
		}
		if d.Mode == 0 {
			d.Mode = direntTypeToMode(e.Type())
		}
		out = append(out, d)
	}
	return out, fuse.OK
}

// openLeafForXattr opens an O_PATH handle to `name` confined beneath the
// volume root. Linux xattr syscalls have no *at variants, so we operate
// on the fd via /proc/self/fd/N.
func (c *ConfinedLoopbackFileSystem) openLeafForXattr(name string) (int, error) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return -1, err
	}
	defer func() { _ = unix.Close(parentFd) }()
	return unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
}

func (c *ConfinedLoopbackFileSystem) GetXAttr(name, attr string, _ *fuse.Context) ([]byte, fuse.Status) {
	fd, err := c.openLeafForXattr(name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer func() { _ = unix.Close(fd) }()
	// First call sizes the buffer; second reads it.
	size, err := unix.Getxattr(procFdPath(fd), attr, nil)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	buf := make([]byte, size)
	n, err := unix.Getxattr(procFdPath(fd), attr, buf)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	return buf[:n], fuse.OK
}

func (c *ConfinedLoopbackFileSystem) SetXAttr(name, attr string, data []byte, flags int, _ *fuse.Context) fuse.Status {
	fd, err := c.openLeafForXattr(name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Setxattr(procFdPath(fd), attr, data, flags); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) ListXAttr(name string, _ *fuse.Context) ([]string, fuse.Status) {
	fd, err := c.openLeafForXattr(name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer func() { _ = unix.Close(fd) }()
	size, err := unix.Listxattr(procFdPath(fd), nil)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	buf := make([]byte, size)
	n, err := unix.Listxattr(procFdPath(fd), buf)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	return splitNullTerminated(buf[:n]), fuse.OK
}

func (c *ConfinedLoopbackFileSystem) RemoveXAttr(name, attr string, _ *fuse.Context) fuse.Status {
	fd, err := c.openLeafForXattr(name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer func() { _ = unix.Close(fd) }()
	if err := unix.Removexattr(procFdPath(fd), attr); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

// splitNullTerminated splits a Linux listxattr buffer (NUL-terminated names
// concatenated, possibly with a trailing NUL) into a string slice.
func splitNullTerminated(b []byte) []string {
	if len(b) == 0 {
		return nil
	}
	parts := strings.Split(string(b), "\x00")
	// Trim trailing empty from the final NUL terminator, if present.
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// direntTypeToMode maps an os.DirEntry type to a Linux S_IFMT bit. Used as
// a fallback when per-entry stat fails (e.g. EACCES on a child); the
// readdir d_type alone still tells us the kind.
func direntTypeToMode(t os.FileMode) uint32 {
	switch {
	case t&os.ModeDir != 0:
		return syscall.S_IFDIR
	case t&os.ModeSymlink != 0:
		return syscall.S_IFLNK
	case t&os.ModeDevice != 0:
		if t&os.ModeCharDevice != 0 {
			return syscall.S_IFCHR
		}
		return syscall.S_IFBLK
	case t&os.ModeNamedPipe != 0:
		return syscall.S_IFIFO
	case t&os.ModeSocket != 0:
		return syscall.S_IFSOCK
	default:
		return syscall.S_IFREG
	}
}
