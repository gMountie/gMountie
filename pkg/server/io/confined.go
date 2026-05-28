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

func (c *ConfinedLoopbackFileSystem) Chmod(name string, mode uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	if err := unix.Fchmodat(parentFd, leaf, mode, 0); err != nil {
		return errnoToStatus(err)
	}
	return fuse.OK
}

func (c *ConfinedLoopbackFileSystem) Chown(name string, uid, gid uint32, _ *fuse.Context) fuse.Status {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(parentFd)
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
	defer unix.Close(parentFd)
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
	defer unix.Close(parentFd)
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_WRONLY | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(fd)
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
	defer unix.Close(parentFd)
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
	defer unix.Close(parentFd)
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
	defer unix.Close(parentFd)
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
	defer unix.Close(parentFd)
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
	defer unix.Close(oldParent)
	newParent, newLeaf, err := resolveBeneath(c.rootFd, newName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(newParent)
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
	defer unix.Close(parentFd)
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
	defer unix.Close(oldParent)
	newParent, newLeaf, err := resolveBeneath(c.rootFd, newName)
	if err != nil {
		return errnoToStatus(err)
	}
	defer unix.Close(newParent)
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
	defer unix.Close(parentFd)
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   uint64(flags) | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, errnoToStatus(err)
	}
	// os.NewFile takes ownership of fd; LoopbackFile closes it via Release.
	return nodefs.NewLoopbackFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
}

// Create creates (or opens with O_CREAT) a file confined to the volume root.
// O_APPEND is stripped for the same reason as Open.
func (c *ConfinedLoopbackFileSystem) Create(name string, flags, mode uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	flags = flags &^ uint32(syscall.O_APPEND)
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   uint64(flags) | unix.O_CREAT | unix.O_CLOEXEC,
		Mode:    uint64(mode),
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, errnoToStatus(err)
	}
	return nodefs.NewLoopbackFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
}

// OpenDir reads directory entries beneath the volume root. Mode and Ino are
// sourced from the underlying syscall.Stat_t, mirroring the loopback's
// fuse.ToStatT conversion exactly.
func (c *ConfinedLoopbackFileSystem) OpenDir(name string, _ *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	parentFd, leaf, err := resolveBeneath(c.rootFd, name)
	if err != nil {
		return nil, errnoToStatus(err)
	}
	defer unix.Close(parentFd)
	// Open the directory itself, confined via openat2.
	fd, err := unix.Openat2(parentFd, leaf, &unix.OpenHow{
		Flags:   unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, errnoToStatus(err)
	}
	// os.NewFile takes ownership of the fd; Close releases it.
	f := os.NewFile(uintptr(fd), leaf)
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, errnoToStatus(err)
	}
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
