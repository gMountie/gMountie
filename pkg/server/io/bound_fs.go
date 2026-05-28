package io

import (
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"gmountie/pkg/utils/log"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
)

// wantsFchown reports whether the identity uses the dac_override path, which
// requires a post-create fchown to assign new entries to the principal rather
// than root.
func wantsFchown(id *Identity) bool { return id.HasCap(CapDacOverride) }

// Make syscall functions package variables for testing.
var (
	setfsuid = syscall.Setfsuid
	setfsgid = syscall.Setfsgid
)

// Identity is the minimal credential set the bound FS applies per op. Mirrors
// service.Identity (kept here to avoid an io->service import cycle).
type Identity struct {
	Uid  uint32
	Gid  uint32
	Gids []uint32
	// Caps holds the admin-capability set for this identity (e.g. "dac_override",
	// "dac_read_search"). Populated by BindIdentity from service.Identity.Caps;
	// dispatch behavior is added in Phase 3 Task 5.
	Caps []string
}

// HasCap reports whether the identity holds the named POSIX capability.
// The cap name is compared case-insensitively against the stored values.
func (id *Identity) HasCap(cap string) bool {
	for _, c := range id.Caps {
		if strings.EqualFold(c, cap) {
			return true
		}
	}
	return false
}

type identityBoundFS struct {
	pathfs.FileSystem
	id *Identity
}

// NewIdentityBoundFS wraps fs so every path op runs with id's credentials.
func NewIdentityBoundFS(fs pathfs.FileSystem, id *Identity) pathfs.FileSystem {
	return &identityBoundFS{FileSystem: fs, id: id}
}

// changeIdentity pins the current OS thread and applies the identity's
// credentials. Dispatch is based on id.Caps:
//
//   - dac_override: setgroups + setfsgid, full caps retained, fsuid stays 0.
//   - dac_read_search: setgroups + setfsgid, fsuid stays 0, EFFECTIVE capped
//     to DAC_READ_SEARCH + SETUID + SETGID (drops DAC_OVERRIDE/FOWNER/FSETID).
//   - no caps (default): setgroups + setfsgid + setfsuid to id.Uid — bit-for-
//     bit identical to the pre-Phase-3 behaviour.
//
// Returns a cleanup that restores root creds and unlocks. On any error the
// thread is left locked (tainted) so it dies with the goroutine.
func changeIdentity(id *Identity) (func(), error) {
	runtime.LockOSThread()
	switch {
	case id.HasCap(CapDacOverride):
		return applyDacOverride(id)
	case id.HasCap(CapDacReadSearch):
		return applyDacReadSearch(id)
	default:
		return applyUnprivileged(id)
	}
}

// applyUnprivileged is the original changeIdentity body (setgroups + setfsgid +
// setfsuid). The LockOSThread call now lives in the dispatcher above.
func applyUnprivileged(id *Identity) (func(), error) {
	origGroups, err := getgroups()
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsuid(int(id.Uid)); err != nil {
		_ = setfsgid(syscall.Getegid())
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setfsuid(syscall.Geteuid()); err != nil {
			log.Log.Error("restore fsuid failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("restore fsgid failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setGroupsRaw(origGroups); err != nil {
			log.Log.Error("restore groups failed; leaking OS thread", zap.Error(err))
			return
		}
		runtime.UnlockOSThread()
	}, nil
}

// applyDacReadSearch sets supplementary groups + fsgid, keeps fsuid=0, and
// reduces the EFFECTIVE capability set to DAC_READ_SEARCH + SETUID + SETGID
// only (dropping DAC_OVERRIDE/FOWNER/FSETID). PERMITTED is left intact —
// dropping from PERMITTED is irreversible within a session; DAC enforcement
// consults EFFECTIVE, so this is sufficient. Restore re-raises the saved
// effective set, restores fsgid, and restores groups.
func applyDacReadSearch(id *Identity) (func(), error) {
	origGroups, err := getgroups()
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	origCaps, err := getCaps()
	if err != nil {
		_ = setfsgid(syscall.Getegid())
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	// Keep only the bits that are already in PERMITTED (can't raise above it).
	newEff := origCaps.permitted & dacReadSearchEffectiveMask()
	if err := setCapsEffective(capState{
		effective:   newEff,
		permitted:   origCaps.permitted,
		inheritable: origCaps.inheritable,
	}); err != nil {
		_ = setfsgid(syscall.Getegid())
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setCapsEffective(origCaps); err != nil {
			log.Log.Error("restore caps failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("restore fsgid failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setGroupsRaw(origGroups); err != nil {
			log.Log.Error("restore groups failed; leaking OS thread", zap.Error(err))
			return
		}
		runtime.UnlockOSThread()
	}, nil
}

// applyDacOverride sets supplementary groups + fsgid but keeps fsuid=0 and
// retains all capabilities. New entries created while this identity is active
// will be owned by root until the caller performs an explicit post-create
// fchown; see the Create/Mkdir/Symlink/Mknod/Link wrappers in identityBoundFS.
func applyDacOverride(id *Identity) (func(), error) {
	origGroups, err := getgroups()
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("restore fsgid failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setGroupsRaw(origGroups); err != nil {
			log.Log.Error("restore groups failed; leaking OS thread", zap.Error(err))
			return
		}
		runtime.UnlockOSThread()
	}, nil
}

// setGroupsRaw sets the supplementary groups for the CURRENT OS thread only.
// Go's syscall.Setgroups broadcasts to all threads via AllThreadsSyscall, so we
// must issue the raw syscall against the calling (pinned) thread instead.
func setGroupsRaw(gids []uint32) error {
	var p uintptr
	if len(gids) > 0 {
		p = uintptr(unsafe.Pointer(&gids[0]))
	}
	if _, _, errno := syscall.RawSyscall(syscall.SYS_SETGROUPS, uintptr(len(gids)), p, 0); errno != 0 {
		return errno
	}
	return nil
}

func getgroups() ([]uint32, error) {
	g, err := syscall.Getgroups()
	if err != nil {
		return nil, err
	}
	out := make([]uint32, len(g))
	for i, v := range g {
		out[i] = uint32(v)
	}
	return out, nil
}

func (a *identityBoundFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.GetAttr(name, context)
}

func (a *identityBoundFS) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Chmod(name, mode, context)
}

func (a *identityBoundFS) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Chown(name, uid, gid, context)
}

func (a *identityBoundFS) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Utimens(name, Atime, Mtime, context)
}

func (a *identityBoundFS) Truncate(name string, size uint64, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Truncate(name, size, context)
}

func (a *identityBoundFS) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume identity", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	attr, st := a.FileSystem.GetAttr(name, context)
	if !st.Ok() {
		return st
	}
	if accessAllowed(attr, a.id, mode) {
		return fuse.OK
	}
	return fuse.EACCES
}

func (a *identityBoundFS) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := a.FileSystem.Link(oldName, newName, context)
	if st.Ok() && wantsFchown(a.id) {
		if fst := a.FileSystem.Chown(newName, a.id.Uid, a.id.Gid, context); !fst.Ok() {
			log.Log.Warn("dac_override post-create fchown failed; entry left root-owned",
				zap.String("path", newName), zap.Uint32("uid", a.id.Uid), zap.Uint32("gid", a.id.Gid),
				zap.Stringer("status", fst))
		}
	}
	return st
}

func (a *identityBoundFS) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := a.FileSystem.Mkdir(name, mode, context)
	if st.Ok() && wantsFchown(a.id) {
		if fst := a.FileSystem.Chown(name, a.id.Uid, a.id.Gid, context); !fst.Ok() {
			log.Log.Warn("dac_override post-create fchown failed; entry left root-owned",
				zap.String("path", name), zap.Uint32("uid", a.id.Uid), zap.Uint32("gid", a.id.Gid),
				zap.Stringer("status", fst))
		}
	}
	return st
}

func (a *identityBoundFS) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := a.FileSystem.Mknod(name, mode, dev, context)
	if st.Ok() && wantsFchown(a.id) {
		if fst := a.FileSystem.Chown(name, a.id.Uid, a.id.Gid, context); !fst.Ok() {
			log.Log.Warn("dac_override post-create fchown failed; entry left root-owned",
				zap.String("path", name), zap.Uint32("uid", a.id.Uid), zap.Uint32("gid", a.id.Gid),
				zap.Stringer("status", fst))
		}
	}
	return st
}

func (a *identityBoundFS) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Rename(oldName, newName, context)
}

func (a *identityBoundFS) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Rmdir(name, context)
}

func (a *identityBoundFS) Unlink(name string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Unlink(name, context)
}

func (a *identityBoundFS) GetXAttr(name string, attribute string, context *fuse.Context) (data []byte, code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.GetXAttr(name, attribute, context)
}

func (a *identityBoundFS) ListXAttr(name string, context *fuse.Context) (attributes []string, code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.ListXAttr(name, context)
}

func (a *identityBoundFS) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.RemoveXAttr(name, attr, context)
}

func (a *identityBoundFS) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.SetXAttr(name, attr, data, flags, context)
}

func (a *identityBoundFS) Open(name string, flags uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Open(name, flags, context)
}

func (a *identityBoundFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file nodefs.File, code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	f, st := a.FileSystem.Create(name, flags, mode, context)
	if st.Ok() && wantsFchown(a.id) {
		if fst := a.FileSystem.Chown(name, a.id.Uid, a.id.Gid, context); !fst.Ok() {
			log.Log.Warn("dac_override post-create fchown failed; entry left root-owned",
				zap.String("path", name), zap.Uint32("uid", a.id.Uid), zap.Uint32("gid", a.id.Gid),
				zap.Stringer("status", fst))
		}
	}
	return f, st
}

func (a *identityBoundFS) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.OpenDir(name, context)
}

func (a *identityBoundFS) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := a.FileSystem.Symlink(value, linkName, context)
	if st.Ok() && wantsFchown(a.id) {
		if fst := a.FileSystem.Chown(linkName, a.id.Uid, a.id.Gid, context); !fst.Ok() {
			log.Log.Warn("dac_override post-create fchown failed; entry left root-owned",
				zap.String("path", linkName), zap.Uint32("uid", a.id.Uid), zap.Uint32("gid", a.id.Gid),
				zap.Stringer("status", fst))
		}
	}
	return st
}

func (a *identityBoundFS) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	cleanup, err := changeIdentity(a.id)
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return "", fuse.EPERM
	}
	defer cleanup()
	return a.FileSystem.Readlink(name, context)
}
