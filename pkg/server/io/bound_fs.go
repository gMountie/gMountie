package io

import (
	"runtime"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// wantsFchown reports whether the identity uses the dac_override path, which
// requires a post-create fchown to assign new entries to the principal rather
// than root.
func wantsFchown(id *Identity) bool { return id.HasCap(CapDacOverride) }

// Syscall + thread seams, injectable for the non-root failure-policy tests in
// bound_fs_rollback_test.go. Production never reassigns them.
//
// setfsuid/setfsgid point at read-back-verified wrappers: on Linux,
// SYS_SETFSUID/SYS_SETFSGID never set errno — the kernel returns the PREVIOUS
// id — so Go's syscall.Setfsuid/Setfsgid always return nil and a silently
// ignored credential change (e.g. a failed restore to root) would otherwise be
// treated as success. The wrappers below probe the current value back and
// surface a mismatch as the error the apply/cleanup branches handle.
var (
	setfsuid         = setfsuidVerified
	setfsgid         = setfsgidVerified
	setGroupsRaw     = setGroupsRawThread
	getgroups        = getgroupsImpl
	getCaps          = capgetThread
	setCapsEffective = capsetEffectiveThread
	lockOSThread     = runtime.LockOSThread
	unlockOSThread   = runtime.UnlockOSThread
)

// rawSetfsuid issues SYS_SETFSUID on the calling thread and returns the
// PREVIOUS fsuid. The kernel never reports an error for this syscall; calling
// it with -1 (an invalid uid) is the documented probe idiom — nothing changes
// and the current fsuid is returned.
func rawSetfsuid(uid int) int {
	prev, _, _ := syscall.RawSyscall(syscall.SYS_SETFSUID, uintptr(uid), 0, 0)
	return int(prev)
}

// rawSetfsgid is rawSetfsuid for the filesystem gid.
func rawSetfsgid(gid int) int {
	prev, _, _ := syscall.RawSyscall(syscall.SYS_SETFSGID, uintptr(gid), 0, 0)
	return int(prev)
}

// setfsuidVerified sets the calling thread's filesystem uid and verifies the
// change took effect by reading the current value back. A mismatch is the
// only observable failure mode of setfsuid(2).
func setfsuidVerified(uid int) error {
	rawSetfsuid(uid)
	if cur := rawSetfsuid(-1); cur != uid {
		return errors.Errorf("setfsuid(%d) did not take effect (fsuid is %d)", uid, cur)
	}
	return nil
}

// setfsgidVerified is setfsuidVerified for the filesystem gid.
func setfsgidVerified(gid int) error {
	rawSetfsgid(gid)
	if cur := rawSetfsgid(-1); cur != gid {
		return errors.Errorf("setfsgid(%d) did not take effect (fsgid is %d)", gid, cur)
	}
	return nil
}

// Identity is the minimal credential set the bound FS applies per op. Mirrors
// service.Identity (kept here to avoid an io->service import cycle).
type Identity struct {
	Uid  uint32
	Gid  uint32
	Gids []uint32
	// Caps holds the admin-capability set for this identity (e.g. "dac_override",
	// "dac_read_search"). Populated by BindIdentity from service.Identity.Caps.
	Caps []string
}

// HasCap reports whether the identity holds the named POSIX capability.
// The cap name is compared case-insensitively against the stored values.
func (id *Identity) HasCap(name string) bool {
	for _, c := range id.Caps {
		if strings.EqualFold(c, name) {
			return true
		}
	}
	return false
}

// NewIdentityBoundFS wraps fs so every path op runs with id's credentials.
// Implemented as a resolverBoundFS over a constant resolver: the identity is
// snapshotted at construction (the passthrough mode builds a fresh wrapper per
// RPC from the wire caller), and all per-op machinery — thread pinning,
// credential switch, dac_override post-create chown, the Access check — is
// shared with the resolver-bound path instead of being duplicated.
func NewIdentityBoundFS(fs pathfs.FileSystem, id *Identity) pathfs.FileSystem {
	fixed := *id
	return NewResolverBoundFS(fs, func(string) (Identity, error) { return fixed, nil }, "")
}

// IdentityResolveFunc resolves an authenticated principal to an io.Identity.
// The service package provides this as a closure, keeping io free of import
// cycles while allowing per-op fresh resolution via the TTL-cached resolver.
type IdentityResolveFunc func(principal string) (Identity, error)

// resolverBoundFS is a cached-wrapper variant where every path op resolves the
// identity fresh via fn (backed by a TTL-cached resolver) before pinning the
// thread and changing credentials. This makes caching the wrapper itself
// trivially safe: freshness is fully delegated to the resolver's TTL.
type resolverBoundFS struct {
	pathfs.FileSystem
	resolve   IdentityResolveFunc
	principal string
}

// NewResolverBoundFS wraps fs so every path op resolves identity via fn (which
// is typically a closure over a TTL-cached IdentityResolver) before applying
// credentials. Safe to cache permanently — no identity snapshot is stored.
func NewResolverBoundFS(fs pathfs.FileSystem, fn IdentityResolveFunc, principal string) pathfs.FileSystem {
	return &resolverBoundFS{FileSystem: fs, resolve: fn, principal: principal}
}

// changeIdentityFor resolves the principal once and applies its credentials,
// returning the resolved identity alongside the cleanup func. Callers that
// need the identity after the switch (Access check, dac_override post-create
// chown) MUST use the returned value rather than re-resolving — a TTL refresh
// between two resolves could otherwise check permissions against a different
// identity than the credentials actually applied.
func (r *resolverBoundFS) changeIdentityFor() (Identity, func(), error) {
	id, err := r.resolve(r.principal)
	if err != nil {
		return Identity{}, nil, err
	}
	cleanup, err := changeIdentity(&id)
	return id, cleanup, err
}

// maybeChownNew assigns a just-created entry to the principal when the
// identity uses the dac_override path (fsuid stays 0 there, so new entries
// would otherwise be root-owned). Failure is logged, not fatal — the entry
// exists and is usable, just root-owned.
func (r *resolverBoundFS) maybeChownNew(path string, id *Identity, context *fuse.Context) {
	if !wantsFchown(id) {
		return
	}
	if fst := r.FileSystem.Chown(path, id.Uid, id.Gid, context); !fst.Ok() {
		log.Log.Warn("dac_override post-create fchown failed; entry left root-owned",
			zap.String("path", path), zap.Uint32("uid", id.Uid), zap.Uint32("gid", id.Gid),
			zap.Stringer("status", fst))
	}
}

func (r *resolverBoundFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.GetAttr(name, context)
}

func (r *resolverBoundFS) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Chmod(name, mode, context)
}

func (r *resolverBoundFS) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Chown(name, uid, gid, context)
}

func (r *resolverBoundFS) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Utimens(name, Atime, Mtime, context)
}

func (r *resolverBoundFS) Truncate(name string, size uint64, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Truncate(name, size, context)
}

func (r *resolverBoundFS) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume identity", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	attr, st := r.FileSystem.GetAttr(name, context)
	if !st.Ok() {
		return st
	}
	// The permission check runs against the SAME identity whose credentials
	// are applied — id came from the single resolve in changeIdentityFor.
	if accessAllowed(attr, &id, mode) {
		return fuse.OK
	}
	return fuse.EACCES
}

func (r *resolverBoundFS) Link(oldName string, newName string, context *fuse.Context) fuse.Status {
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := r.FileSystem.Link(oldName, newName, context)
	if st.Ok() {
		r.maybeChownNew(newName, &id, context)
	}
	return st
}

func (r *resolverBoundFS) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := r.FileSystem.Mkdir(name, mode, context)
	if st.Ok() {
		r.maybeChownNew(name, &id, context)
	}
	return st
}

func (r *resolverBoundFS) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := r.FileSystem.Mknod(name, mode, dev, context)
	if st.Ok() {
		r.maybeChownNew(name, &id, context)
	}
	return st
}

func (r *resolverBoundFS) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Rename(oldName, newName, context)
}

func (r *resolverBoundFS) Rmdir(name string, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Rmdir(name, context)
}

func (r *resolverBoundFS) Unlink(name string, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Unlink(name, context)
}

func (r *resolverBoundFS) GetXAttr(name string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.GetXAttr(name, attribute, context)
}

func (r *resolverBoundFS) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.ListXAttr(name, context)
}

func (r *resolverBoundFS) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.RemoveXAttr(name, attr, context)
}

func (r *resolverBoundFS) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.SetXAttr(name, attr, data, flags, context)
}

func (r *resolverBoundFS) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Open(name, flags, context)
}

func (r *resolverBoundFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	f, st := r.FileSystem.Create(name, flags, mode, context)
	if st.Ok() {
		r.maybeChownNew(name, &id, context)
	}
	return f, st
}

func (r *resolverBoundFS) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return nil, fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.OpenDir(name, context)
}

func (r *resolverBoundFS) Symlink(value string, linkName string, context *fuse.Context) fuse.Status {
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	st := r.FileSystem.Symlink(value, linkName, context)
	if st.Ok() {
		r.maybeChownNew(linkName, &id, context)
	}
	return st
}

func (r *resolverBoundFS) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	_, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return "", fuse.EPERM
	}
	defer cleanup()
	return r.FileSystem.Readlink(name, context)
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
// Returns a cleanup that restores root creds and unlocks. On any error —
// during apply or during a later restore — the thread is left locked
// (tainted) so it dies with the goroutine; see rollback for the single
// policy implementation.
func changeIdentity(id *Identity) (func(), error) {
	lockOSThread()
	switch {
	case id.HasCap(CapDacOverride):
		return applyDacOverride(id)
	case id.HasCap(CapDacReadSearch):
		return applyDacReadSearch(id)
	default:
		return applyUnprivileged(id)
	}
}

// restoreState describes what a rollback must undo. Zero-value fields mean
// "nothing was changed at that level, skip it".
type restoreState struct {
	fsuid  bool      // restore fsuid to the process euid
	fsgid  bool      // restore fsgid to the process egid
	groups []uint32  // restore supplementary groups when non-nil
	caps   *capState // restore the capability triple when non-nil
}

// rollback undoes credential changes in reverse-apply order (caps → fsuid →
// fsgid → groups) and unlocks the OS thread ONLY when every restore
// succeeded. On any restore failure it logs and returns with the thread still
// locked: the thread carries partially-foreign credentials and must die with
// its goroutine rather than re-enter the scheduler pool. This is the single
// unwind policy for both mid-apply failures and the post-op cleanups — the
// two used to diverge (mid-apply unwinds unlocked unconditionally after
// best-effort restores with ignored errors).
func rollback(st restoreState) {
	if st.caps != nil {
		if err := setCapsEffective(*st.caps); err != nil {
			log.Log.Error("restore caps failed; leaking OS thread", zap.Error(err))
			return
		}
	}
	if st.fsuid {
		if err := setfsuid(syscall.Geteuid()); err != nil {
			log.Log.Error("restore fsuid failed; leaking OS thread", zap.Error(err))
			return
		}
	}
	if st.fsgid {
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("restore fsgid failed; leaking OS thread", zap.Error(err))
			return
		}
	}
	if st.groups != nil {
		if err := setGroupsRaw(st.groups); err != nil {
			log.Log.Error("restore groups failed; leaking OS thread", zap.Error(err))
			return
		}
	}
	unlockOSThread()
}

// applyUnprivileged is the classic credential switch (setgroups + setfsgid +
// setfsuid). The LockOSThread call lives in the dispatcher above.
func applyUnprivileged(id *Identity) (func(), error) {
	origGroups, err := getgroups()
	if err != nil {
		rollback(restoreState{}) // nothing applied; just unlock
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		rollback(restoreState{})
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		rollback(restoreState{groups: origGroups})
		return nil, err
	}
	if err := setfsuid(int(id.Uid)); err != nil {
		rollback(restoreState{fsgid: true, groups: origGroups})
		return nil, err
	}
	return func() {
		rollback(restoreState{fsuid: true, fsgid: true, groups: origGroups})
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
		rollback(restoreState{})
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		rollback(restoreState{})
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		rollback(restoreState{groups: origGroups})
		return nil, err
	}
	origCaps, err := getCaps()
	if err != nil {
		rollback(restoreState{fsgid: true, groups: origGroups})
		return nil, err
	}
	// Keep only the bits that are already in PERMITTED (can't raise above it).
	newEff := origCaps.permitted & dacReadSearchEffectiveMask()
	if err := setCapsEffective(capState{
		effective:   newEff,
		permitted:   origCaps.permitted,
		inheritable: origCaps.inheritable,
	}); err != nil {
		rollback(restoreState{fsgid: true, groups: origGroups})
		return nil, err
	}
	return func() {
		rollback(restoreState{caps: &origCaps, fsgid: true, groups: origGroups})
	}, nil
}

// applyDacOverride sets supplementary groups + fsgid but keeps fsuid=0 and
// retains all capabilities. New entries created while this identity is active
// will be owned by root until the caller performs an explicit post-create
// fchown; see maybeChownNew and the Create/Mkdir/Symlink/Mknod/Link wrappers.
func applyDacOverride(id *Identity) (func(), error) {
	origGroups, err := getgroups()
	if err != nil {
		rollback(restoreState{})
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		rollback(restoreState{})
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		rollback(restoreState{groups: origGroups})
		return nil, err
	}
	return func() {
		rollback(restoreState{fsgid: true, groups: origGroups})
	}, nil
}

// setGroupsRawThread sets the supplementary groups for the CURRENT OS thread
// only. Go's syscall.Setgroups broadcasts to all threads via
// AllThreadsSyscall, so we must issue the raw syscall against the calling
// (pinned) thread instead.
func setGroupsRawThread(gids []uint32) error {
	var p uintptr
	if len(gids) > 0 {
		p = uintptr(unsafe.Pointer(&gids[0]))
	}
	if _, _, errno := syscall.RawSyscall(syscall.SYS_SETGROUPS, uintptr(len(gids)), p, 0); errno != 0 {
		return errno
	}
	return nil
}

func getgroupsImpl() ([]uint32, error) {
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
