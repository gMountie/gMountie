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
//
// When exec is non-nil (constant-identity volumes — squash/static) ops skip
// the per-op resolve + credential switch entirely and run on the executor's
// pre-credentialed worker threads instead; see runAs.
type resolverBoundFS struct {
	pathfs.FileSystem
	resolve   IdentityResolveFunc
	principal string

	// exec, when non-nil, is the pre-credentialed executor fast path. execID
	// is the identity its workers carry, snapshotted at construction — only
	// valid because the resolver is constant by contract (see
	// NewConstantResolverBoundFS); per-principal modes leave exec nil.
	exec   *identityExecutor
	execID Identity
}

// NewResolverBoundFS wraps fs so every path op resolves identity via fn (which
// is typically a closure over a TTL-cached IdentityResolver) before applying
// credentials. Safe to cache permanently — no identity snapshot is stored.
func NewResolverBoundFS(fs pathfs.FileSystem, fn IdentityResolveFunc, principal string) pathfs.FileSystem {
	return &resolverBoundFS{FileSystem: fs, resolve: fn, principal: principal}
}

// NewConstantResolverBoundFS is NewResolverBoundFS for volumes whose resolver
// returns the same identity for this principal on every call for the process
// lifetime (squash/static modes). Ops are routed through the process-wide
// pre-credentialed executor for id — replacing the per-op thread pin + ~9
// credential syscalls with a channel hop to an already-switched worker. If
// the executor cannot start (executorFor returns nil), the wrapper degrades
// to the per-op switching path, byte-for-byte the NewResolverBoundFS
// behavior.
//
// id MUST be the identity fn resolves to — callers (BindIdentity) have just
// resolved it. workers <= 0 selects the default pool size.
func NewConstantResolverBoundFS(fs pathfs.FileSystem, fn IdentityResolveFunc, principal string, id Identity, workers int) pathfs.FileSystem {
	return &resolverBoundFS{
		FileSystem: fs,
		resolve:    fn,
		principal:  principal,
		exec:       executors.executorFor(id, workers),
		execID:     id,
	}
}

// runAs executes op with the bound identity's credentials applied, via one of
// two paths:
//
//   - exec != nil (constant-identity volumes): dispatch to the pre-
//     credentialed executor — no per-op resolve, no credential syscalls. The
//     identity handed to op is the construction-time snapshot, which equals
//     what the resolver would return (constant by contract).
//   - exec == nil: the classic per-op path — resolve the principal fresh, pin
//     the thread, switch credentials, run op, restore.
//
// Returns OK when op ran (op reports its own result via captured variables)
// and EPERM — without running op, failing closed — when the identity could
// not be resolved or applied.
func (r *resolverBoundFS) runAs(op func(id *Identity)) fuse.Status {
	if r.exec != nil {
		r.exec.do(func() { op(&r.execID) })
		return fuse.OK
	}
	id, cleanup, err := r.changeIdentityFor()
	if err != nil {
		log.Log.Error("failed to assume user", zap.Error(err))
		return fuse.EPERM
	}
	defer cleanup()
	op(&id)
	return fuse.OK
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
	var (
		attr *fuse.Attr
		st   fuse.Status
	)
	if rs := r.runAs(func(*Identity) { attr, st = r.FileSystem.GetAttr(name, context) }); !rs.Ok() {
		return nil, rs
	}
	return attr, st
}

func (r *resolverBoundFS) Chmod(name string, mode uint32, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Chmod(name, mode, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Chown(name, uid, gid, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Utimens(name string, Atime *time.Time, Mtime *time.Time, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Utimens(name, Atime, Mtime, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Truncate(name string, size uint64, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Truncate(name, size, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Access(name string, mode uint32, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(id *Identity) {
		attr, gst := r.FileSystem.GetAttr(name, context)
		if !gst.Ok() {
			st = gst
			return
		}
		// The permission check runs against the SAME identity whose credentials
		// are applied — id is either the single resolve of this op or the
		// executor's construction-time constant.
		if accessAllowed(attr, id, mode) {
			st = fuse.OK
		} else {
			st = fuse.EACCES
		}
	}); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Link(oldName string, newName string, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(id *Identity) {
		st = r.FileSystem.Link(oldName, newName, context)
		if st.Ok() {
			r.maybeChownNew(newName, id, context)
		}
	}); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(id *Identity) {
		st = r.FileSystem.Mkdir(name, mode, context)
		if st.Ok() {
			r.maybeChownNew(name, id, context)
		}
	}); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(id *Identity) {
		st = r.FileSystem.Mknod(name, mode, dev, context)
		if st.Ok() {
			r.maybeChownNew(name, id, context)
		}
	}); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Rename(oldName string, newName string, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Rename(oldName, newName, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Rmdir(name string, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Rmdir(name, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Unlink(name string, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.Unlink(name, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) GetXAttr(name string, attribute string, context *fuse.Context) ([]byte, fuse.Status) {
	var (
		data []byte
		st   fuse.Status
	)
	if rs := r.runAs(func(*Identity) { data, st = r.FileSystem.GetXAttr(name, attribute, context) }); !rs.Ok() {
		return nil, rs
	}
	return data, st
}

func (r *resolverBoundFS) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
	var (
		attrs []string
		st    fuse.Status
	)
	if rs := r.runAs(func(*Identity) { attrs, st = r.FileSystem.ListXAttr(name, context) }); !rs.Ok() {
		return nil, rs
	}
	return attrs, st
}

func (r *resolverBoundFS) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.RemoveXAttr(name, attr, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(*Identity) { st = r.FileSystem.SetXAttr(name, attr, data, flags, context) }); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Open(name string, flags uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	var (
		f  nodefs.File
		st fuse.Status
	)
	if rs := r.runAs(func(*Identity) { f, st = r.FileSystem.Open(name, flags, context) }); !rs.Ok() {
		return nil, rs
	}
	return f, st
}

func (r *resolverBoundFS) Create(name string, flags uint32, mode uint32, context *fuse.Context) (nodefs.File, fuse.Status) {
	var (
		f  nodefs.File
		st fuse.Status
	)
	if rs := r.runAs(func(id *Identity) {
		f, st = r.FileSystem.Create(name, flags, mode, context)
		if st.Ok() {
			r.maybeChownNew(name, id, context)
		}
	}); !rs.Ok() {
		return nil, rs
	}
	return f, st
}

func (r *resolverBoundFS) OpenDir(name string, context *fuse.Context) ([]fuse.DirEntry, fuse.Status) {
	var (
		entries []fuse.DirEntry
		st      fuse.Status
	)
	if rs := r.runAs(func(*Identity) { entries, st = r.FileSystem.OpenDir(name, context) }); !rs.Ok() {
		return nil, rs
	}
	return entries, st
}

// ReadDirPlus forwards under the SAME credential switch as OpenDir — the
// listing and the per-entry stats all run with the principal's fsuid/fsgid.
// When the wrapped FS lacks the capability, fall back to its OpenDir with nil
// attrs (still under the switched credentials) so the wrapper satisfies
// ReadDirPlusser unconditionally without inventing attributes.
func (r *resolverBoundFS) ReadDirPlus(name string, context *fuse.Context) ([]DirEntryPlus, fuse.Status) {
	var (
		out []DirEntryPlus
		st  fuse.Status
	)
	if rs := r.runAs(func(*Identity) {
		if rdp, ok := r.FileSystem.(ReadDirPlusser); ok {
			out, st = rdp.ReadDirPlus(name, context)
			return
		}
		// Inner FS lacks the capability: degrade to OpenDir with nil attrs
		// (never invent attributes) under the same identity.
		entries, ost := r.FileSystem.OpenDir(name, context)
		if !ost.Ok() {
			out, st = nil, ost
			return
		}
		plus := make([]DirEntryPlus, len(entries))
		for i, e := range entries {
			plus[i] = DirEntryPlus{Entry: e}
		}
		out, st = plus, ost
	}); !rs.Ok() {
		return nil, rs
	}
	return out, st
}

func (r *resolverBoundFS) Symlink(value string, linkName string, context *fuse.Context) fuse.Status {
	var st fuse.Status
	if rs := r.runAs(func(id *Identity) {
		st = r.FileSystem.Symlink(value, linkName, context)
		if st.Ok() {
			r.maybeChownNew(linkName, id, context)
		}
	}); !rs.Ok() {
		return rs
	}
	return st
}

func (r *resolverBoundFS) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
	var (
		target string
		st     fuse.Status
	)
	if rs := r.runAs(func(*Identity) { target, st = r.FileSystem.Readlink(name, context) }); !rs.Ok() {
		return "", rs
	}
	return target, st
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
//
// This is the PER-OP path (save → apply → op → restore). identityExecutor
// workers share the apply step via applyCreds but skip the save/restore
// halves entirely: their threads are switched once and never come back.
func changeIdentity(id *Identity) (func(), error) {
	lockOSThread()
	origGroups, err := getgroups()
	if err != nil {
		rollback(restoreState{}) // nothing applied; just unlock
		return nil, err
	}
	st, err := applyCreds(id, origGroups)
	if err != nil {
		rollback(st) // undo exactly the levels that were applied
		return nil, err
	}
	return func() { rollback(st) }, nil
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

// applyCreds applies id's credentials to the CALLING thread — apply only: no
// thread pinning, no original-state capture, no rollback. It returns the
// cumulative restoreState describing exactly what WAS applied (the full set on
// success, the partial set on mid-sequence failure) so the two callers can
// each impose their own unwind policy:
//
//   - changeIdentity (per-op path) passes the saved origGroups, and feeds the
//     returned state to rollback — both on mid-apply failure and as the post-op
//     cleanup.
//   - identityExecutor workers (constant-identity path) pass origGroups=nil
//     and discard the state: their threads are switched once at startup and
//     never restored.
//
// origGroups is only embedded into the returned state (restoreState.groups);
// it does not affect what is applied.
func applyCreds(id *Identity, origGroups []uint32) (restoreState, error) {
	switch {
	case id.HasCap(CapDacOverride):
		return applyDacOverrideCreds(id, origGroups)
	case id.HasCap(CapDacReadSearch):
		return applyDacReadSearchCreds(id, origGroups)
	default:
		return applyUnprivilegedCreds(id, origGroups)
	}
}

// applyUnprivilegedCreds is the classic credential switch (setgroups +
// setfsgid + setfsuid) — bit-for-bit the pre-Phase-3 sequence.
func applyUnprivilegedCreds(id *Identity, origGroups []uint32) (restoreState, error) {
	st, err := applyDacOverrideCreds(id, origGroups) // same groups+fsgid prefix
	if err != nil {
		return st, err
	}
	if err := setfsuid(int(id.Uid)); err != nil {
		return st, err
	}
	st.fsuid = true
	return st, nil
}

// applyDacReadSearchCreds sets supplementary groups + fsgid, keeps fsuid=0,
// and reduces the EFFECTIVE capability set to DAC_READ_SEARCH + SETUID +
// SETGID only (dropping DAC_OVERRIDE/FOWNER/FSETID). PERMITTED is left
// intact — dropping from PERMITTED is irreversible within a session; DAC
// enforcement consults EFFECTIVE, so this is sufficient. The returned state
// re-raises the saved effective set first, then fsgid, then groups.
func applyDacReadSearchCreds(id *Identity, origGroups []uint32) (restoreState, error) {
	st, err := applyDacOverrideCreds(id, origGroups)
	if err != nil {
		return st, err
	}
	origCaps, err := getCaps()
	if err != nil {
		return st, err
	}
	// Keep only the bits that are already in PERMITTED (can't raise above it).
	newEff := origCaps.permitted & dacReadSearchEffectiveMask()
	if err := setCapsEffective(capState{
		effective:   newEff,
		permitted:   origCaps.permitted,
		inheritable: origCaps.inheritable,
	}); err != nil {
		return st, err
	}
	st.caps = &origCaps
	return st, nil
}

// applyDacOverrideCreds sets supplementary groups + fsgid but keeps fsuid=0
// and retains all capabilities. New entries created while this identity is
// active will be owned by root until the caller performs an explicit
// post-create fchown; see maybeChownNew and the Create/Mkdir/Symlink/Mknod/
// Link wrappers. Also the shared groups+fsgid prefix of the other two paths.
func applyDacOverrideCreds(id *Identity, origGroups []uint32) (restoreState, error) {
	st := restoreState{}
	if err := setGroupsRaw(id.Gids); err != nil {
		return st, err
	}
	st.groups = origGroups
	if err := setfsgid(int(id.Gid)); err != nil {
		return st, err
	}
	st.fsgid = true
	return st, nil
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
