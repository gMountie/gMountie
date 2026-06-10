package io

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

// ResolverBoundFSExecSuite proves the executor fast path of resolverBoundFS:
// when exec is non-nil, ops are dispatched to the pre-credentialed pool and
// NEVER touch the per-op machinery (resolve, thread pin, credential seams);
// when exec is nil, the per-op path is byte-for-byte the pre-executor
// behavior. Runs unprivileged through the same syscall seams the rollback
// suite stubs.
type ResolverBoundFSExecSuite struct {
	suite.Suite

	resolveCalls   atomic.Int64
	getgroupsCalls atomic.Int64
	setgroupsCalls atomic.Int64
	setfsgidCalls  atomic.Int64
	setfsuidCalls  atomic.Int64
	lockCalls      atomic.Int64

	origSetfsuid     func(int) error
	origSetfsgid     func(int) error
	origSetGroupsRaw func([]uint32) error
	origGetgroups    func() ([]uint32, error)
	origLock         func()
	origUnlock       func()
}

func TestResolverBoundFSExecSuite(t *testing.T) { suite.Run(t, new(ResolverBoundFSExecSuite)) }

func (s *ResolverBoundFSExecSuite) SetupTest() {
	s.resolveCalls.Store(0)
	s.getgroupsCalls.Store(0)
	s.setgroupsCalls.Store(0)
	s.setfsgidCalls.Store(0)
	s.setfsuidCalls.Store(0)
	s.lockCalls.Store(0)
	s.origSetfsuid, s.origSetfsgid = setfsuid, setfsgid
	s.origSetGroupsRaw, s.origGetgroups = setGroupsRaw, getgroups
	s.origLock, s.origUnlock = lockOSThread, unlockOSThread

	setfsuid = func(int) error { s.setfsuidCalls.Add(1); return nil }
	setfsgid = func(int) error { s.setfsgidCalls.Add(1); return nil }
	setGroupsRaw = func([]uint32) error { s.setgroupsCalls.Add(1); return nil }
	getgroups = func() ([]uint32, error) { s.getgroupsCalls.Add(1); return []uint32{0}, nil }
	lockOSThread = func() { s.lockCalls.Add(1) }
	unlockOSThread = func() {}
}

func (s *ResolverBoundFSExecSuite) TearDownTest() {
	setfsuid, setfsgid = s.origSetfsuid, s.origSetfsgid
	setGroupsRaw, getgroups = s.origSetGroupsRaw, s.origGetgroups
	lockOSThread, unlockOSThread = s.origLock, s.origUnlock
}

func (s *ResolverBoundFSExecSuite) countingResolve(id Identity) IdentityResolveFunc {
	return func(string) (Identity, error) {
		s.resolveCalls.Add(1)
		return id, nil
	}
}

// boundWithExec builds a resolverBoundFS over a loopback with a live (seam-
// stubbed) executor, then zeroes the counters so tests observe only per-op
// activity, not worker startup.
func (s *ResolverBoundFSExecSuite) boundWithExec(id Identity) *resolverBoundFS {
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	e, err := newIdentityExecutor(id, 2)
	s.Require().NoError(err)
	s.T().Cleanup(e.shutdown)
	r := &resolverBoundFS{FileSystem: base, resolve: s.countingResolve(id), principal: "alice", exec: e, execID: id}
	s.resetCounters()
	return r
}

func (s *ResolverBoundFSExecSuite) resetCounters() {
	s.resolveCalls.Store(0)
	s.getgroupsCalls.Store(0)
	s.setgroupsCalls.Store(0)
	s.setfsgidCalls.Store(0)
	s.setfsuidCalls.Store(0)
	s.lockCalls.Store(0)
}

func (s *ResolverBoundFSExecSuite) assertNoPerOpMachinery() {
	s.T().Helper()
	s.EqualValues(0, s.resolveCalls.Load(), "exec path must not resolve per op")
	s.EqualValues(0, s.getgroupsCalls.Load(), "exec path must not save groups per op")
	s.EqualValues(0, s.setgroupsCalls.Load(), "exec path must not setgroups per op")
	s.EqualValues(0, s.setfsgidCalls.Load(), "exec path must not setfsgid per op")
	s.EqualValues(0, s.setfsuidCalls.Load(), "exec path must not setfsuid per op")
	s.EqualValues(0, s.lockCalls.Load(), "exec path must not pin the calling thread per op")
}

// TestExecPathRoutesOpsWithoutCredentialSwitch is the headline property: with
// an executor, GetAttr/Open/Mkdir/OpenDir/Create run to completion against
// the inner FS with ZERO resolves and ZERO credential-seam calls.
func (s *ResolverBoundFSExecSuite) TestExecPathRoutesOpsWithoutCredentialSwitch() {
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	r := s.boundWithExec(id)

	attr, st := r.GetAttr("", nil)
	s.Require().True(st.Ok(), "GetAttr through the executor must reach the inner FS")
	s.Require().NotNil(attr)

	s.Require().True(r.Mkdir("d", 0o755, nil).Ok())
	entries, st := r.OpenDir("", nil)
	s.Require().True(st.Ok())
	s.Require().Len(entries, 1)
	s.Equal("d", entries[0].Name)

	f, st := r.Create("f", uint32(syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC), 0o644, nil)
	s.Require().True(st.Ok())
	f.Release()
	f, st = r.Open("f", 0, nil)
	s.Require().True(st.Ok())
	f.Release()

	s.assertNoPerOpMachinery()
}

// TestNilExecUsesPerOpPath: without an executor the classic sequence is
// intact — per op: one resolve, one thread pin, one groups save, then the
// apply + restore credential calls.
func (s *ResolverBoundFSExecSuite) TestNilExecUsesPerOpPath() {
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := &resolverBoundFS{FileSystem: base, resolve: s.countingResolve(id), principal: "alice"}

	_, st := r.GetAttr("", nil)
	s.Require().True(st.Ok())
	s.EqualValues(1, s.resolveCalls.Load(), "per-op path resolves once per op")
	s.EqualValues(1, s.getgroupsCalls.Load(), "per-op path saves original groups")
	s.EqualValues(1, s.lockCalls.Load(), "per-op path pins the thread")
	s.EqualValues(2, s.setgroupsCalls.Load(), "apply + restore")
	s.EqualValues(2, s.setfsgidCalls.Load(), "apply + restore")
	s.EqualValues(2, s.setfsuidCalls.Load(), "apply + restore")

	_, st = r.GetAttr("", nil)
	s.Require().True(st.Ok())
	s.EqualValues(2, s.resolveCalls.Load(), "every op resolves afresh")
}

// TestNilExecResolveFailureFailsClosed: a resolve failure on the per-op path
// is still EPERM and the inner FS is never reached.
func (s *ResolverBoundFSExecSuite) TestNilExecResolveFailureFailsClosed() {
	inner := &recordingFS{FileSystem: pathfs.NewDefaultFileSystem()}
	r := &resolverBoundFS{
		FileSystem: inner,
		resolve:    func(string) (Identity, error) { return Identity{}, errors.New("no such principal") },
		principal:  "mallory",
	}
	_, st := r.GetAttr("x", nil)
	s.Equal(fuse.EPERM, st)
	s.Equal(fuse.EPERM, r.Mkdir("x", 0o755, nil))
	s.EqualValues(0, inner.getattrCalls.Load(), "inner FS must not be reached on resolve failure")
	s.EqualValues(0, inner.mkdirCalls.Load())
}

// TestExecPathPostCreateChownUsesConstantIdentity: the dac_override
// post-create chown must still fire on the executor path, using the
// construction-time identity.
func (s *ResolverBoundFSExecSuite) TestExecPathPostCreateChownUsesConstantIdentity() {
	id := Identity{Uid: 2000, Gid: 2001, Gids: []uint32{2001}, Caps: []string{CapDacOverride}}
	inner := &recordingFS{FileSystem: pathfs.NewDefaultFileSystem()}
	e, err := newIdentityExecutor(id, 1)
	s.Require().NoError(err)
	s.T().Cleanup(e.shutdown)
	r := &resolverBoundFS{FileSystem: inner, resolve: s.countingResolve(id), principal: "bob", exec: e, execID: id}
	s.resetCounters()

	s.Require().True(r.Mkdir("d", 0o755, nil).Ok())
	s.EqualValues(1, inner.mkdirCalls.Load())
	s.Equal([]uint32{2000, 2001}, inner.lastChown.Load().([]uint32),
		"post-create chown must assign the entry to the constant identity")
	s.assertNoPerOpMachinery()
}

// TestConstantConstructorStartupFailureFallsBack: end-to-end through the
// REAL global registry — when worker startup fails, NewConstantResolverBoundFS
// hands back a wrapper with a nil executor, and ops succeed via the per-op
// path once credentials are switchable again.
func (s *ResolverBoundFSExecSuite) TestConstantConstructorStartupFailureFallsBack() {
	// Unique identity tuple: the global registry caches failures per key.
	id := Identity{Uid: 424242, Gid: 424242, Gids: []uint32{424242}}
	setGroupsRaw = func([]uint32) error { return errors.New("EPERM (not privileged)") }

	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewConstantResolverBoundFS(base, s.countingResolve(id), "alice", id, 2).(*resolverBoundFS)
	s.Require().Nil(r.exec, "startup failure must yield a nil executor (per-op fallback)")

	// Credentials become switchable (e.g. the process actually is root): the
	// per-op path must work untouched.
	setGroupsRaw = func([]uint32) error { s.setgroupsCalls.Add(1); return nil }
	s.resetCounters()
	_, st := r.GetAttr("", nil)
	s.Require().True(st.Ok())
	s.EqualValues(1, s.resolveCalls.Load(), "fallback wrapper uses the per-op resolve path")
	s.EqualValues(1, s.getgroupsCalls.Load())
}

// TestConstantConstructorWiresExecutor: the happy construction path routes
// through the shared registry — two wrappers for the same identity share one
// executor.
func (s *ResolverBoundFSExecSuite) TestConstantConstructorWiresExecutor() {
	// Unique tuple to avoid cross-test registry hits.
	id := Identity{Uid: 515151, Gid: 515151, Gids: []uint32{515151}}
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r1 := NewConstantResolverBoundFS(base, s.countingResolve(id), "alice", id, 1).(*resolverBoundFS)
	r2 := NewConstantResolverBoundFS(base, s.countingResolve(id), "bob", id, 1).(*resolverBoundFS)
	s.Require().NotNil(r1.exec)
	s.Same(r1.exec, r2.exec, "same identity tuple must share one process-wide executor")
}

// TestConstructorZeroWorkersDisablesExecutor: the io-level half of the
// default-off guarantee — even if a caller reaches the constant constructor
// with workers=0, the registry's build seam never fires and the wrapper comes
// back with a nil exec (per-op path), so no pinned threads are ever started.
// (The service-level half — BindIdentity not constructing an executor-routed
// FS at all under the default config — lives in volume_bind_test.go.)
func (s *ResolverBoundFSExecSuite) TestConstructorZeroWorkersDisablesExecutor() {
	origBuild := executors.build
	defer func() { executors.build = origBuild }()
	invoked := false
	executors.build = func(id Identity, workers int) (*identityExecutor, error) {
		invoked = true
		return origBuild(id, workers)
	}

	id := Identity{Uid: 606060, Gid: 606060, Gids: []uint32{606060}}
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	r := NewConstantResolverBoundFS(base, s.countingResolve(id), "zero", id, 0).(*resolverBoundFS)
	s.False(invoked, "workers=0 must not invoke the registry build seam")
	s.Nil(r.exec, "workers=0 must yield a nil executor (per-op fallback)")
}

// TestExecPathReadDirPlusForwardsToPlusser: with an executor, ReadDirPlus
// routes through exec.do, calls zero changeIdentity seams, and — when the
// inner FS implements ReadDirPlusser — forwards to it and returns its entries
// with non-nil attrs.
func (s *ResolverBoundFSExecSuite) TestExecPathReadDirPlusForwardsToPlusser() {
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	dir := s.T().TempDir()
	// ConfinedLoopbackFileSystem implements ReadDirPlusser.
	inner, err := NewConfinedLoopbackFileSystem(dir)
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = unix.Close(inner.rootFd) })

	_, isPlusser := pathfs.FileSystem(inner).(ReadDirPlusser)
	s.Require().True(isPlusser, "fixture must implement ReadDirPlus")

	e, execErr := newIdentityExecutor(id, 2)
	s.Require().NoError(execErr)
	s.T().Cleanup(e.shutdown)
	r := &resolverBoundFS{FileSystem: inner, resolve: s.countingResolve(id), principal: "alice", exec: e, execID: id}
	s.resetCounters()

	// Create one entry via the confined FS so ReadDirPlus returns something
	// observable. Use the real inner FS directly (no credential switch needed
	// for the setup itself).
	s.Require().True(inner.Mkdir("hello", 0o755, nil).Ok())
	s.resetCounters()

	entries, st := r.ReadDirPlus("", nil)
	s.Require().True(st.Ok())
	s.Require().Len(entries, 1)
	s.Equal("hello", entries[0].Entry.Name)
	s.Require().NotNil(entries[0].Attr, "inner ReadDirPlusser must supply attrs")
	s.assertNoPerOpMachinery()
}

// TestExecPathReadDirPlusFallbackWhenInnerLacksCapability: with an executor,
// ReadDirPlus routes through exec.do for an inner FS that does NOT implement
// ReadDirPlusser — falls back to OpenDir with nil attrs, zero per-op
// credential machinery.
func (s *ResolverBoundFSExecSuite) TestExecPathReadDirPlusFallbackWhenInnerLacksCapability() {
	id := Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	dir := s.T().TempDir()
	// pathfs.NewLoopbackFileSystem does NOT implement ReadDirPlusser.
	inner := pathfs.NewLoopbackFileSystem(dir)
	_, isPlusser := inner.(ReadDirPlusser)
	s.Require().False(isPlusser, "fixture must NOT implement ReadDirPlus")

	e, err := newIdentityExecutor(id, 2)
	s.Require().NoError(err)
	s.T().Cleanup(e.shutdown)
	r := &resolverBoundFS{FileSystem: inner, resolve: s.countingResolve(id), principal: "alice", exec: e, execID: id}

	// Create one entry so the fallback OpenDir returns it.
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "item.txt"), []byte("x"), 0o644))
	s.resetCounters()

	entries, st := r.ReadDirPlus("", nil)
	s.Require().True(st.Ok())
	s.Require().Len(entries, 1)
	s.Equal("item.txt", entries[0].Entry.Name)
	s.Nil(entries[0].Attr, "fallback must not invent attributes")
	s.assertNoPerOpMachinery()
}

// recordingFS counts inner-FS invocations and records the last Chown, so
// tests can assert what reached the wrapped filesystem. Embeds the ENOSYS
// default FS for everything not overridden.
type recordingFS struct {
	pathfs.FileSystem
	getattrCalls atomic.Int64
	mkdirCalls   atomic.Int64
	lastChown    atomic.Value // []uint32{uid, gid}
}

func (f *recordingFS) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
	f.getattrCalls.Add(1)
	return &fuse.Attr{Mode: fuse.S_IFDIR | 0o755}, fuse.OK
}

func (f *recordingFS) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
	f.mkdirCalls.Add(1)
	return fuse.OK
}

func (f *recordingFS) Chown(name string, uid uint32, gid uint32, context *fuse.Context) fuse.Status {
	f.lastChown.Store([]uint32{uid, gid})
	return fuse.OK
}
