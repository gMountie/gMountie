package io

import (
	"fmt"
	"syscall"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
)

// BoundFSRollbackSuite drives every apply*/cleanup path in bound_fs.go through
// failure injection on the syscall seams, asserting the two policies the file
// documents WITHOUT requiring root:
//
//  1. Restore ordering: rollback undoes credential changes in reverse-apply
//     order (caps → fsuid → fsgid → groups) and only the levels that were
//     actually applied.
//  2. Lock/leak: the OS thread is unlocked exactly once on a fully successful
//     unwind, and NOT unlocked (leaked, to die with its goroutine) when any
//     restore fails — for mid-apply unwinds and post-op cleanups alike.
//
// The root-only kernel-proof suites (BoundFSCredsSuite, BoundFSCapsSuite)
// remain the evidence that the real syscalls behave as modelled; this suite
// owns the unwind-policy logic those can't reach (you can't make setgroups
// fail on demand as root).
type BoundFSRollbackSuite struct {
	suite.Suite

	calls   []string // ordered log of seam invocations
	locks   int
	unlocks int

	origSetfsuid         func(int) error
	origSetfsgid         func(int) error
	origSetGroupsRaw     func([]uint32) error
	origGetgroups        func() ([]uint32, error)
	origGetCaps          func() (capState, error)
	origSetCapsEffective func(capState) error
	origLock             func()
	origUnlock           func()
}

func TestBoundFSRollbackSuite(t *testing.T) { suite.Run(t, new(BoundFSRollbackSuite)) }

func (s *BoundFSRollbackSuite) SetupTest() {
	s.calls = nil
	s.locks, s.unlocks = 0, 0
	s.origSetfsuid, s.origSetfsgid = setfsuid, setfsgid
	s.origSetGroupsRaw, s.origGetgroups = setGroupsRaw, getgroups
	s.origGetCaps, s.origSetCapsEffective = getCaps, setCapsEffective
	s.origLock, s.origUnlock = lockOSThread, unlockOSThread

	// Default: everything succeeds and is recorded. Individual tests overwrite
	// single seams with failing variants.
	setfsuid = func(uid int) error { s.record("setfsuid(%d)", uid); return nil }
	setfsgid = func(gid int) error { s.record("setfsgid(%d)", gid); return nil }
	setGroupsRaw = func(gids []uint32) error { s.record("setgroups(%v)", gids); return nil }
	getgroups = func() ([]uint32, error) { s.record("getgroups"); return []uint32{0}, nil }
	getCaps = func() (capState, error) {
		s.record("getcaps")
		return capState{effective: ^uint64(0), permitted: ^uint64(0)}, nil
	}
	setCapsEffective = func(c capState) error { s.record("setcaps(0x%x)", c.effective); return nil }
	lockOSThread = func() { s.locks++ }
	unlockOSThread = func() { s.unlocks++ }
}

func (s *BoundFSRollbackSuite) TearDownTest() {
	setfsuid, setfsgid = s.origSetfsuid, s.origSetfsgid
	setGroupsRaw, getgroups = s.origSetGroupsRaw, s.origGetgroups
	getCaps, setCapsEffective = s.origGetCaps, s.origSetCapsEffective
	lockOSThread, unlockOSThread = s.origLock, s.origUnlock
}

func (s *BoundFSRollbackSuite) record(format string, args ...any) {
	s.calls = append(s.calls, fmt.Sprintf(format, args...))
}

func (s *BoundFSRollbackSuite) noCapsID() *Identity {
	return &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000, 2000}}
}

// ---------- applyUnprivileged ----------

func (s *BoundFSRollbackSuite) TestUnprivileged_HappyPath_CleanupRestoresInOrder() {
	cleanup, err := changeIdentity(s.noCapsID())
	s.Require().NoError(err)
	s.Equal(1, s.locks)
	s.Equal(0, s.unlocks, "thread must stay locked while creds are foreign")
	s.Equal([]string{"getgroups", "setgroups([1000 2000])", "setfsgid(1000)", "setfsuid(1000)"}, s.calls)

	s.calls = nil
	cleanup()
	euid, egid := syscall.Geteuid(), syscall.Getegid()
	s.Equal([]string{
		fmt.Sprintf("setfsuid(%d)", euid),
		fmt.Sprintf("setfsgid(%d)", egid),
		"setgroups([0])",
	}, s.calls, "cleanup must restore in reverse-apply order: fsuid, fsgid, groups")
	s.Equal(1, s.unlocks, "fully successful cleanup unlocks exactly once")
}

func (s *BoundFSRollbackSuite) TestUnprivileged_SetgroupsFails_UnlocksWithoutRestores() {
	setGroupsRaw = func(gids []uint32) error { s.record("setgroups-fail"); return errors.New("boom") }
	_, err := changeIdentity(s.noCapsID())
	s.Require().Error(err)
	// Nothing was applied (setgroups is atomic), so the only unwind action is
	// the unlock — no fsuid/fsgid/groups restores.
	s.Equal([]string{"getgroups", "setgroups-fail"}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestUnprivileged_SetfsgidFails_RestoresGroupsThenUnlocks() {
	setfsgid = func(gid int) error { s.record("setfsgid-fail(%d)", gid); return errors.New("boom") }
	_, err := changeIdentity(s.noCapsID())
	s.Require().Error(err)
	s.Equal([]string{"getgroups", "setgroups([1000 2000])", "setfsgid-fail(1000)", "setgroups([0])"}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestUnprivileged_SetfsuidFails_RestoresFsgidGroupsThenUnlocks() {
	setfsuid = func(uid int) error { s.record("setfsuid-fail(%d)", uid); return errors.New("boom") }
	_, err := changeIdentity(s.noCapsID())
	s.Require().Error(err)
	egid := syscall.Getegid()
	s.Equal([]string{
		"getgroups", "setgroups([1000 2000])", "setfsgid(1000)", "setfsuid-fail(1000)",
		fmt.Sprintf("setfsgid(%d)", egid), "setgroups([0])",
	}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestUnprivileged_CleanupFsuidRestoreFails_LeaksThread() {
	cleanup, err := changeIdentity(s.noCapsID())
	s.Require().NoError(err)

	// The restore-to-root setfsuid fails: the thread is tainted and must NOT
	// be unlocked, and no further restores may run on it.
	setfsuid = func(uid int) error { s.record("setfsuid-restore-fail(%d)", uid); return errors.New("boom") }
	s.calls = nil
	cleanup()
	s.Equal([]string{fmt.Sprintf("setfsuid-restore-fail(%d)", syscall.Geteuid())}, s.calls,
		"no fsgid/groups restore may run after a failed fsuid restore")
	s.Equal(0, s.unlocks, "thread must be leaked (stay locked) on failed restore")
}

func (s *BoundFSRollbackSuite) TestUnprivileged_CleanupGroupsRestoreFails_LeaksThread() {
	cleanup, err := changeIdentity(s.noCapsID())
	s.Require().NoError(err)

	setGroupsRaw = func(gids []uint32) error { s.record("setgroups-restore-fail"); return errors.New("boom") }
	cleanup()
	s.Equal(0, s.unlocks, "thread must be leaked on failed groups restore")
}

// ---------- applyDacReadSearch ----------

func (s *BoundFSRollbackSuite) dacReadSearchID() *Identity {
	return &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}, Caps: []string{CapDacReadSearch}}
}

func (s *BoundFSRollbackSuite) TestDacReadSearch_HappyPath_NeverTouchesFsuid() {
	cleanup, err := changeIdentity(s.dacReadSearchID())
	s.Require().NoError(err)
	s.Equal([]string{"getgroups", "setgroups([1000])", "setfsgid(1000)", "getcaps",
		fmt.Sprintf("setcaps(0x%x)", dacReadSearchEffectiveMask())}, s.calls,
		"dac_read_search path: groups, fsgid, then cap reduction — fsuid stays 0")

	s.calls = nil
	cleanup()
	egid := syscall.Getegid()
	s.Equal([]string{
		fmt.Sprintf("setcaps(0x%x)", ^uint64(0)), // caps restored FIRST
		fmt.Sprintf("setfsgid(%d)", egid),
		"setgroups([0])",
	}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestDacReadSearch_GetCapsFails_RestoresFsgidGroupsThenUnlocks() {
	getCaps = func() (capState, error) { s.record("getcaps-fail"); return capState{}, errors.New("boom") }
	_, err := changeIdentity(s.dacReadSearchID())
	s.Require().Error(err)
	egid := syscall.Getegid()
	s.Equal([]string{"getgroups", "setgroups([1000])", "setfsgid(1000)", "getcaps-fail",
		fmt.Sprintf("setfsgid(%d)", egid), "setgroups([0])"}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestDacReadSearch_SetCapsFails_RestoresFsgidGroupsThenUnlocks() {
	setCapsEffective = func(c capState) error { s.record("setcaps-fail"); return errors.New("boom") }
	_, err := changeIdentity(s.dacReadSearchID())
	s.Require().Error(err)
	egid := syscall.Getegid()
	s.Equal([]string{"getgroups", "setgroups([1000])", "setfsgid(1000)", "getcaps", "setcaps-fail",
		fmt.Sprintf("setfsgid(%d)", egid), "setgroups([0])"}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestDacReadSearch_MidApplyRestoreFails_LeaksThread() {
	// setcaps fails mid-apply AND the fsgid restore in the unwind fails too:
	// the thread must be leaked — this is exactly the divergence CQ-3 fixed
	// (the old ladders unlocked unconditionally with `_ =` restores).
	setCapsEffective = func(capState) error { return errors.New("boom") }
	failNext := false
	s.T().Cleanup(func() {}) // keep suite shape; seams restored in TearDownTest
	origFsgid := setfsgid
	setfsgid = func(gid int) error {
		if failNext {
			s.record("setfsgid-restore-fail(%d)", gid)
			return errors.New("boom")
		}
		failNext = true // first call (apply) succeeds, second (restore) fails
		return origFsgid(gid)
	}
	_, err := changeIdentity(s.dacReadSearchID())
	s.Require().Error(err)
	s.Equal(0, s.unlocks, "failed restore during a mid-apply unwind must leak the thread")
}

func (s *BoundFSRollbackSuite) TestDacReadSearch_CleanupCapsRestoreFails_LeaksThread() {
	cleanup, err := changeIdentity(s.dacReadSearchID())
	s.Require().NoError(err)

	setCapsEffective = func(c capState) error { s.record("setcaps-restore-fail"); return errors.New("boom") }
	s.calls = nil
	cleanup()
	s.Equal([]string{"setcaps-restore-fail"}, s.calls,
		"no fsgid/groups restore may run after a failed caps restore")
	s.Equal(0, s.unlocks, "thread must be leaked on failed caps restore")
}

// ---------- applyDacOverride ----------

func (s *BoundFSRollbackSuite) dacOverrideID() *Identity {
	return &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}, Caps: []string{CapDacOverride}}
}

func (s *BoundFSRollbackSuite) TestDacOverride_HappyPath_KeepsFsuidAndCaps() {
	cleanup, err := changeIdentity(s.dacOverrideID())
	s.Require().NoError(err)
	s.Equal([]string{"getgroups", "setgroups([1000])", "setfsgid(1000)"}, s.calls,
		"dac_override path: groups + fsgid only — fsuid and caps untouched")

	s.calls = nil
	cleanup()
	egid := syscall.Getegid()
	s.Equal([]string{fmt.Sprintf("setfsgid(%d)", egid), "setgroups([0])"}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestDacOverride_SetfsgidFails_RestoresGroupsThenUnlocks() {
	setfsgid = func(gid int) error { s.record("setfsgid-fail(%d)", gid); return errors.New("boom") }
	_, err := changeIdentity(s.dacOverrideID())
	s.Require().Error(err)
	s.Equal([]string{"getgroups", "setgroups([1000])", "setfsgid-fail(1000)", "setgroups([0])"}, s.calls)
	s.Equal(1, s.unlocks)
}

func (s *BoundFSRollbackSuite) TestDacOverride_CleanupGroupsRestoreFails_LeaksThread() {
	cleanup, err := changeIdentity(s.dacOverrideID())
	s.Require().NoError(err)

	setGroupsRaw = func([]uint32) error { return errors.New("boom") }
	cleanup()
	s.Equal(0, s.unlocks)
}
