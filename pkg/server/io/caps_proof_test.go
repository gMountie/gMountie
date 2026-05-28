package io

import (
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

// Linux capability bit numbers — stable ABI.
const (
	capDACOverride   = 1
	capDACReadSearch = 2
	capSetGID        = 6
	capSetUID        = 7
)

// capMask returns the uint32 bitmask for capability bit n (bits 0–31 → lo word).
func capMask(n uint) uint32 {
	return 1 << n
}

// savedCaps holds the cap state captured before a keepOnlyCaps call so it can
// be restored exactly.
type savedCaps struct {
	data [2]unix.CapUserData
}

// keepOnlyCaps reduces the calling thread's EFFECTIVE capability set to exactly
// the supplied bits. PERMITTED is left intact — the Linux kernel treats permitted
// as a monotonically non-increasing ceiling within a session; dropping it makes
// restore impossible. DAC enforcement consults the effective set, so reducing
// effective is sufficient to enforce the read-only constraint.
//
// Returns the original cap state so the caller can restore it.
// Pid=0 targets the calling thread.
func keepOnlyCaps(t *testing.T, caps ...uint) savedCaps {
	t.Helper()
	var mask uint32
	for _, c := range caps {
		mask |= capMask(c)
	}
	hdr := &unix.CapUserHeader{
		Version: unix.LINUX_CAPABILITY_VERSION_3,
		Pid:     0,
	}

	// Capture current state before mutating.
	var orig [2]unix.CapUserData
	if err := unix.Capget(hdr, &orig[0]); err != nil {
		t.Fatalf("Capget: %v", err)
	}

	// Write back: keep Permitted unchanged, reduce Effective to requested mask.
	// V3 uses a [2]CapUserData array (lo/hi 64-bit split); all caps used here
	// are < 32 so only the lo word ([0]) is non-zero.
	data := [2]unix.CapUserData{
		{Effective: mask, Permitted: orig[0].Permitted, Inheritable: 0},
		{Effective: 0, Permitted: orig[1].Permitted, Inheritable: 0},
	}
	if err := unix.Capset(hdr, &data[0]); err != nil {
		t.Skipf("Capset failed (not root?): %v", err)
	}
	return savedCaps{data: orig}
}

// restoreCaps restores the effective capability set to the state captured by
// a prior keepOnlyCaps call. Permitted is not changed (it was not changed by
// keepOnlyCaps either).
func restoreCaps(t *testing.T, saved savedCaps) {
	t.Helper()
	hdr := &unix.CapUserHeader{
		Version: unix.LINUX_CAPABILITY_VERSION_3,
		Pid:     0,
	}
	// Restore original effective; leave permitted as-is.
	data := [2]unix.CapUserData{
		{Effective: saved.data[0].Effective, Permitted: saved.data[0].Permitted, Inheritable: 0},
		{Effective: saved.data[1].Effective, Permitted: saved.data[1].Permitted, Inheritable: 0},
	}
	if err := unix.Capset(hdr, &data[0]); err != nil {
		t.Fatalf("restoreCaps: Capset failed: %v", err)
	}
}

// ownerOnlyFixture creates a regular file under /tmp owned by uid 65500, mode
// 0o600. The file is readable/writable only by its owner under normal DAC
// rules. We use /tmp directly (mode 1777) so a root thread with altered fsuid
// can still traverse to it (t.TempDir() uses a 0700 root dir that blocks
// traversal once fsuid changes).
func ownerOnlyFixture(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "caps-proof-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	f := filepath.Join(dir, "owneronly")
	if err := os.WriteFile(f, []byte("secret"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	// Owned by uid 65500 (unlikely to collide); gid 65500 irrelevant for 0o600.
	if err := os.Chown(f, 65500, 65500); err != nil {
		t.Fatalf("Chown: %v", err)
	}
	if err := os.Chmod(f, 0o600); err != nil {
		t.Fatalf("Chmod file: %v", err)
	}
	return f
}

// CapsProofSuite empirically verifies that a raw per-thread SYS_capset can
// retain CAP_DAC_READ_SEARCH while dropping CAP_DAC_OVERRIDE, and that the
// kernel enforces the distinction on file access. This underpins the Phase 3
// dac_read admin capability design (spec §3.4).
type CapsProofSuite struct{ suite.Suite }

func TestCapsProofSuite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root — run on the kubevirt VM with sudo")
	}
	suite.Run(t, new(CapsProofSuite))
}

// TestKeepDacReadSearchAllowsReadDeniesWrite is the core proof:
//
//  1. Pin to an OS thread so capability mutations don't leak to other goroutines.
//  2. Create a 0o600 file owned by uid 65500.
//  3. Drop EFFECTIVE to {CAP_DAC_READ_SEARCH, CAP_SETUID, CAP_SETGID} only
//     (PERMITTED left intact — it is monotonically non-increasing and cannot be
//     restored once dropped; the kernel consults EFFECTIVE for DAC checks).
//  4. Assert: O_RDONLY succeeds (DAC_READ_SEARCH bypasses owner-bit DAC for read).
//  5. Assert: O_WRONLY fails with EACCES (DAC_OVERRIDE absent from effective).
//  6. Restore original effective caps before returning.
func (s *CapsProofSuite) TestKeepDacReadSearchAllowsReadDeniesWrite() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	f := ownerOnlyFixture(s.T())

	// Reduce effective to read-only admin caps; save original for restore.
	saved := keepOnlyCaps(s.T(), capDACReadSearch, capSetUID, capSetGID)
	defer restoreCaps(s.T(), saved)

	// Read must succeed: CAP_DAC_READ_SEARCH grants read access regardless of
	// owner bits.
	rfd, err := syscall.Open(f, syscall.O_RDONLY, 0)
	s.Require().NoError(err, "O_RDONLY should succeed with CAP_DAC_READ_SEARCH")
	syscall.Close(rfd)

	// Write must fail: CAP_DAC_OVERRIDE is absent from effective, and we are not
	// uid 65500.
	wfd, werr := syscall.Open(f, syscall.O_WRONLY, 0)
	if werr == nil {
		syscall.Close(wfd)
	}
	s.Require().ErrorIs(werr, syscall.EACCES,
		"O_WRONLY should be denied without CAP_DAC_OVERRIDE in effective set")
}

// TestRestoreFullCapsReEnablesWrite confirms that restoring the original
// effective capability set after the dac_read drop re-enables write on the same
// file. This validates the cap restore path that the production code will use
// after each request.
func (s *CapsProofSuite) TestRestoreFullCapsReEnablesWrite() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	f := ownerOnlyFixture(s.T())

	// Drop effective to read-only caps, then immediately restore.
	saved := keepOnlyCaps(s.T(), capDACReadSearch, capSetUID, capSetGID)
	restoreCaps(s.T(), saved)

	// After restore, write must succeed (root with full effective caps).
	wfd, err := syscall.Open(f, syscall.O_WRONLY, 0)
	s.Require().NoError(err, "O_WRONLY should succeed after cap restore")
	syscall.Close(wfd)
}
