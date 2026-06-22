package io

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
)

// BoundFSKillPrivSuite guards the SERVER-SIDE half of the HANDLE_KILLPRIV_V2
// security model (docs/design/security-and-transport.md §10). With the cap
// advertised, the client kernel delegates privilege-stripping and applies it by
// sending the identity-bound server an explicit setattr (Chmod). That delegation
// is only safe if the server — which runs as root — cannot itself retain a
// privilege bit the principal is not entitled to set. The identity-bound FS
// assumes the principal's credentials per OS thread (setfsuid → fsuid non-root),
// and that fsuid transition DROPS CAP_FSETID, so the kernel applies the same
// setgid-stripping it would for any unprivileged user. The e2e suite
// (test/e2e/fs/killpriv_test.go) covers the client-side strip-on-write end to
// end for passthrough; this pins the mapping-mode-independent server guarantee
// that the original investigation only verified by hand on the VM.
//
// Root-only: per-thread setfsuid/setgroups need privilege. Run on the kubevirt
// VM with sudo (matches BoundFSCapsSuite / CapsProofSuite).
type BoundFSKillPrivSuite struct {
	suite.Suite
	tempDir string
}

func TestBoundFSKillPrivSuite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root — per-thread setfsuid/setgroups; run on the kubevirt VM with sudo")
	}
	suite.Run(t, new(BoundFSKillPrivSuite))
}

// The principal owns the fixture file (so it may chmod it). kpForeignGID is a
// group the principal is NOT a member of, so setting setgid on the file would
// require CAP_FSETID — which the identity binding drops.
const (
	kpPrincipalUID = 65501
	kpForeignGID   = 65500
)

func (s *BoundFSKillPrivSuite) SetupTest() {
	// /tmp (mode 1777) rather than t.TempDir() (a 0700 root-owned dir) so a
	// thread with an altered fsuid can still traverse to the fixture — same
	// reason as BoundFSCapsSuite.
	dir, err := os.MkdirTemp("/tmp", "boundfs-killpriv-")
	s.Require().NoError(err)
	s.Require().NoError(os.Chmod(dir, 0o755))
	s.T().Cleanup(func() { _ = os.RemoveAll(dir) })
	s.tempDir = dir
}

func (s *BoundFSKillPrivSuite) newBoundFS(id *Identity) pathfs.FileSystem {
	base, err := NewLocalFilesystem(s.tempDir)
	s.Require().NoError(err)
	return NewIdentityBoundFS(base, id)
}

// writeOwnedFile creates name under the source dir owned by the principal with
// group gid, then asserts root CAN set setgid here (the test process holds
// CAP_FSETID) — the positive control that makes a later "stripped" assertion
// meaningful. It returns the backing path at mode 0o755 (no special bits).
func (s *BoundFSKillPrivSuite) writeOwnedFile(name string, gid uint32) string {
	backing := filepath.Join(s.tempDir, name)
	s.Require().NoError(os.WriteFile(backing, []byte("x"), 0o644))
	s.Require().NoError(os.Chown(backing, kpPrincipalUID, int(gid)))

	// Positive control: root (CAP_FSETID) can set setgid regardless of group,
	// proving the bit is otherwise settable so the test discriminates on
	// capability, not on some unrelated failure. NOTE os.Chmod takes an
	// os.FileMode, where setgid is os.ModeSetgid (not the raw 0o2000 octal) —
	// the identity-bound FS.Chmod below instead takes a raw uint32 mode where
	// 0o2755 genuinely carries setgid.
	s.Require().NoError(os.Chmod(backing, 0o755|os.ModeSetgid))
	ctrl, err := os.Stat(backing)
	s.Require().NoError(err)
	s.Require().NotZero(ctrl.Mode()&os.ModeSetgid,
		"control: root (CAP_FSETID) must be able to set setgid on %q", name)
	s.Require().NoError(os.Chmod(backing, 0o755)) // reset before the real op
	return backing
}

// TestSetgidStrippedForForeignGroupPrincipal — a non-root principal that OWNS
// the file but is NOT in its group cannot set setgid: the identity-bound thread
// lacks CAP_FSETID, so the kernel clears S_ISGID on chmod. This is the core
// killpriv server-side guarantee — the root server does not retain a privilege
// bit the principal could not have set itself.
func (s *BoundFSKillPrivSuite) TestSetgidStrippedForForeignGroupPrincipal() {
	name := "foreigngid"
	backing := s.writeOwnedFile(name, kpForeignGID)

	// Principal: non-root owner, NOT a member of kpForeignGID.
	id := &Identity{Uid: kpPrincipalUID, Gid: kpPrincipalUID, Gids: []uint32{kpPrincipalUID}}
	st := s.newBoundFS(id).Chmod(name, 0o2755, &fuse.Context{})
	s.Require().Equal(fuse.OK, st, "owner chmod should succeed (the bit is silently stripped, not refused)")

	post, err := os.Stat(backing)
	s.Require().NoError(err)
	s.Zero(post.Mode()&os.ModeSetgid,
		"setgid MUST be stripped: the identity-bound thread (fsuid=%d) lacks CAP_FSETID "+
			"for a non-member group, so the server cannot retain it (backing mode %o)",
		kpPrincipalUID, post.Mode())
}

// TestSetgidKeptForOwnGroupPrincipal — the same principal, but now a MEMBER of
// the file's group, MAY keep setgid (in_group_p is true, so the kernel does not
// strip it even without CAP_FSETID). This proves the strip above is the real
// in_group_p/CAP_FSETID rule, not a blanket "the binding always strips setgid".
func (s *BoundFSKillPrivSuite) TestSetgidKeptForOwnGroupPrincipal() {
	name := "owngid"
	backing := s.writeOwnedFile(name, kpForeignGID)

	// Principal owns the file AND is a member of its group kpForeignGID.
	id := &Identity{Uid: kpPrincipalUID, Gid: kpPrincipalUID, Gids: []uint32{kpPrincipalUID, kpForeignGID}}
	st := s.newBoundFS(id).Chmod(name, 0o2755, &fuse.Context{})
	s.Require().Equal(fuse.OK, st, "owner chmod should succeed")

	post, err := os.Stat(backing)
	s.Require().NoError(err)
	s.NotZero(post.Mode()&os.ModeSetgid,
		"setgid must be RETAINED when the principal is in the file's group "+
			"(in_group_p true; backing mode %o)", post.Mode())
}
