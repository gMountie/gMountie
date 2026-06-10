package io

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

// StatFailureSuite tests the per-entry stat-failure branch in ReadDirPlus:
// when fstatatFn returns an error for one entry, the listing continues with
// all entries present, the poisoned entry carries nil Attr and a d_type-derived
// Mode, and the healthy entries carry full attrs.
type StatFailureSuite struct {
	suite.Suite
	origFstatat func(dirfd int, path string, stat *unix.Stat_t, flags int) error
}

func TestStatFailureSuite(t *testing.T) { suite.Run(t, new(StatFailureSuite)) }

func (s *StatFailureSuite) SetupTest() {
	s.origFstatat = fstatatFn
}

func (s *StatFailureSuite) TearDownTest() {
	fstatatFn = s.origFstatat
}

// TestStatFailureContinues: a 3-entry directory where the middle entry's stat
// returns ENOENT (e.g. entry vanished between getdents and stat). The listing
// must return ALL 3 entries; the poisoned entry has nil Attr and a
// d_type-derived Mode; the two healthy entries have full attrs.
func (s *StatFailureSuite) TestStatFailureContinues() {
	dir := s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "alpha.txt"), []byte("a"), 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "bravo.txt"), []byte("b"), 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "charlie.txt"), []byte("c"), 0o644))

	fs, err := NewConfinedLoopbackFileSystem(dir)
	s.Require().NoError(err)
	defer unix.Close(fs.rootFd)

	// Stub: bravo.txt's stat returns ENOENT; all others use the real syscall.
	fstatatFn = func(dirfd int, path string, stat *unix.Stat_t, flags int) error {
		if path == "bravo.txt" {
			return unix.ENOENT
		}
		return unix.Fstatat(dirfd, path, stat, flags)
	}

	entries, st := fs.ReadDirPlus("", nil)
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(entries, 3, "all 3 entries must be returned despite one stat failure")

	byName := entriesByName(entries)

	// Healthy entries must carry non-nil attrs with a sensible ino.
	for _, name := range []string{"alpha.txt", "charlie.txt"} {
		e := byName[name]
		s.Require().NotNil(e.Attr, "%s must have full attrs", name)
		s.NotZero(e.Attr.Ino, "%s ino must be set", name)
		s.Equal(uint32(syscall.S_IFREG), e.Entry.Mode&syscall.S_IFMT, "%s entry mode", name)
	}

	// The poisoned entry must have nil Attr and its mode derived from d_type (S_IFREG).
	poisoned := byName["bravo.txt"]
	s.Nil(poisoned.Attr, "bravo.txt must have nil Attr after stat failure")
	s.Equal(uint32(syscall.S_IFREG), poisoned.Entry.Mode, "bravo.txt mode must come from d_type fallback")
}

// ReadDirPlusSuite exercises ConfinedLoopbackFileSystem.ReadDirPlus with a
// real tmpdir fixture. Plain syscalls only — no FUSE mount required, so this
// suite runs in any environment.
type ReadDirPlusSuite struct {
	suite.Suite
	rootDir string
	fs      *ConfinedLoopbackFileSystem
}

func TestReadDirPlusSuite(t *testing.T) { suite.Run(t, new(ReadDirPlusSuite)) }

func (s *ReadDirPlusSuite) SetupTest() {
	s.rootDir = s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "small.txt"), []byte("hi"), 0o644))
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "big.txt"), make([]byte, 4096), 0o600))
	s.Require().NoError(os.Mkdir(filepath.Join(s.rootDir, "sub"), 0o755))
	s.Require().NoError(os.WriteFile(filepath.Join(s.rootDir, "sub", "child.txt"), []byte("c"), 0o644))
	s.Require().NoError(os.Symlink("small.txt", filepath.Join(s.rootDir, "link")))
	s.Require().NoError(os.Symlink("does-not-exist", filepath.Join(s.rootDir, "dangling")))

	var err error
	s.fs, err = NewConfinedLoopbackFileSystem(s.rootDir)
	s.Require().NoError(err)
}

func (s *ReadDirPlusSuite) TearDownTest() {
	if s.fs != nil {
		unix.Close(s.fs.rootFd)
	}
}

// entriesByName indexes a ReadDirPlus result for lookup assertions.
func entriesByName(entries []DirEntryPlus) map[string]DirEntryPlus {
	m := make(map[string]DirEntryPlus, len(entries))
	for _, e := range entries {
		m[e.Entry.Name] = e
	}
	return m
}

// TestAttrsMatchLstat: every entry carries non-nil attrs whose ino/mode/size
// agree with an independent os.Lstat of the same path.
func (s *ReadDirPlusSuite) TestAttrsMatchLstat() {
	entries, st := s.fs.ReadDirPlus("", nil)
	s.Require().Equal(fuse.OK, st)
	byName := entriesByName(entries)
	s.Require().Len(byName, 5, "small.txt, big.txt, sub, link, dangling")

	for name, e := range byName {
		s.Require().NotNil(e.Attr, "attrs missing for %q", name)
		info, err := os.Lstat(filepath.Join(s.rootDir, name))
		s.Require().NoError(err)
		st := info.Sys().(*syscall.Stat_t)
		s.Equal(st.Ino, e.Attr.Ino, "%q ino", name)
		s.Equal(st.Mode, e.Attr.Mode, "%q mode", name)
		s.Equal(uint64(st.Size), e.Attr.Size, "%q size", name)
		// The plain entry fields are sourced from the same stat.
		s.Equal(st.Ino, e.Entry.Ino, "%q entry ino", name)
		s.Equal(st.Mode, e.Entry.Mode, "%q entry mode", name)
	}
}

// TestDanglingSymlinkGetsLinkAttrs: AT_SYMLINK_NOFOLLOW means a dangling
// symlink yields the attrs of the LINK itself, not ENOENT from following it.
func (s *ReadDirPlusSuite) TestDanglingSymlinkGetsLinkAttrs() {
	entries, st := s.fs.ReadDirPlus("", nil)
	s.Require().Equal(fuse.OK, st)
	e, ok := entriesByName(entries)["dangling"]
	s.Require().True(ok)
	s.Require().NotNil(e.Attr, "dangling symlink must stat the link itself")
	s.Equal(uint32(syscall.S_IFLNK), e.Attr.Mode&syscall.S_IFMT)
	s.Equal(uint64(len("does-not-exist")), e.Attr.Size, "symlink size is the target string length")
}

// TestSubdirectoryListing: ReadDirPlus on a subdirectory path resolves
// beneath the root like any other wire path.
func (s *ReadDirPlusSuite) TestSubdirectoryListing() {
	entries, st := s.fs.ReadDirPlus("/sub", nil)
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(entries, 1)
	s.Equal("child.txt", entries[0].Entry.Name)
	s.Require().NotNil(entries[0].Attr)
	s.Equal(uint32(syscall.S_IFREG), entries[0].Attr.Mode&syscall.S_IFMT)
}

// TestEmptyDirectory: an empty directory lists successfully with no entries.
func (s *ReadDirPlusSuite) TestEmptyDirectory() {
	s.Require().NoError(os.Mkdir(filepath.Join(s.rootDir, "empty"), 0o755))
	entries, st := s.fs.ReadDirPlus("empty", nil)
	s.Equal(fuse.OK, st)
	s.Empty(entries)
}

// TestConfinementMatchesOpenDir: a path escaping the volume root returns the
// SAME status OpenDir gives (EACCES via the EXDEV mapping) — ReadDirPlus must
// not weaken the confinement contract.
func (s *ReadDirPlusSuite) TestConfinementMatchesOpenDir() {
	_, odStatus := s.fs.OpenDir("../../etc", nil)
	_, rdpStatus := s.fs.ReadDirPlus("../../etc", nil)
	s.Equal(odStatus, rdpStatus)
	s.Equal(fuse.EACCES, rdpStatus)
}

// TestMissingDirectory: ENOENT passes through unchanged, like OpenDir.
func (s *ReadDirPlusSuite) TestMissingDirectory() {
	_, st := s.fs.ReadDirPlus("no-such-dir", nil)
	s.Equal(fuse.ENOENT, st)
}

// BoundReadDirPlusSuite verifies the resolverBoundFS forwarding: ReadDirPlus
// runs under the same credential-switch seams as every other op, and the
// wrapper degrades to OpenDir + nil attrs when the inner FS lacks the
// capability. The syscall seams are stubbed (pattern shared with
// BoundFSRollbackSuite) so the suite runs without root.
type BoundReadDirPlusSuite struct {
	suite.Suite

	fsuidCalls []int // every setfsuid target, in order (apply then restore)

	origSetfsuid     func(int) error
	origSetfsgid     func(int) error
	origSetGroupsRaw func([]uint32) error
	origGetgroups    func() ([]uint32, error)
	origLock         func()
	origUnlock       func()
}

func TestBoundReadDirPlusSuite(t *testing.T) { suite.Run(t, new(BoundReadDirPlusSuite)) }

func (s *BoundReadDirPlusSuite) SetupTest() {
	s.fsuidCalls = nil
	s.origSetfsuid, s.origSetfsgid = setfsuid, setfsgid
	s.origSetGroupsRaw, s.origGetgroups = setGroupsRaw, getgroups
	s.origLock, s.origUnlock = lockOSThread, unlockOSThread

	setfsuid = func(uid int) error { s.fsuidCalls = append(s.fsuidCalls, uid); return nil }
	setfsgid = func(int) error { return nil }
	setGroupsRaw = func([]uint32) error { return nil }
	getgroups = func() ([]uint32, error) { return []uint32{0}, nil }
	lockOSThread = func() {}
	unlockOSThread = func() {}
	resetBaselineGroups() // the stubbed getgroups must be re-read per test
}

func (s *BoundReadDirPlusSuite) TearDownTest() {
	setfsuid, setfsgid = s.origSetfsuid, s.origSetfsgid
	setGroupsRaw, getgroups = s.origSetGroupsRaw, s.origGetgroups
	lockOSThread, unlockOSThread = s.origLock, s.origUnlock
	resetBaselineGroups() // don't leak the stub's groups to later suites
}

func (s *BoundReadDirPlusSuite) identity() Identity {
	return Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001}}
}

// TestForwardsUnderCredentialSwitch: the wrapper applies + restores the
// identity around the inner ReadDirPlus and returns its attrs.
func (s *BoundReadDirPlusSuite) TestForwardsUnderCredentialSwitch() {
	dir := s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644))
	inner, err := NewConfinedLoopbackFileSystem(dir)
	s.Require().NoError(err)
	defer unix.Close(inner.rootFd)

	id := s.identity()
	bound := NewIdentityBoundFS(inner, &id)
	rdp, ok := bound.(ReadDirPlusser)
	s.Require().True(ok, "identity-bound wrapper must expose ReadDirPlus")

	entries, st := rdp.ReadDirPlus("", nil)
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(entries, 1)
	s.Require().NotNil(entries[0].Attr, "attrs must pass through the wrapper")
	// Exactly one apply (to the identity's uid) followed by one restore (to
	// the process euid) — the same switch every other wrapped op performs.
	s.Equal([]int{1001, syscall.Geteuid()}, s.fsuidCalls)
}

// TestFallsBackWhenInnerLacksCapability: wrapping a plain pathfs loopback
// (no ReadDirPlus) yields the OpenDir listing with nil attrs.
func (s *BoundReadDirPlusSuite) TestFallsBackWhenInnerLacksCapability() {
	dir := s.T().TempDir()
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a"), 0o644))
	inner := pathfs.NewLoopbackFileSystem(dir)
	_, isPlusser := inner.(ReadDirPlusser)
	s.Require().False(isPlusser, "fixture must NOT implement ReadDirPlus")

	id := s.identity()
	bound := NewIdentityBoundFS(inner, &id)
	entries, st := bound.(ReadDirPlusser).ReadDirPlus("", nil)
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(entries, 1)
	s.Equal("a.txt", entries[0].Entry.Name)
	s.Nil(entries[0].Attr, "fallback must not invent attributes")
}

// TestResolveErrorReturnsEPERM: a failing identity resolve maps to EPERM,
// matching every other wrapped op.
func (s *BoundReadDirPlusSuite) TestResolveErrorReturnsEPERM() {
	dir := s.T().TempDir()
	inner, err := NewConfinedLoopbackFileSystem(dir)
	s.Require().NoError(err)
	defer unix.Close(inner.rootFd)

	bound := NewResolverBoundFS(inner, func(string) (Identity, error) {
		return Identity{}, syscall.ENOENT
	}, "ghost")
	entries, st := bound.(ReadDirPlusser).ReadDirPlus("", nil)
	s.Equal(fuse.EPERM, st)
	s.Nil(entries)
}
