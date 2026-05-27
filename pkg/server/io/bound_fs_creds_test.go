package io

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
)

type BoundFSCredsSuite struct{ suite.Suite }

func TestBoundFSCredsSuite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to exercise setfsuid/setfsgid/setgroups")
	}
	suite.Run(t, new(BoundFSCredsSuite))
}

// groupOnlyFixture creates a file readable ONLY via group gid 6000: owner 5000,
// group 6000, mode 0040 (no owner/other read). It lives directly under /tmp
// (mode 1777, world-traversable) so a thread with an unrelated fsuid can still
// *traverse* to it — t.TempDir() nests under a 0700 root-owned dir, which would
// deny traversal and mask the group check we're actually testing.
func (s *BoundFSCredsSuite) groupOnlyFixture() string {
	dir, err := os.MkdirTemp("/tmp", "boundfs-creds-")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = os.RemoveAll(dir) })
	s.Require().NoError(os.Chmod(dir, 0o755))
	f := filepath.Join(dir, "grouponly")
	s.Require().NoError(os.WriteFile(f, []byte("secret"), 0o600))
	s.Require().NoError(os.Chown(f, 5000, 6000))
	s.Require().NoError(os.Chmod(f, 0o040))
	return f
}

// TestPerThreadGroupsEnforcedAndIsolated: the group-only file is readable by a
// thread whose changeIdentity set supplementary group 6000 (uid 7000, primary
// gid 8000 — neither owner nor file group), AND a sibling thread without that
// group gets EACCES. Proves the kernel enforces supplementary groups set via
// raw per-thread setgroups, and that one thread's groups don't leak to another.
func (s *BoundFSCredsSuite) TestPerThreadGroupsEnforcedAndIsolated() {
	f := s.groupOnlyFixture()

	try := func(gids []uint32) error {
		cleanup, err := changeIdentity(&Identity{Uid: 7000, Gid: 8000, Gids: gids})
		if err != nil {
			return err
		}
		defer cleanup()
		fd, oerr := syscall.Open(f, syscall.O_RDONLY, 0)
		if oerr != nil {
			return oerr
		}
		syscall.Close(fd)
		return nil
	}

	var wg sync.WaitGroup
	var withErr, withoutErr error
	wg.Add(2)
	go func() { defer wg.Done(); withErr = try([]uint32{6000}) }()
	go func() { defer wg.Done(); withoutErr = try([]uint32{9999}) }()
	wg.Wait()

	s.Require().NoError(withErr, "thread with supp group 6000 should read")
	s.Require().ErrorIs(withoutErr, syscall.EACCES, "thread without 6000 should be denied (no leak)")
}

// TestCleanupRestoresOriginalGroups proves the cleanup path restores the
// thread's supplementary groups. The test pins its own thread first so the
// post-cleanup read happens on the same thread changeIdentity mutated (its
// nested LockOSThread/UnlockOSThread leaves our outer lock in place).
func (s *BoundFSCredsSuite) TestCleanupRestoresOriginalGroups() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	orig, err := getgroups()
	s.Require().NoError(err)

	cleanup, err := changeIdentity(&Identity{Uid: 7000, Gid: 8000, Gids: []uint32{6000}})
	s.Require().NoError(err)

	active, err := getgroups()
	s.Require().NoError(err)
	s.ElementsMatch([]uint32{6000}, active, "groups should be the identity's while bound")

	cleanup()

	restored, err := getgroups()
	s.Require().NoError(err)
	s.ElementsMatch(orig, restored, "groups should be restored after cleanup")
}
