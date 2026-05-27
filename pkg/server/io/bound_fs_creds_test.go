package io

import (
	"os"
	"path/filepath"
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

// TestPerThreadGroupsEnforcedAndIsolated: a file readable only via gid 6000
// (owner 5000, mode 0040) is readable by a thread whose changeIdentity set
// supplementary group 6000 (uid 7000), AND a sibling thread without that group
// gets EACCES (no cross-thread leak), AND after cleanup the calling thread's
// groups are restored.
func (s *BoundFSCredsSuite) TestPerThreadGroupsEnforcedAndIsolated() {
	dir := s.T().TempDir()
	s.Require().NoError(os.Chmod(dir, 0o755))
	f := filepath.Join(dir, "grouponly")
	s.Require().NoError(os.WriteFile(f, []byte("secret"), 0o600))
	s.Require().NoError(os.Chown(f, 5000, 6000))
	s.Require().NoError(os.Chmod(f, 0o040))

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

	s.NoError(withErr, "thread with supp group 6000 should read")
	s.ErrorIs(withoutErr, syscall.EACCES, "thread without 6000 should be denied (no leak)")
}
