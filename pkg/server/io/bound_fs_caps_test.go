package io

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

// BoundFSCapsSuite exercises the three credential paths in identityBoundFS
// dispatched by id.Caps: no caps (unchanged), dac_read_search, dac_override.
// All tests require root — capabilities and per-thread setfsuid are unavailable
// to unprivileged processes.
type BoundFSCapsSuite struct {
	suite.Suite
	tempDir string
}

func TestBoundFSCapsSuite(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root — run on the kubevirt VM with sudo")
	}
	suite.Run(t, new(BoundFSCapsSuite))
}

// SetupTest builds a fresh tempdir for each test case and writes
// secret.txt (uid 65500:65500, mode 0o600) inside it.
func (s *BoundFSCapsSuite) SetupTest() {
	// Use /tmp directly (mode 1777) rather than t.TempDir() which creates a
	// 0o700 root-owned dir — that blocks traversal once fsuid changes.
	dir, err := os.MkdirTemp("/tmp", "boundfs-caps-")
	s.Require().NoError(err)
	s.Require().NoError(os.Chmod(dir, 0o755))
	s.T().Cleanup(func() { _ = os.RemoveAll(dir) })
	s.tempDir = dir

	secret := filepath.Join(dir, "secret.txt")
	s.Require().NoError(os.WriteFile(secret, []byte("hello caps"), 0o600))
	s.Require().NoError(os.Chown(secret, 65500, 65500))
	s.Require().NoError(os.Chmod(secret, 0o600))
}

// newBoundFS builds a ConfinedLoopbackFileSystem over s.tempDir wrapped by an
// identityBoundFS with the supplied identity.
func (s *BoundFSCapsSuite) newBoundFS(id *Identity) *identityBoundFS {
	base, err := NewLocalFilesystem(s.tempDir)
	s.Require().NoError(err)
	return NewIdentityBoundFS(base, id).(*identityBoundFS)
}

// TestDacReadSearchReadsForeignFile — a uid 65501 identity with dac_read_search
// can open and read secret.txt (owned by 65500, mode 0o600) via O_RDONLY.
func (s *BoundFSCapsSuite) TestDacReadSearchReadsForeignFile() {
	id := &Identity{Uid: 65501, Gid: 65501, Gids: []uint32{65501}, Caps: []string{CapDacReadSearch}}
	fs := s.newBoundFS(id)

	file, st := fs.Open("secret.txt", syscall.O_RDONLY, &fuse.Context{})
	s.Require().Equal(fuse.OK, st, "Open O_RDONLY should succeed with dac_read_search")
	s.Require().NotNil(file)
	defer file.Release()

	// Read a byte to confirm the handle is truly readable.
	buf := make([]byte, 10)
	rr, rst := file.Read(buf, 0)
	s.Require().Equal(fuse.OK, rst, "Read from handle should succeed")
	data, _ := rr.Bytes(buf)
	s.Require().Equal("hello caps", string(data))
}

// TestDacReadSearchCannotWriteForeignFile — same identity CANNOT open for
// writing; DAC_OVERRIDE is absent from effective so the kernel denies it.
func (s *BoundFSCapsSuite) TestDacReadSearchCannotWriteForeignFile() {
	id := &Identity{Uid: 65501, Gid: 65501, Gids: []uint32{65501}, Caps: []string{CapDacReadSearch}}
	fs := s.newBoundFS(id)

	_, st := fs.Open("secret.txt", syscall.O_WRONLY, &fuse.Context{})
	s.Require().Equal(fuse.EACCES, st, "Open O_WRONLY should be denied without dac_override")
}

// TestDacOverrideWritesForeignFile — uid 65501 with dac_override can open
// secret.txt (owned by 65500, 0o600) for writing.
func (s *BoundFSCapsSuite) TestDacOverrideWritesForeignFile() {
	id := &Identity{Uid: 65501, Gid: 65501, Gids: []uint32{65501}, Caps: []string{CapDacOverride}}
	fs := s.newBoundFS(id)

	file, st := fs.Open("secret.txt", syscall.O_WRONLY, &fuse.Context{})
	s.Require().Equal(fuse.OK, st, "Open O_WRONLY should succeed with dac_override")
	s.Require().NotNil(file)
	defer file.Release()

	_, wst := file.Write([]byte("admin"), 0)
	s.Require().Equal(fuse.OK, wst, "Write should succeed with dac_override")
}

// TestDacOverrideCreateChownsToPrincipal — Create and Mkdir under dac_override
// produce entries owned by the principal (uid 65501), not root.
func (s *BoundFSCapsSuite) TestDacOverrideCreateChownsToPrincipal() {
	id := &Identity{Uid: 65501, Gid: 65501, Gids: []uint32{65501}, Caps: []string{CapDacOverride}}
	fs := s.newBoundFS(id)

	// Create a new file.
	file, st := fs.Create("newfile.txt", syscall.O_WRONLY, 0o644, &fuse.Context{})
	s.Require().Equal(fuse.OK, st, "Create should succeed with dac_override")
	if file != nil {
		file.Release()
	}

	// The created file must be owned by the principal, not root.
	info, err := os.Stat(filepath.Join(s.tempDir, "newfile.txt"))
	s.Require().NoError(err)
	sysStat, ok := info.Sys().(*syscall.Stat_t)
	s.Require().True(ok, "expected *syscall.Stat_t from file info")
	s.Require().EqualValues(65501, sysStat.Uid, "newfile.txt should be owned by uid 65501, not root")

	// Mkdir: same chown expectation.
	mst := fs.Mkdir("newdir", 0o755, &fuse.Context{})
	s.Require().Equal(fuse.OK, mst, "Mkdir should succeed with dac_override")

	dinfo, err := os.Stat(filepath.Join(s.tempDir, "newdir"))
	s.Require().NoError(err)
	dStat, ok := dinfo.Sys().(*syscall.Stat_t)
	s.Require().True(ok, "expected *syscall.Stat_t from dir info")
	s.Require().EqualValues(65501, dStat.Uid, "newdir should be owned by uid 65501, not root")
}

// TestNoCapsUnchangedBehavior — an identity with no caps uses the classic
// setfsuid/setfsgid path and cannot read a file it doesn't own (0o600, uid 65500).
func (s *BoundFSCapsSuite) TestNoCapsUnchangedBehavior() {
	id := &Identity{Uid: 65501, Gid: 65501, Gids: []uint32{65501}} // no caps
	fs := s.newBoundFS(id)

	_, st := fs.Open("secret.txt", syscall.O_RDONLY, &fuse.Context{})
	s.Require().Equal(fuse.EACCES, st, "no-caps path should be denied: setfsuid removes root bypass")
}
