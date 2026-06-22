package fuse_test

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"

	iomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/io"
	clientfuse "go.gmountie.dev/gmountie/pkg/client/fuse"
	clientio "go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// Direct-IO policy tests. FOPEN_DIRECT_IO makes the kernel bypass its page
// cache for the handle and refuse a shared writable mmap with a clean error
// instead of letting the page fault die with SIGBUS. We return it
// unconditionally for SQLite shared-memory sidecars (-shm), which WAL mode
// MAP_SHARED-mmaps, and for every file when fuse.direct_io is set.

// TestRootOpen_ShmGetsDirectIO: opening a "*-shm" file must return
// FOPEN_DIRECT_IO even with the global flag off, so SQLite WAL fails cleanly
// instead of SIGBUS-ing (and bricking the DB via leftover sidecars).
func (s *NodeAdapterTestSuite) TestRootOpen_ShmGetsDirectIO() {
	child := s.childNode("notes.db-shm", 99)
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Open(mock.Anything, "notes.db-shm", uint32(0)).Return(fh, proto.FsError_FS_OK)

	_, flags, errno := child.(fs.NodeOpener).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(fuse.FOPEN_DIRECT_IO), flags&uint32(fuse.FOPEN_DIRECT_IO),
		"a -shm sidecar must be opened direct-IO to avoid the WAL mmap SIGBUS")
}

// TestRootOpen_RegularFileNoDirectIO: an ordinary file keeps the page cache
// (flags 0) with the global flag off — the -shm targeting must not regress
// the data path.
func (s *NodeAdapterTestSuite) TestRootOpen_RegularFileNoDirectIO() {
	child := s.childNode("bigdata.bin", 100)
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Open(mock.Anything, "bigdata.bin", uint32(0)).Return(fh, proto.FsError_FS_OK)

	_, flags, errno := child.(fs.NodeOpener).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(0), flags, "ordinary files must keep the kernel page cache")
}

// TestRootOpen_GlobalDirectIO: with fuse.direct_io=true every open is
// direct-IO, the documented escape hatch for general mmap workloads (LMDB…).
func (s *NodeAdapterTestSuite) TestRootOpen_GlobalDirectIO() {
	backend := iomocks.NewMockFileSystemBackend(s.T())
	root := clientfuse.NewMountieRoot(backend, true /* directIOAlways */)
	fs.NewNodeFS(root, &fs.Options{})

	fh := iomocks.NewMockFileHandle(s.T())
	backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(fh, proto.FsError_FS_OK)
	_, flags, errno := root.(fs.NodeOpener).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(fuse.FOPEN_DIRECT_IO), flags&uint32(fuse.FOPEN_DIRECT_IO),
		"fuse.direct_io must force direct-IO on every open")
}

// TestRootCreate_ShmGetsDirectIO: a freshly created -shm sidecar is mmap'd
// too, so Create must also return FOPEN_DIRECT_IO.
func (s *NodeAdapterTestSuite) TestRootCreate_ShmGetsDirectIO() {
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Create(mock.Anything, "", "notes.db-shm", uint32(0), uint32(0o644)).Return(
		fh, &clientio.Attr{Ino: 7, Mode: fuse.S_IFREG | 0o644}, proto.FsError_FS_OK,
	)
	out := &fuse.EntryOut{}
	_, _, flags, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "notes.db-shm", 0, 0o644, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(fuse.FOPEN_DIRECT_IO), flags&uint32(fuse.FOPEN_DIRECT_IO),
		"a newly created -shm sidecar must be opened direct-IO")
}
