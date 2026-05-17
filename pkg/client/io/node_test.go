package io_test

import (
	"context"
	"syscall"
	"testing"

	iomocks "gmountie/internal/mocks/pkg/client/io"
	clientio "gmountie/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// NodeAdapterTestSuite exercises the gMountieRoot / gMountieNode /
// gMountieFile adapters with a mocked FileSystemBackend. The heavy
// semantic tests live in backend_grpc_test.go; this suite only verifies
// the delegation shape (path computation, type translation, errno
// propagation).
//
// The suite lives in package io_test so it can use the
// internal/mocks/pkg/client/io package, which itself imports
// gmountie/pkg/client/io (an internal test would deadlock the import
// graph). Adapters are exercised via the exported fs.NodeXXX /
// fs.FileXXX interfaces they implement on the value returned from
// NewMountieRoot.
type NodeAdapterTestSuite struct {
	suite.Suite
	backend *iomocks.MockFileSystemBackend
	root    fs.InodeEmbedder
}

func (s *NodeAdapterTestSuite) SetupTest() {
	s.backend = iomocks.NewMockFileSystemBackend(s.T())
	s.root = clientio.NewMountieRoot(s.backend)
	// fs.NewNodeFS wires up the rawBridge so Inode.NewInode() can run
	// without a real FUSE mount. We never call Mount, so no kernel
	// interaction; the bridge just satisfies the in-memory inode tree.
	fs.NewNodeFS(s.root, &fs.Options{})
}

// rootAs casts the root to the requested fs.NodeXXX interface, failing
// the test if the assertion doesn't hold. Centralizing the cast keeps
// the per-test code readable.
func rootAs[T any](s *NodeAdapterTestSuite) T {
	v, ok := s.root.(T)
	s.Require().True(ok, "root does not implement %T", *new(T))
	return v
}

// --- Lookup ---

func (s *NodeAdapterTestSuite) TestRootLookup_Found() {
	s.backend.EXPECT().Lookup(mock.Anything, "", "child").Return(
		&clientio.Attr{Ino: 42, Mode: fuse.S_IFREG | 0o644}, fuse.OK,
	)
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeLookuper](s).Lookup(context.Background(), "child", out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(42), out.Attr.Ino)
	s.Assert().Equal(uint64(42), inode.StableAttr().Ino)
}

func (s *NodeAdapterTestSuite) TestRootLookup_NotFound() {
	s.backend.EXPECT().Lookup(mock.Anything, "", "missing").Return(nil, fuse.ENOENT)
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeLookuper](s).Lookup(context.Background(), "missing", out)
	s.Require().Equal(syscall.Errno(fuse.ENOENT), errno)
	s.Assert().Nil(inode)
}

// childNode does a Lookup against the root so we get a real child
// InodeEmbedder for path-computation tests (gMountieNode is unexported).
func (s *NodeAdapterTestSuite) childNode(name string, ino uint64) fs.InodeEmbedder {
	s.backend.EXPECT().Lookup(mock.Anything, "", name).Return(
		&clientio.Attr{Ino: ino, Mode: fuse.S_IFDIR | 0o755}, fuse.OK,
	).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeLookuper](s).Lookup(context.Background(), name, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	return inode.Operations()
}

// --- Readdir ---

func (s *NodeAdapterTestSuite) TestRootReaddir_Happy() {
	s.backend.EXPECT().ListDir(mock.Anything, "").Return(
		[]clientio.DirEntry{
			{Ino: 1, Mode: fuse.S_IFREG | 0o644, Name: "a"},
			{Ino: 2, Mode: fuse.S_IFDIR | 0o755, Name: "b"},
		}, fuse.OK,
	)
	stream, errno := rootAs[fs.NodeReaddirer](s).Readdir(context.Background())
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(stream)
	var got []string
	for stream.HasNext() {
		e, _ := stream.Next()
		got = append(got, e.Name)
	}
	stream.Close()
	s.Assert().Equal([]string{"a", "b"}, got)
}

// --- Open ---

func (s *NodeAdapterTestSuite) TestRootOpen_Happy() {
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(fh, fuse.OK)
	got, flags, errno := rootAs[fs.NodeOpener](s).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(0), flags)
	s.Require().NotNil(got)
}

func (s *NodeAdapterTestSuite) TestRootOpen_Error() {
	s.backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(nil, fuse.EACCES)
	got, _, errno := rootAs[fs.NodeOpener](s).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(fuse.EACCES), errno)
	s.Assert().Nil(got)
}

// --- Create ---

func (s *NodeAdapterTestSuite) TestRootCreate_Happy() {
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Create(mock.Anything, "", "new.txt", uint32(0), uint32(0o644)).
		Return(fh, nil, fuse.OK)
	// proto.CreateReply carries no Attr today; createAt falls back to Stat.
	s.backend.EXPECT().Stat(mock.Anything, "new.txt").Return(
		&clientio.Attr{Ino: 7, Mode: fuse.S_IFREG | 0o644}, fuse.OK,
	)
	out := &fuse.EntryOut{}
	inode, file, _, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "new.txt", 0, 0o644, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Require().NotNil(file)
	s.Assert().Equal(uint64(7), out.Attr.Ino)
}

func (s *NodeAdapterTestSuite) TestRootCreate_Error() {
	s.backend.EXPECT().Create(mock.Anything, "", "new.txt", uint32(0), uint32(0o644)).
		Return(nil, nil, fuse.EPERM)
	out := &fuse.EntryOut{}
	inode, file, _, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "new.txt", 0, 0o644, out)
	s.Require().Equal(syscall.Errno(fuse.EPERM), errno)
	s.Assert().Nil(inode)
	s.Assert().Nil(file)
}

// TestRootCreate_StatFailureSurfacesError verifies that when Create
// succeeds but the post-Create Stat fails (proto.CreateReply carries no
// Attr today, so node.go falls back to Stat), the error is surfaced
// rather than swallowed. Returning the handle with a zero EntryOut
// would poison the kernel dentry cache for EntryTimeout seconds.
func (s *NodeAdapterTestSuite) TestRootCreate_StatFailureSurfacesError() {
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Create(mock.Anything, "", "new.txt", uint32(0), uint32(0o644)).
		Return(fh, nil, fuse.OK)
	s.backend.EXPECT().Stat(mock.Anything, "new.txt").Return(nil, fuse.ENOENT)
	out := &fuse.EntryOut{}
	inode, file, _, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "new.txt", 0, 0o644, out)
	s.Require().Equal(syscall.Errno(fuse.ENOENT), errno)
	s.Assert().Nil(inode)
	s.Assert().Nil(file)
	// The kernel dentry cache must not be poisoned with a zero EntryOut.
	s.Assert().Equal(uint64(0), out.Attr.Size)
	s.Assert().Equal(uint32(0), out.Attr.Mode)
}

// --- Getattr ---

func (s *NodeAdapterTestSuite) TestRootGetattr_DelegatesToStat() {
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Size: 1024, Mode: fuse.S_IFDIR | 0o755}, fuse.OK,
	)
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeGetattrer](s).Getattr(context.Background(), nil, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(1024), out.Attr.Size)
}

// --- Setattr ---

func (s *NodeAdapterTestSuite) TestRootSetattr_TruncateAndChmod() {
	s.backend.EXPECT().Truncate(mock.Anything, "", uint64(512)).Return(fuse.OK)
	s.backend.EXPECT().Chmod(mock.Anything, "", uint32(0o600)).Return(fuse.OK)
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Size: 512, Mode: fuse.S_IFREG | 0o600}, fuse.OK,
	)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE | fuse.FATTR_MODE
	in.Size = 512
	in.Mode = 0o600
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(512), out.Attr.Size)
}

func (s *NodeAdapterTestSuite) TestRootSetattr_ChownPartial_StatsForUnset() {
	// Only GID is set; setattrAt must Stat first to fill UID, then call
	// Chown with the read UID. Two Stats happen: once for the fill-in,
	// once for the final out.
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Uid: 1000, Gid: 1000, Mode: fuse.S_IFREG | 0o644}, fuse.OK,
	).Twice()
	s.backend.EXPECT().Chown(mock.Anything, "", uint32(1000), uint32(2000)).Return(fuse.OK)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_GID
	in.Owner = fuse.Owner{Gid: 2000}
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
}

// --- Mkdir ---

func (s *NodeAdapterTestSuite) TestRootMkdir_Happy() {
	s.backend.EXPECT().Mkdir(mock.Anything, "newdir", uint32(0o755)).Return(fuse.OK)
	s.backend.EXPECT().Stat(mock.Anything, "newdir").Return(
		&clientio.Attr{Ino: 9, Mode: fuse.S_IFDIR | 0o755}, fuse.OK,
	)
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeMkdirer](s).Mkdir(context.Background(), "newdir", 0o755, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(9), out.Attr.Ino)
}

// --- Rmdir / Unlink ---

func (s *NodeAdapterTestSuite) TestRootRmdir() {
	s.backend.EXPECT().Rmdir(mock.Anything, "olddir").Return(fuse.OK)
	errno := rootAs[fs.NodeRmdirer](s).Rmdir(context.Background(), "olddir")
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootUnlink() {
	s.backend.EXPECT().Unlink(mock.Anything, "gone.txt").Return(fuse.OK)
	errno := rootAs[fs.NodeUnlinker](s).Unlink(context.Background(), "gone.txt")
	s.Assert().Equal(syscall.Errno(0), errno)
}

// --- Rename ---

func (s *NodeAdapterTestSuite) TestRootRename_RootToRoot() {
	s.backend.EXPECT().Rename(mock.Anything, "a.txt", "b.txt").Return(fuse.OK)
	errno := rootAs[fs.NodeRenamer](s).Rename(context.Background(), "a.txt", s.root, "b.txt", 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootRename_RootToNode() {
	// Look up a child to get a real gMountieNode InodeEmbedder, then
	// rename root/"a.txt" -> sub/"b.txt" and verify the path computed.
	sub := s.childNode("sub", 5)
	s.backend.EXPECT().Rename(mock.Anything, "a.txt", "sub/b.txt").Return(fuse.OK)
	errno := rootAs[fs.NodeRenamer](s).Rename(context.Background(), "a.txt", sub, "b.txt", 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootRename_UnknownParent_EINVAL() {
	// A foreign InodeEmbedder (raw fs.Inode) should map to EINVAL —
	// Rename should never see one in practice, but the guard documents
	// the invariant.
	errno := rootAs[fs.NodeRenamer](s).Rename(context.Background(), "a.txt", &fs.Inode{}, "b.txt", 0)
	s.Assert().Equal(syscall.Errno(fuse.EINVAL), errno)
}

// --- Statfs ---

func (s *NodeAdapterTestSuite) TestRootStatfs() {
	s.backend.EXPECT().StatFs(mock.Anything, "").Return(
		&clientio.StatFs{Blocks: 100, Bfree: 50, Bsize: 4096, Namelen: 255}, fuse.OK,
	)
	out := &fuse.StatfsOut{}
	errno := rootAs[fs.NodeStatfser](s).Statfs(context.Background(), out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(100), out.Blocks)
	s.Assert().Equal(uint32(4096), out.Bsize)
	s.Assert().Equal(uint32(255), out.NameLen)
}

// --- Access ---

func (s *NodeAdapterTestSuite) TestRootAccess() {
	s.backend.EXPECT().Access(mock.Anything, "", uint32(4)).Return(fuse.OK)
	errno := rootAs[fs.NodeAccesser](s).Access(context.Background(), 4)
	s.Assert().Equal(syscall.Errno(0), errno)
}

// --- Getxattr ---

func (s *NodeAdapterTestSuite) TestRootGetxattr_Happy() {
	s.backend.EXPECT().GetXAttr(mock.Anything, "", "user.foo").Return([]byte("bar"), fuse.OK)
	dest := make([]byte, 16)
	n, errno := rootAs[fs.NodeGetxattrer](s).Getxattr(context.Background(), "user.foo", dest)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(3), n)
	s.Assert().Equal("bar", string(dest[:n]))
}

func (s *NodeAdapterTestSuite) TestRootGetxattr_Erange() {
	s.backend.EXPECT().GetXAttr(mock.Anything, "", "user.foo").Return([]byte("bar"), fuse.OK)
	dest := make([]byte, 1)
	n, errno := rootAs[fs.NodeGetxattrer](s).Getxattr(context.Background(), "user.foo", dest)
	s.Assert().Equal(syscall.Errno(fuse.ERANGE), errno)
	s.Assert().Equal(uint32(3), n) // tells caller the required size
}

// --- gMountieNode path computation ---

func (s *NodeAdapterTestSuite) TestNodeLookup_BuildsChildPath() {
	sub := s.childNode("sub", 11)
	s.backend.EXPECT().Lookup(mock.Anything, "sub", "child").Return(
		&clientio.Attr{Ino: 12, Mode: fuse.S_IFREG | 0o644}, fuse.OK,
	)
	out := &fuse.EntryOut{}
	inode, errno := sub.(fs.NodeLookuper).Lookup(context.Background(), "child", out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
}

// --- gMountieFile ---

// openFile drives Open against the root so we get a real gMountieFile;
// the helper returns it as fs.FileHandle and the underlying mock so
// per-handle EXPECTs can be set up.
func (s *NodeAdapterTestSuite) openFile() (fs.FileHandle, *iomocks.MockFileHandle) {
	mockFH := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(mockFH, fuse.OK).Once()
	fh, _, errno := rootAs[fs.NodeOpener](s).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	return fh, mockFH
}

func (s *NodeAdapterTestSuite) TestFileRead_Happy() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Read(mock.Anything, mockFH, int64(0), mock.Anything).
		RunAndReturn(func(_ context.Context, _ clientio.FileHandle, _ int64, dst []byte) (int, fuse.Status) {
			copy(dst, []byte("hello"))
			return 5, fuse.OK
		})
	dest := make([]byte, 16)
	res, errno := fh.(fs.FileReader).Read(context.Background(), dest, 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(res)
	buf := make([]byte, 16)
	out, st := res.Bytes(buf)
	s.Require().Equal(fuse.OK, st)
	s.Assert().Equal("hello", string(out))
}

func (s *NodeAdapterTestSuite) TestFileRead_Error() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Read(mock.Anything, mockFH, int64(0), mock.Anything).
		Return(0, fuse.EIO)
	dest := make([]byte, 16)
	res, errno := fh.(fs.FileReader).Read(context.Background(), dest, 0)
	s.Assert().Equal(syscall.Errno(fuse.EIO), errno)
	s.Assert().Nil(res)
}

func (s *NodeAdapterTestSuite) TestFileWrite_Happy() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Write(mock.Anything, mockFH, int64(8), []byte("payload")).
		Return(uint32(7), fuse.OK)
	n, errno := fh.(fs.FileWriter).Write(context.Background(), []byte("payload"), 8)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(7), n)
}

func (s *NodeAdapterTestSuite) TestFileFlush() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Flush(mock.Anything, mockFH).Return(fuse.OK)
	s.Assert().Equal(syscall.Errno(0), fh.(fs.FileFlusher).Flush(context.Background()))
}

func (s *NodeAdapterTestSuite) TestFileFsync() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Fsync(mock.Anything, mockFH, int64(0)).Return(fuse.OK)
	s.Assert().Equal(syscall.Errno(0), fh.(fs.FileFsyncer).Fsync(context.Background(), 0))
}

func (s *NodeAdapterTestSuite) TestFileRelease() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Release(mock.Anything, mockFH).Return(fuse.OK)
	s.Assert().Equal(syscall.Errno(0), fh.(fs.FileReleaser).Release(context.Background()))
}

func (s *NodeAdapterTestSuite) TestFileAllocate() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Allocate(mock.Anything, mockFH, uint64(0), uint64(4096), uint32(0)).Return(fuse.OK)
	errno := fh.(fs.FileAllocater).Allocate(context.Background(), 0, 4096, 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestFileGetlk() {
	fh, mockFH := s.openFile()
	lk := &fuse.FileLock{Start: 0, End: 16, Typ: 1, Pid: 99}
	s.backend.EXPECT().GetLk(mock.Anything, mockFH, uint64(42), lk, uint32(0), mock.AnythingOfType("*fuse.FileLock")).
		Return(fuse.OK)
	out := &fuse.FileLock{}
	errno := fh.(fs.FileGetlker).Getlk(context.Background(), 42, lk, 0, out)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestFileSetlk() {
	fh, mockFH := s.openFile()
	lk := &fuse.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.backend.EXPECT().SetLk(mock.Anything, mockFH, uint64(7), lk, uint32(0)).Return(fuse.OK)
	errno := fh.(fs.FileSetlker).Setlk(context.Background(), 7, lk, 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestFileSetlkw() {
	fh, mockFH := s.openFile()
	lk := &fuse.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.backend.EXPECT().SetLkw(mock.Anything, mockFH, uint64(7), lk, uint32(0)).Return(fuse.OK)
	errno := fh.(fs.FileSetlkwer).Setlkw(context.Background(), 7, lk, 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func TestNodeAdapterTestSuite(t *testing.T) {
	suite.Run(t, new(NodeAdapterTestSuite))
}
