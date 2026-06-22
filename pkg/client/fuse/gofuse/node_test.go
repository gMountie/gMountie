package gofuse_test

import (
	"context"
	"syscall"
	"testing"
	"time"

	iomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/io"
	gofusepkg "go.gmountie.dev/gmountie/pkg/client/fuse/gofuse"
	clientio "go.gmountie.dev/gmountie/pkg/client/io"
	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// NodeAdapterTestSuite exercises the gMountieNode (root and descendants) /
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
	s.root = gofusepkg.NewMountieRoot(s.backend, false)
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
		&clientio.Attr{Ino: 42, Mode: fuse.S_IFREG | 0o644}, proto.FsError_FS_OK,
	)
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeLookuper](s).Lookup(context.Background(), "child", out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(42), out.Ino)
	s.Assert().Equal(uint64(42), inode.StableAttr().Ino)
}

func (s *NodeAdapterTestSuite) TestRootLookup_NotFound() {
	s.backend.EXPECT().Lookup(mock.Anything, "", "missing").Return(nil, proto.FsError_FS_ENOENT)
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeLookuper](s).Lookup(context.Background(), "missing", out)
	s.Require().Equal(fserr.ToErrno(proto.FsError_FS_ENOENT), errno)
	s.Assert().Nil(inode)
}

// childNode does a Lookup against the root so we get a real child
// InodeEmbedder for path-computation tests (gMountieNode is unexported).
// node.path() walks the live inode tree (via Inode.Path), which the FUSE
// bridge normally seeds by calling AddChild after each Lookup; tests run
// without the bridge, so we attach the child here ourselves.
func (s *NodeAdapterTestSuite) childNode(name string, ino uint64) fs.InodeEmbedder {
	s.backend.EXPECT().Lookup(mock.Anything, "", name).Return(
		&clientio.Attr{Ino: ino, Mode: fuse.S_IFDIR | 0o755}, proto.FsError_FS_OK,
	).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeLookuper](s).Lookup(context.Background(), name, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.root.EmbeddedInode().AddChild(name, inode, true)
	return inode.Operations()
}

// --- Readdir ---

func (s *NodeAdapterTestSuite) TestRootReaddir_Happy() {
	s.backend.EXPECT().ListDir(mock.Anything, "").Return(
		[]clientio.DirEntryPlus{
			{DirEntry: clientio.DirEntry{Ino: 1, Mode: fuse.S_IFREG | 0o644, Name: "a"}},
			{DirEntry: clientio.DirEntry{Ino: 2, Mode: fuse.S_IFDIR | 0o755, Name: "b"}},
		}, proto.FsError_FS_OK,
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
	s.backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(fh, proto.FsError_FS_OK)
	got, flags, errno := rootAs[fs.NodeOpener](s).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(0), flags)
	s.Require().NotNil(got)
}

func (s *NodeAdapterTestSuite) TestRootOpen_Error() {
	s.backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(nil, proto.FsError_FS_EACCES)
	got, _, errno := rootAs[fs.NodeOpener](s).Open(context.Background(), 0)
	s.Require().Equal(fserr.ToErrno(proto.FsError_FS_EACCES), errno)
	s.Assert().Nil(got)
}

// --- Create ---

func (s *NodeAdapterTestSuite) TestRootCreate_Happy() {
	fh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Create(mock.Anything, "", "new.txt", uint32(0), uint32(0o644)).
		Return(fh, nil, proto.FsError_FS_OK)
	// proto.CreateReply carries no Attr today; createAt falls back to Stat.
	s.backend.EXPECT().Stat(mock.Anything, "new.txt").Return(
		&clientio.Attr{Ino: 7, Mode: fuse.S_IFREG | 0o644}, proto.FsError_FS_OK,
	)
	out := &fuse.EntryOut{}
	inode, file, _, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "new.txt", 0, 0o644, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Require().NotNil(file)
	s.Assert().Equal(uint64(7), out.Ino)
}

func (s *NodeAdapterTestSuite) TestRootCreate_Error() {
	s.backend.EXPECT().Create(mock.Anything, "", "new.txt", uint32(0), uint32(0o644)).
		Return(nil, nil, proto.FsError_FS_EPERM)
	out := &fuse.EntryOut{}
	inode, file, _, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "new.txt", 0, 0o644, out)
	s.Require().Equal(fserr.ToErrno(proto.FsError_FS_EPERM), errno)
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
		Return(fh, nil, proto.FsError_FS_OK)
	s.backend.EXPECT().Stat(mock.Anything, "new.txt").Return(nil, proto.FsError_FS_ENOENT)
	out := &fuse.EntryOut{}
	inode, file, _, errno := rootAs[fs.NodeCreater](s).Create(context.Background(), "new.txt", 0, 0o644, out)
	s.Require().Equal(fserr.ToErrno(proto.FsError_FS_ENOENT), errno)
	s.Assert().Nil(inode)
	s.Assert().Nil(file)
	// The kernel dentry cache must not be poisoned with a zero EntryOut.
	s.Assert().Equal(uint64(0), out.Size)
	s.Assert().Equal(uint32(0), out.Mode)
}

// --- Getattr ---

func (s *NodeAdapterTestSuite) TestRootGetattr_DelegatesToStat() {
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Size: 1024, Mode: fuse.S_IFDIR | 0o755}, proto.FsError_FS_OK,
	)
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeGetattrer](s).Getattr(context.Background(), nil, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(1024), out.Size)
}

// --- Setattr ---

// TestRootSetattr_SingleRPC_ModeSizeMtime is the core RTT-win pin: a SETATTR
// touching mode+size+mtime issues exactly ONE backend.SetAttr (the old path
// fanned out Truncate+Chmod+Utimens+trailing Stat). out.Attr is filled from
// the reply — no Stat expectation is registered, so any trailing Stat fails
// the test (absence proof).
func (s *NodeAdapterTestSuite) TestRootSetattr_SingleRPC_ModeSizeMtime() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == fuse.FATTR_SIZE|fuse.FATTR_MODE|fuse.FATTR_MTIME &&
			in.Size == 512 && in.Mode == 0o600 &&
			in.Atime == nil &&
			in.Mtime != nil && in.Mtime.Unix() == 1577836800
	})).Return(&clientio.Attr{Ino: 1, Size: 512, Mode: fuse.S_IFREG | 0o600, Mtime: 1577836800}, proto.FsError_FS_OK).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE | fuse.FATTR_MODE | fuse.FATTR_MTIME
	in.Size = 512
	in.Mode = 0o600
	in.Mtime = 1577836800
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(512), out.Size)
	s.Assert().Equal(uint32(fuse.S_IFREG|0o600), out.Mode)
	s.Assert().Equal(uint64(1577836800), out.Mtime)
}

// TestRootSetattr_NowBitResolvesToConcreteTime pins the _NOW contract: the
// server does NOT interpret FATTR_MTIME_NOW, so the client must resolve
// "now" before the wire. go-fuse's GetMTime() folds the _NOW bit into
// time.Now(); the wire SetAttrIn must carry plain FATTR_MTIME with that
// concrete timestamp and must NOT forward the _NOW bit.
func (s *NodeAdapterTestSuite) TestRootSetattr_NowBitResolvesToConcreteTime() {
	before := time.Now().Add(-time.Minute)
	after := time.Now().Add(time.Minute)
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == uint32(fuse.FATTR_MTIME) && // _NOW bit stripped
			in.Mtime != nil && in.Mtime.After(before) && in.Mtime.Before(after)
	})).Return(&clientio.Attr{Ino: 1}, proto.FsError_FS_OK).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MTIME | fuse.FATTR_MTIME_NOW
	in.Mtime = 0 // kernel sends no meaningful timestamp with _NOW
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootSetattr_SizeOnly() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == uint32(fuse.FATTR_SIZE) && in.Size == 4096 &&
			in.Atime == nil && in.Mtime == nil
	})).Return(&clientio.Attr{Ino: 1, Size: 4096}, proto.FsError_FS_OK).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = 4096
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(4096), out.Size)
}

func (s *NodeAdapterTestSuite) TestRootSetattr_TimesOnly() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == fuse.FATTR_ATIME|fuse.FATTR_MTIME &&
			in.Atime != nil && in.Atime.Unix() == 100 &&
			in.Mtime != nil && in.Mtime.Unix() == 200
	})).Return(&clientio.Attr{Ino: 1, Atime: 100, Mtime: 200}, proto.FsError_FS_OK).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_ATIME | fuse.FATTR_MTIME
	in.Atime = 100
	in.Mtime = 200
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(100), out.Atime)
	s.Assert().Equal(uint64(200), out.Mtime)
}

// TestRootSetattr_GidOnly_ForwardsBitNoStat replaces the old client-side
// half-fill: with only FATTR_GID set, the client used to Stat first to read
// the unset uid. The server resolves the unset half now (Task 12), so the
// client just forwards the GID bit — no Stat expectation is registered.
func (s *NodeAdapterTestSuite) TestRootSetattr_GidOnly_ForwardsBitNoStat() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == uint32(fuse.FATTR_GID) && in.Gid == 2000
	})).Return(&clientio.Attr{Ino: 1, Uid: 1000, Gid: 2000}, proto.FsError_FS_OK).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_GID
	in.Owner = fuse.Owner{Gid: 2000}
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(2000), out.Gid)
}

func (s *NodeAdapterTestSuite) TestRootSetattr_BackendErrorPropagates() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.Anything).Return(nil, proto.FsError_FS_EPERM).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0o600
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Assert().Equal(fserr.ToErrno(proto.FsError_FS_EPERM), errno)
}

// TestRootSetattr_Valid0_FHOnly: a fuse.SetAttrIn with ONLY FATTR_FH set
// (no contract bits — FATTR_FH is a kernel-internal flag that go-fuse
// propagates to the node but our adapter strips via setAttrValidMask) flows
// through Setattr as a backend.SetAttr call with Valid==0. The reply attrs
// are returned into out.Attr and no error is produced. No other backend
// call (Stat etc.) is expected — the strict mock enforces the absence
// proof.
func (s *NodeAdapterTestSuite) TestRootSetattr_Valid0_FHOnly() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.MatchedBy(func(in clientio.SetAttrIn) bool {
		return in.Valid == 0 // all contract bits were masked out
	})).Return(&clientio.Attr{Ino: 5, Mode: fuse.S_IFREG | 0o644, Size: 128}, proto.FsError_FS_OK).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_FH // kernel-only bit; no contract bit survives masking
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(5), out.Ino)
	s.Assert().Equal(uint64(128), out.Size)
	s.Assert().Equal(uint32(fuse.S_IFREG|0o644), out.Mode)
}

// TestRootSetattr_NilAttrsFallsBackToStat: a server whose post-apply stat
// failed replies OK with no attrs; the node must Stat rather than hand the
// kernel a zero fuse.Attr (cache-poisoning concern, mirrors Create).
func (s *NodeAdapterTestSuite) TestRootSetattr_NilAttrsFallsBackToStat() {
	s.backend.EXPECT().SetAttr(mock.Anything, "", mock.Anything).Return(nil, proto.FsError_FS_OK).Once()
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Mode: fuse.S_IFREG | 0o600}, proto.FsError_FS_OK,
	).Once()
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0o600
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(fuse.S_IFREG|0o600), out.Mode)
}

// --- Mkdir ---

// TestRootMkdir_Happy: the EntryOut is filled from the reply attrs, with NO
// trailing Stat — no Stat expectation is registered, so the strict mock
// proves the absence (the Mkdir RTT win).
func (s *NodeAdapterTestSuite) TestRootMkdir_Happy() {
	s.backend.EXPECT().Mkdir(mock.Anything, "newdir", uint32(0o755)).Return(
		&clientio.Attr{Ino: 9, Mode: fuse.S_IFDIR | 0o755}, proto.FsError_FS_OK,
	).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeMkdirer](s).Mkdir(context.Background(), "newdir", 0o755, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(9), out.Ino)
	s.Assert().Equal(uint32(fuse.S_IFDIR|0o755), out.Mode)
}

// TestRootMkdir_NilAttrsFallsBackToStat: a server whose post-create stat
// failed replies OK with no attrs; the node must Stat ONCE rather than hand
// the kernel a zero EntryOut (kernel-cache poisoning — same rationale as
// the Create/Setattr fallbacks).
func (s *NodeAdapterTestSuite) TestRootMkdir_NilAttrsFallsBackToStat() {
	s.backend.EXPECT().Mkdir(mock.Anything, "newdir", uint32(0o755)).Return(nil, proto.FsError_FS_OK).Once()
	s.backend.EXPECT().Stat(mock.Anything, "newdir").Return(
		&clientio.Attr{Ino: 9, Mode: fuse.S_IFDIR | 0o755}, proto.FsError_FS_OK,
	).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeMkdirer](s).Mkdir(context.Background(), "newdir", 0o755, out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(9), out.Ino)
}

func (s *NodeAdapterTestSuite) TestRootMkdir_BackendErrorPropagates() {
	s.backend.EXPECT().Mkdir(mock.Anything, "newdir", uint32(0o755)).Return(nil, proto.FsError_FS_EACCES).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeMkdirer](s).Mkdir(context.Background(), "newdir", 0o755, out)
	s.Assert().Equal(fserr.ToErrno(proto.FsError_FS_EACCES), errno)
	s.Assert().Nil(inode)
}

// --- Symlink ---

// TestRootSymlink_Happy: like Mkdir, the EntryOut comes straight from the
// reply attrs (S_IFLNK — the link itself); no Stat expectation = absence
// proof of the trailing Stat.
func (s *NodeAdapterTestSuite) TestRootSymlink_Happy() {
	s.backend.EXPECT().Symlink(mock.Anything, "/elsewhere", "lnk").Return(
		&clientio.Attr{Ino: 13, Mode: fuse.S_IFLNK | 0o777}, proto.FsError_FS_OK,
	).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeSymlinker](s).Symlink(context.Background(), "/elsewhere", "lnk", out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(13), out.Ino)
	s.Assert().Equal(uint32(fuse.S_IFLNK|0o777), out.Mode)
}

// TestRootSymlink_NilAttrsFallsBackToStat mirrors the Mkdir fallback: nil
// reply attrs → exactly one Stat, never a zero EntryOut.
func (s *NodeAdapterTestSuite) TestRootSymlink_NilAttrsFallsBackToStat() {
	s.backend.EXPECT().Symlink(mock.Anything, "/elsewhere", "lnk").Return(nil, proto.FsError_FS_OK).Once()
	s.backend.EXPECT().Stat(mock.Anything, "lnk").Return(
		&clientio.Attr{Ino: 13, Mode: fuse.S_IFLNK | 0o777}, proto.FsError_FS_OK,
	).Once()
	out := &fuse.EntryOut{}
	inode, errno := rootAs[fs.NodeSymlinker](s).Symlink(context.Background(), "/elsewhere", "lnk", out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Require().NotNil(inode)
	s.Assert().Equal(uint64(13), out.Ino)
}

// --- Rmdir / Unlink ---

func (s *NodeAdapterTestSuite) TestRootRmdir() {
	s.backend.EXPECT().Rmdir(mock.Anything, "olddir").Return(proto.FsError_FS_OK)
	errno := rootAs[fs.NodeRmdirer](s).Rmdir(context.Background(), "olddir")
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootUnlink() {
	s.backend.EXPECT().Unlink(mock.Anything, "gone.txt").Return(proto.FsError_FS_OK)
	errno := rootAs[fs.NodeUnlinker](s).Unlink(context.Background(), "gone.txt")
	s.Assert().Equal(syscall.Errno(0), errno)
}

// --- Rename ---

func (s *NodeAdapterTestSuite) TestRootRename_RootToRoot() {
	s.backend.EXPECT().Rename(mock.Anything, "a.txt", "b.txt").Return(proto.FsError_FS_OK)
	errno := rootAs[fs.NodeRenamer](s).Rename(context.Background(), "a.txt", s.root, "b.txt", 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootRename_RootToNode() {
	// Look up a child to get a real gMountieNode InodeEmbedder, then
	// rename root/"a.txt" -> sub/"b.txt" and verify the path computed.
	sub := s.childNode("sub", 5)
	s.backend.EXPECT().Rename(mock.Anything, "a.txt", "sub/b.txt").Return(proto.FsError_FS_OK)
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
		&clientio.StatFs{Blocks: 100, Bfree: 50, Bsize: 4096, Namelen: 255}, proto.FsError_FS_OK,
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
	s.backend.EXPECT().Access(mock.Anything, "", uint32(4)).Return(proto.FsError_FS_OK)
	errno := rootAs[fs.NodeAccesser](s).Access(context.Background(), 4)
	s.Assert().Equal(syscall.Errno(0), errno)
}

// --- Getxattr ---

func (s *NodeAdapterTestSuite) TestRootGetxattr_Happy() {
	s.backend.EXPECT().GetXAttr(mock.Anything, "", "user.foo").Return([]byte("bar"), proto.FsError_FS_OK)
	dest := make([]byte, 16)
	n, errno := rootAs[fs.NodeGetxattrer](s).Getxattr(context.Background(), "user.foo", dest)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(3), n)
	s.Assert().Equal("bar", string(dest[:n]))
}

func (s *NodeAdapterTestSuite) TestRootGetxattr_Erange() {
	s.backend.EXPECT().GetXAttr(mock.Anything, "", "user.foo").Return([]byte("bar"), proto.FsError_FS_OK)
	dest := make([]byte, 1)
	n, errno := rootAs[fs.NodeGetxattrer](s).Getxattr(context.Background(), "user.foo", dest)
	s.Assert().Equal(syscall.Errno(fuse.ERANGE), errno)
	s.Assert().Equal(uint32(3), n) // tells caller the required size
}

// --- gMountieNode path computation ---

func (s *NodeAdapterTestSuite) TestNodeLookup_BuildsChildPath() {
	sub := s.childNode("sub", 11)
	s.backend.EXPECT().Lookup(mock.Anything, "sub", "child").Return(
		&clientio.Attr{Ino: 12, Mode: fuse.S_IFREG | 0o644}, proto.FsError_FS_OK,
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
	s.backend.EXPECT().Open(mock.Anything, "", uint32(0)).Return(mockFH, proto.FsError_FS_OK).Once()
	fh, _, errno := rootAs[fs.NodeOpener](s).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	return fh, mockFH
}

func (s *NodeAdapterTestSuite) TestFileRead_Happy() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Read(mock.Anything, mockFH, int64(0), mock.Anything).
		RunAndReturn(func(_ context.Context, _ clientio.FileHandle, _ int64, dst []byte) (int, proto.FsError) {
			copy(dst, []byte("hello"))
			return 5, proto.FsError_FS_OK
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
		Return(0, proto.FsError_FS_EIO)
	dest := make([]byte, 16)
	res, errno := fh.(fs.FileReader).Read(context.Background(), dest, 0)
	s.Assert().Equal(fserr.ToErrno(proto.FsError_FS_EIO), errno)
	s.Assert().Nil(res)
}

func (s *NodeAdapterTestSuite) TestFileWrite_Happy() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Write(mock.Anything, mockFH, int64(8), []byte("payload")).
		Return(uint32(7), proto.FsError_FS_OK)
	n, errno := fh.(fs.FileWriter).Write(context.Background(), []byte("payload"), 8)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint32(7), n)
}

func (s *NodeAdapterTestSuite) TestFileFlush() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Flush(mock.Anything, mockFH).Return(proto.FsError_FS_OK)
	s.Assert().Equal(syscall.Errno(0), fh.(fs.FileFlusher).Flush(context.Background()))
}

func (s *NodeAdapterTestSuite) TestFileFsync() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Fsync(mock.Anything, mockFH, int64(0)).Return(proto.FsError_FS_OK)
	s.Assert().Equal(syscall.Errno(0), fh.(fs.FileFsyncer).Fsync(context.Background(), 0))
}

func (s *NodeAdapterTestSuite) TestFileRelease() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Release(mock.Anything, mockFH).Return(proto.FsError_FS_OK)
	s.Assert().Equal(syscall.Errno(0), fh.(fs.FileReleaser).Release(context.Background()))
}

func (s *NodeAdapterTestSuite) TestFileAllocate() {
	fh, mockFH := s.openFile()
	s.backend.EXPECT().Allocate(mock.Anything, mockFH, uint64(0), uint64(4096), uint32(0)).Return(proto.FsError_FS_OK)
	errno := fh.(fs.FileAllocater).Allocate(context.Background(), 0, 4096, 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestFileGetlk() {
	fh, mockFH := s.openFile()
	lk := &fuse.FileLock{Start: 0, End: 16, Typ: 1, Pid: 99}
	s.backend.EXPECT().GetLk(mock.Anything, mockFH, uint64(42), lk, uint32(0), mock.AnythingOfType("*fuse.FileLock")).
		Return(proto.FsError_FS_OK)
	out := &fuse.FileLock{}
	errno := fh.(fs.FileGetlker).Getlk(context.Background(), 42, lk, 0, out)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestFileSetlk() {
	fh, mockFH := s.openFile()
	lk := &fuse.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.backend.EXPECT().SetLk(mock.Anything, mockFH, uint64(7), lk, uint32(0)).Return(proto.FsError_FS_OK)
	errno := fh.(fs.FileSetlker).Setlk(context.Background(), 7, lk, 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestFileSetlkw() {
	fh, mockFH := s.openFile()
	lk := &fuse.FileLock{Start: 10, End: 20, Typ: 1, Pid: 5}
	s.backend.EXPECT().SetLkw(mock.Anything, mockFH, uint64(7), lk, uint32(0)).Return(proto.FsError_FS_OK)
	errno := fh.(fs.FileSetlkwer).Setlkw(context.Background(), 7, lk, 0)
	s.Assert().Equal(syscall.Errno(0), errno)
}

// openFileOn opens a file node with a distinct mock handle and returns
// the fs.FileHandle (a *gMountieFile) plus the mock leaf.
func (s *NodeAdapterTestSuite) openFileOn(node fs.InodeEmbedder, path string) (fs.FileHandle, *iomocks.MockFileHandle) {
	mfh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Open(mock.Anything, path, uint32(0)).Return(mfh, proto.FsError_FS_OK).Once()
	fh, _, errno := node.(fs.NodeOpener).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	return fh, mfh
}

func (s *NodeAdapterTestSuite) TestNodeCopyFileRange() {
	n1 := s.childNode("f1", 21)
	n2 := s.childNode("f2", 22)
	fh1, m1 := s.openFileOn(n1, "f1")
	fh2, m2 := s.openFileOn(n2, "f2")

	s.backend.EXPECT().CopyFileRange(mock.Anything, m1, uint64(0), m2, uint64(100), uint64(4096), uint64(0)).
		Return(uint64(4096), proto.FsError_FS_OK).Once()
	n, errno := n1.(fs.NodeCopyFileRanger).CopyFileRange(context.Background(), fh1, 0, nil, fh2, 100, 4096, 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Equal(uint32(4096), n)
}

func (s *NodeAdapterTestSuite) TestNodeCopyFileRange_ForeignHandle_EBADF() {
	n1 := s.childNode("f3", 23)
	fh1, _ := s.openFileOn(n1, "f3")
	_, errno := n1.(fs.NodeCopyFileRanger).CopyFileRange(context.Background(), fh1, 0, nil, nil, 0, 1, 0)
	s.Equal(syscall.EBADF, errno)

	// Also cover the foreign fhIn branch: valid fhOut, nil fhIn → EBADF.
	fh3, _ := s.openFileOn(n1, "f3")
	_, errno = n1.(fs.NodeCopyFileRanger).CopyFileRange(context.Background(), nil, 0, nil, fh3, 0, 1, 0)
	s.Equal(syscall.EBADF, errno)
}

func (s *NodeAdapterTestSuite) TestFileLseek() {
	n1 := s.childNode("f4", 24)
	fh1, m1 := s.openFileOn(n1, "f4")
	s.backend.EXPECT().Lseek(mock.Anything, m1, uint64(7), uint32(3) /* SEEK_DATA */).
		Return(uint64(42), proto.FsError_FS_OK).Once()
	off, errno := fh1.(fs.FileLseeker).Lseek(context.Background(), 7, 3)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Equal(uint64(42), off)
}

func (s *NodeAdapterTestSuite) TestSetxattr_RootAndNode() {
	s.backend.EXPECT().SetXAttr(mock.Anything, "", "user.k", []byte("v"), uint32(0)).Return(proto.FsError_FS_OK).Once()
	errno := rootAs[fs.NodeSetxattrer](s).Setxattr(context.Background(), "user.k", []byte("v"), 0)
	s.Equal(syscall.Errno(0), errno)

	child := s.childNode("d", 31)
	s.backend.EXPECT().SetXAttr(mock.Anything, "d", "user.k", []byte("v"), uint32(0)).Return(proto.FsError_FS_OK).Once()
	errno = child.(fs.NodeSetxattrer).Setxattr(context.Background(), "user.k", []byte("v"), 0)
	s.Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRemovexattr() {
	child := s.childNode("d2", 32)
	s.backend.EXPECT().RemoveXAttr(mock.Anything, "d2", "user.k").Return(proto.FsError_FS_OK).Once()
	errno := child.(fs.NodeRemovexattrer).Removexattr(context.Background(), "user.k")
	s.Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestListxattr_FitsAndERANGE() {
	child := s.childNode("d3", 33)
	s.backend.EXPECT().ListXAttr(mock.Anything, "d3").Return([]string{"user.a", "user.b"}, proto.FsError_FS_OK).Times(3)

	dest := make([]byte, 64)
	n, errno := child.(fs.NodeListxattrer).Listxattr(context.Background(), dest)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Equal(uint32(14), n) // "user.a\0user.b\0"
	s.Equal([]byte("user.a\x00user.b\x00"), dest[:n])

	small := make([]byte, 4)
	n, errno = child.(fs.NodeListxattrer).Listxattr(context.Background(), small)
	s.Equal(syscall.ERANGE, errno)
	s.Equal(uint32(14), n) // needed size still reported

	// size-probe (listxattr(path, NULL, 0)): zero-length dest → ERANGE + needed size
	n, errno = child.(fs.NodeListxattrer).Listxattr(context.Background(), nil)
	s.Equal(syscall.ERANGE, errno)
	s.Equal(uint32(14), n)
}

func (s *NodeAdapterTestSuite) TestListxattr_Empty() {
	child := s.childNode("d4", 34)
	s.backend.EXPECT().ListXAttr(mock.Anything, "d4").Return([]string{}, proto.FsError_FS_OK).Once()
	n, errno := child.(fs.NodeListxattrer).Listxattr(context.Background(), nil)
	s.Equal(syscall.Errno(0), errno)
	s.Equal(uint32(0), n)
}

func (s *NodeAdapterTestSuite) TestRemovexattr_Root() {
	s.backend.EXPECT().RemoveXAttr(mock.Anything, "", "user.k").Return(proto.FsError_FS_OK).Once()
	s.Equal(syscall.Errno(0), rootAs[fs.NodeRemovexattrer](s).Removexattr(context.Background(), "user.k"))
}

func (s *NodeAdapterTestSuite) TestListxattr_Root() {
	s.backend.EXPECT().ListXAttr(mock.Anything, "").Return([]string{"user.r"}, proto.FsError_FS_OK).Once()
	dest := make([]byte, 16)
	n, errno := rootAs[fs.NodeListxattrer](s).Listxattr(context.Background(), dest)
	s.Equal(syscall.Errno(0), errno)
	s.Equal(uint32(7), n)
	s.Equal([]byte("user.r\x00"), dest[:n])
}

func TestNodeAdapterTestSuite(t *testing.T) {
	suite.Run(t, new(NodeAdapterTestSuite))
}
