// node.go bridges go-fuse v2 fs.NodeXXX interfaces to FileSystemBackend.
// Each adapter method is intentionally thin: translate path/types,
// delegate to the backend, and translate fuse.Status to syscall.Errno.
//
// Two node types exist: gMountieRoot is the mount point root, gMountieNode
// is every descendant. Both share a FileSystemBackend (set at
// construction). Paths are computed on demand via Inode.Path(nil) rather
// than cached on the struct — go-fuse mutates the inode tree under us
// (Rename's MvChild, hardlinks) and a cached path would go stale, sending
// subsequent ops to the old name on the server. Mirrors the
// LoopbackNode.relativePath() pattern in go-fuse's reference adapter.
// Shared logic lives in helper functions (lookupAt/openAt/createAt/etc.).
package io

import (
	"context"
	"path"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gMountieRoot is the root inode of a gMountie mount. Holds the
// FileSystemBackend that every descendant inode delegates to.
type gMountieRoot struct {
	fs.Inode

	backend FileSystemBackend
}

// NewMountieRoot constructs the root inode wrapping a FileSystemBackend.
// Mount code passes the returned value to fs.Mount.
func NewMountieRoot(backend FileSystemBackend) fs.InodeEmbedder {
	return &gMountieRoot{backend: backend}
}

// gMountieNode is a non-root inode. Its path relative to the mount root
// is derived on demand from the inode tree's current position (see
// path()); never cache it. backend is shared with the root (set at
// construction time).
type gMountieNode struct {
	fs.Inode

	backend FileSystemBackend
}

// path returns the inode's path relative to the mount root, with no
// leading slash. Computed from the live inode tree so that go-fuse's
// MvChild (during Rename) is reflected immediately. The root inode's
// path is "".
func (n *gMountieNode) path() string {
	return n.Path(nil)
}

// gMountieFile is the open-file adapter satisfying fs.FileReader,
// fs.FileWriter, fs.FileFlusher, fs.FileFsyncer, fs.FileReleaser.
type gMountieFile struct {
	backend FileSystemBackend
	fh      FileHandle
}

// Compile-time interface assertions — these ensure the node satisfies
// the go-fuse interfaces it claims to. If a signature drifts upstream,
// the build breaks here, not at mount-time.
var (
	_ fs.NodeLookuper   = (*gMountieRoot)(nil)
	_ fs.NodeReaddirer  = (*gMountieRoot)(nil)
	_ fs.NodeStatfser   = (*gMountieRoot)(nil)
	_ fs.NodeGetattrer  = (*gMountieRoot)(nil)
	_ fs.NodeSetattrer  = (*gMountieRoot)(nil)
	_ fs.NodeOpener     = (*gMountieRoot)(nil)
	_ fs.NodeCreater    = (*gMountieRoot)(nil)
	_ fs.NodeMkdirer    = (*gMountieRoot)(nil)
	_ fs.NodeRmdirer    = (*gMountieRoot)(nil)
	_ fs.NodeUnlinker   = (*gMountieRoot)(nil)
	_ fs.NodeRenamer    = (*gMountieRoot)(nil)
	_ fs.NodeAccesser   = (*gMountieRoot)(nil)
	_ fs.NodeGetxattrer = (*gMountieRoot)(nil)

	_ fs.NodeLookuper   = (*gMountieNode)(nil)
	_ fs.NodeReaddirer  = (*gMountieNode)(nil)
	_ fs.NodeStatfser   = (*gMountieNode)(nil)
	_ fs.NodeGetattrer  = (*gMountieNode)(nil)
	_ fs.NodeSetattrer  = (*gMountieNode)(nil)
	_ fs.NodeOpener     = (*gMountieNode)(nil)
	_ fs.NodeCreater    = (*gMountieNode)(nil)
	_ fs.NodeMkdirer    = (*gMountieNode)(nil)
	_ fs.NodeRmdirer    = (*gMountieNode)(nil)
	_ fs.NodeUnlinker   = (*gMountieNode)(nil)
	_ fs.NodeRenamer    = (*gMountieNode)(nil)
	_ fs.NodeAccesser   = (*gMountieNode)(nil)
	_ fs.NodeGetxattrer = (*gMountieNode)(nil)

	_ fs.FileReader    = (*gMountieFile)(nil)
	_ fs.FileWriter    = (*gMountieFile)(nil)
	_ fs.FileFlusher   = (*gMountieFile)(nil)
	_ fs.FileFsyncer   = (*gMountieFile)(nil)
	_ fs.FileReleaser  = (*gMountieFile)(nil)
	_ fs.FileAllocater = (*gMountieFile)(nil)
	_ fs.FileGetlker   = (*gMountieFile)(nil)
	_ fs.FileSetlker   = (*gMountieFile)(nil)
	_ fs.FileSetlkwer  = (*gMountieFile)(nil)
)

// setAttrFromBackend populates a fuse.Attr from a backend Attr. Used by
// Getattr/Lookup/Create/Mkdir handlers.
func setAttrFromBackend(dst *fuse.Attr, a *Attr) {
	dst.Ino = a.Ino
	dst.Size = a.Size
	dst.Blocks = a.Blocks
	dst.Atime = a.Atime
	dst.Mtime = a.Mtime
	dst.Ctime = a.Ctime
	dst.Atimensec = a.Atimensec
	dst.Mtimensec = a.Mtimensec
	dst.Ctimensec = a.Ctimensec
	dst.Mode = a.Mode
	dst.Nlink = a.Nlink
	dst.Owner.Uid = a.Uid
	dst.Owner.Gid = a.Gid
	dst.Rdev = a.Rdev
	dst.Blksize = a.Blksize
}

// childPath joins the parent path with the child name. Uses path.Join
// so "" + "x" -> "x", "a/b" + "x" -> "a/b/x".
func childPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return path.Join(parent, name)
}

// --- Lookup ---

func (r *gMountieRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupAt(ctx, &r.Inode, r.backend, "", name, out)
}

func (n *gMountieNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupAt(ctx, &n.Inode, n.backend, n.path(), name, out)
}

func lookupAt(ctx context.Context, parentInode *fs.Inode, backend FileSystemBackend, parent, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	a, st := backend.Lookup(ctx, parent, name)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a)
	child := parentInode.NewInode(ctx, &gMountieNode{
		backend: backend,
	}, fs.StableAttr{
		Mode: a.Mode,
		Ino:  a.Ino,
	})
	return child, 0
}

// --- Readdir ---

func (r *gMountieRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readdirAt(ctx, r.backend, "")
}

func (n *gMountieNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return readdirAt(ctx, n.backend, n.path())
}

func readdirAt(ctx context.Context, backend FileSystemBackend, p string) (fs.DirStream, syscall.Errno) {
	entries, st := backend.ListDir(ctx, p)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	fuseEntries := make([]fuse.DirEntry, 0, len(entries))
	for _, e := range entries {
		fuseEntries = append(fuseEntries, fuse.DirEntry{
			Mode: e.Mode,
			Name: e.Name,
			Ino:  e.Ino,
		})
	}
	return fs.NewListDirStream(fuseEntries), 0
}

// --- Getattr ---

func (r *gMountieRoot) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getattrAt(ctx, r.backend, "", out)
}

func (n *gMountieNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getattrAt(ctx, n.backend, n.path(), out)
}

func getattrAt(ctx context.Context, backend FileSystemBackend, p string, out *fuse.AttrOut) syscall.Errno {
	a, st := backend.Stat(ctx, p)
	if !st.Ok() {
		return syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a)
	return 0
}

// --- Setattr ---

func (r *gMountieRoot) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return setattrAt(ctx, r.backend, "", in, out)
}

func (n *gMountieNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return setattrAt(ctx, n.backend, n.path(), in, out)
}

// setattrAt dispatches on SetAttrIn.Valid flags. The backend has no
// atime/mtime mutation today, so those bits are silently no-op'd (same
// as the legacy code). For Chown with only one of uid/gid set, we Stat
// first to read the unset side, so we don't accidentally overwrite it.
func setattrAt(ctx context.Context, backend FileSystemBackend, p string, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if sz, ok := in.GetSize(); ok {
		if st := backend.Truncate(ctx, p, sz); !st.Ok() {
			return syscall.Errno(st)
		}
	}
	if mode, ok := in.GetMode(); ok {
		if st := backend.Chmod(ctx, p, mode); !st.Ok() {
			return syscall.Errno(st)
		}
	}
	uid, uidOK := in.GetUID()
	gid, gidOK := in.GetGID()
	if uidOK || gidOK {
		if uidOK != gidOK {
			a, sst := backend.Stat(ctx, p)
			if !sst.Ok() {
				return syscall.Errno(sst)
			}
			if !uidOK {
				uid = a.Uid
			}
			if !gidOK {
				gid = a.Gid
			}
		}
		if st := backend.Chown(ctx, p, uid, gid); !st.Ok() {
			return syscall.Errno(st)
		}
	}
	a, st := backend.Stat(ctx, p)
	if !st.Ok() {
		return syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a)
	return 0
}

// --- Open ---

func (r *gMountieRoot) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return openAt(ctx, r.backend, "", flags)
}

func (n *gMountieNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return openAt(ctx, n.backend, n.path(), flags)
}

func openAt(ctx context.Context, backend FileSystemBackend, p string, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	h, st := backend.Open(ctx, p, flags)
	if !st.Ok() {
		return nil, 0, syscall.Errno(st)
	}
	return &gMountieFile{backend: backend, fh: h}, 0, 0
}

// --- Create ---

func (r *gMountieRoot) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return createAt(ctx, &r.Inode, r.backend, "", name, flags, mode, out)
}

func (n *gMountieNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return createAt(ctx, &n.Inode, n.backend, n.path(), name, flags, mode, out)
}

func createAt(ctx context.Context, parentInode *fs.Inode, backend FileSystemBackend, parent, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	handle, attr, st := backend.Create(ctx, parent, name, flags, mode)
	if !st.Ok() {
		return nil, nil, 0, syscall.Errno(st)
	}
	full := childPath(parent, name)
	// Today proto.CreateReply doesn't carry Attr; fall back to Stat so
	// the kernel gets a populated EntryOut. If Stat fails, surface the
	// error rather than returning a zero EntryOut — the kernel would
	// otherwise cache the zero (Mode=0, Size=0) for EntryTimeout (~1s
	// per single.go) and poison subsequent stat ops. The server-side
	// Create already succeeded; this leaks a temporary fd until
	// Release-on-close fires, but that's bounded and cleaner than a
	// poisoned dentry cache.
	if attr == nil {
		a, sst := backend.Stat(ctx, full)
		if !sst.Ok() {
			return nil, nil, 0, syscall.Errno(sst)
		}
		attr = a
	}
	setAttrFromBackend(&out.Attr, attr)
	child := parentInode.NewInode(ctx, &gMountieNode{
		backend: backend,
	}, fs.StableAttr{
		Mode: attr.Mode,
		Ino:  attr.Ino,
	})
	return child, &gMountieFile{backend: backend, fh: handle}, 0, 0
}

// --- Mkdir ---

func (r *gMountieRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return mkdirAt(ctx, &r.Inode, r.backend, "", name, mode, out)
}

func (n *gMountieNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return mkdirAt(ctx, &n.Inode, n.backend, n.path(), name, mode, out)
}

func mkdirAt(ctx context.Context, parentInode *fs.Inode, backend FileSystemBackend, parent, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	full := childPath(parent, name)
	if st := backend.Mkdir(ctx, full, mode); !st.Ok() {
		return nil, syscall.Errno(st)
	}
	// Backend Mkdir doesn't return attrs; Stat for the EntryOut so the
	// kernel can populate its dentry.
	a, sst := backend.Stat(ctx, full)
	if !sst.Ok() {
		return nil, syscall.Errno(sst)
	}
	setAttrFromBackend(&out.Attr, a)
	child := parentInode.NewInode(ctx, &gMountieNode{
		backend: backend,
	}, fs.StableAttr{
		Mode: a.Mode,
		Ino:  a.Ino,
	})
	return child, 0
}

// --- Rmdir / Unlink ---

func (r *gMountieRoot) Rmdir(ctx context.Context, name string) syscall.Errno {
	return syscall.Errno(r.backend.Rmdir(ctx, childPath("", name)))
}

func (n *gMountieNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return syscall.Errno(n.backend.Rmdir(ctx, childPath(n.path(), name)))
}

func (r *gMountieRoot) Unlink(ctx context.Context, name string) syscall.Errno {
	return syscall.Errno(r.backend.Unlink(ctx, childPath("", name)))
}

func (n *gMountieNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return syscall.Errno(n.backend.Unlink(ctx, childPath(n.path(), name)))
}

// --- Rename ---

func (r *gMountieRoot) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return renameAt(ctx, r.backend, "", name, newParent, newName, flags)
}

func (n *gMountieNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return renameAt(ctx, n.backend, n.path(), name, newParent, newName, flags)
}

// renameAt resolves the destination parent's path from the live inode
// tree. It must be one of our own node types — Rename is always within
// the same mount, so the type switch is exhaustive in practice; an
// EINVAL guard documents the invariant for the reader.
func renameAt(ctx context.Context, backend FileSystemBackend, parent, name string, newParent fs.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	var newParentPath string
	switch np := newParent.(type) {
	case *gMountieRoot:
		newParentPath = ""
	case *gMountieNode:
		newParentPath = np.path()
	default:
		return syscall.Errno(fuse.EINVAL)
	}
	oldP := childPath(parent, name)
	newP := childPath(newParentPath, newName)
	return syscall.Errno(backend.Rename(ctx, oldP, newP))
}

// --- Statfs ---

func (r *gMountieRoot) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return statfsAt(ctx, r.backend, "", out)
}

func (n *gMountieNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	return statfsAt(ctx, n.backend, n.path(), out)
}

func statfsAt(ctx context.Context, backend FileSystemBackend, p string, out *fuse.StatfsOut) syscall.Errno {
	s, st := backend.StatFs(ctx, p)
	if !st.Ok() {
		return syscall.Errno(st)
	}
	out.Blocks = s.Blocks
	out.Bfree = s.Bfree
	out.Bavail = s.Bavail
	out.Files = s.Files
	out.Ffree = s.Ffree
	out.Bsize = s.Bsize
	out.NameLen = s.Namelen
	out.Frsize = s.Frsize
	return 0
}

// --- Access ---

func (r *gMountieRoot) Access(ctx context.Context, mask uint32) syscall.Errno {
	return syscall.Errno(r.backend.Access(ctx, "", mask))
}

func (n *gMountieNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	return syscall.Errno(n.backend.Access(ctx, n.path(), mask))
}

// --- Getxattr ---

func (r *gMountieRoot) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return getxattrAt(ctx, r.backend, "", attr, dest)
}

func (n *gMountieNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	return getxattrAt(ctx, n.backend, n.path(), attr, dest)
}

func getxattrAt(ctx context.Context, backend FileSystemBackend, p, attr string, dest []byte) (uint32, syscall.Errno) {
	data, st := backend.GetXAttr(ctx, p, attr)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	if len(data) > len(dest) {
		return uint32(len(data)), syscall.Errno(fuse.ERANGE)
	}
	return uint32(copy(dest, data)), 0
}

// --- File handle ops ---

func (f *gMountieFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, st := f.backend.Read(ctx, f.fh, off, dest)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (f *gMountieFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, st := f.backend.Write(ctx, f.fh, off, data)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	return n, 0
}

func (f *gMountieFile) Flush(ctx context.Context) syscall.Errno {
	return syscall.Errno(f.backend.Flush(ctx, f.fh))
}

func (f *gMountieFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return syscall.Errno(f.backend.Fsync(ctx, f.fh, int64(flags)))
}

func (f *gMountieFile) Release(ctx context.Context) syscall.Errno {
	return syscall.Errno(f.backend.Release(ctx, f.fh))
}

func (f *gMountieFile) Allocate(ctx context.Context, off, size uint64, mode uint32) syscall.Errno {
	return syscall.Errno(f.backend.Allocate(ctx, f.fh, off, size, mode))
}

func (f *gMountieFile) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	return syscall.Errno(f.backend.GetLk(ctx, f.fh, owner, lk, flags, out))
}

func (f *gMountieFile) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return syscall.Errno(f.backend.SetLk(ctx, f.fh, owner, lk, flags))
}

func (f *gMountieFile) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	return syscall.Errno(f.backend.SetLkw(ctx, f.fh, owner, lk, flags))
}
