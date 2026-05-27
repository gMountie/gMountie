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
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gMountieRoot is the root inode of a gMountie mount. Holds the
// FileSystemBackend that every descendant inode delegates to.
type gMountieRoot struct {
	fs.Inode

	backend  FileSystemBackend
	rewriter *IDRewriter
}

// NewMountieRoot constructs the root inode wrapping a FileSystemBackend.
// rewriter translates server uid/gid ↔ local uid/gid; pass nil for
// passthrough (raw_ids mounts or when WhoAmI returned no identity).
// Mount code passes the returned value to fs.Mount.
func NewMountieRoot(backend FileSystemBackend, rewriter *IDRewriter) fs.InodeEmbedder {
	return &gMountieRoot{backend: backend, rewriter: rewriter}
}

// gMountieNode is a non-root inode. Its path relative to the mount root
// is derived on demand from the inode tree's current position (see
// path()); never cache it. backend and rewriter are shared with the root
// (copied at construction time, matching the backend propagation pattern).
type gMountieNode struct {
	fs.Inode

	backend  FileSystemBackend
	rewriter *IDRewriter
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

// setAttrFromBackend populates a fuse.Attr from a backend Attr and applies
// the mount's IDRewriter so the caller sees local display uid/gid rather than
// raw server ids. rw may be nil (identity transform). Used by
// Getattr/Lookup/Create/Mkdir/Setattr handlers.
func setAttrFromBackend(dst *fuse.Attr, a *Attr, rw *IDRewriter) {
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
	dst.Uid = a.Uid
	dst.Gid = a.Gid
	dst.Rdev = a.Rdev
	dst.Blksize = a.Blksize
	dst.Uid, dst.Gid = rw.Inbound(dst.Uid, dst.Gid)
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
	return lookupAt(ctx, &r.Inode, r.backend, r.rewriter, "", name, out)
}

func (n *gMountieNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return lookupAt(ctx, &n.Inode, n.backend, n.rewriter, n.path(), name, out)
}

func lookupAt(ctx context.Context, parentInode *fs.Inode, backend FileSystemBackend, rw *IDRewriter, parent, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	a, st := backend.Lookup(ctx, parent, name)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a, rw)
	child := parentInode.NewInode(ctx, &gMountieNode{
		backend:  backend,
		rewriter: rw,
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
	return getattrAt(ctx, r.backend, r.rewriter, "", out)
}

func (n *gMountieNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	return getattrAt(ctx, n.backend, n.rewriter, n.path(), out)
}

func getattrAt(ctx context.Context, backend FileSystemBackend, rw *IDRewriter, p string, out *fuse.AttrOut) syscall.Errno {
	a, st := backend.Stat(ctx, p)
	if !st.Ok() {
		return syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a, rw)
	return 0
}

// --- Setattr ---

func (r *gMountieRoot) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return setattrAt(ctx, r.backend, r.rewriter, "", in, out)
}

func (n *gMountieNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	return setattrAt(ctx, n.backend, n.rewriter, n.path(), in, out)
}

// setattrAt dispatches on SetAttrIn.Valid flags: size -> Truncate, mode ->
// Chmod, uid/gid -> Chown, atime/mtime -> Utimens. For Chown with only one of
// uid/gid set, we Stat first to read the unset side so we don't overwrite it.
// in.GetATime()/GetMTime() return the resolved concrete time (UTIME_NOW is
// already resolved to time.Now() by go-fuse); a false ok means the bit was
// unset (UTIME_OMIT), so that timestamp is passed as nil and left unchanged.
// rw.Outbound is applied to the local uid/gid before calling Chown so the
// server receives the server-namespace ids; rw.Inbound is applied by
// setAttrFromBackend on the final Stat result.
func setattrAt(ctx context.Context, backend FileSystemBackend, rw *IDRewriter, p string, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
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
		uid, gid = rw.Outbound(uid, gid)
		if st := backend.Chown(ctx, p, uid, gid); !st.Ok() {
			return syscall.Errno(st)
		}
	}
	atime, aok := in.GetATime()
	mtime, mok := in.GetMTime()
	if aok || mok {
		var ap, mp *time.Time
		if aok {
			ap = &atime
		}
		if mok {
			mp = &mtime
		}
		if st := backend.Utimens(ctx, p, ap, mp); !st.Ok() {
			return syscall.Errno(st)
		}
	}
	a, st := backend.Stat(ctx, p)
	if !st.Ok() {
		return syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a, rw)
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
	return createAt(ctx, &r.Inode, r.backend, r.rewriter, "", name, flags, mode, out)
}

func (n *gMountieNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	return createAt(ctx, &n.Inode, n.backend, n.rewriter, n.path(), name, flags, mode, out)
}

func createAt(ctx context.Context, parentInode *fs.Inode, backend FileSystemBackend, rw *IDRewriter, parent, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	handle, attr, st := backend.Create(ctx, parent, name, flags, mode)
	if !st.Ok() {
		return nil, nil, 0, syscall.Errno(st)
	}
	full := childPath(parent, name)
	// When the server populates CreateReply.Attributes the backend maps it
	// to attr directly, saving a round-trip. Fall back to Stat for older
	// servers that omit the field. If Stat fails, surface the error rather
	// than returning a zero EntryOut — the kernel would cache the zero
	// (Mode=0, Size=0) for EntryTimeout and poison subsequent stat ops.
	// The server-side Create already succeeded; the leaked fd is bounded
	// and cleaned up by Release-on-close.
	if attr == nil {
		a, sst := backend.Stat(ctx, full)
		if !sst.Ok() {
			return nil, nil, 0, syscall.Errno(sst)
		}
		attr = a
	}
	setAttrFromBackend(&out.Attr, attr, rw)
	child := parentInode.NewInode(ctx, &gMountieNode{
		backend:  backend,
		rewriter: rw,
	}, fs.StableAttr{
		Mode: attr.Mode,
		Ino:  attr.Ino,
	})
	return child, &gMountieFile{backend: backend, fh: handle}, 0, 0
}

// --- Mkdir ---

func (r *gMountieRoot) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return mkdirAt(ctx, &r.Inode, r.backend, r.rewriter, "", name, mode, out)
}

func (n *gMountieNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return mkdirAt(ctx, &n.Inode, n.backend, n.rewriter, n.path(), name, mode, out)
}

func mkdirAt(ctx context.Context, parentInode *fs.Inode, backend FileSystemBackend, rw *IDRewriter, parent, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
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
	setAttrFromBackend(&out.Attr, a, rw)
	child := parentInode.NewInode(ctx, &gMountieNode{
		backend:  backend,
		rewriter: rw,
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
