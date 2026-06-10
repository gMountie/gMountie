// node.go bridges go-fuse v2 fs.NodeXXX interfaces to FileSystemBackend.
// Each adapter method is intentionally thin: translate path/types,
// delegate to the backend, and translate fuse.Status to syscall.Errno.
//
// A single node type serves both the mount root and every descendant —
// go-fuse's Inode.Path(nil) already returns "" for the root inode, so no
// dedicated root type is needed (mirrors go-fuse's own LoopbackNode). All
// nodes share a FileSystemBackend (set at construction). Paths are computed
// on demand via Inode.Path(nil) rather than cached on the struct — go-fuse
// mutates the inode tree under us (Rename's MvChild, hardlinks) and a cached
// path would go stale, sending subsequent ops to the old name on the server.
package io

import (
	"context"
	"math"
	"path"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// gMountieNode is an inode of a gMountie mount (root and descendants alike).
// Its path relative to the mount root is derived on demand from the inode
// tree's current position (see path()); never cache it. backend and rewriter
// are shared across the whole tree (copied at construction time).
type gMountieNode struct {
	fs.Inode

	backend  FileSystemBackend
	rewriter *IDRewriter
}

// NewMountieRoot constructs the root inode wrapping a FileSystemBackend.
// rewriter translates server uid/gid ↔ local uid/gid; pass nil for
// passthrough (raw_ids mounts or when WhoAmI returned no identity).
// Mount code passes the returned value to fs.Mount.
func NewMountieRoot(backend FileSystemBackend, rewriter *IDRewriter) fs.InodeEmbedder {
	return &gMountieNode{backend: backend, rewriter: rewriter}
}

// path returns the inode's path relative to the mount root, with no
// leading slash. Computed from the live inode tree so that go-fuse's
// MvChild (during Rename) is reflected immediately. The root inode's
// path is "".
func (n *gMountieNode) path() string {
	return n.Path(nil)
}

// newChild attaches a child inode sharing this node's backend/rewriter.
func (n *gMountieNode) newChild(ctx context.Context, a *Attr) *fs.Inode {
	return n.NewInode(ctx, &gMountieNode{
		backend:  n.backend,
		rewriter: n.rewriter,
	}, fs.StableAttr{
		Mode: a.Mode,
		Ino:  a.Ino,
	})
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
	_ fs.NodeLookuper       = (*gMountieNode)(nil)
	_ fs.NodeReaddirer      = (*gMountieNode)(nil)
	_ fs.NodeStatfser       = (*gMountieNode)(nil)
	_ fs.NodeGetattrer      = (*gMountieNode)(nil)
	_ fs.NodeSetattrer      = (*gMountieNode)(nil)
	_ fs.NodeOpener         = (*gMountieNode)(nil)
	_ fs.NodeCreater        = (*gMountieNode)(nil)
	_ fs.NodeMkdirer        = (*gMountieNode)(nil)
	_ fs.NodeRmdirer        = (*gMountieNode)(nil)
	_ fs.NodeUnlinker       = (*gMountieNode)(nil)
	_ fs.NodeRenamer        = (*gMountieNode)(nil)
	_ fs.NodeAccesser       = (*gMountieNode)(nil)
	_ fs.NodeGetxattrer     = (*gMountieNode)(nil)
	_ fs.NodeSetxattrer     = (*gMountieNode)(nil)
	_ fs.NodeRemovexattrer  = (*gMountieNode)(nil)
	_ fs.NodeListxattrer    = (*gMountieNode)(nil)
	_ fs.NodeCopyFileRanger = (*gMountieNode)(nil)

	_ fs.FileReader    = (*gMountieFile)(nil)
	_ fs.FileWriter    = (*gMountieFile)(nil)
	_ fs.FileFlusher   = (*gMountieFile)(nil)
	_ fs.FileFsyncer   = (*gMountieFile)(nil)
	_ fs.FileReleaser  = (*gMountieFile)(nil)
	_ fs.FileAllocater = (*gMountieFile)(nil)
	_ fs.FileGetlker   = (*gMountieFile)(nil)
	_ fs.FileSetlker   = (*gMountieFile)(nil)
	_ fs.FileSetlkwer  = (*gMountieFile)(nil)
	_ fs.FileLseeker   = (*gMountieFile)(nil)
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

func (n *gMountieNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	a, st := n.backend.Lookup(ctx, n.path(), name)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a, n.rewriter)
	return n.newChild(ctx, a), 0
}

// --- Readdir ---

func (n *gMountieNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, st := n.backend.ListDir(ctx, n.path())
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	// Only the dirent part feeds the kernel here; per-entry attrs (plus
	// listings) are consumed by the cache layer, which primes its attr
	// cache before the entries reach this adapter.
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

func (n *gMountieNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	a, st := n.backend.Stat(ctx, n.path())
	if !st.Ok() {
		return syscall.Errno(st)
	}
	setAttrFromBackend(&out.Attr, a, n.rewriter)
	return 0
}

// --- Setattr ---

// Setattr folds the whole kernel SETATTR into ONE backend.SetAttr call
// (previously a serial Truncate→Chmod→Chown→Utimens fan-out plus a trailing
// Stat — up to 5 RTTs). The reply carries the final attrs, so no trailing
// Stat is issued.
//
// Bit mapping: MODE/UID/GID/SIZE/ATIME/MTIME pass through 1:1 via go-fuse's
// Get* helpers. ATIME_NOW/MTIME_NOW are resolved HERE: the server does not
// interpret the _NOW bits, and GetATime()/GetMTime() already fold them into
// time.Now() — the wire carries plain FATTR_ATIME/MTIME with the concrete
// timestamp. A single-half chown forwards just the set bit; the server
// resolves the unset half (no client-side Stat-to-fill). rw.Outbound maps
// local→server ids before the wire; rw.Inbound is applied by
// setAttrFromBackend on the returned attrs. The fs.FileHandle argument is
// ignored, as before — the wire SetAttr is path-based.
func (n *gMountieNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p := n.path()
	var req SetAttrIn
	if sz, ok := in.GetSize(); ok {
		req.Valid |= fuse.FATTR_SIZE
		req.Size = sz
	}
	if mode, ok := in.GetMode(); ok {
		req.Valid |= fuse.FATTR_MODE
		req.Mode = mode
	}
	uid, uidOK := in.GetUID()
	gid, gidOK := in.GetGID()
	if uidOK || gidOK {
		// Outbound rewrites uid and gid independently, so rewriting a half
		// whose bit is unset is harmless — that half never reaches the wire.
		uid, gid = n.rewriter.Outbound(uid, gid)
		if uidOK {
			req.Valid |= fuse.FATTR_UID
			req.Uid = uid
		}
		if gidOK {
			req.Valid |= fuse.FATTR_GID
			req.Gid = gid
		}
	}
	if atime, ok := in.GetATime(); ok {
		req.Valid |= fuse.FATTR_ATIME
		req.Atime = &atime
	}
	if mtime, ok := in.GetMTime(); ok {
		req.Valid |= fuse.FATTR_MTIME
		req.Mtime = &mtime
	}
	a, st := n.backend.SetAttr(ctx, p, req)
	if !st.Ok() {
		return syscall.Errno(st)
	}
	// The server omits attrs only when its post-apply stat failed. Fall back
	// to Stat rather than handing the kernel a zero fuse.Attr — the kernel
	// would cache the zero (Mode=0, Size=0) for AttrTimeout (same poisoning
	// concern as Create's fallback).
	if a == nil {
		a, st = n.backend.Stat(ctx, p)
		if !st.Ok() {
			return syscall.Errno(st)
		}
	}
	setAttrFromBackend(&out.Attr, a, n.rewriter)
	return 0
}

// --- Open ---

func (n *gMountieNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	h, st := n.backend.Open(ctx, n.path(), flags)
	if !st.Ok() {
		return nil, 0, syscall.Errno(st)
	}
	return &gMountieFile{backend: n.backend, fh: h}, 0, 0
}

// --- Create ---

func (n *gMountieNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	parent := n.path()
	handle, attr, st := n.backend.Create(ctx, parent, name, flags, mode)
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
		a, sst := n.backend.Stat(ctx, full)
		if !sst.Ok() {
			return nil, nil, 0, syscall.Errno(sst)
		}
		attr = a
	}
	setAttrFromBackend(&out.Attr, attr, n.rewriter)
	return n.newChild(ctx, attr), &gMountieFile{backend: n.backend, fh: handle}, 0, 0
}

// --- Mkdir ---

func (n *gMountieNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	full := childPath(n.path(), name)
	a, st := n.backend.Mkdir(ctx, full, mode)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	// The reply carries the new directory's attrs, so no trailing Stat. The
	// server omits them only when its post-create stat failed; fall back to
	// Stat rather than handing the kernel a zero EntryOut — it would cache
	// the zero (Mode=0, Size=0) for EntryTimeout and poison subsequent stat
	// ops (same rationale as the Create/Setattr fallbacks).
	if a == nil {
		var sst fuse.Status
		a, sst = n.backend.Stat(ctx, full)
		if !sst.Ok() {
			return nil, syscall.Errno(sst)
		}
	}
	setAttrFromBackend(&out.Attr, a, n.rewriter)
	return n.newChild(ctx, a), 0
}

// --- Symlink / Readlink ---

// Symlink creates a new symbolic link `name` whose target is `target` (an
// arbitrary string — confinement enforces on resolve, not on creation).
// Returns the new inode so the kernel can populate its dentry.
func (n *gMountieNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	full := childPath(n.path(), name)
	a, st := n.backend.Symlink(ctx, target, full)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	// The reply carries the new link's attrs (S_IFLNK — the link itself, not
	// the target), so no trailing Stat. The server omits them only when its
	// post-create stat failed; fall back to Stat rather than handing the
	// kernel a zero EntryOut (kernel-cache poisoning, same as Mkdir/Create).
	if a == nil {
		var sst fuse.Status
		a, sst = n.backend.Stat(ctx, full)
		if !sst.Ok() {
			return nil, syscall.Errno(sst)
		}
	}
	setAttrFromBackend(&out.Attr, a, n.rewriter)
	return n.newChild(ctx, a), 0
}

// Readlink returns the link's target string. The kernel calls this during
// pathwalk when it encounters S_IFLNK; without it FUSE returns EOPNOTSUPP
// and every symlink under the mount is unfollowable.
func (n *gMountieNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, st := n.backend.Readlink(ctx, n.path())
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	return []byte(target), 0
}

// --- Rmdir / Unlink ---

func (n *gMountieNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return syscall.Errno(n.backend.Rmdir(ctx, childPath(n.path(), name)))
}

func (n *gMountieNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return syscall.Errno(n.backend.Unlink(ctx, childPath(n.path(), name)))
}

// --- Rename ---

// Rename resolves the destination parent's path from the live inode tree. It
// must be our own node type — Rename is always within the same mount, so the
// type assert holds in practice; an EINVAL guard documents the invariant for
// the reader.
func (n *gMountieNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, _ uint32) syscall.Errno {
	np, ok := newParent.(*gMountieNode)
	if !ok {
		return syscall.Errno(fuse.EINVAL)
	}
	oldP := childPath(n.path(), name)
	newP := childPath(np.path(), newName)
	return syscall.Errno(n.backend.Rename(ctx, oldP, newP))
}

// --- CopyFileRange ---

// CopyFileRange forwards the kernel's copy request to the server so the
// bytes never transit the client. Both handles are ours by construction
// (same bridge ⇒ same mount); a failed assert is EBADF, not EXDEV. The
// reply is capped at the 32-bit width of the FUSE_COPY_FILE_RANGE reply
// (the kernel caps the request below 4 GiB anyway).
func (n *gMountieNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, _ *fs.Inode, fhOut fs.FileHandle, offOut uint64, length uint64, flags uint64) (uint32, syscall.Errno) {
	src, ok := fhIn.(*gMountieFile)
	if !ok {
		return 0, syscall.EBADF
	}
	dst, ok := fhOut.(*gMountieFile)
	if !ok {
		return 0, syscall.EBADF
	}
	copied, st := n.backend.CopyFileRange(ctx, src.fh, offIn, dst.fh, offOut, length, flags)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	if copied > math.MaxUint32 {
		copied = math.MaxUint32
	}
	return uint32(copied), 0
}

// --- Statfs ---

func (n *gMountieNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	s, st := n.backend.StatFs(ctx, n.path())
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

func (n *gMountieNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	return syscall.Errno(n.backend.Access(ctx, n.path(), mask))
}

// --- Getxattr ---

func (n *gMountieNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	data, st := n.backend.GetXAttr(ctx, n.path(), attr)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	if len(data) > len(dest) {
		return uint32(len(data)), syscall.Errno(fuse.ERANGE)
	}
	return uint32(copy(dest, data)), 0
}

// --- Setxattr / Removexattr / Listxattr ---

func (n *gMountieNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return syscall.Errno(n.backend.SetXAttr(ctx, n.path(), attr, data, flags))
}

func (n *gMountieNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return syscall.Errno(n.backend.RemoveXAttr(ctx, n.path(), attr))
}

// Listxattr marshals names into the kernel's NUL-joined buffer format.
// go-fuse contract: if dest is too small, return ERANGE AND the needed
// size so the caller can re-issue with a bigger buffer.
func (n *gMountieNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	names, st := n.backend.ListXAttr(ctx, n.path())
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	sz := 0
	for _, name := range names {
		sz += len(name) + 1
	}
	if sz > len(dest) {
		return uint32(sz), syscall.ERANGE
	}
	off := 0
	for _, name := range names {
		off += copy(dest[off:], name)
		dest[off] = 0
		off++
	}
	return uint32(sz), 0
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

func (f *gMountieFile) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	o, st := f.backend.Lseek(ctx, f.fh, off, whence)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	return o, 0
}
