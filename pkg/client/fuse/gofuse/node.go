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
package gofuse

import (
	"context"
	"math"
	"path"
	"strings"
	"syscall"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// sqliteShmSuffix is the SQLite WAL shared-memory sidecar suffix. SQLite
// MAP_SHARED-mmaps this file to coordinate WAL access; over a FUSE mount that
// shared writable mapping can't be backed, so the page fault dies with SIGBUS
// (and the leftover -shm/-wal pin the DB in WAL mode, so every later open
// SIGBUSes too — bricking it). Opening it direct-IO makes the kernel refuse
// the mmap with a clean error, so SQLite returns SQLITE_IOERR instead of
// crashing. See issue #111.
const sqliteShmSuffix = "-shm"

// gMountieNode is an inode of a gMountie mount (root and descendants alike).
// Its path relative to the mount root is derived on demand from the inode
// tree's current position (see path()); never cache it. backend is shared
// across the whole tree (copied at construction time). UID/GID rewriting is
// no longer done here — the identity backend layer (pkg/client/backend/identity),
// composed outermost, rewrites server↔local ids so this adapter is pure
// type-translation.
type gMountieNode struct {
	fs.Inode

	backend backend.FileSystemBackend
	// directIOAlways forces FOPEN_DIRECT_IO on every Open/Create (the
	// fuse.direct_io escape hatch for mmap-heavy workloads). Independently,
	// SQLite -shm sidecars always open direct-IO; see wantDirectIO.
	directIOAlways bool
}

// NewMountieRoot constructs the root inode wrapping a FileSystemBackend.
// UID/GID rewriting is handled by the identity backend layer (composed
// outermost), not here — the backend passed in already yields local display
// ids on the way up and expects local ids on SetAttr on the way down.
// directIOAlways forces direct-IO on every handle (fuse.direct_io); even when
// false, SQLite -shm sidecars still open direct-IO (see wantDirectIO).
// Mount code passes the returned value to fs.Mount.
func NewMountieRoot(backend backend.FileSystemBackend, directIOAlways bool) fs.InodeEmbedder {
	return &gMountieNode{backend: backend, directIOAlways: directIOAlways}
}

// path returns the inode's path relative to the mount root, with no
// leading slash. Computed from the live inode tree so that go-fuse's
// MvChild (during Rename) is reflected immediately. The root inode's
// path is "".
func (n *gMountieNode) path() string {
	return n.Path(nil)
}

// newChild attaches a child inode sharing this node's backend.
func (n *gMountieNode) newChild(ctx context.Context, a *backend.Attr) *fs.Inode {
	return n.NewInode(ctx, &gMountieNode{
		backend:        n.backend,
		directIOAlways: n.directIOAlways,
	}, fs.StableAttr{
		Mode: a.Mode,
		Ino:  a.Ino,
	})
}

// gMountieFile is the open-file adapter satisfying fs.FileReader,
// fs.FileWriter, fs.FileFlusher, fs.FileFsyncer, fs.FileReleaser.
type gMountieFile struct {
	backend backend.FileSystemBackend
	fh      backend.FileHandle
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

// setAttrFromBackend populates a fuse.Attr from a backend Attr. It is a plain
// field copy — the backend (with the identity layer composed outermost) has
// already rewritten Uid/Gid to local display ids, so no rewrite happens here.
// Used by Getattr/Lookup/Create/Mkdir/Setattr/Symlink handlers.
func setAttrFromBackend(dst *fuse.Attr, a *backend.Attr) {
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
	if st != proto.FsError_FS_OK {
		return nil, fserr.ToErrno(st)
	}
	setAttrFromBackend(&out.Attr, a)
	return n.newChild(ctx, a), 0
}

// --- Readdir ---

func (n *gMountieNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	entries, st := n.backend.ListDir(ctx, n.path())
	if st != proto.FsError_FS_OK {
		return nil, fserr.ToErrno(st)
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
	if st != proto.FsError_FS_OK {
		return fserr.ToErrno(st)
	}
	setAttrFromBackend(&out.Attr, a)
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
// resolves the unset half (no client-side Stat-to-fill). UID/GID are forwarded
// as LOCAL ids; the identity backend layer (composed outermost) maps
// local→server on the request and server→local on the reply, so this adapter
// is namespace-agnostic. The fs.FileHandle argument is ignored, as before —
// the wire SetAttr is path-based.
func (n *gMountieNode) Setattr(ctx context.Context, _ fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	p := n.path()
	var req backend.SetAttrIn
	if sz, ok := in.GetSize(); ok {
		req.Valid |= backend.FATTR_SIZE
		req.Size = sz
	}
	if mode, ok := in.GetMode(); ok {
		req.Valid |= backend.FATTR_MODE
		req.Mode = mode
	}
	// UID/GID are passed through as LOCAL ids with their valid bits set; the
	// identity backend layer (composed outermost) rewrites local→server before
	// the request reaches the wire (see pkg/client/backend/identity), so this adapter
	// no longer touches ids.
	if uid, ok := in.GetUID(); ok {
		req.Valid |= backend.FATTR_UID
		req.Uid = uid
	}
	if gid, ok := in.GetGID(); ok {
		req.Valid |= backend.FATTR_GID
		req.Gid = gid
	}
	if atime, ok := in.GetATime(); ok {
		req.Valid |= backend.FATTR_ATIME
		req.Atime = &atime
	}
	if mtime, ok := in.GetMTime(); ok {
		req.Valid |= backend.FATTR_MTIME
		req.Mtime = &mtime
	}
	a, st := n.backend.SetAttr(ctx, p, req)
	if st != proto.FsError_FS_OK {
		return fserr.ToErrno(st)
	}
	// The server omits attrs only when its post-apply stat failed. Fall back
	// to Stat rather than handing the kernel a zero fuse.Attr — the kernel
	// would cache the zero (Mode=0, Size=0) for AttrTimeout (same poisoning
	// concern as Create's fallback).
	if a == nil {
		a, st = n.backend.Stat(ctx, p)
		if st != proto.FsError_FS_OK {
			return fserr.ToErrno(st)
		}
	}
	setAttrFromBackend(&out.Attr, a)
	return 0
}

// --- Open ---

// wantDirectIO reports whether a handle for the given path should be opened
// with FOPEN_DIRECT_IO. Direct-IO bypasses the kernel page cache for the
// handle and — the reason it matters here — makes the kernel refuse a shared
// writable mmap with a clean error instead of a SIGBUS. It is returned for
// every file when fuse.direct_io is set, and always for SQLite -shm sidecars
// (see sqliteShmSuffix / issue #111).
func (n *gMountieNode) wantDirectIO(relPath string) bool {
	return n.directIOAlways || strings.HasSuffix(relPath, sqliteShmSuffix)
}

func (n *gMountieNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	p := n.path()
	h, st := n.backend.Open(ctx, p, flags)
	if st != proto.FsError_FS_OK {
		return nil, 0, fserr.ToErrno(st)
	}
	var fuseFlags uint32
	if n.wantDirectIO(p) {
		fuseFlags = fuse.FOPEN_DIRECT_IO
	}
	return &gMountieFile{backend: n.backend, fh: h}, fuseFlags, 0
}

// --- Create ---

func (n *gMountieNode) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	parent := n.path()
	handle, attr, st := n.backend.Create(ctx, parent, name, flags, mode)
	if st != proto.FsError_FS_OK {
		return nil, nil, 0, fserr.ToErrno(st)
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
		if sst != proto.FsError_FS_OK {
			return nil, nil, 0, fserr.ToErrno(sst)
		}
		attr = a
	}
	setAttrFromBackend(&out.Attr, attr)
	var fuseFlags uint32
	if n.wantDirectIO(full) {
		fuseFlags = fuse.FOPEN_DIRECT_IO
	}
	return n.newChild(ctx, attr), &gMountieFile{backend: n.backend, fh: handle}, fuseFlags, 0
}

// --- Mkdir ---

func (n *gMountieNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	full := childPath(n.path(), name)
	a, st := n.backend.Mkdir(ctx, full, mode)
	if st != proto.FsError_FS_OK {
		return nil, fserr.ToErrno(st)
	}
	// The reply carries the new directory's attrs, so no trailing Stat. The
	// server omits them only when its post-create stat failed; fall back to
	// Stat rather than handing the kernel a zero EntryOut — it would cache
	// the zero (Mode=0, Size=0) for EntryTimeout and poison subsequent stat
	// ops (same rationale as the Create/Setattr fallbacks).
	if a == nil {
		var sst proto.FsError
		a, sst = n.backend.Stat(ctx, full)
		if sst != proto.FsError_FS_OK {
			return nil, fserr.ToErrno(sst)
		}
	}
	setAttrFromBackend(&out.Attr, a)
	return n.newChild(ctx, a), 0
}

// --- Symlink / Readlink ---

// Symlink creates a new symbolic link `name` whose target is `target` (an
// arbitrary string — confinement enforces on resolve, not on creation).
// Returns the new inode so the kernel can populate its dentry.
func (n *gMountieNode) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	full := childPath(n.path(), name)
	a, st := n.backend.Symlink(ctx, target, full)
	if st != proto.FsError_FS_OK {
		return nil, fserr.ToErrno(st)
	}
	// The reply carries the new link's attrs (S_IFLNK — the link itself, not
	// the target), so no trailing Stat. The server omits them only when its
	// post-create stat failed; fall back to Stat rather than handing the
	// kernel a zero EntryOut (kernel-cache poisoning, same as Mkdir/Create).
	if a == nil {
		var sst proto.FsError
		a, sst = n.backend.Stat(ctx, full)
		if sst != proto.FsError_FS_OK {
			return nil, fserr.ToErrno(sst)
		}
	}
	setAttrFromBackend(&out.Attr, a)
	return n.newChild(ctx, a), 0
}

// Readlink returns the link's target string. The kernel calls this during
// pathwalk when it encounters S_IFLNK; without it FUSE returns EOPNOTSUPP
// and every symlink under the mount is unfollowable.
func (n *gMountieNode) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	target, st := n.backend.Readlink(ctx, n.path())
	if st != proto.FsError_FS_OK {
		return nil, fserr.ToErrno(st)
	}
	return []byte(target), 0
}

// --- Rmdir / Unlink ---

func (n *gMountieNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	return fserr.ToErrno(n.backend.Rmdir(ctx, childPath(n.path(), name)))
}

func (n *gMountieNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return fserr.ToErrno(n.backend.Unlink(ctx, childPath(n.path(), name)))
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
	return fserr.ToErrno(n.backend.Rename(ctx, oldP, newP))
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
	if st != proto.FsError_FS_OK {
		return 0, fserr.ToErrno(st)
	}
	if copied > math.MaxUint32 {
		copied = math.MaxUint32
	}
	return uint32(copied), 0
}

// --- Statfs ---

func (n *gMountieNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	s, st := n.backend.StatFs(ctx, n.path())
	if st != proto.FsError_FS_OK {
		return fserr.ToErrno(st)
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
	return fserr.ToErrno(n.backend.Access(ctx, n.path(), mask))
}

// --- Getxattr ---

func (n *gMountieNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	data, st := n.backend.GetXAttr(ctx, n.path(), xattrNameToBackend(attr))
	if st != proto.FsError_FS_OK {
		return 0, fserr.ToErrno(st)
	}
	if len(data) > len(dest) {
		return uint32(len(data)), syscall.Errno(fuse.ERANGE)
	}
	return uint32(copy(dest, data)), 0
}

// --- Setxattr / Removexattr / Listxattr ---

func (n *gMountieNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return fserr.ToErrno(n.backend.SetXAttr(ctx, n.path(), xattrNameToBackend(attr), data, xattrFlagsToBackend(flags)))
}

func (n *gMountieNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return fserr.ToErrno(n.backend.RemoveXAttr(ctx, n.path(), xattrNameToBackend(attr)))
}

// Listxattr marshals names into the kernel's NUL-joined buffer format.
// go-fuse contract: if dest is too small, return ERANGE AND the needed
// size so the caller can re-issue with a bigger buffer.
func (n *gMountieNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	backendNames, st := n.backend.ListXAttr(ctx, n.path())
	if st != proto.FsError_FS_OK {
		return 0, fserr.ToErrno(st)
	}
	// Present backend names to the kernel under their original namespace
	// (user.com.apple.* -> com.apple.*); a no-op off the macFUSE path. Build a
	// fresh slice rather than mutating in place: the backend (cache layer) may
	// retain the returned slice, and rewriting it would poison the cache.
	names := make([]string, len(backendNames))
	for i, name := range backendNames {
		names[i] = xattrNameFromBackend(name)
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
	if st != proto.FsError_FS_OK {
		return nil, fserr.ToErrno(st)
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (f *gMountieFile) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, st := f.backend.Write(ctx, f.fh, off, data)
	if st != proto.FsError_FS_OK {
		return 0, fserr.ToErrno(st)
	}
	return n, 0
}

func (f *gMountieFile) Flush(ctx context.Context) syscall.Errno {
	return fserr.ToErrno(f.backend.Flush(ctx, f.fh))
}

func (f *gMountieFile) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return fserr.ToErrno(f.backend.Fsync(ctx, f.fh, int64(flags)))
}

func (f *gMountieFile) Release(ctx context.Context) syscall.Errno {
	return fserr.ToErrno(f.backend.Release(ctx, f.fh))
}

func (f *gMountieFile) Allocate(ctx context.Context, off, size uint64, mode uint32) syscall.Errno {
	return fserr.ToErrno(f.backend.Allocate(ctx, f.fh, off, size, mode))
}

func (f *gMountieFile) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	in := backend.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid}
	var res backend.FileLock
	st := f.backend.GetLk(ctx, f.fh, owner, &in, flags, &res)
	*out = fuse.FileLock{Start: res.Start, End: res.End, Typ: res.Typ, Pid: res.Pid}
	return fserr.ToErrno(st)
}

func (f *gMountieFile) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	in := backend.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid}
	return fserr.ToErrno(f.backend.SetLk(ctx, f.fh, owner, &in, flags))
}

func (f *gMountieFile) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	in := backend.FileLock{Start: lk.Start, End: lk.End, Typ: lk.Typ, Pid: lk.Pid}
	return fserr.ToErrno(f.backend.SetLkw(ctx, f.fh, owner, &in, flags))
}

func (f *gMountieFile) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	o, st := f.backend.Lseek(ctx, f.fh, off, whence)
	if st != proto.FsError_FS_OK {
		return 0, fserr.ToErrno(st)
	}
	return o, 0
}
