// Package memfs is a thread-safe, in-memory reference implementation of
// backend.FileSystemBackend. It exists so the FileSystemBackend behavioral
// contract can be exercised against a known-correct backend (see
// pkg/client/backend/contract) and so future semantic layers (the WAL, a
// write-batcher) have a real backend to decorate in tests without a server
// or a FUSE mount.
//
// It is NOT a production filesystem: it models just enough — a tree of
// directories, regular files (byte slices) and symlinks, per-node attrs and
// xattrs — to satisfy the contract's close-to-open-consistency assertions and
// to return the correct proto.FsError codes. Locking is not modeled
// (GetLk/SetLk/SetLkw succeed without tracking ranges); StatFs returns
// plausible constants.
//
// Path convention MUST match the cache layer's joinPath/pathParent (root is
// the empty string "", no leading slash, parent + "/" + name) so that a cache
// decorator forwarding opaque path strings resolves to the same node whether
// it calls Lookup(parent, name) or Stat(joinPath(parent, name)).
package memfs

import (
	"context"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/backend"
	"go.gmountie.dev/gmountie/pkg/proto"
)

const (
	// modeDir / modeLink are the type bits memfs stamps onto created nodes.
	modeDir  = uint32(syscall.S_IFDIR)
	modeFile = uint32(syscall.S_IFREG)
	modeLink = uint32(syscall.S_IFLNK)

	defaultDirMode  = modeDir | 0o755
	defaultLinkMode = modeLink | 0o777
)

// node is a single inode in the in-memory tree. Every node is guarded by the
// owning memFS's single mutex; node fields are never touched without it held.
type node struct {
	ino  uint64
	mode uint32 // full mode incl. type bits
	uid  uint32
	gid  uint32

	atime, mtime, ctime time.Time

	// version is bumped on every mutation of this node so GetAttrIfChanged
	// can answer "changed since knownVersion?" cheaply.
	version uint64

	// data is the file contents for regular files.
	data []byte
	// target is the link target for symlinks (stored verbatim).
	target string
	// children maps child name -> node for directories.
	children map[string]*node

	// xattrs is the per-node extended-attribute store.
	xattrs map[string][]byte
}

func (n *node) isDir() bool  { return n.mode&syscall.S_IFMT == modeDir }
func (n *node) isLink() bool { return n.mode&syscall.S_IFMT == modeLink }

// nlink computes the link count: dirs report 2 + subdir count (. + .. +
// each child's ..), everything else reports 1. Good enough for the contract.
func (n *node) nlink() uint32 {
	if !n.isDir() {
		return 1
	}
	nl := uint32(2)
	for _, c := range n.children {
		if c.isDir() {
			nl++
		}
	}
	return nl
}

func (n *node) size() uint64 {
	switch {
	case n.isLink():
		return uint64(len(n.target))
	case n.isDir():
		return 4096
	default:
		return uint64(len(n.data))
	}
}

// attr snapshots the node into an backend.Attr. Caller holds the fs lock.
func (n *node) attr() *backend.Attr {
	sz := n.size()
	return &backend.Attr{
		Ino:     n.ino,
		Size:    sz,
		Blocks:  (sz + 511) / 512,
		Atime:   uint64(n.atime.Unix()),
		Mtime:   uint64(n.mtime.Unix()),
		Ctime:   uint64(n.ctime.Unix()),
		Mode:    n.mode,
		Nlink:   n.nlink(),
		Uid:     n.uid,
		Gid:     n.gid,
		Blksize: 4096,
		Version: n.version,
	}
}

func (n *node) touch() {
	now := time.Now()
	n.mtime = now
	n.ctime = now
	n.version++
}

// memHandle is the per-open handle. It is a leaf: Unwrap returns itself.
type memHandle struct {
	path string
	n    *node
}

func (h *memHandle) Path() string               { return h.path }
func (h *memHandle) Unwrap() backend.FileHandle { return h }

// memFS is the in-memory backend. All state is guarded by mu.
type memFS struct {
	mu      sync.Mutex
	root    *node
	nextIno uint64
}

// New returns a fresh in-memory FileSystemBackend with an empty root
// directory.
func New() backend.FileSystemBackend {
	fs := &memFS{nextIno: 1}
	now := time.Now()
	fs.root = &node{
		ino:      fs.nextIno,
		mode:     defaultDirMode,
		atime:    now,
		mtime:    now,
		ctime:    now,
		children: map[string]*node{},
		xattrs:   map[string][]byte{},
	}
	fs.nextIno++
	return fs
}

// alloc returns the next inode number. Caller holds mu.
func (fs *memFS) allocIno() uint64 {
	ino := fs.nextIno
	fs.nextIno++
	return ino
}

// splitPath splits a clean path (memfs/cache convention) into its components.
// The root "" / "/" yields no components.
func splitPath(p string) []string {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return nil
	}
	return strings.Split(p, "/")
}

// pathParent mirrors the cache layer's pathParent so split/join agree.
func pathParent(p string) string {
	if p == "" || p == "/" {
		return ""
	}
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return ""
	}
	return p[:idx]
}

// baseName returns the final path component.
func baseName(p string) string {
	p = strings.TrimSuffix(p, "/")
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return p
	}
	return p[idx+1:]
}

// lookup walks the tree to the node at path. Caller holds mu. Returns
// (node, FS_OK) or (nil, ENOENT/ENOTDIR).
func (fs *memFS) lookup(path string) (*node, proto.FsError) {
	cur := fs.root
	for _, part := range splitPath(path) {
		if !cur.isDir() {
			return nil, proto.FsError_FS_ENOTDIR
		}
		next, ok := cur.children[part]
		if !ok {
			return nil, proto.FsError_FS_ENOENT
		}
		cur = next
	}
	return cur, proto.FsError_FS_OK
}

// parentOf resolves the parent directory of path and returns it plus the
// base name. Caller holds mu.
func (fs *memFS) parentOf(path string) (*node, string, proto.FsError) {
	base := baseName(path)
	if base == "" {
		return nil, "", proto.FsError_FS_EINVAL
	}
	parent, st := fs.lookup(pathParent(path))
	if st != proto.FsError_FS_OK {
		return nil, "", st
	}
	if !parent.isDir() {
		return nil, "", proto.FsError_FS_ENOTDIR
	}
	return parent, base, proto.FsError_FS_OK
}

// resolveHandle walks the Unwrap chain to the leaf *memHandle. Returns
// (node, FS_OK) or (nil, EBADF) for a foreign handle.
func resolveHandle(fh backend.FileHandle) (*node, proto.FsError) {
	cur := fh
	for cur != nil {
		if mh, ok := cur.(*memHandle); ok {
			if mh.n == nil {
				return nil, proto.FsError_FS_EBADF
			}
			return mh.n, proto.FsError_FS_OK
		}
		next := cur.Unwrap()
		if next == cur {
			break
		}
		cur = next
	}
	return nil, proto.FsError_FS_EBADF
}

// --- read path ---

func (fs *memFS) Stat(_ context.Context, path string) (*backend.Attr, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	return n.attr(), proto.FsError_FS_OK
}

func (fs *memFS) GetAttrIfChanged(_ context.Context, path string, knownVersion uint64) (*backend.Attr, bool, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st == proto.FsError_FS_ENOENT {
		return nil, false, proto.FsError_FS_ENOENT
	}
	if st != proto.FsError_FS_OK {
		// Treat other walk errors like a transport failure so callers fall
		// through to a full Stat (mirrors the interface's EIO contract).
		return nil, false, proto.FsError_FS_EIO
	}
	if n.version == knownVersion {
		return nil, true, proto.FsError_FS_OK
	}
	return n.attr(), false, proto.FsError_FS_OK
}

func (fs *memFS) Lookup(_ context.Context, parent, name string) (*backend.Attr, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	pn, st := fs.lookup(parent)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	if !pn.isDir() {
		return nil, proto.FsError_FS_ENOTDIR
	}
	child, ok := pn.children[name]
	if !ok {
		return nil, proto.FsError_FS_ENOENT
	}
	return child.attr(), proto.FsError_FS_OK
}

func (fs *memFS) ListDir(_ context.Context, path string) ([]backend.DirEntryPlus, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	if !n.isDir() {
		return nil, proto.FsError_FS_ENOTDIR
	}
	out := make([]backend.DirEntryPlus, 0, len(n.children))
	for name, child := range n.children {
		out = append(out, backend.DirEntryPlus{
			DirEntry: backend.DirEntry{Ino: child.ino, Mode: child.mode, Name: name},
			Attr:     child.attr(),
		})
	}
	return out, proto.FsError_FS_OK
}

func (fs *memFS) Access(_ context.Context, path string, _ uint32) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	_, st := fs.lookup(path)
	return st
}

func (fs *memFS) StatFs(_ context.Context, path string) (*backend.StatFs, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if _, st := fs.lookup(path); st != proto.FsError_FS_OK {
		return nil, st
	}
	return &backend.StatFs{
		Blocks:  1 << 20,
		Bfree:   1 << 19,
		Bavail:  1 << 19,
		Files:   1 << 16,
		Ffree:   1 << 15,
		Bsize:   4096,
		Namelen: 255,
		Frsize:  4096,
	}, proto.FsError_FS_OK
}

// --- xattrs ---

func (fs *memFS) GetXAttr(_ context.Context, path, attr string) ([]byte, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	v, ok := n.xattrs[attr]
	if !ok {
		return nil, proto.FsError_FS_ENO_XATTR
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, proto.FsError_FS_OK
}

func (fs *memFS) SetXAttr(_ context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return st
	}
	_, exists := n.xattrs[attr]
	// XATTR_CREATE fails if it exists; XATTR_REPLACE fails if it doesn't.
	if flags&uint32(0x1) != 0 && exists { // XATTR_CREATE
		return proto.FsError_FS_EEXIST
	}
	if flags&uint32(0x2) != 0 && !exists { // XATTR_REPLACE
		return proto.FsError_FS_ENO_XATTR
	}
	buf := make([]byte, len(data))
	copy(buf, data)
	n.xattrs[attr] = buf
	n.ctime = time.Now()
	n.version++
	return proto.FsError_FS_OK
}

func (fs *memFS) RemoveXAttr(_ context.Context, path, attr string) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return st
	}
	if _, ok := n.xattrs[attr]; !ok {
		return proto.FsError_FS_ENO_XATTR
	}
	delete(n.xattrs, attr)
	n.ctime = time.Now()
	n.version++
	return proto.FsError_FS_OK
}

func (fs *memFS) ListXAttr(_ context.Context, path string) ([]string, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	names := make([]string, 0, len(n.xattrs))
	for k := range n.xattrs {
		names = append(names, k)
	}
	return names, proto.FsError_FS_OK
}

// --- open / create / file ops ---

func (fs *memFS) Open(_ context.Context, path string, _ uint32) (backend.FileHandle, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	if n.isDir() {
		return nil, proto.FsError_FS_EISDIR
	}
	return &memHandle{path: path, n: n}, proto.FsError_FS_OK
}

func (fs *memFS) Create(_ context.Context, parent, name string, flags, mode uint32) (backend.FileHandle, *backend.Attr, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	pn, st := fs.lookup(parent)
	if st != proto.FsError_FS_OK {
		return nil, nil, st
	}
	if !pn.isDir() {
		return nil, nil, proto.FsError_FS_ENOTDIR
	}
	if existing, ok := pn.children[name]; ok {
		if flags&uint32(syscall.O_EXCL) != 0 {
			return nil, nil, proto.FsError_FS_EEXIST
		}
		// Re-open the existing file (O_CREAT without O_EXCL).
		if existing.isDir() {
			return nil, nil, proto.FsError_FS_EISDIR
		}
		full := joinPath(parent, name)
		return &memHandle{path: full, n: existing}, existing.attr(), proto.FsError_FS_OK
	}
	now := time.Now()
	fileMode := modeFile | (mode & 0o7777)
	child := &node{
		ino:    fs.allocIno(),
		mode:   fileMode,
		atime:  now,
		mtime:  now,
		ctime:  now,
		xattrs: map[string][]byte{},
	}
	pn.children[name] = child
	pn.touch()
	full := joinPath(parent, name)
	return &memHandle{path: full, n: child}, child.attr(), proto.FsError_FS_OK
}

func (fs *memFS) Read(_ context.Context, fh backend.FileHandle, off int64, dest []byte) (int, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := resolveHandle(fh)
	if st != proto.FsError_FS_OK {
		return 0, st
	}
	if off < 0 || off >= int64(len(n.data)) {
		return 0, proto.FsError_FS_OK
	}
	num := copy(dest, n.data[off:])
	return num, proto.FsError_FS_OK
}

func (fs *memFS) Write(_ context.Context, fh backend.FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := resolveHandle(fh)
	if st != proto.FsError_FS_OK {
		return 0, st
	}
	if off < 0 {
		return 0, proto.FsError_FS_EINVAL
	}
	end := off + int64(len(data))
	if end > int64(len(n.data)) {
		grown := make([]byte, end)
		copy(grown, n.data)
		n.data = grown
	}
	copy(n.data[off:end], data)
	n.touch()
	return uint32(len(data)), proto.FsError_FS_OK
}

func (fs *memFS) Release(_ context.Context, fh backend.FileHandle) proto.FsError {
	_, st := resolveHandle(fh)
	return st
}

func (fs *memFS) Flush(_ context.Context, fh backend.FileHandle) proto.FsError {
	_, st := resolveHandle(fh)
	return st
}

func (fs *memFS) Fsync(_ context.Context, fh backend.FileHandle, _ int64) proto.FsError {
	_, st := resolveHandle(fh)
	return st
}

func (fs *memFS) Allocate(_ context.Context, fh backend.FileHandle, off, size uint64, _ uint32) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := resolveHandle(fh)
	if st != proto.FsError_FS_OK {
		return st
	}
	end := int64(off + size)
	if end > int64(len(n.data)) {
		grown := make([]byte, end)
		copy(grown, n.data)
		n.data = grown
		n.touch()
	}
	return proto.FsError_FS_OK
}

func (fs *memFS) GetLk(_ context.Context, fh backend.FileHandle, _ uint64, _ *backend.FileLock, _ uint32, _ *backend.FileLock) proto.FsError {
	_, st := resolveHandle(fh)
	return st
}

func (fs *memFS) SetLk(_ context.Context, fh backend.FileHandle, _ uint64, _ *backend.FileLock, _ uint32) proto.FsError {
	_, st := resolveHandle(fh)
	return st
}

func (fs *memFS) SetLkw(_ context.Context, fh backend.FileHandle, _ uint64, _ *backend.FileLock, _ uint32) proto.FsError {
	_, st := resolveHandle(fh)
	return st
}

func (fs *memFS) CopyFileRange(_ context.Context, fhIn backend.FileHandle, offIn uint64, fhOut backend.FileHandle, offOut uint64, length, _ uint64) (uint64, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	src, st := resolveHandle(fhIn)
	if st != proto.FsError_FS_OK {
		return 0, st
	}
	dst, st := resolveHandle(fhOut)
	if st != proto.FsError_FS_OK {
		return 0, st
	}
	if int64(offIn) >= int64(len(src.data)) {
		return 0, proto.FsError_FS_OK // source EOF: short count of 0
	}
	avail := uint64(len(src.data)) - offIn
	if length > avail {
		length = avail
	}
	end := int64(offOut + length)
	if end > int64(len(dst.data)) {
		grown := make([]byte, end)
		copy(grown, dst.data)
		dst.data = grown
	}
	copy(dst.data[offOut:end], src.data[offIn:offIn+length])
	dst.touch()
	return length, proto.FsError_FS_OK
}

func (fs *memFS) Lseek(_ context.Context, fh backend.FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := resolveHandle(fh)
	if st != proto.FsError_FS_OK {
		return 0, st
	}
	// SEEK_SET=0, SEEK_CUR=1, SEEK_END=2 (POSIX whence values).
	switch whence {
	case 0: // SEEK_SET
		return offset, proto.FsError_FS_OK
	case 2: // SEEK_END
		return uint64(len(n.data)) + offset, proto.FsError_FS_OK
	case 1: // SEEK_CUR — memfs has no per-handle cursor; treat as from start.
		return offset, proto.FsError_FS_OK
	default:
		return 0, proto.FsError_FS_EINVAL
	}
}

// --- path-level mutations ---

func (fs *memFS) Mkdir(_ context.Context, path string, mode uint32) (*backend.Attr, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	parent, base, st := fs.parentOf(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	if _, ok := parent.children[base]; ok {
		return nil, proto.FsError_FS_EEXIST
	}
	now := time.Now()
	child := &node{
		ino:      fs.allocIno(),
		mode:     modeDir | (mode & 0o7777),
		atime:    now,
		mtime:    now,
		ctime:    now,
		children: map[string]*node{},
		xattrs:   map[string][]byte{},
	}
	parent.children[base] = child
	parent.touch()
	return child.attr(), proto.FsError_FS_OK
}

func (fs *memFS) Rmdir(_ context.Context, path string) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	parent, base, st := fs.parentOf(path)
	if st != proto.FsError_FS_OK {
		return st
	}
	child, ok := parent.children[base]
	if !ok {
		return proto.FsError_FS_ENOENT
	}
	if !child.isDir() {
		return proto.FsError_FS_ENOTDIR
	}
	if len(child.children) > 0 {
		return proto.FsError_FS_ENOTEMPTY
	}
	delete(parent.children, base)
	parent.touch()
	return proto.FsError_FS_OK
}

func (fs *memFS) Unlink(_ context.Context, path string) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	parent, base, st := fs.parentOf(path)
	if st != proto.FsError_FS_OK {
		return st
	}
	child, ok := parent.children[base]
	if !ok {
		return proto.FsError_FS_ENOENT
	}
	if child.isDir() {
		return proto.FsError_FS_EISDIR
	}
	delete(parent.children, base)
	parent.touch()
	return proto.FsError_FS_OK
}

func (fs *memFS) Rename(_ context.Context, oldPath, newPath string) proto.FsError {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	oldParent, oldBase, st := fs.parentOf(oldPath)
	if st != proto.FsError_FS_OK {
		return st
	}
	moving, ok := oldParent.children[oldBase]
	if !ok {
		return proto.FsError_FS_ENOENT
	}
	newParent, newBase, st := fs.parentOf(newPath)
	if st != proto.FsError_FS_OK {
		return st
	}
	// If the destination exists it is replaced (a non-empty dir over a dir
	// would be ENOTEMPTY, but the contract only renames a plain file).
	if existing, ok := newParent.children[newBase]; ok {
		if existing.isDir() && len(existing.children) > 0 {
			return proto.FsError_FS_ENOTEMPTY
		}
	}
	delete(oldParent.children, oldBase)
	newParent.children[newBase] = moving
	moving.ctime = time.Now()
	moving.version++
	oldParent.touch()
	newParent.touch()
	return proto.FsError_FS_OK
}

func (fs *memFS) Readlink(_ context.Context, path string) (string, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return "", st
	}
	if !n.isLink() {
		return "", proto.FsError_FS_EINVAL
	}
	return n.target, proto.FsError_FS_OK
}

func (fs *memFS) Symlink(_ context.Context, target, linkPath string) (*backend.Attr, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	parent, base, st := fs.parentOf(linkPath)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	if _, ok := parent.children[base]; ok {
		return nil, proto.FsError_FS_EEXIST
	}
	now := time.Now()
	child := &node{
		ino:    fs.allocIno(),
		mode:   defaultLinkMode,
		target: target,
		atime:  now,
		mtime:  now,
		ctime:  now,
		xattrs: map[string][]byte{},
	}
	parent.children[base] = child
	parent.touch()
	return child.attr(), proto.FsError_FS_OK
}

func (fs *memFS) SetAttr(_ context.Context, path string, in backend.SetAttrIn) (*backend.Attr, proto.FsError) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	n, st := fs.lookup(path)
	if st != proto.FsError_FS_OK {
		return nil, st
	}
	// Apply in the documented server order: size -> mode -> owner -> times.
	if in.Valid&backend.FATTR_SIZE != 0 {
		if n.isDir() {
			return nil, proto.FsError_FS_EISDIR
		}
		sz := int64(in.Size)
		if sz < int64(len(n.data)) {
			n.data = n.data[:sz]
		} else if sz > int64(len(n.data)) {
			grown := make([]byte, sz)
			copy(grown, n.data)
			n.data = grown
		}
	}
	if in.Valid&backend.FATTR_MODE != 0 {
		n.mode = (n.mode & syscall.S_IFMT) | (in.Mode & 0o7777)
	}
	if in.Valid&backend.FATTR_UID != 0 {
		n.uid = in.Uid
	}
	if in.Valid&backend.FATTR_GID != 0 {
		n.gid = in.Gid
	}
	if in.Valid&backend.FATTR_ATIME != 0 && in.Atime != nil {
		n.atime = *in.Atime
	}
	if in.Valid&backend.FATTR_MTIME != 0 && in.Mtime != nil {
		n.mtime = *in.Mtime
	}
	n.ctime = time.Now()
	n.version++
	return n.attr(), proto.FsError_FS_OK
}

func (fs *memFS) Close() error { return nil }

// joinPath mirrors the cache layer's joinPath byte-for-byte (root "" passes
// through, otherwise parent + "/" + name) so a memHandle's reported Path()
// matches the path a cache decorator would derive for the same (parent, name).
func joinPath(parent, name string) string {
	if parent == "" {
		return name
	}
	return parent + "/" + name
}
