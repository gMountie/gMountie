// Package io contains the client-side filesystem implementation. backend.go
// defines FileSystemBackend, the op-shaped interface that the go-fuse node
// adapters in node.go delegate to. Sub-spec B's cache will decorate
// FileSystemBackend; today there is one impl (BackendClient in
// backend_grpc.go).
package io

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// Attr is the per-inode attribute snapshot returned by Stat/Lookup. Keeps
// FileSystemBackend decoupled from pkg/proto's wire types.
type Attr struct {
	Ino       uint64
	Size      uint64
	Blocks    uint64
	Atime     uint64
	Mtime     uint64
	Ctime     uint64
	Atimensec uint32
	Mtimensec uint32
	Ctimensec uint32
	Mode      uint32
	Nlink     uint32
	Uid       uint32
	Gid       uint32
	Rdev      uint32
	Blksize   uint32
}

// DirEntry mirrors a single directory listing entry.
type DirEntry struct {
	Ino  uint64
	Mode uint32
	Name string
}

// StatFs mirrors the per-volume statfs reply.
type StatFs struct {
	Blocks  uint64
	Bfree   uint64
	Bavail  uint64
	Files   uint64
	Ffree   uint64
	Bsize   uint32
	Namelen uint32
	Frsize  uint32
}

// FileHandle is an opaque per-open-file handle returned by Open/Create and
// passed to Read/Write/Flush/Fsync/Release. Implementations may hold
// session+fd, a readahead state, a write coalescer, and a per-handle ctx.
type FileHandle interface {
	// Path returns the path the handle was opened against. Mainly for
	// logging and the read/write retry diagnostics.
	Path() string
	// Unwrap returns the next FileHandle in a decorator chain, or the
	// receiver itself for a leaf handle. A FileSystemBackend that needs
	// to type-assert to a concrete handle (BackendClient -> *grpcFileHandle)
	// walks the Unwrap chain to reach the leaf — this lets Sub-spec B+
	// wrap handles (cache decorator etc.) without confusing the gRPC
	// backend. Leaf handles return themselves; the walk terminates when
	// cur.Unwrap() == cur.
	Unwrap() FileHandle
}

// FileSystemBackend is the seam between the go-fuse adapter (node.go) and
// the gRPC layer (BackendClient in backend_grpc.go). Sub-spec B of Phase 4
// will plug a cache decorator at this interface.
//
// Semantics mirror FUSE ops: path-keyed for metadata, FileHandle-keyed for
// I/O. Implementations must be safe for concurrent calls.
type FileSystemBackend interface {
	// Stat returns the attributes of path. Used by Getattr.
	Stat(ctx context.Context, path string) (*Attr, fuse.Status)
	// Lookup resolves a child name under parent, returning attrs + inode.
	Lookup(ctx context.Context, parent, name string) (*Attr, fuse.Status)
	// ListDir returns the entries of a directory.
	ListDir(ctx context.Context, path string) ([]DirEntry, fuse.Status)

	// Access mirrors the access(2) check.
	Access(ctx context.Context, path string, mode uint32) fuse.Status
	// StatFs returns filesystem statistics for the volume containing path.
	StatFs(ctx context.Context, path string) (*StatFs, fuse.Status)
	// GetXAttr returns the extended-attribute bytes for path/attr.
	GetXAttr(ctx context.Context, path, attr string) ([]byte, fuse.Status)

	// Open opens an existing file. flags follow the FUSE open flags.
	Open(ctx context.Context, path string, flags uint32) (FileHandle, fuse.Status)
	// Create creates a new file as a child of parent.
	Create(ctx context.Context, parent, name string, flags, mode uint32) (FileHandle, *Attr, fuse.Status)
	// Read fills dest starting at off and returns the number of bytes read.
	Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, fuse.Status)
	// Write writes data at off and returns the number of bytes written.
	Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, fuse.Status)
	// Release closes the open file referenced by fh.
	Release(ctx context.Context, fh FileHandle) fuse.Status
	// Flush is called on each close(2) of a fd that opened the file.
	Flush(ctx context.Context, fh FileHandle) fuse.Status
	// Fsync sync()s the file.
	Fsync(ctx context.Context, fh FileHandle, flags int64) fuse.Status
	// Allocate preallocates space at off..off+size for future writes
	// (fallocate(2)).
	Allocate(ctx context.Context, fh FileHandle, off, size uint64, mode uint32) fuse.Status
	// GetLk queries the lock state for a region of the file
	// (fcntl(F_GETLK)).
	GetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status
	// SetLk attempts to acquire a lock without blocking
	// (fcntl(F_SETLK)).
	SetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status
	// SetLkw attempts to acquire a lock, blocking until it can be
	// granted (fcntl(F_SETLKW)).
	SetLkw(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status

	// Mkdir creates a directory.
	Mkdir(ctx context.Context, path string, mode uint32) fuse.Status
	// Rmdir removes an empty directory.
	Rmdir(ctx context.Context, path string) fuse.Status
	// Unlink removes a non-directory.
	Unlink(ctx context.Context, path string) fuse.Status
	// Rename moves a file/directory.
	Rename(ctx context.Context, oldPath, newPath string) fuse.Status
	// Truncate changes a file's length.
	Truncate(ctx context.Context, path string, size uint64) fuse.Status
	// Chmod changes file permissions.
	Chmod(ctx context.Context, path string, mode uint32) fuse.Status
	// Chown changes ownership.
	Chown(ctx context.Context, path string, uid, gid uint32) fuse.Status
}
