// Package io contains the client-side filesystem implementation. backend.go
// defines FileSystemBackend, the op-shaped interface that the go-fuse node
// adapters in node.go delegate to. Sub-spec B's cache will decorate
// FileSystemBackend; today there is one impl (BackendClient in
// backend_grpc.go).
package io

import (
	"context"
	"time"

	"go.gmountie.dev/gmountie/pkg/proto"

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
	// Version is the server-side version stamp (VersionFromAttr) used for
	// lightweight revalidation via GetAttrIfChanged. Zero from servers that
	// predate Sub-spec D.
	Version uint64
}

// SetAttrIn mirrors the FUSE SETATTR valid-bitmask (FATTR_* MODE/UID/GID/
// SIZE/ATIME/MTIME) so one kernel SETATTR maps to one RPC. Only the value
// fields whose bit is set in Valid are meaningful; the rest are ignored by
// the server. ATIME_NOW/MTIME_NOW never appear here: the node adapter
// resolves "now" to a concrete timestamp before building a SetAttrIn (the
// server does not interpret the _NOW bits).
type SetAttrIn struct {
	Valid uint32
	Mode  uint32
	Uid   uint32
	Gid   uint32
	Size  uint64
	Atime *time.Time
	Mtime *time.Time
}

// DirEntry mirrors a single directory listing entry.
type DirEntry struct {
	Ino  uint64
	Mode uint32
	Name string
}

// DirEntryPlus pairs a directory entry with its attrs when the backend was
// asked for a plus listing (Attr nil otherwise, or when the per-entry stat
// failed server-side). When WithXattrListings is enabled XattrListed is true
// and XattrNames carries the entry's xattr names (empty slice == no xattrs).
type DirEntryPlus struct {
	DirEntry
	Attr        *Attr
	XattrNames  []string // entry's xattr names when the server listed them
	XattrListed bool     // true == server ran listxattr for this entry
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

// FileSystemBackend is the client's filesystem operation surface and the
// decorator seam: layers wrap an inner FileSystemBackend.
//
// Behavioral contract (all layers must honor):
//   - Status: every op returns proto.FsError; FS_OK (0) means success. Layers
//     propagate the inner status unchanged unless they deliberately handle it.
//   - Retry ownership: ONLY the transport leaf retries (see retryOp). No other
//     layer re-attempts a failed op; layers above propagate errors upward.
//   - Idempotency: mutating ops carry a request_id at the transport so the
//     server dedups a retried attempt. Layers must not duplicate a mutating op.
//   - Ordering/durability: writes may be buffered and acked optimistically;
//     durability is established on Flush/Fsync/Release. A layer that defers a
//     write must drain at these boundaries (today only the transport does).
//   - Handles: Open/Create return a FileHandle; a layer that wraps handles must
//     implement FileHandle.Unwrap() returning its inner so resolveHandle reaches
//     the transport leaf.
//   - Invalidation flows UP: the transport owns the Subscribe stream; events
//     propagate outward (transport -> ... -> cache -> node).
//   - Observer vs semantic: observer layers (metrics/trace/audit) may embed
//     PassthroughBackend; semantic layers (cache, write-batcher, WAL) must
//     implement every method explicitly.
type FileSystemBackend interface {
	// Stat returns the attributes of path. Used by Getattr.
	Stat(ctx context.Context, path string) (*Attr, proto.FsError)

	// GetAttrIfChanged is a lightweight revalidation check (Sub-spec D).
	// Returns (nil, true, OK) when the server's current version matches
	// knownVersion; (newAttr, false, OK) when version changed; (nil, false,
	// ENOENT) when the path is gone. RPC errors return (nil, false, EIO) so
	// callers fall through to a full Stat.
	GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*Attr, bool, proto.FsError)
	// Lookup resolves a child name under parent, returning attrs + inode.
	Lookup(ctx context.Context, parent, name string) (*Attr, proto.FsError)
	// ListDir returns the entries of a directory. A backend configured for
	// plus listings (READDIRPLUS) also carries per-entry attrs so a cache
	// decorator can prime its attr cache from the listing; Attr is nil per
	// entry otherwise, or when the server-side per-entry stat failed.
	ListDir(ctx context.Context, path string) ([]DirEntryPlus, proto.FsError)

	// Access mirrors the access(2) check.
	Access(ctx context.Context, path string, mode uint32) proto.FsError
	// StatFs returns filesystem statistics for the volume containing path.
	StatFs(ctx context.Context, path string) (*StatFs, proto.FsError)
	// GetXAttr returns the extended-attribute bytes for path/attr.
	GetXAttr(ctx context.Context, path, attr string) ([]byte, proto.FsError)
	// SetXAttr stores an extended attribute (flags: XATTR_CREATE/REPLACE).
	SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError
	// RemoveXAttr deletes an extended attribute.
	RemoveXAttr(ctx context.Context, path, attr string) proto.FsError
	// ListXAttr returns the extended-attribute names of path.
	ListXAttr(ctx context.Context, path string) ([]string, proto.FsError)

	// Open opens an existing file. flags follow the FUSE open flags.
	Open(ctx context.Context, path string, flags uint32) (FileHandle, proto.FsError)
	// Create creates a new file as a child of parent.
	Create(ctx context.Context, parent, name string, flags, mode uint32) (FileHandle, *Attr, proto.FsError)
	// Read fills dest starting at off and returns the number of bytes read.
	Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, proto.FsError)
	// Write writes data at off and returns the number of bytes written.
	Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, proto.FsError)
	// Release closes the open file referenced by fh.
	Release(ctx context.Context, fh FileHandle) proto.FsError
	// Flush is called on each close(2) of a fd that opened the file.
	Flush(ctx context.Context, fh FileHandle) proto.FsError
	// Fsync sync()s the file.
	Fsync(ctx context.Context, fh FileHandle, flags int64) proto.FsError
	// Allocate preallocates space at off..off+size for future writes
	// (fallocate(2)).
	Allocate(ctx context.Context, fh FileHandle, off, size uint64, mode uint32) proto.FsError
	// GetLk queries the lock state for a region of the file
	// (fcntl(F_GETLK)).
	GetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) proto.FsError
	// SetLk attempts to acquire a lock without blocking
	// (fcntl(F_SETLK)).
	SetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError
	// SetLkw attempts to acquire a lock, blocking until it can be
	// granted (fcntl(F_SETLKW)).
	SetLkw(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError
	// CopyFileRange copies length bytes server-side from fhIn@offIn to
	// fhOut@offOut without the data crossing the wire. flags must be 0.
	// Short counts are legal (source EOF); callers reissue.
	CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, fhOut FileHandle, offOut uint64, length, flags uint64) (uint64, proto.FsError)
	// Lseek probes hole geometry (SEEK_DATA/SEEK_HOLE) on an open handle.
	Lseek(ctx context.Context, fh FileHandle, offset uint64, whence uint32) (uint64, proto.FsError)

	// Mkdir creates a directory and returns its attrs from the reply so the
	// caller skips the trailing Stat. A nil Attr with FS_OK means the
	// server omitted the attrs (its post-create stat failed); callers fall
	// back to Stat.
	Mkdir(ctx context.Context, path string, mode uint32) (*Attr, proto.FsError)
	// Rmdir removes an empty directory.
	Rmdir(ctx context.Context, path string) proto.FsError
	// Unlink removes a non-directory.
	Unlink(ctx context.Context, path string) proto.FsError
	// Rename moves a file/directory.
	Rename(ctx context.Context, oldPath, newPath string) proto.FsError
	// Readlink returns the target string of the symbolic link at path.
	Readlink(ctx context.Context, path string) (string, proto.FsError)
	// Symlink creates a new symbolic link at linkPath pointing at target.
	// The target string is stored verbatim — confinement is enforced at
	// resolve time, not at create time. Returns the new link's attrs like
	// Mkdir (nil with FS_OK when the server's trailing stat failed).
	Symlink(ctx context.Context, target, linkPath string) (*Attr, proto.FsError)
	// SetAttr applies the fields named by in.Valid in one RPC and returns the
	// resulting attrs (no trailing Stat needed). The server applies
	// size→mode→owner→times and stops at the first failure, so a non-OK
	// status may mean earlier fields were already applied. Mutating: retried
	// with a stable request_id. Replaced the removed per-field Truncate/
	// Chmod/Chown/Utimens fan-out for FUSE SETATTR.
	SetAttr(ctx context.Context, path string, in SetAttrIn) (*Attr, proto.FsError)

	// Close releases resources held by the backend. For the gRPC backend
	// this is a no-op (the connection is owned by the caller); for
	// cachedBackend it stops the subscriber goroutine. Mount code must call
	// Close before discarding a backend on Unmount.
	Close() error
}
