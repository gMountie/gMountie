# Phase 4 / Sub-spec A: pathfs → fs migration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the legacy `pathfs.FileSystem`-based client with a `go-fuse/v2/fs` inode-based implementation, introducing a `FileSystemBackend` interface that abstracts gRPC. Pure refactor — no caching, no new RPCs, no persistence. Sub-specs B/C/D build on this.

**Architecture:** Two layers below the go-fuse `fs.Inode` API. (1) `FileSystemBackend` interface mirroring FUSE op semantics with explicit `FileHandle` per open file. (2) `BackendClient` concrete implementation that translates each method into a gRPC call (carrying forward all Phase 1-3 behavior: streaming Read/Write, readahead, write coalescing, retry, session/fd lifecycle, per-call Snappy). The go-fuse adapter layer (`gMountieRoot`/`gMountieNode`/`gMountieFile`) is thin — translates `fs.NodeXXX` interface calls into `FileSystemBackend` op calls.

**Tech Stack:** `github.com/hanwen/go-fuse/v2/fs` (inode-based API, replacing the legacy `fuse/pathfs` + `fuse/nodefs`), existing gRPC services (`pkg/proto`), testify suites, mockery v3.7.0.

**Spec reference:** `docs/superpowers/specs/2026-05-17-phase-4a-pathfs-to-fs-migration.md`.

**Working agreements (apply in every task):**
- testify suites mandatory; benchmarks stay flat (`Benchmark*` funcs, no suite).
- `task gen:mocks` regenerates `internal/mocks/`; never hand-edit.
- Errors via `github.com/pkg/errors.Wrap`.
- Logging via `gmountie/pkg/utils/log`.
- Conventional commits, NO `Co-Authored-By:` / `Signed-off-by:` trailers.
- BC is NOT a concern (this is a refactor; FUSE syscall surface is the only contract that holds).
- FUSE-mount tests on the kubevirt VM at `192.168.11.11`; unit tests run locally.
- Pre-Sub-spec-A commit: `c547f07`.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `pkg/client/io/backend.go` | **create** | `FileSystemBackend` interface + adapter types (`Attr`, `DirEntry`, `StatFs`, `FileHandle`) |
| `pkg/client/io/backend_grpc.go` | **create** | `BackendClient` (gRPC impl of `FileSystemBackend`) + `grpcFileHandle` (FileHandle impl, carries fd + readahead + coalescer + lifeCtx) |
| `pkg/client/io/backend_grpc_test.go` | **create** | testify suite covering all `BackendClient` methods against mocked gRPC clients (port of `fs_test.go` + `file_test.go`) |
| `pkg/client/io/node.go` | **create** | go-fuse `fs.NodeXXX` adapters: `gMountieRoot`, `gMountieNode`, `gMountieFile`. Each adapter method is 2–10 lines: translate fs types ↔ backend types, delegate |
| `pkg/client/io/node_test.go` | **create** | testify suite covering the `fs.NodeXXX` adapters with mocked `FileSystemBackend` |
| `pkg/client/io/readahead.go` | unchanged | per-fd readahead state (Phase 3 Task 8) |
| `pkg/client/io/coalesce.go` | unchanged | per-fd write coalescing (Phase 3 Task 9) |
| `pkg/client/io/retry.go` | unchanged | `retryableCall` + `withMetaTimeout` / `withIOTimeout` |
| `pkg/client/io/compound.go` | unchanged | Compound batcher helper (Phase 3 Task 4, still unused) |
| `pkg/client/io/fs.go` | **delete** | replaced by `backend_grpc.go` + `node.go` |
| `pkg/client/io/file.go` | **delete** | replaced by `backend_grpc.go` (`grpcFileHandle`) |
| `pkg/client/io/fs_test.go` | **delete** | tests moved to `backend_grpc_test.go` + `node_test.go` |
| `pkg/client/io/file_test.go` | **delete** | tests moved to `backend_grpc_test.go` |
| `pkg/client/io/readahead_test.go` | unchanged | |
| `pkg/client/io/coalesce_test.go` | unchanged | |
| `pkg/client/io/common_test.go` | **delete if exists** | check & remove if no longer relevant |
| `pkg/client/mount/single.go` | **modify** | swap pathfs construction for `fs.Mount` |
| `pkg/client/mount/vfs.go` | **modify** | same |
| `pkg/client/mount/single_test.go` | **modify** | update mock expectations to use `PerFileConfig()` only (already done in F2; verify still works after migration) |
| `pkg/client/mount/vfs_test.go` | **modify** | same |
| `.mockery.yml` | **modify** | register `FileSystemBackend` and `FileHandle` for mockery to generate |
| `internal/mocks/pkg/client/io/mock_FileSystemBackend.go` | **regenerate** | via `task gen:mocks` |
| `internal/mocks/pkg/client/io/mock_FileHandle.go` | **regenerate** | via `task gen:mocks` |

The legacy `LocalFileSystem` (in `fs.go`) and `GrpcFile` (in `file.go`) and their tests are removed wholesale; nothing else should reference them after Tasks 1–4.

---

## Task 1: Define `FileSystemBackend` interface + adapter types

**Files:**
- Create: `pkg/client/io/backend.go`
- Modify: `.mockery.yml`
- Regenerate: `internal/mocks/pkg/client/io/mock_FileSystemBackend.go`, `mock_FileHandle.go`

- [ ] **Step 1: Write `pkg/client/io/backend.go`**

```go
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
```

- [ ] **Step 2: Register the new interfaces in `.mockery.yml`**

Find the existing `pkg/client/io/` section (or add one) and add:

```yaml
  pkg/client/io:
    interfaces:
      FileSystemBackend:
      FileHandle:
```

Match the indentation / style of existing entries.

- [ ] **Step 3: Regenerate mocks**

```bash
task gen:mocks
```

Expected: two new files appear under `internal/mocks/pkg/client/io/`: `mock_FileSystemBackend.go` and `mock_FileHandle.go`. Other mocks should be unchanged.

- [ ] **Step 4: Build check**

```bash
go build ./...
```

Expected: clean compile. No tests to run yet — the interface has no impl.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend.go .mockery.yml internal/mocks/pkg/client/io/
git commit -m "$(cat <<'EOF'
feat(client/io): introduce FileSystemBackend interface

First piece of Phase 4 / Sub-spec A. Defines the op-shaped fd-based
interface that the new go-fuse fs.NodeXXX adapters will delegate to,
and that Sub-spec B will later decorate with the cache layer. Pure
declaration; no implementation, no tests yet - those land in the next
task with BackendClient.

Mockery picks up FileSystemBackend and FileHandle; existing mocks
unchanged.
EOF
)"
```

---

## Task 2: Implement `BackendClient` (gRPC adapter)

**Files:**
- Create: `pkg/client/io/backend_grpc.go`
- Create: `pkg/client/io/backend_grpc_test.go`

**Approach:** port the gRPC translations from the existing `pkg/client/io/fs.go` (path-level ops) and `pkg/client/io/file.go` (`GrpcFile`'s methods, renamed to operate on a `FileHandle`). The legacy files remain in place during Task 2; the new code lives alongside them under different type names.

- [ ] **Step 1: Write `pkg/client/io/backend_grpc.go` — skeleton + path-level ops**

```go
// backend_grpc.go implements FileSystemBackend against the gRPC layer.
// It owns the wiring previously spread across fs.go (path-level ops) and
// file.go (per-fd ops + streaming Read/Write). Behaviour mirrors the
// legacy implementation: same retry shape, same per-call Snappy on
// Read/Write, same session/fd/request_id discipline from Phase 1.

package io

import (
	"context"
	stdio "io"
	"time"

	grpcclient "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"
	"gmountie/pkg/server/grpc/snappy"
	"gmountie/pkg/utils/log"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// BackendClient is the gRPC implementation of FileSystemBackend. It is
// safe for concurrent use; per-fd state lives on grpcFileHandle.
type BackendClient struct {
	client grpc.Client // gRPC client from pkg/client/grpc
	volume string
}

// NewBackendClient constructs a gRPC-backed FileSystemBackend bound to a
// single volume.
func NewBackendClient(client grpcclient.Client, volume string) *BackendClient {
	return &BackendClient{client: client, volume: volume}
}

// callerFromCtx extracts a *proto.Caller from the FUSE caller info that
// go-fuse drops on the context. Returns a zero-uid Caller if absent.
func callerFromCtx(ctx context.Context) *proto.Caller {
	c, ok := fuse.FromContext(ctx)
	if !ok || c == nil {
		return &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}, Pid: 0}
	}
	return &proto.Caller{
		Owner: &proto.Owner{Uid: c.Uid, Gid: c.Gid},
		Pid:   c.Pid,
	}
}

// Stat implements FileSystemBackend.Stat.
func (b *BackendClient) Stat(ctx context.Context, path string) (*Attr, fuse.Status) {
	ctx, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "GetAttr", func(ctx context.Context) (*proto.GetAttrReply, error) {
		return b.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
			Volume: b.volume,
			Caller: callerFromCtx(ctx),
			Path:   path,
		})
	})
	if err != nil {
		log.Log.Error("error in call: GetAttr", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
	}
	if res.GetAttributes() == nil {
		return nil, fuse.Status(res.Status)
	}
	return attrFromProto(res.GetAttributes()), fuse.Status(res.Status)
}

// attrFromProto translates a *proto.Attr to a backend Attr.
func attrFromProto(p *proto.Attr) *Attr {
	return &Attr{
		Ino:       p.Ino,
		Size:      p.Size,
		Blocks:    p.Blocks,
		Atime:     p.Atime,
		Mtime:     p.Mtime,
		Ctime:     p.Ctime,
		Atimensec: p.Atimensec,
		Mtimensec: p.Mtimensec,
		Ctimensec: p.Ctimensec,
		Mode:      p.Mode,
		Nlink:     p.Nlink,
		Uid:       p.Uid,
		Gid:       p.Gid,
		Rdev:      p.Rdev,
		Blksize:   p.Blksize,
	}
}
```

The remaining path-level methods (`Lookup`, `ListDir`, `Access`, `StatFs`, `GetXAttr`, `Mkdir`, `Rmdir`, `Unlink`, `Rename`, `Truncate`, `Chmod`, `Chown`) follow the same pattern: thin wrapper around the existing fs.go method's body, returning `(typed result, fuse.Status)` instead of writing into a `*fuse.Attr` out-param.

For each one, copy the gRPC call site from the corresponding method in `pkg/client/io/fs.go` and adapt:
- input: take `ctx`, path/args
- replace `withMetaTimeout(fctx, ...)` with `withMetaTimeout(ctx, ...)` — the ctx is now the go-fuse `fs` ctx, not the pathfs `*fuse.Context`. `callerFromCtx` extracts Uid/Gid the same way.
- mutating ops still generate `requestID := uuid.NewString()` *outside* the retry closure (Phase 1d invariant).
- output: return adapter types (`*Attr`, `[]DirEntry`, `[]byte`, `*StatFs`) instead of populating `*fuse.Attr`-style out-params.

`Lookup`: call `GetAttr` server-side (existing semantics — the server doesn't have a separate Lookup RPC). Return both the `*Attr` and the inode (use `attr.Ino`).

`ListDir`: call `OpenDir`, translate each `*proto.DirEntry` to a backend `DirEntry`.

- [ ] **Step 2: Add fd-level ops + `grpcFileHandle`**

Append to `backend_grpc.go`:

```go
// grpcFileHandle is the FileHandle returned by Open/Create. Carries fd +
// session id from the server, plus the per-fd readahead and write
// coalescer state from Phase 3 Tasks 8 and 9.
type grpcFileHandle struct {
	client            proto.RpcFileClient
	volume            string
	path              string
	fd                uint64
	ioTimeout         time.Duration
	sessionID         string
	readahead         *Readahead
	coalescer         *WriteCoalescer
	coalesceThreshold int
	lifeCtx           context.Context
	lifeCancel        context.CancelFunc
}

// Path implements FileHandle.
func (h *grpcFileHandle) Path() string { return h.path }

// newGrpcFileHandle is a direct port of the GrpcFile constructor in
// file.go - same per-fd state, same lifeCtx lifecycle, same readahead +
// coalescer initialization gated by PerFileConfig.
func newGrpcFileHandle(
	client proto.RpcFileClient,
	volume, path string,
	fd uint64,
	ioTimeout time.Duration,
	sessionID string,
	cfg grpcclient.PerFileConfig,
) *grpcFileHandle {
	ctx, cancel := context.WithCancel(context.Background())
	h := &grpcFileHandle{
		client:            client,
		volume:            volume,
		path:              path,
		fd:                fd,
		ioTimeout:         ioTimeout,
		sessionID:         sessionID,
		coalesceThreshold: cfg.WriteCoalesceBytes,
		lifeCtx:           ctx,
		lifeCancel:        cancel,
	}
	if cfg.ReadaheadChunkBytes > 0 && cfg.ReadaheadThreshold > 0 {
		h.readahead = NewReadahead(cfg.ReadaheadChunkBytes, cfg.ReadaheadThreshold)
	}
	if cfg.WriteCoalesceBytes > 0 {
		h.coalescer = NewWriteCoalescer(cfg.WriteCoalesceBytes)
	}
	return h
}
```

Then port the methods from `GrpcFile` in `file.go` onto `*BackendClient`, taking a `FileHandle` argument:

```go
// Open implements FileSystemBackend.Open by issuing the Open RPC and
// wrapping the returned fd in a grpcFileHandle.
func (b *BackendClient) Open(ctx context.Context, path string, flags uint32) (FileHandle, fuse.Status) {
	ctx2, cancel := withMetaTimeout(ctx, b.client.MetaTimeout())
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Open", func(ctx context.Context) (*proto.OpenReply, error) {
		return b.client.File().Open(ctx, &proto.OpenRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Flags:     flags,
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Open", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
	}
	if fuse.Status(res.Status) != fuse.OK {
		return nil, fuse.Status(res.Status)
	}
	return newGrpcFileHandle(
		b.client.File(),
		b.volume,
		path,
		res.Fd,
		b.client.IOTimeout(),
		b.client.SessionID(),
		b.client.PerFileConfig(),
	), fuse.OK
}

// Read implements FileSystemBackend.Read. Body lifted verbatim from
// GrpcFile.Read in file.go - streaming Read + readahead serve/observe
// + retry. fh MUST be a *grpcFileHandle returned by Open/Create.
func (b *BackendClient) Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, fuse.Status) {
	h, ok := fh.(*grpcFileHandle)
	if !ok {
		return 0, fuse.EBADF
	}
	if h.readahead != nil {
		if n, hit := h.readahead.Serve(dest, off); hit {
			if prefetchOff, ok := h.readahead.Observe(off, n); ok {
				go b.doPrefetch(h, prefetchOff)
			}
			return n, fuse.OK
		}
	}
	rctx, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	// ... port the streaming Recv loop from file.go ...
}
```

Continue porting `Write`, `Release`, `Flush`, `Fsync` similarly. The streaming loop bodies are direct copies from `file.go`; the only changes are the receiver type (`*BackendClient`) and the `fh.(*grpcFileHandle)` cast at the top of each fd-level method.

**Critically:** the `doPrefetch` helper is a method on `*BackendClient` now (takes `h *grpcFileHandle`), not on the handle directly.

- [ ] **Step 3: Write `pkg/client/io/backend_grpc_test.go` — testify suite**

Port the existing tests from `fs_test.go` + `file_test.go`. Same fixture pattern (MockRpcFsClient, MockRpcFileClient on a mock Client) but the methods under test are now on `*BackendClient`:

```go
package io

import (
	"context"
	stdio "io"
	"testing"
	"time"

	mockProto "gmountie/internal/mocks/pkg/proto"
	grpcmocks "gmountie/internal/mocks/pkg/client/grpc"
	grpcclient "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type BackendClientTestSuite struct {
	suite.Suite
	client     *grpcmocks.MockClient
	fsClient   *mockProto.MockRpcFsClient
	fileClient *mockProto.MockRpcFileClient
	backend    *BackendClient
}

func (s *BackendClientTestSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.fsClient = mockProto.NewMockRpcFsClient(s.T())
	s.fileClient = mockProto.NewMockRpcFileClient(s.T())
	s.client.EXPECT().Fs().Return(s.fsClient).Maybe()
	s.client.EXPECT().File().Return(s.fileClient).Maybe()
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
	s.client.EXPECT().IOTimeout().Return(30 * time.Second).Maybe()
	s.client.EXPECT().SessionID().Return("test-session").Maybe()
	s.client.EXPECT().PerFileConfig().Return(grpcclient.PerFileConfig{}).Maybe()
	s.backend = NewBackendClient(s.client, "testVolume")
}

func (s *BackendClientTestSuite) TestStat() {
	s.fsClient.EXPECT().GetAttr(mock.Anything, mock.MatchedBy(func(r *proto.GetAttrRequest) bool {
		return r.Volume == "testVolume" && r.Path == "/file"
	})).Return(&proto.GetAttrReply{
		Attributes: &proto.Attr{Ino: 42, Size: 100, Mode: 0o644},
		Status:     int32(fuse.OK),
	}, nil)

	a, st := s.backend.Stat(context.Background(), "/file")
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(a)
	s.Assert().Equal(uint64(42), a.Ino)
	s.Assert().Equal(uint64(100), a.Size)
}

// ... continue porting the rest: Lookup, ListDir, Access, StatFs,
// GetXAttr, Open, Create, Read (with multiple frames), Write (chunked +
// retry-reuses-request-id, mirroring TestWriteRetryReusesRequestID from
// the legacy file_test.go), Release, Flush, Fsync, Mkdir, Rmdir,
// Unlink, Rename, Truncate, Chmod, Chown.
```

Each ported test should keep its original name (e.g. the legacy `TestRead` becomes `TestRead` here too — they don't coexist because the legacy test file is deleted later) and the same fixture data so a reviewer can diff old-vs-new with grep.

- [ ] **Step 4: Run unit tests**

```bash
go test -race -run TestBackendClientTestSuite ./pkg/client/io/
go vet ./...
```

Expected: all suite methods pass, race-clean.

- [ ] **Step 5: Build check (legacy + new coexist)**

```bash
go build ./...
go test ./pkg/client/io/ -count=1
```

Expected: both old `LocalFileSystem`/`GrpcFile` tests AND new `BackendClient` tests compile and pass. `task gen:mocks` is not required (no interfaces changed since Task 1).

- [ ] **Step 6: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/backend_grpc_test.go
git commit -m "$(cat <<'EOF'
feat(client/io): BackendClient - gRPC impl of FileSystemBackend

Second piece of Phase 4 / Sub-spec A. Ports the gRPC translations from
fs.go + file.go onto a single BackendClient that implements the
FileSystemBackend interface from Task 1. Per-fd state (fd + session_id
+ readahead + write coalescer + lifeCtx) lives on grpcFileHandle,
returned from Open/Create.

Phase 1-3 behaviour preserved verbatim: streaming Read/Write loops,
readahead Serve/Observe + Store, write coalescing flush-on-Threshold
/discontiguous-offset/Flush/Release/Fsync, retryableCall with
bounded backoff, per-call Snappy compressor on Read/Write only,
request_id generated outside the retry closure on mutating ops.

Legacy LocalFileSystem and GrpcFile remain in place during this task -
they share the package but no symbol collisions. Removed in Task 5.
EOF
)"
```

---

## Task 3: Implement go-fuse `fs.NodeXXX` adapters

**Files:**
- Create: `pkg/client/io/node.go`
- Create: `pkg/client/io/node_test.go`

**Approach:** the adapters are thin. Each `fs.NodeXXX` method:
1. Computes the path of the inode (using `n.Path(rootNode)` from go-fuse).
2. Calls the corresponding `FileSystemBackend` method.
3. Translates the result into the fs.X out-params (`*fuse.EntryOut`, `*fuse.AttrOut`, `*fuse.StatfsOut`, `fs.FileHandle`, etc.).
4. Returns `syscall.Errno` (via `syscall.Errno(status)` — `fuse.Status` and `syscall.Errno` are both `uint32` aliases at the wire).

- [ ] **Step 1: Write `pkg/client/io/node.go` — root + node + file adapters**

```go
// node.go bridges go-fuse v2 fs.NodeXXX interfaces to FileSystemBackend.
// Each method is intentionally thin: translate path/types, delegate.

package io

import (
	"context"
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

// gMountieNode is a non-root inode. relPath is its path relative to
// the root. backend is shared with the root.
type gMountieNode struct {
	fs.Inode

	backend FileSystemBackend
	relPath string
}

var (
	_ fs.NodeLookuper  = (*gMountieRoot)(nil)
	_ fs.NodeReaddirer = (*gMountieRoot)(nil)
	_ fs.NodeStatfser  = (*gMountieRoot)(nil)

	_ fs.NodeLookuper   = (*gMountieNode)(nil)
	_ fs.NodeReaddirer  = (*gMountieNode)(nil)
	_ fs.NodeGetattrer  = (*gMountieNode)(nil)
	_ fs.NodeSetattrer  = (*gMountieNode)(nil)
	_ fs.NodeOpener     = (*gMountieNode)(nil)
	_ fs.NodeCreater    = (*gMountieNode)(nil)
	_ fs.NodeMkdirer    = (*gMountieNode)(nil)
	_ fs.NodeRmdirer    = (*gMountieNode)(nil)
	_ fs.NodeUnlinker   = (*gMountieNode)(nil)
	_ fs.NodeRenamer    = (*gMountieNode)(nil)
	_ fs.NodeGetxattrer = (*gMountieNode)(nil)
	_ fs.NodeAccesser   = (*gMountieNode)(nil)
	_ fs.NodeStatfser   = (*gMountieNode)(nil)
)

// nodePath returns the slash-rooted path of the inode relative to the
// mount root. For the root itself it returns "".
func (n *gMountieNode) path() string { return n.relPath }
func (r *gMountieRoot) path() string { return "" }

// fillEntryOut populates *fuse.EntryOut from a backend Attr. Used in
// Lookup/Create/Mkdir.
func fillEntryOut(out *fuse.EntryOut, a *Attr) {
	out.NodeId = a.Ino
	out.Attr.FromAttr(a) // helper defined below
}

// fillAttrOut populates *fuse.AttrOut from a backend Attr.
func fillAttrOut(out *fuse.AttrOut, a *Attr) {
	out.Attr.FromAttr(a)
}

// Helpers for converting between backend Attr and fuse.Attr live below
// this section. They are unexported and pure; small enough to inline.
```

Then implement each adapter method. The shape is uniform — here's `Lookup` and `Read` as templates; the others follow exactly the same pattern:

```go
// Lookup on the root resolves `name` as a top-level entry.
func (r *gMountieRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	a, st := r.backend.Lookup(ctx, "", name)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	fillEntryOut(out, a)
	child := r.NewInode(ctx, &gMountieNode{backend: r.backend, relPath: name}, fs.StableAttr{
		Mode: a.Mode,
		Ino:  a.Ino,
	})
	return child, 0
}

// Read on the file handle.
func (f *gMountieFile) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n, st := f.backend.Read(ctx, f.fh, off, dest)
	if !st.Ok() {
		return nil, syscall.Errno(st)
	}
	return fuse.ReadResultData(dest[:n]), 0
}
```

`gMountieFile` is the file-handle adapter:

```go
type gMountieFile struct {
	backend FileSystemBackend
	fh      FileHandle
}

var (
	_ fs.FileReader   = (*gMountieFile)(nil)
	_ fs.FileWriter   = (*gMountieFile)(nil)
	_ fs.FileFlusher  = (*gMountieFile)(nil)
	_ fs.FileFsyncer  = (*gMountieFile)(nil)
	_ fs.FileReleaser = (*gMountieFile)(nil)
)
```

Continue implementing each interface method as a thin translator. The full list of interfaces to satisfy on `gMountieNode` and `gMountieRoot` matches the spec's component description; refer back to the legacy `fs.go` for the FUSE-side semantic each was wired to.

- [ ] **Step 2: Helper to map backend Attr → fuse.Attr**

Inside `node.go`, add a method on `*fuse.Attr` via a small helper because go-fuse's Attr.FromAttr doesn't exist:

```go
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
```

Update the `fillEntryOut`/`fillAttrOut` helpers above to call `setAttrFromBackend(&out.Attr, a)`.

- [ ] **Step 3: Write `pkg/client/io/node_test.go` — testify suite**

```go
package io

import (
	"context"
	"testing"

	iomocks "gmountie/internal/mocks/pkg/client/io"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type NodeAdapterTestSuite struct {
	suite.Suite
	backend *iomocks.MockFileSystemBackend
	root    fs.InodeEmbedder
}

func (s *NodeAdapterTestSuite) SetupTest() {
	s.backend = iomocks.NewMockFileSystemBackend(s.T())
	s.root = NewMountieRoot(s.backend)
}

func (s *NodeAdapterTestSuite) TestRootLookup() {
	s.backend.EXPECT().Lookup(mock.Anything, "", "child").Return(
		&Attr{Ino: 42, Mode: fuse.S_IFREG | 0o644},
		fuse.OK,
	)
	out := &fuse.EntryOut{}
	// Drive Lookup directly on the root impl.
	r := s.root.(*gMountieRoot)
	_, errno := r.Lookup(context.Background(), "child", out)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Assert().Equal(uint64(42), out.NodeId)
	s.Assert().Equal(uint64(42), out.Attr.Ino)
}

// Add one test per non-trivial adapter method. Smoke-test each delegation;
// the heavy semantic testing already lives in backend_grpc_test.go.
```

Focus on:
- Lookup (root + node)
- Getattr (node)
- Open + Read + Write + Release (file handle delegation)
- Create (verify both inode and file-handle are returned)
- Mkdir / Rmdir / Unlink / Rename (mutating ops)
- Readdir (DirStream iteration)
- Statfs (root + node)

A 12-method test suite is right-sized.

- [ ] **Step 4: Run tests**

```bash
go test -race ./pkg/client/io/
go vet ./...
```

Expected: all green. Mocks for `FileSystemBackend` were generated in Task 1.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/node.go pkg/client/io/node_test.go
git commit -m "$(cat <<'EOF'
feat(client/io): go-fuse fs.NodeXXX adapters delegating to FileSystemBackend

Third piece of Phase 4 / Sub-spec A. gMountieRoot, gMountieNode, and
gMountieFile satisfy the relevant go-fuse v2 fs.NodeXXX and
fs.FileXXX interfaces and translate calls through to the
FileSystemBackend from Task 1.

Each adapter method is 2-10 lines: compute path, marshal/unmarshal
Attr/DirEntry/StatFs, delegate, translate fuse.Status -> syscall.Errno.
The heavy semantic testing lives in backend_grpc_test.go; node_test.go
covers the delegation shape with mocked FileSystemBackend.

Legacy fs.go / file.go are still present - they are removed in Task 5
once the mount path is migrated.
EOF
)"
```

---

## Task 4: Migrate mount paths to `fs.Mount`

**Files:**
- Modify: `pkg/client/mount/single.go`
- Modify: `pkg/client/mount/vfs.go`

- [ ] **Step 1: Update `pkg/client/mount/single.go::Mount`**

Find the current pathfs-based construction:

```go
fs := io.NewLocalFileSystem(m.client, volume)
nodeFS := pathfs.NewPathNodeFs(fs, createFsOptions())
connector := nodefs.NewFileSystemConnector(nodeFS.Root(), createConnectorOptions())
server, err := fuse.NewServer(connector.RawFS(), path, createMountOptions(m.client.GetEndpoint(), volume, m.fuse, maxWrite))
```

Replace with the fs-based construction:

```go
backend := io.NewBackendClient(m.client, volume)
root := io.NewMountieRoot(backend)
mountOpts := createMountOptions(m.client.GetEndpoint(), volume, m.fuse, maxWrite)
entry := time.Second
attr := time.Second
fsOpts := &gofs.Options{
	MountOptions:    *mountOpts,
	EntryTimeout:    &entry,
	AttrTimeout:     &attr,
	NegativeTimeout: nil, // matches the legacy behaviour
}
server, err := gofs.Mount(path, root, fsOpts)
if err != nil {
	return errors.Wrap(err, "mount fail")
}
```

(Import alias: `gofs "github.com/hanwen/go-fuse/v2/fs"` so we don't shadow our local `io` package via `fs`. Adjust imports — remove `pathfs` / `nodefs`.)

`fs.Mount` returns a `*fuse.Server` directly, so `m.mounts.Store(volume, server)` works unchanged.

The existing post-mount code (`go server.Serve()`, `server.WaitMount()`) is NOT needed — `fs.Mount` already starts the server. Remove those two lines and the `m.mounts.Store(volume, server)` after them.

Wait — verify against go-fuse: `fs.Mount` does both NewServer + go Serve + WaitMount internally? Check by reading `/home/john/go/pkg/mod/github.com/hanwen/go-fuse/v2@v2.10.1/fs/api.go` around the Mount function. Adjust the snippet above based on the actual behaviour.

- [ ] **Step 2: Update `pkg/client/mount/vfs.go::mountMemFS`**

Mirror the change. The VFS mounter mounts an in-memory root and attaches per-volume subdirs via `connector.MountInode`. The fs API equivalent is to construct a root inode that has its children pre-populated via `OnAdd`. **This is the riskier of the two changes — verify by reading `dynamic_example_test.go` in go-fuse v2 source** to see the recommended pattern for an inode tree that gains children at runtime.

If the migration shape isn't a clean fit, report BLOCKED with the specific go-fuse API question.

- [ ] **Step 3: Update mount tests**

`pkg/client/mount/single_test.go` and `vfs_test.go` — the existing mock-Client expectations from F2 (`PerFileConfig()`, etc.) should still work. Run the suites to check:

```bash
go test -race ./pkg/client/mount/...
```

If a test fails because it expected a `pathfs.PathNodeFs` to exist or similar, update that test to the new shape. Sandbox-FUSE-only failures (`fusermount exited with code 256`) are pre-existing and OK to leave.

- [ ] **Step 4: Sync to VM and run mount-affecting e2e**

```bash
rsync -av --delete --exclude '.git/' --exclude 'ui/frontend/node_modules' ./ ubuntu@192.168.11.11:~/gMountie/
ssh ubuntu@192.168.11.11 'cd ~/gMountie && go test -v -timeout 5m ./pkg/client/mount/... ./test/e2e/api/ 2>&1 | tail -50'
```

Expected: green. If a real FUSE test fails (not the sandbox stub), investigate before continuing.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/single.go pkg/client/mount/vfs.go pkg/client/mount/single_test.go pkg/client/mount/vfs_test.go
git commit -m "$(cat <<'EOF'
feat(client/mount): switch single and VFS mounters to go-fuse fs API

Fourth piece of Phase 4 / Sub-spec A. SingleVolumeMounter.Mount and
VFSVolumeMounter.mountMemFS now construct a gMountieRoot wrapping a
BackendClient and hand it to fs.Mount, replacing the pathfs +
nodefs.FileSystemConnector chain.

negotiateMaxWriteBytes (server-driven FUSE frame ceiling) is
unchanged. The mount options carry over: createMountOptions still
governs MaxWrite, MaxBackground, WritebackCache, plus the locally
chosen entry/attr timeouts (1s each, matching legacy behavior).

Mount tests adjusted where they touched pathfs-specific shape;
e2e suites on the VM run green.
EOF
)"
```

---

## Task 5: Delete legacy `fs.go` and `file.go`

**Files:**
- Delete: `pkg/client/io/fs.go`
- Delete: `pkg/client/io/file.go`
- Delete: `pkg/client/io/fs_test.go`
- Delete: `pkg/client/io/file_test.go`

- [ ] **Step 1: Confirm no references remain**

```bash
grep -r "LocalFileSystem\|NewLocalFileSystem\|GrpcFile\|NewGrpcFile" pkg/ cmd/ test/ 2>&1 | grep -v internal/mocks | head -20
```

Expected: zero hits. Everything has moved to `BackendClient` / `grpcFileHandle`. If anything still references the legacy types, fix that file before deleting.

- [ ] **Step 2: Delete the files**

```bash
git rm pkg/client/io/fs.go pkg/client/io/file.go pkg/client/io/fs_test.go pkg/client/io/file_test.go
```

- [ ] **Step 3: Regenerate mocks (drops orphaned mock_GrpcFile_*.go if any existed)**

```bash
task gen:mocks
git status --short internal/mocks/
```

If any mocks under `internal/mocks/pkg/client/io/` are now orphaned, they should disappear from disk. If any new "missing interface" warnings appear, investigate.

- [ ] **Step 4: Full unit test pass**

```bash
go test -race ./pkg/client/... ./test/e2e/utils/...
go vet ./...
```

Expected: green (modulo `pkg/client/mount` FUSE-sandbox failures, which are pre-existing).

- [ ] **Step 5: Commit**

```bash
git add -A pkg/client/io/ internal/mocks/
git commit -m "$(cat <<'EOF'
chore(client/io): delete legacy pathfs-based fs.go and file.go

Phase 4 / Sub-spec A endgame. LocalFileSystem and GrpcFile have been
fully replaced by BackendClient and grpcFileHandle behind the
FileSystemBackend interface; the mount path uses fs.Mount.

Tests previously in fs_test.go / file_test.go are now in
backend_grpc_test.go and node_test.go. Mocks regenerated; orphans
removed.

Migration complete on the code path. VM e2e validation is Task 6.
EOF
)"
```

---

## Task 6: Full e2e validation on the VM

**Files:** none — this task is verification only. Commits only if a regression is found that requires a code fix.

- [ ] **Step 1: Sync the post-migration tree to the VM**

```bash
rsync -av --delete --exclude '.git/' --exclude 'ui/frontend/node_modules' ./ ubuntu@192.168.11.11:~/gMountie/
```

- [ ] **Step 2: Full `task test` on the VM**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && task test 2>&1 | tail -40'
```

Expected: every test suite green. Pay attention to:
- `TestSimpleFSTestSuite` (legacy fs.go semantics — Open, Create, Read, Write, ReadDir, Stat, Unlink, Rename, Truncate)
- `TestStreamingReadE2ESuite` / `TestStreamingWriteE2ESuite` (Phase 3 GiB-scale streaming)
- `TestCompoundE2ESuite` (raw RPC, FUSE-independent — must still pass)
- `TestWriteCoalesceE2ESuite` (Phase 3 Task 9 — must still observe 4096-writes → 1 RPC)
- `TestFioTestSuite` (fio sequential + random workloads through the new mount)

If anything fails, report BLOCKED with the test name and the failure mode. Likely categories:
- Behavioural regression in `BackendClient` (the streaming Read/Write loops are sensitive — copy errors from `file.go` will show up here)
- Adapter-layer translation bug (`gMountieNode` returns wrong errno or fills out-params incorrectly)
- Mount-time wiring error (entry/attr timeouts mismatched with what tests expect)

- [ ] **Step 3: If green, no commit; if any fix was made, commit it under a `fix(client/io): post-migration ...` header**

---

## Task 7: Perf comparison vs Phase 3 final baseline

**Files:**
- Create: `docs/perf/phase4a-2026-05-17-localhost.txt`
- Create: `docs/perf/phase4a-2026-05-17-tcp.txt`
- Create: `docs/perf/phase4a-2026-05-17.md`

- [ ] **Step 1: Run localhost bench**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && nohup task perf:bench OUT=docs/perf/phase4a-2026-05-17-localhost.txt > /tmp/bench-4a.stdout 2>&1 & echo "pid: $!"; disown'
```

Poll until exit (~22 min). Pull the file back: `scp ubuntu@192.168.11.11:~/gMountie/docs/perf/phase4a-2026-05-17-localhost.txt docs/perf/`.

- [ ] **Step 2: Run TCP bench**

```bash
ssh ubuntu@192.168.11.11 'cd ~/gMountie && nohup task perf:bench:tcp OUT=docs/perf/phase4a-2026-05-17-tcp.txt > /tmp/bench-4a-tcp.stdout 2>&1 & echo "pid: $!"; disown'
```

Poll until exit. Pull the file.

- [ ] **Step 3: Benchstat diff vs Phase 3 final**

```bash
benchstat docs/perf/phase3-final-2026-05-15-localhost.txt docs/perf/phase4a-2026-05-17-localhost.txt
benchstat docs/perf/phase3-final-2026-05-15-slow30ms.txt docs/perf/phase4a-2026-05-17-tcp.txt
```

(Phase 3 didn't have a TCP run — `phase3-final-2026-05-15-slow30ms.txt` was bufconn-bound. The TCP-vs-bufconn-slow30ms comparison won't be apples-to-apples, but it's the only baseline we have. Note this in the summary doc.)

**Expected:** all sec/op deltas within ±10%. The migration is a refactor; a measurable regression beyond noise is a bug.

- [ ] **Step 4: Write `docs/perf/phase4a-2026-05-17.md`**

Short summary doc:
- Commit measured
- Bench environment (VM specs from baseline.md)
- The two benchstat tables (paste output)
- One sentence per non-noise delta — pass or fail
- Verdict: refactor is observationally equivalent (or: investigate X regression before merging)

- [ ] **Step 5: Commit**

```bash
git add docs/perf/phase4a-2026-05-17*.txt docs/perf/phase4a-2026-05-17.md
git commit -m "$(cat <<'EOF'
docs(perf): Phase 4 / Sub-spec A migration - perf delta vs Phase 3

Confirms the pathfs->fs migration is observationally equivalent.
benchstat deltas (localhost + TCP) for every benchmark within +/-10%
of Phase 3 final (or: documented investigation for the outliers).

Sub-spec A migration ready to merge.
EOF
)"
```

---

## Spec coverage self-check

| Spec section | Covered by |
|---|---|
| `FileSystemBackend` interface + adapter types | Task 1 |
| `BackendClient` gRPC impl | Task 2 |
| `grpcFileHandle` (port of `GrpcFile`) | Task 2 |
| go-fuse `fs.NodeXXX` adapters | Task 3 |
| Mount path migration | Task 4 |
| Inode strategy (server-provided ino) | Task 3 (`StableAttr.Ino: a.Ino`) |
| Behavior preservation (streaming, readahead, coalescing, retry, session/fd) | Task 2 (verbatim port) + Task 6 (e2e verification) |
| Delete legacy `fs.go` / `file.go` | Task 5 |
| Mocks regen | Task 1 (interface), Task 5 (cleanup) |
| Unit tests | Task 2, Task 3 |
| e2e on VM | Task 6 |
| Perf within 10% of Phase 3 | Task 7 |

All acceptance criteria from the spec map to tasks. No placeholders. Type names consistent throughout: `FileSystemBackend`, `BackendClient`, `FileHandle`, `grpcFileHandle`, `gMountieRoot`, `gMountieNode`, `gMountieFile`.
