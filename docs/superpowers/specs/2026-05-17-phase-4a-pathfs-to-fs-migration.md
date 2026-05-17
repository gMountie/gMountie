# Phase 4 / Sub-spec A: `pathfs → fs` migration

**Status:** Approved 2026-05-17
**Parent phase:** Phase 4 — persistent client-side cache (roadmap lines 179–249).
**Sub-spec order:** A (this) → B (in-memory cache + TTL) → C (persistence) → D (Subscribe / version push). B/C/D in either order after A lands.

## Goal

Replace the legacy `pathfs.FileSystem` client implementation with the
modern `go-fuse/v2/fs` inode-based API, and introduce a
`FileSystemBackend` interface that abstracts the gRPC layer. Preserves
**all** existing client behavior (readahead, write coalescing, retry,
session/fd lifecycle, retry-on-Unavailable, per-file config). No
caching, no new RPCs, no persistence — this sub-spec is the foundation
the rest of Phase 4 builds on.

## Motivation

The roadmap (line 196) promotes the migration from "opportunistic debt"
into Phase 4 scope because the cache key strategy needs inode
stability: path-based keys break on rename; the existing
`pkg/client/io/fs.go:57` comment about "ignoring `Ino` to force the
client to generate its own ino numbers" is the symptom that goes away
once we adopt the inode-based API. Migrating once now is cheaper than
building Sub-spec B's cache on `pathfs` and re-doing it.

The migration also gives us the natural seam — `FileSystemBackend` —
where Sub-spec B will insert the cache as a decorator. Without a
unified interface, the current two-layer split (`fs.go` is a
`pathfs.FileSystem`, `file.go` is a `nodefs.File`, each independently
talking gRPC) makes the cache invisible to whichever path it doesn't
wrap.

## Architecture

```
                  go-fuse v2 kernel handlers (kernel ↔ user)
                              ↓
                  fs.Inode / fs.NodeXXX  (new — node.go)
                              ↓
                  FileSystemBackend  (new — backend.go)
                              ↓
                  BackendClient  (new — backend_grpc.go, gRPC impl)
                              ↓
                  grpc.Client → RpcFs / RpcFile / Session / Version
```

`FileSystemBackend` is the seam where Sub-spec B's cache will later
decorate the gRPC implementation:

```
  fs.Inode handlers → FileSystemBackend
                            ↑
            ┌──────────────┴──────────────┐
            │  cachedBackend (Sub-spec B)  │
            └──────────────┬──────────────┘
                            ↓
                   BackendClient (gRPC)
```

## Components

### `pkg/client/io/backend.go` (new)

Defines `FileSystemBackend`, the op-shaped fd-based interface that
mirrors FUSE-op semantics one-to-one. Roughly 15 methods:

```go
type FileSystemBackend interface {
    Stat(ctx context.Context, path string) (*Attr, fuse.Status)
    Lookup(ctx context.Context, parent, name string) (attr *Attr, ino uint64, status fuse.Status)
    ListDir(ctx context.Context, path string) ([]DirEntry, fuse.Status)

    Open(ctx context.Context, path string, flags uint32) (FileHandle, fuse.Status)
    Create(ctx context.Context, parent, name string, mode, flags uint32) (FileHandle, *Attr, fuse.Status)
    Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, fuse.Status)
    Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, fuse.Status)
    Release(ctx context.Context, fh FileHandle) fuse.Status
    Flush(ctx context.Context, fh FileHandle) fuse.Status
    Fsync(ctx context.Context, fh FileHandle, flags int64) fuse.Status

    Mkdir(ctx context.Context, path string, mode uint32) fuse.Status
    Rmdir(ctx context.Context, path string) fuse.Status
    Unlink(ctx context.Context, path string) fuse.Status
    Rename(ctx context.Context, oldPath, newPath string) fuse.Status
    Truncate(ctx context.Context, path string, size uint64) fuse.Status
    Chmod(ctx context.Context, path string, mode uint32) fuse.Status
    Chown(ctx context.Context, path string, uid, gid uint32) fuse.Status

    StatFs(ctx context.Context, path string) (*StatFs, fuse.Status)
    GetXAttr(ctx context.Context, path, attr string) ([]byte, fuse.Status)
}

// FileHandle is an opaque per-open-file handle. Concrete
// implementations may hold a session/fd pair, a readahead state, a
// write coalescer, and a per-handle context.
type FileHandle interface {
    Path() string
}
```

`Attr`, `DirEntry`, `StatFs` are small Go types (mirroring the
`fuse.Attr`/`pb.GetAttrReply` shape) — adapter values that keep the
interface decoupled from `pkg/proto`. They live in `backend.go`.

### `pkg/client/io/backend_grpc.go` (new)

`BackendClient` struct implements `FileSystemBackend` against the gRPC
layer. Owns everything currently spread across `fs.go` + `file.go`:

- gRPC client handle (`grpc.Client` from `pkg/client/grpc`)
- Volume name
- Request-ID generation per mutating op
- `retryableCall` for all idempotent / idempotency-tokened ops
- The streaming Read / Write loops from Phase 3

The methods are translations of the existing `LocalFileSystem`
methods — same RPCs, same retry shape, same compression policy.

`grpcFileHandle` is the concrete `FileHandle`. It holds:

- `fd uint64` and `sessionID string` (from Open/Create)
- `path string`
- `readahead *Readahead` (Phase 3 Task 8)
- `coalescer *WriteCoalescer` (Phase 3 Task 9)
- `lifeCtx context.Context` + `lifeCancel` (cancelled on Release)
- `cfg grpc.PerFileConfig` (F2)

This is a direct port of `GrpcFile` from `pkg/client/io/file.go`. The
type loses the `nodefs.File` embedding (no longer needed) and gains
the `FileHandle` interface satisfaction.

### `pkg/client/io/node.go` (new)

Holds the go-fuse `fs.NodeXXX` adapters that translate Inode-style
calls into `FileSystemBackend` op calls.

- `gMountieRoot` — root inode. Implements `fs.NodeOnAdder` (to
  populate children on first lookup), `fs.NodeStatfser`, etc. Holds
  the `FileSystemBackend` reference.
- `gMountieNode` — non-root inode. Implements
  `fs.NodeLookuper`, `fs.NodeReaddirer`, `fs.NodeOpener`,
  `fs.NodeCreater`, `fs.NodeGetattrer`, `fs.NodeSetattrer`,
  `fs.NodeMkdirer`, `fs.NodeRmdirer`, `fs.NodeUnlinker`,
  `fs.NodeRenamer`. Holds the path it represents and a reference to
  the root's backend.
- `gMountieFile` — file handle adapter implementing `fs.FileReader`,
  `fs.FileWriter`, `fs.FileFlusher`, `fs.FileFsyncer`,
  `fs.FileReleaser`. Wraps a `FileHandle` from the backend.

Each adapter method is two to ten lines: marshal/unmarshal between
fs and backend types, delegate to the backend.

### `pkg/client/mount/single.go` and `vfs.go` (modified)

The current code:

```go
fs := io.NewLocalFileSystem(m.client, volume)
nodeFS := pathfs.NewPathNodeFs(fs, createFsOptions())
connector := nodefs.NewFileSystemConnector(nodeFS.Root(), createConnectorOptions())
server, err := fuse.NewServer(connector.RawFS(), path, ...)
```

becomes (exact API depends on go-fuse v2.10.1's `fs` surface — verify
during implementation):

```go
backend := io.NewBackendClient(m.client, volume)
rootNode := io.NewMountieRoot(backend)
fsServer, err := fs.Mount(path, rootNode, &fs.Options{
    MountOptions: *createMountOptions(...),
    // entry/attr timeouts as before
})
```

All FUSE-side knobs from F2's `FUSEConfig` (`MaxWriteBytes`,
`MaxBackground`, `WritebackCache`) carry over unchanged. The
`negotiateMaxWriteBytes` call still happens before Mount.

### Files deleted

- `pkg/client/io/fs.go` — content redistributed to `backend_grpc.go`
  (gRPC translations) and `node.go` (Inode adapters).
- `pkg/client/io/file.go` — content redistributed to
  `backend_grpc.go` (the streaming Read/Write logic, retry,
  request-id stamping) and `node.go` (the FileReader/Writer adapter).

`readahead.go`, `coalesce.go`, `retry.go` are unchanged.

## Inode strategy

Use server-provided `Attr.Ino` as the FUSE inode number. The loopback
filesystem on the server returns the host filesystem's inodes, which
are stable per file across renames. Mapping is direct: `fs.Inode`
constructor takes the ino from `BackendClient.Lookup`.

The current "ignore Ino, generate our own" workaround in `fs.go:57`
is the legacy-pathfs symptom and goes away. Sub-spec B's content
cache will key off `(volume, ino, version)`, which only works if
inodes are stable.

For the volume root inode: ino = 1 by convention. Server doesn't
need to provide one.

## Behavior preservation

Every existing feature stays:

- **Streaming Read** (Phase 3 Task 2) — `BackendClient.Read` runs the
  same per-frame Recv loop.
- **Streaming Write** (Phase 3 Task 3) — `BackendClient.Write` runs
  the same header-on-frame-1 + chunked-send loop.
- **Compound RPC client helper** (Phase 3 Task 4) — still
  shipped-but-unused; Sub-spec B picks it up.
- **Per-call Snappy compression** (Phase 3 Task 5) — same `CallOption`
  on Read/Write only.
- **Keepalive + max message size** (Phase 3 Task 6) — server config,
  not touched here.
- **FUSE mount option negotiation** (Phase 3 Task 7) — same
  `negotiateMaxWriteBytes` call in the mount path.
- **Per-fd readahead** (Phase 3 Task 8) — `grpcFileHandle.readahead`.
- **Per-fd write coalescing** (Phase 3 Task 9) — `grpcFileHandle.coalescer`.
- **Session + idempotency** (Phase 1c/1d) — request-id stamping on
  mutating RPCs, session ID threaded through every call.
- **Retry with bounded backoff** (Phase 1b) — `retryableCall` wraps
  every gRPC call exactly as before.
- **Cancel-on-Release** — `lifeCtx` cancels in-flight prefetches
  before sending the server-side Release.

## Configuration

No new config keys. F2's `PerFileConfig` continues to be the per-file
knob bundle; Sub-spec B will add cache-related keys to it (or to a new
`CacheConfig` sub-struct — defer to B's brainstorm).

## Error handling

Identical to current behavior:

- Every backend method returns `(result, fuse.Status)`.
- gRPC errors are classified by `retryableCall`; retryable ones get
  bounded retries, non-retryable ones surface as `fuse.EIO`.
- The `gMountieNode` adapters propagate `fuse.Status` directly to the
  go-fuse `fs` layer, which translates it to the kernel.
- The mount-startup EIO flake (F3) stays in the bench harness — the
  production code's mitigation (`server.Wait()` after `Unmount()`) is
  unchanged.

## Testing

### Unit tests

- `backend_grpc_test.go` (new) — testify suite. Each `FileSystemBackend`
  method tested against `mockProto.MockRpc{Fs,File}Client`. Direct port
  of `fs_test.go` + `file_test.go` with the new method names.
- `node_test.go` (new) — testify suite. fs.NodeXXX adapters tested
  against `MockFileSystemBackend` (newly generated by mockery).
  Mostly straight-through delegation checks.
- `readahead_test.go`, `coalesce_test.go` — unchanged; the helpers
  they test don't move.
- `common_test.go` (mount) — `negotiateMaxWriteBytes` test unchanged.

### Mocks

`task gen:mocks` adds:

- `MockFileSystemBackend` (new) — in `internal/mocks/pkg/client/io/`.
- `MockFileHandle` (new) — same directory.

Existing mocks remain.

### e2e

Existing `test/e2e/api/*_test.go` and `test/e2e/fs/*_test.go` run
unchanged against the new fs implementation. **They are the
load-bearing acceptance check.** The harness mounts via
`SingleVolumeMounter.Mount`, which now wires the new
`fs.NewNodeFS`-based tree; if the existing tests pass, the migration
is observationally equivalent.

Specifically:

- `TestSimpleFSTestSuite` (fs.go-style operations)
- `TestStreamingReadE2ESuite` / `TestStreamingWriteE2ESuite` (Phase 3
  bidirectional GiB-scale tests)
- `TestCompoundE2ESuite` (raw RPC, doesn't touch the FUSE layer —
  unchanged behavior)
- `TestWriteCoalesceE2ESuite` (Phase 3 Task 9 coalescing e2e — must
  still show 4096→1 RPC reduction)
- `TestFioTestSuite` (fio-driven sequential and random workloads)

### Bench

The `test/e2e/perf/` harness runs against the new fs implementation
unchanged. **Expected outcome:** numbers in the same ballpark as
Phase 3 final (`docs/perf/phase3-final-2026-05-15-localhost.txt`).
A delta worse than ~10% on any bench is a migration regression to
investigate before merging Sub-spec A.

Both `task perf:bench` (bufconn) and `task perf:bench:tcp` (F4)
should be run; only the TCP variant is comparable across migrations
since the in-process bufconn timing is dominated by goroutine
scheduling, not the actual code path.

## Out of scope (explicit)

- **No caching** — attribute, directory, or content. That's Sub-spec B.
- **No new RPCs** — no Subscribe, no GetAttrIfChanged, no
  `Attr.version`. That's Sub-spec D.
- **No persistence** — no bbolt, no chunks/ directory. That's Sub-spec C.
- **No Compound wiring** — the client helper is shipped from Phase 3
  Task 4 but Sub-spec A does not wire it into the new node adapters.
  Sub-spec B picks it up for `ListDir`-with-stat.
- **No perf optimization** beyond preserving existing behavior. The
  migration is observationally equivalent; any speedup or slowdown is
  unintended and gets investigated.
- **No new feature flags** — big-bang switch on `develop`, single
  commit range, old code deleted.

## Risk and mitigation

The migration touches every client-side request path. The mitigations:

1. **The full e2e suite on the VM (`task test`) must be green** before
   the Sub-spec A merge. This includes the FUSE-mount tests, the fio
   suite, and the Phase 3 streaming + coalescing e2e tests.
2. **The perf harness numbers must be within 10% of the Phase 3
   baseline.** A measurable regression on a refactor means a bug.
3. **The migration is bounded in commit range.** If the new
   implementation has a subtle bug, we revert the range, not just one
   commit. Big-bang on `develop` (not master) keeps blast radius local.
4. **Mocks regen is verified to not silently extend the interface
   surface** — `git diff internal/mocks/` should only add the two new
   files and refresh the existing ones that depend on interface
   changes.

## Acceptance criteria

- [ ] `pkg/client/io/fs.go` and `pkg/client/io/file.go` deleted.
- [ ] `pkg/client/io/backend.go`, `backend_grpc.go`, `node.go` created.
- [ ] `pkg/client/mount/single.go` and `vfs.go` use the new fs API.
- [ ] `task test` green on the kubevirt VM, including all e2e suites.
- [ ] `task perf:bench` and `task perf:bench:tcp` produce numbers
  within 10% of Phase 3 final on every benchmark (no migration
  regression).
- [ ] `task gen:mocks` produces a minimal diff (only new
  `MockFileSystemBackend` + `MockFileHandle`, plus refreshes for
  interface changes).
- [ ] No new BindEnv keys; no new validate tags; no proto changes;
  no go.mod additions.
