# Server-side copy and missing filesystem operations

Status: approved design, pre-implementation.

## Motivation

Copying a file within a mounted volume streams every byte from the server to
the client and back up. The kernel offers `copy_file_range(2)` to short-circuit
exactly this, but gMountie implements it nowhere in the stack, so the copy
degrades to a read/write loop over gRPC. On new kernels (FUSE protocol 7.45+)
the first copy attempt also logs a one-time
`Unimplemented opcode COPY_FILE_RANGE_64` from go-fuse — harmless probe noise,
but a symptom of the same gap. (go-fuse, including master, has no handler for
the 64-bit opcode; the kernel falls back to the 32-bit `FUSE_COPY_FILE_RANGE`,
which go-fuse *does* dispatch. Fixing go-fuse upstream is explicitly out of
scope; the 32-bit path is fully functional.)

A coverage audit found the complete set of FUSE operations with no wire
protocol support:

| Operation | Used by | In scope |
|---|---|---|
| `copy_file_range` | Nautilus/GIO, coreutils ≥ 9 | **yes** |
| `lseek` (`SEEK_DATA`/`SEEK_HOLE`) | sparse-aware `cp`, `tar`, rsync | **yes** |
| `setxattr`/`removexattr`/`listxattr` | GIO attrs, `rsync -X`, ACLs | **yes** |
| `link` (hard links) | `tar`, build tools | no |
| `mknod` | FIFOs/sockets | no |

The xattr *read* side (`GetXAttr`) already exists end-to-end, and
`ConfinedLoopbackFileSystem` already implements all four xattr ops with tests —
only the wire plumbing is missing.

Backward compatibility is a non-goal: client and server ship together. The
existing gRPC `Unimplemented → ENOSYS` mapping remains as incidental
robustness, untested across versions.

## Approach

Each op gets its own explicit RPC following the established vertical:
proto → controller (session resolution, identity binding, idempotency where
mutating, event emission) → `ConfinedLoopbackFileSystem` → client backend →
cache layer → go-fuse node interface. No generic extension RPC, no `Compound`
changes (it stays read-only metadata).

## Wire protocol

**`file.proto` (`RpcFile`)** — handle-based, field style mirrors `Allocate`
(`session_id` but no `request_id`; a replayed copy of the same range is
byte-identical):

```proto
rpc CopyFileRange(CopyFileRangeRequest) returns (CopyFileRangeReply);
rpc Lseek(LseekRequest) returns (LseekReply);

message CopyFileRangeRequest {
  string volume = 1;
  Caller caller = 2;
  uint64 fd_in = 3;       // open server handle, source
  string path_in = 4;
  uint64 off_in = 5;
  uint64 fd_out = 6;      // open server handle, destination
  string path_out = 7;
  uint64 off_out = 8;
  uint64 length = 9;
  uint64 flags = 10;      // must be 0 (copy_file_range(2) contract)
  string session_id = 11;
}
message CopyFileRangeReply { uint64 bytes_copied = 1; }

message LseekRequest {
  string volume = 1;
  Caller caller = 2;
  uint64 fd = 3;
  string path = 4;
  uint64 offset = 5;
  uint32 whence = 6;      // SEEK_DATA | SEEK_HOLE only
  string session_id = 7;
}
message LseekReply { uint64 offset = 1; }
```

**`fs.proto` (`RpcFs`)** — path-based, mirrors `GetXAttr` including the
embedded `int32 status` errno convention:

```proto
rpc SetXAttr(SetXAttrRequest) returns (SetXAttrReply);
rpc RemoveXAttr(RemoveXAttrRequest) returns (RemoveXAttrReply);
rpc ListXAttr(ListXAttrRequest) returns (ListXAttrReply);
```

`SetXAttrRequest` adds `bytes data` + `uint32 flags`
(`XATTR_CREATE`/`XATTR_REPLACE`) and, being mutating, `request_id` for
`withIdempotency` dedup (like `Chmod`); `RemoveXAttrRequest` likewise.
`ListXAttrReply` carries `repeated string attributes` + `int32 status`.

## Server

**`controller/file.go` — `CopyFileRange`:**

1. Resolve session; look up both handles in the session's handle table; either
   missing or foreign → `EBADF`.
2. `flags != 0` → `EINVAL`.
3. No identity re-bind: permission was checked at `Open`; the fds carry their
   access rights (same trust model as `Read`/`Write`/`Allocate`).
4. Delegate to the copy engine; on success emit the same mutation event
   `WriteAndFlush` emits, for the **destination** path.

**Copy engine** (new method on the server file-handle type):

- Loop `unix.CopyFileRange(srcFd, &offIn, dstFd, &offOut, remaining, 0)` until
  `length` exhausted or EOF (n == 0).
- Fall back to a server-side `pread`/`pwrite` loop (fixed ~1 MiB buffer)
  **only** on `EXDEV`, `EOPNOTSUPP`, `ENOSYS`. **Not** on `EINVAL` — that
  signals overlapping ranges on the same file and must propagate; the fallback
  path performs its own same-inode overlap check and returns `EINVAL` itself.
- Short copies are legal; return `bytes_copied` as-is, callers loop.
- Both branches keep the data entirely on the server.

**`controller/file.go` — `Lseek`:** validate
`whence ∈ {SEEK_DATA, SEEK_HOLE}` → else `EINVAL` (the kernel resolves
SET/CUR/END itself and never sends them); then `unix.Seek(fd, offset, whence)`.
Safe on shared fds: `Read`/`Write` are `pread`/`pwrite`-based, and each
`lseek(2)` atomically returns its result — no dependence on fd offset state.
`ENXIO` (no data/hole past offset) propagates; it is functional, not an error.

**`controller/fs.go` — xattr writes:** follow `Chmod`'s shape:
`resolveSession` → namespace policy check → `BindIdentity` →
`withIdempotency(request_id)` (Set/Remove) → the existing
`ConfinedLoopbackFileSystem.{Set,Remove,List}XAttr` (`confined.go`) → emit the
attr-mutation event (Set/Remove).

**Namespace policy** (controller layer — policy, not mechanism): writes are
allowed only for `user.*`, `system.posix_acl_access`,
`system.posix_acl_default`; anything else → `EPERM`. Rationale: the server may
run privileged and `setfsuid` does not drop capabilities, so clients must not
plant `trusted.*` or `security.capability` xattrs on server-side files. Reads
(`Get`/`List`) stay unfiltered, as today.

**Errno transport:** xattr replies embed `int32 status` like `GetXAttrReply`;
`CopyFileRange`/`Lseek` mirror `Allocate`'s convention exactly.

## Client

**`io.FileSystemBackend` + `BackendClient`** gain five methods:

- `CopyFileRange(ctx, fhIn, offIn, fhOut, offOut, length, flags) (uint64, fuse.Status)`
  — fd-level, `ioCtx` deadline (same class as `Allocate`/`Fsync`). A single
  FUSE copy op is kernel-capped below 4 GiB; large `cp`s arrive as a sequence
  of calls, each with its own deadline.
- `Lseek(ctx, fh, offset, whence) (uint64, fuse.Status)` — fd-level, `ioCtx`.
- `SetXAttr`/`RemoveXAttr` via the `mutatePath` + `request_id` helper, and
  `ListXAttr` — path-level, `metaCtx`.

**Cache layer (`cachedBackend`)** — the cache is write-through, so there is no
client-side dirty data to reconcile:

- `CopyFileRange`: pass through; on success invalidate destination attr
  (size/mtime) and data-cache range `[off_out, off_out+copied)` — the `Write`
  pattern. Source untouched (atime only).
- `Lseek`: pure pass-through.
- Xattr ops: pass-through (`GetXAttr` already is); xattrs do not affect `stat`,
  so the attr cache is untouched.

**Node layer (`io/node.go`):**

- `gMountieNode` implements `fs.NodeCopyFileRanger`: assert both `FileHandle`s
  to `*gMountieFile` (failed assert → `EBADF`; same-bridge means same mount, so
  foreign handles cannot occur), delegate to the backend, cap the return at
  `uint32` (the 32-bit opcode's reply width; the kernel caps the request).
- `fs.NodeLseeker` via the file handle.
- `fs.NodeSetxattrer`/`NodeRemovexattrer`/`NodeListxattrer` on **both**
  `gMountieRoot` and `gMountieNode` (mirroring `Getxattr`). `Listxattr` follows
  the go-fuse buffer contract: NUL-joined names, return needed size; the bridge
  handles `ERANGE`.
- New assertions in the `var _` block.

## Error mapping

Less-common errnos that must round-trip:

| Errno | Op | Meaning |
|---|---|---|
| `ENXIO` | Lseek | no more data/hole past offset (functional) |
| `EINVAL` | Copy | overlapping ranges / nonzero flags; Lseek: bad whence |
| `ENODATA` | xattr | attr absent (already handled for Get) |
| `ERANGE` | ListXAttr | caller buffer too small (client-side, go-fuse contract) |
| `EPERM` | Set/RemoveXAttr | namespace policy rejection |
| `EEXIST` | SetXAttr | `XATTR_CREATE` on existing attr |
| `EBADF` | Copy | handle not owned by session / wrong type |

## Testing

1. **Server FS unit** (`pkg/server/io/`): copy engine — reflink-capable path,
   `EXDEV`/`EOPNOTSUPP` fallback, same-file overlap → `EINVAL`, short copy at
   EOF; `Lseek` over a punched sparse file (`SEEK_DATA`/`SEEK_HOLE`/`ENXIO`);
   xattr writes extend `confined_xattr_test.go`.
2. **Controller unit** (`pkg/server/controller/`): handle-ownership rejection
   (`EBADF`), flags/whence validation, namespace policy (`user.*` ok,
   `trusted.*`/`security.*` → `EPERM`), idempotent replay of Set/RemoveXAttr,
   mutation-event emission for copy destination and xattr writes.
3. **E2E, real mount** (`test/e2e/fs/`): `unix.CopyFileRange` on mounted files
   → content identical **and no `Read` stream reaches the client** (the
   regression this design exists to fix); sparse `cp` via `SEEK_DATA`; xattr
   set/list/remove through the mount; `node_test.go` mock-backend coverage for
   the five new node methods.

## Non-goals

- Hard links (`link`), `mknod`, `FICLONE`/reflink ioctls.
- Upstream go-fuse `COPY_FILE_RANGE_64` handler (tracked separately; the
  once-per-mount log line persists until then).
- Cross-version compatibility testing; feature negotiation.
- Filtering xattr *reads*.
