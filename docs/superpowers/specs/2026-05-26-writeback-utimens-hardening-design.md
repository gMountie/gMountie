# Writeback Hardening: Utimens RPC + Close-Tail Error Coverage — Design

**Date:** 2026-05-26
**Status:** approved design, pending spec review → implementation plan
**Branch:** `worktree-writeback-utimens` (off `origin/master`)
**Scope:** add an `Utimens` RPC end-to-end (wire → server → client → cache →
FUSE node) and two e2e tests hardening the writeback opt-in shipped in SP4-A.
No fd-model change; no readahead/read-path change (that is SP5).

---

## Goal

Close the timestamp-correctness gap in the FUSE node: `setattrAt` currently
honors size/mode/uid/gid but **silently drops `FATTR_ATIME`/`FATTR_MTIME`**, so
`utimensat(2)` / `touch` against a gMountie mount is a no-op that the kernel
believes succeeded. Add an `Utimens` RPC so atime/mtime changes persist to the
server's backing file. Then harden the SP4-A writeback opt-in with two e2e
tests: that timestamps persist, and that a write failure surfaces as the
`close()` errno.

## Motivation

Two independent reasons:

1. **Explicit timestamp ops (unambiguous).** `touch`, `cp -p`, `rsync
   --times`, build tools, and tar extraction all call `utimensat`. Today these
   silently no-op on a gMountie mount — the file's mtime never changes on the
   server. This is a correctness bug regardless of writeback.

2. **Writeback-mode mtime (to be confirmed by probe).** Under
   `CAP_WRITEBACK_CACHE` the kernel owns size/mtime for cached files and *may*
   push `FATTR_MTIME` to the FS on flush. Whether the Linux kernel actually
   sets `FATTR_MTIME` (vs only `FATTR_SIZE`) on a writeback flush is verified
   by a probe (first implementation step) — it changes only the spec's framing,
   not the code. Either way, honoring `FATTR_MTIME` is the correct behavior.

## Non-goals (YAGNI)

- **No multi-client close-to-open test.** That exercises the subscribe/validity
  layer (unchanged by this work) and needs a two-mount e2e harness that does
  not exist yet. Deferred to a separate spec.
- **No server-clock `UTIME_NOW`.** go-fuse's `SetAttrIn.GetMTime()` /
  `GetATime()` already resolve `FATTR_*_NOW` to `time.Now()` on the client
  (verified: `fuse/types.go:196-222`). We use the resolved concrete time. A
  tri-state wire field for server-side "now" is unnecessary complexity.
- **No atime filtering.** atime is passed straight through to the backing FS.
  If the backing FS is mounted `noatime`/`relatime`, the OS drops it — that is
  the operator's choice and correct. The kernel only sends `FATTR_ATIME` when
  userspace explicitly sets it, so there is no read-amplification risk.
- **No SP5 read-path work.** Separate effort.

## Architecture

The change mirrors the existing path-based metadata-op pattern
(`Chmod` / `Chown` / `Truncate`) at every layer. Each of those is a unary
RPC carrying `volume`, `caller`, `path`, a value, `request_id`, `session_id`,
returning `status`; the server resolves the volume, applies the op under
`withIdempotency`, and emits a `MUTATED` event. `Utimens` follows that shape
exactly, differing only in carrying two optional timestamps.

### Layer 1 — Wire (`api/proto/fs.proto`)

```proto
// FileTime is an optional absolute timestamp. Field types match Attr's
// mtime/mtimensec so conversions are lossless. A nil FileTime message means
// UTIME_OMIT — leave that timestamp unchanged.
message FileTime {
  uint64 sec  = 1;
  uint32 nsec = 2;
}

message UtimensRequest {
  string   volume     = 1;
  Caller   caller     = 2;
  string   path       = 3;
  FileTime atime      = 4;   // nil => leave atime unchanged (UTIME_OMIT)
  FileTime mtime      = 5;   // nil => leave mtime unchanged (UTIME_OMIT)
  string   request_id = 6;
  string   session_id = 7;
}

message UtimensReply {
  int32 status = 1;
}
```

Add to `service RpcFs`:

```proto
rpc Utimens (UtimensRequest) returns (UtimensReply);
```

Regenerate stubs with `task gen:grpc`.

**Optionality choice:** proto3 message presence (nil pointer) cleanly expresses
"omit this timestamp." This matches `utimensat`'s `UTIME_OMIT` per-field
semantics — a caller can set mtime while leaving atime untouched, and vice
versa.

### Layer 2 — Server (`pkg/server/controller/fs.go`)

`Utimens` handler is structurally identical to `Chmod`:

```go
func (r *RpcServerImpl) Utimens(ctx context.Context, request *proto.UtimensRequest) (*proto.UtimensReply, error) {
    sess, err := resolveSession(r.sessions, request.SessionId)
    if err != nil {
        return nil, err
    }
    fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
    if err != nil {
        return nil, err
    }
    return withIdempotency(sess, request.RequestId, func() (*proto.UtimensReply, error) {
        atime := fileTimeToTime(request.Atime) // nil-safe: returns nil for nil input
        mtime := fileTimeToTime(request.Mtime)
        s := fs.Utimens(request.Path, atime, mtime, createContext(ctx, request.Caller))
        if s == fuse.OK {
            r.bus.Emit(request.Volume, request.Path, r.versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
        }
        return &proto.UtimensReply{Status: int32(s)}, nil
    })
}
```

`fs.Utimens` already exists: `pathfs.NewLoopbackFileSystem` implements
`Utimens(name string, Atime, Mtime *time.Time, context *fuse.Context) fuse.Status`,
and `AssumeUserMiddleware` already forwards it (`pkg/server/io/middleware/asume_user.go:98`).

`fileTimeToTime` is a small helper (server-side):

```go
func fileTimeToTime(ft *proto.FileTime) *time.Time {
    if ft == nil {
        return nil
    }
    t := time.Unix(int64(ft.Sec), int64(ft.Nsec))
    return &t
}
```

### Layer 3 — Client backend (`pkg/client/io/backend_grpc.go`)

`BackendClient.Utimens` mirrors `Chmod`, wrapped in `retryableCall`:

```go
func (b *BackendClient) Utimens(ctx context.Context, path string, atime, mtime *time.Time) fuse.Status {
    ctx2, cancel := b.callCtx(ctx)
    defer cancel()
    res, err := retryableCall(ctx2, "Utimens", func(ctx context.Context) (*proto.UtimensReply, error) {
        return b.fs.Utimens(ctx, &proto.UtimensRequest{
            Volume:    b.volume,
            Caller:    b.caller(),
            Path:      path,
            Atime:     timeToFileTime(atime), // nil-safe
            Mtime:     timeToFileTime(mtime),
            SessionId: b.sessionID(),
            RequestId: newRequestID(),
        })
    })
    if err != nil {
        return errToStatus(err)
    }
    return fuse.Status(res.Status)
}
```

`timeToFileTime` (client-side helper):

```go
func timeToFileTime(t *time.Time) *proto.FileTime {
    if t == nil {
        return nil
    }
    return &proto.FileTime{Sec: uint64(t.Unix()), Nsec: uint32(t.Nanosecond())}
}
```

The exact field accessors (`b.caller()`, `b.sessionID()`, `b.callCtx`,
`newRequestID`, `errToStatus`) follow whatever `Chmod`/`Chown` use in the
current file — the implementer mirrors the sibling method rather than the names
guessed here.

### Layer 4 — Cache decorator (`pkg/client/cache/backend.go`)

`cachedBackend.Utimens` mirrors `Chmod`: apply, then invalidate the attr slice
on success (mtime/atime change the attributes, hence the cached `Attr` and its
version):

```go
func (b *cachedBackend) Utimens(ctx context.Context, p string, atime, mtime *time.Time) fuse.Status {
    st := b.inner.Utimens(ctx, p, atime, mtime)
    if st != fuse.OK {
        return st
    }
    b.attr.invalidate(p)
    return fuse.OK
}
```

### Layer 5 — Backend interface (`pkg/client/io/backend.go`)

Add to `FileSystemBackend`, next to `Chown`:

```go
// Utimens sets atime and/or mtime. A nil pointer leaves that timestamp
// unchanged (UTIME_OMIT semantics).
Utimens(ctx context.Context, path string, atime, mtime *time.Time) fuse.Status
```

### Layer 6 — FUSE node (`pkg/client/io/node.go`, `setattrAt`)

After the chown block, before the final `Stat`, dispatch the timestamp bits:

```go
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
```

Update the `setattrAt` doc comment: it currently says atime/mtime "are silently
no-op'd (same as the legacy code)" — that line is now wrong and must be
replaced with a description of the `Utimens` dispatch and `UTIME_OMIT`
handling. `in.GetATime()`/`GetMTime()` return the resolved concrete time
(including `UTIME_NOW` → `time.Now()`); `ok=false` means the bit was unset
(`UTIME_OMIT`).

### Layer 7 — Mocks

`task gen:mocks` regenerates `internal/mocks` after the interface and proto
changes. Do not hand-edit generated files.

## Data flow

```
touch / cp -p / utimensat(UTIME_NOW|value|UTIME_OMIT)
  → kernel FUSE SETATTR (FATTR_ATIME / FATTR_MTIME [+ _NOW], NOW resolved)
  → gMountieNode.Setattr → setattrAt
  → in.GetATime()/GetMTime()  (concrete time or omit)
  → backend.Utimens (cachedBackend: invalidate attr on success)
  → BackendClient.Utimens (retryableCall)
  → Utimens RPC
  → server handler → loopback fs.Utimens → utimensat on backing file
  → bus.Emit(KindMutated): version bump + subscriber notify
```

## Error handling

- **Transient RPC failure** (Unavailable / DeadlineExceeded): retried by
  `retryableCall`, same budget as other metadata ops; final failure maps to a
  fuse status → errno at the syscall.
- **Backing-FS errno** (EPERM, ENOENT, EROFS): returned as `status` in
  `UtimensReply`, surfaced as the `utimensat` errno to the app.
- **Idempotency:** `request_id` under `withIdempotency` makes a retried
  `Utimens` a no-op replay (returns the cached reply), matching Chmod/Chown.
  Setting a timestamp is naturally idempotent, so this is belt-and-suspenders.
- **Writeback close-tail:** write errors (e.g. `ENOSPC`) are not surfaced by
  this RPC — they ride the existing `Flush`/`WriteAndFlush` close-tail path
  (SP3). The error-at-close test verifies that path returns the errno from
  `close()`; it does not touch `Utimens`.

## Testing

### Unit (testify suites, mirroring sibling op tests)

- **`backend_grpc` Utimens:** mock `RpcFs`; assert the request carries the
  right `FileTime` values; assert nil atime/mtime → nil `FileTime` field
  (UTIME_OMIT); assert a non-OK reply status maps to the right fuse status.
  Per the test-protective-property guidance, also assert the retry property
  (ctx not pre-cancelled) rather than guessing retry-go's error wrapping.
- **Cache Utimens:** assert inner is called and, on OK, the attr slice for the
  path is invalidated; on non-OK, no invalidation.
- **`setattrAt` dispatch:** with a mock backend, an `in` carrying `FATTR_MTIME`
  drives exactly one `Utimens` call with the right mtime pointer and a nil
  atime; `FATTR_ATIME|FATTR_MTIME` drives both pointers; neither bit set drives
  no `Utimens` call.

### e2e (`test/e2e/fs`, real FUSE — runs on the kubevirt VM, not the sandbox)

- **Utimens persistence:** mount a volume; `os.Chtimes(path, atime, mtime)`
  with a fixed past time (e.g. 2020-01-01); `Stat` and assert mtime matches.
  Then prove server-side persistence (not just a cache hit): re-`Stat` through a
  fresh client/mount of the same backing dir (sibling `SingleVolumeMounter`),
  asserting the mtime still matches. This catches a regression where the value
  is cached client-side but never written to the backing file.
- **Error-at-close (ENOSPC):** put the server's backing dir on a small tmpfs
  (≈2 MiB, mounted in the test's VM setup); mount the client with
  `writeback_cache: true`; open a file and write past tmpfs capacity; on
  `Close()` assert the error is `ENOSPC` (`syscall.ENOSPC`). This pins that a
  deferred/cached write failure reaches the application via the close-tail
  flush rather than being silently lost. **Probe note:** confirm the failure
  mode before asserting — with writeback the kernel may buffer the data
  (`Write` succeeds) and the server-side write hit `ENOSPC` only at flush; the
  test asserts the errno surfaces at `Close()`, which is exactly the property
  under test.
- The existing SP4-A `WritebackSuite` write-then-read + truncate tests remain.

### Probe (first implementation step, throwaway)

On the VM: mount with `writeback_cache: true`, write to a file, unmount, and
log the `SetAttrIn.Valid` bits the kernel sent on flush. If `FATTR_MTIME` is
set, the writeback-mode motivation (section 2) holds as written; if only
`FATTR_SIZE`, soften the spec's writeback framing to "explicit utimensat
correctness" only. Code is identical either way; this only informs doc wording.

## Files touched

- `api/proto/fs.proto` — `FileTime`, `UtimensRequest`, `UtimensReply`, rpc.
- `pkg/proto/*` — regenerated (`task gen:grpc`).
- `pkg/server/controller/fs.go` — `Utimens` handler + `fileTimeToTime`.
- `pkg/client/io/backend.go` — interface method.
- `pkg/client/io/backend_grpc.go` — `Utimens` + `timeToFileTime`.
- `pkg/client/cache/backend.go` — `cachedBackend.Utimens`.
- `pkg/client/io/node.go` — `setattrAt` dispatch + comment fix.
- `internal/mocks/*` — regenerated (`task gen:mocks`).
- `test/e2e/fs/` — Utimens persistence + error-at-close tests (and tmpfs
  backing-dir setup helper).
- unit tests alongside each touched package.

## Risks / consistency

- **Version bump on timestamp change.** `VersionFromAttr(mtime, size, ctime)`
  means an mtime change bumps the version, correctly invalidating other
  clients' cached attrs via the subscribe layer. Consistent with Truncate.
- **Writeback + explicit touch ordering.** If an app `touch`es a file while it
  has dirty cached pages, the kernel's flush-mtime and the explicit utimensat
  race at the FS. This is inherent to writeback/close-to-open and not made
  worse here; last writer wins, and the single-writer/close-to-open model
  already documents this. Out of scope to arbitrate.
- **tmpfs in CI.** The error-at-close test needs a tmpfs mount, which requires
  privileges available on the VM but not the sandbox. It is gated to the e2e
  suite that already requires the VM, consistent with the existing FUSE tests.
