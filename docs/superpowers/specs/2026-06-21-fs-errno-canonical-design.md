# Canonical FS/file error codes on the wire — design

- **Date:** 2026-06-21
- **Status:** Approved design, pre-implementation
- **Scope:** OSS core repo (`gMountie/`) — the `RpcFs`/`RpcFile` errno path only
- **Branch / worktree:** `worktree-fs-errno-canonical`

## Problem

FS/file RPC replies carry their error as a raw `int32 status` field
(`api/proto/fs.proto`, `api/proto/file.proto`). The server fills it with
`int32(fuse.Status)` (`pkg/server/controller/fs.go`), i.e. a **Linux errno
number** — go-fuse's `fuse.Status` is Linux-numbered, and the server's loopback
FS returns Linux `syscall.Errno`. On the client, `BackendClient`
(`pkg/client/io/backend_grpc.go`) rewraps that int back into a `fuse.Status` and
hands it to the mount adapter.

This is correct for the go-fuse (Linux) adapter — a Linux errno going to a Linux
kernel. It is **wrong for the cgofuse (macOS) adapter**, whose `errc`
(`pkg/client/io/cgofs/status.go`) simply negates the value:

```go
func errc(st fuse.Status) int { if st == fuse.OK { return 0 }; return -int(st) }
```

So a Linux `ENOTEMPTY` (39) is returned to the macFUSE kernel as Darwin errno 39
(`EDESTADDRREQ`), not Darwin's `ENOTEMPTY` (66). Errno **numbers are
OS-specific**, and the codes diverge across exactly the cases a filesystem hits:
`ENOTEMPTY` 39 vs 66, `ENOATTR`/`ENODATA` for a missing xattr (Linux `ENODATA`
61; Darwin `ENOATTR` 93 — different name *and* number), `EAGAIN` 11 vs 35, and
many more. The adapter's inline `-int(fuse.EBADF)` returns in `Read`/`Write`/etc.
are the same Linux-ism. Net effect: macFUSE surfaces wrong/garbage errors;
FUSE-T (NFSv4-backed) masks some of it, which is why early FUSE-T testing didn't
expose it.

Underlying issue: **the wire embeds one OS's errno numbering.** Today that OS is
Linux because the server is Linux-only, but baking "the wire is Linux-numbered"
into the protocol is fragile — we do not want to assume the server is always
Linux, and we now have a non-Linux client.

## Decision

Stop putting any OS's raw errno on the wire. Define a **canonical, OS-neutral
error enum** (`FsError`) and have each endpoint map only between *its own* native
errno and the enum:

- **Server:** native FS errno → `FsError` when building the reply.
- **Wire:** carries `FsError` (replaces the raw-errno meaning of `status`).
- **Client:** `FsError` → the host kernel's errno, in the platform mount adapter.

Each endpoint owns exactly **one** mapping — its native errno ↔ the enum. No
endpoint knows any other OS's numbering, so it scales to any server OS × any
client OS with no translation matrix. This is the approach mature
filesystem/RPC protocols take (NFS `NFSERR_*`, 9P, gRPC canonical codes).

### Alternatives considered and rejected

- **Client reports its OS at session start; server translates errno to the
  client's OS.** Puts cross-OS knowledge on the *server* (it would need Darwin,
  Windows, … errno tables) and makes the wire carry *the client's* OS numbering,
  so a reply's meaning depends on who's connected. It's an N-clients × M-servers
  matrix and still bakes an OS assumption in — just a relocated one. Rejected.
- **Client-only hotfix: make `errc` a `fuse.Status → cgofuse.E*` table** (no
  proto/server change). Fixes the live bug today, but it *bakes in* "the wire is
  Linux-numbered" — the very assumption we want gone. Kept in our back pocket as
  a stopgap only; not the design.
- **Reuse gRPC's canonical status codes.** Too coarse for a filesystem: a FUSE
  client must distinguish `ENOTEMPTY`/`EEXIST`/`ENOSPC`/`EDQUOT`/`EXDEV`, which
  gRPC's ~16 codes collapse. We define our own FS-specific set instead.
- **Make the canonical values "Linux errno numbers, relabelled."** Zero server
  change today (go-fuse already emits them), but it privileges Linux and tempts
  callers to skip the mapping. Since we control both ends and are touching this
  now, we spend the proto change and make the enum genuinely OS-neutral.

## Architecture

```
  server-native errno ──► FsError ──► [wire] ──► FsError ──► host errno
  (Linux syscall.Errno /        (canonical, OS-neutral)        (per adapter)
   fuse.Status today)                                   go-fuse → Linux errno
                                                        cgofuse → Darwin/Win errno
```

Layers and touch points:

1. **Proto (`api/proto/fs.proto`, `file.proto`).** Replace the raw-errno meaning
   of the reply `status` fields with `FsError`. `FsError` is a proto `enum` whose
   `0` value is `FS_OK` (so "unset/zero = success" still holds). Regenerate stubs
   (`task gen:grpc`).
2. **Server (`pkg/server/controller/*.go`).** A `toFsError(syscall.Errno|fuse.Status)
   FsError` table replaces the `int32(status)` casts. Unmapped codes → a generic
   `FS_EIO` (logged once at debug so we can spot gaps).
3. **Client seam (`pkg/client/io/`).** This is the larger mechanical change and
   the one judgement call — see "Seam representation" below.
4. **Mount adapters.**
   - go-fuse (`pkg/client/io/node.go`): `FsError` → `fuse.Status` (Linux errno).
   - cgofuse (`pkg/client/io/cgofs/`): `FsError` → cgofuse's **host-correct**
     errno constants (cgofuse defines `ENOENT`/`ENOTEMPTY`/… from `<errno.h>` per
     build platform), so Darwin (and Windows later) come out right for free. This
     also replaces the inline `-int(fuse.EBADF)` returns.

### The `FsError` enum (scope)

The bounded set of errno the FS/file ops realistically emit, plus a catch-all.
Initial set (extend if a gap is found):

`FS_OK`, `FS_EPERM`, `FS_ENOENT`, `FS_EIO`, `FS_ENXIO`, `FS_EBADF`, `FS_EAGAIN`,
`FS_EACCES`, `FS_EBUSY`, `FS_EEXIST`, `FS_EXDEV`, `FS_ENOTDIR`, `FS_EISDIR`,
`FS_EINVAL`, `FS_EMFILE`, `FS_ENFILE`, `FS_EFBIG`, `FS_ENOSPC`, `FS_EROFS`,
`FS_EMLINK`, `FS_ERANGE`, `FS_ENAMETOOLONG`, `FS_ENOSYS`, `FS_ENOTEMPTY`,
`FS_ELOOP`, `FS_EOVERFLOW`, `FS_EDQUOT`, `FS_ESTALE`, `FS_ENOTSUP`,
`FS_ENO_XATTR` (Linux `ENODATA` ↔ Darwin `ENOATTR`), `FS_EINTR`, `FS_ETXTBSY`.

`FS_ENO_XATTR` is the headline case the raw-int path gets wrong: it maps to
`ENODATA` on a Linux kernel and `ENOATTR` on a Darwin kernel — same concept,
different number, which only a canonical name can carry.

### Seam representation (the one design choice)

`FileSystemBackend` (the FUSE-independent seam, `pkg/client/io/backend.go`) has
**30 methods returning `fuse.Status`** — itself a Linux-flavoured type the
existing design notes flagged for neutralising. Two ways to land the client side:

- **(A, recommended) Neutralise the seam.** Change the seam to return the
  canonical `io.FsError` (a Go mirror of the proto enum). `BackendClient` returns
  `FsError` straight from the wire; each adapter maps `FsError → its kernel`.
  Cleanest and fully OS-neutral end-to-end, no Linux hop. Cost: a mechanical
  sweep across the 30 signatures, the cache decorator, both adapters, the
  generated mocks (`task gen:mocks`), and tests.
- **(B, smaller) Keep the seam as `fuse.Status`.** `BackendClient` maps
  `FsError → fuse.Status` (Linux errno); go-fuse adapter unchanged; only the
  cgofuse adapter maps `fuse.Status → host errno`. Fixes the bug and neutralises
  the *wire*, but keeps a Linux-numbered client-internal hop (and the cgofuse
  adapter still embeds Linux→host knowledge).

Recommend **(A)** — it's the same blast radius we'd eventually pay for the
flagged seam cleanup, and it removes the Linux-ism the principle is about. The
plan will isolate the signature sweep into its own task so review stays tractable.

## Wire compatibility

This is a **breaking wire change** (the `status` field's value space changes).
Per project policy we control both ends and don't design for backwards
compatibility; the OSS client and server release together and the cloud bumps
its OSS dep in lockstep. Document the break in the release notes. No transitional
dual-encoding.

## Testing

- **Enum mapping (table-driven, both directions, runs anywhere):** `toFsError`
  (server) and `FsError → fuse.Status` / `FsError → cgofuse errno` (client) round-
  trip every enum value; assert the divergent cases explicitly (`ENOTEMPTY` →
  66 on Darwin, `FS_ENO_XATTR` → `ENOATTR` on Darwin / `ENODATA` on Linux,
  `EAGAIN` → 35 on Darwin). The Darwin-target assertions compile under the
  `darwin`/`cgofuse` tag against cgofuse's constants.
- **Adapter conformance:** the existing cgofs conformance suite asserts a backend
  error surfaces as the correct host errc.
- **Regression:** the go-fuse path's errno behaviour is unchanged (Linux errno in,
  Linux errno out); existing e2e stays green.

## Non-goals

- **Control-plane RPCs** (`SessionService`, `VolumeService`, `VersionService`).
  They report errors via gRPC status codes, which are fine for them — out of
  scope. This is the FS/file errno path only.
- **Windows (WinFsp)** errno specifics — the enum is written to extend there, but
  Windows is not wired here.
- **Migrating Linux to cgofuse** — unrelated; unchanged.

## Success criteria

- A backend `ENOTEMPTY`/missing-xattr/`EAGAIN`/etc. surfaces with the **correct
  host errno** on macFUSE (verified on the cloud Mac), not a Linux number.
- The wire `status` carries OS-neutral `FsError`; neither the server nor the wire
  assumes Linux. A future non-Linux server maps its own native errno to the enum
  with no client change.
- The go-fuse/Linux path is behaviourally unchanged.
