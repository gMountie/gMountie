# macOS mount (go-fuse + cgofuse adapters)

gMountie's Linux client mounts via `hanwen/go-fuse`, which talks to `/dev/fuse`
directly. On macOS the client **also** uses go-fuse where possible — it ships a
cgo-free macFUSE mount path (`fuse/mount_darwin.go` execs `mount_macfuse`) — and
falls back to a **cgofuse adapter** for the kextless FUSE-T backend. Both adapters
consume the same FUSE-independent backend seam, so everything below the mount is
shared and unchanged.

## Architecture

`FileSystemBackend` (`pkg/client/backend/backend.go`) is a FUSE-independent interface
(path-keyed metadata, `FileHandle`-keyed I/O). `gMountieNode`
(`pkg/client/backend/node.go`, go-fuse's `fs.Node*`) consumes it on Linux and on macOS
when macFUSE is installed; `MountieCgoFS` (`pkg/client/backend/cgofs/`, cgofuse's
`FileSystemInterface`) consumes the same seam on macOS when FUSE-T is detected.
The gRPC `BackendClient` (with the optional cache decorator) implements the seam
underneath. Nothing below the seam — RPC, cache, identity — changes between
platforms or adapters.

```
        Linux / macOS+macFUSE                  macOS+FUSE-T
   ┌──────────────────────────┐              ┌──────────────────┐
   │ go-fuse                  │              │ cgofuse          │
   │ gMountieNode             │ ← adapters → │ MountieCgoFS     │
   │ (fs.Node*)               │              │ (FileSystemIface)│
   └──────────┬───────────────┘              └────────┬─────────┘
              └────────────────────┬──────────────────┘
                                   ▼
                       FileSystemBackend  (unchanged seam)
                                   ▼
                 cache decorator → BackendClient (gRPC) → server
```

On darwin the mounter selects the adapter at **runtime** based on the detected
FUSE backend. Two worker functions exist — `establishGoFuse` and `establishCgoFuse`
— and a darwin-specific dispatcher calls `detectProvider` (the existing
`macos_provider.go` logic) to choose between them. On Linux only go-fuse compiles
in (`CGO_ENABLED=0` preserved). The `-tags cgofuse` benchmark build (Linux) still
compiles only cgofuse, unchanged.

Two concerns the go-fuse object model handles for free are handled explicitly in
the cgofuse adapter: it maps `fuse.Status` → cgofuse's negative-errno convention,
and owns a mutex-guarded `map[uint64]FileHandle` table (cgofuse hands a `uint64`
per open). These do not apply to the go-fuse path.

## FUSE providers (macFUSE *and* FUSE-T)

On darwin the client detects the installed FUSE provider at mount time and routes
to the appropriate adapter:

- **macFUSE → go-fuse** (`gMountieNode`, `node.go`). go-fuse v2.10.1 ships a
  cgo-free macFUSE mount path that execs `mount_macfuse` with the osxfuse
  handshake. `node.go` compiles cgo-free on darwin. This is the preferred path for
  end users who install macFUSE: it is faster (see benchmark below) and exposes the
  full `node.go` feature set.
- **FUSE-T → cgofuse** (`MountieCgoFS`, `cgofs/`). FUSE-T is kextless
  (NFSv4-backed) and cannot be served by go-fuse, which only knows the
  `mount_macfuse`/`mount_osxfuse` mechanism. cgofuse is the only option here. This
  is the required path on cloud Macs where a kext cannot load.

The mounter **auto-detects** the installed provider, **prefers macFUSE**, falls
back to **FUSE-T**, and fails with an actionable install hint when neither is
present. Override with `fuse.provider: macfuse | fuse-t | auto`.

Mount options are provider-specific:

- `volname=<volume>` — names the volume so Finder renders it (both providers).
- `local` — **macFUSE only**; makes Finder treat the mount as a browsable local
  device. This is the fix for "the terminal sees files but Finder doesn't."
  Applied conditionally: FUSE-T rejects unknown options, and on go-fuse this is
  passed as a bare string in `MountOptions.Options`.
- `auto_xattr` / `noappledouble` — macOS xattr handling, toggled by
  `fuse.auto_xattr` (default **true** = `auto_xattr`). See
  [Finder copies and macOS xattrs](#finder-copies-and-macos-xattrs-auto_xattr)
  below. On FUSE-T, `noappledouble` is always passed (it suppresses `._*`/
  `.DS_Store` chatter; FUSE-T stores xattrs in its NFS/FSKit backend regardless).
- Write sizing — the negotiated `maxWrite` must be passed through, or the kernel
  fragments writes into tiny per-op crossings (2–20× slower). macFUSE takes
  `iosize=N` (passed via `MountOptions.MaxWrite` on go-fuse; `-o iosize=N` on
  cgofuse). FUSE-T takes `max_write=N` via cgofuse.

## Finder copies and macOS xattrs (SETXATTR flag translation)

**Symptom.** A Finder drag-drop copy into a macFUSE volume fails with the opaque
**"The operation can't be completed because an unexpected error occurred
(error code -50)"**, while `cp`/`ditto` from a terminal succeed. macOS Finder
stamps FinderInfo on *every* copy via `setattrlist(ATTR_CMN_FNDRINFO)` (even when
the source has none); `cp`/`ditto` don't.

**Root cause (a gMountie bug, found via go-fuse op-level debug).** macFUSE
*does* forward that as a `SETXATTR "com.apple.FinderInfo"` to us — and gMountie
returned **EINVAL**. macOS `<sys/xattr.h>` flag values differ from Linux's, and
macOS adds bits Linux has none of: Finder's FinderInfo write carries
**`XATTR_NODEFAULT` (0x10)**. gMountie was passing the macOS flags straight to
the server's `unix.Setxattr`, where `0x10` is an invalid flag → EINVAL → Finder
-50. (The earlier RPC log hid this: the errno rides in the SetXAttr *reply body*,
not the gRPC status.)

**Fix.** Translate macOS SETXATTR flags to Linux on the darwin client before the
wire call (`gofuse/applexattr.go`, `appleXattrFlagsToBackend`): map
`XATTR_CREATE 0x2→0x1` and `XATTR_REPLACE 0x4→0x2`, drop the macOS-only bits
(`NOFOLLOW`/`NOSECURITY`/`NODEFAULT`/`SHOWCOMPRESSION`). darwin-only, like the
`com.apple.*` name remap. With this, Finder copies succeed on the **clean**
`noappledouble` path — `setattrlist(FNDRINFO)` returns 0, FinderInfo
round-trips **server-side**, and there are **no `._` files** (validated
end-to-end with a real Finder copy). No go-fuse change is needed.

**`fuse.auto_xattr` (default false).** An opt-in to mount `auto_xattr` instead of
`noappledouble`, making macFUSE store all xattrs in `._` AppleDouble files at the
kernel layer (the bindfs approach). Only useful for interop with tools that read
`._` files; it brings `._`/.DS_Store clutter and makes xattrs kernel-local rather
than server-side, so it is **not** the default — the flag translation makes the
clean path work without it. (`auto_xattr` and `noappledouble` conflict; the
toggle picks one.) **FUSE-T/fskit** is another clean Finder-working path, at the
cost of slower metadata over WAN.

## FUSE-T backend: NFS vs FSKit

FUSE-T can serve a mount two ways, selected with `-o backend=`:

- **NFS** (default) — FUSE-T runs an in-process NFSv4 server and macOS mounts it
  via the kernel NFS client. Universally available, but the NFS client amplifies
  metadata over a high-RTT link: it polls `statfs` on nearly every operation
  (26 of 39 RPCs in a single `ls`), issues a `GETATTR` after each write while a
  file grows (~28k for a 1 GB copy), and stores xattrs in `._` AppleDouble
  sidecars it opens once per probe. None of this happens on Linux/go-fuse, where
  the kernel FUSE attr cache absorbs it.
- **FSKit** (`backend=fskit`) — FUSE-T's Apple FSKit backend (`FskitSrvModule`
  extension; macOS 15+) mounts natively, not through NFS, and does **not**
  generate that amplification (measured: `ls` 39→4 RPCs, 1 GB write GETATTR
  ~2,600→10). It requires the extension to be installed **and** enabled in
  *System Settings → General → Login Items & Extensions*.

`fuse.fuset_backend` (`auto` default | `nfs` | `fskit`) picks the backend, and
only applies to the FUSE-T path. `auto` prefers FSKit when the module is present
and **falls back to NFS if the FSKit mount fails** (so an installed-but-not-
enabled extension degrades gracefully rather than failing the mount); `fskit`
forces it with a clear error when unavailable; `nfs` forces the NFS backend. The
client cache's StatFs/optimistic-attr mitigations (see
[caching-and-consistency](caching-and-consistency.md)) blunt the NFS backend's
amplification for macs without FSKit.

## Errno correctness

Error codes are correct on both paths via the errno-canonical work (already
merged). The `FileSystemBackend` seam returns `proto.FsError`; `node.go` maps it
with `fserr.ToErrno(st)`, which uses per-GOOS `syscall.E*` — so a darwin build
yields Darwin errno values, which go-fuse returns directly to macFUSE as
`syscall.Errno`. The cgofuse path has the equivalent mapping through
`MountieCgoFS`. `fserr_darwin_test.go` covers the mapping table on a darwin build
without needing a Mac.

## Identity, locking, and timestamps

- **Identity is unchanged.** The macOS kernel reports the caller's uid/gid on both
  adapters; the code stamps `proto.Caller` exactly as the gRPC backend does. Local
  macOS uids (e.g. 501) map **server-side** via the per-volume mapping mode (squash
  default). The per-thread `setfsuid/setfsgid/setgroups` enforcement is entirely
  server-side and untouched.
- **Byte-range locks are not forwarded on the FUSE-T (cgofuse) path.** cgofuse's
  high-level `FileSystemInterface` exposes no `Getlk/Setlk/Setlkw` callbacks, so
  POSIX advisory locks are handled kernel-locally within the mount only. The
  go-fuse path (macFUSE) forwards locking exactly as the Linux path does. Revisit
  only if a real workload needs cross-client server-side locking on FUSE-T.
- **`Utimens` parity gap (FUSE-T/cgofuse path only).** The cgofuse adapter sets
  both atime and mtime whenever timestamps are provided; it does not honour the
  `UTIME_OMIT`/`UTIME_NOW` sentinels the way `node.go`'s `Setattr` does. A macOS
  `touch -a` via FUSE-T may also rewrite mtime. The go-fuse/macFUSE path uses
  `node.go`'s proper handling. Low-frequency; tracked alongside the locking
  limitation on the cgofuse path.
- **Feature divergence.** macFUSE (go-fuse) users get the full adapter: xattr-names
  prime, session reclaim, directio. FUSE-T (cgofuse) users get the `cgofs` subset.
  This is a permanent support-matrix line, accepted because go-fuse is the better
  path and FUSE-T is the kextless fallback.

## Why go-fuse is preferred on macFUSE

The cgofuse adapter also builds on Linux (`-tags cgofuse`, cgo + libfuse) so it
can be benchmarked head-to-head against go-fuse. The verdict (see
[benchmarks/cgofuse-vs-gofuse.md](benchmarks/cgofuse-vs-gofuse.md)): go-fuse wins
— cgofuse on FUSE2 is ~14% slower (the residual is inherent cgo per-op latency,
not allocations), and FUSE3 is far worse (~3×, reads collapse because its read
size is init-only and cgofuse's high-level API can't set it). cgofuse on darwin
is FUSE2-locked regardless. The same advantage applies on macOS: the macFUSE path
routes to go-fuse for the same reason Linux does. FUSE-T (cgofuse) is the
kextless fallback, not the preferred macOS backend.

### Manual macFUSE verification (no CI — runners can't load the kext)

On a Mac with macFUSE installed, using a release build:

1. `gmountie mount -n <vol> /tmp/mnt` — succeeds, no error.
2. `ls -la /tmp/mnt` — lists entries; correct sizes/modes.
3. Read a file (`cat`), write a file (`echo x > /tmp/mnt/f`), re-read it.
4. `xattr -w user.test v /tmp/mnt/f && xattr -l /tmp/mnt/f` — shows `user.test`.
5. Trigger an error (e.g. `rmdir` a non-empty dir) — correct message (ENOTEMPTY), not a garbled errno.
6. **Finder**: the volume appears with its name and is browsable.
7. `umount /tmp/mnt` (or `gmountie` unmount) — clean.

## Build & release

macOS needs cgo because the darwin binary includes the cgofuse/FUSE-T path
(FUSE-T support requires cgo regardless). The go-fuse/macFUSE path itself is
cgo-free, but the shipped binary is a single artifact containing both adapters.
The split:

- **Linux** — `CGO_ENABLED=0`, go-fuse, cross-compiled on the release runner;
  the production artifact is byte-for-byte its previous path. CI guards it with a
  no-cgo build, a libfuse `cgofs-conformance` lane, and the e2e suites (Actions
  Linux runners have `/dev/fuse`).
- **macOS** — `CGO_ENABLED=1`, single binary with both adapters, built on a macOS
  runner with macFUSE installed (CI `macos-build` lane; release `darwin-binaries`
  job builds both arches, arm64 native and amd64 via `clang -arch x86_64`).
  goreleaser on Linux folds the Mac-built archives into the single signed
  `checksums.txt` and the release via `checksum.extra_files`/`release.extra_files`
  — see [operations-and-packaging.md](operations-and-packaging.md). Gatekeeper
  signing/notarisation is a follow-on.

## Not done here

- **Windows (WinFsp).** The cgofuse adapter is written to be the future Windows
  path, but Windows is not wired or tested.
- **Migrating Linux to cgofuse.** Deferred and, per the benchmark, not worth it.
- **Feature parity on cgofs** (e.g. giving FUSE-T the xattr prime). Separate,
  out of scope.
- **Projection/sync model or an NFS-loopback server.** Rejected: both abandon the
  live-POSIX mount and the kernel-native identity model (NFS's `AUTH_UNIX`
  mismatch, locking/attr-cache gaps) and don't carry to Windows.
