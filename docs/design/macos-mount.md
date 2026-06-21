# macOS mount (cgofuse adapter)

gMountie's Linux client mounts via `hanwen/go-fuse`, which talks to `/dev/fuse`
directly and is Linux-only. macOS (and, later, Windows) mount through a
**cgofuse adapter** instead — a second consumer of the same FUSE-independent
backend seam, so everything below the mount is shared and unchanged.

## Architecture

`FileSystemBackend` (`pkg/client/io/backend.go`) is a FUSE-independent interface
(path-keyed metadata, `FileHandle`-keyed I/O). On Linux `gMountieNode`
(`pkg/client/io/node.go`, go-fuse's `fs.Node*`) consumes it; on macOS
`MountieCgoFS` (`pkg/client/io/cgofs/`, cgofuse's `FileSystemInterface`) consumes
the same seam. The gRPC `BackendClient` (with the optional cache decorator)
implements the seam underneath. Nothing below the seam — RPC, cache, identity —
changes between platforms.

```
        Linux                         macOS (now) / Windows (later)
   ┌──────────────┐                      ┌──────────────────┐
   │ go-fuse      │                      │ cgofuse          │
   │ gMountieNode │   ← adapters →       │ MountieCgoFS     │
   │ (fs.Node*)   │                      │ (FileSystemIface)│
   └──────┬───────┘                      └────────┬─────────┘
          └───────────────┬───────────────────────┘
                          ▼
              FileSystemBackend  (unchanged seam)
                          ▼
        cache decorator → BackendClient (gRPC) → server  (unchanged)
```

A build-tagged mounter (`pkg/client/mount/establish_{gofuse,cgofuse}.go`) selects
the implementation per platform behind the one `SingleVolumeMounter` contract, so
CLI callers are platform-agnostic. The cgofuse files are
`//go:build darwin || cgofuse` — the `cgofuse` tag also builds the adapter on
Linux for the benchmark below, but it is **excluded from the Linux production
build**, which stays `CGO_ENABLED=0` on go-fuse.

Two concerns the go-fuse object model gives for free, handled explicitly here:
the adapter maps `fuse.Status` → cgofuse's negative-errno convention, and owns a
mutex-guarded `map[uint64]FileHandle` table (cgofuse hands a `uint64` per open).

## FUSE providers (macFUSE *and* FUSE-T)

cgofuse speaks the libfuse API, so one adapter serves both providers; the
differences are runtime (which dylib loads, which mount options are valid). The
mounter auto-detects the installed provider, **prefers macFUSE** (fuller feature
set), falls back to **FUSE-T** (kextless, NFSv4-backed), and fails with an
actionable install hint when neither is present. Override with
`fuse.provider: macfuse | fuse-t | auto`.

Mount options are provider-specific:

- `volname=<volume>` — names the volume so Finder renders it (both providers).
- `local` — **macFUSE only**; makes Finder treat the mount as a browsable local
  device. This is the fix for "the terminal sees files but Finder doesn't."
  Applied conditionally, because FUSE-T rejects unknown options.
- `noappledouble` — suppresses `._*`/`.DS_Store` chatter.
- Write sizing — the negotiated `maxWrite` must be passed through, or the kernel
  fragments writes into tiny per-op cgo crossings (2–20× slower). macFUSE takes
  `iosize=N`, FUSE-T takes `max_write=N`.

## Identity, locking, and timestamps

- **Identity is unchanged.** The macOS kernel reports the caller's uid/gid; the
  adapter stamps `proto.Caller` exactly as the gRPC backend does. Local macOS
  uids (e.g. 501) map **server-side** via the per-volume mapping mode (squash
  default). The per-thread `setfsuid/setfsgid/setgroups` enforcement is entirely
  server-side and untouched.
- **Byte-range locks are not forwarded on macOS.** cgofuse's high-level
  `FileSystemInterface` exposes no `Getlk/Setlk/Setlkw` callbacks, so POSIX
  advisory locks are handled kernel-locally within the mount only. The Linux
  go-fuse path keeps server-forwarded locking. Revisit only if a real workload
  needs cross-client server-side locking on macOS.
- **`Utimens` parity gap.** The adapter sets both atime and mtime whenever
  timestamps are provided; it does not honour the `UTIME_OMIT`/`UTIME_NOW`
  sentinels the way the Linux `Setattr` does. A macOS `touch -a` may also rewrite
  mtime. Low-frequency; tracked alongside the locking limitation.

## Why Linux stays on go-fuse

The adapter also builds on Linux (`-tags cgofuse`, cgo + libfuse) so it can be
benchmarked head-to-head against go-fuse. The verdict (see
[benchmarks/cgofuse-vs-gofuse.md](benchmarks/cgofuse-vs-gofuse.md)): go-fuse wins
— cgofuse on FUSE2 is ~14% slower (the residual is inherent cgo per-op latency,
not allocations), and FUSE3 is far worse (~3×, reads collapse because its read
size is init-only and cgofuse's high-level API can't set it). cgofuse on darwin
is FUSE2-locked regardless. So: **go-fuse on Linux, cgofuse-FUSE2 on macOS.**

## Build & release

macOS needs cgo (the cgofuse adapter links the FUSE headers), so it cannot be
cross-compiled from Linux. The split:

- **Linux** — `CGO_ENABLED=0`, go-fuse, cross-compiled on the release runner;
  the production artifact is byte-for-byte its previous path. CI guards it with a
  no-cgo build, a libfuse `cgofs-conformance` lane, and the e2e suites (Actions
  Linux runners have `/dev/fuse`).
- **macOS** — `CGO_ENABLED=1`, built on a macOS runner with macFUSE installed
  (CI `macos-build` lane; release `darwin-binaries` job builds both arches,
  arm64 native and amd64 via `clang -arch x86_64`). goreleaser on Linux folds the
  Mac-built archives into the single signed `checksums.txt` and the release via
  `checksum.extra_files`/`release.extra_files` — see
  [operations-and-packaging.md](operations-and-packaging.md). Gatekeeper
  signing/notarisation is a follow-on.

## Not done here

- **Windows (WinFsp).** The adapter is written to be the future Windows path, but
  Windows is not wired or tested.
- **Migrating Linux to cgofuse.** Deferred and, per the benchmark, not worth it.
- **Projection/sync model or an NFS-loopback server.** Rejected: both abandon the
  live-POSIX mount and the kernel-native identity model (NFS's `AUTH_UNIX`
  mismatch, locking/attr-cache gaps) and don't carry to Windows.
