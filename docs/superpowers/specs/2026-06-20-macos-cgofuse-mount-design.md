# macOS mount via cgofuse — design

- **Date:** 2026-06-20
- **Status:** Approved design, pre-implementation
- **Scope:** OSS core repo (`gMountie/`)
- **Branch / worktree:** `worktree-macos-cgofuse`

## Problem

gMountie's only mount path is `go-fuse` (hanwen/go-fuse), which talks to the Linux
`/dev/fuse` character device directly. That is Linux-only. Early macOS testing (a friend
on a Mac) surfaced two issues:

1. **Finder cannot see files that the terminal can** — a known macFUSE behaviour, not a
   gMountie bug: the volume is not tagged `local` and has no volume name, so Finder refuses
   to enumerate it.
2. **`deadline exceeded` on the resolve step** — a cloud-resolver timeout/retry issue,
   **unrelated to the FUSE binding**. Tracked separately (see Non-goals); an existing
   `raise-resolve-timeout` worktree already addresses it.

Separately, **Windows is on the roadmap**, and Windows has no in-kernel FUSE at all.

We need a macOS mount path now, chosen so it also generalises to Windows later.

## Decision

Add a **cgofuse-based filesystem adapter** that runs against the OS's FUSE driver
(macFUSE *or* FUSE-T on macOS; WinFsp on Windows later). cgofuse speaks the libfuse API, so
a single adapter works against macFUSE and FUSE-T interchangeably.

This keeps gMountie a **true live-POSIX mount** with the existing server-side identity model
intact, and **reuses everything below the adapter unchanged**.

### Alternatives considered and rejected

- **NFS-loopback** (pure-Go NFSv3 server on localhost, à la rclone/FUSE-T): no install, no
  cgo, but it is a shim that fights gMountie's design — `AUTH_UNIX` identity mismatch vs the
  kernel-native per-thread model, NFSv3 locking gap, attribute-cache staleness (a class of
  bug we've already been bitten by), and it does **not** carry over to Windows (Windows' NFS
  client is weak; we'd build a second SMB shim). Rejected: we'd rather not own a filesystem
  protocol implementation.
- **Native projection** (macOS File Provider + Windows Cloud Filter API): most native UX and
  no install, but it converts gMountie from a live mount into a Files-On-Demand *sync
  product* — Swift extension + signed/notarised app bundle + sandbox IPC on macOS, a C
  sync-engine on Windows, two sync state machines, and a loss of POSIX fidelity + the
  identity model. Collides with the cloud ML-GPU-pod streaming use case. Rejected as a
  multi-month product pivot, not a mount.
- **Unify all platforms on cgofuse now**: deferred — see "Linux stays on go-fuse".

## Architecture

`FileSystemBackend` (`pkg/client/io/backend.go`) is a **FUSE-independent** interface
(path-keyed metadata, `FileHandle`-keyed I/O). Today `gMountieNode`
(`pkg/client/io/node.go`, implementing go-fuse's `fs.NodeXxx`) consumes it; the gRPC
`BackendClient` (`pkg/client/io/backend_grpc.go`) implements it underneath, with an optional
cache decorator. The cgofuse adapter becomes a **second consumer of the same seam** — a
sibling of `gMountieNode`. Nothing below the seam changes (RPC, cache, identity all untouched).

```
        Linux                         macOS (now) / Windows (later)
   ┌──────────────┐                      ┌──────────────────┐
   │ go-fuse      │                      │ cgofuse          │
   │ gMountieNode │   ← adapters →       │ MountieCgoFS     │
   │ (fs.NodeXxx) │                      │ (FileSystemIface)│
   └──────┬───────┘                      └────────┬─────────┘
          └───────────────┬───────────────────────┘
                          ▼
              FileSystemBackend  (unchanged seam)
                          ▼
        cache decorator → BackendClient (gRPC) → server  (unchanged)
```

### New package

`pkg/client/io/cgofs/` containing `MountieCgoFS`, which embeds cgofuse's `fuse.FileSystemBase`
and implements the supported ops by delegating to `FileSystemBackend`:

| cgofuse op | FileSystemBackend call |
|---|---|
| `Getattr` | `Stat` |
| `Readdir` | `ListDir` |
| `Open` / `Create` | `Open` / `Create` → `FileHandle` |
| `Read` / `Write` | `Read` / `Write` |
| `Release` / `Flush` / `Fsync` | same |
| `Mkdir` / `Rmdir` / `Unlink` / `Rename` | same |
| `Readlink` / `Symlink` | same |
| `Setxattr` / `Getxattr` / `Listxattr` / `Removexattr` | `*XAttr` |
| `Truncate` / `Chmod` / `Chown` / `Utimens` | `SetAttr` |
| `Statfs` | `StatFs` |
| `Getlk` / `Setlk` / `Setlkw` | `GetLk` / `SetLk` / `SetLkw` |

### Two adapter-specific concerns

1. **Status mapping.** `FileSystemBackend` returns `fuse.Status` (a go-fuse type). The
   adapter maps `fuse.Status` → cgofuse's negative-errno convention (e.g. `-fuse.ENOENT`) in
   a small `status.go` table. Mechanical. (A future refactor could neutralise the seam to
   return `syscall.Errno`; out of scope here — see Open questions.)
2. **File-handle model.** cgofuse hands a `uint64` handle per open; go-fuse hands a
   `FileHandle` object. The adapter owns a `map[uint64]FileHandle` (mutex-guarded handle
   table), allocating an id on `Open`/`Create` and resolving it on
   `Read`/`Write`/`Release`/etc. `gMountieNode` gets this for free from go-fuse's object
   model; the cgofuse adapter manages it explicitly.

### Mounter wiring

A build-tagged factory produces the same `SingleVolumeMounter` contract
(`pkg/client/mount/single.go`) regardless of platform:

- `mounter_linux.go` → go-fuse mounter (`gofs.Mount`), as today.
- `mounter_darwin.go` → cgofuse mounter (`cgofuse.FileSystemHost.Mount`).

CLI callers are unchanged.

## macOS specifics

### FUSE provider support (macFUSE *and* FUSE-T)

The adapter is identical for both (shared libfuse headers; FUSE-T is a drop-in for macFUSE).
Differences are runtime: which `libfuse` dylib loads, and which mount options are valid.

- **Auto-detect** the installed provider at mount time (macFUSE vs FUSE-T install
  paths/dylibs). If neither is present, fail with a clear, actionable error pointing to
  install docs — never a cryptic `dlopen` failure.
- **Default to macFUSE if present** (fuller feature set, better tested); **fall back to
  FUSE-T** (kextless). Explicit override via config: `fuse.provider: macfuse | fuse-t | auto`.

### Mount options (the Finder fix)

Built in the darwin mounter (alongside the existing `MaxWrite` negotiation etc. in
`pkg/client/mount/common.go`):

- `volname=<volume>` — names the volume so Finder renders it (both providers).
- `local` — **macFUSE only**; makes Finder treat it as a browsable local device (the specific
  fix for "terminal sees files, Finder doesn't"). Applied **conditionally** — only when the
  provider is macFUSE, since FUSE-T rejects unknown options.
- `noappledouble` / `noapplexattr` as appropriate to suppress `._*` / `.DS_Store` chatter.

### Identity

No change to the model. The macOS kernel reports the caller's uid/gid; the adapter stamps
`proto.Caller` exactly as the gRPC backend does today. macOS local uids (e.g. 501) resolve
**server-side** via the existing per-volume mapping mode (squash default, etc.). The
per-thread `setfsuid`/`setfsgid`/`setgroups` assumption is entirely server-side (Linux
server) and untouched. macOS identity works via the same path go-fuse-on-mac uses today.

### Locking

`FileSystemBackend` exposes `GetLk/SetLk/SetLkw`, but **cgofuse's high-level
`FileSystemInterface` does not expose byte-range lock callbacks** (no `Getlk/Setlk/Setlkw`).
So on the macOS/cgofuse path, POSIX byte-range locks are **not forwarded to the server** —
advisory locking is handled kernel-locally within the mount only. The Linux go-fuse path is
unaffected and keeps server-forwarded locking. This is consistent with FUSE-T's locking being
best-effort anyway; revisit only if a real workload needs cross-client server-side locking on
macOS. (Verified against winfsp/cgofuse `fuse/fsop.go`, 2026-06-20.)

### Utimens UTIME_OMIT / UTIME_NOW parity gap

The cgofuse adapter's `Utimens` sets both ATIME and MTIME whenever timestamps are provided and
does not interpret the `UTIME_OMIT`/`UTIME_NOW` sentinels the way the Linux go-fuse `Setattr`
does (which sets each half only when the kernel's per-field "ok" bit is set). Consequence: a
macOS `touch -a` (atime-only update) may also rewrite mtime to the current time. This is a
known macOS-path parity gap, low-frequency in practice, and tracked alongside the locking
limitation above; revisit if it bites real workflows.

### Honest FUSE-T caveats (documented, not blockers)

NFSv4-backed: some operations (certain xattrs, fine-grained timestamps, locking edge cases)
behave differently than macFUSE, and historically throughput is lower on some workloads.
Hence **macFUSE is the default, FUSE-T the fallback** — "worst case FUSE-T", not "FUSE-T
always".

### Distribution reality

The macOS binary dynamically links the installed FUSE lib, so it requires macFUSE *or* FUSE-T
installed to run (expected — same as today). No bundled driver.

## Build, release & the Linux benchmark

### Per-GOOS cgo matrix

Today everything is `CGO_ENABLED=0`. After this change:

- **Linux build/CI/release: unchanged** — stays `CGO_ENABLED=0` on go-fuse. The cgofuse
  adapter is excluded from the Linux *production* build via build tags; the default Linux
  artifact is byte-for-byte the same path it is today.
- **macOS build: `CGO_ENABLED=1`**, linked against libfuse headers (macFUSE/FUSE-T). Because
  cross-compiling cgo Linux→darwin via osxcross is brittle, **macOS artifacts build on a
  macOS runner** (GitHub Actions `macos-*`) — a new release-pipeline lane.

### goreleaser / signing

Add a darwin-with-cgo build target on a mac runner; keep the existing Linux lane as-is.
macOS signing/notarisation for Gatekeeper is a follow-on (signing is currently
deprioritised); initial macOS artifacts can be unsigned/dev-distributed for testing.

### Linux benchmark (decides the future "unify Linux" question)

Linux **stays on go-fuse for now**. The cgofuse adapter must *also* compile on Linux (with
cgo + libfuse) so we can run it head-to-head against go-fuse:

- A `CGO_ENABLED=1` Linux build variant (behind a build tag / separate binary, **not** the
  shipped artifact) that mounts via cgofuse.
- Run through the existing **LAN/WAN matrix harness on the perf VM**, against the same
  `FileSystemBackend`, recording to Bencher: metadata-heavy + sequential-throughput + WAN
  profile.
- **Deliverable of this spec:** the Bencher-recorded comparison exists so the later decision
  has data. **The decision itself is deferred** and gated on *both* perf-parity *and*
  acceptable cgo build cost.

### CI testing constraints

GitHub Actions Linux runners have `/dev/fuse`, so the go-fuse path keeps its real-mount e2e
tests. The cgofuse adapter needs FUSE present: Linux runners `apt-get install` libfuse;
macOS mount e2e needs macFUSE/FUSE-T on a mac runner (heavier, likely gated/manual). Adapter
*unit* tests run anywhere with no FUSE.

## Testing

- **Unit (no FUSE, runs anywhere):** status-mapping table, handle table
  (alloc/resolve/release + concurrency), macOS option-string builder, provider
  auto-detection. Written as testify suites (project convention).
- **Adapter conformance (Linux, libfuse in CI):** mount the cgofuse adapter over a
  fake/in-memory `FileSystemBackend` and exercise the op set
  (create/read/write/readdir/rename/xattr/symlink/truncate). Proves correct translation
  independent of the real server. Bulk of the safety net.
- **macOS e2e (mac runner / manual):** real mount against a real server, both macFUSE and
  FUSE-T, asserting Finder-visibility and basic POSIX ops. Gated (`GMOUNTIE_E2E_MACOS`),
  partly manual given runner cost.
- **Regression guard:** the Linux go-fuse path keeps all existing e2e tests untouched (it is
  literally unchanged code).

## Risks & open questions (settle during planning)

1. **`Lookup` semantics.** cgofuse's high-level API is path-based and does per-path
   `Getattr` rather than go-fuse's stateful `Lookup`. Confirm we don't lose the
   negative-entry / `GetAttrIfChanged` version-change optimisations the backend offers;
   verify resulting caching behaviour.
2. **Provider auto-detection.** Pin exact dylib/install-path probes for macFUSE vs FUSE-T
   against current macFUSE 5.x and FUSE-T layouts.
3. **FUSE2-only on macOS.** cgofuse uses the FUSE2 API on macOS; confirm no op we rely on is
   FUSE3-only.
4. **`local` option interaction** with macOS 15.4+/26 "non-local volume" changes — verify it
   still produces the desired Finder behaviour on current macOS.
5. **Neutralising `fuse.Status`** in `FileSystemBackend` to `syscall.Errno` — a deferred
   refactor; noted so keeping `fuse.Status` is a conscious choice, not drift.

## Non-goals

- **Windows (WinFsp)** — later spec. The cgofuse adapter is written to be the future Windows
  path, but Windows is not wired/tested here.
- **Linux migration to cgofuse** — deferred, perf-and-build-cost-gated (see above).
- **The resolve-step `deadline exceeded` bug** — separate cloud-resolver fix (existing
  `raise-resolve-timeout` worktree); not solved by this work.
- **Projection/sync model, NFS server** — explicitly rejected (see Alternatives).

## Success criteria

- The friend mounts on their Mac (macFUSE or FUSE-T) and **Finder shows the files**.
- Same `FileSystemBackend` / gRPC / cache / identity stack as Linux — no behavioural fork
  below the adapter.
- Linux production artifact unchanged (`CGO_ENABLED=0`, go-fuse).
- A Bencher-recorded go-fuse-vs-cgofuse Linux comparison exists to drive the later unify
  decision.
