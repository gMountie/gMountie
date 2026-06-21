# Route macFUSE to go-fuse, reserve cgofuse for FUSE-T (darwin) — design

- **Date:** 2026-06-21
- **Status:** Approved design, pre-implementation
- **Scope:** OSS core repo — the darwin client mount path only
- **Branch / worktree:** `worktree-macfuse-gofuse` (based on merged errno-canonical)

## Problem

On macOS the client **always** uses the cgofuse adapter, chosen at *compile time*
by build tag (`establish_cgofuse.go` is `//go:build darwin || cgofuse`;
`establish_gofuse.go` is `//go:build !darwin && !cgofuse`). cgofuse speaks the
libfuse API and serves both macOS backends — macFUSE (kernel extension) and
FUSE-T (kextless, NFSv4-backed) — so one adapter covered both.

But cgofuse on macOS carries costs the go-fuse adapter does not:
- **cgo** — forces a native Mac runner build (CGO_ENABLED=1 + FUSE headers), and
  every FUSE op pays cgo call latency. The head-to-head benchmark
  (`docs/design/benchmarks/cgofuse-vs-gofuse.md`) found go-fuse faster.
- **Feature lag** — the cgofuse adapter (`pkg/client/io/cgofs/`) is newer and
  thinner than the battle-tested go-fuse adapter (`pkg/client/io/node.go`); e.g.
  the readdirplus xattr-names prime is go-fuse-only.

The original macOS design (`docs/design/macos-mount.md`) asserts "go-fuse has no
macOS support." **This is incorrect for macFUSE.** go-fuse v2.10.1 ships a
cgo-free macFUSE mount path (`fuse/mount_darwin.go` execs
`/Library/Filesystems/macfuse.fs/.../mount_macfuse` with the osxfuse handshake),
and our `node.go` adapter compiles cgo-free for darwin (verified). It is correct
for FUSE-T: go-fuse only knows the `mount_macfuse`/`mount_osxfuse` binaries, not
FUSE-T's NFS mechanism.

It was tried once on a real Mac (macFUSE): it **worked in the terminal**, with two
issues — wrong error codes, and Finder not showing the volume. Both are now
addressable (see below). cgofuse/FUSE-T remains necessary because the maintainer's
own test environment is cloud Macs that **cannot load a kext** (so macFUSE is
impossible there; FUSE-T is the only option).

## Decision

On darwin, choose the adapter at **runtime** by detected backend:

- **macFUSE → go-fuse** — faster, cgo-free-capable code, full `node.go` feature
  set. The preferred path for end users who install macFUSE.
- **FUSE-T → cgofuse** — the kextless path; required on cloud Macs and for users
  who won't install a kext.

Ship **one darwin binary** containing both adapters (it still needs cgo for the
cgofuse/FUSE-T path — accepted, since FUSE-T support requires it regardless). No
build split, no second artifact.

This builds **on top of merged errno-canonical**, which is what makes the error
half correct for free (below).

### Why the two prior issues are now solved

1. **Wrong error codes → fixed by errno-canonical (already merged).** The seam
   (`FileSystemBackend`) now returns `proto.FsError`; `node.go` maps it with
   `fserr.ToErrno(st)`, which uses per-GOOS `syscall.E*`. On a darwin build that
   yields the **Darwin** errno, and go-fuse's `fs.Node` API returns
   `syscall.Errno` directly to macFUSE. So `node.go` is already OS-neutral — no
   adapter changes needed for errno. (This is structural correctness; it does not
   require a Mac to validate the mapping table — `fserr_darwin_test.go` covers it
   on a darwin build.)
2. **Finder didn't show the volume → mount options.** macFUSE needs `-o local`,
   `-o volname=<vol>`, `-o noappledouble`, `-o iosize=<n>` to present a browsable
   Finder volume. `macos_provider.go:macOSMountOptions` already computes these for
   cgofuse; we wire the equivalents into go-fuse's `MountOptions.Options`
   (`iosize` via `MountOptions.MaxWrite`, which `mount_darwin.go` already emits).

## Architecture

### Build-tag restructure (the core change)

Today the two `establishMount` functions are mutually exclusive by GOOS. Split
"the worker that mounts via adapter X" from "the dispatcher that picks X":

**Worker functions** (rename from the current `establishMount`):
- `establishGoFuse(...) (mountHandle, error)` in `establish_gofuse.go` — build tag
  `//go:build !cgofuse` (compiles on linux-default **and darwin**).
- `establishCgoFuse(...) (mountHandle, error)` in `establish_cgofuse.go` — build
  tag unchanged `//go:build darwin || cgofuse`.

Both already return the shared `mountHandle` interface and take the identical
signature (`mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite,
metaTimeout`), so the rename is mechanical.

**Dispatchers** (each defines `establishMount`, partitioning the build space so
exactly one is compiled):
- `//go:build !darwin && !cgofuse` (linux default) → `establishMount` calls
  `establishGoFuse`. Unchanged behavior.
- `//go:build darwin` → `establishMount` runs `detectProvider(cfg.Provider)` (the
  existing `macos_provider.go` logic) and calls `establishGoFuse` for macFUSE or
  `establishCgoFuse` for FUSE-T.
- `//go:build !darwin && cgofuse` (linux benchmark) → `establishMount` calls
  `establishCgoFuse`. Preserves the head-to-head bench build unchanged.

On darwin both workers compile in; the dispatcher selects at runtime. On
linux-default only go-fuse compiles (CGO_ENABLED=0 preserved). On the linux
`-tags cgofuse` bench only cgofuse — all three existing build configurations keep
their current behavior.

### go-fuse darwin mount options

`establishGoFuse` is shared by linux and darwin, so the macFUSE options must be
injected only on darwin. The darwin dispatcher computes them (reusing the
`macos_provider.go` provider+option logic, reformatted as bare `MountOptions.Options`
strings — `volname=<vol>`, `local`, `noappledouble` — rather than cgofuse's
`-o`-pair `[]string`) and passes them to `establishGoFuse`, which folds them into
the `fuse.MountOptions` it builds. `iosize` rides `MountOptions.MaxWrite` (the
negotiated `maxWrite` is already threaded in). Linux passes no extra options
(empty), so its path is unchanged.

Implementation note: porting `createMountOptions`/`buildFSOptions` to compile on
darwin must gate any Linux-only `fuse.MountOptions` fields per-GOOS (killpriv is
already `//go:build !linux`-gated via `killpriv_other.go`; audit for others).

### Provider/adapter selection summary

| Backend (darwin) | Adapter | cgo | Detection |
|---|---|---|---|
| macFUSE | go-fuse (`node.go`) | no* | `libfuse.2.dylib` present |
| FUSE-T  | cgofuse (`cgofs/`)  | yes | `libfuse-t.dylib` present |

\* the go-fuse path itself is cgo-free, but the shipped darwin binary is still
built with cgo because it also contains the cgofuse/FUSE-T path.

`fuse.provider` config (`auto`/`macfuse`/`fuse-t`) is honored as today; `auto`
prefers macFUSE, falls back to FUSE-T, errors if neither dylib is found.

## Testing

The macFUSE→go-fuse path is **not CI-testable** (GitHub macOS runners can't load
the macFUSE kext) and not testable on the maintainer's cloud Macs (same reason).
This is a best-effort change validated as follows:

- **Unit (CI, runnable):** the darwin dispatcher's selection — macFUSE→go-fuse,
  FUSE-T→cgofuse, explicit `fuse.provider` override honored, no-provider error;
  and the go-fuse darwin mount-option construction (`local`/`volname`/
  `noappledouble`/`iosize` present and correctly formatted for `MountOptions.Options`).
  Use a `func(string) bool` exists-probe seam like `macos_provider_test.go`.
- **Compile gate:** darwin build (the existing Mac-runner cgo release job) must
  build with both adapters linked; add a `CGO_ENABLED=0 GOOS=darwin go vet
  ./pkg/client/...` guard for the go-fuse-on-darwin code so its compilation is
  caught in Linux CI.
- **Errno correctness:** structural via `fserr` + `fserr_darwin_test.go` (already
  in tree) — no Mac needed.
- **Manual (friend's Mac with macFUSE), via a shared release build:** mount;
  `ls -la`; read a file; write a file; `xattr -w user.x v <file>` + `xattr -l`;
  **Finder shows the named volume and browses it**; unmount cleanly. This
  checklist ships in the PR description.

## Tradeoffs (accepted)

- **Feature divergence:** macFUSE (go-fuse) users get the full adapter (xattr-names
  prime, reclaim, directio); FUSE-T (cgofuse) users get the `cgofs` subset. The
  two macOS backends now behave slightly differently — a permanent support-matrix
  line, accepted because go-fuse is the better path and FUSE-T is the kextless
  fallback.
- **macFUSE path has no automated test** — only friend/manual verification, as
  above.

## Non-goals

- **Replacing or removing cgofuse** — it stays as the FUSE-T (and future WinFsp)
  adapter.
- **Build split / cgo-free distribution** — rejected; one cgo darwin binary, since
  FUSE-T needs cgo regardless.
- **Feature parity work on cgofs** (e.g. giving FUSE-T the xattr prime) — separate,
  out of scope.
- **Linux behavior** — unchanged.

## Success criteria

- On a Mac with macFUSE, `gmountie mount` uses go-fuse, shows a browsable Finder
  volume, and surfaces correct Darwin errno (e.g. `ENOTEMPTY`, missing-xattr) —
  the two prior issues resolved.
- On a Mac/cloud-Mac with FUSE-T (no kext), `gmountie mount` still uses cgofuse,
  unchanged.
- Linux and the `-tags cgofuse` benchmark build are behaviorally unchanged.
- A shareable darwin release build is produced by the existing pipeline.
