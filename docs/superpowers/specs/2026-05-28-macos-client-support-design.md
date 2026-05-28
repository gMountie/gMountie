# Minimal macOS Client Support — Design

**Status:** approved 2026-05-28
**Roadmap:** not its own phase; folds into `docs/design/operations-and-packaging.md` on completion.
**Transient:** consolidate the durable bits into `docs/design/` on completion, then prune this file.

## Goal

The `gMountie` binary builds, ships, and runs `mount` on macOS (`darwin/amd64` and `darwin/arm64`) with the smallest viable code, CI, distribution, and documentation change. The server stays Linux-only by design — `gMountie serve` is simply absent on darwin.

## Context

- `pkg/client/...` already cross-compiles cleanly to `darwin/arm64` today (verified locally).
- `hanwen/go-fuse v2.10.1` has first-class Darwin support: `mount_darwin.go`, `syscall_darwin.go`, etc. Mounting works via either **macFUSE** (the kext-based traditional driver) or **FUSE-T** (the userspace alternative, no kext required).
- The only `cmd/...` darwin build blockers live in `pkg/server/`:
  - `pkg/server/io/bound_fs.go`: `syscall.Setfsuid`/`Setfsgid` (Linux-only).
  - `pkg/server/io/confined.go`: `unix.OpenHow` / `RESOLVE_BENEATH` / `O_PATH` — Linux-only `openat2` API used by `ConfinedLoopbackFileSystem`.
- `cmd/commands/serve.go` is the **only** file in `cmd/` that imports `pkg/server`. `mount.go` imports only `pkg/client`. `root.go`/`version.go` import neither.
- `serve.go` self-registers with `rootCmd` via its own `func init()` (line 86). Excluding the file at build time removes the subcommand cleanly — no `root.go` edit needed.
- Active worktrees: `identity-phase3-admin-caps` still edits `bound_fs.go`; `codecv2-zerocopy` edits `pkg/client/io/backend_grpc.go`; multiple docs worktrees. This design touches none of those files.

## Decisions

- **Server on darwin: absent (not "present but degraded", not "split binaries").** A single `//go:build linux` tag on `cmd/commands/serve.go` excludes the subcommand from darwin builds. Zero changes to `pkg/server/`, zero collision with any active worktree.
- **Build tag form: `//go:build linux`** (not `!darwin`). Strict, and accurately reflects that the server has always been Linux-only.
- **Minimal ship:** cross-compile guard in CI plus tarball releases through goreleaser. **No** Homebrew tap, **no** Mac runner integration smoke, **no** code signing/notarization, **no** universal binary.
- **FUSE provider guidance:** the runtime is provider-agnostic — both macFUSE and FUSE-T work with `mount_darwin.go`. Docs mention both; the binary doesn't tie itself to either.
- **Runtime UX:** wrap mount errors on darwin that look like a missing FUSE provider, pointing users at install URLs. Inline in this PR.

## Architecture

A single `gMountie` binary with the registered subcommand set selected at build time:

| OS | Subcommands |
|---|---|
| `linux` | `serve`, `mount`, `version` |
| `darwin` | `mount`, `version` |

Goreleaser produces four archives per release: `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`. The container image (`dockers_v2`) stays linux-only.

## Concrete changes

### Code & ship

| File | Change |
|---|---|
| `cmd/commands/serve.go` | First line: `//go:build linux`. |
| `cmd/commands/serve_test.go` | Same `//go:build linux` tag. |
| `pkg/client/mount/mount_error.go` *(new)* | `func wrapMountError(err error) error`. If `currentGOOS == "darwin"` and the error matches a missing-FUSE-provider pattern (`macfuse`, `osxfuse`, `mount_macfuse`, `/dev/macfuse`, `/dev/osxfuse`), wrap with: *"FUSE driver missing — install macFUSE (https://macfuse.io) or FUSE-T (https://www.fuse-t.org/) before mounting on macOS"*. Otherwise pass through. `var currentGOOS = runtime.GOOS` so the test can flip it. |
| `pkg/client/mount/mount_error_test.go` *(new)* | Table-driven: `(goos, err) → expected`. Covers linux+any, darwin+matching-pattern, darwin+unrelated, nil. Pure Go, runs on the existing Linux CI. |
| `pkg/client/mount/single.go` | Wrap the FUSE-mount error path with `wrapMountError(err)` at the boundary where the error surfaces to the user. Exact call site to be located during implementation (the function that performs `server.Mount` / `fs.Mount`). |
| `.goreleaser.yaml` | Replace the existing TODO under `goos:` with `- darwin`. |
| `.github/workflows/ci.yml` | Expand the existing `build-darwin` job: scope changes from `./pkg/client/...` to `./cmd/...`; matrix over `darwin/{amd64,arm64}`. |

### Documentation

Only the platform/install statements that are currently wrong. Not a full "macOS guide".

| File | Edit |
|---|---|
| `README.md` | (a) Platform badge: `platform-Linux` → `platform-Linux%20%7C%20macOS%20(client)`. (b) Banner: "alpha and **Linux-only** today" → "alpha. **Server is Linux-only; the client mounts on Linux and macOS.**" (c) Requirements: add one sentence: *"On macOS the client needs **macFUSE** (https://macfuse.io) or **FUSE-T** (https://www.fuse-t.org/) installed for the mount to work."* (d) Releases: mention the `gMountie_darwin_*.tar.gz` archives alongside the Linux ones. |
| `docs/index.md` | Banner: same revision as the README banner. |
| `docs/quickstart.md` | Short macOS section under install: install macFUSE or FUSE-T, download the darwin tarball, `chmod +x`, then `gmountie mount …` works identically to Linux. |
| `docs/client/cli.md` | One sentence noting the macFUSE / FUSE-T requirement for `mount` on macOS (paths and flags otherwise identical). |
| `docs/client/config.md` | If it states Linux config paths explicitly, add the macOS-equivalent (`~/Library/Application Support/gmountie/...`, supplied automatically by `adrg/xdg`). May be a no-op once we look. |

**Not changed:** `docs/design/*.md`, `docs/server/*.md`, `docs/recipes/*.md`, `docs/roadmap.md`. The architecture description is still accurate (server is Linux-only); the server docs apply to the linux build only; the recipes are server-side/deployment; macOS support isn't a phase.

## Out of scope

- **Homebrew tap** (goreleaser block + tap repo).
- **Mac runner integration smoke** (actually mounting on `macos-latest`).
- **Code signing + Apple notarization** (Gatekeeper-friendly distribution).
- **Universal binary** via `lipo`.
- **`.dmg` installer.**
- **Wails desktop UI on macOS** (Phase 8, `[[project-ui-deferred]]`).
- **Server functionality on macOS** (intentionally absent — Approach A).
- **`bound_fs.go` / `confined.go` build-tag stubs** — not needed because `serve.go` is excluded as a whole.

## Definition of done (testable)

- `GOOS=darwin GOARCH=amd64 go build ./cmd/...` and `GOOS=darwin GOARCH=arm64 go build ./cmd/...` both succeed.
- The `build-darwin` CI job runs on both arches and stays green.
- `task build` (linux snapshot) still works unchanged.
- `goreleaser release --snapshot --clean` produces four archives (linux+darwin × amd64+arm64).
- `pkg/client/mount` tests pass on Linux CI, including the new `wrapMountError` table-driven tests.
- The README badge and banner reflect macOS client support; `docs/index.md` matches.
- Spot-check: a darwin binary's `--help` shows `mount` and `version` but not `serve`.

## Implementation sequence

1. **This worktree → single PR off fresh `origin/master`.** All changes land together: code, ship config, docs, runtime UX.
2. **No follow-up needed** — runtime UX is in scope here.
3. **Post-merge:** consolidate this spec into `docs/design/operations-and-packaging.md` (a short "Platform support" section), then delete this file (standing rule, `[[project_doc_organization]]`).
