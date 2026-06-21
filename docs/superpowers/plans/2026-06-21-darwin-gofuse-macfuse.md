# Darwin go-fuse/macFUSE routing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** On darwin, select the FUSE adapter at runtime — macFUSE→go-fuse, FUSE-T→cgofuse — in a single cgo binary.

**Architecture:** Split the current compile-time-mutually-exclusive `establishMount` into two named workers (`establishGoFuse` / `establishCgoFuse`) plus three per-GOOS dispatchers that own the `establishMount` name. On darwin both workers compile in and the dispatcher picks by detected provider. macFUSE Finder mount options are applied to the go-fuse path via a build-tagged hook (go-fuse on darwin is only ever the macFUSE path). Errno correctness already comes from the merged errno-canonical (`node.go` → `fserr.ToErrno` → per-GOOS errno).

**Tech Stack:** Go, `github.com/hanwen/go-fuse/v2` (cgo-free macFUSE mount via `mount_darwin.go`), `github.com/winfsp/cgofuse` (cgo, FUSE-T), build tags, testify suites.

## Global Constraints

- Module path `go.gmountie.dev/gmountie`; tests are testify suite methods (not `func TestX`); logging via `pkg/utils/log`; errors via `github.com/pkg/errors`.
- Based on **merged errno-canonical**: the seam returns `proto.FsError`; `node.go` maps via `fserr.ToErrno`. Do NOT touch the error path — it is already OS-neutral.
- Single darwin binary, cgo (FUSE-T needs it). No build split.
- Three build configurations must keep working: linux-default (`CGO_ENABLED=0`, go-fuse), darwin (cgo, both adapters), linux-`-tags cgofuse` benchmark (cgo, cgofuse).
- The macFUSE→go-fuse runtime path is NOT CI-testable (runners can't load the kext) and NOT testable on cloud Macs. Unit-test the pure seams; the mount itself is friend-verified via a shared release build.
- Do NOT remove or change cgofuse/FUSE-T behavior. Linux behavior unchanged.

---

## File map

| File | Responsibility | Action |
|---|---|---|
| `pkg/client/mount/macos_provider.go` | add `adapterForProvider` + `goFuseMacFUSEOptions` (pure, testable) | Modify |
| `pkg/client/mount/macos_provider_test.go` | tests for the two pure helpers | Modify |
| `pkg/client/mount/gofuse_platform_other.go` | `applyGoFusePlatformOptions` no-op (`//go:build !darwin`) | Create |
| `pkg/client/mount/gofuse_platform_darwin.go` | `applyGoFusePlatformOptions` macFUSE tweaks (`//go:build darwin`) | Create |
| `pkg/client/mount/common.go:48` | call `applyGoFusePlatformOptions` in `createMountOptions` | Modify |
| `pkg/client/mount/establish_gofuse.go` | rename `establishMount`→`establishGoFuse`; tag `!cgofuse` | Modify |
| `pkg/client/mount/establish_cgofuse.go` | rename `establishMount`→`establishCgoFuse` (tag unchanged) | Modify |
| `pkg/client/mount/dispatch_linux.go` | `establishMount`→`establishGoFuse` (`//go:build !darwin && !cgofuse`) | Create |
| `pkg/client/mount/dispatch_darwin.go` | `establishMount` detect+select (`//go:build darwin`) | Create |
| `pkg/client/mount/dispatch_cgofuse.go` | `establishMount`→`establishCgoFuse` (`//go:build !darwin && cgofuse`) | Create |
| `docs/design/macos-mount.md` | correct the "no go-fuse macOS support" model | Modify |

---

### Task 1: Provider→adapter selection (pure, testable)

**Files:**
- Modify: `pkg/client/mount/macos_provider.go`
- Test: `pkg/client/mount/macos_provider_test.go`

**Interfaces:**
- Consumes: `fuseProvider` (existing: `providerMacFUSE`, `providerFuseT`, `providerAuto`).
- Produces: `type adapterKind int`; `adapterGoFuse`, `adapterCgoFuse adapterKind`; `func adapterForProvider(p fuseProvider) adapterKind`.

- [ ] **Step 1: Write the failing test** in `macos_provider_test.go` (add to the existing suite — find its name, e.g. `MacOSProviderSuite`; if none, add `func TestMacOSProviderSuite(t *testing.T){ suite.Run(t, new(MacOSProviderSuite)) }` and `type MacOSProviderSuite struct{ suite.Suite }`):

```go
func (s *MacOSProviderSuite) TestAdapterForProvider() {
	s.Equal(adapterGoFuse, adapterForProvider(providerMacFUSE))
	s.Equal(adapterCgoFuse, adapterForProvider(providerFuseT))
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/client/mount/ -run MacOSProviderSuite -v`
Expected: FAIL — `adapterForProvider` / `adapterGoFuse` undefined.

- [ ] **Step 3: Implement** in `macos_provider.go`:

```go
// adapterKind names which FUSE adapter mounts a backend on darwin.
type adapterKind int

const (
	adapterGoFuse adapterKind = iota // macFUSE: hanwen/go-fuse (cgo-free, full node.go features)
	adapterCgoFuse                   // FUSE-T: winfsp/cgofuse (kextless)
)

// adapterForProvider maps a resolved (non-auto) provider to its adapter.
// macFUSE speaks the FUSE kernel protocol go-fuse implements; FUSE-T is
// NFSv4-backed and only reachable through cgofuse's libfuse API.
func adapterForProvider(p fuseProvider) adapterKind {
	if p == providerFuseT {
		return adapterCgoFuse
	}
	return adapterGoFuse
}
```

- [ ] **Step 4: Run it to verify it passes**

Run: `go test ./pkg/client/mount/ -run MacOSProviderSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/macos_provider.go pkg/client/mount/macos_provider_test.go
git commit -m "feat(mount): map resolved FUSE provider to go-fuse/cgofuse adapter"
```

---

### Task 2: go-fuse macFUSE mount options (pure helper + platform hook)

**Files:**
- Modify: `pkg/client/mount/macos_provider.go`
- Create: `pkg/client/mount/gofuse_platform_other.go`, `pkg/client/mount/gofuse_platform_darwin.go`
- Modify: `pkg/client/mount/common.go` (`createMountOptions`, ~line 48-85)
- Test: `pkg/client/mount/macos_provider_test.go`

**Interfaces:**
- Consumes: `fuse.MountOptions` (go-fuse); `createMountOptions` (existing).
- Produces: `func goFuseMacFUSEOptions(volume string) []string`; `func applyGoFusePlatformOptions(opts *fuse.MountOptions, volume string)` (build-tagged, two impls).

- [ ] **Step 1: Write the failing test** in `macos_provider_test.go`:

```go
func (s *MacOSProviderSuite) TestGoFuseMacFUSEOptions() {
	// Bare option strings for go-fuse's MountOptions.Options (NO "-o" prefixes,
	// NO iosize — iosize rides MountOptions.MaxWrite, emitted by mount_darwin.go).
	got := goFuseMacFUSEOptions("photos")
	s.Equal([]string{"volname=photos", "local", "noappledouble"}, got)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/client/mount/ -run MacOSProviderSuite/TestGoFuseMacFUSEOptions -v`
Expected: FAIL — `goFuseMacFUSEOptions` undefined.

- [ ] **Step 3: Implement the pure helper** in `macos_provider.go`:

```go
// goFuseMacFUSEOptions returns the macFUSE mount options as bare strings for
// go-fuse's MountOptions.Options (it joins them as a single -o list and adds
// -o iosize=MaxWrite itself in mount_darwin.go). "local" makes Finder show a
// browsable volume (fixes "terminal sees files, Finder doesn't"); "volname"
// names it; "noappledouble" suppresses ._*/.DS_Store chatter. go-fuse on darwin
// is only ever the macFUSE path, so these are unconditional here.
func goFuseMacFUSEOptions(volume string) []string {
	return []string{"volname=" + volume, "local", "noappledouble"}
}
```

- [ ] **Step 4: Add the build-tagged platform hook.** Create `gofuse_platform_other.go`:

```go
//go:build !darwin

package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// applyGoFusePlatformOptions is a no-op off darwin: Linux keeps DirectMount and
// adds no Finder options.
func applyGoFusePlatformOptions(_ *fuse.MountOptions, _ string) {}
```

Create `gofuse_platform_darwin.go`:

```go
//go:build darwin

package mount

import "github.com/hanwen/go-fuse/v2/fuse"

// applyGoFusePlatformOptions tunes go-fuse mount options for macFUSE. DirectMount
// is a Linux-only fast path (mount(2) instead of mount_macfuse); macFUSE must go
// through mount_darwin.go's mount_macfuse exec, so disable it. The macFUSE Finder
// options go into MountOptions.Options (iosize already rides MaxWrite).
func applyGoFusePlatformOptions(opts *fuse.MountOptions, volume string) {
	opts.DirectMount = false
	opts.Options = append(opts.Options, goFuseMacFUSEOptions(volume)...)
}
```

- [ ] **Step 5: Call the hook in `createMountOptions`** (`common.go`). It already has the `volume` param. Add the call just before `return opts` (after the killpriv block, ~line 84):

```go
	// Platform tuning for the go-fuse path (no-op on Linux; macFUSE Finder
	// options + DirectMount-off on darwin). createMountOptions is only reached
	// by the go-fuse mounter, so this never touches the cgofuse/FUSE-T path.
	applyGoFusePlatformOptions(opts, volume)
	return opts
```

- [ ] **Step 6: Run tests + cross-compile both platform files**

Run: `go test ./pkg/client/mount/ -run MacOSProviderSuite -v && go vet ./pkg/client/mount/`
Expected: PASS (linux `!darwin` hook compiles).
Run: `CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build ./pkg/client/io/`
Expected: clean (confirms the node adapter still builds cgo-free for darwin; the darwin hook itself is compiled by the Mac-runner build in Task 3).

- [ ] **Step 7: Commit**

```bash
git add pkg/client/mount/macos_provider.go pkg/client/mount/macos_provider_test.go pkg/client/mount/gofuse_platform_other.go pkg/client/mount/gofuse_platform_darwin.go pkg/client/mount/common.go
git commit -m "feat(mount): apply macFUSE Finder mount options to the go-fuse path"
```

---

### Task 3: Build-tag restructure — workers + per-GOOS dispatchers

**Files:**
- Modify: `pkg/client/mount/establish_gofuse.go` (rename fn + retag)
- Modify: `pkg/client/mount/establish_cgofuse.go` (rename fn)
- Create: `pkg/client/mount/dispatch_linux.go`, `dispatch_darwin.go`, `dispatch_cgofuse.go`
- (No change to `single.go` — it still calls `establishMount`.)

**Interfaces:**
- Consumes: `adapterForProvider` (Task 1), `detectProvider`/`pathExists` (existing), the two workers below.
- Produces: `establishGoFuse(...)` and `establishCgoFuse(...)` with the existing 8-arg signature `(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error)`; and `establishMount` with that same signature in each dispatcher.

- [ ] **Step 1: Rename the go-fuse worker.** In `establish_gofuse.go`: change the build tag from `//go:build !darwin && !cgofuse` to `//go:build !cgofuse` (now compiles on linux-default AND darwin), and rename `func establishMount(` to `func establishGoFuse(`. Leave the body unchanged (it calls `createMountOptions`, which now applies the darwin hook automatically).

- [ ] **Step 2: Rename the cgofuse worker.** In `establish_cgofuse.go`: keep the build tag `//go:build darwin || cgofuse`; rename `func establishMount(` to `func establishCgoFuse(`. Leave the body unchanged.

- [ ] **Step 3: Add the linux-default dispatcher.** Create `dispatch_linux.go`:

```go
//go:build !darwin && !cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

// establishMount on Linux always uses go-fuse (cgo-free).
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error) {
	return establishGoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout)
}
```

- [ ] **Step 4: Add the linux-cgofuse benchmark dispatcher.** Create `dispatch_cgofuse.go`:

```go
//go:build !darwin && cgofuse

package mount

import (
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

// establishMount on the Linux `-tags cgofuse` benchmark build uses cgofuse
// (head-to-head vs go-fuse). Unchanged behavior from before the split.
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error) {
	return establishCgoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout)
}
```

- [ ] **Step 5: Add the darwin dispatcher.** Create `dispatch_darwin.go`:

```go
//go:build darwin

package mount

import (
	"time"

	"github.com/pkg/errors"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
)

// establishMount on darwin selects the adapter by detected backend: macFUSE uses
// go-fuse (cgo-free code, full node.go features), FUSE-T uses cgofuse (kextless).
// detectProvider honors the fuse.provider config (auto → probe dylibs).
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int, metaTimeout time.Duration) (mountHandle, error) {
	provider, err := detectProvider(fuseProvider(cfg.Provider), pathExists)
	if err != nil {
		return nil, errors.Wrap(err, "detect FUSE provider")
	}
	switch adapterForProvider(provider) {
	case adapterCgoFuse:
		return establishCgoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout)
	default:
		return establishGoFuse(mountPath, volume, endpoint, backend, rewriter, cfg, maxWrite, metaTimeout)
	}
}
```

(Confirm `cfg.Provider` is the field name on `config.FUSEConfig`; `establish_cgofuse.go` currently reads it via `fuseProvider(cfg.Provider)` — match that exactly.)

- [ ] **Step 6: Verify all three build configurations compile.**

Run (linux default, cgo-free):
```bash
CGO_ENABLED=0 go build ./pkg/client/mount/ ./cmd/... && echo LINUX_DEFAULT_OK
```
Expected: clean — exactly one `establishMount` (go-fuse) compiled.

Run (linux cgofuse benchmark):
```bash
CGO_ENABLED=1 go build -tags cgofuse ./pkg/client/mount/ && echo LINUX_CGOFUSE_OK
```
Expected: clean — `establishCgoFuse` dispatcher compiled. (Needs libfuse dev headers; if unavailable in the sandbox, note it and rely on CI.)

Run (darwin — vet for duplicate-symbol / tag errors; full cgo build happens on the Mac runner):
```bash
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go vet ./pkg/client/mount/ 2>&1 | head
```
Expected: the ONLY errors are cgofuse's missing C symbols (`undefined: c_struct_fuse`, `EACCES`, …) — that confirms the Go-level tags are correct and the sole darwin dependency on cgo is cgofuse. Any *other* error (duplicate `establishMount`, undefined `establishGoFuse`/`adapterForProvider`) is a real bug to fix.

- [ ] **Step 7: Commit**

```bash
git add pkg/client/mount/establish_gofuse.go pkg/client/mount/establish_cgofuse.go pkg/client/mount/dispatch_linux.go pkg/client/mount/dispatch_darwin.go pkg/client/mount/dispatch_cgofuse.go
git commit -m "feat(mount): runtime adapter selection on darwin (macFUSE→go-fuse, FUSE-T→cgofuse)"
```

---

### Task 4: Documentation

**Files:**
- Modify: `docs/design/macos-mount.md`

- [ ] **Step 1: Correct the provider model.** Read `docs/design/macos-mount.md`. Replace the claims that "go-fuse has no macOS support" and "cgofuse on macOS, period" with the real model: go-fuse v2.10.1 has a cgo-free macFUSE mount (`fuse/mount_darwin.go` execs `mount_macfuse`), so on darwin the client now selects at runtime — **macFUSE→go-fuse** (faster, full feature set), **FUSE-T→cgofuse** (kextless, the only option where a kext can't load, e.g. cloud Macs). Keep the benchmark reference (go-fuse faster) — it now also justifies preferring go-fuse on macFUSE. Note the single cgo binary (FUSE-T needs cgo) and the feature-divergence tradeoff (the xattr-names prime etc. are go-fuse-only).

- [ ] **Step 2: Add a manual macFUSE verification checklist** (a subsection), since this path has no automated test:

```markdown
### Manual macFUSE verification (no CI — runners can't load the kext)

On a Mac with macFUSE installed, using a release build:
1. `gmountie mount -n <vol> /tmp/mnt` — succeeds, no error.
2. `ls -la /tmp/mnt` — lists entries; correct sizes/modes.
3. Read a file (`cat`), write a file (`echo x > /tmp/mnt/f`), re-read it.
4. `xattr -w user.test v /tmp/mnt/f && xattr -l /tmp/mnt/f` — shows `user.test`.
5. Trigger an error (e.g. `rmdir` a non-empty dir) — correct message (ENOTEMPTY), not a garbled errno.
6. **Finder**: the volume appears with its name and is browsable.
7. `umount /tmp/mnt` (or `gmountie` unmount) — clean.
```

- [ ] **Step 3: Commit**

```bash
git add docs/design/macos-mount.md
git commit -m "docs(mount): correct macOS provider model; add macFUSE manual checklist"
```

---

## Self-review notes

- **Spec coverage:** runtime selection (Task 3 darwin dispatcher), build-tag restructure (Task 3), macFUSE Finder options (Task 2), errno (no task — already structural via errno-canonical, per Global Constraints), provider detection reuse (Task 1/3 via existing `detectProvider`), single cgo binary / three build configs (Task 3 Step 6), feature-divergence + doc correction (Task 4), manual testing checklist (Task 4). All spec sections map.
- **No automated macFUSE mount test** — by design (Global Constraints); the pure seams (`adapterForProvider`, `goFuseMacFUSEOptions`) are unit-tested, and Task 3 Step 6 compile-gates all three build configs.
- **Type consistency:** `adapterKind`/`adapterGoFuse`/`adapterCgoFuse` (Task 1) used in Task 3 dispatcher; `goFuseMacFUSEOptions`/`applyGoFusePlatformOptions` (Task 2) consistent; worker names `establishGoFuse`/`establishCgoFuse` (Task 3) match their dispatcher call sites; 8-arg signature identical across workers and dispatchers.
- **DirectMount-off on darwin** is defensive (it's a Linux-only mount(2) fast path); confirm on the Mac that go-fuse still mounts via `mount_macfuse`.
