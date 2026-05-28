# Minimal macOS Client Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a `gmountie` binary that builds, mounts, and (on `mount` failure) gives users a helpful macFUSE/FUSE-T hint on macOS — without touching `pkg/server/*` or breaking the Linux build.

**Architecture:** Single binary, build-tag-gated subcommand set. `cmd/commands/serve.go` gets `//go:build linux`, so its `init()` doesn't register `serveCmd` on darwin and the darwin binary just lacks the `serve` subcommand. Server-side internals (`bound_fs.go`, `confined.go`) are never compiled on darwin and need no stubs. A tiny `wrapMountError` helper turns missing-FUSE-driver errors into install pointers. Goreleaser builds 4 archives; CI guards the cross-compile.

**Tech Stack:** Go 1.26, `hanwen/go-fuse/v2`, Cobra, `pkg/errors`, testify suite, goreleaser, GitHub Actions, actionlint.

**Spec:** `docs/superpowers/specs/2026-05-28-macos-client-support-design.md`

**Worktree:** `/home/john/git/gMountie/.claude/worktrees/macos-client-support` on `worktree-macos-client-support` off master `440d148`.

---

## File map

**Code & ship:**
- Modify: `cmd/commands/serve.go` (add build tag)
- Modify: `cmd/commands/serve_test.go` (add build tag)
- Create: `pkg/client/mount/mount_error.go` (`wrapMountError` helper)
- Create: `pkg/client/mount/mount_error_test.go` (table-driven testify suite)
- Modify: `pkg/client/mount/single.go` (wrap the `gofs.Mount` error at line ~122)
- Modify: `.goreleaser.yaml` (replace TODO with `- darwin`)
- Modify: `.github/workflows/ci.yml` (expand `build-darwin` from `./pkg/client/...` to `./cmd/...` with `{amd64,arm64}` matrix)

**Docs:**
- Modify: `README.md` (badge + banner + requirements + releases lines)
- Modify: `docs/index.md` (banner)
- Modify: `docs/quickstart.mdx` (prerequisites + install)
- Modify: `docs/client/cli.md` (macOS FUSE note)

**Not changed (intentionally):** anything in `pkg/server/*`, `docs/design/*`, `docs/server/*`, `docs/recipes/*`, `docs/roadmap.md`, `docs/client/config.md` (no Linux-specific paths to update — verified).

---

## Task 1: Build-tag `serve.go` for Linux

**Files:**
- Modify: `cmd/commands/serve.go:1` (add `//go:build linux` as first line)
- Modify: `cmd/commands/serve_test.go:1` (add `//go:build linux` as first line)

- [ ] **Step 1: Confirm the current darwin build fails on the expected files (baseline)**

Run:
```bash
cd /home/john/git/gMountie/.claude/worktrees/macos-client-support
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/... 2>&1 | head -8
```

Expected output includes:
```
# gmountie/pkg/server/io
pkg/server/io/bound_fs.go:19:21: undefined: syscall.Setfsuid
pkg/server/io/bound_fs.go:20:21: undefined: syscall.Setfsgid
pkg/server/io/confined.go:26:25: undefined: unix.RESOLVE_BENEATH
... (and more confined.go errors)
```

If you see those, the baseline is correct. If `./cmd/...` already builds on darwin, something has changed since the spec was written — stop and investigate.

- [ ] **Step 2: Add the build tag to `serve.go`**

Insert at the very top of `cmd/commands/serve.go` (before the existing `package commands` line):

```go
//go:build linux

```

(Note the blank line between the build constraint and `package commands` — this is required by Go's build-tag rules.)

The first lines of the file should now read:
```go
//go:build linux

package commands

import (
```

- [ ] **Step 3: Add the same build tag to `serve_test.go`**

Insert at the very top of `cmd/commands/serve_test.go`:

```go
//go:build linux

```

- [ ] **Step 4: Verify darwin cross-compile now succeeds**

Run:
```bash
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/...
echo "darwin/arm64 build exit: $?"

GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/...
echo "darwin/amd64 build exit: $?"
```

Expected: both exit code `0`, no compiler errors. `pkg/server/io` is no longer reached because `cmd/commands/serve.go` is the only path into it.

- [ ] **Step 5: Verify Linux build still works (no regression)**

Run:
```bash
go build ./cmd/...
echo "linux build exit: $?"

go vet ./cmd/...
echo "linux vet exit: $?"

go test -count=1 -run '^$' ./cmd/commands/
echo "linux test compile exit: $?"
```

All three should exit `0`. The `-run '^$'` runs no tests but compiles the test binary, proving `serve_test.go` still compiles on Linux.

- [ ] **Step 6: Commit**

```bash
git add cmd/commands/serve.go cmd/commands/serve_test.go
git commit -m "feat(cmd): build-tag serve subcommand for linux only

cmd/commands/serve.go is the only file in cmd/ that imports pkg/server.
A //go:build linux tag excludes it (and its self-registering init()) on
darwin, so the cmd/ tree compiles cleanly for darwin/{amd64,arm64}
without touching pkg/server/io's Linux-only bound_fs / confined files.
On darwin the binary has mount + version subcommands only."
```

---

## Task 2: `wrapMountError` helper (TDD)

**Files:**
- Create: `pkg/client/mount/mount_error.go`
- Create: `pkg/client/mount/mount_error_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/client/mount/mount_error_test.go`:

```go
package mount

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type WrapMountErrorTestSuite struct {
	suite.Suite
	savedGOOS string
}

func (s *WrapMountErrorTestSuite) SetupTest() {
	s.savedGOOS = currentGOOS
}

func (s *WrapMountErrorTestSuite) TearDownTest() {
	currentGOOS = s.savedGOOS
}

func (s *WrapMountErrorTestSuite) TestNilPassesThrough() {
	s.Nil(wrapMountError(nil))
}

func (s *WrapMountErrorTestSuite) TestLinuxNeverWraps() {
	currentGOOS = "linux"
	// Even an error that LOOKS like missing FUSE on darwin must pass through on Linux.
	in := errors.New("open /dev/osxfuse0: no such file or directory")
	out := wrapMountError(in)
	s.Same(in, out, "Linux must return the same error pointer unchanged")
}

func (s *WrapMountErrorTestSuite) TestDarwinUnrelatedErrorPassesThrough() {
	currentGOOS = "darwin"
	in := errors.New("network unreachable")
	out := wrapMountError(in)
	s.Same(in, out, "darwin with unrelated error must pass through")
}

func (s *WrapMountErrorTestSuite) TestDarwinMissingProviderWraps() {
	currentGOOS = "darwin"

	cases := []string{
		"exec: \"mount_macfuse\": executable file not found in $PATH",
		"open /dev/macfuse0: no such file or directory",
		"open /dev/osxfuse0: no such file or directory",
		"MACFUSE failed to load",        // ensures case-insensitive matching
		"fork/exec /usr/local/bin/mount_osxfuse: no such file or directory",
	}

	for _, msg := range cases {
		s.Run(msg, func() {
			in := errors.New(msg)
			out := wrapMountError(in)

			s.Require().NotNil(out)
			s.NotSame(in, out, "matching error should be wrapped, not returned as-is")

			text := out.Error()
			s.Contains(text, "FUSE driver missing",
				"wrapper must include the canonical hint phrase")
			s.Contains(text, "macfuse.io",
				"wrapper must point at macFUSE")
			s.Contains(text, "fuse-t.org",
				"wrapper must point at FUSE-T")
			s.Contains(text, msg,
				"wrapper must preserve the original error text")
			s.True(errors.Is(out, in) || strings.Contains(text, msg),
				"original error should be discoverable")
		})
	}
}

func TestWrapMountErrorTestSuite(t *testing.T) {
	suite.Run(t, new(WrapMountErrorTestSuite))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
go test -v -run TestWrapMountErrorTestSuite ./pkg/client/mount/
```

Expected: compile failure with messages like:
```
./mount_error_test.go:18:24: undefined: currentGOOS
./mount_error_test.go:30:9: undefined: wrapMountError
```

This is the desired failing-test state.

- [ ] **Step 3: Implement the minimal helper**

Create `pkg/client/mount/mount_error.go`:

```go
package mount

import (
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

// currentGOOS is the runtime OS the helper sees. It defaults to runtime.GOOS
// and is overridable so the table-driven test can exercise both branches from
// a single (Linux) CI host without build tags.
var currentGOOS = runtime.GOOS

// wrapMountError annotates a FUSE mount error on darwin when it looks like the
// host is missing a FUSE provider (macFUSE or FUSE-T). On any other platform —
// or when the error doesn't match the missing-driver pattern — the error is
// returned unchanged (pointer-identical for nil and non-matching cases).
func wrapMountError(err error) error {
	if err == nil || currentGOOS != "darwin" {
		return err
	}
	if !looksLikeMissingFUSE(err) {
		return err
	}
	return errors.Wrap(err,
		"FUSE driver missing — install macFUSE (https://macfuse.io) or "+
			"FUSE-T (https://www.fuse-t.org/) before mounting on macOS")
}

// looksLikeMissingFUSE returns true if the error text suggests the macOS FUSE
// provider (macFUSE's mount_macfuse helper, FUSE-T's /dev/macfuse, the older
// /dev/osxfuse* devices) isn't installed or reachable. Matching is
// case-insensitive substring across a small allow-list to keep false positives
// low.
func looksLikeMissingFUSE(err error) bool {
	s := strings.ToLower(err.Error())
	patterns := []string{
		"macfuse",
		"osxfuse",
		"mount_macfuse",
		"/dev/macfuse",
		"/dev/osxfuse",
		"no such file or directory",
		"executable file not found",
	}
	for _, p := range patterns {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run:
```bash
go test -v -run TestWrapMountErrorTestSuite ./pkg/client/mount/
```

Expected: every sub-test PASS. Also verify race-cleanliness (the helper has no shared state besides the test-controlled `currentGOOS`):

```bash
go test -race -v -run TestWrapMountErrorTestSuite ./pkg/client/mount/
```

Expected: PASS, no race reports.

- [ ] **Step 5: Verify the broader mount package still builds and passes**

Run:
```bash
go test -count=1 ./pkg/client/mount/...
```

Expected: existing tests still pass. (We've only added a new file; we haven't touched the rest of the package yet — that's Task 3.)

- [ ] **Step 6: Commit**

```bash
git add pkg/client/mount/mount_error.go pkg/client/mount/mount_error_test.go
git commit -m "feat(client/mount): wrapMountError points darwin users at macFUSE/FUSE-T

Pure-Go helper that turns a missing-FUSE-driver error into a one-line
install hint when GOOS is darwin and the error string matches a
macFUSE / FUSE-T / osxfuse pattern. Non-darwin and non-matching errors
pass through pointer-unchanged. currentGOOS is a package var so the
table-driven testify suite can exercise both branches from Linux CI."
```

---

## Task 3: Wire `wrapMountError` into `SingleVolumeMounter.Mount`

**Files:**
- Modify: `pkg/client/mount/single.go:122` (wrap the `gofs.Mount` error)

- [ ] **Step 1: Read the current Mount call site**

Run:
```bash
sed -n '115,130p' pkg/client/mount/single.go
```

Expected output (around line 122):
```go
	// gofs.Mount is self-contained: it constructs the raw FS via
	// NewNodeFS, spawns the Serve goroutine, and blocks on WaitMount
	// before returning. No explicit go server.Serve()/WaitMount needed.
	server, err := gofs.Mount(mountPath, root, fsOpts)
	if err != nil {
		...
	}
```

- [ ] **Step 2: Wrap the error**

Edit `pkg/client/mount/single.go` to wrap the error immediately after the `gofs.Mount` call. Change:

```go
	server, err := gofs.Mount(mountPath, root, fsOpts)
```

to:

```go
	server, err := gofs.Mount(mountPath, root, fsOpts)
	err = wrapMountError(err)
```

That single inserted line is the entire change. The existing `if err != nil` block already handles the (now-wrapped) error.

- [ ] **Step 3: Verify the package builds and tests pass on Linux**

Run:
```bash
go build ./pkg/client/mount/...
go test -count=1 ./pkg/client/mount/...
```

Expected: both exit 0, all tests pass. (On Linux, `wrapMountError` is a pass-through, so no existing test behavior changes.)

- [ ] **Step 4: Verify the cross-compile still works**

Run:
```bash
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./pkg/client/mount/...
echo "darwin/arm64 mount build exit: $?"
```

Expected: exit 0.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/single.go
git commit -m "feat(client/mount): wrap go-fuse Mount errors with darwin install hint

Pipes the gofs.Mount return through wrapMountError so darwin users
without macFUSE / FUSE-T installed get a pointer at the install URLs
instead of a bare \"no such file or directory\" from go-fuse. Linux is
unaffected (wrapMountError is a pass-through there)."
```

---

## Task 4: Flip goreleaser to build darwin archives

**Files:**
- Modify: `.goreleaser.yaml:13-19` (replace the TODO block with `- darwin`)

- [ ] **Step 1: Apply the edit**

Read the current state:
```bash
sed -n '13,20p' .goreleaser.yaml
```

Expected current content:
```yaml
    goos:
      - linux
      # TODO(macos client support): flip to `- darwin` once
      # pkg/server/io/bound_fs.go is build-tagged for !linux so cmd/ compiles
      # on darwin. The ci.yml `build-darwin` job is the regression guard until
      # then; see the identity-permissions worktree (which heavily edits that
      # file — coordinate the split with that work).
    id: gMountie
```

Replace the TODO block with `- darwin`. The result should read:

```yaml
    goos:
      - linux
      - darwin
    id: gMountie
```

(The block of comment lines is removed; the rationale is now captured in `docs/design/operations-and-packaging.md` once the spec is consolidated.)

- [ ] **Step 2: Validate the goreleaser config**

Run:
```bash
~/go/bin/goreleaser check
```

Expected: `1 configuration file(s) validated` with no warnings.

- [ ] **Step 3: Snapshot-build a darwin target locally to prove the config works**

Run:
```bash
~/go/bin/goreleaser build --snapshot --clean --single-target --id gMountie \
  --output /tmp/gmountie-darwin-test 2>&1 | tail -5
```

This builds one target (the host platform — Linux). To explicitly verify the darwin combination, use `GOOS`/`GOARCH` overrides which goreleaser respects in single-target mode:

```bash
GOOS=darwin GOARCH=arm64 ~/go/bin/goreleaser build --snapshot --clean --single-target --id gMountie 2>&1 | tail -5
```

Expected: `build succeeded`. A `dist/gMountie_darwin_arm64*/` directory should exist with a `gMountie` binary.

Clean up:
```bash
rm -rf dist /tmp/gmountie-darwin-test
```

- [ ] **Step 4: Commit**

```bash
git add .goreleaser.yaml
git commit -m "build(goreleaser): ship darwin archives

Add darwin to goos (combined with the existing amd64+arm64 arches → 4
archives). dockers_v2 stays linux-only — we don't ship a macOS container
image. The TODO that gated this on the cmd/ darwin build is no longer
relevant: cmd/commands/serve.go is now build-tagged !linux so cmd/
compiles cleanly on darwin."
```

---

## Task 5: Expand the `build-darwin` CI job to cover `cmd/...` on both arches

**Files:**
- Modify: `.github/workflows/ci.yml:137-160` (the existing `build-darwin` job block)

- [ ] **Step 1: Read the current job**

Run:
```bash
sed -n '136,160p' .github/workflows/ci.yml
```

Expected:
```yaml
  build-darwin:
    # Cross-compile regression guard for client-side macOS support. Scoped to
    # ./pkg/client/... for now — ./cmd/... pulls in pkg/server/io/bound_fs.go,
    # which uses Linux-only syscall.Setfsuid/Setfsgid and won't compile on
    # darwin. Once that file is build-tagged for !linux (currently being edited
    # in the identity-permissions worktree, hands off), expand this to
    # `go build ./cmd/...` and uncomment `- darwin` in .goreleaser.yaml.
    runs-on: ubuntu-latest
    timeout-minutes: 5
    steps:
      - uses: actions/checkout@v6
      - name: Setup Go
        uses: actions/setup-go@v6
        with:
          go-version-file: 'go.mod'
          cache-dependency-path: 'go.sum'
      - name: Cross-compile darwin/arm64 (client)
        env:
          GOOS: darwin
          GOARCH: arm64
          CGO_ENABLED: '0'
        run: go build ./pkg/client/...
```

- [ ] **Step 2: Replace with the expanded matrix job**

Replace the whole block (from `build-darwin:` through the end of its last `run:`) with:

```yaml
  build-darwin:
    # Cross-compile regression guard for the macOS client. cmd/ compiles on
    # darwin because cmd/commands/serve.go is //go:build linux — the server
    # subcommand and the Linux-only syscalls it pulls in stay out of the
    # darwin build. Run on both arches so an amd64-only or arm64-only Linux
    # syscall regression still gets caught.
    runs-on: ubuntu-latest
    timeout-minutes: 5
    strategy:
      fail-fast: false
      matrix:
        goarch: [amd64, arm64]
    steps:
      - uses: actions/checkout@v6
      - name: Setup Go
        uses: actions/setup-go@v6
        with:
          go-version-file: 'go.mod'
          cache-dependency-path: 'go.sum'
      - name: Cross-compile darwin/${{ matrix.goarch }}
        env:
          GOOS: darwin
          GOARCH: ${{ matrix.goarch }}
          CGO_ENABLED: '0'
        run: go build ./cmd/...
```

Three things changed: scope (`./pkg/client/...` → `./cmd/...`), matrix over both arches with `fail-fast: false`, and the comment reflects the new reality.

- [ ] **Step 3: Validate with actionlint**

Run:
```bash
~/go/bin/actionlint .github/workflows/ci.yml
echo "actionlint exit: $?"
```

Expected: no output, exit 0.

- [ ] **Step 4: Run the same commands the matrix will run, locally**

Run:
```bash
GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/... && echo "amd64 ok"
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/... && echo "arm64 ok"
```

Expected: both print "ok".

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: expand build-darwin guard to ./cmd/... on amd64 + arm64

Now that cmd/commands/serve.go is //go:build linux, ./cmd/... compiles
on darwin without touching pkg/server/io. Matrix over both arches with
fail-fast: false so we see both failures if one arch regresses."
```

---

## Task 6: README and `docs/index.md` — platform statements

**Files:**
- Modify: `README.md:8` (badge), `README.md:25` (Linux-only banner), `README.md:29` (requirements line), `README.md:39` (releases mention)
- Modify: `docs/index.md:13` (Linux-only banner)

- [ ] **Step 1: Update the README platform badge (line 8)**

Replace:
```
  ![Platform](https://img.shields.io/badge/platform-Linux-informational)
```

with:
```
  ![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20(client)-informational)
```

(The `%20` is space, `%7C` is `|` — `Linux | macOS (client)`.)

- [ ] **Step 2: Update the Linux-only banner (line 25)**

Replace:
```
> gMountie is **alpha** and **Linux-only** today, with a single-user-ish trust model — TLS and security hardening are on the [roadmap](docs/roadmap.md). It's a great fit for mounting your own servers; it is not yet meant to face hostile networks.
```

with:
```
> gMountie is **alpha**, with a single-user-ish trust model — TLS and security hardening are on the [roadmap](docs/roadmap.md). The **server is Linux-only**; the **client mounts on Linux and macOS**. It's a great fit for mounting your own servers; it is not yet meant to face hostile networks.
```

- [ ] **Step 3: Update the Quick Start requirements line (line 29)**

Replace:
```
> The **server** just exposes folders — no special requirements. The **client** mounts via FUSE, so it needs `fuse3` (`/dev/fuse`) on Linux.
```

with:
```
> The **server** (Linux only) just exposes folders — no special requirements. The **client** mounts via FUSE: install `fuse3` (`/dev/fuse`) on Linux, or [**macFUSE**](https://macfuse.io) / [**FUSE-T**](https://www.fuse-t.org/) on macOS.
```

- [ ] **Step 4: Update the releases mention (line 39)**

Replace:
```
Prefer not to build? Grab a `gMountie_linux_*.tar.gz` from the [releases page](https://github.com/gMountie/gMountie/releases), or run the server from the container image `ghcr.io/gmountie/gmountie-server`.
```

with:
```
Prefer not to build? Grab a `gMountie_linux_*.tar.gz` (or `gMountie_darwin_*.tar.gz` for the macOS client) from the [releases page](https://github.com/gMountie/gMountie/releases), or run the server from the container image `ghcr.io/gmountie/gmountie-server`.
```

- [ ] **Step 5: Update the `docs/index.md` banner**

Read the current banner first:
```bash
sed -n '13p' docs/index.md
```

Expected current content:
```
> **Alpha · Linux-only.** gMountie is a great fit for mounting your own servers; it is not yet meant to face hostile networks. See the [roadmap](./roadmap.md) for TLS and security hardening.
```

Replace it with:
```
> **Alpha.** Server is Linux-only; the client mounts on Linux and macOS. gMountie is a great fit for mounting your own servers; it is not yet meant to face hostile networks. See the [roadmap](./roadmap.md) for TLS and security hardening.
```

- [ ] **Step 6: Spot-check the rendered files for typos**

Run:
```bash
grep -n "Linux-only\|Linux only" README.md docs/index.md
```

Expected: only matches in unrelated places (e.g. `Server is Linux-only` is fine; bare `Linux-only` standing for the whole product is not). Verify the only `Linux-only` hits scope the server, not the project.

```bash
grep -n "macfuse\|fuse-t\|FUSE-T\|macFUSE" README.md docs/index.md
```

Expected: README has the macFUSE/FUSE-T mentions; docs/index.md doesn't need them (the banner is short by design).

- [ ] **Step 7: Commit**

```bash
git add README.md docs/index.md
git commit -m "docs: README + index reflect macOS client support

Platform badge, alpha banner, Quick Start requirements, and releases
note all updated to say the server is Linux-only but the client mounts
on Linux and macOS. macFUSE and FUSE-T are both pointed at as install
options."
```

---

## Task 7: `docs/quickstart.mdx` — macOS install path

**Files:**
- Modify: `docs/quickstart.mdx:12-15` (Prerequisites admonition), `docs/quickstart.mdx:18-27` (Install section)

- [ ] **Step 1: Update the Prerequisites admonition**

Read the current state:
```bash
sed -n '12,16p' docs/quickstart.mdx
```

Expected:
```
:::note[Prerequisites]
- Linux on both ends. Server runs on amd64 or arm64; the client mounts via FUSE.
- `fuse3` installed on the client (`apt install fuse3` on Debian/Ubuntu).
- A folder you want to share on the server, plus an empty mount point on the client.
:::
```

Replace with:
```
:::note[Prerequisites]
- **Server:** Linux on amd64 or arm64.
- **Client:** Linux *or* macOS. FUSE on the client:
  - Linux: `fuse3` (`apt install fuse3` on Debian/Ubuntu).
  - macOS: [macFUSE](https://macfuse.io) *or* [FUSE-T](https://www.fuse-t.org/).
- A folder you want to share on the server, plus an empty mount point on the client.
:::
```

- [ ] **Step 2: Update the Install section release-archive line**

Read the current state:
```bash
sed -n '25,28p' docs/quickstart.mdx
```

Expected:
```
Or grab a `gmountie_linux_*.tar.gz` from the [releases page](https://github.com/gMountie/gMountie/releases). The binary is `gmountie` (lowercase); it's both the server and the client.
```

Replace with:
```
Or grab the release archive for your platform from the [releases page](https://github.com/gMountie/gMountie/releases) — `gmountie_linux_*.tar.gz` for Linux, `gmountie_darwin_*.tar.gz` for the macOS client. The binary is `gmountie` (lowercase); on Linux it's both the server and the client, and on macOS it's the client (the `serve` subcommand is Linux-only).
```

- [ ] **Step 3: Verify no other Linux-only assumptions in the file**

Run:
```bash
grep -niE "linux only|only.*linux|linux on both" docs/quickstart.mdx
```

Expected: no remaining matches that would mislead a macOS reader. (Step 2 of the mount instructions is server-side, so "run this on the server" is implicitly Linux — that's fine.)

- [ ] **Step 4: Commit**

```bash
git add docs/quickstart.mdx
git commit -m "docs(quickstart): split prerequisites by role; add macOS client path

Server prerequisite stays Linux; client prerequisite now covers both
Linux (fuse3) and macOS (macFUSE / FUSE-T). The Install section points
macOS readers at the darwin tarball, and notes the server subcommand is
Linux-only."
```

---

## Task 8: `docs/client/cli.md` — macOS FUSE note

**Files:**
- Modify: `docs/client/cli.md` (insert one sentence in the introductory paragraph)

- [ ] **Step 1: Read the current intro paragraph**

Run:
```bash
sed -n '9p' docs/client/cli.md
```

Expected:
```
`gmountie mount` connects to a server, opens a FUSE mount, and proxies every filesystem call to the server. CLI flags override the matching fields in **[client.yaml](./config.md)**, so you can keep the bulk of your settings in the config and override per-invocation.
```

- [ ] **Step 2: Add the macOS FUSE note**

Replace the line above with:
```
`gmountie mount` connects to a server, opens a FUSE mount, and proxies every filesystem call to the server. CLI flags override the matching fields in **[client.yaml](./config.md)**, so you can keep the bulk of your settings in the config and override per-invocation. The mount call is identical on Linux and macOS; on macOS the client needs [macFUSE](https://macfuse.io) or [FUSE-T](https://www.fuse-t.org/) installed first.
```

- [ ] **Step 3: Commit**

```bash
git add docs/client/cli.md
git commit -m "docs(client/cli): note macFUSE/FUSE-T requirement on macOS

One-sentence addition to the intro — mount flags and behavior are the same
on both platforms, but darwin needs a FUSE provider installed first."
```

---

## Task 9: End-to-end verification + open the PR

- [ ] **Step 1: Re-run the full validation matrix**

Run, in order:
```bash
cd /home/john/git/gMountie/.claude/worktrees/macos-client-support

# Darwin cross-compile both arches
GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/... && echo "darwin/amd64 ✓"
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/... && echo "darwin/arm64 ✓"

# Linux baseline still healthy
go build ./cmd/... && echo "linux build ✓"
go vet ./cmd/... && echo "linux vet ✓"
go test -count=1 ./pkg/client/mount/... && echo "mount tests ✓"
go test -race -count=1 -run TestWrapMountErrorTestSuite ./pkg/client/mount/ && echo "wrap race ✓"

# Config validators
~/go/bin/goreleaser check 2>&1 | tail -2
~/go/bin/actionlint .github/workflows/ci.yml && echo "actionlint ✓"
```

All commands should pass.

- [ ] **Step 2: Spot-check the darwin binary's subcommand set**

Run:
```bash
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/gMountie-darwin ./cmd
file /tmp/gMountie-darwin
```

Expected: `Mach-O 64-bit executable arm64`.

Use `go tool nm` to confirm `serveCmd` is *not* present in the darwin build (since `serve.go` was excluded):

```bash
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go build -o /tmp/gMountie-darwin ./cmd
GOOS=linux  GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/gMountie-linux  ./cmd
go tool nm /tmp/gMountie-darwin | grep -c serveCmd
go tool nm /tmp/gMountie-linux  | grep -c serveCmd
```

Expected: darwin shows `0`, linux shows ≥1. Clean up:

```bash
rm /tmp/gMountie-darwin /tmp/gMountie-linux
```

- [ ] **Step 3: Review the commit log**

Run:
```bash
git log --oneline origin/master..HEAD
```

Expected 8 commits (one per task), in order: serve build tag, wrapMountError helper, wrap wired into single.go, goreleaser darwin, ci expand, README/index, quickstart, client/cli. Plus the spec commit (`dbdd915`) and plan commit if you committed that too.

- [ ] **Step 4: Commit the plan itself** *(only if not already committed)*

```bash
git add docs/superpowers/plans/2026-05-28-macos-client-support.md
git diff --cached --quiet || git commit -m "docs(plan): minimal macOS client support implementation plan"
```

- [ ] **Step 5: Push the branch**

```bash
git push -u origin worktree-macos-client-support
```

Expected: branch created on origin.

- [ ] **Step 6: Open the PR**

```bash
gh pr create --base master --head worktree-macos-client-support \
  --title "feat: minimal macOS client support" \
  --body "$(cat <<'EOF'
Implements the design in \`docs/superpowers/specs/2026-05-28-macos-client-support-design.md\`. Single PR covering code, ship, and docs.

## What ships
- **Single \`gMountie\` binary** with build-tag-gated subcommands: linux gets \`serve\`/\`mount\`/\`version\`, darwin gets \`mount\`/\`version\`. \`cmd/commands/serve.go\` gets \`//go:build linux\` — its \`init()\` simply doesn't register on darwin, so \`pkg/server/*\` is never compiled there and we never have to stub \`bound_fs.go\` or \`confined.go\`.
- **Runtime UX** — \`pkg/client/mount.wrapMountError\` turns a missing-FUSE-driver error on darwin into an "install macFUSE or FUSE-T" hint. Pass-through on Linux and on darwin-but-unrelated errors. Table-driven testify suite covers nil, linux, darwin-unrelated, and five missing-provider patterns (race-clean).
- **Goreleaser** ships \`gMountie_darwin_{amd64,arm64}.tar.gz\` alongside the existing Linux archives. dockers_v2 stays linux-only.
- **CI** \`build-darwin\` job expanded from \`./pkg/client/...\` to \`./cmd/...\` with a \`{amd64, arm64}\` matrix and \`fail-fast: false\`.
- **Docs** corrected where they assumed Linux-only: README badge / banner / requirements / releases, \`docs/index.md\` banner, \`docs/quickstart.mdx\` prerequisites + install, \`docs/client/cli.md\` intro.

## Out of scope
Homebrew tap, Mac runner integration smoke, code signing / notarization, universal binary, Wails UI on macOS, server functionality on macOS. (See spec.)

## Verified locally
- \`GOOS=darwin GOARCH={amd64,arm64} go build ./cmd/...\` both succeed
- \`go test -race -run TestWrapMountErrorTestSuite ./pkg/client/mount/\` passes
- \`goreleaser check\` clean
- \`actionlint\` clean
- \`nm\` confirms \`serveCmd\` symbol is absent from the darwin binary, present in the linux one
- \`task build\` (linux snapshot) still works
EOF
)"
```

Expected: prints the PR URL.

- [ ] **Step 7: Post-merge follow-up reminder**

After the PR merges, per the standing rule ([[project_doc_organization]]):
1. Consolidate the durable bits from the spec into a short "Platform support" section in `docs/design/operations-and-packaging.md`.
2. Delete `docs/superpowers/specs/2026-05-28-macos-client-support-design.md` and `docs/superpowers/plans/2026-05-28-macos-client-support.md`.

(This is a follow-up PR, *not* part of this implementation PR.)

---

## Self-review checklist (run before declaring the plan done)

**Spec coverage:** every item in the spec maps to a task:
- Server-on-darwin = absent via `//go:build linux` on `serve.go` → **Task 1** ✓
- `wrapMountError` helper + tests → **Task 2** ✓
- Wire wrap at the `gofs.Mount` call site → **Task 3** ✓
- Goreleaser `goos: darwin` → **Task 4** ✓
- `build-darwin` CI expanded to `./cmd/...` × `{amd64,arm64}` → **Task 5** ✓
- README (badge, banner, requirements, releases) + `docs/index.md` banner → **Task 6** ✓
- `docs/quickstart.mdx` prerequisites + install → **Task 7** ✓
- `docs/client/cli.md` FUSE note → **Task 8** ✓
- E2E verify + PR → **Task 9** ✓
- `docs/client/config.md` macOS-path note: **intentionally omitted** — verified there are no Linux-specific config paths in that file (per design spec's "may be a no-op once we look").

**Type consistency:** `wrapMountError(err error) error` and `currentGOOS string` are used identically in `mount_error.go`, `mount_error_test.go`, and `single.go`. No drift.

**No placeholders.** Every step shows exact files, exact code, exact commands.
