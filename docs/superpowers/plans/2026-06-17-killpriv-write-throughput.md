# HANDLE_KILLPRIV_V2 Write-Throughput Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Advertise the FUSE `CAP_HANDLE_KILLPRIV_V2` capability (gated by a new client config knob, default on) so the kernel sets `S_NOSEC` and stops issuing a `security.capability` getxattr on every write — restoring ~+75% single-file WAN write throughput with no security regression.

**Architecture:** A new `FUSEConfig.HandleKillPriv bool` (default true), mirroring the existing `WritebackCache` knob byte-for-byte, drives a single `opts.ExtraCapabilities |= fuse.CAP_HANDLE_KILLPRIV_V2` line in `createMountOptions`. The kernel then handles suid/sgid/cap stripping via `setattr` (the server applies the chmod), which an e2e test pins as a permanent security gate.

**Tech Stack:** Go, `github.com/hanwen/go-fuse/v2` (v2.10.1), Viper + `go-playground/validator` config, testify suites.

**Reference spec:** `docs/superpowers/specs/2026-06-17-killpriv-write-throughput-design.md`

**Worktree:** `gMountie/.claude/worktrees/killpriv-write-fix` (branch `killpriv-write-fix`, off `origin/master`). All paths below are relative to that worktree root. Run all `git`/`go` commands from there.

**Pre-existing red CI note:** `origin/master` currently fails `TestCacheSubscribeFSSuite/TestPushInvalidatesAcrossClients` (a separate async-persist cache race, fixed in a later PR). That failure is unrelated to this work and will appear in this branch's CI until the cache race lands. Do NOT attempt to fix it here.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `pkg/client/config/fuse.go` | FUSE config knobs + defaults | add `DefaultFUSEHandleKillPriv` const, `HandleKillPriv` field, default literal + `SetDefault` |
| `pkg/client/config/fuse_test.go` | FUSEConfig parse tests | add default-on + override-false tests |
| `pkg/client/config/config.go` | env-var mirroring into sub-trees | add `"handle_kill_priv"` to the `fuse` mirror list |
| `pkg/client/mount/common.go` | builds `fuse.MountOptions` | set `CAP_HANDLE_KILLPRIV_V2` when knob on |
| `pkg/client/mount/common_test.go` | mount-option unit tests (`package mount`) | new suite asserting the cap bit on/off |
| `test/e2e/fs/killpriv_test.go` | e2e suid/sgid strip gate | new file |
| `docs/design/security-and-transport.md` | durable security design | document the killpriv negotiation |
| `docs/design/` config reference (the FUSE knobs doc) | user-facing knob docs | document `fuse.handle_kill_priv` |

---

### Task 1: Config knob in `FUSEConfig`

**Files:**
- Modify: `pkg/client/config/fuse.go`
- Test: `pkg/client/config/fuse_test.go`

- [ ] **Step 1: Write the failing tests**

Add to `pkg/client/config/fuse_test.go` (the suite is `FUSEConfigSuite`, already in the file). These mirror the existing `TestDirectIODefaultsOff` / `TestDirectIOOverride` pattern exactly:

```go
// TestHandleKillPrivDefaultsOn verifies the cap defaults ON (nil and empty
// viper sub-trees), so every mount gets the per-write-getxattr fix without
// configuration.
func (s *FUSEConfigSuite) TestHandleKillPrivDefaultsOn() {
	cfg, err := NewFUSEConfig(nil)
	s.Require().NoError(err)
	s.True(cfg.HandleKillPriv, "handle_kill_priv must default on")

	cfg, err = NewFUSEConfig(viper.New())
	s.Require().NoError(err)
	s.True(cfg.HandleKillPriv, "empty viper sub-tree must default handle_kill_priv on")
}

// TestHandleKillPrivOverride verifies fuse.handle_kill_priv: false round-trips
// (an explicit opt-out is honored, not clobbered by the default).
func (s *FUSEConfigSuite) TestHandleKillPrivOverride() {
	v := viper.New()
	v.Set("handle_kill_priv", false)
	cfg, err := NewFUSEConfig(v)
	s.Require().NoError(err)
	s.False(cfg.HandleKillPriv)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -run 'TestFUSEConfigSuite/TestHandleKillPriv' ./pkg/client/config/ -v`
Expected: FAIL to compile — `cfg.HandleKillPriv undefined (type *FUSEConfig has no field or method HandleKillPriv)`.

- [ ] **Step 3: Add the default constant**

In `pkg/client/config/fuse.go`, inside the `const (...)` block, after `DefaultFUSEEntryTimeout`:

```go
	// DefaultFUSEHandleKillPriv advertises CAP_HANDLE_KILLPRIV_V2 to the
	// kernel by default. Without it the kernel issues a security.capability
	// getxattr on EVERY write (to strip setuid/setgid/file-caps on modify),
	// which a FUSE mount forwards as one GetXattr RPC per write — a
	// round-trip that caps single-file write throughput on high-RTT links
	// (~+75% measured once removed). With the cap set the kernel marks the
	// inode S_NOSEC and instead performs the suid/sgid/cap strip via a
	// setattr the server applies, so the per-write getxattr disappears with
	// no loss of privilege-stripping. Older kernels that do not support the
	// cap simply ignore it. Opt out with fuse.handle_kill_priv: false.
	DefaultFUSEHandleKillPriv = true
```

- [ ] **Step 4: Add the struct field**

In `FUSEConfig`, after the `WritebackCache` field (keep it adjacent to the other capability-bit knob):

```go
	// HandleKillPriv advertises CAP_HANDLE_KILLPRIV_V2 to the kernel when
	// true (the default). It removes a per-write security.capability getxattr
	// round-trip; the kernel still strips setuid/setgid/file-caps on modify
	// via a setattr the server applies. Set false only to fall back to the
	// kernel's per-write getxattr behavior (e.g. to debug a backing FS that
	// mishandles the capability).
	HandleKillPriv bool `mapstructure:"handle_kill_priv"`
```

- [ ] **Step 5: Wire the default into `NewFUSEConfig`**

Two edits inside `NewFUSEConfig`, mirroring `WritebackCache`:

In the `cfg := &FUSEConfig{...}` literal, after `WritebackCache: DefaultFUSEWritebackCache,`:

```go
		HandleKillPriv: DefaultFUSEHandleKillPriv,
```

In the `v.SetDefault(...)` block, after `v.SetDefault("writeback_cache", DefaultFUSEWritebackCache)`:

```go
	v.SetDefault("handle_kill_priv", DefaultFUSEHandleKillPriv)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test -run 'TestFUSEConfigSuite' ./pkg/client/config/ -v`
Expected: PASS (including the two new tests and the existing ones).

- [ ] **Step 7: Commit**

```bash
git add pkg/client/config/fuse.go pkg/client/config/fuse_test.go
git commit -m "feat(client): add fuse.handle_kill_priv config knob (default on)"
```

---

### Task 2: Mirror the knob for env-var configuration

**Files:**
- Modify: `pkg/client/config/config.go`
- Test: `pkg/client/config/fuse_test.go` (reuse the override test by env — optional assert below)

- [ ] **Step 1: Add the env-mirror key**

In `pkg/client/config/config.go`, in the `fuseSub := mirrorEnvToSub(v, "fuse", []string{...})` slice, add `"handle_kill_priv"` after `"entry_timeout"`:

```go
	fuseSub := mirrorEnvToSub(v, "fuse", []string{
		"max_write_bytes",
		"max_background",
		"writeback_cache",
		"direct_io",
		"attr_timeout",
		"entry_timeout",
		"handle_kill_priv",
	})
```

- [ ] **Step 2: Verify the package still builds and parses**

Run: `go test ./pkg/client/config/ -v -run 'TestFUSEConfigSuite|TestConfig'`
Expected: PASS. (`GMOUNTIE_FUSE_HANDLE_KILL_PRIV` now mirrors into the `fuse` sub-tree like the other FUSE knobs; the existing config tests exercise the mirror plumbing.)

- [ ] **Step 3: Commit**

```bash
git add pkg/client/config/config.go
git commit -m "feat(client): mirror GMOUNTIE_FUSE_HANDLE_KILL_PRIV into the fuse sub-tree"
```

---

### Task 3: Advertise the capability at mount time

**Files:**
- Modify: `pkg/client/mount/common.go`
- Test: `pkg/client/mount/common_test.go` (`package mount` — can call the unexported `createMountOptions`)

- [ ] **Step 1: Write the failing test**

Add a new suite to `pkg/client/mount/common_test.go`. It calls the unexported `createMountOptions` directly (the test file is `package mount`). `createMountOptions(endpoint, volume string, cfg *config.FUSEConfig, maxWriteBytes int) *fuse.MountOptions`.

```go
// CreateMountOptionsSuite tests capability-bit wiring in createMountOptions.
type CreateMountOptionsSuite struct{ suite.Suite }

func (s *CreateMountOptionsSuite) baseCfg() *config.FUSEConfig {
	return &config.FUSEConfig{
		MaxWriteBytes:  config.DefaultFUSEMaxWriteBytes,
		MaxBackground:  config.DefaultFUSEMaxBackground,
		WritebackCache: false,
		AttrTimeout:    config.DefaultFUSEAttrTimeout,
		EntryTimeout:   config.DefaultFUSEEntryTimeout,
	}
}

func (s *CreateMountOptionsSuite) TestHandleKillPrivOnSetsCap() {
	cfg := s.baseCfg()
	cfg.HandleKillPriv = true
	opts := createMountOptions("127.0.0.1:9449", "vol", cfg, config.DefaultFUSEMaxWriteBytes)
	s.NotZero(opts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV_V2,
		"HandleKillPriv=true must set CAP_HANDLE_KILLPRIV_V2 in ExtraCapabilities")
}

func (s *CreateMountOptionsSuite) TestHandleKillPrivOffLeavesCapUnset() {
	cfg := s.baseCfg()
	cfg.HandleKillPriv = false
	opts := createMountOptions("127.0.0.1:9449", "vol", cfg, config.DefaultFUSEMaxWriteBytes)
	s.Zero(opts.ExtraCapabilities&fuse.CAP_HANDLE_KILLPRIV_V2,
		"HandleKillPriv=false must leave CAP_HANDLE_KILLPRIV_V2 unset")
}

func TestCreateMountOptionsSuite(t *testing.T) {
	suite.Run(t, new(CreateMountOptionsSuite))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -run 'TestCreateMountOptionsSuite' ./pkg/client/mount/ -v`
Expected: FAIL — `TestHandleKillPrivOnSetsCap` fails its assertion (bit not set), because nothing sets `CAP_HANDLE_KILLPRIV_V2` yet.

- [ ] **Step 3: Set the capability in `createMountOptions`**

In `pkg/client/mount/common.go`, in `createMountOptions`, after the existing `if cfg.WritebackCache { ... } else { ... }` block and before `return opts`:

```go
	// Advertise HANDLE_KILLPRIV_V2 so the kernel marks files S_NOSEC and stops
	// issuing a security.capability getxattr on every write (one GetXattr RPC
	// per write — a WAN throughput killer). The kernel still strips
	// setuid/setgid/file-caps on modify via a setattr the server applies, so
	// there is no loss of privilege-stripping. Kernels without the cap ignore
	// it. No DisabledCapabilities branch is needed: the bit is absent from
	// go-fuse's default capability allowlist, so "unset" already means off.
	if cfg.HandleKillPriv {
		opts.ExtraCapabilities |= fuse.CAP_HANDLE_KILLPRIV_V2
	}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -run 'TestCreateMountOptionsSuite' ./pkg/client/mount/ -v`
Expected: PASS (both cases).

- [ ] **Step 5: Run the whole mount package to catch regressions**

Run: `go test ./pkg/client/mount/`
Expected: `ok` (FUSE-mounting tests in this package are unit-level / use `buildFSOptions`; if any require a real mount and the environment lacks FUSE they skip, but no failures).

- [ ] **Step 6: Commit**

```bash
git add pkg/client/mount/common.go pkg/client/mount/common_test.go
git commit -m "feat(client): advertise CAP_HANDLE_KILLPRIV_V2 at mount time

Removes the per-write security.capability getxattr round-trip; gated by
fuse.handle_kill_priv (default on)."
```

---

### Task 4: e2e suid/sgid strip security gate

**Files:**
- Create: `test/e2e/fs/killpriv_test.go`

This proves the cap (on by default) does NOT cause setuid/setgid retention: with
the cap on, modifying a suid/sgid file still strips the bits on the backing file.
See the spec §5 for why it is passthrough-only, backing-file-checked, and
skip-if-root (the kernel only strips for a non-root writer; CI runs non-root).

- [ ] **Step 1: Write the test (it is the failing artifact — it exercises the new default)**

Create `test/e2e/fs/killpriv_test.go`:

```go
package fs

import (
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// KillPrivFSSuite verifies that with CAP_HANDLE_KILLPRIV_V2 advertised (the
// default, fuse.handle_kill_priv=true), modifying a file that has setuid/setgid
// bits still STRIPS those bits on the backing file. The cap delegates
// priv-stripping to the filesystem; this is the regression guard that the
// delegation does not silently retain the bits.
//
// Scope (passthrough mapping, non-root writer): the kernel only strips
// setuid/setgid when the writing process lacks CAP_FSETID, i.e. is non-root.
// CI and the local VM run the test process non-root, where the strip is
// observable. When run as root the test skips (root retains CAP_FSETID, so no
// strip would occur and the assertion would be meaningless). The strip is
// mapping-mode-independent (kernel issues a setattr the server applies), so a
// per-mode matrix adds no signal — see the design doc.
type KillPrivFSSuite struct {
	suite.Suite
	ctx    *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *KillPrivFSSuite) SetupSuite() {
	if os.Geteuid() == 0 {
		s.T().Skip("killpriv strip is only observable for a non-root writer " +
			"(root retains CAP_FSETID and the kernel skips file_remove_privs); " +
			"run this test as a non-root user")
	}
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false), // passthrough mapping, no pre-created files
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.ctx = ctx
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *KillPrivFSSuite) TearDownSuite() {
	if s.ctx != nil {
		s.Require().NoError(s.ctx.Close())
	}
}

// assertStripped writes a file through the mount with the given mode (which
// includes a setuid and/or setgid bit), modifies it, and asserts the special
// bits are cleared on the BACKING file (server-side source dir).
func (s *KillPrivFSSuite) assertStripped(name string, mode os.FileMode) {
	mp := s.volume.GetMountPath()
	src := s.volume.GetSrcPath()
	mntPath := filepath.Join(mp, name)
	backingPath := filepath.Join(src, name)

	// Create + set the special bits through the mount.
	s.Require().NoError(os.WriteFile(mntPath, []byte("aaa"), 0o644))
	s.Require().NoError(os.Chmod(mntPath, mode))

	// Confirm the bits are actually set on the backing file before the write
	// (otherwise the post-write assertion proves nothing).
	pre, err := os.Stat(backingPath)
	s.Require().NoError(err)
	special := os.ModeSetuid | os.ModeSetgid
	s.Require().NotZero(pre.Mode()&special&mode,
		"precondition: special bits must be set on the backing file before the write")

	// Modify the file (a plain write triggers the kernel's file_remove_privs).
	f, err := os.OpenFile(mntPath, os.O_WRONLY, 0)
	s.Require().NoError(err)
	_, werr := f.WriteAt([]byte("bbb"), 0)
	s.Require().NoError(werr)
	s.Require().NoError(f.Sync())
	s.Require().NoError(f.Close())

	// The setuid/setgid bits requested in `mode` must be cleared on the
	// backing file.
	post, err := os.Stat(backingPath)
	s.Require().NoError(err)
	s.Zero(post.Mode()&special,
		"setuid/setgid must be stripped on the backing file after a non-root write "+
			"(requested mode %o, backing mode after write %o)", mode, post.Mode())
}

func (s *KillPrivFSSuite) TestSetuidStrippedOnWrite() {
	s.assertStripped("suidfile", os.FileMode(0o755)|os.ModeSetuid)
}

func (s *KillPrivFSSuite) TestSetgidStrippedOnWrite() {
	s.assertStripped("sgidfile", os.FileMode(0o755)|os.ModeSetgid)
}

func (s *KillPrivFSSuite) TestSetuidSetgidStrippedOnWrite() {
	s.assertStripped("suidsgidfile", os.FileMode(0o755)|os.ModeSetuid|os.ModeSetgid)
}

func TestKillPrivFSSuite(t *testing.T) {
	suite.Run(t, new(KillPrivFSSuite))
}
```

- [ ] **Step 2: Run the test to verify it passes (cap is on by default)**

Run: `go test -run 'TestKillPrivFSSuite' ./test/e2e/fs/ -v`

Expected (non-root, with /dev/fuse): PASS — all three subtests strip the bits.
Expected (root): all three SKIP with the logged reason.

If running where FUSE is unavailable (sandbox/GoLand), the mount fails; run this
on the kubevirt/Multipass VM or a plain terminal with `/dev/fuse` (see the FUSE
test-env note in project memory). This is an existing constraint for all
`test/e2e/fs` suites, not specific to this test.

- [ ] **Step 3: Negative-control check (confirm the test would catch retention)**

This is a one-off manual sanity check, NOT a committed change. Temporarily set
`fuse.handle_kill_priv` plumbing aside and instead, in
`pkg/client/mount/common.go`, comment out the new `opts.ExtraCapabilities |=
fuse.CAP_HANDLE_KILLPRIV_V2` line, rerun the test, and confirm it STILL passes
(stripping also happens via the kernel's getxattr path when the cap is off — so
this test guards against retention in BOTH modes, which is what we want). Restore
the line. No commit from this step.

Rationale: the test asserts the *safety invariant* (bits always stripped), which
must hold whether the cap is on or off. The throughput win itself is proven by
the VM benchmark recorded in the design doc, not by this test.

- [ ] **Step 4: Commit**

```bash
git add test/e2e/fs/killpriv_test.go
git commit -m "test(e2e): suid/sgid stripping survives CAP_HANDLE_KILLPRIV_V2

Regression guard that advertising the cap does not retain setuid/setgid on
modify. Passthrough mapping, non-root writer, backing-file assertion; skips
when run as root (root retains CAP_FSETID)."
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/design/security-and-transport.md`
- Modify: the FUSE-knobs config reference doc (find it — see Step 1)

- [ ] **Step 1: Locate the config-reference doc for FUSE knobs**

Run: `grep -rln "writeback_cache\|direct_io\|attr_timeout" docs/ website/ 2>/dev/null`
Expected: lists the doc(s) that document the `fuse.*` knobs (e.g. a configuration
reference page). Pick the user-facing reference that lists `writeback_cache` /
`direct_io`; that is where `handle_kill_priv` belongs.

- [ ] **Step 2: Document the knob in the config reference**

Next to the `fuse.writeback_cache` / `fuse.direct_io` entries, add an entry for
`fuse.handle_kill_priv` using the same formatting as its neighbors:

```
fuse.handle_kill_priv (bool, default true): advertise CAP_HANDLE_KILLPRIV_V2 to
the kernel. When on, the kernel stops issuing a security.capability getxattr on
every write (one RPC per write — a WAN write-throughput killer) and instead
strips setuid/setgid/file-capabilities on modify via a setattr the server
applies. Leave on unless a backing filesystem mishandles the capability.
Env: GMOUNTIE_FUSE_HANDLE_KILL_PRIV.
```

(Match the exact prose/format style of the surrounding entries; the above is the
required content, not a verbatim format.)

- [ ] **Step 3: Document the security interaction in the design doc**

In `docs/design/security-and-transport.md`, in the section covering the
identity-bound filesystem / write path (search for `setfsuid` or `killpriv` or
the write/transport section), add a short subsection:

```
### Per-write privilege stripping (HANDLE_KILLPRIV_V2)

The client advertises the FUSE CAP_HANDLE_KILLPRIV_V2 capability by default
(fuse.handle_kill_priv). Without it, the kernel issues a security.capability
getxattr on every write to decide whether to strip setuid/setgid/file-caps on
modify; over a FUSE mount that is one GetXattr RPC per write and caps single-file
write throughput on high-RTT links. With the cap, the kernel marks the inode
S_NOSEC and instead sends a setattr that clears the privilege bits, which the
identity-bound server applies as the resolved principal. The privilege bits are
therefore still stripped on modify (verified across squash/static/system/
passthrough mappings on the test VM); the cap removes the per-write getxattr, not
the stripping. The kernel still performs file_remove_privs in the writing
process's context, so a write by a CAP_FSETID-holding (root) process is exempt
from stripping exactly as on a local filesystem.
```

- [ ] **Step 4: Verify docs build (if a docs build target exists)**

Run: `grep -n "docs\|website" Taskfile.yaml | head`
If a docs lint/build task exists (e.g. for the Docusaurus site in `website/`), run
it; otherwise visually confirm the Markdown renders (no broken tables/headings).
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -m "docs: document fuse.handle_kill_priv and the killpriv security model"
```

---

## Final verification (after all tasks)

- [ ] **Gate the touched packages** (per the repo's local-gate convention — test the union of touched packages, since `./...` can't run locally without FUSE):

```bash
go build ./...
go test ./pkg/client/config/ ./pkg/client/mount/
go vet ./pkg/client/config/ ./pkg/client/mount/ ./test/e2e/fs/
go run github.com/golangci/golangci-lint/cmd/golangci-lint run pkg/client/config/... pkg/client/mount/... test/e2e/fs/...
```
Expected: all green. (`test/e2e/fs` requires FUSE to *run*; `build`/`vet`/`lint`
do not, so they gate it here. Run `TestKillPrivFSSuite` itself on a FUSE-capable
host per Task 4 Step 2.)

- [ ] **Confirm the only failing e2e is the pre-existing cache race** (if running the full e2e on a FUSE host): `TestCacheSubscribeFSSuite/TestPushInvalidatesAcrossClients` may fail — that is the unrelated, separately-tracked async-persist bug, NOT this PR. Everything else green.

- [ ] **Open the PR** with a body summarizing: the per-write getxattr root cause, the +~75% VM measurement, the security verification (suid/sgid/suid+sgid stripping preserved across all four mapping modes on the VM), and the default-on config knob. NO AI attribution (per repo convention).

---

## Self-Review (completed by plan author)

- **Spec coverage:** §1 config knob → Task 1; §2 env plumbing → Task 2; §3 cap wiring → Task 3; §4 unit tests → Tasks 1 & 3; §5 e2e gate → Task 4 (scoped to passthrough + non-root per the 2026-06-17 decision); §6 docs → Task 5. All covered.
- **Placeholders:** none — every code step has complete code; the only "find it" step (Task 5 Step 1) is a grep with a defined selection rule, not a placeholder.
- **Type/name consistency:** `HandleKillPriv` / `DefaultFUSEHandleKillPriv` / `handle_kill_priv` / `CAP_HANDLE_KILLPRIV_V2` used consistently across Tasks 1–4. `createMountOptions` signature matches `common.go`. Test suite names unique (`CreateMountOptionsSuite`, `KillPrivFSSuite`).
