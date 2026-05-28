# Phase 3 — Admin Capabilities Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `dac_read_search` and `dac_override` capabilities to the identity model so service accounts can list/read or modify all volumes regardless of identity, with kernel-native enforcement and a per-thread cred dance — gated on Phase 2 confinement (#42, shipped).

**Architecture:** Extend the existing `identityBoundFS` (pkg/server/io/bound_fs.go) with three dispatch paths chosen by the resolved Identity's Caps: (1) no caps → current setfsuid/setfsgid/setgroups dance; (2) `dac_read` → keep `fsuid=0`, raw `capset` to retain only `CAP_DAC_READ_SEARCH` (+ `CAP_SETUID/SETGID` for restore), drop `DAC_OVERRIDE/FOWNER/FSETID`; (3) `dac_override` → keep `fsuid=0` + full caps, and `fchown` newly-created entries to the principal so admin writes don't leak root-ownership. Caps come from resolvers: `system` derives them from server-side group membership via the new `admin_groups` config; `static` reads them from per-user `caps:`.

**Tech Stack:** Go, `golang.org/x/sys/unix` (Capset/Capget syscalls), raw `SYS_capset` for per-thread caps, go-fuse v2 pathfs, testify suites.

---

## File Structure

- **Modify**: `pkg/server/io/bound_fs.go` — split `changeIdentity` into three apply functions; add capset wrapper; for `dac_override` ops that create entries (Create/Mkdir/Symlink/Mknod/Link), Lstat after success and fchown to the principal.
- **Modify**: `pkg/server/io/bound_fs.go` `Identity` struct — add `Caps []string`.
- **Modify**: `pkg/server/service/volume.go` `BindIdentity` — propagate `Caps` from service.Identity to io.Identity.
- **Modify**: `pkg/server/service/resolver_static.go` — already populates Caps from config; no change unless tests reveal a gap.
- **Modify**: `pkg/server/service/resolver_system.go` — accept the volume's MappingConfig (for admin_groups) at construction; populate Caps from the principal's group membership against admin_groups.
- **Modify**: `pkg/server/config/volumes.go` — add `AdminGroups map[string][]string` to MappingConfig (system mode only); validation.
- **Create**: `pkg/server/io/caps.go` — capset wrapper(s); cap constants (CapDacRead, CapDacOverride) and the cap-bit masks; `dropToCapSet(keep)` helper.
- **Create**: `pkg/server/io/bound_fs_caps_test.go` — testify suite for the three dispatch paths (VM/root-required cases skipped when not root).
- **Modify**: `pkg/server/io/bound_fs_creds_test.go` — extend kernel-verification suite to assert the cap paths.
- **Create**: `test/e2e/fs/admin_caps_test.go` — VM e2e covering both caps end-to-end.

---

## Task Granularity

### Task 1: PROOF — per-thread capset works as advertised

**Files:** `pkg/server/io/caps_proof_test.go` (new, deleted after T5 if redundant)

- [ ] **Write the failing test** asserting:
  - Pin to thread; raw `SYS_capset` to keep only `CAP_DAC_READ_SEARCH + CAP_SETUID + CAP_SETGID` in effective+permitted.
  - Open & read a 0o600 file owned by uid 2; expect success (DAC_READ_SEARCH bypasses DAC).
  - Open same file for O_WRONLY; expect EACCES (DAC_OVERRIDE dropped).
  - Restore full caps; thread can write again.
- [ ] **Run** with `go test -count=1 ./pkg/server/io/ -run TestCapsProof` on the VM as root. Expect PASS — confirms primitive.
- [ ] If FAIL: STOP and report; the design assumption needs revisiting.
- [ ] **Commit**: `test(server/io): prove per-thread capset(DAC_READ_SEARCH only) blocks DAC_OVERRIDE`

### Task 2: Caps plumbing (io.Identity + BindIdentity)

**Files:** `pkg/server/io/bound_fs.go`, `pkg/server/service/volume.go`, `pkg/server/io/bound_fs_test.go`

- [ ] **Write the failing test** asserting BindIdentity carries Caps from service.Identity into io.Identity (for static and squash modes — squash has none).
- [ ] **Implement**: add `Caps []string` to `io.Identity`; copy in BindIdentity.
- [ ] **Verify**: existing bound_fs tests still pass; new Caps field tests pass.
- [ ] **Commit**: `feat(server): plumb identity caps through BindIdentity`

### Task 3: System resolver — admin_groups derives caps

**Files:** `pkg/server/config/volumes.go`, `pkg/server/service/resolver_system.go`, `pkg/server/service/resolver_system_test.go`, `pkg/server/config/volumes_test.go`

- [ ] **Write the failing test** for system resolver: principal in group `wheel` and config `admin_groups: { dac_override: [wheel] }` → Identity.Caps contains "dac_override". Principal NOT in any admin group → empty Caps.
- [ ] **Add config field** `AdminGroups map[string][]string `mapstructure:"admin_groups"`` to MappingConfig with validation: only valid cap names (`dac_read_search`, `dac_override`), only for system mode.
- [ ] **Change** `NewSystemResolver` to accept `MappingConfig` so it can read admin_groups; update the call site in volume.go.
- [ ] **Implement**: after resolving the principal's gids/group-names, walk admin_groups: if any group name in `groupNames` matches a list under a cap, add that cap.
- [ ] **Verify**: unit tests pass.
- [ ] **Commit**: `feat(server): system resolver derives caps from admin_groups membership`

### Task 4: Static resolver — caps already plumbed, verify

**Files:** `pkg/server/service/resolver_static_test.go`

- [ ] **Write the test** asserting a static-mode user with `caps: [dac_read_search]` produces Identity.Caps containing that value.
- [ ] If already green (since the field exists): just add the assertion. If not: trace and fix.
- [ ] **Commit** (only if file changed): `test(server): cover static resolver Caps plumbing`

### Task 5: identityBoundFS — three-path cred dispatch + post-create fchown

**Files:** `pkg/server/io/bound_fs.go`, `pkg/server/io/caps.go` (new), `pkg/server/io/bound_fs_caps_test.go` (new), `pkg/server/io/bound_fs_creds_test.go` (extend)

- [ ] **Write failing tests** (VM-only / skipped when not root):
  - `dac_read` path: bound FS can read a 0o600 root-owned file as an unprivileged identity, but writing it returns EACCES.
  - `dac_override` path: bound FS can write a 0o600 root-owned file. After a `Create` op, the new file is owned by the principal's uid/gid (not root).
- [ ] **Create** `pkg/server/io/caps.go` with:
  - String constants `CapDacRead`, `CapDacOverride`.
  - `dropCaps(keep []uintptr) error` — raw `SYS_capset` against the calling thread (caps are per-thread on Linux).
- [ ] **Refactor** `changeIdentity` into three apply paths; pick by `id.Caps`. Each returns a `cleanup func()` matching today's contract. The unprivileged path stays bit-for-bit identical to today's code (no behavior change in the no-caps case).
- [ ] **For `dac_override`**, wrap entry-creating ops (Create/Mkdir/Symlink/Mknod/Link) so after success the new entry is `fchownat(AT_FDCWD, path, id.Uid, id.Gid, AT_SYMLINK_NOFOLLOW)`. On chown failure: log + leave root-owned (the spec's "admin-path edge logged for manual cleanup").
- [ ] **Verify** all tests pass; existing bound_fs suite still green.
- [ ] **Commit**: `feat(server/io): cap-aware bound FS — dac_read + dac_override paths`

### Task 6: VM e2e — admin caps end-to-end

**Files:** `test/e2e/fs/admin_caps_test.go`

- [ ] **Write tests** that spin up a server as root with a static-mode volume defining two users: `reader` with `caps:[dac_read_search]` and `admin` with `caps:[dac_override]`. As reader: cat a 0o600 root-owned file (PASS), write same (EACCES). As admin: write same (PASS), create a new file (PASS) and verify its owner is the admin's uid (not root).
- [ ] **Run on VM**; expect all PASS.
- [ ] **Commit**: `test(e2e/fs): admin caps end-to-end (dac_read_search + dac_override)`

---

## Self-Review

- **Spec coverage**: §3.2 (admin_groups), §3.4 (capset path), §3.11 (dac_override gated by confinement) all covered.
- **No placeholders**: each step has the actual op to do; cap constants and the dispatch shape are spelled out.
- **Type consistency**: io.Identity gets Caps as `[]string`; CapDacRead/CapDacOverride are string consts used both in caps.go and in resolvers.

## Execution Handoff

Per established Phase 2 pattern: subagent-driven execution. Each task is dispatched to a fresh subagent with the task's full text + scene-setting context; controller reviews and commits between tasks.
