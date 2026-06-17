# HANDLE_KILLPRIV_V2 Write-Throughput Fix — Design

**Status:** Approved (brainstorm 2026-06-17)
**Goal:** Eliminate the per-write `security.capability` getxattr round-trip by advertising the FUSE `CAP_HANDLE_KILLPRIV_V2` capability, gated by a new client config knob defaulting on. Restores ~+75% single-file write throughput on high-RTT (WAN) links with no security regression.

---

## Problem

On every `write(2)` syscall the kernel runs `file_remove_privs`, which checks
`security.capability` (to strip setuid/setgid/file-capabilities on modify). For a
FUSE mount this is forwarded to the userspace filesystem as a **GetXattr request
per write** — gMountie turns that into a `GetXattr` RPC, i.e. **one client↔server
round-trip per write syscall**. At WAN RTT this serializes the write path and
caps single-file throughput.

### Evidence (gmountie-perf VM, 1Gbit, 25ms each-way netem, `dd bs=1M conv=fdatasync`, 2 passes)

| build | throughput | server `openat2`/write | suid/sgid strip |
|---|---|---|---|
| stock (cap off) | 8.1–8.4 MB/s | 129 (= 1 per write) | ✓ (4755/2755/6755 → 755) |
| `CAP_HANDLE_KILLPRIV_V2` | **14.3–14.7 MB/s (+~75%)** | **0** | ✓ (4755/2755/6755 → 755) |

The per-write `openat2` traces to `ConfinedLoopbackFileSystem.openLeafForXattr`
(the server-side leaf open for the xattr lookup). Advertising the cap makes the
kernel set `S_NOSEC` and stop issuing the per-write getxattr.

### Why `attr_timeout` cannot fix it

`AttrTimeout` caches **stat** attributes (size/mode/mtime), not xattr values, and
the kernel clears the `S_NOSEC` flag unless killpriv is negotiated. Measured: a/b
`attr_timeout` 1s vs 300s gave identical throughput (8.x MB/s) and identical 129
opens/write. The knob cannot reach this code path.

---

## Approach

Advertise `fuse.CAP_HANDLE_KILLPRIV_V2` in the mount's `ExtraCapabilities`,
gated by a new `FUSEConfig.HandleKillPriv bool` defaulting `true`. This mirrors
the existing `WritebackCache` → `CAP_WRITEBACK_CACHE` pattern exactly.

**Rejected alternatives:**
- `MountOptions.DisableXAttrs` — also eliminates the getxattr and hits the same
  ~14.5 MB/s, but is too blunt: it returns ENOSYS for *all* xattr ops, breaking
  ACL / `security.*` / `system.posix_acl_*` passthrough that the
  identity/permissions feature relies on.
- Client-side negative-cache of `security.capability` — adds an invalidation
  burden and gMountie-specific complexity for no benefit over the kernel-native
  cap.

**Security note (verified, not assumed):** advertising `HANDLE_KILLPRIV_V2`
delegates suid/sgid/cap stripping responsibility to the filesystem. gMountie's
server writes as root with `setfsuid`/`setgroups` (retaining `CAP_FSETID`), which
raised the concern that a backing write could *retain* a setuid bit the kernel
strips today. **Tested against the backing file** (gmountie-perf VM): with the
cap on, suid (4755), sgid (2755), and suid+sgid (6755) are all still stripped to
755 on the backing file — the kernel performs the strip via a `setattr`
(`openat2` count 0 on the privileged write). No retention, no regression. The
e2e suite below makes this a permanent merge gate across all mapping modes.

---

## Components

### 1. Config knob — `pkg/client/config/fuse.go`

- Add `DefaultFUSEHandleKillPriv = true` to the defaults block.
- Add field `HandleKillPriv bool` with `mapstructure:"handle_kill_priv"` to
  `FUSEConfig`.
- Mirror `WritebackCache` exactly (it is the byte-for-byte precedent):
  - constructor literal in `NewDefaultFUSEConfig`: `HandleKillPriv: DefaultFUSEHandleKillPriv`
  - `v.SetDefault("handle_kill_priv", DefaultFUSEHandleKillPriv)` in `NewFUSEConfig`
    (alongside the other `v.SetDefault` calls), so an explicit `false` in a file is
    still honored by `v.Unmarshal`.

### 2. Config plumbing — `pkg/client/config/config.go`

- Add `"handle_kill_priv"` to the `fuseSub` env-mirror key list (so
  `GMOUNTIE_FUSE_HANDLE_KILL_PRIV` works like the other FUSE knobs).

### 3. Cap wiring — `pkg/client/mount/common.go`

In `createMountOptions`, after the existing `WritebackCache` block:

```go
if cfg.HandleKillPriv {
    opts.ExtraCapabilities |= fuse.CAP_HANDLE_KILLPRIV_V2
}
```

No `DisabledCapabilities` branch is required: the bit is not in go-fuse's default
capability allowlist, so its absence already means "off". (Confirmed in
go-fuse v2.10.1 `opcode.go` `doInit`: kernel flags are masked to a fixed
allowlist plus `ExtraCapabilities`.)

### 4. Unit tests — `pkg/client/mount/common_test.go`, `pkg/client/config/fuse_test.go`

- `common_test.go`: `createMountOptions` sets the `CAP_HANDLE_KILLPRIV_V2` bit in
  `ExtraCapabilities` when `HandleKillPriv` is true, and leaves it unset when
  false.
- `fuse_test.go`: `NewDefaultFUSEConfig().HandleKillPriv == true`; a config that
  explicitly sets `handle_kill_priv: false` parses to false; unset → true.

### 5. e2e security gate — `test/e2e/fs/` (new test, runs in CI which has /dev/fuse)

For **each** mapping mode `{squash, static, system, passthrough}`: mount a volume
configured in that mode, create a file, `chmod` it suid (4755), sgid (2755), and
suid+sgid (6755), perform a write, and assert the privilege bits are **stripped**
on the **backing file** (server side), not merely as seen through the mount.

**The sharp edge (must be honored):** the kernel only strips privilege bits when
the writing process **lacks `CAP_FSETID`** — i.e. a *non-root* writer. CI test
processes typically run as **root**, which retains `CAP_FSETID`, so a naive write
would never trigger stripping and the test would falsely pass. The test MUST
perform the privileged-bit write as an **unprivileged uid** (e.g. drop fsuid /
run the write in a child with a non-root identity, or skip with a clear message
when the test cannot drop privileges). Without this, the test asserts nothing.

Follow the env-gating convention: this suite really mounts in CI (CI has FUSE),
so it should NOT be VM-gated; but the privilege-drop requirement may need a
capability check that skips cleanly (with an explicit logged reason) where it
cannot be satisfied.

### 6. Docs

- `docs/design/security-and-transport.md`: note the `HANDLE_KILLPRIV_V2`
  negotiation and its interaction with the identity-bound filesystem (kernel
  performs the suid/sgid/cap strip via setattr; server need not).
- Config reference (the FUSE knobs section): document `fuse.handle_kill_priv`
  (default true) and `GMOUNTIE_FUSE_HANDLE_KILL_PRIV`.

---

## Out of scope

- The single-connection TCP throughput ceiling (~40–48 MB/s) and a client
  read+write connection pool — a separate, complementary effort. This fix removes
  a per-write tax; it does not change the connection model.
- The async-persist stale-read cache race (separate bug, separate PR — tracked
  in memory `project_cache_async_persist_stale_race`). Note: master is currently
  red on that test, so this PR's CI will show that pre-existing failure until the
  cache race is fixed.

---

## Testing summary

- Unit: cap-bit wiring + config default/parse.
- e2e: suid/sgid/suid+sgid stripping on the backing file across all 4 mapping
  modes, with a non-root writer.
- Manual (already done, recorded above): VM throughput a/b proving +~75% and the
  security gate.
