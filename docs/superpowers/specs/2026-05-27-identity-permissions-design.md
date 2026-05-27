# Identity, Permissions, and Local Transparency — Revised Design

**Status:** Draft for review
**Date:** 2026-05-27
**Supersedes:** `docs/design/identity-and-permissions.md` (2026-05-14 draft). On
ship, the durable record folds back into that path and this transient spec is
pruned (per the doc-organization working agreement).

## 0. What changed since the 2026-05-14 draft

The original draft predates several things that have since shipped, and one of
its foundational assumptions turned out to be false:

- **Sessions exist.** `SessionService` (`Create`/`Resume`/`Keepalive`,
  `pkg/server/service/session.go`) owns per-client state. `WhoAmI` becomes a
  method on it, not a new service.
- **`Utimens` shipped** as a first-class RPC; `AssumeUserMiddleware` already
  wraps it.
- **Mapping-mode rework.** The draft's near-duplicate `static`/`identity` modes
  are collapsed: `static` (config table) and `system` (real OS accounts via
  NSS) are two backends behind one resolver; the redundant `identity` label is
  dropped.
- **`passthrough` is now genuinely transparent**, with `root_squash` exposed
  both ways (§3.7).
- **The "no per-thread `setgroups`" assumption was wrong** — verified on Linux
  6.8 / Go 1.26.2 (2026-05-27). A raw `SYS_setgroups` on a
  `runtime.LockOSThread`-pinned goroutine sets supplementary groups for *that
  thread only*, and the kernel then enforces group access. This **eliminates
  the user-space permission evaluator** the draft proposed: the kernel becomes
  the sole permission authority via per-request `setfsuid` + `setfsgid` + raw
  `setgroups`. Supplementary groups, **POSIX ACLs**, sticky bits, setgid dirs,
  and chown rules all work natively and correctly, with no TOCTOU window and no
  re-implemented POSIX logic. See §3.4.

## 1. The one principle

The **authenticated principal is identity.** The **server is the sole
authority** for who owns a file and whether an operation is permitted, and it
enforces that **through the kernel** by assuming the principal's full
credentials per request. The **wire UID/GID is advisory** — used for display
hints, and (only in `passthrough`) as the literal identity. The **client
rewrites IDs purely for local display** so a mount renders like a local
filesystem.

## 2. Goals & non-goals

**In scope:**

- Per-volume mapping modes: `squash` (default), `system`, `static`,
  `passthrough`.
- A pluggable `IdentityResolver` interface (principal → `{uid, gid, gids,
  caps}`).
- **Kernel-native permission enforcement** via per-thread `setfsuid` +
  `setfsgid` + raw `setgroups` (supplementary groups, ACLs, sticky, setgid,
  chown rules — all for free).
- An admin capability model (`dac_read`, `dac_override`) decoupled from
  client-side `sudo`.
- `WhoAmI` on `SessionService` + client identity cache.
- Client-side UID/GID rewriting, a `raw_ids` opt-out for backup/admin tooling,
  and symbolic names in `Owner`.
- Safe handling of client-root per mode; real `no_root_squash` passthrough as
  an explicit option.
- **Volume confinement** — path resolution that cannot escape the volume root
  via symlinks or `..` (§3.10). Ships as Phase 2 — after identity, before caps.
- **Resolver robustness and fail-closed semantics** — injection-safe resolution,
  timeouts, and a defined policy for unresolved principals (§3.11).

**Deferred (see §11):** `chown_any` capability, POSIX advisory locks,
readdirplus, multi-user mounts (`allow_other`), server-push identity
invalidation. (POSIX ACLs are **no longer deferred** — kernel enforcement
covers them automatically.)

## 3. Design

### 3.1 Authentication is the identity boundary

The authenticated principal (basic-auth username today; mTLS subject / OIDC
later) is the source of truth. `proto.Caller.Owner` is **advisory** in the
mapped modes and **authoritative** only in `passthrough`.

### 3.2 Per-volume mapping modes

Each volume declares one mode. `squash` is the default when nothing is
configured.

```yaml
volumes:
  - name: photos                 # simple shared store
    path: /srv/photos
    mapping:
      mode: squash               # default; run every op as one fixed identity
      uid: 1000
      gid: 1000

  - name: team                   # real multi-user, server owns accounts
    path: /srv/team
    mapping:
      mode: system               # principal -> real OS account via NSS
      admin_groups:              # caps derived from server group membership
        dac_override: [wheel]
        dac_read:     [backup]

  - name: appliance              # no real OS accounts on the box
    path: /srv/appliance
    mapping:
      mode: static
      users:
        alice: { uid: 1001, gid: 1001, groups: [developers], caps: [] }
        carol: { uid: 1003, gid: 1003, groups: [], caps: [dac_read] }
      groups:
        developers: 2000

  - name: mylan                  # I own both ends, behave like local disk
    path: /srv/mylan
    mapping:
      mode: passthrough
      root_squash: false         # no_root_squash: sudo-write lands root-owned
```

| Mode | Principal → identity | Use case |
|---|---|---|
| `squash` *(default)* | everyone → one fixed `{uid, gid}` | shared appliance volume; safe default |
| `system` | principal name → real OS account via the system name service (`getent`/`id`) | real multi-user; groups sync via server `/etc/group` *and* LDAP/SSSD |
| `static` | principal → config table `{uid, gid, gids, caps}` | locked-down server with no OS accounts for these principals |
| `passthrough` | wire `{uid, gid}` verbatim | I control both ends / trusted LAN |

In every mode the resolved identity's full credentials are assumed in the
kernel per request (§3.4) — there is no separate enforcement path per mode.
`passthrough` simply takes the identity from the wire instead of a resolver.

**`system` mode resolves against the server's OS name service.** Because the
released server binary is built `CGO_ENABLED=0` (see `.goreleaser.yaml`), the
pure-Go `os/user` would see only `/etc/passwd`/`/etc/group` and miss LDAP/SSSD.
To honor the *full* name service (nsswitch, incl. LDAP/SSSD) the resolver
**shells out to `getent passwd`/`getent group`/`id`**, which consult the system
NSS regardless of the binary's cgo setting. Results are cached per session, so
the per-resolve cost is negligible. Requires `getent`/`id` on the server
(standard on Linux). A stripped container image with no accounts will not
resolve principals; **containerized deployments use `static` or `squash`.**
(Coordinate with the in-flight `phase6-dockerfile-compose` work: the
docker-compose example should default to `squash` with an explicit uid/gid,
replacing the `chmod 777` sidecar.)

### 3.3 Identity resolver interface

```go
// Identity is the resolved server-side identity of a principal on a volume.
type Identity struct {
    Principal  string
    Uid, Gid   uint32
    Gids       []uint32          // supplementary, includes Gid
    Caps       CapSet            // dac_read, dac_override
    UserName   string            // display
    GroupNames map[uint32]string // display, for gids the caller is in
}

// IdentityResolver maps an authenticated principal to a server-side Identity
// for a given volume. One implementation per mode.
type IdentityResolver interface {
    Resolve(principal string) (Identity, error)
}
```

`squash`, `system`, `static` each implement this; `passthrough` bypasses it
(identity comes straight off the wire). Future backends (LDAP-direct, OIDC
claims, external resolver) implement the same interface with **no proto or
config-schema change**.

### 3.4 Server-side permission enforcement — kernel-native, per-thread

The server assumes the resolved identity's **full** credentials on the OS
thread handling the request and lets the **kernel** perform the permission
check as part of the actual op. Verified feasible (§0): credentials are
per-thread on Linux, so this does not affect other in-flight requests.

An **identity-bound filesystem wrapper** (mapped modes) performs, per op, on a
`LockOSThread`-pinned thread:

1. Snapshot the thread's current supplementary groups (for restore in step 5),
   then `setgroups(identity.Gids)` via **raw `SYS_setgroups`** (not Go's
   broadcasting `syscall.Setgroups`) — installs the full supplementary set.
2. `setfsgid(identity.Gid)`.
3. `setfsuid(identity.Uid)` — this also **drops the fs-related capabilities**
   (`CAP_DAC_OVERRIDE`/`READ_SEARCH`/`FOWNER`/`FSETID`), so the kernel enforces
   DAC normally for an unprivileged principal.
4. Run the loopback op. The kernel checks owner/group/other bits, the full
   group set, sticky-bit unlink rules, setgid-dir inheritance, and **POSIX
   ACLs** — all natively. Created files are owned by `identity.Uid:Gid`.
   (Honoring POSIX ACLs requires the backing server filesystem to be mounted
   with `acl` — default on most ext4/xfs today, but not guaranteed; documented
   as a server prerequisite.)
5. On cleanup, restore the thread's **original** credentials before unlocking:
   `setfsuid`/`setfsgid` back to root **and** raw `setgroups` back to the group
   list snapshotted in step 1. If any restore call fails, do **not**
   `runtime.UnlockOSThread` — let the tainted thread die with the goroutine.
   (The existing `asume_user.go` cleanup follows this rule for fsuid/fsgid only;
   it **must be extended** to snapshot and restore the supplementary group list,
   or threads return to the pool carrying the principal's groups.)

There is **no user-space permission evaluator** and **no separate `Lstat`
check** — the single kernel-checked op is the enforcement, so there is no
TOCTOU window.

**Wiring (`fuse.Context` cannot carry identity).** go-fuse's `fuse.Context` is a
fixed struct (`{uid, gid, pid, cancel}`) with no extensibility, and the pathfs
middleware chain is baked per-volume at startup and only sees that struct.
Supplementary gids and caps therefore **cannot** ride `fuse.Context`, so
credential-setting cannot live in a `fuse.Context`-reading pathfs middleware
(the original draft's plan). The resolved identity is threaded via
`context.Context` instead, in two stages — and since **every path-resolving RPC
is unary** (Read/Write are streams but ride the already-open fd and need no
creds), a unary interceptor suffices:

1. **Auth interceptor** (extended) stashes the authenticated `Principal` on the
   request `context.Context` via a typed key (today it only logs the username
   and drops it).
2. **`VolumeService.BindIdentity(ctx, volume)`** resolves `principal + volume
   mapping → Identity` (via the per-volume resolver + the server-side identity
   cache, §3.11) and returns a **per-request identity-bound wrapper** over the
   volume's loopback FS — one small struct holding the volume FS pointer + a
   single `*Identity` (allocation-cheap; pinned by an `AllocsPerRun` test). The
   wrapper runs the 5-step cred dance above on each op. Controllers call
   `BindIdentity` in place of `GetVolumeFileSystem`. For `passthrough`,
   `BindIdentity` derives the Identity from `proto.Caller` (with `root_squash`),
   not from the ctx-principal.

The current `AssumeUserMiddleware` (`pkg/server/io/middleware/asume_user.go`,
wired only when `linux && uid==0`) is **subsumed by this wrapper and deleted** —
its `setfsuid`/`setfsgid` mechanics move into the wrapper, extended with
`setgroups`. No dead alternate path is left in the tree.

**`Access` is the one op the kernel won't check for us:** `access(2)` tests the
**real** uid/gid, which stays root on the server, so the loopback's `Access`
would always allow. The `Access` handler must instead evaluate the requested
mode against the resolved identity (or be backed by `faccessat2`), rather than
calling `access()`. Low-stakes (tools that ignore the hint and `open()` get the
correct kernel denial), but it must not silently always-allow.

**Capabilities** (admin bypass, decoupled from client `sudo`):

- `dac_override` — **keep `fsuid=0`** for the op (steps 2–3 set groups/fsgid
  but skip `setfsuid`), so the thread retains `CAP_DAC_OVERRIDE` and the kernel
  permits anything. Verified (§0, worker C). For a *create* under
  `dac_override`, `fchown` the new file to `identity.Uid:Gid` afterward so the
  admin does not silently leave root-owned files. (Create-then-`fchown` is not
  atomic: if the `fchown` fails after the create, a root-owned file remains — an
  admin-path edge that is logged for manual cleanup; acceptable here.)
- `dac_read` — read/traverse-only bypass. Keep `fsuid=0` but **`capset` the
  thread to drop `CAP_DAC_OVERRIDE`/`FOWNER`/`FSETID` while keeping
  `CAP_DAC_READ_SEARCH`**. Capabilities are per-thread, so this is feasible;
  the exact `capset` is a Phase 3 task to verify before relying on it.

Both cap paths require the **server to run as root** (or hold the matching
Linux file-capabilities) — an accepted precondition.

### 3.5 `WhoAmI` on `SessionService`

Identity is **volume-scoped** (the same principal maps differently per volume),
so `WhoAmI` carries a volume:

```proto
message WhoAmIRequest { string volume = 1; }

message Identity {
  string principal                = 1;
  uint32 uid                      = 2;
  uint32 primary_gid              = 3;
  repeated uint32 gids            = 4;
  string user_name                = 5;
  map<uint32, string> group_names = 6;
}

service SessionService {           // existing — add one method
  rpc WhoAmI (WhoAmIRequest) returns (Identity);
}
```

The authenticated principal is read from the gRPC context; the server resolves
against the volume's mapping and returns the **caller's own** identity only.
The full user/group table is never pushed to the client. The client caches the
`Identity` per mount (TTL default 60s, configurable; re-fetched on auth
refresh).

### 3.6 Symbolic names in `Owner` and display fidelity

Extend `Owner` so attribute returns can carry display names:

```proto
message Owner {
  uint32 uid        = 1;
  uint32 gid        = 2;
  string user_name  = 3;  // optional, best-effort
  string group_name = 4;  // optional, best-effort
}
```

**[DECISION — locked] Display fidelity.** **Hybrid** — the server fills `group_name` for groups the caller is a member of
(so shared directories render sensibly) and `user_name` only for the caller's
own identity; **all other users render as `nobody`** on the client. Reveals
that a group exists but never leaks the server's full user list. Alternatives:
privacy-first (`nobody:nogroup` for anyone-but-me) or idmap-like (reveal all
names).

### 3.7 Client-root handling

- **`squash` / `system` / `static`:** wire UID is advisory, so client root is
  closed *by construction* — `sudo` on the client is still just "the
  principal." This also fixes the **current live bug** where the wire UID (root
  included) is fed straight into `setfsuid`.
- **`passthrough`:** `root_squash` is a per-volume knob, **both directions**:
  - `root_squash: true` *(conservative default within passthrough)* — incoming
    `uid==0` → `anon_uid`. Since the server runs as root, "its own UID" is 0 and
    therefore unsafe; when `anon_uid` is unset it **defaults to nobody (65534)**,
    never 0, so root_squash can never silently become a no-op.
  - `root_squash: false` (`no_root_squash`) — wire `{uid, gid}` verbatim, root
    included; a `sudo`-write lands **root-owned** on the server. Full
    transparency for "I own both ends."

  **Security note (conscious choice):** `no_root_squash` means anyone who can
  authenticate to the volume has root-equivalent write on the exposed tree.
  Intended for trusted single-tenant use. The server logs a warning at startup
  when `no_root_squash` is combined with `auth: none`.

### 3.8 Client-side UID/GID rewriting, `raw_ids`, and cache interaction

The client FUSE layer rewrites IDs at the attribute boundary, anchored to the
session `Identity`. Identity is **per-volume**, so the rewrite (and its cached
`Identity`) is **scoped per backend** — the multi-volume mounter overlays
several volumes under one root, each with its own principal/identity, and must
not apply one volume's identity to another's attrs:

**Inbound (server → client):** `attr.uid == identity.uid` → local mounting
user's uid; `attr.gid == identity.primary_gid` → local gid; a gid the caller
is in with a known `group_name` → that name; everything else → `nobody` /
`nogroup`.

**Outbound (client → server):** local mounting user's uid → principal's server
uid; reject/remap `chown` to IDs that cannot be resolved.

**`raw_ids` mount option (for backups/admin tooling).** When set, the client
**disables rewriting** and surfaces the server's real uids/gids unchanged. This
is the supported way to preserve ownership through a backup: an admin principal
with `dac_read` mounts with `raw_ids: true`, and `rsync -a`/`restic` see and
restore real ownership. (A `passthrough` mount is the other way to get raw IDs;
either is fine.) `raw_ids` composes with any mode. The opt-out is symmetric: the
**outbound** path also passes through, so a `chown` is sent with its literal IDs
(not rewritten) and a backup tool can restore ownership faithfully.

**[DECISION — locked] Cache storage.** The persistent client cache stores
**server (wire) IDs**; rewriting happens on every FUSE return, not at
store time. Consequence: an identity change (TTL refresh, re-auth) needs **no
cache invalidation**, and `raw_ids` is just "skip the rewrite step" over the
same cached rows.

### 3.9 Client mount defaults

`nosuid`, `nodev` always; `allow_other=false`, single-user mount (one mount =
one principal's view); `entry_timeout`/`attr_timeout` left at FUSE defaults,
exposed as knobs.

### 3.10 Volume confinement (path-resolution safety)

The server's loopback FS is **path-based** and follows symlinks and `..`
relative to the *server's* root, not the volume root. A client can create
`evil -> /etc` (or send a `..`-laden path) and the server will operate outside
the volume. Today this is bounded by the wire UID; once `dac_override` runs at
`fsuid=0` (Phase 3), it becomes a **remote root-equivalent escape of the entire
server box**, not just the volume.

This is an independent, already-latent bug. It ships as **Phase 2** — after the
identity foundation (Phase 1 is safe without it; see §9) but **before** the
capability phase, and `dac_override` must not ship until it is in place.
Confinement is enforced at the path-resolution / `Open` boundary (the data path
then rides the confined fd, per §3.4):

- Resolve every wire path **beneath the volume root** with
  `openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS)` (Linux ≥5.6), or refactor
  the loopback to fd-relative `*at` ops anchored to a volume-root dirfd.
- Reject absolute symlinks and traversal that would leave the root with
  `EXDEV`/`EACCES`.

**Not a small change** — it touches how the loopback resolves names. Scoped as
its own PR precisely so it can be reviewed and tested in isolation.

### 3.11 Resolver robustness, failure policy, and config validation

- **Injection-safe:** the principal name comes from auth. `system` mode invokes
  `getent`/`id` via **argv (never a shell string)**, and the principal is
  validated against a strict charset before use.
- **Blocking/timeouts:** `getent` under SSSD/NSCD can block. Resolution runs
  with a **timeout + circuit-breaker**. A **permanent** unknown principal →
  **deny (`EACCES`), never fall back** to squash/anon/root (fail closed). A
  **transient** resolver failure (timeout, SSSD blip) does **not** tear down a
  session whose identity is already cached — it serves the cached identity and
  retries refresh later.
- **Server-side identity cache:** resolved identities are cached server-side
  (not just per client), keyed by `{volume, principal}` with a TTL, to avoid a
  `getent` fork-storm across concurrent new sessions; refreshed on TTL and on
  session resume.
- **Auth is mandatory for mapped modes:** `system`/`static` resolve the
  authenticated principal, so the server **refuses to start** with
  `mode: system|static` combined with `auth: none`. `squash` (fixed identity)
  and `passthrough` (wire identity) are the only modes valid without auth.

## 4. Worked examples

Running config: volume `team`, `mode: system`, server accounts `alice(1001)`,
`bob(1002)` both in group `developers(2000)`; `carol(1003)` with `dac_read`;
`dave(1004)` with `dac_override`.

### 4.1 Alice writes, Bob reads (the cross-user scenario)

Alice creates a file in a setgid `developers` dir → on disk `1001:2000`, mode
`0664`. Bob's request runs on a thread with `setgroups([…,2000])` +
`setfsuid(1002)` + `setfsgid(1002)`; the **kernel** sees `2000` in Bob's group
set and grants group `rw` → **read/write allowed**. Bob's `ls -l` shows
`nobody:developers` (group name revealed because Bob is in it; Alice's name
withheld). If Alice had used `0600`, the kernel returns **EACCES** directly.

### 4.2 Bob with `sudo`

`sudo cat` makes the local FUSE caller `uid=0`, but `system` mode ignores the
wire uid — the op runs as principal `bob`'s resolved creds. **No escalation.**

### 4.3 Carol backs up everything

Carol authenticates as principal `carol` (`dac_read`) and mounts with
`raw_ids: true` (or uses a `passthrough` mount). Her requests run with
`fsuid=0` + a `capset` that keeps `CAP_DAC_READ_SEARCH`, so the kernel lets her
read/traverse every file including others' `0600` ones; `raw_ids` means
`rsync -a`/`restic` see the **real** owners and preserve them faithfully. Power
came from the **principal**, not from running the backup tool as local root.

### 4.4 My-LAN passthrough, `no_root_squash`

Volume `mylan`, `passthrough`, `root_squash: false`. `sudo touch` on the client
→ file owned `0:0` (root) on the server, exactly as written. Behaves like local
disk / NFS `no_root_squash`.

## 5. Proto changes

- `SessionService`: add `WhoAmI(WhoAmIRequest) → Identity`.
- `Owner`: add optional `user_name`, `group_name`.
- `Caller.Owner`: preserved, documented advisory (authoritative only in
  `passthrough`).

## 6. Server changes

- `pkg/common/config`: per-volume `mapping` block (`mode`, `uid`, `gid`,
  `users`, `groups`, `admin_groups`, `root_squash`, `anon_uid`); validation.
- `pkg/server/service`: `IdentityResolver` interface + `squash`/`system`/
  `static` implementations; `system` shells out to `getent`/`id` (binary is
  `CGO_ENABLED=0`, so pure-Go `os/user` would miss LDAP/SSSD).
- **Auth interceptor** (`pkg/server/grpc/auth.go`): stash the authenticated
  principal on `context.Context` via a typed key (today it only logs it).
- **`VolumeService.BindIdentity(ctx, volume) → pathfs.FileSystem`**: resolves
  `principal + volume mapping → Identity` (resolver + server-side cache) and
  returns a per-request **identity-bound wrapper** over the volume's loopback FS.
  For `passthrough`, derives the Identity from `proto.Caller` + `root_squash`.
- **Identity-bound wrapper** (`pkg/server/io`, replaces `AssumeUserMiddleware`):
  one struct holding `{fs, *Identity}` implementing `pathfs.FileSystem`; per op,
  `LockOSThread` + raw `setgroups(Gids)` + `setfsgid` + `setfsuid` (or keep
  `fsuid=0` for caps) + run op + restore-and-unlock. **Delete
  `pkg/server/io/middleware/asume_user.go`** (subsumed).
- **Controllers** (`fs.go`, `file.go`): call `BindIdentity(ctx, volume)` instead
  of `GetVolumeFileSystem(volume)` for path ops. (Read/Write/fd ops keep using
  the stored fd — no identity binding on the data path.)
- `SessionService.WhoAmI` controller + service plumbing.
- `pkg/server/io` (Phase 2): confine path resolution to the volume root
  (`openat2(RESOLVE_BENEATH|RESOLVE_NO_MAGICLINKS)` or fd-relative loopback).
- Config validation: reject `mode: system|static` + `auth: none`; reject
  `passthrough` `no_root_squash` only logs a warning (not a hard error).
- `Access` handler: evaluate against the resolved identity, not `access(2)`.
- Server-side identity cache keyed by `{volume, principal}` (TTL) with a resolve
  timeout + circuit-breaker; `getent`/`id` invoked via argv (never a shell) with
  a validated principal.

## 7. Client changes

- `pkg/client/grpc`: call `WhoAmI` at mount; cache `Identity` (TTL); re-fetch on
  session resume; expose to the FUSE layer.
- `pkg/client/io`: rewrite IDs on attribute return; rewrite outgoing `Chown`;
  honor the `raw_ids` mount option (skip rewrite, **both** directions).
- `pkg/client/io/cache`: stores server IDs (per §3.8); no invalidation on
  identity change.
- Identity (and its cache) is **per-volume/per-backend** — the multi-volume
  mounter must not cross-apply one volume's identity to another's attrs.
- Mount defaults: `nosuid`, `nodev`, `allow_other=false`; new `raw_ids` flag.

## 8. Decisions (all locked, 2026-05-27)

1. **Display fidelity (§3.6):** hybrid — reveal `group_name` for groups the
   caller is a member of, render other users as `nobody`.
2. **Cache storage (§3.8):** store server IDs, rewrite on every FUSE return (no
   invalidation on identity change).

Plus, from discussion: squash default; real passthrough with `root_squash` both
ways; `WhoAmI` on `SessionService`; admin-via-capability not `sudo`;
kernel-native enforcement (per-thread `setgroups`); backups via `raw_ids` or
`passthrough`; capability set = `dac_read` + `dac_override` (`chown_any`
deferred); server runs as root; volume confinement as Phase 2 (after identity,
before caps) per the sequencing chosen 2026-05-27.

## 9. Phasing (one spec; Phase 1 splits into 1a/1b, then 2 and 3 — one PR each)

Per the worktree-per-feature working agreement, each phase is its own worktree
+ PR. The kernel-enforcement pivot removed the user-space evaluator, so the old
"primary-group-only interim limitation" is **gone** — Phase 1 ships correct
supplementary-group permissions from the start.

1. **Phase 1 — identity foundation + correct permissions.** Split into two PRs:

   **Phase 1a — server-side identity & kernel enforcement (PR 1, ships first).**
   - Config `mapping` schema + validation (incl. fail-closed + auth-required,
     §3.11).
   - `IdentityResolver` (`squash`, `system`, `static`) + `passthrough` Identity
     derivation with `root_squash`.
   - Auth interceptor stashes the principal on `context.Context`.
   - `VolumeService.BindIdentity` + the identity-bound FS wrapper (raw
     `setgroups` + `setfsuid`/`setfsgid` + restore); **delete
     `AssumeUserMiddleware`**; controllers call `BindIdentity`.
   - `Access` evaluated against the resolved identity.
   - Server-side identity cache (resolve timeout, `getent`/`id` via argv).
   - **Safe without confinement:** mapped modes ignore the wire uid and the
     kernel checks as the (unprivileged) principal, so a symlink escape is just
     a normal `EACCES`; it only turns dangerous under `dac_override` (Phase 3).

   **Phase 1b — `WhoAmI` + client local-feel (PR 2).**
   - `WhoAmI` on `SessionService` + client identity cache.
   - Client-side UID/GID rewriting (hybrid display), `raw_ids` option (both
     directions), symbolic names in `Owner`, per-volume identity scoping.
2. **Phase 2 — volume confinement (PR 3).**
   - `ConfinedLoopbackFileSystem` resolving every op beneath the volume root via
     `openat2(RESOLVE_BENEATH)` (§3.10). Independent of identity; fixes a latent
     escape. **Must merge before Phase 3.**
3. **Phase 3 — admin capabilities (PR 4).**
   - `dac_override` (keep `fsuid=0`; `fchown` cap-granted creates to the
     principal) and `dac_read` (selective per-thread `capset` — verify first).
   - `admin_groups` derivation in `system` mode.
   - **Gated on Phase 2** — `dac_override` at `fsuid=0` over an unconfined
     loopback would be a remote root-equivalent escape.

On Phase 3 ship, fold the durable record into
`docs/design/identity-and-permissions.md` and prune this transient spec.

## 10. Testing

- **Per-thread credential test (regression):** the §0 experiment promoted to a
  real test — raw `setgroups` on a locked thread grants supplementary-group
  access; a sibling thread with different groups is unaffected (no leak);
  `setfsuid(nonzero)` drops fs-caps; `fsuid=0` retains `CAP_DAC_OVERRIDE`.
- **Resolver unit tests** (testify suites): each mode maps a principal to the
  expected identity; `system` against a fixture NSS lookup; `passthrough`
  root_squash both ways.
- **Confinement (Phase 2):** symlink-escape (`evil -> /etc/passwd`) and `..`
  traversal attempts are rejected; a symlink *within* the volume still resolves.
- **Resolver failure policy:** unknown principal → `EACCES` (fail closed);
  `mode: system|static` + `auth: none` → server refuses to start; a simulated
  resolver timeout serves the cached identity rather than tearing down a live
  session.
- **`Access` correctness:** `Access` denies where the resolved identity lacks
  permission (does not always-allow as root).
- **e2e (kubevirt VM, real FUSE):** the four worked examples in §4 end-to-end,
  including supplementary-group read/write, a POSIX-ACL'd file honored
  correctly, sticky-bit unlink, setgid-dir inheritance, `sudo`-no-escalation,
  `dac_read` + `raw_ids` backup preserving ownership, and `no_root_squash`
  root-owned write.
- **`-race`** on the middleware chain (concurrent ops across pinned threads).

**Implementation note (guardrail):** no code path may call Go's
`syscall.Setuid`/`Setgid`/`Setgroups` (they broadcast across all threads via
`AllThreadsSyscall` and would clobber the per-thread creds set on locked
threads). Per-thread credential changes use raw syscalls only.

## 11. Deferred / open questions

- **`chown_any` capability** — deferred; `dac_override` covers the admin case.
- **POSIX advisory locks** (`SetLk`/`GetLk`) — a later phase, needs a reconnect
  recovery story.
- **Readdirplus** — independent perf win, ship separately.
- **Multi-user mounts (`allow_other`)** — single-user is the only supported
  mode for the first release.
- **Server-push identity invalidation** — TTL + auth-refresh is enough until a
  real use case appears.
- **`Subscribe` per-path authorization** *(found during Phase 1a)* — the event
  channel (`controller/subscribe.go` → `bus.Subscribe`) forwards every volume
  event (path, new_path, version) to any authenticated subscriber regardless of
  per-path permission. The subscribe gate only checks volume existence. A
  metadata/path-leak via the event stream; pre-existing, not introduced by the
  identity work. Gate subscriptions (and/or filter events) by the principal's
  path access in a follow-up.
- **`Caller` on `GetAttrIfChangedRequest`** *(found during Phase 1a)* — that RPC
  is identity-bound with a nil caller, so in `passthrough` mode it resolves to
  anon (root-squash) rather than the wire caller. Correct in mapped modes (ctx
  principal). Add a `Caller` field to the request to make passthrough
  cache-revalidation use the real wire identity.
- **Dedicated mapped-mode identity e2e** — Phase 1a is proven at the unit level
  (resolvers, BindIdentity, Access), at the kernel level on the VM
  (`BoundFSCredsSuite`: per-thread setgroups enforcement + isolation + restore),
  and end-to-end via the api/fs e2e suites running through `BindIdentity`
  (passthrough). A dedicated e2e that authenticates as a real principal over
  basic-auth and asserts a `static`/`system` volume enforces that principal's
  uid/supplementary-groups end-to-end (the alice-writes/bob-reads scenario)
  would close the last wire-level seam; add as a fast-follow.

*(POSIX ACLs are intentionally NOT here — kernel enforcement handles them.)*

## 12. North-star acceptance test

On a freshly-mounted `system`-mode volume: `git clone` of a real-sized repo
completes; `git status` returns within 2–3× of local over a typical link;
`make` against a sample C project builds (atomic rename + `fsync`). These cover
ownership rendering, atomic rename, open-then-unlink, fsync, readdir cost, and
basic locking — most of the "feels local" long tail.
