# Identity, Permissions, and Local Transparency — Revised Design

**Status:** Draft for review
**Date:** 2026-05-27
**Supersedes:** `docs/design/identity-and-permissions.md` (2026-05-14 draft). On
ship, the durable record folds back into that path and this transient spec is
pruned (per the doc-organization working agreement).

## 0. What changed since the 2026-05-14 draft

The original draft predates several things that have since shipped and which
this revision reconciles:

- **Sessions exist.** `SessionService` (`Create`/`Resume`/`Keepalive`,
  `pkg/server/service/session.go`) now owns per-client state (fd table,
  idempotency cache). `WhoAmI` becomes a method on `SessionService`, not a new
  service.
- **`Utimens` shipped** as a first-class RPC; `AssumeUserMiddleware` already
  wraps it. The evaluator/middleware list below includes it.
- **Mapping-mode rework.** The draft's `static` and `identity` modes were
  near-duplicates. This revision collapses them: `static` (config table) and
  `system` (real OS accounts via NSS) are two backends behind one resolver
  interface; the redundant `identity` label is dropped.
- **`passthrough` is now genuinely transparent**, with `root_squash` exposed
  both ways (see §3.7) — the draft only offered squash-on.

## 1. The one principle

The **authenticated principal is identity.** The **server is the sole
authority** for who owns a file and whether an operation is permitted. The
**wire UID/GID is advisory** — used for display hints and (only in
`passthrough`) as the literal identity, never trusted for permission decisions
in the mapped modes. The **client rewrites IDs purely for local display** so a
mount renders like a local filesystem.

Everything below is the machinery to honor that principle while making a mount
feel local (goal 4 from the original draft: applications should not be able to
tell a mount is remote).

## 2. Goals & non-goals

**In scope:**

- Per-volume mapping modes: `squash` (default), `system`, `static`,
  `passthrough`.
- A pluggable `IdentityResolver` interface (principal → `{uid, gid, gids,
  caps}`).
- Server-side, user-space POSIX permission evaluation with supplementary
  groups, sticky-bit dirs, setgid dirs (Phase 2).
- An admin capability model (`dac_read`, `dac_override`) decoupled from
  client-side `sudo` (Phase 2).
- `WhoAmI` on `SessionService` + client identity cache.
- Client-side UID/GID rewriting and symbolic names in `Owner`.
- Safe handling of client-root per mode; real `no_root_squash` passthrough as
  an explicit option.

**Deferred (see §11):** POSIX ACL passthrough/awareness, `chown_any`
capability, POSIX advisory locks, readdirplus, multi-user mounts
(`allow_other`), server-push identity invalidation.

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

| Mode | Principal → identity | Permission enforcement | Use case |
|---|---|---|---|
| `squash` *(default)* | everyone → one fixed `{uid, gid}` | kernel via `setfsuid` | shared appliance volume; safe default |
| `system` | principal name → real OS account (`getpwnam` + `getgrouplist`) | user-space evaluator (Phase 2) | real multi-user; groups sync via server `/etc/group` / LDAP-SSSD |
| `static` | principal → config table `{uid, gid, gids, caps}` | user-space evaluator (Phase 2) | locked-down server with no OS accounts for these principals |
| `passthrough` | wire `{uid, gid}` verbatim | kernel via `setfsuid`; **no evaluator, no caps** | I control both ends / trusted LAN |

**`system` mode requires the server to resolve principals via NSS** — a real
OS account, or LDAP/SSSD wired into NSS. A stripped container image will not
have these entries; **containerized deployments use `static` or `squash`.**
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
    Caps       CapSet            // dac_read, dac_override (Phase 2)
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

### 3.4 Server-side permission evaluation (Phase 2)

Linux has no per-thread `setgroups`, so `setfsuid`/`setfsgid` can enforce only
the **primary** gid. The mapped modes therefore evaluate permission in user
space before touching disk.

Middleware order (mapped modes): `IdentityResolver → PermissionEval →
AssumeUser → loopback`.

```go
// Decision is the evaluator's verdict for one op against one target.
type Decision struct {
    Allowed  bool
    Elevated bool // the access is granted ONLY by a capability; the actual
                  // IO must run privileged (as root), not as the mapped uid,
                  // or the kernel would re-deny it.
}
```

1. `PermissionEval` `Lstat`s the target, reads `{uid, gid, mode}`, and checks
   it against the resolved `Identity` (owner/group/other bits, full `Gids`,
   sticky-bit unlink rules, setgid-dir inheritance on create).
2. Capability short-circuit: `dac_override` ⇒ `{Allowed:true, Elevated:true}`
   for any op; `dac_read` ⇒ same for read/traverse ops. Elevated is set
   **only** when the mapped uid alone would have been denied.
3. `AssumeUser` performs the IO:
   - **Default:** `setfsuid(identity.Uid)` + `setfsgid(identity.Gid)` so the
     kernel enforces and **created files are owned by the principal's uid.**
   - **`Elevated`:** run the IO as the server's real (root) identity so the
     kernel permits it. **Ownership rule for cap-granted creates:** the file
     is still attributed to the principal's resolved uid (a `setfsuid` to the
     principal's uid is applied for the create/ownership-setting step even
     when read access came via a cap), so an admin writing into the tree does
     not silently create root-owned files. Pure reads/traversals under a cap
     leave no ownership trace, so they simply run as root.

`dac_override`/`dac_read` thus require the **server to run privileged** (root
or the matching Linux file-capability) to actually wield the bypass — the same
precondition `AssumeUserMiddleware` already has.

**Phase 1 interim:** ships before the evaluator, with **primary-group-only**
enforcement (`setfsuid` + `setfsgid(primary)`), documented as a known
limitation. Supplementary-group correctness and caps arrive with Phase 2.

### 3.5 `WhoAmI` on `SessionService`

Identity is **volume-scoped** (the same principal maps differently per
volume), so `WhoAmI` carries a volume:

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

**[DECISION — flagged for review] Display fidelity.** Recommended default:
**hybrid** — the server fills `group_name` for groups the caller is a member
of (so shared directories render sensibly), and fills `user_name` only for the
caller's own identity; **all other users render as `nobody`** on the client.
This reveals that a group exists but never leaks the server's full user list.
Alternatives: privacy-first (`nobody:nogroup` for anyone-but-me) or
idmap-like (reveal all names).

### 3.7 Client-root handling

- **`squash` / `system` / `static`:** wire UID is advisory, so client root is
  closed *by construction* — `sudo` on the client is still just "the
  principal." This also fixes the **current live bug** where the wire UID
  (root included) is fed straight into `setfsuid`.
- **`passthrough`:** `root_squash` is a per-volume knob, **both directions**:
  - `root_squash: true` *(conservative default within passthrough)* — incoming
    `uid==0` → `anon_uid` (default: the server's own UID, never 0).
  - `root_squash: false` (`no_root_squash`) — wire `{uid, gid}` used verbatim,
    root included; a `sudo`-write lands **root-owned** on the server. This is
    the full-transparency mode for "I own both ends."

  **Security note (conscious choice):** `no_root_squash` means anyone who can
  authenticate to the volume has root-equivalent write on the exposed tree.
  Intended for trusted single-tenant use. The server logs a warning at startup
  when `no_root_squash` is combined with `auth: none`.

### 3.8 Client-side UID/GID rewriting + cache interaction

The client FUSE layer rewrites IDs at the attribute boundary, anchored to the
session `Identity`:

**Inbound (server → client):** `attr.uid == identity.uid` → local mounting
user's uid; `attr.gid == identity.primary_gid` → local gid; a gid the caller
is in with a known `group_name` → that name; everything else → `nobody` /
`nogroup`.

**Outbound (client → server):** local mounting user's uid → principal's server
uid; reject/remap `chown` to IDs that cannot be resolved.

**[DECISION — flagged for review] Cache storage.** The persistent client
cache stores **server (wire) IDs**; rewriting happens on every FUSE return,
not at store time. Consequence: an identity change (TTL refresh, re-auth)
needs **no cache invalidation** — the rewrite layer is identity-agnostic and
re-derives display IDs from the current session identity each time.

### 3.9 Client mount defaults

`nosuid`, `nodev` always; `allow_other=false`, single-user mount (one mount =
one principal's view); `entry_timeout`/`attr_timeout` left at FUSE defaults,
exposed as knobs.

## 4. Worked examples

Running config: volume `team`, `mode: system`, with server accounts
`alice(1001)`, `bob(1002)` both in group `developers(2000)`; `carol(1003)`
with `dac_read`; `dave(1004)` with `dac_override`.

### 4.1 Alice writes, Bob reads (the cross-user scenario)

Alice creates a file in a setgid `developers` dir → on disk `1001:2000`, mode
`0664`. Bob (principal `bob`, resolved `1002`, member of `2000`) reads:
evaluator checks group bits against Bob's `Gids` → group `rw` → **allowed**.
Bob's `ls -l` shows `nobody:developers` (group name revealed because Bob is in
it; Alice's name withheld). If Alice had used `0600`, the server returns
**EACCES**.

### 4.2 Bob with `sudo`

`sudo cat` makes the local FUSE caller `uid=0`, but `system` mode ignores the
wire uid — the op is still evaluated as principal `bob`. **No escalation.**
If Bob lacks server-side permission, `sudo` gets EACCES too.

### 4.3 Carol backs up everything

Carol authenticates as principal `carol` (`dac_read`). The evaluator
short-circuits read/traverse checks → `{Allowed:true, Elevated:true}`;
`AssumeUser` reads as root so the kernel permits it. `restic`/`rsync -a` reads
every file including others' `0600` ones. Power came from the **principal**,
not from running the backup tool as local root.

### 4.4 My-LAN passthrough, `no_root_squash`

Volume `mylan`, `passthrough`, `root_squash: false`. `sudo touch` on the
client → file owned `0:0` (root) on the server, exactly as written. Behaves
like local disk / NFS `no_root_squash`.

## 5. Proto changes

- `SessionService`: add `WhoAmI(WhoAmIRequest) → Identity`.
- `Owner`: add optional `user_name`, `group_name`.
- `Caller.Owner`: preserved, documented advisory (authoritative only in
  `passthrough`).
- No proto change for the user-space evaluator (server-internal).

## 6. Server changes

- `pkg/common/config`: per-volume `mapping` block (`mode`, `uid`, `gid`,
  `users`, `groups`, `admin_groups`, `root_squash`, `anon_uid`); validation.
- `pkg/server/service`: `IdentityResolver` interface + `squash`/`system`/
  `static` implementations; `system` uses cgo-free NSS via `os/user`
  (`Lookup`, `LookupGroupIds`).
- New `IdentityResolverMiddleware`: resolves principal → `Identity`, stashes on
  context (mapped modes only).
- New `PermissionEvalMiddleware` (Phase 2): user-space POSIX check + caps.
- `AssumeUserMiddleware`: hardened — in mapped modes it consumes the resolved
  identity, never the wire uid; honors the `Elevated` decision.
- `AuthService`: propagate authenticated principal onto the gRPC context.
- `controller.createContext`: in mapped modes, build context from resolved
  identity; in `passthrough`, from the wire caller (with `root_squash`
  applied).
- `SessionService.WhoAmI` controller + service plumbing.

## 7. Client changes

- `pkg/client/grpc`: call `WhoAmI` at mount; cache `Identity` (TTL); expose to
  FUSE layer.
- `pkg/client/io`: rewrite IDs on attribute return; rewrite outgoing `Chown`.
- `pkg/client/io/cache`: stores server IDs (per §3.8); no invalidation on
  identity change.
- Mount defaults: `nosuid`, `nodev`, `allow_other=false`.

## 8. Decisions flagged for the review gate

These are **recommended defaults**, not yet locked — please confirm or adjust:

1. **Resolver emphasis:** `system` (NSS) is the recommended primary for
   multi-user; `static` is the container/appliance fallback; `squash` is the
   default; `passthrough` is opt-in. (You've locked: squash default,
   passthrough real with `root_squash` both ways.)
2. **Display fidelity (§3.6):** hybrid (reveal group names you're in, hide
   other users).
3. **Capability set (§3.4):** `dac_read` + `dac_override` only. `chown_any` is
   deferred (covered by `dac_override` for the admin case).
4. **Cache storage (§3.8):** store server IDs, rewrite on return.

## 9. Phasing (one spec, two implementation plans, one PR each)

Per the worktree-per-feature working agreement, each phase is its own worktree
+ PR.

1. **Phase 1 — identity foundation (PR 1).**
   - Config `mapping` schema + validation.
   - `IdentityResolver` (`squash`, `system`, `static`) + `passthrough` wiring
     with `root_squash` knob.
   - `IdentityResolverMiddleware`; hardened `AssumeUserMiddleware`.
   - `WhoAmI` + client identity cache.
   - Client-side UID/GID rewriting; symbolic names in `Owner`.
   - Primary-group-only enforcement (documented interim limitation).
   - **The 2026-05-14 draft's "interim safety patch" is folded in here**, not
     shipped separately: in mapped modes the wire uid is ignored entirely
     (subsuming the patch), and `passthrough` handles `uid==0` via
     `root_squash`. (If Phase 1 slips by weeks, revisit shipping the ~5-line
     `uid==0` guard standalone, gated to non-passthrough.)
2. **Phase 2 — correct permissions (PR 2).**
   - `PermissionEvalMiddleware`: supplementary groups, sticky-bit, setgid dirs.
   - Capability set (`dac_read`, `dac_override`) + the `Elevated` privileged-IO
     path; `admin_groups` derivation in `system` mode.

On Phase 2 ship, fold the durable record into
`docs/design/identity-and-permissions.md` and prune this transient spec.

## 10. Testing

- **Resolver unit tests** (testify suites): each mode maps a principal to the
  expected identity; `system` mode against a fixture NSS lookup; `passthrough`
  root_squash both ways.
- **Evaluator unit tests:** owner/group/other matrices, supplementary-group
  hits, sticky-bit unlink, setgid inheritance, cap short-circuit + `Elevated`.
- **e2e (kubevirt VM, real FUSE):** the four worked examples in §4 as
  end-to-end assertions; `sudo`-no-escalation; `no_root_squash` root-owned
  write.
- **`-race`** on the middleware chain (concurrent ops, `setfsuid` thread
  pinning).

## 11. Deferred / open questions

- **POSIX ACL passthrough + evaluator awareness.** Mostly free via xattr, but
  the evaluator must understand `system.posix_acl_*` for an accurate check.
  Deferred to a follow-up; the evaluator ships mode-bits + sticky + setgid +
  caps first.
- **`chown_any` capability.** Deferred; `dac_override` covers the admin case.
- **POSIX advisory locks** (`SetLk`/`GetLk`) — Phase 3, needs reconnect
  recovery story.
- **Readdirplus** — independent perf win, ship separately.
- **Multi-user mounts (`allow_other`)** — single-user is the only supported
  mode for the first release.
- **Server-push identity invalidation** — TTL + auth-refresh is enough until a
  real use case appears.

## 12. North-star acceptance test

On a freshly-mounted `system`-mode volume: `git clone` of a real-sized repo
completes; `git status` returns within 2–3× of local over a typical link;
`make` against a sample C project builds (atomic rename + `fsync`). These cover
ownership rendering, atomic rename, open-then-unlink, fsync, readdir cost, and
basic locking — most of the "feels local" long tail.
