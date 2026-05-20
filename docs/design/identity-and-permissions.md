# Identity, Permissions, and Local Transparency

**Status:** Draft
**Last updated:** 2026-05-14

## 1. Context

gMountie exposes server-side directories as named volumes. Today:

- The client forwards its local FUSE caller's UID/GID over the wire as
  `proto.Caller.Owner`.
- `pkg/server/controller.createContext` builds a `fuse.Context` directly from
  those values.
- The optional `AssumeUserMiddleware` (`pkg/server/io/middleware/asume_user.go`),
  enabled only when the server runs as root on Linux, calls `setfsuid` /
  `setfsgid` per request so that the kernel's permission check runs as the
  caller's UID/GID.

This model has three problems we need to solve before gMountie can be the
"NFS over the internet without a VPN" the project is aimed at:

1. **UID namespaces never match across the internet.** Client uid `1001` and
   server uid `1001` are coincidentally the same number, not the same person.
2. **Supplementary groups don't fit Linux's per-thread setfsuid model.** Group
   bits matter for almost any shared volume, but Linux has no per-thread
   `setgroups`.
3. **Client root currently equals server root.** A client running as `uid=0`
   sets `setfsuid(0)` on the server thread. Equivalent to NFS `no_root_squash`,
   on by default.

A separate, equally important goal:

4. **Applications should not be able to tell that a gMountie mount is remote.**
   Ownership, permissions, errno values, and common POSIX idioms must match a
   local filesystem.

This document proposes a coherent design that addresses (1) (2) (3) and aligns
with (4).

## 2. Goals & non-goals

**In scope (this document):**

- Identity model on the wire and at the server.
- Per-volume mapping modes (passthrough, squash, static, identity).
- Server-side permission evaluation, including supplementary groups.
- Identity advertisement to the client (`WhoAmI`) and symbolic names in
  `Attr`.
- Client-side UID/GID rewriting for local-feel transparency.
- Safe default handling of client-root requests.

**Deferred (tracked in §8):**

- POSIX advisory locking (`flock`, `fcntl`).
- Session re-establishment / open-handle recovery after disconnect.
- Readdirplus (attributes alongside directory entries).
- Full POSIX ACL passthrough.
- Multi-user mounts (`allow_other`).

## 3. Design

### 3.1 Authentication is the identity boundary

The authenticated principal is the source of truth for identity. Today the
principal comes from `pkg/server/service.AuthService` (basic auth username);
later it may come from mTLS subject, OIDC, etc. The wire field
`proto.Caller.Owner` becomes **advisory only**: useful to the client for local
display hints, never trusted by the server for permission decisions.

### 3.2 Per-volume mapping modes

Every volume declares how it maps the authenticated principal to a server-side
identity (`uid`, primary `gid`, supplementary `gids`).

```yaml
volumes:
  - name: photos
    path: /srv/photos
    mapping:
      mode: squash          # squash | static | identity | passthrough
      uid: 1000
      gid: 1000

  - name: team-shared
    path: /srv/team
    mapping:
      mode: identity
      users:
        alice: { uid: 1001, gid: 1001, groups: [developers, ops] }
        bob:   { uid: 1002, gid: 1002, groups: [developers] }
      groups:
        developers: 2000
        ops:        2001
```

| Mode | Behaviour |
|---|---|
| `squash` *(default)* | Ignore the caller, run every op as a fixed UID/GID. Matches NFS `all_squash`. |
| `static` | Look up the principal in a static table → `{uid, gid, gids[]}`. |
| `identity` | Same as `static` today; structured so future backends (LDAP, OIDC claims, external resolver) plug in without proto changes. |
| `passthrough` | Trust `proto.Caller.Owner`. Only safe on a LAN with a shared UID namespace; **must be opt-in per volume**. |

`mode: squash` to the server process's own UID/GID is the safe default when
nothing is configured.

### 3.3 Server-side permission evaluation

Linux has no per-thread `setgroups`, so we cannot enforce supplementary-group
membership via `setfsuid` + `setfsgid` alone. Instead, the server resolves the
principal's full identity (`uid`, primary `gid`, supplementary `gids`) and
performs the POSIX permission check in user-space before delegating to the
loopback FS:

1. Identity-resolver middleware (new) resolves the principal → identity and
   stashes it on the `fuse.Context`.
2. Permission-eval middleware (new) `Lstat`s the target, reads
   `{uid, gid, mode}` (and POSIX ACLs if present), and evaluates the check
   against the resolved identity. Returns `EACCES` on failure.
3. On success, `AssumeUserMiddleware` continues to set `fsuid`/`fsgid` to the
   primary identity so that newly-created files are owned correctly by the
   server kernel.

Phase 1 may ship with primary-group-only enforcement
(`setfsuid` + `setfsgid(primary_gid)`, no user-space eval) as a documented
limitation. Phase 2 introduces the user-space evaluator.

Edge cases the evaluator must handle:

- Sticky-bit directories (`/tmp` semantics): only the file owner or directory
  owner may unlink.
- Setgid directories: created files inherit the directory's GID.
- `chown`: allowed only if the principal owns the file and targets a GID it
  is a member of (unless a per-volume `can_chown_arbitrary` role is granted).
- `CAP_DAC_OVERRIDE` semantics: not granted to any principal by default.

### 3.4 Identity advertisement (`WhoAmI`)

The client cannot render `ls -l` sensibly without knowing its server-side
identity. Add a session-scoped RPC:

```proto
message Identity {
  string principal              = 1;  // "alice"
  uint32 uid                    = 2;  // server-side UID
  uint32 primary_gid            = 3;
  repeated uint32 gids          = 4;  // supplementary
  string user_name              = 5;  // display name
  map<uint32, string> group_names = 6;
}
```

Called once at session start, cached for the session, re-fetched on auth
refresh. TTL on the cache (default 60s, configurable).

The full user/group table is **never** pushed to the client — only the
caller's own identity. The server is the only thing that knows the mapping.

### 3.5 Symbolic names in `Attr`

Extend `proto.Owner` so `GetAttr` and friends can ship display-friendly names
alongside the numeric IDs:

```proto
message Owner {
  uint32 uid        = 1;
  uint32 gid        = 2;
  string user_name  = 3;  // optional, best-effort
  string group_name = 4;
}
```

Server fills these in from its identity resolver's cache. Client uses them
for display fallback when the numeric ID is unknown.

### 3.6 Client-side UID/GID rewriting

In the identity model, server-side files for principal *alice* are owned by
the *server's* UID for alice (e.g. `1234`), not the user's local UID (e.g.
`1001`). Without rewriting, `ls -l` shows numeric strangers, `cp -p` breaks,
Make rules that check ownership misfire, and `git` complains about untrusted
ownership.

The client FUSE layer rewrites UIDs/GIDs on the way in and out, anchored to
the session identity from `WhoAmI`:

**Inbound (server → client) — in `GetAttr`, `OpenDir`/readdirplus, etc.:**

- `attr.uid == server_self_uid`   → rewrite to local mounting user's uid.
- `attr.gid == server_primary_gid` → rewrite to local mounting user's gid.
- Any `attr.uid` matching a known supplementary group/identity → rewrite
  symbolically if a local name exists, else map to `nobody`/`anon_uid`.
- Everything else → `nobody`/`anon_uid`.

**Outbound (client → server) — `Chown`, `Create`, etc.:**

- Local mounting user's uid → server self uid.
- Reject or remap `chown` to UIDs/GIDs we cannot resolve.

This is the model used by `sshfs -o idmap=user` and NFSv4 + idmapd.

**Consequence:** a gMountie mount is implicitly **single-user** — it
represents one principal's view of the volume. `allow_other=false` is the
default and (for the first release) the only supported mode. Multi-user
mounts can be reconsidered later if there's demand.

### 3.7 Client-root handling

Without changes, a client running as `uid=0` causes `setfsuid(0)` on the
server thread, which (when the server runs as root) bypasses every permission
model in this document. This is NFS's `no_root_squash` footgun.

**In `identity` / `squash` / `static` modes**, the client's UID is advisory
only, so the footgun is closed by construction. A client running as `uid=0`
is just "the principal, who happens to be local root" — server identity is
unchanged.

**In `passthrough` mode**, apply `root_squash` by default: incoming
`caller.Owner.Uid == 0` is mapped to a configured `anon_uid` (default: the
server's own UID, never `0`). An explicit per-volume `allow_root: true` flag
unlocks `no_root_squash` behaviour; the server refuses to start with
`allow_root: true` combined with `auth: basic`, which is footgun-on-footgun.

**Interim safety patch (independent of the rest of this design):** harden
`assume_user.go` to reject or remap `context.Owner.Uid == 0` before calling
`setfsuid`. A ~5-line change that closes the most acute version of the
problem while the larger work proceeds.

### 3.8 Client mount defaults

To make the mount feel local *and* be safe:

- `nosuid`, `nodev` always (defence in depth even though the server enforces
  ACLs).
- `allow_other=false`, `allow_root=false` (single-user mount).
- `entry_timeout` / `attr_timeout`: leave at FUSE defaults (1s) initially;
  expose as config knobs.

## 4. Worked examples

### 4.1 `ls -l` of a directory containing files owned by me, by another user, and by root

1. Client issues `OpenDir` (later: readdirplus).
2. Server evaluates dir-read permission against the principal's identity.
3. Server returns entries; each `Attr.Owner` carries numeric IDs plus
   `user_name` / `group_name` for the principal's own identity.
4. Client rewrites:
   - Files owned by the principal → local uid + local user_name.
   - Files owned by anyone else → `nobody` / `nogroup`.
5. Output looks like a normal `ls -l` with one "me" and a bunch of
   "nobody"s. No leaked server-side names for other users.

### 4.2 `cp -a /home/alice/foo /mnt/gmountie/photos/foo`

1. `cp` calls `creat` → client sends with the local caller's UID; server
   ignores it, creates the file owned by the principal's server-side UID
   (using `setfsuid` for correctness in the kernel).
2. `cp` calls `chmod`, `utimens` → server runs as the principal, succeeds.
3. `cp` calls `chown alice:alice` → client maps `local-alice` → server
   principal; server checks "is the principal alice and targeting her own
   gid?" — yes — allowed.
4. End state: file appears locally as owned by alice. `cp -a` works.

### 4.3 Backup tool (`restic` / `rsync -a`) running as local root

1. Local root reads the mount → client sends caller as uid=0; server ignores
   it (identity mode); server permission check uses the principal's identity.
2. The tool only sees files the principal can read.

If "back up everything" is required, create a dedicated principal with a
`bypass_read_perms` role on the volume and run the backup tool authenticated
as that principal. Do not couple privilege to client-side UID.

## 5. Proto changes

Summary of additions / extensions (details belong in the proto PR):

- New service `Session` (or extend `Volume`): `WhoAmI(volume) → Identity`.
- `Owner` adds optional `user_name`, `group_name`.
- `proto.Caller.Owner` is preserved but documented as advisory.
- No proto change required for the user-space permission evaluator (server
  internal).

## 6. Server changes

- `pkg/common/config`: per-volume `mapping` block with `mode`, `uid`, `gid`,
  `users`, `groups`, `allow_root`, `anon_uid`.
- New middleware: `IdentityResolverMiddleware` runs first, resolves principal
  → identity, stashes on context.
- New middleware: `PermissionEvalMiddleware` (phase 2) performs user-space
  POSIX check.
- `AssumeUserMiddleware`: hardened to refuse `Uid == 0` unless explicitly
  configured.
- `AuthService`: propagate authenticated principal onto the gRPC context so
  the identity middleware can read it.
- `controller.createContext`: read the resolved identity from the gRPC
  context, not from `proto.Caller.Owner`.

## 7. Client changes

- `pkg/client/grpc`: call `WhoAmI` at session start; cache `Identity` with
  TTL; expose to the FUSE layer.
- `pkg/client/io`: rewrite UIDs/GIDs in `Attr` returns; rewrite outgoing
  `Chown` calls.
- Mount defaults: `nosuid`, `nodev`, `allow_other=false`.

## 8. Deferred / open questions

- **POSIX advisory locks.** Required by compilers, sqlite, git, dpkg.
  Proto needs `SetLk` / `GetLk`; server needs lock tracking; needs a recovery
  story across reconnects. Phase 3.
- **Session re-establishment.** After a long network outage, can the client
  re-authenticate and continue using its open file handles, or does the
  mount have to be torn down? Leaning toward "re-open by path on reconnect,
  return `ESTALE` if gone" rather than full NFSv4-style state recovery.
  Phase 3.
- **Readdirplus.** Return `Attr` alongside `DirEntry` to avoid the
  N-stats-per-`ls -l` cliff. Independent of this design; should ship early
  because the perceived-latency impact is huge.
- **POSIX ACLs end-to-end.** Mostly free via xattr passthrough
  (`system.posix_acl_*`), but the user-space evaluator must understand them
  for the check to be accurate.
- **Permission-hint field on `Attr`** (effective r/w/x bitmask for the
  caller): nice for UIs and for short-circuiting doomed ops on the client.
  Cheap once the evaluator exists; defer until a use case appears.
- **Server-push identity invalidation.** Skip until there's a real use case;
  TTL + auth-refresh is enough.

## 9. Phasing

1. **Phase 1 — Identity foundation.**
   - Config schema for `mapping`.
   - `IdentityResolverMiddleware` (squash, static, identity, passthrough).
   - `WhoAmI` RPC + client-side identity cache.
   - Client-side UID/GID rewriting.
   - Symbolic names in `Owner`.
   - Hardened `AssumeUserMiddleware` (reject uid=0 by default).
   - Interim: primary-group-only enforcement.
2. **Phase 2 — Correct permissions.**
   - User-space `PermissionEvalMiddleware` with supplementary groups,
     sticky bits, setgid dirs.
   - POSIX ACL awareness in the evaluator.
3. **Phase 3 — Local feel.**
   - Readdirplus.
   - POSIX locks.
   - Session re-establishment / handle recovery.
4. **Phase 4 — Polish.**
   - Effective-access hint on `Attr`.
   - Perf tuning (writeback cache, attr-timeout knobs, pipelining audit).

## 10. North-star acceptance test

A gMountie mount passes the "feels local" bar when, on a freshly-mounted
volume:

1. `git clone` of a real-sized repository completes without errors.
2. `git status` in that working tree returns in time comparable to a local
   FS (within 2–3× over a typical internet link).
3. `make` against a sample C project builds successfully, including any
   atomic-rename and `fsync` operations the toolchain performs.

These three cover ownership rendering, atomic rename, open-then-unlink,
fsync, hardlinks, readdir cost, and basic locking — most of the long tail of
"feels local" in one suite.
