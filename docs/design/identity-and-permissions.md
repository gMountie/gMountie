# Identity, Permissions, and Local Transparency

**Status:** Shipped (Phase 1a → 3, 2026-05-28)
**Last updated:** 2026-05-28

This is the as-shipped record of gMountie's identity model. The deep-dive
brainstorm spec at
`docs/superpowers/specs/2026-05-27-identity-permissions-design.md` covers
the rationale and rejected alternatives; this document is the durable
summary you should read first.

## 1. The one principle

The **authenticated principal is the identity boundary.** Whoever the gRPC
auth interceptor accepted is who the server runs ops as on Linux, by
assuming that principal's full credentials on the OS thread handling the
request. The kernel enforces permissions, mode bits, supplementary
groups, sticky bits, setgid dirs, and POSIX ACLs natively.

`proto.Caller.Owner` (the uid/gid the client claims it is) is **advisory**
in every mapped mode and **authoritative only in `passthrough`**.

## 2. Per-volume mapping modes

Each volume declares one mode. `squash` is the default when nothing is
configured.

| Mode | Principal → identity | When to use |
|---|---|---|
| `squash` *(default)* | everyone → one fixed `{uid, gid}` | shared appliance volume; safe default |
| `static` | principal → config table `{uid, gid, gids, caps}` | locked-down server with no OS accounts for these principals |
| `system` | principal name → real OS account via NSS (`getent`/`id`, honors LDAP/SSSD) | real multi-user box with admin'd accounts |
| `passthrough` | wire `{uid, gid}` verbatim | I control both ends / trusted LAN |

`passthrough` honors `root_squash` (default `true`): a wire `uid=0` is
remapped to `anon_uid` (default `65534` = nobody) unless explicitly
disabled. The server's own root (`uid=0`) is never a valid squash target;
the resolver falls back to nobody if `anon_uid` is left at `0`.

## 3. Server-side enforcement — kernel-native, per-thread

Per request, on a `runtime.LockOSThread`-pinned OS thread, the
identity-bound FS wrapper (`pkg/server/io/bound_fs.go`):

1. Snapshots the thread's current supplementary groups (for restore in
   step 5), then **raw `SYS_setgroups(id.Gids)`** — Go's
   `syscall.Setgroups` broadcasts across all threads, so we issue the
   syscall directly so only the pinned thread is affected.
2. `setfsgid(id.Gid)`.
3. `setfsuid(id.Uid)` — this also drops the fs-related capabilities,
   so the kernel enforces DAC normally for an unprivileged principal.
4. Runs the loopback op. The kernel checks owner/group/other bits, the
   full supplementary group set, sticky-bit unlink rules, setgid-dir
   inheritance, and POSIX ACLs.
5. On cleanup, restores the original creds before unlocking. Any restore
   failure logs and leaks the thread (it dies with the goroutine) — the
   pinned thread is **never** released to the runtime with stale creds.

Capability levels live on the resolved `Identity.Caps`:

- **no caps** → the path above (unprivileged kernel enforcement).
- **`dac_read_search`** → after step 1+2, **keep `fsuid=0`** and raw
  `SYS_capset` to drop `CAP_DAC_OVERRIDE`/`FOWNER`/`FSETID` from the
  **EFFECTIVE** set while keeping `CAP_DAC_READ_SEARCH` (and
  `CAP_SETUID`/`SETGID` for the restore path). PERMITTED is left
  untouched — it is monotonically non-increasing on Linux, so dropping
  from it would forfeit the ability to restore. Restore re-raises the
  saved EFFECTIVE.
- **`dac_override`** → after step 1+2, **keep `fsuid=0`** and keep all
  caps. After a successful `Create`/`Mkdir`/`Symlink`/`Mknod`/`Link`,
  the new entry is `fchown`ed to the principal so admin writes don't
  silently leave root-owned files. `fchown` failure logs and continues
  (admin-path edge for manual cleanup).

Both cap paths require the **server to run as root**. Non-root servers
silently ignore caps and run every principal unprivileged.

## 4. Volume confinement

`pkg/server/io/confined.go` replaces the unconfined go-fuse loopback.
Every wire path is resolved beneath the volume root via
`openat2(RESOLVE_BENEATH | RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV)`.
Absolute paths, leading `..`, and symlink targets outside the volume
all return `EXDEV` from the kernel, which the FS layer maps to
`EACCES`. Open/Create return confined fds so the data path inherits
the boundary without controller-layer changes.

Confinement gates the admin caps: a `dac_override` server over an
unconfined loopback would be a remote root-equivalent escape via a
planted symlink. Phase 2 ships before Phase 3 for exactly this reason.

`Access` and `Chmod` use the openat2 leaf-resolution pattern + a
`/proc/self/fd/N` reference for the syscall, because `Faccessat`/
`Fchmodat` with `flags=0` follow symlinks unguarded and would let a
symlink → /etc/passwd be access-checkable or chmod-able through the
volume.

## 5. Client-side rendering

The server resolves identity per request; the client needs to know
"who am I on this volume" once at mount time so `ls -l` shows local
uids and FUSE-emitted Chown ops carry the right wire uid.

- `SessionService.WhoAmI(volume, caller)` returns the principal's
  resolved `Identity` (uid, gid, gids, user_name, group_names).
- `pkg/client/io/idrewrite.go` translates inbound attrs (server-side
  uid/gid → local uid/gid for `ls -l`) and outbound `Chown` (local
  uid/gid → server-side).
- `--raw-ids` / config `mount.raw_ids: true` disables rewriting for
  backup tools or admin scripts that need to see the server's view.
- If `WhoAmI` fails, the mount succeeds with raw ids (graceful
  degradation — no rewrite is better than no mount).

## 6. Cache & subscribe — identity-scoped

- `GetAttrIfChangedRequest` carries `Caller` so passthrough cache
  revalidation uses the wire identity instead of anon-squashing.
- `SubscribeRequest` carries `Caller` so the event stream is filtered:
  for each event the server runs `Access(path, F_OK)` against the
  bound identity-FS and drops events for paths the principal cannot
  see. Heartbeats always pass. Renames require both old and new
  paths to be accessible.

## 7. Symlinks

Server's confined FS implements `Readlink`/`Symlink`; the client
exposes them via go-fuse's `NodeReadlinker`/`NodeSymlinker` so the
kernel's standard symlink chase Just Works through the mount. Server-
side, `Readlink` is identity-bound but no session (read-only metadata);
`Symlink` uses the standard session + idempotency + MUTATED event
emit shape.

## 8. Deferred (still on the list)

- **POSIX advisory locks** (`SetLk`/`GetLk`) — needs a reconnect
  recovery story.
- **Readdirplus** — independent perf win; ships separately.
- **Multi-user mounts (`allow_other`)** — single-user is the only
  supported mode for the first release.
- **Identity refresh on resume** — currently the client captures
  identity at mount time; long sessions don't see server-side group
  changes until remount.
- **`chown_any` capability** — superseded by `dac_override`.
- **POSIX ACL passthrough** — already covered by kernel-native
  enforcement (the kernel honors ACLs natively as long as the backing
  filesystem is mounted with `acl`).
- **Server-push identity invalidation** — TTL + auth-refresh covers
  the use cases we have.

## 9. Acceptance — alice writes, bob reads

The wire-level seam test (`test/e2e/api/`) authenticates as a real
principal over basic-auth and asserts:

- Per-principal kernel enforcement (alice's chmod 700 file is EACCES
  to bob even though both are mapped uids on the same volume).
- Confinement (escape paths over gRPC return `PermissionDenied`).
- Admin caps (a `dac_read_search` principal reads a 0o600 file owned
  by a third uid; a `dac_override` principal writes that same file and
  creates entries that land owned by the admin, not root).

These three together close the "feels remote" gap: the application sees
local Linux behavior, regardless of what credentials the client
process holds.
