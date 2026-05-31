# Server-Side Revocation — Design

**Status:** Approved (brainstorm 2026-05-31). One PR.
**Repo:** `github.com/gMountie/gMountie` (OSS), module `go.gmountie.dev/gmountie`.

## Purpose

gMountie issues **long-lived** mTLS client certs (no expiry — the cloud's
Candidate D auth leans on revocation instead of short TTLs). Today the server
reads its config **once at startup**, has **no revocation** of any kind, and
**no `VerifyPeerCertificate`** anywhere. This feature adds the revocation
mechanism the whole auth model depends on: an operator can kick a user or a
single lost device **without restarting the pod** (a restart kills *all*
sessions on the volume — the exact disruption we avoid).

It is also a feature any self-hoster wants: reload the ACL and revoke a
credential without a restart.

## Decisions (locked)

1. **One PR**, ~8–10 TDD tasks, each independently green.
2. **Reload re-reads the config file** (not a request body). The cert-serial
   blocklist and the ACL live in the server config, so they are **durable
   across a pod restart for free** — a restarted pod re-reads them and stays
   **fail-closed**. (A push-into-memory-only design would fail *open* on
   restart.) The cloud operator writes the new config onto the pod and triggers
   the reload; the projection-freshness problem is the cloud's, not the OSS
   server's.
3. **Ops-plane auth is pluggable**: basic-auth/none stays the default; an
   optional **operator mTLS** mode is added for the mutating reload endpoint.
   The ops HTTP server today is plain HTTP with no TLS — this adds an optional
   TLS listener.

## Components

### 1. Two atomic snapshots (the hot-reloadable state)

Today `VolumeServiceImpl` holds `aclByPrincipal` / `defaultAllow` / `aclEnabled`
as plain fields, built once in `NewVolumeService` and read **unlocked** (safe
only because immutable). To reload them under concurrent RPC traffic:

- **ACL snapshot.** Bundle the three fields into an immutable `aclSnapshot`
  struct held in an `atomic.Pointer[aclSnapshot]`. `PrincipalCanAccess` does one
  `.Load()` per call. Extract today's build-once logic into a shared
  `buildACLSnapshot(cfg *config.Config) *aclSnapshot` used by **both** the
  constructor and the new reload path. New method:
  `VolumeService.ReloadAuth(cfg *config.Config)` rebuilds the snapshot and
  atomic-swaps it. The `volumes` map and `boundFSCache` are **not** touched by a
  reload (only access permissions change, not which volumes exist).

- **Revocation store.** New `RevocationStore`
  (`pkg/server/service/revocation.go`): an
  `atomic.Pointer[map[serialKey]struct{}]` of blocked cert serials.
  - `Set(serials []string)` — rebuild + swap the blocklist.
  - `IsBlocked(serial *big.Int) bool` — one `.Load()` + map lookup.
  - `serialKey(*big.Int) string` — canonical **lowercase hex** of the serial,
    applied identically when loading the blocklist and when checking a presented
    cert, so formatting can never cause a miss.

  Lives on `AppContext`. Writer: the ops reload handler. Readers: the TLS
  handshake hook, the gRPC auth interceptor, and the reaper.

`atomic.Pointer` is a new pattern for this codebase (existing concurrency uses
`xsync.MapOf` and one `sync.RWMutex` in `io/eventbus.go`); it is the right fit
because each snapshot is immutable once built and is read on the hot RPC path.

### 2. Config additions (the durable source of truth)

- `auth.revoked_serials: ["<hex>", …]` — the cert-serial blocklist. Read at
  startup **and** on reload ⇒ durable ⇒ fail-closed across restart. Basic-auth
  deployments simply leave it empty.
- `server.ops.tls: { cert_file, key_file, client_ca_file }` and
  `server.ops.auth.type: mtls` — **optional**. When set, the ops listener serves
  TLS with `RequireAndVerifyClientCert` against `client_ca_file`. When absent,
  the ops server is unchanged (plain HTTP, basic-auth or none, loopback-bound).

The ACL itself is already in config (`auth.users[].volumes`, `default_allow`) —
no schema change there; the reload just re-reads it.

### 3. `POST /ops/acl/reload`

A new route on the existing ops HTTP server (`pkg/server/ops/`). No request
body. Behavior:

1. Re-read the config from its **known file path** (captured at startup).
2. **Validate** the freshly-parsed config. On any parse/validation error →
   `400`, **no swap, no reap** (fail-safe: never apply a broken reload; the
   previous good state stands).
3. On success: `VolumeService.ReloadAuth(newCfg)` (swap ACL snapshot) →
   `RevocationStore.Set(newCfg.Auth.RevokedSerials)` (swap blocklist) → run the
   reaper.
4. Respond `200 {"reaped": <n>}`.

Only auth state is hot-reloaded; volumes, FS wiring, and live sessions' fds are
otherwise left intact. The handler needs `VolumeService`, `SessionManager`, the
`RevocationStore`, and the config path — passed in by extending
`ops.NewServer(...)` (today it receives none of these).

Auth on this route: basic-auth (default) or operator mTLS (when
`server.ops.tls` is configured). Because it mutates authorization, the security
posture is **operator mTLS for the cloud**; basic-auth on loopback remains fine
for a single-operator self-host.

### 4. Three enforcement layers for cert-serial revocation

A blocked serial must be rejected on three paths, because each covers a gap the
others don't:

- **Handshake** — add `tlsCfg.VerifyPeerCertificate` (in `app.go`, in the mTLS
  branch) that reads the `RevocationStore` and **rejects the TLS handshake** if
  the verified leaf serial is blocked. Stops *new* connections from a revoked
  device. (No hook exists today.)
- **Per-RPC** — the gRPC auth interceptor (`grpc/auth.go`) checks the presented
  serial isn't blocked, denying with `Unauthenticated`. Catches connections that
  completed their handshake **before** the serial was blocked (the hook only
  fires on new handshakes).
- **Reaper** — on reload, force-close the fds of revoked sessions (next
  section). Required, not just an optimization: an already-open fd does **not**
  re-run `PrincipalCanAccess` on each Read/Write, so without the reap a revoked
  device keeps reading a file it held open at revocation time until it chooses
  to close it.

### 5. Session carries its serial; the reaper

- `Session` gains `Serial() string`; `SessionManager.Create` grows a serial
  parameter, threaded from a new `service.VerifiedCertSerial(ctx)` (sibling of
  `VerifiedCertPrincipal`, reads `leaf.SerialNumber` from the verified chain)
  called in the `SessionController.Create` handler. Basic-auth connections have
  no client cert ⇒ empty serial ⇒ never matches the blocklist.
- The **reaper** ranges all live sessions (via a new
  `SessionManager.ReapIf(predicate)` or `Range`, since there is no
  enumerate-by-principal API today) and `ReleaseAll`s any session for which the
  predicate holds.

**Reaper predicate (the exact, selective rule).** Reap a session **iff**:

1. its own cert serial is in the *new* blocklist, **OR**
2. its principal can no longer access **any** configured volume (evaluated
   against the *new* ACL snapshot).

Everything else is left untouched. Consequences, stated precisely:

- An **additive** reload — enrolling a new device, granting a principal access —
  blocks no serial and removes no access ⇒ predicate false for every session ⇒
  **zero reaps**, zero disruption. (A test asserts exactly this.)
- **Revoke one device** = add that device's serial to `revoked_serials`. Only
  sessions presenting that serial reap; the same user's other devices (different
  certs ⇒ different serials) are untouched.
- **Revoke a whole user** = remove the principal from `users[].volumes`. All of
  that principal's sessions reap (condition 2).
- **Reap granularity is the session.** In a multi-volume deployment where a
  principal loses *one* of several volumes (still has others), condition 2 is
  false, so the session is **not** reaped — the now-denied volume is enforced by
  the per-op `PrincipalCanAccess` gate instead (already-open fds on that volume
  are the documented coarse edge). For the cloud's one-volume-per-user model,
  losing the volume = losing all = exact reap.

`ReleaseAll` closes the session's open fds; an in-flight Read/Write then fails
`EBADF` at the next frame (~1 MiB; sub-second to a few seconds). It does **not**
sever the TCP/TLS connection — denial of further RPCs comes from the per-RPC
serial check and the ACL gate. End-to-end revoke latency ≈ seconds (operator
reconcile + one HTTP call).

## Data flow — revoke a device

```
operator updates MountieVolume CR
  → renders new config onto the pod (adds serial to auth.revoked_serials,
    or removes principal from users[].volumes)
  → POST /ops/acl/reload  (operator mTLS)
      → re-read + validate config
      → swap ACL snapshot + serial blocklist
      → reaper: ReleaseAll on sessions matching the predicate
  → in-flight ops on reaped fds fail EBADF (~sub-second)
  → reconnect from the revoked serial rejected at the TLS handshake
```

A restarted pod re-reads `revoked_serials` from config ⇒ the serial stays
blocked ⇒ **fail-closed**.

## Error handling

- **Bad config on reload** → `400`, previous good state retained, nothing
  swapped or reaped. The reload is all-or-nothing.
- **Reload with no backing config file** (config came purely from defaults/env,
  no file path captured) → `400` with a clear message; there is nothing to
  re-read. (Cloud always has a file; this guards the self-host default-config
  case.)
- **Atomic swap** guarantees a concurrent `PrincipalCanAccess` / `IsBlocked`
  always sees a fully-consistent snapshot — never a half-updated map.
- **Empty/whole-config validation**: ReloadAuth only consumes `cfg.Auth`; a
  change to any non-auth field in the file is ignored by the reload (documented
  — only auth is hot-reloadable).

## Known gap (documented, accepted)

`SessionService.Create` does not itself check the ACL (only file `Open`/`Create`
do, via `BindIdentity`/`PrincipalCanAccess`). A revoked-but-still-cert-holding
caller can obtain a *new* session ID but is denied at the first file op — and,
once its serial is blocked, rejected at the handshake. One-RPC gap; documented
so no one assumes `Create` is the gate.

## Testing (all server-unit level — no FUSE, runs in the sandbox)

- `aclSnapshot` atomic swap + a concurrent reader **race test** (`-race`).
- `RevocationStore`: `Set`/`IsBlocked`, `serialKey` normalization (leading
  zeros, case, with/without separators).
- `VerifiedCertSerial`: extracts the serial from synthetic verified chains;
  returns absent for basic-auth (empty `VerifiedChains`).
- Reaper: **additive reload reaps nothing**; blocked-serial session reaped;
  fully-denied-principal session reaped; a same-user different-serial session
  and a still-permitted session both survive.
- `VerifyPeerCertificate` hook: blocked serial → handshake error; allowed serial
  → pass.
- Interceptor: blocked serial → `Unauthenticated`.
- Ops endpoint: reload applies + reaps; **bad config → 400, no swap** (assert
  prior state intact); basic-auth and mTLS auth both gate the route.
- Config parsing: `auth.revoked_serials`, `server.ops.tls`,
  `server.ops.auth.type: mtls`.

## Out of scope

- Renewal / short-TTL certs (we lean on revocation by design).
- CRL/OCSP (the blocklist is the revocation mechanism).
- Per-fd volume tracking for surgical partial-revocation reaping (session
  granularity is accepted; the per-op gate covers the denied volume).
- The operator, CRD, and config rendering (cloud side, separate repo).
