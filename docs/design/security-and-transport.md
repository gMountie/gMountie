# Security and Transport

**Status:** Shipped (Phase 7, PRs #53–#55, 2026-05-29; TLS leaf live-reload added in v0.15; client cert auto-renewal + volume-scoped client certs added in v0.16)
**Last updated:** 2026-06-12

The durable record of gMountie's transport security, credential storage,
and access-control model — what makes it deployable on a non-trusted
network. The brainstorm spec and per-PR implementation plans that drove
the design have been pruned now that the work has shipped; this document is
the durable record, and the task-by-task history lives in the git log.

## 1. The one principle

**Plaintext never leaves the loopback.** Everything outside `127.0.0.1`
— gRPC traffic, basic-auth credentials, password storage at rest, ops
endpoints, reflection metadata — is encrypted, hashed, authenticated,
or bound to localhost. There is no permissive default that requires the
operator to flip a switch to be safe; the zero-config first run already
lands on TLS.

## 2. Transport — server TLS

Every gRPC connection is TLS-terminated. Config (`server.tls`):

```yaml
server:
  tls:
    cert_file: /etc/gmountie/server.crt   # omit → auto-generate (§2.1)
    key_file:  /etc/gmountie/server.key
    client_ca_file: /etc/gmountie/clients-ca.crt  # set → enables mTLS (§6)
    min_version: "1.3"                    # default; validated to {1.2,1.3}
    disabled: false                       # dev-only escape (§2.3)
```

`pkg/server/grpc.NewServer` takes the credentials via a `WithCredentials`
option; the bootstrap in `pkg/server/app.go` builds the `tls.Config`
(`MinVersion: TLS1.3`, `NextProtos: ["h2"]`) before the listener binds.

**Cert rotation is live (leaf live-reload, v0.15).** Both the gRPC and
ops listeners serve their certificate through `pkg/server/tls.Reloader`,
a `tls.Config.GetCertificate` callback backed by a stat-checked cached
pair: when the cert file's identity stamp — `(mtime, size, inode)`, so
kubelet's atomic `..data` symlink swap is caught — changes, the next
handshake reloads the pair. Always-on, no config knob. Failure semantics
are **fail-open to the last good pair**: a torn swap, briefly missing
file, or unparsable PEM keeps serving the cached cert (one
state-transition warning, recovery logged); a successful swap logs the
old → new fingerprint. Existing connections are never re-handshaken, so
rotation disturbs no live session. Out of scope: client-CA pool reload
and client-side cert reload (client certs are long-lived by design);
there is no manual trigger surface (no SIGHUP, no ops endpoint).

### 2.1 Auto-generated cert — the SSH host-key pattern

When `cert_file`/`key_file` are unset and TLS is not disabled, the server
generates a self-signed cert on **first** startup (`pkg/server/tls`):

- ECDSA P-256, SHA-256, 10-year validity. CN = bind hostname (or
  `gmountie-server`); SAN includes the hostname + bound IP literals.
- Stored at `$XDG_STATE_HOME/gmountie/server.{crt,key}` (key `0600`,
  cert `0644`). Systemd installs get `/var/lib/gmountie/` via
  `StateDirectory=gmountie`.
- The SHA-256 fingerprint (`SHA256:<base64>`, SSH form) is logged once
  at startup.
- Subsequent starts load the existing files — no regeneration (rotate by
  deleting + restarting). **An operator-provided `cert_file` wins** and
  skips auto-gen entirely, so Let's Encrypt / internal-CA setups override
  cleanly with no code-path divergence.

### 2.2 `gmountie fingerprint`

A read-only subcommand prints the fingerprint of the cert the server
would present (config `cert_file` if set, else the auto-gen path). One
machine-readable line by default (`expected_fingerprint: $(ssh host
gmountie fingerprint)`); `--verbose` adds subject/issuer/validity. Never
auto-generates — inspection must not mutate state. Shares the cert-load
helper with `serve`.

### 2.3 `tls.disabled`

Local-dev escape only: starts plaintext, logs a recurring WARN, and the
listener **refuses any non-loopback bind**. Incompatible with mTLS
(startup error).

## 3. Transport — client verification

Config (`server.tls` on the client):

```yaml
server:
  tls:
    ca_file: /etc/gmountie/server-ca.crt  # empty → system roots
    verify: verify                        # verify | tofu | insecure
    expected_fingerprint: ""              # static pin (SHA256:…)
    server_name: ""                       # empty → derive from endpoint
    cert_file: ""                         # mTLS client cert (§6)
    key_file:  ""
```

Three verification modes (`pkg/client/tls.BuildConfig`):

- **`verify`** (default) — strict chain validation against `ca_file` or
  the system trust store. An optional `expected_fingerprint` adds a
  leaf-cert SHA-256 pin on top.
- **`tofu`** — trust on first use. The client pins the server's leaf-cert
  fingerprint to `$XDG_STATE_HOME/gmountie/known_hosts` (keyed by
  endpoint) on first connect, and refuses on any later mismatch ("cert
  fingerprint changed; … remove the entry … and re-pin"). The recommended
  pairing with auto-generated server certs — strictly safer than
  `insecure`, no CA distribution needed. SSH's exact pattern.
- **`insecure`** — skip verification; explicit prototyping escape, loud
  WARN.

`BasicAuthCredentials.RequireTransportSecurity()` is `true`, so basic
auth over plaintext fails at dial time rather than leaking the password.

## 4. Credentials at rest — argon2id

Basic-auth passwords are stored as argon2id PHC strings
(`$argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>`), parameters m=64 MiB /
t=3 / p=4 (OWASP 2026). `pkg/common/passhash` owns `Hash`/`Verify`
(constant-time)/`IsHashed`.

- **`gmountie genpass`** reads a password (no-echo on a TTY, double-entry
  confirm) and prints the PHC string to paste into config.
- **Startup is fail-closed:** any `password_hash` not starting with
  `$argon2id$` aborts startup pointing at `gmountie genpass`. No silent
  acceptance of plaintext.
- **First-run default config** (no `-c` flag): the server generates a
  **random** admin password via `crypto/rand`, hashes it with argon2id,
  writes the hash into `~/.config/gmountie/server.yaml`, and prints the
  plaintext password **once** to the console. The default bind is
  `0.0.0.0` (not loopback) — this is intentional: the random password
  eliminates the fixed-default-credential risk, and auto-TLS (§2.1)
  encrypts the channel, so the zero-config first run is safe to expose
  without a prior manual hardening step. Rotate with `gmountie genpass`.
- **TOFU pinning workflow:** run `gmountie fingerprint` on the server
  (prints `expected_fingerprint: SHA256:…`); paste the value into
  `server.tls.expected_fingerprint` in the client config with
  `verify: tofu`. The client will pin on first connect and refuse any
  cert change thereafter — same pattern as SSH `known_hosts`.

### 4a. Session-scoped authentication

argon2id is deliberately expensive (m=64 MiB / t=3), so verifying it on
**every** RPC made the server CPU/allocation-bound under high request rates —
a self-inflicted DoS and a real throughput collapse under load. Auth is
therefore **session-scoped**:

- The password is verified **once**, at `SessionService/Create` (and again at
  `Resume`, to re-prove identity after a reconnect). These two methods always
  run the full `authService.Authorize` path.
- `Create` binds the authenticated principal onto the server-side session
  (`Session.Principal()`), keyed by the session's UUIDv4 `session_id`.
- Every other RPC carries its `session_id` in gRPC metadata
  (`common.MetadataSessionID`). The auth interceptor resolves it to the live
  session and injects that principal **without** re-running argon2. The
  downstream volume-ACL check (`PrincipalCanAccess`) still runs per request, so
  authorization is unchanged — only the password re-derivation is skipped.
- **Fail-closed:** an absent, empty, or unknown `session_id` never grants
  access — it falls through to the full `Authorize`, which denies missing or
  invalid credentials. A valid `session_id` can only be obtained by passing
  argon2 at `Create`.
- **Credential omission:** once the keepalive-backed session is confirmed live,
  the client stops sending the now-redundant basic-auth username/password on the
  wire and sends only `session_id` (trimming per-RPC metadata). Basic-auth is
  still sent for `Create`/`Resume` and throughout any reconnect/recovery window
  (gated on a "session healthy" signal), so authorization continues to work if
  the session was reaped. This is a wire/perf refinement; the authorization
  decision above is unchanged.

**Model shift:** post-handshake, the `session_id` is a bearer credential for
the session's lifetime (until it is reaped, `GracePeriod` after disconnect). It
is a 122-bit UUIDv4 (`crypto/rand`) that travels only over TLS (§1), so it is at
least as strong as the username+password it replaces in the same metadata. This
is consistent with the threat model (§7): a stolen `session_id` is equivalent to
a stolen credential, and compromised clients/binaries are already out of scope.

### 4b. Session ownership binding and token hygiene

The bearer model only holds if the token stays secret and possession is the
*only* way to act as its owner. Two reinforcements:

- **Never log the live token.** `session_id` is a secret, so it is never logged
  in full. Every log site emits `session_fp` — the first 64 bits of
  `sha256(session_id)` (`common.FingerprintID`) — which correlates a session
  across log lines without exposing a replayable credential.
- **Bind the session to the caller's identity where one exists.** Under mTLS the
  connection carries an independent, unforgeable identity (the client cert), so
  the `session_id` must not override it. The auth interceptor therefore takes the
  argon2-skip fast path **only when no verified client cert is present** (the
  basic-auth case); with a client cert it always runs the cheap cert check, so
  the principal comes from the cert. Every consumer of a `session_id` —
  `resolveSession` (which hands out the open-fd table + idempotency cache),
  `Resume`, and `Keepalive` — then enforces `principal == session.Principal()`.
  This is a **no-op for basic-auth** (there the principal is derived from the
  session, so it matches by construction — the documented bearer model) and
  **binds mTLS**: a cert-CN=bob presenting alice's `session_id` is denied
  `PermissionDenied`, for both data access (her fds) and lifecycle ops (reaping
  her session). The fd-table guard is the important half — fd numbers are small
  per-session integers, so without it a leaked/guessed `session_id` would expose
  another principal's open handles.

Not yet addressed: an absolute **session lifetime cap** / `session_id` rotation
to bound a leaked basic-auth token's window — deferred because a filesystem
session is long-lived (holds open fds), so the robust form is live re-keying,
not a max-age that would break active mounts.

## 5. Server surface hardening

- **Ops endpoints** (`/metrics`, `/healthz`, `/readyz`, `/version`,
  `/debug/pprof`) bind to `127.0.0.1:9090` by default (`server.ops.addr`).
  An optional `server.ops.auth` block (`type: basic|none`, argon2id
  `users`) gates them; `auth.type: none` on a **non-loopback** addr is a
  startup error. Unknown-user requests run a sentinel-hash verify to keep
  auth latency uniform (no user-existence timing leak). `/debug/pprof`
  also stays behind the existing `server.pprof` flag.
- **gRPC reflection** is opt-in: `server.grpc.reflection` defaults to
  `false`, so production doesn't expose the service surface to anonymous
  callers.
- **DoS limits** under `server.grpc.limits`: `max_recv_message_size`
  (16 MiB), `max_concurrent_streams` (256), `max_connection_idle` (5m),
  `max_connection_age` (0 = unlimited; long-lived sessions are the norm).
  Idle/age fold into the gRPC keepalive parameters.

## 6. mTLS

`auth.type: mtls` makes the **verified client certificate** the identity.
The TLS layer does the cryptographic check: when mTLS is selected the
server sets `ClientCAs` (from `server.tls.client_ca_file`, required) and
`ClientAuth: RequireAndVerifyClientCert`. `mtlsAuthService` then extracts
the principal from the verified leaf — **CN, or the first DNS SAN when CN
is empty**. The client presents its cert via `cert_file`/`key_file`
(loaded into `tls.Config.Certificates`, orthogonal to how it verifies the
*server*). mTLS users carry only `volumes:` (no `password_hash`). mTLS is
incompatible with `tls.disabled`.

### 6.1 Client cert auto-renewal (token mode)

Static on-disk client certs are the default, but the client also supports
**in-memory cert auto-renewal** via an opt-in `renew` config block. In token
mode the client presents a bearer token to a token→certificate HTTP endpoint,
mints a fresh P-256 key in process memory, and exchanges a CSR for a
short-lived cert. The private key never leaves process memory; no cert or key is
written to disk. The initial exchange runs synchronously at mount start; a
background goroutine renews before expiry (`renew.before`, default 8 h). The
renewed cert takes effect at the next TLS handshake; active RPCs are
uninterrupted. The CA for verifying the data-plane server can be delivered by the
exchange endpoint when no `server.tls.ca_file` is configured. See
[`renew` block — client config](../client/config.md#certificate-auto-renewal-renew)
for the full field reference and wire contract.

### 6.2 Volume-scoped client certs

A client certificate may carry URI SANs of the form
`gmountie://vol/<volume-name>` to restrict it to specific volumes.
`VerifiedCertVolumeScopes` (`pkg/server/service/auth.go`) extracts these from the
verified leaf and `PrincipalCanAccess` enforces them **before** the per-user ACL,
so a scoped-out caller cannot learn whether the volume exists. A wildcard SAN
`gmountie://vol/*` is explicitly unrestricted. Certs with no such SANs are
unrestricted — fully backwards compatible. Scope can only narrow ACL access,
never widen it. There is no server config knob; scope is carried by the cert and
enforced by the server natively. See [Volume-scoped client certs — server
config](../server/config.md#volume-scoped-client-certs) for the SAN format table.

## 7. Per-user volume ACL

Restricts which volumes a principal may list, mount, or call.

- `auth.users[].volumes: [<name>…]` — explicit grant. Empty/unset = the
  default policy; explicit `[]` = no access.
- `auth.default_allow` (default `true` = compat). Set `false` for
  fail-closed: a principal with no explicit list gets nothing.
- **Single enforcement point:** `VolumeService.PrincipalCanAccess(ctx,
  volume) error` (returns `PermissionDenied`), folded into the top of
  `BindIdentity` so every FS op is covered, plus `List(ctx)` filtering
  and the `WhoAmI` path. The principal comes from `principal.FromContext`
  (set by the auth interceptor from the basic-auth username or the mTLS
  cert CN). A request with no authenticated principal is denied.

## 8. Revocation — reload + cert-serial blocklist

Client certs are long-lived (no short TTL), so revocation — not expiry — is
the protection against a lost device or a removed user. An operator revokes
**without restarting** the server (a restart kills *all* sessions on a volume;
revocation reaps only the revoked ones).

- **`auth.revoked_serials: [<hex>…]`** — the cert-serial blocklist. Read at
  startup **and** on reload, so a restart re-reads it and stays **fail-closed**.
  Any hex format (`ab:cd`, `0xABCD`, `abcd`) normalizes to one canonical key
  (`service.SerialKey`), so a config entry and a presented cert serial can never
  miss on formatting.
- **`POST /ops/acl/reload`** (ops plane) — re-reads the config file, validates
  it, atomically swaps the ACL (`VolumeService.ReloadAuth`) and the serial
  blocklist (`RevocationStore.Set`), then reaps. A bad/invalid config →
  **`400`, nothing swapped or reaped** (fail-safe — the prior good state
  stands). Only auth state is hot-reloaded; volumes/FS are untouched. Reload
  needs a config **file**: a server started with no `-c` and no on-disk config
  (state from defaults/env only) has nothing to re-read and returns `400`.
- **Reap predicate (`SessionManager.ReapIf`):** reap a session **iff** its cert
  serial is now blocked **OR** its principal can access no configured volume.
  An additive reload (grant a user, enrol a device) blocks no serial and removes
  no access → **reaps nothing**. Reaping `ReleaseAll`s the session's fds; an
  in-flight Read/Write then fails `EBADF` (~sub-second). The predicate is
  whole-session: a principal who loses **one** of several volumes (still has
  others) is **not** reaped — new ops on the now-denied volume are blocked by the
  per-RPC `PrincipalCanAccess` gate, but fds already open on it at revocation
  time keep working until closed. (In a one-volume-per-principal deployment,
  losing the volume = losing all → exact reap.)
- **Three enforcement layers for a blocked serial:** the TLS handshake
  (`VerifyPeerCertificate`) rejects new connections; the gRPC auth interceptor
  rejects per-RPC (catching connections that handshook before revocation, before
  the session fast-path); the reaper force-closes already-open fds. Basic-auth
  connections carry no client cert → never matched.
- **Ops-plane auth:** basic-auth (default, loopback) or **operator mTLS** —
  `server.ops.tls.{cert_file,key_file,client_ca_file}` + `server.ops.auth.type:
  mtls` turns the ops listener into `RequireAndVerifyClientCert`. The reload
  endpoint mutates authorization, so production uses operator mTLS.

**Operational footgun — revoke-by-deletion under `default_allow: true`.**
Deleting a principal from `users[]` is **not** revocation when `default_allow`
is `true` (the compat default): the principal falls through to "allowed", so the
reaper keeps the session and the cert still authenticates. To actually revoke a
**user**, set their `volumes: []`, or run `default_allow: false`. To revoke a
**device**, add its serial to `revoked_serials` (always effective regardless of
`default_allow`). The reload handler logs a warning when `default_allow: true`
so an operator isn't misled.

**Known gap (accepted):** `SessionService.Create` does not itself check the ACL
— a revoked-but-cert-holding caller can obtain a new session id but is denied at
the first file op, and rejected at the handshake once its serial is blocked.

## 9. Threat model — out of scope

- A compromised server binary (same model as NFS/SSHFS).
- TLS side channels beyond standard Go hardening.
- An operator with config write access (can always reset creds / disable
  TLS).

## 10. Deferred follow-ups

- **OIDC / JWT** auth (JWKS cache, key rotation, audience) — schedule
  once mTLS has real-deployment mileage.
- **Per-connection byte-rate limit** — the size + concurrency caps cover
  the basic DoS shape; bytes/sec matters for multi-tenant shared servers.
- **OS-keyring client credential storage** (libsecret) — today the client
  reads passwords from YAML; shares the desktop-UI scope problem (Phase 8).

## 11. North-star acceptance

A server bound to `0.0.0.0:9449`, TLS-terminated, `default_allow: false`,
operated under steady authenticated traffic plus `nmap`/`testssl.sh` and
an attacker who knows the proto surface but holds no credentials, leaks
**zero unauthenticated bytes** off the loopback, **zero plaintext
credentials** in logs or config, and grants **zero volume access** to a
principal not explicitly listed.
