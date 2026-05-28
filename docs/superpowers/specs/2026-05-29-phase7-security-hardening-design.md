# Phase 7 — Security Hardening Design

**Status:** Draft, 2026-05-29
**Goal:** make gMountie deployable on a non-trusted network. Closes every gap
in roadmap Appendix A except those explicitly deferred to a future phase.

## 1. The one principle

**Plaintext leaves the loopback.** Everything outside `127.0.0.1` — gRPC,
basic-auth credentials, password storage, ops endpoints, reflection
metadata — is either encrypted, hashed, authenticated, or bound to
localhost. There is no "permissive default" that requires the operator
to remember to flip a switch to be safe.

## 2. Scope

In scope (this design):

- **Transport security.** Server-TLS terminates every gRPC connection.
  Clients verify the server cert. mTLS (client cert as principal) is an
  optional opt-in; basic-auth-over-TLS remains the default scheme.
- **Credentials at rest.** Basic-auth passwords are stored as argon2id
  hashes. A `gmountie genpass` subcommand prints a hash to paste into
  config. Startup rejects plaintext entries with a clear error.
- **Ops endpoints** (`/metrics`, `/healthz`, `/readyz`, `/version`,
  `/debug/pprof`) bind to `127.0.0.1` by default; cluster ops reach them
  via a sidecar / port-forward. Optional basic-auth gates them when
  binding to a non-loopback addr.
- **gRPC reflection** is opt-in via `server.grpc.reflection: true`
  (default `false`). Production deployments don't leak the service
  surface to unauthenticated callers.
- **DoS limits.** Per-connection caps: `max_recv_message_size`,
  `max_concurrent_streams`, `max_connection_idle`. Default values err
  on the side of "absorb a normal client" and reject obviously hostile
  traffic.
- **Per-user volume ACL.** `auth.users[].volumes: [<name>...]`
  restricts which volumes a principal can list, mount, or call. Default
  policy is configurable: `auth.default_allow: true` (current behavior,
  all configured volumes) or `false` (fail-closed; users must explicitly
  list).

Deferred / follow-up:

- **OIDC / JWT** as an auth scheme. Pulls in JWKS cache, key rotation,
  audience config. Schedule next once mTLS is exercised in real
  deployments.
- **Per-connection byte rate limit** (bytes/sec). Useful for multi-
  tenant shared servers; the request-size + concurrency caps cover the
  basic DoS shape.
- **OS-keyring credential storage on the client.** Today the client
  reads passwords from YAML; a follow-up moves that to the keyring
  (libsecret on Linux). Same scope problem the desktop UI has (Phase 8).

## 3. Design

### 3.1 TLS — server side

```yaml title="server.yaml"
server:
  tls:
    cert_file: /etc/gmountie/server.crt
    key_file:  /etc/gmountie/server.key
    # Client cert verification — when set, the server enables mTLS.
    # Empty: server-TLS only.
    client_ca_file: /etc/gmountie/clients-ca.crt
    # Min TLS version. Default is "1.3" — explicit so downgrade attacks
    # die in config validation, not at handshake time.
    min_version: "1.3"
```

Implementation:

- `pkg/server/grpc/server.go` `NewServer` reads the TLS config and
  passes `grpc.Creds(credentials.NewTLS(tlsCfg))` to the gRPC server.
- TLS config: `MinVersion: tls.VersionTLS13`, `NextProtos: ["h2"]`,
  reload-on-SIGHUP deferred (rotate by restart for now).
- If `client_ca_file` is set, `ClientCAs` populated and
  `ClientAuth: tls.RequireAndVerifyClientCert`. The verified client's
  `CommonName` (or first SAN) becomes the principal for mTLS auth.
- **No TLS config = fail startup.** There is no "TLS optional" toggle —
  every non-loopback connection is encrypted.
- An explicit `server.tls.disabled: true` override exists for local
  dev only; startup logs a loud `WARN` every 60s and the listener
  refuses any non-loopback bind addr.

### 3.2 TLS — client side

```yaml title="client.yaml"
server:
  endpoint: gmountie.example.com:9244
  tls:
    # Path to CA bundle. Empty: use system trust store.
    ca_file: /etc/gmountie/server-ca.crt
    # Skip server cert verification. Dev only; loud WARN at mount.
    insecure_skip_verify: false
    # Server name to verify against. Empty: derive from endpoint host.
    server_name: ""
    # mTLS — present this cert to the server.
    cert_file: ""
    key_file:  ""
```

Implementation:

- `pkg/client/grpc/factory.go` constructs `tls.Config` from the
  client's `TLSConfig`; passes `grpc.WithTransportCredentials(...)`.
- `pkg/client/grpc/auth.go` `BasicAuthCredentials.RequireTransportSecurity()`
  returns `true` (currently `false`). Trying to use basic-auth without
  TLS now errors at dial time instead of leaking the password.
- The commented-out TLS line in `pkg/client/grpc/client.go:276` becomes
  the actual code path.

### 3.3 Password hashing — argon2id

- New field `BasicAuthConfigUser.PasswordHash string` replaces
  `Password string`. Format is the standard PHC encoding
  (`$argon2id$v=19$m=65536,t=3,p=4$<salt>$<hash>`).
- Parameters: `m=64 MiB, t=3, p=4` (OWASP 2026 recommendation for
  argon2id).
- New CLI: `gmountie genpass` prompts for a password (no echo, twice
  to confirm) and prints the PHC string to stdout. Operators paste it
  into config.
- Server-side verification uses constant-time `subtle.ConstantTimeCompare`
  on the derived hash (`golang.org/x/crypto/argon2`'s `IDKey` does the
  KDF; comparison is on the resulting hash bytes after splitting the
  PHC format).
- **Migration / startup detection:** if any `users[].password_hash`
  doesn't start with `$argon2id$`, the server refuses to start with
  `cred storage requires argon2id hash; run 'gmountie genpass' and
  paste the output into <volume>.password_hash`. No silent acceptance
  of legacy plaintext.
- First-run default config writes a hash of `admin` (the existing weak
  default) with a `# CHANGE ME — run 'gmountie genpass'` comment. This
  preserves the current "it just works after `gmountie serve`"
  onboarding while making the weak credential discoverable.

### 3.4 Ops endpoints

- `server.metrics_addr` default changes from `:9090` to `127.0.0.1:9090`.
  Existing operators who bound it to `0.0.0.0:9090` get a startup
  `WARN` recommending the new default.
- New `server.ops.auth` block:
  ```yaml
  server:
    ops:
      addr: "127.0.0.1:9090"   # bind addr
      auth:
        type: basic            # basic | none
        users:
          - { username: prom-scraper, password_hash: "$argon2id$..." }
  ```
- `auth.type: none` is allowed only when `addr` is a loopback addr;
  combined with a non-loopback addr it's a startup error.
- The `/debug/pprof` mount stays gated behind the existing
  `server.pprof` flag — it's an additional layer on top of the new
  loopback default.

### 3.5 gRPC reflection + DoS limits

```yaml
server:
  grpc:
    reflection: false        # default false; opt-in for dev clusters
    max_recv_message_size: 16777216    # 16 MiB; matches frame_size default
    max_concurrent_streams: 256        # per connection
    max_connection_idle: 5m            # closes idle conns
```

- `pkg/server/grpc/server.go` currently registers reflection
  unconditionally; gate on the new flag.
- All four limits are gRPC server options
  (`grpc.MaxRecvMsgSize`, `grpc.MaxConcurrentStreams`,
  `grpc.KeepaliveParams{MaxConnectionIdle:...}`).

### 3.6 Per-user volume ACL

- Add `Volumes []string` to `BasicAuthConfigUser`. Empty / unset means
  "use the default policy"; explicit `[]` means "no volume access".
- New top-level `auth.default_allow: true|false` (default `true`,
  current behavior). `false` means a user without an explicit
  `volumes:` list cannot access any volume — fail-closed.
- Enforcement lives in **one place**: a new
  `VolumeService.PrincipalCanAccess(ctx, volumeName) error`
  that returns `PermissionDenied` if the authenticated principal
  (from ctx) isn't allowed. Every controller method calls this BEFORE
  `BindIdentity`. `VolumeService.List` filters its result through the
  same check.
- For mTLS principals, the cert CN/SAN is the principal name; the
  matching `users[]` entry's `volumes:` decides access. Server-side
  cert config can declare a fallback `volumes:` for "any verified
  client cert with no matching users[] entry" if useful.

### 3.7 Threat model — what we're NOT defending against

- **A compromised server.** If the server binary is replaced, all
  guarantees collapse. This is the same threat model as NFS / SSHFS.
- **Side channels in TLS.** Standard Go TLS hardening only.
- **Operator with config write access.** The operator can always
  reset passwords or disable TLS in config.

## 4. Worked examples

### 4.1 Internet-deployed multi-tenant

```yaml
server:
  tls:
    cert_file: /etc/letsencrypt/live/gmountie.example.com/fullchain.pem
    key_file:  /etc/letsencrypt/live/gmountie.example.com/privkey.pem
  grpc:
    reflection: false
  ops:
    addr: "127.0.0.1:9090"   # default

auth:
  type: basic
  default_allow: false       # fail-closed
  users:
    - username: alice
      password_hash: "$argon2id$v=19$m=65536,t=3,p=4$..."
      volumes: [photos, team]
    - username: bob
      password_hash: "$argon2id$..."
      volumes: [team]

volumes:
  - name: photos
    path: /srv/photos
    mapping: { mode: squash, uid: 1000, gid: 1000 }
  - name: team
    path: /srv/team
    mapping: { mode: system }
```

### 4.2 Hardened single-tenant with mTLS

```yaml
server:
  tls:
    cert_file: /etc/gmountie/server.crt
    key_file:  /etc/gmountie/server.key
    client_ca_file: /etc/gmountie/clients-ca.crt   # mTLS
  grpc:
    reflection: false

auth:
  type: mtls                 # principal = client cert CN
  default_allow: false
  users:
    - username: backup-runner   # matches cert CN
      volumes: [archive]
```

### 4.3 Local dev — disabled

```yaml
server:
  bind: 127.0.0.1:9244
  tls:
    disabled: true           # logs WARN every 60s
```

## 5. Phasing

One spec, multiple PRs because the surface is wide. Each PR is its own
worktree (per the project working agreement).

1. **PR 1 — TLS foundation.** Server + client TLS config and bootstrap.
   `RequireTransportSecurity() = true`. `tls.disabled` dev escape hatch.
   Worked-example smoke test on the VM. No password-hash change yet —
   shipped in PR 2.
2. **PR 2 — Password hashing.** `password_hash` field; `gmountie genpass`;
   startup rejection of plaintext; first-run default writes a hash.
3. **PR 3 — Ops endpoints.** Loopback bind default; optional auth;
   loud WARN when binding to a non-loopback addr without auth.
4. **PR 4 — gRPC reflection + DoS limits.** Reflection opt-in; the
   four limits exposed in config.
5. **PR 5 — Per-user volume ACL.** `users[].volumes`,
   `auth.default_allow`, `PrincipalCanAccess` centralized check.
6. **PR 6 — mTLS auth scheme.** `auth.type: mtls`; cert-CN-as-
   principal; integration with the existing identity layer.

PRs 1-5 are independent of each other except 1 must merge first
(everything else assumes TLS is on the wire). PR 6 may land before or
after PR 5 — they're orthogonal.

## 6. Testing

- **Unit:** TLS config parsing + validation; argon2id verify roundtrip;
  ACL check matrix; ops auth interceptor; reflection gate.
- **VM e2e:** server-TLS handshake with a self-signed cert from the
  test fixture; basic-auth-over-TLS round-trip; mTLS round-trip in
  PR 6; rejection of plaintext password in config; rejection of
  non-loopback ops bind without auth; volume-ACL deny path.
- **Existing test suites must remain green.** Most don't speak TLS;
  the test harness will spin up a self-signed cert per `NewAppTestingContext`
  so every existing test exercises TLS transparently.

## 7. Decisions to lock

| # | Decision | Choice |
|---|---|---|
| 1 | TLS minimum version | 1.3 |
| 2 | Password KDF | argon2id (m=64 MiB, t=3, p=4) |
| 3 | Ops bind default | 127.0.0.1 |
| 4 | Reflection default | off (opt-in) |
| 5 | ACL default | `default_allow: true` (compat); operators choose `false` for fail-closed |
| 6 | mTLS principal source | client cert CN; SAN if CN empty |
| 7 | `tls.disabled` dev mode | allowed but refuses non-loopback bind |
| 8 | Existing weak admin/admin default | replaced by first-run hash + CHANGE ME comment |

## 8. North-star acceptance test

A gMountie server bound to `0.0.0.0:9244`, TLS terminated by Let's
Encrypt, configured with three users and three volumes via
`default_allow: false`, can be operated for a week under:

- Steady gRPC traffic from authenticated clients,
- `nmap` and `testssl.sh` against the public port,
- An attacker who knows the proto definitions (full reflection-like
  knowledge) but no credentials,

with **zero unauthenticated bytes** leaving the loopback, **zero
plaintext credentials** in logs or in config, and **zero volume
accesses** from a principal not explicitly granted them.
