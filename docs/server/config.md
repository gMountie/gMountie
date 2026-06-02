---
title: Server configuration
sidebar_label: Configuration
description: Every server.yaml field — server, keepalive, tls, grpc, ops, log, auth, volumes — with types, defaults, and valid ranges.
---

# Server configuration

The server reads a YAML file. The main sections are **`server`** (listen address and transport tuning, including the `tls`, `grpc`, `ops`, and `keepalive` sub-blocks), **`auth`** (credentials and per-user volume access), **`volumes`** (the directories you're sharing), and an optional **`log`** block. If you don't pass `-c`, gMountie writes a default config to `~/.config/gmountie/server.yaml` on first run.

## Configuration File Structure

The configuration file has these main sections:

- Server configuration (`server`, with `tls` / `grpc` / `ops` / `keepalive` sub-blocks)
- Authentication configuration (`auth`)
- Volumes configuration (`volumes`)
- Logging configuration (`log`, optional)

Basic example:

```yaml
server:
  address: 0.0.0.0
  port: 9449
auth:
  type: basic
  users:
    - username: admin
      password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # output of: gmountie genpass
volumes:
  - name: shared
    path: /srv/shared
```

## Server Options

The `server` section configures the core server settings:

| Option                          | Type     | Default      | Description                                                                                  |
|---------------------------------|----------|--------------|----------------------------------------------------------------------------------------------|
| address                         | string   | "0\.0\.0\.0" | IP address the server listens on                                                             |
| port                            | integer  | 9449         | Port number for the gRPC server                                                              |
| metrics                         | boolean  | true         | Enable/disable the ops HTTP server (`/metrics`, `/healthz`, `/readyz`, …)                    |
| metrics\_addr                   | string   | ":9090"      | **Deprecated** — use `server.ops.addr`. Legacy address for the ops HTTP server.              |
| max\_message\_bytes             | integer  | 16777216     | Cap on inbound/outbound gRPC message size (16 MiB default)                                    |
| frame\_size\_bytes              | integer  | 1048576      | Chunk size for server-streamed reads (1 MiB). Range [4096, 16777216].                        |
| compound\_max\_parallel         | integer  | 8            | Max concurrent sub-ops in flight for a single Compound RPC. Range [1, 256].                  |
| pprof                           | boolean  | false        | Expose `/debug/pprof/*` on the ops HTTP server                                               |
| subscribe\_buffer\_size         | integer  | 256          | Per-subscriber channel depth in the event bus. Minimum 1.                                    |
| subscribe\_heartbeat\_interval  | duration | 10s          | How often the event bus emits a HEARTBEAT to each live subscriber                            |

`max_message_bytes` is validated to the range [65536, 67108864] (64 KiB to
64 MiB). The default sits well above the streaming `frame_size_bytes` so a
single Read/Write frame plus header overhead always fits.

`metrics_addr` is **deprecated**: it is the legacy single address for the ops
HTTP server. New configs should use the [`server.ops`](#ops-endpoints) block
(`server.ops.addr`), which also carries the ops-listener auth and TLS settings.

Example:

```yaml
server:
  address: 192.168.1.100 # Listen on specific interface
  port: 8080 # Custom port
  metrics: false # Disable metrics
  max_message_bytes: 33554432 # 32 MiB
```

### Keepalive

The `server.keepalive` block tunes gRPC HTTP/2 keepalive pings. Defaults
make the server ping idle connections every 30s and tear them down 10s
after a missed ACK, so a dead client (or a half-open NAT path) surfaces
within ~40s instead of waiting on TCP timeouts.

| Option                          | Type     | Default | Description                                                      |
|---------------------------------|----------|---------|------------------------------------------------------------------|
| time                            | duration | 30s     | Interval between pings to an idle connection                     |
| timeout                         | duration | 10s     | Wait time for a ping ACK before closing the connection           |
| min\_time                       | duration | 10s     | Minimum interval the server tolerates between client pings       |
| permit\_without\_stream         | boolean  | true    | Allow client pings when no streams are in flight                 |

Example:

```yaml
server:
  keepalive:
    time: 15s
    timeout: 5s
    min_time: 5s
    permit_without_stream: true
```

### TLS

The `server.tls` block controls how the server presents TLS to connecting
clients. When `cert_file`/`key_file` are unset and `disabled` is `false`, the
server auto-generates a self-signed cert on first startup (the SSH host-key
pattern). Setting `client_ca_file` enables client-cert verification (mTLS).

| Option            | Type    | Default | Description                                                                |
|-------------------|---------|---------|----------------------------------------------------------------------------|
| cert\_file        | string  | ""      | Server certificate. Empty → auto-generate a self-signed cert on first run. |
| key\_file         | string  | ""      | Server private key. Empty → auto-generate.                                 |
| client\_ca\_file  | string  | ""      | CA bundle to verify client certs. Set → enables mTLS.                      |
| min\_version      | string  | "1.3"   | Minimum TLS version. Validated to one of {"1.2", "1.3"}.                   |
| disabled          | boolean | false   | Dev-only escape: serve plaintext. The listener refuses any non-loopback bind. |

```yaml
server:
  tls:
    cert_file: /etc/gmountie/server.crt
    key_file: /etc/gmountie/server.key
    min_version: "1.3"
```

### gRPC

The `server.grpc` block tunes gRPC server behavior that isn't transport (TLS)
or routing. `limits` caps per-connection resource use to absorb a normal client
and refuse obviously hostile traffic.

| Option                            | Type     | Default  | Description                                                                  |
|-----------------------------------|----------|----------|------------------------------------------------------------------------------|
| reflection                        | boolean  | false    | Enable the gRPC reflection service. Off by default so production doesn't leak the service surface to anonymous callers. |
| limits.max\_recv\_message\_size   | integer  | 16777216 | Largest single gRPC message the server accepts, in bytes (16 MiB).           |
| limits.max\_concurrent\_streams   | integer  | 256      | Max in-flight streams per single connection.                                 |
| limits.max\_connection\_idle      | duration | 5m       | Close idle connections after this duration. 0 disables.                      |
| limits.max\_connection\_age       | duration | 0        | Hard cap on total connection lifetime. 0 = unlimited (the gMountie norm).    |

```yaml
server:
  grpc:
    reflection: false
    limits:
      max_recv_message_size: 16777216
      max_concurrent_streams: 256
      max_connection_idle: 5m
      max_connection_age: 0
```

### Ops endpoints

The `server.ops` block controls the operational HTTP listener that serves
`/metrics`, `/healthz`, `/readyz`, `/version`, `/debug/pprof`, and
`/ops/acl/reload`. It binds `127.0.0.1:9090` by default so cluster ops reach it
via a sidecar / port-forward. Binding to a **non-loopback** address requires
`ops.auth.type` to be something other than `none` (enforced at startup).

This block replaces the deprecated `server.metrics_addr` key.

| Option                  | Type   | Default            | Description                                                          |
|-------------------------|--------|--------------------|----------------------------------------------------------------------|
| addr                    | string | "127.0.0.1:9090"   | Address the ops HTTP listener binds.                                 |
| auth.type               | string | "none"             | Ops-listener auth: one of "none", "basic", "mtls".                   |
| auth.users              | array  | []                 | Basic-auth users (same shape as top-level `auth.users`) when `auth.type: basic`. |
| tls.cert\_file          | string | ""                 | Server cert for the ops listener. Empty → plain HTTP.                |
| tls.key\_file           | string | ""                 | Server key for the ops listener.                                     |
| tls.client\_ca\_file    | string | ""                 | CA bundle to verify operator client certs when `auth.type: mtls`.    |

```yaml
server:
  ops:
    addr: 127.0.0.1:9090
    auth:
      type: basic
      users:
        - username: ops
          password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # output of: gmountie genpass
    tls:
      cert_file: /etc/gmountie/ops.crt
      key_file: /etc/gmountie/ops.key
```

## Logging

The `log` block controls log output.

| Option | Type   | Description                                                                       |
|--------|--------|-----------------------------------------------------------------------------------|
| format | string | Encoder: `console` or `json`. Empty → `console` on a TTY, `json` otherwise.        |
| level  | string | Minimum level: `debug`, `info`, `warn`, or `error`. Empty → `info`.               |

```yaml
log:
  format: json
  level: info
```

## Authentication Options

The `auth` section configures user authentication:

| Option            | Type     | Required | Default | Description                                                              |
|-------------------|----------|----------|---------|--------------------------------------------------------------------------|
| type              | string   | yes      | —       | Authentication type: `basic` or `mtls`.                                  |
| users             | array    | yes      | —       | List of users (credentials + per-user volume allowlist).                |
| default\_allow    | boolean  | no       | true    | Access policy for principals with no explicit `volumes` list. `false` = fail-closed. |
| revoked\_serials  | array    | no       | []      | Cert-serial blocklist (mTLS). Any hex format; normalized at load. Read at startup and on reload. |

Authentication is required; every server must configure at least one user.

Each entry in `users` supports:

| Field            | Type     | Description                                                                                       |
|------------------|----------|---------------------------------------------------------------------------------------------------|
| username         | string   | Principal name. For `basic` this is the login; for `mtls` it must match the cert CN (or first DNS SAN). |
| password\_hash   | string   | argon2id PHC string. Required for `basic`; must be empty/omitted for `mtls`.                      |
| volumes          | array    | Explicit list of volume names this user may access. Unset → use `default_allow`; explicit `[]` → no access. |

### Basic Authentication

Enables username/password authentication. The `password_hash` field must be an argon2id PHC string — the server rejects any value that doesn't start with `$argon2id$` and points you at `gmountie genpass`. Generate a hash with:

```bash
gmountie genpass
# Password: (enter password, no echo)
# $argon2id$v=19$... (copy this into password_hash)
```

```yaml
auth:
  type: basic
  users:
    - username: admin
      password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # output of: gmountie genpass
    - username: user1
      password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # output of: gmountie genpass
      volumes: [shared]   # user1 may only access the "shared" volume
```

### mTLS Authentication

With `auth.type: mtls`, the **verified client certificate** is the identity —
the server extracts the principal from the cert's CN (or first DNS SAN when CN
is empty). Enabling mTLS requires `server.tls.client_ca_file` to be set so the
server can verify presented client certs. mTLS users carry only `username` (to
match the cert) and an optional `volumes` allowlist; `password_hash` must be
empty.

```yaml
auth:
  type: mtls
  default_allow: false
  revoked_serials:
    - ab:cd:ef
  users:
    - username: alice            # must match the client cert CN / first DNS SAN
      volumes: [shared, media]
    - username: backup
      volumes: [backup]
```

## Volume Configuration

The `volumes` section defines shared directories. Each volume has a name, a path on the server, and (optionally) an identity-`mapping` block.

| Option    | Type    | Required | Description                                                          |
|-----------|---------|----------|----------------------------------------------------------------------|
| `name`    | string  | yes      | Unique volume identifier (clients reference this).                   |
| `path`    | string  | yes      | Absolute path on the server to the shared directory.                 |
| `mapping` | object  | no       | How the authenticated principal maps to a server-side identity. Defaults to `squash`. See below. |

Example with multiple volumes:

```yaml
volumes:
  - name: documents
    path: /srv/documents
  - name: media
    path: /srv/media
  - name: backup
    path: /srv/backup
```

The server validates every `path` at startup: if a volume's `path` does not exist or is not a directory, `gmountie serve` fails immediately with an error naming the offending volume, instead of surfacing a cryptic failure at the first file access.

### Identity mapping

Each volume picks **one** of four modes for `mapping.mode`. The mode decides which uid/gid the server uses when handling RPCs for that volume — i.e. the server-side identity that file-permission checks run against. See **[Concepts → Identity & ownership](../concepts/identity.mdx)** for the model.

| `mode`        | Extra fields                          | Behaviour                                                                                       |
|---------------|---------------------------------------|-------------------------------------------------------------------------------------------------|
| `squash`      | `uid`, `gid`                          | Every authenticated principal becomes one fixed `(uid, gid)`. Default if `mapping` is omitted.   |
| `static`      | `users{}`, `groups{}`                 | Lookup table: `username → {uid, gid, groups[]}` and `groupname → gid`.                          |
| `system`      | _(none)_                              | Resolve the principal against the server's NSS (`/etc/passwd`, `/etc/group`, LDAP/SSSD…).        |
| `passthrough` | `root_squash`, `anon_uid`             | Use the uid/gid the client claims. `root_squash` (default `true`) remaps client root to `anon_uid`. |

#### Examples

```yaml title="squash — one identity for everyone"
volumes:
  - name: shared
    path: /srv/shared
    mapping:
      mode: squash
      uid: 1000
      gid: 1000
```

```yaml title="static — declared table of users"
volumes:
  - name: shared
    path: /srv/shared
    mapping:
      mode: static
      users:
        alice:
          uid: 1001
          gid: 1001
          groups: [editors]
        bob:
          uid: 1002
          gid: 1002
          groups: [editors, ops]
      groups:
        editors: 2000
        ops:     2001
```

```yaml title="system — resolve against the server's user database"
volumes:
  - name: home
    path: /home
    mapping:
      mode: system
```

```yaml title="passthrough — trust the client's uid/gid"
volumes:
  - name: backup
    path: /srv/backup
    mapping:
      mode: passthrough
      root_squash: true
      anon_uid: 65534
```

If you don't include `mapping`, the volume gets `mode: squash` with `uid: 0` / `gid: 0` — pick something else unless you mean it.

#### Admin capabilities (`dac_read_search`, `dac_override`)

A principal can be granted one of two POSIX capabilities so service accounts can list/read or modify everything on a volume regardless of file ownership. Enforcement is kernel-native via per-thread `capset`. Both require the **server to run as root**.

| Capability         | Effect                                                                                          |
|--------------------|-------------------------------------------------------------------------------------------------|
| `dac_read_search`  | Read and traverse any path on the volume regardless of mode bits. **Cannot write**.             |
| `dac_override`     | Read, traverse, **and modify** any path. New entries created during the op are `fchown`ed to the principal so admin writes don't silently leave root-owned files. |

In `static` mode you grant caps per user:

```yaml title="static — per-user admin caps"
volumes:
  - name: shared
    path: /srv/shared
    mapping:
      mode: static
      users:
        alice:
          uid: 1001
          gid: 1001
        backup:
          uid: 1500
          gid: 1500
          caps: [dac_read_search]   # backup can read everything, can't modify
        ops-admin:
          uid: 1501
          gid: 1501
          caps: [dac_override]      # break-glass write access
```

In `system` mode you grant caps to **server-side group memberships**, so adding a user to a real OS group (e.g. `wheel`, `backup`) is enough:

```yaml title="system — admin caps via server group membership"
volumes:
  - name: team
    path: /srv/team
    mapping:
      mode: system
      admin_groups:
        dac_override:    [wheel]
        dac_read_search: [backup]
```

`squash` and `passthrough` ignore caps. See **[Concepts → Identity & ownership](../concepts/identity.mdx#admin-capabilities)** for the threat model.

## Example Configuration

Here's an example configuration file that enables basic authentication and
exposes two volumes:

```yaml
server:
  address: 0.0.0.0
  port: 9449
  metrics: true
auth:
  type: basic
  users:
    - username: admin
      password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # output of: gmountie genpass
volumes:
  - name: shared
    path: /srv/shared
  - name: private
    path: /srv/private
```

On **first run** (`gmountie serve` with no `-c`), gMountie auto-generates this config at `~/.config/gmountie/server.yaml` with a randomly generated admin password (printed once to the console) and a `shared` volume at `$XDG_DATA_HOME/gmountie/shared`.

## See also

- [Server CLI](./cli.md) — `gmountie serve` invocation.
- [Wire protocol](../concepts/wire-protocol.mdx) — what the server speaks on the wire.
- [Sessions & reconnect](../concepts/sessions-and-reconnect.mdx) — how keepalive surfaces dead connections.
