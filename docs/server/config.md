---
title: Server configuration
sidebar_label: Configuration
description: Every server.yaml field — server, keepalive, auth, volumes — with types, defaults, and valid ranges.
---

# Server configuration

The server reads a YAML file with three sections: **`server`** (listen address and transport tuning), **`auth`** (credentials), and **`volumes`** (the directories you're sharing). If you don't pass `-c`, gMountie writes a default config to `~/.config/gmountie/server.yaml` on first run.

## Configuration File Structure

The configuration file has three main sections:

- Server configuration
- Authentication configuration
- Volumes configuration

Basic example:

```yaml
server:
  address: 0.0.0.0
  port: 9449
auth:
  type: basic
  users:
    - username: admin
      password: admin
volumes:
  - name: shared
    path: /shared
```

## Server Options

The `server` section configures the core server settings:

| Option              | Type     | Default      | Description                                                |
|---------------------|----------|--------------|------------------------------------------------------------|
| address             | string   | "0\.0\.0\.0" | IP address the server listens on                           |
| port                | integer  | 9449         | Port number for the gRPC server                            |
| metrics             | boolean  | true         | Enable/disable Prometheus metrics                          |
| metrics\_addr       | string   | ":9090"      | Address the ops HTTP server listens on                     |
| max\_message\_bytes | integer  | 16777216     | Cap on inbound/outbound gRPC message size (16 MiB default) |

`max_message_bytes` is validated to the range [65536, 67108864] (64 KiB to
64 MiB). The default sits well above the streaming `frame_size_bytes` so a
single Read/Write frame plus header overhead always fits.

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

## Authentication Options

The `auth` section configures user authentication:

| Option | Type   | Required | Description                   |
|--------|--------|----------|-------------------------------|
| type   | string | yes      | Authentication type ("basic") |
| users  | array  | yes      | List of user credentials      |

Authentication is required; every server must configure at least one user.

### Basic Authentication

Enables username/password authentication:

```yaml
auth:
  type: basic
  users:
    - username: admin
      password: admin
    - username: user1
      password: pass123
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
      password: admin
volumes:
  - name: shared
    path: /shared
  - name: private
    path: /private
```

## See also

- [Server CLI](./cli.md) — `gmountie serve` invocation.
- [Wire protocol](../concepts/wire-protocol.mdx) — what the server speaks on the wire.
- [Sessions & reconnect](../concepts/sessions-and-reconnect.mdx) — how keepalive surfaces dead connections.
