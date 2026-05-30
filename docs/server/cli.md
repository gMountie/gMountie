---
title: Server CLI
sidebar_label: CLI
description: Flags for `gmountie serve` — the server entry point. Most settings live in the config file; the CLI keeps to file selection and verbosity.
---

# Server CLI

`gmountie serve` starts the server: it loads a YAML config, opens the configured volumes, and listens on the configured port for gRPC connections. Everything tunable lives in the **[server config file](./config.md)** — the CLI keeps a tight surface.

## Basic Usage

The basic syntax for starting the server is:

```bash
gmountie serve [flags]
```

## Command Flags

| Flag      | Short | Default                        | Description                |
|-----------|-------|--------------------------------|----------------------------|
| --config  | -c    | ~/.config/gmountie/server.yaml | Path to configuration file |
| --verbose | -v    | false                          | Enable verbose logging     |

## Configuration File

If no configuration file is specified, gMountie will:

1. Look for a config file at `~/.config/gmountie/server.yaml`
2. If not found, generate one at that location with the following defaults:
   - `address: 0.0.0.0`, `port: 9449`
   - one volume `shared` whose path is auto-created at `$XDG_DATA_HOME/gmountie/shared`
   - one user `admin` with a **randomly generated password**, printed once to the console and stored as an argon2id hash
3. Print the generated admin password **once** — save it, it will not be shown again

To rotate the password later, run `gmountie genpass`, copy the printed hash, and update `auth.users[0].password_hash` in the config.

An illustrative default config looks like:

```yaml
server:
  address: 0.0.0.0
  port: 9449
  metrics: true
auth:
  type: basic
  users:
    - username: admin
      password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # randomly generated; rotate with: gmountie genpass
volumes:
  - name: shared
    path: /home/user/.local/share/gmountie/shared
```

## Examples

1. Start server with default configuration:
   ```bash
   gmountie serve
   ```

2. Start server with custom config file:
   ```bash
   gmountie serve -c /path/to/config.yaml
   ```

3. Start server with verbose logging:
   ```bash
   gmountie serve -v
   ```

## Security Considerations

1. The server ships with TLS enabled by default — a self-signed cert is auto-generated on first run. Run `gmountie fingerprint` on the server to get the fingerprint for TOFU pinning on the client.
2. Server credentials use argon2id hashing — the config field is `password_hash`, not `password`. Use `gmountie genpass` to generate valid hashes.
3. The first-run default binds to `0.0.0.0`. This is intentional: the random generated password and auto-TLS make the zero-config default safe to expose. To restrict to a specific interface, set `server.address` in the config.

## See Also
- [Server Configuration](./config.md) - Detailed configuration file options
