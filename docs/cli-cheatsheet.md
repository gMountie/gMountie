---
id: cli-cheatsheet
title: CLI cheatsheet
sidebar_label: CLI cheatsheet
description: Every gMountie command and flag on one page. Copy-paste reference for serving, mounting, and unmounting.
---

# CLI cheatsheet

Every command, every flag, one page. The binary is `gmountie` — one binary, both roles.

## Commands

| Command               | What it does                                                              |
| --------------------- | ------------------------------------------------------------------------- |
| `gmountie serve`      | Start the server. Exposes the volumes in your config over a gRPC stream.  |
| `gmountie mount`      | Mount a server's volume locally via FUSE.                                 |
| `gmountie unmount`    | Unmount a gMountie volume (cleanly stops a `--daemon` mount). Alias: `umount`. |
| `gmountie status`     | List active gMountie mounts on this machine (mountpoint, server, volume, pid, uptime). |
| `gmountie ls`         | List the volumes a server exposes (same auth as `mount`).                 |
| `gmountie config show`| Print a config file (server or client) verbatim, with secrets redacted.   |
| `gmountie genpass`    | Read a password (no-echo) and print the argon2id PHC hash to paste into the server config. |
| `gmountie fingerprint`| Print the server's TLS cert fingerprint for TOFU pinning.                |
| `gmountie completion` | Print a shell completion script (`bash`, `zsh`, `fish`, `powershell`).    |
| `gmountie version`    | Print the build version and exit.                                         |

## Global flags

These work on every subcommand:

| Flag             | Default                         | Meaning                                 |
| ---------------- | ------------------------------- | --------------------------------------- |
| `-c, --config`   | `~/.config/gmountie/<role>.yaml` | Path to the YAML config file.           |
| `-v, --verbose`  | `false`                         | Enable verbose (debug-level) logging.   |

If `-c` is omitted, gMountie writes a default config to the per-role path on first run.

## `gmountie serve`

```bash
gmountie serve [-c path/to/server.yaml] [-v]
```

The server has no command-specific flags beyond the globals — everything else lives in the config file. See **[Server configuration](./server/config.md)** for every option.

## `gmountie mount`

```bash
# Shorthand (recommended):
gmountie mount [user@]host[:port]/volume mountpoint

# Flag form:
gmountie mount mountpoint -s host:port -n volume -u username [flags]
```

CLI flags override the corresponding fields in the client config. With `-c`, anything not set on the CLI falls back to the config.

| Flag                | Short | Default          | Meaning                                                                       |
| ------------------- | ----- | ---------------- | ----------------------------------------------------------------------------- |
| `--server`          | `-s`  | `127.0.0.1:9449` | Server `host:port`.                                                           |
| `--volume`          | `-n`  | _(required)_     | Volume name to mount.                                                          |
| `--auth-type`       | `-t`  | `basic`          | Authentication scheme. Only `basic` is supported today.                       |
| `--username`        | `-u`  | _(required)_     | Username for basic auth.                                                       |
| `--password`        | `-p`  | _(optional)_     | Password for basic auth (visible in shell history; prefer the prompt or `$GMOUNTIE_AUTH_PASSWORD`). |
| `--daemon`          |       | `false`          | Detach after mount is ready. Logs go to `$XDG_STATE_HOME/gmountie/mount-daemon.log`. |
| `--raw-ids`         |       | `false`          | Expose the server's raw uid/gid on file metadata, instead of mapping to the local user. Useful for backups and admin tooling. |

**Password resolution order** (first non-empty wins): `--password` flag → config file (`auth.password`) → `$GMOUNTIE_AUTH_PASSWORD` → interactive no-echo prompt.

The mountpoint is **required** and must be given as a positional argument — there's no config fallback for it.

See **[Client configuration](./client/config.md)** for every YAML field including RPC tuning, FUSE knobs, and the optional client-side cache.

## `gmountie unmount`

```bash
gmountie unmount <mountpoint>     # alias: gmountie umount <mountpoint>
```

Unmounts a gMountie volume. For a mount this machine started — including
`--daemon` mounts — it signals the mount process to unmount cleanly (the same
path as Ctrl-C). For a mount started some other way it falls back to
`fusermount3 -u` / `umount`.

## `gmountie status`

```bash
gmountie status
```

Lists the volumes currently mounted by gMountie (foreground and `--daemon`) with
their mountpoint, server, volume, pid, and uptime. Entries whose process has
died are pruned automatically. Prints `No active gMountie mounts.` when there
are none.

## `gmountie ls`

```bash
gmountie ls [user@]host[:port]
gmountie ls -c client.yaml
```

Lists volumes the server exposes. Uses the same auth resolution as `mount`.

## `gmountie config show`

```bash
gmountie config show [--for server|client]
gmountie config show --effective [--for server|client]
```

By default, prints the chosen config file **verbatim**, with secrets (passwords
and inline private keys) redacted. It does not merge defaults — omitted fields
fall back to the documented defaults (see the client/server config docs).

Add `--effective` to instead print the **resolved** configuration — your file
merged with the built-in defaults and `GMOUNTIE_*` environment overrides — so
you can see every value gMountie will actually use. Secrets are still redacted.
With `--effective`, pass `--for server` to render a server config (it defaults
to client).

## `gmountie completion`

```bash
gmountie completion bash | zsh | fish | powershell
```

Generates a shell completion script for the given shell. Load it once to get
tab-completion for commands and flags, e.g.:

```bash
# bash (current shell)
source <(gmountie completion bash)
# zsh (persist)
gmountie completion zsh > "${fpath[1]}/_gmountie"
```

Run `gmountie completion --help` for per-shell install instructions.

## Common recipes

```bash
# Zero-config first run (random password printed once)
gmountie serve

# List what a server exposes
gmountie ls admin@example.com:9449

# Mount shorthand — prompts for password interactively
gmountie mount admin@example.com:9449/shared /mnt/shared

# Mount in the background (detach after mount is ready)
gmountie mount admin@example.com:9449/shared /mnt/shared --daemon

# Mount from a config file (password from config or prompt — not in shell history)
gmountie mount -c ~/.config/gmountie/client.yaml /mnt/shared

# Mount with password from environment (for scripts)
GMOUNTIE_AUTH_PASSWORD=secret gmountie mount admin@example.com:9449/shared /mnt/shared

# Verbose logging for troubleshooting
gmountie mount -v -c client.yaml /mnt/shared

# Server with a custom config
gmountie serve -c /etc/gmountie/server.yaml

# Generate a new password hash to paste into server config
gmountie genpass

# Print the server cert fingerprint for TOFU pinning on the client
gmountie fingerprint

# Show effective config (passwords redacted)
gmountie config show --for server

# See what's mounted
gmountie status

# Unmount (cleanly stops a --daemon mount; works for any gmountie mount)
gmountie unmount /mnt/shared
# …or press Ctrl-C in the terminal where a foreground gmountie mount is running,
# …or, for a mount started elsewhere: umount /mnt/shared
```

## Environment variables

| Variable                  | Effect                                                                                  |
| ------------------------- | --------------------------------------------------------------------------------------- |
| `GMOUNTIE_AUTH_PASSWORD`  | Password for basic auth — checked after `--password` and config file, before prompt.   |
| `GMOUNTIE_PPROF_ADDR`     | If set (e.g. `127.0.0.1:6060`), the client serves `/debug/pprof/` on that address.    |

Most settings are configured via YAML, not the environment. `GMOUNTIE_AUTH_PASSWORD` is useful in scripts where an interactive prompt is not available.

## See also

- [Server configuration](./server/config.md) · [Client configuration](./client/config.md)
- [Troubleshooting](./troubleshooting.mdx)
