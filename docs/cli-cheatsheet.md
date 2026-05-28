---
id: cli-cheatsheet
title: CLI cheatsheet
sidebar_label: CLI cheatsheet
description: Every gMountie command and flag on one page. Copy-paste reference for serving, mounting, and unmounting.
---

# CLI cheatsheet

Every command, every flag, one page. The binary is `gmountie` — one binary, both roles.

## Commands

| Command            | What it does                                                            |
| ------------------ | ----------------------------------------------------------------------- |
| `gmountie serve`   | Start the server. Exposes the volumes in your config over a gRPC stream. |
| `gmountie mount`   | Mount a server's volume locally via FUSE.                                |
| `gmountie version` | Print the build version and exit.                                        |

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
gmountie mount [<mountpoint>] [flags]
```

CLI flags override the corresponding fields in the client config. With `-c`, anything not set on the CLI falls back to the config.

| Flag                | Short | Default          | Meaning                                                                       |
| ------------------- | ----- | ---------------- | ----------------------------------------------------------------------------- |
| `--server`          | `-s`  | `127.0.0.1:9449` | Server `host:port`.                                                           |
| `--volume`          | `-n`  | _(required)_     | Volume name to mount.                                                          |
| `--auth-type`       | `-t`  | `basic`          | Authentication scheme. Only `basic` is supported today.                       |
| `--username`        | `-u`  | _(required)_     | Username for basic auth.                                                       |
| `--password`        | `-p`  | _(required)_     | Password for basic auth.                                                       |
| `--raw-ids`         |       | `false`          | Expose the server's raw uid/gid on file metadata, instead of mapping to the local user. Useful for backups and admin tooling. |

Mountpoint can also come from `mount.path` in the client config; either works.

See **[Client configuration](./client/config.md)** for every YAML field including RPC tuning, FUSE knobs, and the optional client-side cache.

## Common recipes

```bash
# Mount with everything on the CLI
gmountie mount /mnt/shared \
  --server example.com:9449 \
  --auth-type basic --username admin --password '<password>' \
  --volume shared

# Mount from a config file (so the password isn't in shell history)
gmountie mount -c ~/.config/gmountie/client.yaml

# Verbose logging for troubleshooting
gmountie mount -v -c client.yaml

# Server with a custom config
gmountie serve -c /etc/gmountie/server.yaml

# Unmount
umount /mnt/shared          # from another shell
# …or press Ctrl-C in the terminal where gmountie mount is running.
```

## Environment variables

| Variable               | Effect                                                                                  |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `GMOUNTIE_PPROF_ADDR`  | If set (e.g. `127.0.0.1:6060`), the client serves `/debug/pprof/` on that address.       |

Most settings are configured via YAML, not the environment. The pprof toggle is the exception — it's a diagnostic hook, not a runtime feature.

## See also

- [Server configuration](./server/config.md) · [Client configuration](./client/config.md)
- [Troubleshooting](./troubleshooting.mdx)
