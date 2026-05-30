---
title: Client CLI
sidebar_label: CLI
description: Flags for `gmountie mount` — the client entry point. CLI flags override the corresponding fields in client.yaml.
---

# Client CLI

`gmountie mount` connects to a server, opens a FUSE mount, and proxies every filesystem call to the server. CLI flags override the matching fields in **[client.yaml](./config.md)**, so you can keep the bulk of your settings in the config and override per-invocation. The mount call is identical on Linux and macOS; on macOS the client needs [macFUSE](https://macfuse.io) or [FUSE-T](https://www.fuse-t.org/) installed first.

## Basic Usage

The recommended shorthand form:

```bash
gmountie mount [user@]host[:port]/volume mountpoint
```

Or the explicit flag form (still supported):

```bash
gmountie mount mountpoint -s host:port -n volume -u username [flags]
```

Examples:

```bash
# Shorthand — prompts for password interactively (no echo)
gmountie mount admin@your-server.example:9449/shared ~/mnt/shared

# Default port 9449 can be omitted
gmountie mount admin@your-server.example/shared ~/mnt/shared

# Flag form
gmountie mount ~/mnt/shared -s your-server.example:9449 -n shared -u admin
```

## Command Flags

| Flag         | Short | Default        | Description                                                                            |
|--------------|-------|----------------|----------------------------------------------------------------------------------------|
| `--server`   | `-s`  | 127.0.0.1:9449 | Server address and port.                                                               |
| `--volume`   | `-n`  | _(required)_   | Volume name to mount.                                                                  |
| `--auth-type`| `-t`  | basic          | Authentication scheme. Only `basic` is supported today.                                |
| `--username` | `-u`  | _(required)_   | Username for basic auth.                                                                |
| `--password` | `-p`  | _(optional)_   | Password for basic auth (visible in shell history; prefer the prompt or `$GMOUNTIE_AUTH_PASSWORD`). |
| `--daemon`   |       | false          | Mount in the background; detaches after mount is ready. Logs go to `$XDG_STATE_HOME/gmountie/mount-daemon.log`. |
| `--raw-ids`  |       | false          | Expose the server's raw uid/gid on file metadata instead of mapping to the local user. |
| `--verbose`  | `-v`  | false          | Enable verbose (debug-level) logging.                                                   |
| `--config`   | `-c`  |                | Path to client.yaml. CLI flags override fields in this file.                            |

### Password resolution

The password is never required on the command line. Resolution order (first non-empty wins):

1. `--password` flag
2. `auth.password` in the client config file (if `-c` is used)
3. `$GMOUNTIE_AUTH_PASSWORD` environment variable
4. Interactive no-echo prompt on the terminal

### `--raw-ids`

On mount, the client calls a `WhoAmI` RPC and learns which `(uid, gid)` the server resolves you to — the result of running the volume's [identity mapping](../concepts/identity.mdx) (squash · static · system · passthrough). By default, the client uses that answer to **rewrite file metadata** in `ls -l`: anything the server reports as your resolved uid renders as your **local** invoking user. The listing reads naturally.

`--raw-ids` disables the rewriting. You see exactly the uid/gid the server reports. Useful when:

- you're backing up the volume and need server-side ownership preserved literally,
- you're debugging "why does this file claim to be owned by `nobody`?" — the raw uid tells you what the server thinks,
- you've intentionally aligned uids on both ends and want the local user to be irrelevant.

For day-to-day mounts, leave it off.

## Authentication Types

The client supports the following authentication method:

1. Basic (username/password) — password is resolved as described above; never required on the CLI.

## Examples

1. Mount with shorthand (interactive password prompt):
   ```bash
   gmountie mount admin@192.168.1.100:9449/shared /mnt/shared
   ```

2. Mount with shorthand using default port:
   ```bash
   gmountie mount admin@myserver.example/documents /home/user/docs
   ```

3. Mount in the background:
   ```bash
   gmountie mount admin@myserver.example:9449/shared /mnt/shared --daemon
   ```

4. Mount with verbose logging for troubleshooting:
   ```bash
   gmountie mount -v admin@server:9449/media /mnt/media
   ```

5. Mount from a config file (password from config or prompt):
   ```bash
   gmountie mount -c ~/.config/gmountie/client.yaml
   ```

## Security Considerations

1. **Password:** Never pass passwords on the command line in production — they show up in `ps` output and shell history. Use the interactive prompt, `$GMOUNTIE_AUTH_PASSWORD`, or a config file instead.

2. **TLS:** The connection is TLS-encrypted. For self-signed server certs (the default on first run), use TOFU mode so the client pins the server fingerprint on first connect. See **[Client configuration → TLS](./config.md#tls)** and `gmountie fingerprint`.

## Unmounting

To unmount a filesystem:

1. Use the standard system unmount command:
   ```bash
   umount /mountpoint
   ```

2. Or press Ctrl+C in the terminal where gMountie is running

## Common Issues

1. Permission Denied
    - Verify user has proper permissions
    - Check authentication credentials
    - Ensure mountpoint directory exists and is empty

2. Connection Failed
    - Verify server address and port
    - Check network connectivity
    - Confirm server is running

3. Mount Failed
    - Ensure FUSE is installed
    - Verify user has permission to mount
    - Check if mountpoint is already in use

## See Also

- [Client Configuration](./config.md) - Detailed configuration file options
- [Quickstart Guide](../quickstart.mdx) - Getting started with gMountie
