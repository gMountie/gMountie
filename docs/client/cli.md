---
title: Client CLI
sidebar_label: CLI
description: Flags for `gmountie mount` — the client entry point. CLI flags override the corresponding fields in client.yaml.
---

# Client CLI

`gmountie mount` connects to a server, opens a FUSE mount, and proxies every filesystem call to the server. CLI flags override the matching fields in **[client.yaml](./config.md)**, so you can keep the bulk of your settings in the config and override per-invocation.

## Basic Usage

The basic syntax for mounting a remote filesystem is:

```bash
gmountie mount [flags] <mountpoint>
```

## Command Flags

| Flag         | Short | Default        | Description                                                                            |
|--------------|-------|----------------|----------------------------------------------------------------------------------------|
| `--server`   | `-s`  | 127.0.0.1:9449 | Server address and port.                                                               |
| `--volume`   | `-n`  | _(required)_   | Volume name to mount.                                                                  |
| `--auth-type`| `-t`  | basic          | Authentication scheme. Only `basic` is supported today.                                |
| `--username` | `-u`  | _(required)_   | Username for basic auth.                                                                |
| `--password` | `-p`  | _(required)_   | Password for basic auth.                                                                |
| `--raw-ids`  |       | false          | Expose the server's raw uid/gid on file metadata instead of mapping to the local user. |
| `--verbose`  | `-v`  | false          | Enable verbose (debug-level) logging.                                                   |
| `--config`   | `-c`  |                | Path to client.yaml. CLI flags override fields in this file.                            |

### `--raw-ids`

On mount, the client calls a `WhoAmI` RPC and learns which `(uid, gid)` the server resolves you to — the result of running the volume's [identity mapping](../concepts/identity.mdx) (squash · static · system · passthrough). By default, the client uses that answer to **rewrite file metadata** in `ls -l`: anything the server reports as your resolved uid renders as your **local** invoking user. The listing reads naturally.

`--raw-ids` disables the rewriting. You see exactly the uid/gid the server reports. Useful when:

- you're backing up the volume and need server-side ownership preserved literally,
- you're debugging "why does this file claim to be owned by `nobody`?" — the raw uid tells you what the server thinks,
- you've intentionally aligned uids on both ends and want the local user to be irrelevant.

For day-to-day mounts, leave it off.

## Authentication Types

The client supports the following authentication method:

1. Basic (username/password)
   ```bash
   gmountie mount -s server:9449 -n volume -t basic -u user -p pass /mountpoint
   ```

## Examples

1. Mount with default settings (local server):
   ```bash
   gmountie mount -n shared /mnt/shared
   ```

2. Mount remote volume with basic auth:
   ```bash
   gmountie mount -s 192.168.1.100:9449 -n documents -t basic -u admin -p secret /home/user/docs
   ```

3. Mount with verbose logging:
   ```bash
   gmountie mount -v -s server:9449 -n media /mnt/media
   ```

## Security Considerations

1. Password Security
    - Avoid passing passwords on the command line in production
    - Use configuration files instead
    - Consider using environment variables

2. Network Security
    - Use TLS in production environments
    - Avoid mounting over untrusted networks
    - Consider using VPN for remote mounts

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

- [Client Configuration](client/config.md) - Detailed configuration file options
- [Quickstart Guide](quickstart.mdx) - Getting started with gMountie
