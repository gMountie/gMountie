# Client CLI Options

The gMountie client provides the `mount` command for connecting to remote
filesystems
with various configuration options.

## Basic Usage

The basic syntax for mounting a remote filesystem is:

```bash
gmountie mount [flags] <mountpoint>
```

## Command Flags

| Flag        | Short | Default        | Description                     |
|-------------|-------|----------------|---------------------------------|
| --server    | -s    | 127.0.0.1:9449 | Server address and port         |
| --volume    | -n    |                | Volume name to mount (required) |
| --auth-type | -t    | basic          | Authentication type             |
| --username  | -u    |                | Username for basic auth         |
| --password  | -p    |                | Password for basic auth         |
| --verbose   | -v    | false          | Enable verbose logging          |

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
- [Quickstart Guide](quickstart.md) - Getting started with gMountie
