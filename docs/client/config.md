# Client Configuration

The gMountie client configuration file uses YAML format and supports various
options for connecting to servers, authentication, and mounting volumes.

## Configuration File Structure

The configuration file has three main sections:

- Server connection settings
- Authentication configuration
- Mount configuration

Basic example:

```yaml
server:
  address: 127.0.0.1
  port: 9449
  tls: false
auth:
  type: basic
  username: admin
  password: admin
mount:
  type: single
  volume: shared
  path: /mnt/shared
```

## Server Options

The `server` section configures the connection to the gMountie server:

| Option  | Type    | Default   | Description                   |
|---------|---------|-----------|-------------------------------|
| address | string  | 127.0.0.1 | Server IP address or hostname |
| port    | integer | 9449      | Server port number            |
| tls     | boolean | false     | Enable/disable TLS encryption |

Example:

```yaml
server:
  address: 192.168.1.100  # Remote server address
  port: 8080              # Custom port
  tls: true               # Enable TLS
```

## RPC Options

The `rpc` section configures per-RPC timeouts, message-size caps, and
HTTP/2 keepalive params on the client side. Match the server's keepalive
defaults so dead-connection detection is symmetric in both directions.

| Option                  | Type     | Default  | Description                                                 |
|-------------------------|----------|----------|-------------------------------------------------------------|
| timeout\_meta           | duration | 5s       | Per-RPC timeout for metadata ops                            |
| timeout\_io             | duration | 30s      | Per-RPC timeout for data ops (Read, Write, ...)             |
| readahead\_chunk\_bytes | integer  | 65536    | Size of a single readahead fetch (0 disables readahead)     |
| readahead\_threshold    | integer  | 3        | Sequential reads required before a prefetch is armed        |
| write\_coalesce\_bytes  | integer  | 1048576  | Per-fd small-write coalescing threshold (0 disables)        |
| max\_message\_bytes     | integer  | 16777216 | Cap on inbound/outbound gRPC message size (16 MiB default)  |

`max_message_bytes` is validated to the range [65536, 67108864] (64 KiB to
64 MiB) and should typically mirror the server's value.

`readahead_threshold` is validated to the range [1, 16]; smaller values
arm prefetch sooner (more aggressive, more wasted fetches on
random-access workloads), larger values delay arming. Setting
`readahead_chunk_bytes: 0` disables the readahead path entirely,
regardless of threshold.

`write_coalesce_bytes` is validated to the range [0, 16777216] (0 to
16 MiB). 0 disables coalescing entirely so every Write call hits the
network; the default 1 MiB matches the streaming-frame size and absorbs
the common "many tiny appends" pattern (logs, build outputs, etc.).

### Readahead

When sequential reads are detected on an fd, the client prefetches a
single `readahead_chunk_bytes`-sized chunk one chunk ahead of the
current offset. The next Read that lines up with the prefetched range
is served from the in-memory ring without touching the network. There
is at most one outstanding prefetch per fd; the ring is dropped on any
non-sequential Read (backwards seek or gap), and the in-flight prefetch
is cancelled when the fd is released.

The win shows up most clearly on high-RTT connections where each
round-trip costs. Localhost is roughly neutral.

### Write Coalescing

Per-fd, small contiguous writes accumulate in an in-memory buffer up to
`write_coalesce_bytes`. The buffer flushes on three conditions:

- the buffer reaches the threshold,
- the next Write lands at a non-contiguous offset (the prior buffer is
  flushed; the new write seeds a fresh buffer at its offset), or
- the application calls Flush, Fsync, or closes the fd (Release).

Writes equal to or larger than the threshold bypass the buffer entirely:
the pending buffer (if any) is flushed first, then the big write goes
through. This preserves on-disk byte order.

Coalescing returns from Write *optimistically* — FUSE's write-then-Flush
durability model means applications that need a write observed by another
reader must Flush (or close). A failed buffered write surfaces on the
next Flush/Fsync as `EIO`; Release swallows the error and logs it
(symmetric with how Release already handles RPC failures).

Like readahead, the win shows up most clearly on high-RTT connections;
localhost is roughly neutral. Workloads that already write in large
chunks (>= the threshold) are unaffected.

### Keepalive

The `rpc.keepalive` block tunes gRPC HTTP/2 keepalive pings on the client.
Defaults ping every 30s and time out after 10s, surfacing a dead server as
an `Unavailable` error within ~40s instead of waiting on TCP timeouts.

| Option                  | Type     | Default | Description                                           |
|-------------------------|----------|---------|-------------------------------------------------------|
| time                    | duration | 30s     | Interval between pings to an idle connection          |
| timeout                 | duration | 10s     | Wait time for a ping ACK before closing the conn      |
| permit\_without\_stream | boolean  | true    | Allow pings when no streams are in flight             |

Example:

```yaml
rpc:
  timeout_meta: 5s
  timeout_io: 30s
  readahead_chunk_bytes: 131072  # 128 KiB
  readahead_threshold: 3
  write_coalesce_bytes: 1048576  # 1 MiB
  max_message_bytes: 33554432  # 32 MiB
  keepalive:
    time: 15s
    timeout: 5s
    permit_without_stream: true
```

## FUSE Options

The `fuse` section tunes the FUSE-kernel-side mount knobs. The defaults
match a 1 MiB streaming-frame profile; raise `max_write_bytes` if the
server's `frame_size_bytes` is larger.

| Option              | Type    | Default  | Description                                                      |
|---------------------|---------|----------|------------------------------------------------------------------|
| max\_write\_bytes   | integer | 1048576  | Ceiling for FUSE WRITE/READ size in bytes (1 MiB default)        |
| max\_background     | integer | 64       | Max async background requests the kernel may have in flight      |
| writeback\_cache    | boolean | false    | Enable the kernel's writeback page cache for the mount           |

`max_write_bytes` is validated to the range [4096, 16777216] (4 KiB to
16 MiB). go-fuse sets the kernel's `max_read` equal to `MaxWrite`, so
this single knob drives both directions of FUSE-kernel transfer size.

`max_background` is validated to the range [1, 1024]; the upper bound is
a sanity ceiling, not a tuned value.

At mount time the client calls the server's `Version` RPC and caps
`max_write_bytes` at the server's advertised `frame_size_bytes` so the
kernel never asks for a frame the server would split anyway. A failed
or unavailable Version call falls back to the configured value — the
mount is not gated on the negotiation.

`writeback_cache` defaults to off; the client read/write path is still
synchronous pending the cache layer. Toggling it on enables the FUSE
`CAP_WRITEBACK_CACHE` capability bit.

Example:

```yaml
fuse:
  max_write_bytes: 2097152  # 2 MiB
  max_background: 128
  writeback_cache: false
```

## Cache Options

The `cache` section configures the optional client-side in-memory cache
layer that decorates the gRPC backend. When enabled, the cache holds
recent attribute lookups, directory listings, and file-data chunks in
process memory; on a hit the FUSE op short-circuits without crossing
the wire.

Disabled by default. Sub-spec B is the in-memory layer only; Sub-spec C
will add persistence and flip the default once the disk side is proven.

| Key                     | Type     | Default  | Description                                                            |
|-------------------------|----------|----------|------------------------------------------------------------------------|
| enabled                 | boolean  | false    | Enable the client-side in-memory cache decorator                       |
| max\_size\_bytes        | integer  | 1073741824 (1 GiB) | Total byte budget across the attr+dir+data sub-caches        |
| chunk\_size\_bytes      | integer  | 1048576 (1 MiB)    | Granularity of the data cache; reads chunk-align against this |
| attr\_ttl               | duration | 5s       | TTL for positive attribute cache entries                               |
| dir\_ttl                | duration | 5s       | TTL for directory listing cache entries                                |
| negative\_ttl           | duration | 2s       | TTL for negative attribute cache entries (ENOENT lookups)              |

`max_size_bytes` is validated to the range [0, 68719476736] (0 to
64 GiB). 0 disables the byte budget — entries still age out on TTL but
nothing is force-evicted on size pressure.

`chunk_size_bytes` is validated to the range [4096, 16777216] (4 KiB
to 16 MiB). The data cache stores fixed-size chunks; a 1 MiB read at a
non-aligned offset spans two chunks. The default mirrors the streaming
frame size so chunk fetches map 1:1 to a single Read RPC.

The three TTLs control coherence vs. RPC traffic. Short TTLs (the
defaults) make file-system changes made by other clients visible
quickly, at the cost of more frequent revalidation. Longer TTLs are
safe only when this client is the sole writer.

Example:

```yaml
cache:
  enabled: true
  max_size_bytes: 268435456  # 256 MiB
  chunk_size_bytes: 1048576  # 1 MiB
  attr_ttl: 5s
  dir_ttl: 5s
  negative_ttl: 2s
```

## Authentication Options

The `auth` section configures client authentication:

| Option   | Type   | Required        | Description                             |
|----------|--------|-----------------|-----------------------------------------|
| type     | string | yes             | Authentication type ("none" or "basic") |
| username | string | yes (for basic) | Username for basic auth                 |
| password | string | yes (for basic) | Password for basic auth                 |

### None Authentication

Disables authentication (not recommended for production):

```yaml
auth:
  type: none
```

### Basic Authentication

Enables username/password authentication:

```yaml
auth:
  type: basic
  username: admin
  password: admin
```

## Mount Configuration

The `mount` section defines how volumes are mounted. There are two mount types:

1. Single volume mount
2. VFS (Virtual File System) mount

### Single Volume Mount

Mounts a single volume at a specified path:

| Option | Type   | Required | Description            |
|--------|--------|----------|------------------------|
| type   | string | yes      | Must be "single"       |
| volume | string | yes      | Volume name to mount   |
| path   | string | yes      | Local mount point path |

Example:

```yaml
mount:
  type: single
  volume: documents
  path: /home/user/documents
```

### VFS Mount

Mounts multiple volumes under a single mount point:

| Option    | Type     | Required | Description                       |
|-----------|----------|----------|-----------------------------------|
| type      | string   | yes      | Must be "vfs"                     |
| path      | string   | yes      | Base mount point path             |
| mount_all | boolean  | no       | Mount all available volumes       |
| volumes   | []string | no       | List of specific volumes to mount |

Example:

```yaml
mount:
  type: vfs
  path: /mnt/gmountie
  mount_all: false
  volumes:
  - documents
  - media
  - backup
```

## Complete Configuration Examples

### Single Volume Mount Example

```yaml
server:
  address: 192.168.1.100
  port: 9449
  tls: false
auth:
  type: basic
  username: admin
  password: admin
mount:
  type: single
  volume: documents
  path: /home/user/documents
```

### VFS Mount Example

```yaml
server:
  address: 192.168.1.100
  port: 9449
  tls: false
auth:
  type: basic
  username: admin
  password: admin
mount:
  type: vfs
  path: /mnt/gmountie
  mount_all: false
  volumes:
    - documents
    - media
    - backup
```

