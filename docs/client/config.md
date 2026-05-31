---
title: Client configuration
sidebar_label: Configuration
description: Every client.yaml field — server, RPC tuning, FUSE knobs, optional cache, auth, mount type — with types, defaults, and valid ranges.
---

# Client configuration

The client reads a YAML file with up to six sections: **`server`** (where to connect), **`rpc`** (per-RPC timeouts, message size, [readahead](#readahead), [write coalescing](#write-coalescing), keepalive), **`fuse`** (kernel-side mount knobs), **`cache`** (optional client-side cache), **`auth`** (credentials), and **`mount`** (volume and path). CLI flags override the corresponding fields — see **[Client CLI](./cli.md)** for the override list.

## Configuration File Structure

The configuration file has three main sections:

- Server connection settings
- Authentication configuration
- Mount configuration

Basic example:

```yaml
server:
  address: your-server.example
  port: 9449
  tls:
    verify: tofu     # pin server cert fingerprint on first connect (recommended for self-signed certs)
auth:
  type: basic
  username: admin
  password: ""       # leave empty to be prompted; or set $GMOUNTIE_AUTH_PASSWORD
mount:
  type: single
  volume: shared
  path: /mnt/shared
```

## Server Options

The `server` section configures the connection to the gMountie server:

| Option           | Type    | Default   | Description                                              |
|------------------|---------|-----------|----------------------------------------------------------|
| address          | string  | 127.0.0.1 | Server IP address or hostname                            |
| port             | integer | 9449      | Server port number                                       |
| tls.verify       | string  | `"verify"` | Verification mode: `verify` \| `tofu` \| `insecure`    |
| tls.ca_file      | string  | _(system roots)_ | Path to a CA cert to validate the server against  |
| tls.expected_fingerprint | string | _(none)_ | Static SHA-256 pin (`SHA256:…`); verified on top of chain check in `verify` mode, or replaces TOFU pin in `tofu` mode |
| tls.server_name  | string  | _(from endpoint)_ | Override the TLS server name (SNI)               |
| tls.cert_file    | string  | _(none)_  | mTLS client cert                                         |
| tls.key_file     | string  | _(none)_  | mTLS client key                                          |

**Verification modes:**
- `verify` (default) — full chain validation against `tls.ca_file` or system roots; add `tls.expected_fingerprint` to also pin the leaf cert.
- `tofu` — trust on first use; pins the server's leaf-cert SHA-256 to `$XDG_STATE_HOME/gmountie/known_hosts` on first connect, and refuses on any later mismatch.
- `insecure` — skip verification; for local development only.

Example connecting to a server with an auto-generated self-signed cert:

```yaml
server:
  address: your-server.example
  port: 9449
  tls:
    verify: tofu
```

Example with a static fingerprint pin (get the value by running `gmountie fingerprint` on the server):

```yaml
server:
  address: your-server.example
  port: 9449
  tls:
    verify: tofu
    expected_fingerprint: SHA256:AAAA...  # output of: gmountie fingerprint
```

## RPC Options

The `rpc` section configures per-RPC timeouts, message-size caps, and
HTTP/2 keepalive params on the client side. Match the server's keepalive
defaults so dead-connection detection is symmetric in both directions.

| Option                  | Type     | Default  | Description                                                 |
|-------------------------|----------|----------|-------------------------------------------------------------|
| timeout\_meta           | duration | 5s       | Per-RPC timeout for metadata ops                            |
| timeout\_io             | duration | 30s      | Per-RPC timeout for data ops (Read, Write, ...)             |
| readahead\_chunk\_bytes | integer  | 1048576  | Size of a single readahead fetch / prefetch chunk (0 disables readahead) |
| readahead\_threshold    | integer  | 3        | Sequential reads required before a prefetch is armed        |
| readahead\_window       | integer  | 4        | Prefetch chunks kept in flight ahead of the cursor (range 1–64) |
| write\_coalesce\_bytes  | integer  | 1048576  | Per-fd small-write coalescing threshold (0 disables)        |
| max\_message\_bytes     | integer  | 16777216 | Cap on inbound/outbound gRPC message size (16 MiB default)  |

`max_message_bytes` is validated to the range [65536, 67108864] (64 KiB to
64 MiB) and should typically mirror the server's value.

`readahead_threshold` is validated to the range [1, 16]; smaller values
arm prefetch sooner (more aggressive, more wasted fetches on
random-access workloads), larger values delay arming. `readahead_window`
is validated to [1, 64] and sets how many `readahead_chunk_bytes` chunks
the client keeps in flight ahead of the cursor — raise it on
high-RTT/high-bandwidth links to keep the read pipe full. Setting
`readahead_chunk_bytes: 0` disables the readahead path entirely,
regardless of threshold or window.

`write_coalesce_bytes` is validated to the range [0, 16777216] (0 to
16 MiB). 0 disables coalescing entirely so every Write call hits the
network; the default 1 MiB matches the streaming-frame size and absorbs
the common "many tiny appends" pattern (logs, build outputs, etc.).

### Readahead

When sequential reads are detected on an fd (after `readahead_threshold`
in-order reads), the client keeps up to `readahead_window`
`readahead_chunk_bytes`-sized chunks in flight ahead of the cursor, each
its own streaming Read RPC. gRPC multiplexes them over the one
connection, so a deep window hides the per-fetch round-trip latency and
keeps the read pipe full. Reads are served from the in-flight chunks
without touching the network: a read of any size is satisfied by copying
across one or more contiguous ready chunks, and a partially-read chunk
is retained so the next sequential read continues from its tail. A read
not yet fully prefetched falls through to a normal Read (never a short
read). A non-sequential Read (backwards seek or gap) evicts chunks
behind the new cursor and re-arms from there; in-flight prefetches are
cancelled when the fd is released.

The win shows up most clearly on high-RTT connections where each
round-trip costs — a deeper `readahead_window` scales read throughput
toward the link bandwidth until the pipe is full. Localhost is roughly
neutral; the default window of 4 is a starting point for ~50 ms links,
raise it for longer/fatter pipes.

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

| Option   | Type   | Required | Description                                        |
|----------|--------|----------|----------------------------------------------------|
| type     | string | yes      | Authentication type ("basic")                      |
| username | string | yes      | Username for basic auth                            |
| password | string | no       | Password for basic auth; if empty, falls back to `$GMOUNTIE_AUTH_PASSWORD` then an interactive prompt |

Authentication is required; every client must supply at least `username` and `type`. The password can be omitted from the config file and supplied at runtime.

### Basic Authentication

```yaml
auth:
  type: basic
  username: admin
  password: ""   # omit or leave empty to use $GMOUNTIE_AUTH_PASSWORD or an interactive prompt
```

## Mount Configuration

The `mount` section defines how a volume is mounted.

| Option | Type   | Required | Description            |
|--------|--------|----------|------------------------|
| type   | string | yes      | Must be `"single"`     |
| volume | string | yes      | Volume name to mount   |
| path   | string | yes      | Local mount point path |

Example:

```yaml
mount:
  type: single
  volume: documents
  path: /home/user/documents
```

## Complete Configuration Example

```yaml
server:
  address: your-server.example
  port: 9449
  tls:
    verify: tofu
    expected_fingerprint: SHA256:AAAA...  # output of: gmountie fingerprint
auth:
  type: basic
  username: admin
  # password: omitted — resolved from $GMOUNTIE_AUTH_PASSWORD or interactive prompt
mount:
  type: single
  volume: shared
  path: /home/user/shared
```

