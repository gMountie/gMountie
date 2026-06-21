---
title: Client configuration
sidebar_label: Configuration
description: Every client.yaml field — server, RPC tuning (timeouts, retry window, readahead, write coalescing, keepalive), FUSE knobs, cache, auth, mount type — with types, defaults, and valid ranges.
---

# Client configuration

The client reads a YAML file with up to six sections: **`server`** (where to connect), **`rpc`** (per-RPC timeouts, retry window, message size, [readahead](#readahead), [write coalescing](#write-coalescing), keepalive), **`fuse`** (kernel-side mount knobs), **`cache`** (client-side cache), **`auth`** (credentials), and **`mount`** (volume and path). CLI flags override the corresponding fields — see **[Client CLI](./cli.md)** for the override list.

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
| address          | string  | _(required)_ | Server IP address or hostname                         |
| port             | integer | 9449      | Server port number                                       |
| tls.verify       | string  | `"verify"` | Verification mode: `verify` \| `tofu` \| `insecure`    |
| tls.ca_file      | string  | _(system roots)_ | Path to a CA cert to validate the server against  |
| tls.expected_fingerprint | string | _(none)_ | Static SHA-256 pin (`SHA256:…`); verified on top of chain check in `verify` mode, or replaces TOFU pin in `tofu` mode |
| tls.server_name  | string  | _(from endpoint)_ | Override the TLS server name (SNI)               |
| tls.cert_file    | string  | _(none)_  | mTLS client cert                                         |
| tls.key_file     | string  | _(none)_  | mTLS client key                                          |
| tls.ca_pem       | string  | _(none)_  | Inline CA cert PEM — alternative to `tls.ca_file`        |
| tls.cert_pem     | string  | _(none)_  | Inline mTLS client cert PEM — alternative to `tls.cert_file` |
| tls.key_pem      | string  | _(none)_  | Inline mTLS client key PEM — alternative to `tls.key_file`   |

**Inline PEM vs file paths:** each of CA / client cert / client key may be given
either as a file path (`*_file`) or inline as PEM (`*_pem`) — the latter is
handy for container-native injection (e.g. `GMOUNTIE_SERVER_TLS_CERT_PEM` env
var or a mounted Secret). Setting both the inline PEM and the file path for the
same item is an error; cert and key must be supplied together.

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

The `rpc` section configures per-RPC timeouts, the transient-retry
window, message-size caps, and HTTP/2 keepalive params on the client
side. Match the server's keepalive defaults so dead-connection detection
is symmetric in both directions.

`timeout_meta` and `timeout_io` bound a **single attempt** — the time
budget for one try at the underlying gRPC call. `retry_window` bounds the
**whole operation** across retries: as long as time remains in the window,
a transient `Unavailable` / `DeadlineExceeded` retries with a fresh
per-attempt deadline and exponential backoff (100 ms → 1 s).

| Option                  | Type     | Default  | Description                                                 |
|-------------------------|----------|----------|-------------------------------------------------------------|
| timeout\_meta           | duration | 10s      | Deadline for a single metadata-op attempt (Lookup, GetAttr, Readdir, …). The one-time pre-session connect/resolve uses at least 30s regardless, so a cold mTLS dial on a slow link isn't bounded by this. |
| timeout\_io             | duration | 30s      | Deadline for a single data-op attempt (Read, Write, …)      |
| retry\_window           | duration | 60s      | Wall-clock budget for retrying one FS op through transient failures. `0` = fail-fast (single attempt, no retry). Set high for hard-mount-style behaviour. |
| readahead\_chunk\_bytes | integer  | 1048576  | Size of a single readahead fetch / prefetch chunk (0 disables readahead) |
| readahead\_threshold    | integer  | 3        | Sequential reads required before a prefetch is armed        |
| readahead\_window       | integer  | 16       | Prefetch chunks kept in flight ahead of the cursor (range 1–64) |
| initial\_conn\_window\_bytes   | integer  | 16777216 | Pin gRPC HTTP/2 connection flow-control window; `0` (with stream=0) keeps autotuning (range 0–1 GiB) |
| initial\_stream\_window\_bytes | integer  | 8388608  | Pin gRPC HTTP/2 per-stream flow-control window; set together with the conn window (range 0–1 GiB) |
| connections             | integer  | 4        | Number of gRPC connections opened per mount (range 1–16). Each is a separate TCP flow; Read/Write streams spread across them (least-in-flight, sequential streams stay on the primary connection) so throughput can exceed a single flow's ceiling on high-BDP links (1 Gbit WAN, high-RTT). Metadata RPCs and the session keepalive/Subscribe streams use the primary connection. Set to `1` for single-connection behaviour. |
| write\_coalesce\_bytes  | integer  | 1048576  | Per-fd small-write coalescing threshold (0 disables)        |
| max\_message\_bytes     | integer  | 16777216 | Cap on inbound/outbound gRPC message size (16 MiB default)  |
| compression             | string   | none     | gRPC compressor for every RPC on the connection: `none` \| `snappy` |

`max_message_bytes` is validated to the range [65536, 67108864] (64 KiB to
64 MiB) and should typically mirror the server's value.

`compression` is off by default: on a fast link the compressor itself
becomes the bottleneck, so Snappy is opt-in. Set `compression: snappy`
when the network — not CPU — is the constraint (slow WAN links,
text-heavy payloads); see [Performance § 2.7](../design/performance.md)
for the measurements behind the default.

`retry_window` is validated to `gte=0` (any non-negative duration). The
default 60 s is aligned with `server.session.grace_period` (also 60 s) so
a transient disconnect can be transparently resumed within the whole retry
window. To match the pre-retry behaviour (one attempt, fail fast) set
`retry_window: 0`.

`readahead_threshold` is validated to the range [1, 16]; smaller values
arm prefetch sooner (more aggressive, more wasted fetches on
random-access workloads), larger values delay arming. `readahead_window`
is validated to [1, 64] and sets how many `readahead_chunk_bytes` chunks
the client keeps in flight ahead of the cursor — raise it on
high-RTT/high-bandwidth links to keep the read pipe full. Setting
`readahead_chunk_bytes: 0` disables the readahead path entirely,
regardless of threshold or window.

`initial_conn_window_bytes` and `initial_stream_window_bytes` pin the
gRPC HTTP/2 flow-control receive windows — the connection-level window
(default 16 MiB) and the per-stream window (default 8 MiB). gRPC's BDP
autotuner can hold these tighter than a high-bandwidth-delay link needs,
capping a single connection below the wire; pinning generous windows
(≥ the worst-case bandwidth-delay product) lets one connection fill the
pipe. Both are validated to the range [0, 1073741824] (0 to 1 GiB).
Autotuning is restored only when **both** are 0 — setting either one
nonzero disables autotuning globally and leaves the other dimension at
gRPC's small default, so set the two together or leave both at 0. Values
below the 64 KiB HTTP/2 floor are silently ignored by gRPC.

`connections` is validated to the range [1, 16]. The default of 4 opens
four gRPC connections to the server — each a distinct TCP flow — and
spreads Read/Write streams across them (least-in-flight; a sequential single-stream workload stays on the primary connection). On a high-bandwidth-delay
link (1 Gbit WAN, inter-datacenter) a single TCP flow is often limited by
its congestion window, so multiple flows let the client aggregate bandwidth
from each. Metadata RPCs (Lookup, GetAttr, Readdir, …) and the
session-lifecycle RPCs (Establish, Resume, keepalive pings, Subscribe)
always use the primary (first) connection; only data-plane streams are
distributed. Set `connections: 1` to restore the historical
single-connection behaviour.

`write_coalesce_bytes` is validated to the range [0, 16777216] (0 to
16 MiB). 0 disables coalescing entirely so every Write call hits the
network; the default 1 MiB matches the streaming-frame size and absorbs
the common "many tiny appends" pattern (logs, build outputs, etc.).

### Readahead

When sequential reads are detected on an fd (after `readahead_threshold`
in-order reads), the client keeps up to `readahead_window`
`readahead_chunk_bytes`-sized chunks in flight ahead of the cursor, each
its own streaming Read RPC. gRPC multiplexes them across the available
connections (see `connections`), so a deep window hides the per-fetch
round-trip latency and keeps the read pipe full. Reads are served from the in-flight chunks
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
neutral; the default window of 16 keeps enough chunks overlapped to fill
a ~50 ms link, raise it for longer/fatter pipes.

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
  timeout_meta: 10s
  timeout_io: 30s
  retry_window: 60s       # 0 = fail-fast (single attempt)
  readahead_chunk_bytes: 131072  # 128 KiB
  readahead_threshold: 3
  initial_conn_window_bytes: 16777216    # 16 MiB; 0 (with stream=0) restores autotuning
  initial_stream_window_bytes: 8388608   # 8 MiB; set together with the conn window
  connections: 4          # TCP flows per mount; raise on high-BDP (WAN) links, set 1 for single-conn
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

| Option              | Type     | Default  | Description                                                      |
|---------------------|----------|----------|------------------------------------------------------------------|
| max\_write\_bytes   | integer  | 1048576  | Ceiling for FUSE WRITE/READ size in bytes (1 MiB default)        |
| max\_background     | integer  | 64       | Max async background requests the kernel may have in flight      |
| writeback\_cache    | boolean  | false    | Enable the kernel's writeback page cache for the mount           |
| handle\_kill\_priv | boolean  | true     | Advertise `CAP_HANDLE_KILLPRIV_V2`; eliminates the per-write `security.capability` getxattr RPC (env: `GMOUNTIE_FUSE_HANDLE_KILL_PRIV`) |
| attr\_timeout       | duration | 1s       | How long the kernel caches inode attributes (size, mode, timestamps) before issuing a GETATTR |
| entry\_timeout      | duration | 1s       | How long the kernel caches dentry (name→inode) mappings before issuing a LOOKUP |
| fuset\_backend      | string   | auto     | macOS/FUSE-T only: `auto` (prefer Apple FSKit, fall back to NFS), `nfs`, or `fskit` |

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

`writeback_cache` defaults to off. Toggling it on enables the FUSE
`CAP_WRITEBACK_CACHE` capability bit — an opt-in for single-writer
workloads over high-latency links. The client-side cache, readahead, and
write coalescing work independently of this kernel knob; see
[Caching & consistency](../design/caching-and-consistency.md).

`handle_kill_priv` defaults to on. Advertising `CAP_HANDLE_KILLPRIV_V2`
tells the kernel that the FUSE server handles privilege stripping on
modify — so the kernel skips the `security.capability` getxattr it would
otherwise issue on **every write** to decide whether to strip setuid,
setgid, or file-capability bits. Over a FUSE mount that getxattr is one
extra round-trip RPC per write, which caps single-file write throughput on
high-RTT links. With the capability advertised, the kernel marks the inode
`S_NOSEC` and instead sends a `setattr` when modify requires stripping; the
identity-bound server applies it as the resolved principal, so the bits
**are still stripped** — the capability removes the per-write xattr probe,
not the stripping itself. Leave this on unless a backing filesystem
mishandles the capability; set `GMOUNTIE_FUSE_HANDLE_KILL_PRIV=false` or
`fuse.handle_kill_priv: false` to opt out. See [§10 in
security-and-transport](../design/security-and-transport.md#per-write-privilege-stripping-handle_killpriv_v2)
for the security model.

`direct_io` defaults to off. When on, every file handle is opened with
`FOPEN_DIRECT_IO`: the kernel bypasses its page cache for reads/writes and
**refuses a shared writable `mmap` with a clean error instead of a `SIGBUS`**.
It disables kernel-side read caching for the whole mount, so leave it off
unless you run an `mmap`-heavy workload that needs `MAP_SHARED` writable
mappings (e.g. LMDB, or SQLite with a large `mmap_size`).

**SQLite WAL works out of the box** — you do **not** need `direct_io` for it.
SQLite's WAL mode `MAP_SHARED`-mmaps a `-shm` sidecar, which cannot be backed
over a network FUSE mount; the client always opens `*-shm` files direct-IO so
that mapping fails cleanly rather than bus-faulting (which previously also
left the database unopenable until the sidecars were deleted by hand). WAL
itself is not coherent over the mount, so SQLite returns a normal I/O error;
for a working journal use `PRAGMA journal_mode=DELETE` (and `PRAGMA
mmap_size=0` if you raised it). The data is never corrupted by any of this.

`attr_timeout` and `entry_timeout` both default to 1 s, matching go-fuse's
built-in default. Raising them cuts GETATTR / LOOKUP round-trips at the cost
of a wider kernel-side staleness window. **Important:** the Subscribe
push-invalidation stream cannot reach inside the kernel's dentry or attribute
caches — only VFS operations that bypass or expire the cache will see fresh
data. Values above ~1 s therefore explicitly trade coherence for reduced
chatter and are safe only when this client is the sole writer, or when
metadata staleness is acceptable (e.g. read-only analytics workloads).
Both fields are validated to `gte=0`; `0` (or unset) means **use the
default**, never cache-off — a non-nil zero would turn every kernel op
into a fresh round-trip (~1000× metadata amplification). To approximate
disabling a tier, set a tiny value such as `1ms`.

`fuset_backend` applies only on macOS when the resolved provider is FUSE-T
(it is ignored for macFUSE and on Linux). FUSE-T's default **NFS** backend
makes the macOS NFS client amplify metadata RPCs over a high-RTT link
(free-space polling, GETATTR-during-write, `._` AppleDouble sidecars).
FUSE-T's **FSKit** backend (Apple FSKit, macOS 15+) mounts natively and avoids
that amplification. `auto` (the default) uses FSKit when its `FskitSrvModule`
extension is installed and **transparently falls back to NFS** if the FSKit
mount fails (e.g. the extension is installed but not enabled in *System
Settings → General → Login Items & Extensions*). `fskit` forces it and fails
with a clear error if unavailable; `nfs` forces the NFS backend. See
[macOS mount](../design/macos-mount.md).

Example:

```yaml
fuse:
  max_write_bytes: 2097152  # 2 MiB
  max_background: 128
  writeback_cache: false
  handle_kill_priv: true    # default on; set false only if the backing fs mishandles CAP_HANDLE_KILLPRIV_V2
  direct_io: false    # set true only for MAP_SHARED mmap workloads (LMDB, SQLite mmap_size>0)
  attr_timeout: 5s    # raise on read-only / sole-writer mounts to cut GETATTR traffic
  entry_timeout: 5s   # same tradeoff for dentry (name→inode) lookups
```

## Cache Options

The `cache` section configures the client-side two-tier cache layer that
decorates the gRPC backend. When enabled, the cache holds recent
attribute lookups, directory listings, and file-data chunks across an
in-memory tier and a disk-persistent tier (under `path`, which survives
a remount); on a hit the FUSE op short-circuits without crossing the
wire.

Enabled by default. The disk tier persists chunks and metadata under
`path`, so a warm cache survives across mounts.

| Key                     | Type     | Default  | Description                                                            |
|-------------------------|----------|----------|------------------------------------------------------------------------|
| enabled                 | boolean  | true     | Enable the client-side cache decorator                                 |
| subscribe\_enabled      | boolean  | true     | Open a Subscribe stream for server-pushed invalidation (else pure TTL) |
| path                    | string   | `$XDG_CACHE_HOME/gmountie` | Root directory for the disk-persistent tier          |
| memory\_max\_bytes      | integer  | 268435456 (256 MiB) | Byte budget for the in-memory tier across the attr+dir+data sub-caches |
| disk\_max\_bytes        | integer  | 10737418240 (10 GiB) | Byte budget for the disk-persistent tier                    |
| chunk\_size\_bytes      | integer  | 1048576 (1 MiB)    | Granularity of the data cache; reads chunk-align against this |
| attr\_ttl               | duration | 5m       | TTL for positive attribute cache entries                               |
| dir\_ttl                | duration | 5m       | TTL for directory listing cache entries                                |
| negative\_ttl           | duration | 30s      | TTL for negative attribute cache entries (ENOENT lookups)              |

`memory_max_bytes` is validated to the range [0, 68719476736] (0 to
64 GiB) and `disk_max_bytes` to [0, ∞). 0 disables that tier's byte
budget — entries still age out on TTL but nothing is force-evicted on
size pressure. The negative cache is memory-only and is not counted
against either byte budget.

`chunk_size_bytes` is validated to the range [4096, 16777216] (4 KiB
to 16 MiB). The data cache stores fixed-size chunks; a 1 MiB read at a
non-aligned offset spans two chunks. The default mirrors the streaming
frame size so chunk fetches map 1:1 to a single Read RPC.

The TTLs control coherence vs. RPC traffic. With `subscribe_enabled`
on (the default), server-pushed invalidation is the primary freshness
signal and the TTLs act as a backstop; with it off the cache is pure
TTL. Shorter TTLs make file-system changes made by other clients
visible sooner at the cost of more frequent revalidation; longer TTLs
are safe only when this client is the sole writer.

Example:

```yaml
cache:
  enabled: true
  subscribe_enabled: true
  path: ~/.cache/gmountie
  memory_max_bytes: 268435456   # 256 MiB
  disk_max_bytes: 10737418240   # 10 GiB
  chunk_size_bytes: 1048576     # 1 MiB
  attr_ttl: 5m
  dir_ttl: 5m
  negative_ttl: 30s
```

## Authentication Options

The `auth` section configures client authentication:

| Option           | Type   | Required | Description                                        |
|------------------|--------|----------|----------------------------------------------------|
| type             | string | yes      | Authentication type: `basic` or `mtls` (mTLS uses the client cert/key under `server.tls`) |
| username         | string | yes      | Username for basic auth                            |
| password         | string | no       | Inline plaintext password. Prefer `password_command` or `password_file` to keep secrets out of the config. |
| password_command | string | no       | Shell command whose stdout (trailing newline stripped) is the password — e.g. `pass show gmountie/work`. Runs via `sh -c`. |
| password_file    | string | no       | Path to a file containing the password (must be mode 0600). `$GMOUNTIE_AUTH_PASSWORD_FILE` is checked when this field is absent. |

Authentication is required. With `type: basic` the client must supply at least `username`; the password can be omitted from the config file and supplied at runtime. With `type: mtls` no username/password is needed — the verified client certificate (`server.tls.cert_file`/`key_file` or the `*_pem` variants) is the identity.

**Password resolution order** (first non-empty wins): `--password` flag → `auth.password_command` → `auth.password_file` / `$GMOUNTIE_AUTH_PASSWORD_FILE` → `auth.password` (inline) → `$GMOUNTIE_AUTH_PASSWORD` → interactive no-echo prompt.

### Basic Authentication

```yaml
auth:
  type: basic
  username: admin
  password: ""   # omit or leave empty to use $GMOUNTIE_AUTH_PASSWORD or an interactive prompt
```

To retrieve the password from a password manager:

```yaml
auth:
  type: basic
  username: admin
  password_command: "pass show gmountie/work"
```

To load the password from a 0600 file:

```yaml
auth:
  type: basic
  username: admin
  password_file: /run/secrets/gmountie-password
```

## Certificate Auto-Renewal (`renew`)

The `renew` block opts the client into automatic client-cert renewal. When
configured, the client exchanges a bearer token for a short-lived mTLS
client certificate at a token→certificate endpoint, renews it before
expiry, and holds both the key and the certificate **exclusively in process
memory** — no cert or key is ever written to disk. In token mode you do not
need `server.tls.cert_file`/`key_file` at all; the CA for verifying the
data-plane server is delivered by the exchange endpoint and used
automatically when no `server.tls.ca_file` is set.

An absent `renew` block — or one with no `endpoint` — disables renewal entirely. The
client behaves exactly as without the block; no default-on behaviour exists.

| Option       | Type     | Default | Description                                                                                                   |
|--------------|----------|---------|---------------------------------------------------------------------------------------------------------------|
| endpoint     | string   | _(none)_ | Base URL of the exchange service. The client appends `/profile` and `/renew`; see [Wire contract](#wire-contract) below. |
| token        | string   | _(none)_ | Inline bearer token. `token_file` wins when both are set.                                                      |
| token\_file  | string   | _(none)_ | Path to a file whose (whitespace-trimmed) contents are the bearer token. **Re-read on every exchange** so the token can rotate without a restart. |
| before       | duration | 8h      | Renewal lead time before cert expiry. Must be positive.                                                        |

Either `token` or `token_file` is required when `endpoint` is set. The token
can also be supplied via the `$GMOUNTIE_RENEW_TOKEN` environment variable (set
`token_file` to a file populated by your secrets provider to avoid inline
secrets).

```yaml
renew:
  endpoint: https://cp.example.com/v1/certs   # GET /profile + POST /renew appended
  token_file: /var/run/secrets/mount-token    # or token: inline / $GMOUNTIE_RENEW_TOKEN
  before: 8h
```

### Wire contract

The client makes two HTTP requests to `endpoint`:

**1. `GET {endpoint}/profile`** — `Authorization: Bearer <token>`. Response
(`application/json`):

```json
{
  "subject": "device-xyz",
  "sans":    ["spiffe://example.com/device/xyz"],
  "ca_pem":  "-----BEGIN CERTIFICATE-----\n…"
}
```

`subject` becomes the cert CN; `sans` entries that look like URIs (have a
non-empty scheme) become URI SANs, everything else becomes DNS SANs. `ca_pem`
is the CA to use for verifying the data-plane server when no
`server.tls.ca_file` is configured.

**2. `POST {endpoint}/renew`** — `Authorization: Bearer <token>`,
`Content-Type: application/json`. Request body:

```json
{ "csr_pem": "-----BEGIN CERTIFICATE REQUEST-----\n…" }
```

The CSR carries a fresh in-memory P-256 key matching the profile's subject and
SANs. Response:

```json
{
  "cert_chain_pem": "-----BEGIN CERTIFICATE-----\n…",
  "ca_pem":         "-----BEGIN CERTIFICATE-----\n…",
  "not_after":      "2026-06-13T12:00:00Z"
}
```

The client validates that the returned leaf cert matches the CSR key and subject,
then swaps it into the in-memory cert source. `not_after` is informational; the
client schedules renewal from the leaf certificate's own `NotAfter` field, not
from the JSON field.

### Renewal lifecycle

The initial exchange runs **synchronously at mount start** — the first TLS
handshake needs a cert and CA. If the initial exchange fails, `mount` fails
immediately. After mount, a background goroutine runs the renewal loop: it
sleeps until `not_after − before`, attempts renewal, and retries with
exponential backoff ([1 m, 15 m]) on failure. A failed renewal never tears down
the mount; the existing cert continues to be used until it actually expires.

The new cert takes effect at the **next TLS handshake** — the live connection is
not re-handshaked, so active RPCs are uninterrupted. The key never leaves process
memory.

`renew.endpoint` and a static client cert (`server.tls.cert_file`/`cert_pem`)
are mutually exclusive; the client returns an error at startup if both are set.

### Token-mode credential bundle

`GMOUNTIE_CREDENTIALS` bundles gain a token form that carries just the
endpoint and a renew token — no cert or key material. The mount command
enables token mode automatically when it decodes a token-mode bundle:

```json
{
  "endpoint":       "resolver.example.com:443",
  "renew_endpoint": "https://cp.example.com/v1/certs",
  "renew_token":    "eyJ…"
}
```

Static bundles (carrying `cert_pem`/`key_pem`/`ca_pem`) are unchanged and
remain the default.

## Mount Configuration

The `mount` section defines the volume and (optionally) a default mountpoint.

| Option | Type   | Required | Description            |
|--------|--------|----------|------------------------|
| type   | string | yes      | Must be `"single"`     |
| volume | string | no       | Volume name to mount. Overridden by the `--volume` flag or the shorthand `host/volume` argument. |
| path   | string | no       | Default local mountpoint. When set, the positional mountpoint argument may be omitted on the command line. |

`volume` and `path` are the fields that make a client config useful as a **profile**: store the target volume (and optionally its default mountpoint) alongside the server and auth settings, then select it with `--profile <name>`. Profiles live at `~/.config/gmountie/profiles/<name>.yaml`; any valid client config can be used as a profile. `--profile` and `--config` are mutually exclusive. Profile names are restricted to `[A-Za-z0-9._-]` (no path separators, no `..`), so `--profile` can only ever resolve to a file inside the profiles directory.

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

