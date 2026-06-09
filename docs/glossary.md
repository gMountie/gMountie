---
id: glossary
title: Glossary
sidebar_label: Glossary
description: One-line definitions for every gMountie term that shows up in the docs.
---

# Glossary

One-line definitions. Cross-linked into the rest of the docs — when you hit an unfamiliar term elsewhere, this is the canonical reference.

## A

**Anonymous user (`anon_uid`)** — In `passthrough` mapping mode, the uid the server uses when `root_squash` is on and the client is root. Default `65534` (typically `nobody`). See [Identity & ownership](./concepts/identity.mdx).

**`auth`** — Top-level config section on both server and client. Two schemes are shipped: `type: basic` and `type: mtls`. See [Server configuration](./server/config.md) and [Client configuration](./client/config.md).

## B

**Basic auth** — One of the two shipped authentication schemes. Username + password sent on every connection, validated by a server-side interceptor. Passwords are argon2id-hashed server-side.

## C

**Cache (client-side)** — Decorator over the gRPC backend. On a hit, the FUSE op returns without crossing the wire. Two-tier (in-memory + on-disk) and **enabled by default**; disable with `cache.enabled: false`. See [Cache & consistency](./concepts/cache.mdx).

**Chunk** — Fixed-size unit (`cache.chunk_size_bytes`, default 1 MiB) the data cache stores file content in. A read spanning two chunks consumes two cache entries.

**Close-to-open consistency** — The consistency model gMountie targets. When one client closes a file, subsequent opens on any client see the new bytes. Same model NFS popularized.

**Cluster (Helm)** — A Kubernetes deployment of the server. The chart at `deployments/charts/gmountie-server` ships pre-configured for it. See [Recipes — Docker & containers](./recipes/docker.md) for image basics.

## D

**`dac_override`** — Admin capability. Granted per principal in `static` mode (`caps:`) or per server-group in `system` mode (`admin_groups:`); lets the principal read, traverse, AND modify any path on the volume regardless of mode bits. Created entries are `fchown`ed to the principal. Requires the server to run as root. See [Identity & ownership — Admin capabilities](./concepts/identity.mdx#admin-capabilities).

**`dac_read_search`** — Admin capability. Like `dac_override` but read-only: reads and traverses bypass DAC, writes still return `EACCES`. Right cap for backup runners and audit accounts.

**`dirent`** — Directory entry. A `(name, inode)` pair returned by `readdir`.

## E

**`EIO`** — POSIX "I/O error." gMountie surfaces this when a syscall can't be served (server gone past keepalive, fd no longer valid). See [Sessions & reconnect](./concepts/sessions-and-reconnect.mdx).

## F

**`fd`** — File descriptor. Server allocates one per `Open`; the client reuses it for subsequent `Read` / `Write` / `Release`. fds survive a reconnect blip thanks to sessions.

**Frame size (`frame_size_bytes`)** — Server-side cap on the size of one streaming Read/Write frame. The client's FUSE `max_write_bytes` is capped at this on mount.

**FUSE** — Filesystem in Userspace. Kernel module that lets `gmountie mount` implement a real filesystem in a regular process. The client runs on Linux and macOS (via macFUSE / FUSE-T); the server is Linux-only. See [Wire protocol](./concepts/wire-protocol.mdx).

## G

**`gMountie` vs `gmountie`** — Brand convention. **`gMountie`** in prose (the project name); **`gmountie`** for the CLI, binary, code identifiers, and URLs. The `g` is for gRPC.

**gRPC** — The wire protocol. HTTP/2 with protobuf payloads (optionally Snappy-compressed — opt-in via `rpc.compression: snappy`, default `none`). Five services share one connection: `fs`, `file`, `session`, `volume`, `version`.

## I

**Idempotency token** — Per-request token on mutating RPCs (`Mkdir`, `Rename`, etc.). The server caches `(session_id, request_id) → reply` so retrying after a reconnect is safe.

**Inode** — A filesystem object's identifier (file, directory, symlink). `Lookup` resolves a name to an inode + attrs.

## K

**Keepalive** — gRPC HTTP/2 pings in both directions that detect dead connections quickly (~40 s default). Configured under `server.keepalive` and `rpc.keepalive`. See [Sessions & reconnect](./concepts/sessions-and-reconnect.mdx).

## L

**`Lookup`** — The FUSE operation that resolves a name in a parent directory to an inode + attrs. There is no dedicated `Lookup` RPC; the client serves it with a `GetAttr` call on the `fs` service.

## M

**Mapping mode** — Per-volume identity policy on the server: **`squash`**, **`static`**, **`system`**, or **`passthrough`**. Decides which uid/gid the server acts as for that volume's RPCs. See [Identity & ownership](./concepts/identity.mdx).

**`max_message_bytes`** — Cap on inbound/outbound gRPC message size (default 16 MiB). Should match between server and client.

**Mount** — In FUSE terms, attaching a filesystem at a path. In gMountie terms, attaching a server's named volume to a local path via `gmountie mount`.

**mTLS** — Mutual TLS (`auth.type: mtls`). The client presents a certificate; the server derives the principal from the cert CN (or first DNS SAN). Supports per-user volume ACLs, a `revoked_serials` list, and a `/ops/acl/reload` endpoint. See [Server configuration](./server/config.md).

**Mountpoint** — The local directory where `gmountie mount` attaches a volume. Must be an empty directory.

## N

**NSS** — Name Service Switch. The server's user/group database (`/etc/passwd`, `/etc/group`, optionally LDAP/SSSD). The `system` mapping mode resolves principals against it.

## P

**`Pacer`** — The marmot mascot. Appears as a TOC card on every page of these docs.

**Passthrough** — One of the four mapping modes. Use the uid/gid the client claims (with optional `root_squash`). Useful for backups and trusted clients.

**POSIX** — Portable Operating System Interface. gMountie aims for POSIX-correct semantics where applicable (atomic rename, mtime preservation, fcntl locks where supported, `mmap` support).

**Principal** — The authenticated user (a basic-auth `username`, or the CN / first DNS SAN of a client cert under mTLS). Maps to a server-side identity via the volume's `mapping`.

## R

**Readahead** — Speculative prefetching of upcoming chunks on a sequentially-read fd. Tuned by `rpc.readahead_*`. See [Cache — Sequential reads](./concepts/cache.mdx).

**Readahead chunk** — One prefetched chunk (`rpc.readahead_chunk_bytes`, default 1 MiB).

**Readahead window** — Number of prefetched chunks the client keeps in flight ahead of the cursor (`rpc.readahead_window`, default 4).

**Readahead threshold** — Sequential reads required before prefetch arms (`rpc.readahead_threshold`, default 3).

**`readdir`** — RPC on the `fs` service. Returns the dirent list of a directory.

**Reconnect** — Re-establishing the gRPC connection after a network blip. fds reattach automatically; the application above FUSE doesn't see the blip.

**`Release`** — RPC on the `file` service. Closes a server-side fd; flushes buffered writes.

**`root_squash`** — In `passthrough` mode, remaps client-root (`uid 0`) to `anon_uid` so client-root can't write as server-root. Default `true`.

## S

**Session** — Server-side context for one client connection. Holds the fd table and the idempotency cache. Reaped on disconnect.

**Snappy** — Opt-in compression codec for gRPC payloads (`rpc.compression: snappy`, default `none`), registered as a custom codec at `pkg/server/grpc/snappy`. Worth enabling on slow WAN links; on fast links the compressor itself is the bottleneck — see [Performance § 2.7](./design/performance.md).

**`Squash`** — One of the four mapping modes. Every authenticated principal becomes one fixed `(uid, gid)`. NFS `all_squash` style.

**`Static` (mapping)** — One of the four mapping modes. Lookup table from `username` to `{uid, gid, groups[]}`.

**`Subscribe`** — Long-lived RPC on the `volume` service. The server pushes cache-invalidation events to subscribed clients.

**`System` (mapping)** — One of the four mapping modes. Resolves the principal against the server's NSS.

## T

**TLS** — Transport Layer Security. Shipped natively. The server auto-generates a self-signed ECDSA P-256 certificate on first run (10-year validity; inspect its SHA-256 fingerprint with `gmountie fingerprint`). The client verifies the server with one of three modes: `verify` (system roots), `tofu` (trust-on-first-use, pinned to a `known_hosts` file), or `insecure`.

**TTL** — Time-to-live for cache entries (`cache.attr_ttl`, `cache.dir_ttl`, `cache.negative_ttl`).

## V

**Volume** — A named directory the server exposes (e.g., `shared: /srv/shared`). Every RPC is volume-scoped — the client tells the server which volume it's asking about; the server has no implicit "current volume" state.

## W

**`WhoAmI`** — RPC on the `session` service. The client asks the server "as you see me, who am I?" The server's answer (uid, gid, groups, user_name, group_names) drives client-side metadata rewriting in `ls -l`.

**Write coalescing** — Client-side buffering of small contiguous writes to reduce per-write round-trips. Tuned by `rpc.write_coalesce_bytes` (default 1 MiB). See [Client config — Write Coalescing](./client/config.md#write-coalescing).
