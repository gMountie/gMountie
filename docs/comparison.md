---
id: comparison
title: Compared to other ways of mounting remote storage
sidebar_label: Comparison
description: Where gMountie sits next to NFS-over-VPN, sshfs, Tailscale Drive, and S3-FUSE — a property matrix plus when each makes sense.
---

# Compared to other ways of mounting remote storage

If you're evaluating gMountie against another tool, this page is the property matrix and the "when each makes sense" cheat sheet. Stick to facts, no slander — the differences below are protocol-level, not opinions.

## Property matrix

| Property                       | gMountie                                         | NFS over VPN (Tailscale / WireGuard)        | sshfs                                  | Tailscale Drive                       | s3fs / goofys / rclone mount     |
| ------------------------------ | ------------------------------------------------ | ------------------------------------------- | -------------------------------------- | ------------------------------------- | -------------------------------- |
| **Protocol**                   | gRPC over HTTP/2, Snappy-compressed              | NFSv4 over TCP, inside a VPN tunnel         | SFTP over SSH                          | WebDAV inside WireGuard               | HTTP to S3 / S3-compatible API   |
| **POSIX correctness**          | atomic rename · mtime · fcntl* · mmap            | high (NFSv4)                                | partial — locking limited              | low — no atomic rename, no fcntl, no mmap | low — eventual consistency       |
| **WAN read throughput**        | designed for it — streaming reads, readahead window, write coalescing | poor — chatty metadata is RTT-amplified inside the VPN | poor on high-RTT                       | limited by WebDAV; DERP-relay path is slow | metadata-heavy; per-object HTTP   |
| **Client cache**               | optional, push-invalidated, on-disk persistence  | none in protocol                            | none                                   | none                                  | varies (rclone has one; s3fs minimal) |
| **Reconnect / blip survival**  | fds + idempotency tokens — `~40 s` keepalive    | depends on VPN; mount stalls / hard-locks   | breaks the SSH connection              | breaks the WebDAV session             | per-request; no session          |
| **Auth**                       | basic or mTLS, over native TLS                   | inherits VPN + system uids                  | SSH keys / agent                       | tailnet ACLs                          | AWS-style keys / IAM             |
| **Multi-writer**               | last-`Release`-wins                              | NFSv4 byte-range locks                      | none                                   | none                                  | none                             |
| **Setup**                      | one binary, two commands                         | install VPN + NFS server + auth             | sshd + sshfs                           | install Tailscale on every endpoint   | bucket + creds + mount tool      |
| **Client OS**                  | Linux + macOS (macFUSE / FUSE-T)                 | yes                                         | yes                                    | yes                                   | yes                              |

<small>*fcntl locks tracked in [design/architecture.md](./design/architecture.md); the full POSIX-coverage table lives there.</small>

## When each makes sense

- **gMountie** — you want a mount that reads as a real local folder, over the public internet, with one binary and no VPN tunnel. Especially good on **ephemeral compute** (rent a GPU box, mount your dataset, work, tear it down).
- **NFS over Tailscale / WireGuard** — you already run a VPN to that machine and just want NFS semantics. Accept the RTT-amplification on metadata and the home-uplink throughput cap.
- **sshfs** — quick and dirty, occasional browsing and `cat` over an SSH path you already have. Don't run a build on it.
- **Tailscale Drive** — you already use Tailscale and want a Taildrop-shaped "share a folder with another node" workflow for static content. WebDAV under the hood limits the use case.
- **s3fs / goofys / rclone mount** — your data really is in S3 (or compatible) object storage and you can tolerate POSIX-imperfect semantics (no atomic rename, eventual consistency).

## What gMountie isn't

Worth saying explicitly:

- **Not a sync tool.** Files don't live on the client; reads go over the wire (with caching). If you want offline access, mount a snapshot of the data, not the live volume.
- **Not a CDN.** Reads are billed in syscalls, not in cache-edge hits. For hot static paths, put a real cache in front.
- **Not multi-master.** One server can serve many clients, but writes serialise through that server's filesystem. No CRDTs, no merge — last writer's `Release` wins.

If your use case looks more like one of those, gMountie is the wrong tool — the comparison column above will tell you what's right.

## See also

- [Quickstart](./quickstart.mdx) — see it work in 90 seconds.
- [Wire protocol](./concepts/wire-protocol.mdx) — what's actually crossing the network.
- [Cache & consistency](./concepts/cache.mdx) — why the cache is the differentiator on WAN.
