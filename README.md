<div align="center">
  <img class="logo" src="assets/wordmark.png" alt="gMountie" width="360"/>
  <h3>Your Filesystem's Best Friend</h3>
  <p><i>Because remote filesystems shouldn't feel so... remote</i></p>

  [![Release](https://img.shields.io/github/v/release/gMountie/gMountie?include_prereleases&sort=semver&label=release)](https://github.com/gMountie/gMountie/releases)
  [![Go](https://img.shields.io/github/go-mod/go-version/gMountie/gMountie)](go.mod)
  ![Platform](https://img.shields.io/badge/platform-Linux-informational)
  ![Status](https://img.shields.io/badge/status-alpha-orange)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
</div>

**gmountie mounts a directory from a remote server and makes it behave like a local folder — over the public internet, no VPN, and without falling apart when the network hiccups.** It's built on [FUSE](https://www.kernel.org/doc/html/latest/filesystems/fuse.html) and [gRPC](https://grpc.io): a `gmountie serve` process exposes folders as named **volumes**, and a `gmountie mount` client mounts one locally and proxies every filesystem call to the server.

## Why gMountie? 🤔

Accessing files on a server across the internet usually means a VPN plus NFS/SMB (brittle once real-world latency shows up), or a sync tool that copies everything to disk first. gMountie goes for the NFS experience — a real, live mounted filesystem — but designed for the internet from day one:

- **No VPN.** It's just a gRPC connection to your server. Expose the port or put it behind a reverse proxy, and mount from anywhere.
- **It survives the network.** Sessions and idempotent RPCs mean a 30-second drop, an ISP IP change, or a server restart won't kill your mount or your open files — they reconnect and resume.
- **Reads get cheap after the first one.** A persistent client-side cache (in memory *and* on disk, kept fresh by server push) means re-reading a file doesn't cross the wire again.
- **One small binary.** The same `gMountie` is both the server and the client.

> [!NOTE]
> gMountie is **alpha** and **Linux-only** today, with a single-user-ish trust model — TLS and security hardening are on the [roadmap](docs/roadmap.md). It's a great fit for mounting your own servers; it is not yet meant to face hostile networks.

## Quick Start 🚀

> The **server** just exposes folders — no special requirements. The **client** mounts via FUSE, so it needs `fuse3` (`/dev/fuse`) on Linux.

### 1. Get `gMountie`

```bash
# Build from source (Go 1.2x) — gives you both the server and the client
git clone https://github.com/gMountie/gMountie && cd gMountie
go build -o gMountie ./cmd
```

Prefer not to build? Grab a `gMountie_linux_*.tar.gz` from the [releases page](https://github.com/gMountie/gMountie/releases), or run the server from the container image `ghcr.io/gmountie/gmountie-server`.

### 2. Start a server

Point a volume at a folder you want to share (`config.yaml`):

```yaml
server:
  address: 0.0.0.0
  port: 9449
auth:
  type: basic
  users:
    - username: admin
      password: change-me
volumes:
  - name: shared
    path: /srv/shared        # the folder to expose
```

```bash
gmountie serve -c config.yaml
```

### 3. Mount it from a client

No config file needed — point the client at the server and pick a volume:

```bash
mkdir -p ~/mnt/shared
gmountie mount ~/mnt/shared \
  --server your-server.example:9449 \
  --auth-type basic --username admin --password change-me \
  --volume shared

# ...and use it like any other folder:
ls ~/mnt/shared
```

Reads and writes now flow to the server, are cached locally, and the mount rides out network blips. For a persistent setup (a `client.yaml` with the same fields, plus multi-volume mounts), see the full walkthrough at **[docs.gmountie.dev](https://docs.gmountie.dev)**.

## Features ✨

**Fast**
- gRPC **streaming** Read/Write with a tunable frame size — no message-size ceiling on large files
- A **persistent client cache** for attributes, directory listings, and file data — in memory and on disk
- **Readahead** and **write coalescing** for sequential I/O, **compound** metadata batching to cut round-trips, plus an optional **writeback** mode and **Snappy** compression for high-latency links

**Resilient**
- **Sessions + idempotent RPCs**: retries and reconnects are safe and invisible; file handles opened before a blip stay valid after it
- **Push-based invalidation** (a server `Subscribe` stream) keeps clients coherent — close-to-open consistency across multiple clients

**Simple & observable**
- One binary, two commands: `gmountie serve` and `gmountie mount`
- Prometheus metrics, health/readiness endpoints, structured logs, and basic authentication

## How It Works 🏗️

```
     your machine                                   remote server
┌─────────────────────┐       gRPC over HTTP/2     ┌─────────────────────┐
│   gmountie mount    │ ◀────────────────────────▶ │   gmountie serve    │
│  FUSE mount point   │  metadata · data · events  │  real folders       │
│  + local cache      │                            │  exposed as volumes │
└─────────────────────┘                            └─────────────────────┘
```

The client implements a FUSE filesystem and turns each syscall into a gRPC call against the server, which serves it from the configured volume's real directory. Metadata, file data, and cache-invalidation events travel over three separate gRPC services, so they can be routed and tuned independently.

Dig deeper: **[Architecture & Protocol](docs/design/architecture.md)** · **[Caching & Consistency](docs/design/caching-and-consistency.md)** · **[Performance](docs/design/performance.md)**

## Documentation 📚

Full documentation lives at **[docs.gmountie.dev](https://docs.gmountie.dev)**. Highlights:

- [Quick Start](docs/quickstart.md)
- [Architecture & Protocol](docs/design/architecture.md)
- [Caching & Consistency](docs/design/caching-and-consistency.md)
- [Performance](docs/design/performance.md)
- [Roadmap](docs/roadmap.md)
- Configuration & CLI reference — [server](docs/server/config.md) · [client](docs/client/config.md)

## Naming & Branding 📛

The project name is written **`gMountie`** — lowercase `g`, capital `M`. The `g` is for **gRPC**, the wire protocol the client and server speak.

Canonical forms across the project:

| Form | Use for |
| --- | --- |
| `gMountie` | Prose, docs, marketing, README, CLI help strings |
| `gmountie` | Go module path, binary name, package names, file/repo names, URLs |
| `GMOUNTIE_` | Environment variable prefix |

Please avoid `GMountie`, `Gmountie`, or other variants when contributing.

## Contributing 🤝

We love contributions! Whether it's:

- 🐛 Bug reports
- 💡 Feature suggestions
- 📝 Documentation improvements
- 🔧 Code contributions

Open an [issue](https://github.com/gMountie/gMountie/issues) or a pull request to get started. (A dedicated contributing guide is on the way.)

## Support & Community 💬

- 📫 [GitHub Issues](https://github.com/gMountie/gMountie/issues) for bug reports and feature requests
- ⭐ [Star us on GitHub](https://github.com/gMountie/gMountie) to show your support
- 💖 [Become a sponsor](https://github.com/sponsors/johnbuluba) to support development

## License 📜

gMountie is proudly open source, licensed under the [Apache License 2.0](LICENSE).

---

<div align="center">
  <i>Happy Mounting! 🎉</i>
</div>
