<div align="center">
  <img class="logo" src="assets/lockup-mascot-stack.png" alt="gMountie" width="320"/>
  <h3>Your Filesystem's Best Friend</h3>
  <p><i>Because remote filesystems shouldn't feel so... remote</i></p>

  [![Release](https://img.shields.io/github/v/release/gMountie/gMountie?include_prereleases&sort=semver&label=release)](https://github.com/gMountie/gMountie/releases)
  [![Go](https://img.shields.io/github/go-mod/go-version/gMountie/gMountie)](go.mod)
  ![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20(client)-informational)
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
> gMountie is **alpha**. The **server is Linux-only**; the **client mounts on Linux and macOS**. Transport is TLS (auto-generated cert on first run) with basic-auth and per-user volume ACLs — it's a great fit for mounting your own servers over the public internet. OIDC/JWT auth remains on the [roadmap](docs/roadmap.md).

## Quick Start 🚀

> The **server** (Linux only) just exposes folders — no special requirements. The **client** mounts via FUSE: install `fuse3` (`/dev/fuse`) on Linux, or [**macFUSE**](https://macfuse.io) / [**FUSE-T**](https://www.fuse-t.org/) on macOS.

### 1. Get `gMountie`

```bash
# Build from source (Go 1.2x) — gives you both the server and the client
git clone https://github.com/gMountie/gMountie && cd gMountie
go build -o gMountie ./cmd
```

Prefer not to build? Grab a `gMountie_linux_*.tar.gz` (or `gMountie_darwin_*.tar.gz` for the macOS client) from the [releases page](https://github.com/gMountie/gMountie/releases), or run the server from the container image `ghcr.io/gmountie/gmountie-server`.

### 2. Start a server

**Zero-config first run:** just run `gmountie serve` with no arguments. On first start it:
- auto-generates `~/.config/gmountie/server.yaml` binding `0.0.0.0:9449`
- creates a `shared` volume at `$XDG_DATA_HOME/gmountie/shared`
- generates a **random admin password**, prints it once to the console, and stores an argon2id hash in the config

After first run you can inspect or edit the config at `~/.config/gmountie/server.yaml`. To rotate the password run `gmountie genpass`, copy the printed hash, and paste it into `auth.users[0].password_hash`.

Or write your own config from the start (`server.yaml`):

```yaml
server:
  address: 0.0.0.0
  port: 9449
auth:
  type: basic
  users:
    - username: admin
      password_hash: $argon2id$v=19$m=19456,t=2,p=1$...  # output of: gmountie genpass
volumes:
  - name: shared
    path: /srv/shared        # the folder to expose
```

```bash
gmountie serve -c server.yaml
```

> **Note:** `password_hash` must be a `$argon2id$` PHC string — the server rejects plaintext at startup. Generate one with `gmountie genpass`.

### 3. Mount it from a client

Use the shorthand form — the password is prompted interactively (no echo):

```bash
mkdir -p ~/mnt/shared
gmountie mount admin@your-server.example:9449/shared ~/mnt/shared
# Password: (enter the password printed on first server run)

# List what the server exposes before mounting:
gmountie ls admin@your-server.example:9449

# ...and use the mount like any other folder:
ls ~/mnt/shared
```

The password comes from `--password`, then the config file, then `$GMOUNTIE_AUTH_PASSWORD`, then an interactive no-echo prompt. The old flag form still works: `gmountie mount ~/mnt/shared -s host:9449 -n shared -u admin`.

Reads and writes now flow to the server, are cached locally, and the mount rides out network blips. For background mounts, TOFU TLS pinning, and a full `client.yaml` reference, see **[docs.gmountie.dev](https://docs.gmountie.dev)**.

## Features ✨

**Fast**
- gRPC **streaming** Read/Write with a tunable frame size — no message-size ceiling on large files
- A **persistent client cache** for attributes, directory listings, and file data — in memory and on disk
- **Readahead** and **write coalescing** for sequential I/O, **compound** metadata batching to cut round-trips, plus an optional **writeback** mode and **Snappy** compression for high-latency links

**Resilient**
- **Sessions + idempotent RPCs**: retries and reconnects are safe and invisible; file handles opened before a blip stay valid after it
- **Push-based invalidation** (a server `Subscribe` stream) keeps clients coherent — close-to-open consistency across multiple clients

**Simple & observable**
- One binary: `gmountie serve`, `gmountie mount`, `gmountie ls`, `gmountie config show`, and more
- TLS with auto-generated cert + TOFU pinning; argon2id-hashed credentials; per-user volume ACLs
- Prometheus metrics, health/readiness endpoints, and structured logs

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

- [Quick Start](docs/quickstart.mdx)
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
