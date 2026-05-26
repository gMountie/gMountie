<div align="center">
  <img class="logo" src="assets/wordmark.png" alt="gMountie" width="360"/>
  <h3>Your Filesystem's Best Friend</h3>
  <p><i>Because remote filesystems shouldn't feel so... remote</i></p>
</div>

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## What's This All About? 🤔

Ever wished you could access your remote files as easily as if they were right there on your computer? That's exactly what gMountie does! It's like having a really efficient courier service for your files, but instead of waiting days for delivery, everything happens instantly.

Built with modern tech (FUSE and gRPC) and a lot of ❤️, gMountie makes remote filesystems feel local. No more complicated mounting procedures or slow network transfers - just smooth, efficient file access.

## Features That'll Make You Smile 😊

- **Lightning Fast**: gRPC streaming plus a persistent client-side cache — after the first read, the bytes don't cross the network again
- **Resilient**: Sessions and idempotent RPCs keep mounts and open files alive across reconnects and server restarts
- **Rock Solid**: Extensive unit and end-to-end test coverage
- **Simple CLI**: `gMountie serve` and `gMountie mount` — a desktop app is in progress
- **Authenticated**: Built-in basic auth; TLS transport is on the [roadmap](docs/roadmap.md)
- **FUSE-native**: Real filesystem semantics via FUSE (Linux is the supported target today)

## Installation 📦

Detailed installation instructions and a quick start are in our [documentation](https://docs.gmountie.dev).

## Documentation 📚

Full documentation lives at **[docs.gmountie.dev](https://docs.gmountie.dev)**. Highlights:

- [Quick Start](docs/quickstart.md)
- [Architecture & Protocol](docs/design/architecture.md)
- [Caching & Consistency](docs/design/caching-and-consistency.md)
- [Performance](docs/design/performance.md)
- [Roadmap](docs/roadmap.md)
- Configuration & CLI reference — [server](docs/server/config.md) · [client](docs/client/config.md)

## Architecture 🏗️

gMountie uses a client-server architecture:

```
Client (Your Computer) <-> gRPC <-> Server (Remote System)
↓                                   ↓
FUSE Mount                          Real Filesystem
```

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


