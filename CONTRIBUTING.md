# Contributing to gMountie

Thanks for your interest in gMountie! 🎉 Contributions of every kind are welcome — bug reports, feature ideas, documentation, and code. This guide gets you set up and explains how we work.

By participating, please be respectful and constructive: assume good faith, keep discussion technical, and help make this a friendly place to build.

## Ways to contribute

- 🐛 **Report a bug** — open a [bug report](https://github.com/gMountie/gMountie/issues/new?template=bug_report.yml).
- 💡 **Suggest a feature** — open a [feature request](https://github.com/gMountie/gMountie/issues/new?template=feature_request.yml), or start a [Discussion](https://github.com/gMountie/gMountie/discussions) if you want to talk it through first.
- 🔒 **Report a vulnerability** — see [SECURITY.md](SECURITY.md). Do **not** use public issues for security problems.
- 📝 **Improve the docs** — they live under [`docs/`](docs/) and publish to [docs.gmountie.dev](https://docs.gmountie.dev).
- 🔧 **Send code** — see below.

## Project layout

gMountie is a FUSE + gRPC network filesystem. A `gmountie serve` process exposes directories as **volumes**; a `gmountie mount` client mounts one over FUSE and proxies syscalls to the server over gRPC. The module path is `go.gmountie.dev/gmountie`, so internal imports look like `go.gmountie.dev/gmountie/pkg/...`.

Start with **[docs/design/architecture.md](docs/design/architecture.md)** for the layered server (`controller → service → io`), the client FUSE backend, and the wire protocol. The desktop app lives under `ui/` (Wails 3).

> Server-side features run on **Linux only**; the CLI client builds for Linux and macOS. The desktop UI needs CGO + `libwebkit2gtk-4.1-dev` / `gtk+-3.0`.

## Development setup

You'll need **Go 1.2x** and [**Task**](https://taskfile.dev) (`go-task`), which is the entrypoint for everything. Then:

```bash
git clone https://github.com/gMountie/gMountie && cd gMountie
task setup          # installs git hooks and dev tooling (protoc plugins, mockery, linters)
```

Some tasks need extra tools: `protoc` for regenerating gRPC stubs, and `fuse3` (`/dev/fuse`) to run the FUSE end-to-end tests. `task setup:tools` / `task setup:system` help bootstrap these.

## Build, test, lint

```bash
task build          # snapshot build via goreleaser
task test           # go test -failfast ./...
task test:race      # race detector
task test:coverage  # coverage profile (mocks / generated code excluded)
task lint           # golangci-lint
```

Run a single test directly:

```bash
go test -v -run TestName ./pkg/server/service/...
```

End-to-end tests in `test/e2e/` spin up a real server + FUSE mount in-process; the `fs/` suite includes `fio`-based benchmarks and needs `fio` installed.

### Generated code — don't hand-edit

- **gRPC stubs** (`pkg/proto/`) — regenerate with `task gen:grpc` after any change to `api/proto/*.proto`.
- **Mocks** (`internal/mocks/`) — regenerate with `task gen:mocks`; the directory is wiped on every run.
- **Wails TS bindings** (`ui/frontend/src/bindings/`) — regenerate via the `ui:` build tasks after changing exported Go controller methods.

## Conventions

- **Errors:** wrap with `github.com/pkg/errors` (`errors.Wrap`) — it's pervasive in the codebase.
- **Logging:** use the named zap logger at `pkg/utils/log` (`log.Log`), not `slog`/`fmt.Println`.
- **gRPC requests are volume-scoped:** most RPCs carry a `volume` field; the server looks up the right filesystem per call. New RPCs should follow that pattern rather than holding session state.
- **Naming:** write the project name **`gMountie`** in prose, **`gmountie`** for the binary / module / packages / files / URLs, and **`GMOUNTIE_`** for env vars. Avoid `GMountie` / `Gmountie`.

## Submitting changes

1. **Branch** off `master` and keep your change focused — one logical change per PR.
2. **Commit messages** follow Conventional Commits: `feat(client): …`, `fix(server): …`, `docs: …`, `test(e2e): …`.
3. **Before opening a PR**, make sure `task test` and `task lint` pass, and regenerate any affected stubs/mocks/bindings.
4. **Open the PR** against `master`, fill in the template, and link any related issue (`Fixes #123`).
5. CI runs tests, lint, and security scans; please keep it green.

A maintainer will review as soon as they can — thanks for helping make gMountie better. 🙌
