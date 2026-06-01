# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

gMountie is a network filesystem built on FUSE (via `github.com/hanwen/go-fuse/v2`) and gRPC. A server (`gmountie serve`) exposes configured local directories as named "volumes"; a client (`gmountie mount`) mounts a volume locally via FUSE and proxies syscalls to the server over gRPC. It ships as a single `gmountie` binary (server + single-volume CLI client) plus the importable `pkg/...` library — no desktop UI.

The server (`gmountie serve`) is Linux-only; the CLI client (`gmountie mount`/`ls`) also builds and runs on macOS (via macFUSE / FUSE-T) — see the `//go:build linux || darwin` tags in `cmd/commands/`.

## Commands

The repo uses [Task](https://taskfile.dev) (`go-task`) as the entrypoint for everything via `Taskfile.yaml` at the root.

CLI / server (root Taskfile):
- `task test` — `go test -failfast -v ./...`
- `task test:coverage` — coverage run; filters out `internal/mocks`, `test/`, and `pkg/proto` from `coverage.out`
- `task lint` — runs `golangci-lint` via `go run` (uses `.golang-ci.yaml`)
- `task build` — `goreleaser build --snapshot --clean`
- `task gen:grpc` — regenerates Go stubs from `api/proto/*.proto` (requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`)
- `task gen:mocks` — wipes `internal/mocks` and regenerates via mockery (config in `.mockery.yml`)

Run a single test: `go test -v -run TestName ./pkg/server/service/...` (Task has no single-test target).

E2E tests live in `test/e2e/` and spin up a real server + FUSE mount in-process (see `test/e2e/utils`). The `fs/` suite includes fio-based benchmarks and requires `fio` installed.

## Architecture

### Wire protocol
`api/proto/*.proto` (`common`, `file`, `fs`, `volume`) defines four gRPC services. Generated Go lives in `pkg/proto/` — regenerate with `task gen:grpc` after any `.proto` change. The server uses Snappy compression as a custom codec (`pkg/server/grpc/snappy`).

### Server (`pkg/server/`)
Layered as `controller → service → io`:
- `controller/` — gRPC handlers (`fs.go`, `file.go`, `volume.go`) implementing the proto services. They look up a volume by name on each request and translate FUSE return codes to gRPC status.
- `service/` — `VolumeService` resolves volume name → filesystem; `AuthService` handles `none` / `basic` auth. `VolumeService` accepts a middleware chain (`io.Middleware`).
- `io/` — `LocalFilesystem` wraps `pathfs.NewLoopbackFileSystem` from go-fuse. `io/middleware/` contains pieces like `AssumeUserMiddleware`, which is auto-enabled in `app.go` only when running as root on Linux (it uses `setfsuid`/`setfsgid` per request).
- `grpc/` — server bootstrap, auth interceptors, Prometheus metrics (`server.metrics: true` enables `/metrics`).
- Entry point: `pkg/server/Start(cfg)` builds `AppContext`, registers the three gRPC controllers, and starts serving.

### Client (`pkg/client/`)
One mount model, built on go-fuse:
- `mount.SingleVolumeMounter` — mounts one remote volume at a given path (used by `gmountie mount`).
- `io/` — `pathfs.FileSystem` implementation that translates FUSE ops into gRPC calls. Uses `avast/retry-go` for retries on transient failures.
- `grpc/factory.go` — builds the gRPC client (TLS, auth, snappy) from a `config.Config`.

### Config
Server and client configs both go through `pkg/common/config` (Viper + `go-playground/validator`). Default paths come from `adrg/xdg`. Env vars use the `GMOUNTIE_` prefix. `gmountie serve` writes a default config on first run if none is found.

### Mocks
`task gen:mocks` regenerates `internal/mocks/` from `.mockery.yml`. Tests import these as `go.gmountie.dev/gmountie/internal/mocks/...`. Don't hand-edit anything under `internal/mocks/` — it's blown away on every regen. The coverage task explicitly strips this directory from the profile.

## Conventions worth knowing

- The module path is `go.gmountie.dev/gmountie` (a vanity import, not a GitHub URL), so internal imports look like `go.gmountie.dev/gmountie/pkg/...`.
- Logging: use `go.gmountie.dev/gmountie/pkg/utils/log` (`log.Log`, a named zap logger) rather than `slog` or `fmt.Println` directly.
- Errors: wrap with `github.com/pkg/errors` (`errors.Wrap`) — already pervasive in the codebase.
- gRPC requests are volume-scoped: most RPCs carry a `volume` field and the server uses it to look up the correct `FileSystem` on every call. New RPCs should follow the same pattern rather than holding session state.
- `scripts/start-slow-loopback.sh` / `stop-slow-loopback.sh` use `tc netem` to throttle the loopback interface for realistic perf testing — handy when investigating client retry/timeout behavior.
