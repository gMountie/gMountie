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
- `task lint` — runs `golangci-lint` via `go run` (uses `.golangci.yaml`)
- `task build` — `goreleaser build --snapshot --clean`
- `task gen:grpc` — regenerates Go stubs from `api/proto/*.proto` (requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`)
- `task gen:mocks` — wipes `internal/mocks` and regenerates via mockery (config in `.mockery.yml`)

Run a single test: `go test -v -run TestName ./pkg/server/service/...` (Task has no single-test target).

E2E tests live in `test/e2e/` and spin up a real server + FUSE mount in-process (see `test/e2e/utils`). The `fs/` suite includes fio-based benchmarks and requires `fio` installed.

## Architecture

### Wire protocol
`api/proto/*.proto` (`common`, `file`, `fs`, `session`, `version`, `volume`) defines five gRPC services: `RpcFs`, `RpcFile`, `VolumeService`, `SessionService`, `VersionService`. Generated Go lives in `pkg/proto/` — regenerate with `task gen:grpc` after any `.proto` change. A Snappy codec (`pkg/common/grpc/snappy`) is available as an opt-in compressor (`rpc.compression: snappy`, default `none`).

### Server (`pkg/server/`)
Layered as `controller → service → io`:
- `controller/` — gRPC handlers (`fs.go`, `file.go`, `volume.go`, `session.go`, `version.go`) implementing the proto services. They resolve the session, apply idempotency, look up a volume by name on each request, and translate FUSE return codes to gRPC status.
- `service/` — `VolumeService` resolves volume name → filesystem, gated by the per-user ACL (`PrincipalCanAccess`) and bound to the caller's identity (`BindIdentity`); `AuthService` handles `basic` / `mtls` auth (session-scoped: the password is verified once per session, later RPCs authorize by `session_id`); `SessionManager` owns sessions, fd tables, and grace-period reap; `resolver_{squash,static,system}.go` implement the per-volume identity mapping modes.
- `io/` — `ConfinedLoopbackFileSystem` (loopback FS confined via `openat2(RESOLVE_BENEATH)`) plus the identity-bound FS wrappers in `bound_fs.go`, which assume the resolved principal's credentials per OS thread (`setfsuid`/`setfsgid`/`setgroups`) so the kernel enforces permissions. (The old `io/middleware/AssumeUserMiddleware` is gone — identity-bound filesystems subsumed it.)
- `grpc/` — server bootstrap, auth interceptors, Prometheus metrics, TLS (with leaf live-reload via `pkg/server/tls.Reloader`).
- Entry point: `pkg/server/Start(cfg)` builds `AppContext`, registers the five gRPC controllers (`GetGrpcServices`), and starts serving; an ops HTTP server (`/healthz`, `/readyz`, `/metrics`, `/ops/acl/reload`) binds loopback by default.

### Client (`pkg/client/`)
One mount model, built on go-fuse v2's `fs` (node) API:
- `mount.SingleVolumeMounter` — mounts one remote volume at a given path (used by `gmountie mount`).
- `io/` — `node.go` implements the go-fuse `fs.Node*` adapters, delegating every op to the `FileSystemBackend` interface; `BackendClient` (`backend_grpc.go`) is the gRPC implementation. Transient failures retry inside a windowed `retryOp` loop (`rpc.retry_window`, fresh per-attempt deadlines, session-change guard).
- `cache/` — `NewCachedBackend` decorates `FileSystemBackend` with the two-tier (memory + disk) cache and Subscribe-driven invalidation.
- `grpc/factory.go` — builds the gRPC client (TLS, auth, optional snappy) from a `config.Config`.

### Config
Server and client configs both go through `pkg/common/config` (Viper + `go-playground/validator`). Default paths come from `adrg/xdg`. Env vars use the `GMOUNTIE_` prefix. `gmountie serve` writes a default config on first run if none is found.

### Mocks
`task gen:mocks` regenerates `internal/mocks/` from `.mockery.yml`. Tests import these as `go.gmountie.dev/gmountie/internal/mocks/...`. Don't hand-edit anything under `internal/mocks/` — it's blown away on every regen. The coverage task explicitly strips this directory from the profile.

## Conventions worth knowing

- The module path is `go.gmountie.dev/gmountie` (a vanity import, not a GitHub URL), so internal imports look like `go.gmountie.dev/gmountie/pkg/...`.
- Logging: use `go.gmountie.dev/gmountie/pkg/utils/log` (`log.Log`, a named zap logger) rather than `slog` or `fmt.Println` directly.
- Errors: wrap with `github.com/pkg/errors` (`errors.Wrap`) — already pervasive in the codebase.
- gRPC requests are volume-scoped: most RPCs carry a `volume` field and the server uses it to look up (and ACL-check + identity-bind) the correct `FileSystem` on every call; fd-carrying and mutating RPCs also carry `session_id` / `request_id`. New RPCs should follow the same pattern.
- `scripts/start-slow-loopback.sh` / `stop-slow-loopback.sh` use `tc netem` to throttle the loopback interface for realistic perf testing — handy when investigating client retry/timeout behavior.
