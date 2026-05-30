# gMountie CLI/Config UX Overhaul — Design

**Date:** 2026-05-30
**Branch:** `worktree-cli-ux-overhaul`
**Status:** Approved design, pre-implementation

## Problem

A code-grounded UX/DX evaluation found gMountie's CLI functional but more verbose and
manual than the tools it replaces (sshfs, NFS, rclone). The friction concentrates in
three places: a first-run server that does not actually start, a verbose multi-flag
mount with the password on the command line, and missing lifecycle/discovery
conveniences. Specific, verified findings:

- **First-run `serve` is dead on arrival.** The generated default config
  (`cmd/commands/serve.go:26-37`) has `auth` but **no `volumes`** section, and `volumes`
  is `required` (`pkg/server/config/config.go:27`), so the server fails validation on the
  very first run.
- **Fixed default credential.** First run hashes the literal password `"admin"`
  (`serve.go:42`) — a known credential, dangerous the moment the server is reachable.
- **Loopback-only default bind** (`serve.go:27`, `address: 127.0.0.1`) for a tool whose
  entire purpose is remote access; quickstart examples show `0.0.0.0`, so docs and
  behavior disagree.
- **Password on the command line.** `mount` requires `--password` (`mount.go:103-104`),
  landing credentials in shell history and `ps`. The mount command builds its own
  `viper.New()` and validates the password *before* `ParseConfig` runs env binding
  (`mount.go:103` vs `pkg/client/config/config.go:97`), so `GMOUNTIE_AUTH_PASSWORD` does
  not reliably satisfy the check today.
- **Verbose mount syntax.** `gmountie mount /mnt -s host:9449 -t basic -u u -p p -n vol`
  vs `sshfs user@host:/path /mnt`.
- **No background mode.** `mount` blocks on Ctrl+C (`mount.go:158-161`); no daemonize.
- **No discovery.** No way to list a server's volumes, though `VolumeService.List`
  already exists (`api/proto/volume.proto:17`) and `Client.Volume()` exposes it
  (`pkg/client/grpc/client.go:205`).
- **No startup path validation.** `volumes[].path` is never checked for existence
  (`pkg/server/config/config.go` only does `required,dive`); a bad path surfaces as a
  cryptic error at first I/O, not at startup.
- **Errors lack remediation**, and global flag help (`--config`/`--verbose`) is bare.

## Goals

Make the happy path braindead-simple: a zero-edit first-run server, a one-line mount
that reads like sshfs, credentials that never touch the command line, and the basic
lifecycle/discovery commands users expect. No backwards-compatibility constraints (user
controls both ends); document any breaks in release notes.

## Non-goals

- OIDC/JWT auth, per-connection byte-rate limits (already deferred elsewhere).
- Automating the TLS fingerprint *exchange* (server-assisted trust). We only make the
  manual TOFU pin easier to copy, not automatic.
- Any change to the desktop UI (`ui/`, `pkg/ui/`) — out of scope per project phase.

## Locked decisions

| Decision | Choice |
|---|---|
| First-run server posture | Bind `0.0.0.0` **and** generate a random password printed once |
| Mount shorthand | `[user@]host[:port]/volume MOUNTPOINT` |
| Background mode | Both `--daemon` re-exec **and** a templated systemd unit |
| Delivery | One worktree + one PR (`worktree-cli-ux-overhaul`) |

## Design

### 1. Frictionless first-run server — `cmd/commands/serve.go`, `pkg/server/`

**1a. Real default volume.** The first-run config exposes one working volume named
`shared` pointing at an auto-created data directory under `$XDG_DATA_HOME/gmountie/shared`
(resolved via `adrg/xdg`, consistent with existing path handling in `pkg/common/config`).
`serve` creates the directory (0700) before writing the config so validation and the
first mount both succeed with zero edits. The config comment explains how to add or
change volumes.

**1b. Random password, printed once.** Replace `passhash.Hash("admin")` with a generated
passphrase: crypto-random (`crypto/rand`), human-transcribable (≈ 6 words from a small
embedded wordlist, or a 24-char base32 string — implementer picks one and documents it).
The argon2id hash goes into the config; the **plaintext is printed once** to stderr with
a clearly delimited notice ("shown only now — save it") plus the username and a pointer
to `gmountie genpass` for rotation. The plaintext is never written to disk. Generation
lives in a small helper (e.g. `pkg/common/passhash` or a new `genpass`-adjacent function)
so it is unit-testable independent of the command.

**1c. Bind `0.0.0.0`.** Default template uses `address: 0.0.0.0` with an inline comment
noting it accepts remote connections and how to restrict it. Safe because the default now
ships a random password + auto-generated TLS.

**1d. Startup volume-path validation.** Add validation (in `server.Start`, or a
`Validate()` on the server config invoked there) that every `volumes[].path` exists and
is a directory. On failure, return an error naming the offending volume and path with a
remediation hint. The auto-created default dir means first run still passes. This is a
behavior change for misconfigured servers (fail fast at startup instead of at first I/O);
note it in release notes.

### 2. Frictionless mount — `cmd/commands/mount.go`

**2a. Positional shorthand.** Accept `gmountie mount [user@]host[:port]/volume MOUNTPOINT`.
`Args` becomes 1 **or** 2 positionals:
- 2 args: first is the spec, second is the mountpoint.
- 1 arg: mountpoint only (current behavior; flags supply server/volume — back-compat).

A dedicated parser (e.g. `parseMountSpec(string) (mountSpec, error)` in its own file)
splits `[user@]host[:port]/volume` into username/address/port/volume. Rules: `user@` is
optional; `:port` is optional and defaults to 9449; the volume is everything after the
first `/`; missing host or volume is an error with an example in the message. Explicit
flags (`--server`, `--volume`, `--username`) override values parsed from the spec. The
parser is pure and table-tested.

**2b. Password resolution order.** Resolve the basic-auth password as: `--password` flag
→ `GMOUNTIE_AUTH_PASSWORD` env → interactive TTY prompt → error. The prompt reuses the
no-echo terminal reader pattern from `cmd/commands/genpass.go` (factor the reader into a
shared helper so both commands use it and it can be stubbed in tests). Resolution happens
**before** the validation check, and only prompts when stdin is a TTY; in a
non-interactive context with no flag/env, it returns a clear error (so `--daemon` and
scripts fail loudly rather than hang). `--password` is retained but its help text warns
it is visible in `ps`/history.

**2c. Daemonize.** Add `--daemon`. When set, the process re-execs itself detached
(rclone-style): the parent spawns a child with `--daemon` stripped and an internal marker
flag/env set, waits until the child signals "mount ready" (e.g. child writes a ready byte
to an inherited pipe, or parent polls the mountpoint), then exits 0; the child runs the
existing foreground mount loop with stdout/stderr redirected to a log file (default under
`$XDG_STATE_HOME/gmountie/`, overridable). The re-exec is behind an injectable seam
(an interface/func var for "spawn child" + "signal ready") so tests exercise the
orchestration without actually forking. Password must be resolved via flag/env in daemon
mode (no TTY); enforced by 2b.

**2d. systemd unit + docs.** Ship a templated unit file (e.g.
`packaging/systemd/gmountie-mount@.service` and `gmountie-serve.service`) parameterized by
volume/mountpoint, using `GMOUNTIE_AUTH_PASSWORD` via an `EnvironmentFile`. Document the
unit and the `nohup` fallback.

**2e. Error remediation.** Wrap the connect/auth/mount errors (`mount.go:129-153`) with
actionable hints, matched on the underlying condition: unreachable → "server unreachable
at `<addr>` — check the address/port and that `gmountie serve` is running"; auth failed →
"authentication failed — check username/password (server uses argon2id hashes via
`gmountie genpass`)"; volume not found → "volume `<name>` not found — run `gmountie ls
<host>` to list available volumes". Implemented as a small classifier helper, unit-tested
against representative wrapped errors.

### 3. Discovery & visibility — new commands

**3a. `gmountie ls [user@]host[:port]`.** Connects with the same config/shorthand/auth
resolution as `mount`, calls `Client.Volume().List(ctx, &VolumeListRequest{})`, and prints
each volume (name; and any fields the reply carries). Reuses `NewClientFromConfig`,
`parseMountSpec` (host portion), and the password resolver. No proto/server change. Errors
use the same remediation classifier.

**3b. `gmountie config show [--for server|client]`.** Loads and merges config + flags +
env and prints the effective configuration with secrets redacted (`auth.password`,
`password_hash`). Makes the precedence (flag > file > env > default) debuggable. `--for`
selects which schema to render; defaults inferred from which config file is present.

### 4. Help & docs

- Expand `--config` / `--verbose` descriptions on the root command
  (`cmd/commands/root.go`) to state accepted file (server/client YAML) and precedence.
- `gmountie fingerprint` emits a copy-paste-ready `server.tls.expected_fingerprint: …`
  YAML snippet (in addition to the raw fingerprint) for the client config.
- Docs (Docusaurus `website/`, plus `README.md`): rewrite the quickstart to the new
  zero-edit happy path (serve prints password → `gmountie ls` → `gmountie mount
  user@host/shared /mnt`); explain `password_hash`/argon2 and `genpass`; document TLS
  auto-gen + TOFU pinning + the systemd unit. Durable transport/security architecture
  stays in `docs/design/security-and-transport.md`; quickstart/how-to is the published
  site. No perf data in docs.

## Components & boundaries

| Unit | Responsibility | Depends on | Tested by |
|---|---|---|---|
| `parseMountSpec` | pure parse of `[user@]host[:port]/volume` | — | table test |
| password resolver | flag → env → TTY prompt → error | term reader (stubbable) | precedence test |
| term reader helper | no-echo stdin read | `golang.org/x/term` | shared by genpass/mount |
| first-run config builder | random pw + default volume + dir create | `passhash`, `xdg` | unit test (no real serve) |
| server config validator | volume path exists + is dir | `os` | unit test (temp dirs) |
| daemon orchestrator | re-exec child, await ready | injectable spawn seam | orchestration test (no fork) |
| error classifier | wrapped error → remediation string | — | unit test |
| `ls` / `config show` cmds | thin command wiring | client, parser, resolver | bufconn / in-process |

## Testing

testify suites (per project convention), TDD-first. Coverage:

- `serve`: default-config generation produces a random (non-`admin`) password, includes a
  `shared` volume, creates the data dir; path-validation rejects a missing/zero dir with a
  clear error.
- `mount`: `parseMountSpec` table (`user@host:port/vol`, `host/vol`, default port,
  flag-override, malformed inputs); password precedence (flag > env > prompt > error);
  daemon orchestration via the spawn seam (parent waits for ready, child runs mount, no
  real fork); error classifier mapping.
- `ls` / `config show`: against an in-process/bufconn server; `config show` redaction
  asserted.
- FUSE-mounting e2e remains env-gated (`GMOUNTIE_E2E_*`) per existing CI conventions;
  real-mount validation runs on the kubevirt VM (sandbox/GoLand cannot mount).

## Risks & mitigations

- **0.0.0.0 default exposure** — mitigated by random password + auto-TLS shipped in the
  same change; documented prominently.
- **Daemon re-exec portability** — Linux-only target today; the spawn seam keeps it
  testable and contained. If re-exec proves fragile, the systemd unit is the
  production-recommended path regardless.
- **Startup path validation breaking existing deployments** — intentional fail-fast;
  called out in release notes; the auto-created default keeps first-run green.

## Delivery

Single worktree (`worktree-cli-ux-overhaul`) → one PR. Conventional-commit subjects with
short descriptive bodies; no Co-Authored-By/Signed-off trailers. Mocks (if any new
interfaces) regenerated via `task gen:mocks`, never hand-edited. Release notes document
the behavior breaks (default bind, random password, startup path validation).
