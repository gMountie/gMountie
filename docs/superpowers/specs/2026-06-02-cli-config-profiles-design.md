# gMountie CLI Config Profiles — Design

**Date:** 2026-06-02
**Branch:** `worktree-config-profiles`
**Status:** Approved design, pre-implementation

## Problem

`gmountie mount` is either typed in full every time
(`gmountie mount admin@host:9449/shared /mnt/work`) or driven by a single per-role
`client.yaml` via `-c`. A user who mounts the same few remotes regularly (work, home, a
NAS) has no first-class way to save a named target — connection **and** tuning — and
reuse it. The single default `client.yaml` holds exactly one target, so juggling several
means hand-managing several `-c <path>` files with no discovery and no ergonomics.

Two supporting facts shape the design:

- A profile does not need a new schema. The client `Config`
  (`pkg/client/config/config.go`) already models everything a target needs — server,
  auth, cache, FUSE, RPC, TLS — and `config show --effective` (added in the prior CLI-UX
  PR) can already render a resolved client config. A profile can simply **be** a client
  config file.
- The client config already has a `mount:` block with `volume` and `path`
  (`pkg/client/config/mount.go`, `SingleMountConfig`), but `gmountie mount` ignores them
  today — it reads only `raw_ids` (`cmd/commands/mount.go:189`) and derives the volume
  solely from the shorthand spec or `-n` (`mount.go:153-174`). So a profile that names a
  volume needs that field wired into mount resolution.

A secondary gap: reusing a saved config safely means **not** baking a plaintext password
into it. Today the only non-interactive password sources are the inline `auth.password`
field and `$GMOUNTIE_AUTH_PASSWORD` (`mount.go:81-113`); there is no file or
password-manager integration.

## Goals

- Name a target once and mount it by name: `gmountie mount --profile work /mnt/work`.
- A profile carries connection **and** tuning (per-profile cache path/size, FUSE/RPC
  knobs, TLS pin) — i.e. a full client config.
- Keep secrets out of the profile when wanted: load the password from a **file** or a
  **command** (password manager / vault), while still allowing inline or prompt.
- Discovery: list profiles and shell-complete `--profile` names.
- No protocol or server changes. Precedence stays "CLI/env override the config."

## Non-goals (this PR)

- **Positional alias** (`gmountie mount work /mnt`, sshfs-style). Decided flag-only for
  v1; the positional sugar can be added later on top of the same machinery.
- **Profile management commands** (`profile add/edit/rm`). A profile is a YAML file the
  user edits directly; only `profile list` ships now.
- **OS keyring** (libsecret/keychain) — needs CGO/platform code this repo deliberately
  dropped (the desktop UI was removed). `password_command` covers vault/keychain via the
  user's own CLI (`secret-tool`, `op`, `pass`).

## Design

### What a profile is

A profile is a standard client config file at:

```
$XDG_CONFIG_HOME/gmountie/profiles/<name>.yaml      (default ~/.config/gmountie/profiles/)
```

It uses the exact `client.yaml` schema, so it already works with `-c`, `config show`, and
`config show --effective`. A profile may specify `mount.volume` (and optionally
`mount.path`) so the target's volume — and an optional default mountpoint — travel with
the profile.

Per-file (one file per profile) rather than a single `profiles.yaml` map: each profile is
then a valid standalone config, listing is a directory scan, and there is zero new
parsing — `ParseConfig` is reused as-is.

### Invocation: the `--profile` flag

A new `--profile <name>` flag (short `-P` where that letter is free on the command) is
added to the client commands that read a client config: `mount`, `ls`, and `config show`.

- `--profile <name>` resolves to `<config-dir>/profiles/<name>.yaml` and uses it as the
  config source, exactly as if `-c <that-path>` had been passed.
- `--profile` and `-c` are **mutually exclusive** (error if both are set) — they both name
  the config source.
- `<name>` is validated: it must match `^[A-Za-z0-9._-]+$` (no path separators, no `..`),
  so it can only ever resolve inside the profiles directory.
- A missing profile produces a clear error that **lists the available profile names** (a
  scan of the profiles dir), e.g.
  `profile "wrok" not found in ~/.config/gmountie/profiles (available: home, work)`.

### Precedence (unchanged model)

`--profile` only selects the base config. The existing order is preserved:

```
explicit CLI flags / shorthand spec  >  profile (or -c) file  >  GMOUNTIE_* env defaults  >  built-in defaults
```

So `gmountie mount --profile work /mnt --volume other -v` mounts the `work` profile but
overrides the volume and enables verbose.

### Volume and mountpoint resolution

`mount` gains profile-aware resolution for the two values a profile can now supply:

- **Volume:** after the existing shorthand/`-n` resolution, fall back to the parsed
  `cfg.Mount` (`SingleMountConfig.Volume`). Final order: shorthand spec → `-n` flag →
  `mount.volume` from the profile/config. The existing "volume name is required" check
  (`mount.go:173-175`) still fires if none of these supplied one.
- **Mountpoint:** stays a per-invocation positional. If omitted, fall back to
  `mount.path` from the profile/config; error if neither is present. (Lets
  `gmountie mount --profile work` work when the profile pins a mountpoint.)

This requires relaxing `SingleMountConfig` validation: `Path` and `Volume` change from
always-`required` to optional at the config layer (`pkg/client/config/mount.go`), with the
final requirement enforced at the command (where it already is). The mount block remains
optional overall (nil when absent), so existing configs are unaffected.

### Secrets: file and command, inline still allowed

Two new optional fields are added to the client auth config (alongside the existing
`username`/`password`): `password_file` and `password_command`. The password resolution
chain (today in `mount.go:resolveAuth`, also used by `ls`) becomes, first non-empty wins:

```
--password flag
  > auth.password_command   (run via `sh -c`, capture stdout, trim trailing newline; non-zero exit = error)
  > auth.password_file       (read file, trim trailing newline; refuse if group/world-readable)
  > auth.password            (inline plaintext — supported, discouraged)
  > $GMOUNTIE_AUTH_PASSWORD
  > interactive no-echo prompt
```

- `password_file` also honors a `$GMOUNTIE_AUTH_PASSWORD_FILE` env twin (Docker/systemd
  secrets convention).
- `password_command` runs through `sh -c` so pipes/managers work
  (`pass show gmountie/work`, `op read op://vault/gmountie/work`, `secret-tool lookup ...`,
  or simply `cat /path`). It is the user's own config, so this is intentional execution,
  not injection.
- Resolution is centralized in one helper so `mount` and `ls` share identical behavior.
- `config show` already redacts `password`/`password_hash`/`key_pem`; `password_file` and
  `password_command` are paths/commands, not secrets, so they are shown as-is.

A profile is therefore safe to commit/share with no secret in it (just a
`password_command` or `password_file`), or convenient with an inline password, or
zero-stored (prompt) — the user picks per profile.

### Discovery

- **`gmountie profile list`** — a new `profile` parent command with a `list` subcommand
  that scans the profiles dir and prints each profile name, plus a one-line summary
  (its `server.address:port` and `mount.volume`, when present). Prints
  `No profiles in <dir>.` when empty.
- **Shell completion** — `RegisterFlagCompletionFunc` for `--profile` on `mount`/`ls`/
  `config show`, returning profile names from the dir, so
  `gmountie mount --profile <TAB>` completes.

### `config show --profile`

`config show` accepts `--profile <name>` as an alternative to `--config`, resolving to the
profile path. Combined with the existing `--effective`,
`gmountie config show --profile work --effective` renders the profile's fully resolved
config (defaults + env merged, secrets redacted) — the natural "what will this profile
actually do?" inspector.

## Components / files to change

- `pkg/common/config/paths.go` — add `GetProfilesDir()` / `GetProfilePath(name)` and the
  `^[A-Za-z0-9._-]+$` name validation; helpers to list profile names.
- `pkg/client/config/auth.go` — add `PasswordFile` (`password_file`) and
  `PasswordCommand` (`password_command`) to the client auth config.
- `pkg/client/config/mount.go` — relax `SingleMountConfig.Path`/`Volume` to optional.
- `cmd/commands/profileflag.go` (new) — shared `--profile` flag registration, name→path
  resolution, the mutually-exclusive-with-`-c` check, the missing-profile error, and the
  completion function.
- `cmd/commands/password.go` (new or extend `passread.go`) — the centralized password
  resolver (`command` → `file` → inline → env → prompt), shared by `mount`/`ls`.
- `cmd/commands/mount.go` — apply `--profile`; wire `mount.volume`/`mount.path`; use the
  shared password resolver.
- `cmd/commands/ls.go` — apply `--profile`; use the shared password resolver.
- `cmd/commands/configshow.go` — accept `--profile`.
- `cmd/commands/profile.go` (new) — `gmountie profile` + `profile list`.
- `docs/cli-cheatsheet.md`, `docs/client/config.md` — document `--profile`, profiles dir,
  `password_file`/`password_command`, and `profile list`.

## Error handling

- `--profile` + `-c` together → explicit "use one of --profile or --config" error.
- Invalid profile name (path separators / `..`) → rejected before any filesystem access.
- Missing profile → error listing available names.
- `password_file` unreadable or group/world-readable → clear error (refuse rather than
  silently proceed); `password_command` non-zero exit → error including stderr.
- Existing remediation wrapping (`remediate(...)`) is preserved for connection failures.

## Testing

Unit (no FUSE, run locally + in CI):
- Profile name validation (accepts `a-z0-9._-`; rejects `/`, `..`, empty) and name→path
  resolution under a temp `XDG_CONFIG_HOME`.
- `--profile` + `-c` mutual exclusion; missing-profile error lists available names.
- Password resolver precedence: command > file > inline > env > prompt; `password_file`
  trims newline and refuses lax perms; `password_command` captures stdout and surfaces a
  non-zero exit.
- Volume resolution precedence: shorthand/`-n` override `mount.volume`; `mount.volume`
  used when CLI omits it; "volume required" still fires when nothing supplies it.
- `config show --profile [--effective]` renders the resolved profile with secrets redacted
  (reuses the prior redaction tests).
- `profile list` output on a populated and an empty profiles dir.

E2e (real FUSE, on the kubevirt VM and in CI):
- Write a profile file, `gmountie mount --profile <name> <mountpoint>` against the in-process
  test server, assert the volume mounts and basic I/O works, then unmount — proving the
  profile drives a real mount end to end.

## Out of scope (future)

- Positional alias (`gmountie mount work /mnt`) layered on `--profile`.
- `profile add/edit/rm` management commands.
- OS keyring integration.
