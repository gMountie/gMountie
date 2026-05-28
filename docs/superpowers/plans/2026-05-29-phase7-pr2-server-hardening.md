# Phase 7 PR 2 — Server Hardening Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Replace cleartext credential storage, bind ops endpoints to loopback by default, gate gRPC reflection behind a flag, and expose four DoS limits — all of the "tighten the server's surface" knobs land together because they share config + interceptor machinery.

**Architecture:** A new `pkg/common/passhash` package owns argon2id PHC encode/verify (used by both `gmountie genpass` CLI and the auth service). `BasicAuthConfigUser.Password` is renamed to `PasswordHash` and parsed strictly — a non-`$argon2id$` prefix at startup is a fatal config error pointing at `gmountie genpass`. The ops server gains `auth` config (`type: basic|none`); `auth: none` is allowed only on loopback bind, enforced at startup. `server.grpc.reflection: false` is the new default; existing dev setups opt in. Four DoS knobs (`max_recv_message_size`, `max_concurrent_streams`, `max_connection_idle`, `max_connection_age`) flow through `pkg/server/grpc.NewServer`'s option list.

**Tech Stack:** `golang.org/x/crypto/argon2`, `crypto/rand`, `crypto/subtle` (constant-time hash compare), cobra CLI, gRPC server options.

**Reference:** the Phase 7 brainstorm spec (pruned on ship; see `docs/design/security-and-transport.md`).3 (passwords), §3.4 (ops), §3.5 (reflection + DoS).

---

## File Structure

**Create:**
- `pkg/common/passhash/argon2id.go` — `Hash(password string) (string, error)` (returns PHC), `Verify(hash, password string) (bool, error)`, `IsHashed(s string) bool` (cheap prefix check used at config-load time).
- `pkg/common/passhash/argon2id_test.go`.
- `cmd/commands/genpass.go` + test — cobra subcommand reading from stdin (twice, no echo) and printing the PHC string to stdout.
- `pkg/server/grpc/limits.go` — small helper that builds the `grpc.ServerOption` slice for the four DoS knobs from `config.LimitsConfig`.
- `pkg/server/ops/auth.go` — middleware that wraps the ops mux with basic-auth (constant-time compare).
- `pkg/server/ops/auth_test.go`.

**Modify:**
- `pkg/server/config/auth.go` — `BasicAuthConfigUser.Password` → `PasswordHash`; viper unmarshal validates prefix.
- `pkg/server/service/auth.go` — verify via `passhash.Verify` (was direct string equality).
- `pkg/server/config/server.go` — add `Ops OpsConfig` (`Addr`, `Auth{Type, Users}`), `GRPC GRPCConfig` (`Reflection bool`, `Limits LimitsConfig{MaxRecvMessageSize, MaxConcurrentStreams, MaxConnectionIdle, MaxConnectionAge}`). Default `Ops.Addr` = `"127.0.0.1:9090"`.
- `pkg/server/app.go` — propagate ops auth into `ops.NewServer`; enforce "auth: none requires loopback bind"; default-population for new fields.
- `pkg/server/ops/server.go` — `NewServer` accepts an optional `*basicAuth` mux wrapper.
- `pkg/server/grpc/server.go` — gate `reflection.Register` on cfg flag; add the DoS-limits options.
- `cmd/commands/serve.go` (or first-run config writer) — first-run default config writes a hashed admin password + comment `# CHANGE ME — run 'gmountie genpass'`.

---

## Task Granularity

### Task 1: `pkg/common/passhash` — argon2id PHC

**Files:** `pkg/common/passhash/argon2id.go`, `pkg/common/passhash/argon2id_test.go`

- [ ] **Failing tests** in a testify `Argon2idSuite`:
  - `TestHash_RoundTripsThroughVerify` — `Hash("hunter2")` returns a string starting with `$argon2id$v=19$m=65536,t=3,p=4$`; `Verify(h, "hunter2")` returns `(true, nil)`; `Verify(h, "wrong")` returns `(false, nil)`.
  - `TestHash_RandomSaltDifferentEachCall` — two `Hash("same")` calls return DIFFERENT strings (salt is per-call). Both Verify against `"same"`.
  - `TestVerify_RejectsMalformedHash` — `Verify("not-a-hash", "x")` returns an error.
  - `TestIsHashed_Cheap` — `IsHashed("$argon2id$...")` → true; `IsHashed("plaintext")` → false; no allocation, no crypto.
- [ ] **Implement** with `golang.org/x/crypto/argon2`'s `IDKey(password, salt, time, mem, threads, keyLen)`. Parameters: `m=64*1024 (KiB), t=3, p=4, keyLen=32`. Salt is 16 random bytes (`crypto/rand`). PHC format encoding via stdlib `encoding/base64.RawStdEncoding` (no padding) for both salt and hash. Verify parses the PHC, recomputes the hash with the parsed params, compares with `subtle.ConstantTimeCompare`.
- [ ] **Commit:** `feat(common/passhash): argon2id PHC encode + verify`

### Task 2: `gmountie genpass` CLI subcommand

**Files:** `cmd/commands/genpass.go`, `cmd/commands/genpass_test.go`, `cmd/commands/root.go`

- [ ] **Failing test** (cobra fresh-root pattern; mirror `fingerprint_test.go`): when the test pipes a password into the command's stdin twice, the command prints the resulting PHC to stdout and exits 0; the PHC verifies against the original password via `passhash.Verify`.
- [ ] Edge cases tested:
  - Mismatched re-entry → exits non-zero with `passwords do not match`.
  - Empty password → exits non-zero with `password required`.
- [ ] **Implement** with `golang.org/x/term` for `term.ReadPassword(fd)` when stdin is a TTY, falling back to `bufio.Scanner` reading raw lines when not (so tests can pipe — `os.Stdin` is not a TTY in tests). Both reads must use the same path so test pipes and interactive use share the same code.
- [ ] Register subcommand in `cmd/commands/root.go`.
- [ ] **Commit:** `feat(cmd): gmountie genpass subcommand`

### Task 3: Rename Password → PasswordHash + verify via argon2id

**Files:** `pkg/server/config/auth.go`, `pkg/server/service/auth.go`, `pkg/server/service/auth_test.go`, `pkg/server/config/auth_test.go`, plus the first-run default-config writer in `pkg/server/config/server.go` or `pkg/server/app.go`.

- [ ] **Failing tests:**
  - `pkg/server/config/auth_test.go` — `TestRejectsCleartextPassword`: viper-loaded config with `password: hunter2` (or `password_hash: hunter2`) returns a load error containing `password_hash must be a $argon2id$ PHC string; run 'gmountie genpass'`.
  - Update existing `pkg/server/service/auth_test.go` cases that today seed `Password: "pass"` to seed `PasswordHash: <PHC>`. Add a positive `TestAuthorize_AcceptsCorrectPasswordViaHash` and a negative `TestAuthorize_RejectsWrongPassword`.
- [ ] **Rename** the field `BasicAuthConfigUser.Password` → `PasswordHash` (and `mapstructure:"password_hash"`). Add a load-time check via `passhash.IsHashed` at config-parse time — fail-closed.
- [ ] **Verify path:** `pkg/server/service/auth.go` swaps the direct string equality for `passhash.Verify(user.PasswordHash, given)`. On Verify error (malformed PHC) fail-closed.
- [ ] **First-run default config:** when `serve` writes a default config on first run, it must call `passhash.Hash("admin")` and write the result with a `# CHANGE ME — run 'gmountie genpass'` comment.
- [ ] **Commit:** `feat(server): basic-auth credential storage uses argon2id PHC`

### Task 4: Ops endpoint loopback default + optional auth + non-loopback startup guard

**Files:** `pkg/server/config/server.go`, `pkg/server/ops/server.go`, `pkg/server/ops/auth.go`, `pkg/server/ops/auth_test.go`, `pkg/server/app.go`.

- [ ] **Failing tests:**
  - `pkg/server/ops/auth_test.go` — basic-auth middleware: missing header → 401 with `WWW-Authenticate: Basic`; wrong password → 401; right password → 200 and the wrapped handler ran. Constant-time compare verified by uniform timing (don't try to test timing; just assert correctness).
  - `pkg/server/config/server_ops_test.go` — viper parses the new `server.ops` block (`addr`, `auth.type`, `auth.users`); default `addr` is `127.0.0.1:9090`; non-loopback addr + `auth.type: none` returns a load error.
- [ ] **Add config**: `OpsConfig{Addr string; Auth OpsAuthConfig{Type string; Users []BasicAuthConfigUser}}`. Mode `none|basic`. Default `Type` is `none` (works only on loopback). Default `Addr` is `127.0.0.1:9090`.
- [ ] **Implement** `ops/auth.go` middleware (`func basicAuth(handler http.Handler, users map[string][]byte) http.Handler`). The users map is `username → password-hash bytes`; the middleware extracts `Authorization: Basic ...`, base64-decodes, splits on `:`, looks up the user, calls `passhash.Verify`. Constant-time on the hash compare.
- [ ] **Wire it** into `ops.NewServer` — accept an optional `BasicAuth` (nil when `Type == none`); if non-nil, wrap the mux.
- [ ] **Enforce at startup** in `pkg/server/app.go`: `Type == none && !isLoopback(Addr)` → fatal error pointing at the spec. `auth.type: basic` requires non-empty `Users`.
- [ ] **Commit:** `feat(server/ops): loopback bind default + optional basic-auth`

### Task 5: gRPC reflection opt-in + DoS limits

**Files:** `pkg/server/config/server.go`, `pkg/server/grpc/server.go`, `pkg/server/grpc/limits.go`, `pkg/server/grpc/server_test.go`.

- [ ] **Failing tests:**
  - `TestReflectionOptIn` — server built with `GRPC.Reflection: false` (default) — make a real `grpc.ServerReflectionInfo` request via a bufconn dial; expect `codes.Unimplemented`.
  - With `GRPC.Reflection: true` — same call returns the service list.
  - `TestLimits_AppliedAsServerOptions` — capture the options passed to `grpc.NewServer` (an option-accumulator test fake or just inspect the side effect: build a server, dial it, send a payload over the limit, expect `ResourceExhausted`).
- [ ] **Add config**: `GRPCConfig{Reflection bool; Limits LimitsConfig}`. `LimitsConfig{MaxRecvMessageSize int (bytes), MaxConcurrentStreams uint32, MaxConnectionIdle time.Duration, MaxConnectionAge time.Duration}`. Defaults: 16 MiB / 256 / 5m / 0 (0 = unlimited, sensible for long-lived sessions).
- [ ] **Wire limits** via `pkg/server/grpc/limits.go` `BuildOptions(cfg) []grpc.ServerOption`. Use `grpc.MaxRecvMsgSize`, `grpc.MaxConcurrentStreams`, `grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle, MaxConnectionAge})`.
- [ ] **Gate reflection**: in `NewServer`, only `reflection.Register(s.server)` when `cfg.GRPC.Reflection` is true. The default is `false`.
- [ ] **Commit:** `feat(server/grpc): reflection opt-in + DoS limits (recv/streams/idle/age)`

### Task 6: VM e2e for startup behavior

**Files:** `test/e2e/api/hardening_test.go`

- [ ] **Failing tests:**
  - `TestServeRejectsCleartextPassword` — write a server config with `auth.users[0].password: hunter2` (the old key OR a non-PHC `password_hash`); invoke `gmountie serve --config <path>`; expect non-zero exit + stderr contains `gmountie genpass`.
  - `TestServeRejectsOpsAuthNoneOnNonLoopback` — config with `server.ops.addr: 0.0.0.0:9090, auth.type: none`; expect startup error containing `loopback`.
  - `TestReflectionDisabledByDefault` — start a normal test server (default config); dial as the reflection client; expect `Unimplemented`. Sanity test against the existing api fixture which doesn't enable reflection.
  - `TestServerEnforcesMaxRecvMessageSize` — send an oversized gRPC payload (e.g., a chunky `Compound` or oversized `WriteRequest`); expect `ResourceExhausted`. If shaping a real oversized request is fiddly, skip this case with a TODO — the unit test in T5 already covers the option being applied.
- [ ] Run on VM; expect all PASS (or one SKIP for the over-size payload if shaping it is too gnarly).
- [ ] **Commit:** `test(e2e): server hardening startup + reflection + DoS-limit guards`

---

## Self-Review

- **Spec coverage:** §3.3 (passwords, including `genpass`), §3.4 (ops + loopback default + optional auth), §3.5 (reflection opt-in + four DoS knobs).
- **No placeholders:** every step has files + behavior + a commit message.
- **Type consistency:** `passhash.Hash/Verify/IsHashed` named the same way at all 5 call sites (CLI, config load, auth service, ops auth middleware, first-run config writer).
- **Migration path:** there is none. The breakage is explicit and discoverable (config-load error pointing at `gmountie genpass`). Per project memory, gMountie does not design for backwards compatibility.

## Execution Handoff

Subagent-driven per task. Each subagent gets the relevant Task block + scene-setting context. T1 → T2 → T3 → T4 → T5 → T6. T1 unblocks T2 + T3 + T4 (all need passhash); the rest are mostly independent of one another but the dependency on T1 makes serial execution cleanest.
