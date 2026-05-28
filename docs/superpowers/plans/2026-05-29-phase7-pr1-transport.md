# Phase 7 PR 1 — Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** every gRPC connection is server-TLS-terminated. Zero-config first start auto-generates a cert (SSH host-key pattern); the client trusts it via TOFU or an explicit pin. `gmountie fingerprint` exposes the cert for client-side config. Test fixture exercises TLS transparently.

**Architecture:** New `pkg/server/tls` package owns cert lifecycle (generate, load, persist, fingerprint). Server `NewServer` builds `tls.Config` from `pkg/server/config.TLSConfig` via that helper. Client `pkg/client/tls` package owns verify-mode policy: strict chain, TOFU (`$XDG_STATE_HOME/gmountie/known_hosts`), explicit pin, or insecure. `pkg/client/grpc/factory.go` builds the client `tls.Config` from `client/config.TLSConfig`. `BasicAuthCredentials.RequireTransportSecurity() = true`. `test/e2e/utils.NewAppTestingContext` generates a self-signed cert per context and pins it on the client side so every existing test exercises TLS.

**Tech Stack:** Go stdlib `crypto/tls`, `crypto/x509`, `crypto/ecdsa`, `crypto/elliptic`; `google.golang.org/grpc/credentials`; XDG via `github.com/adrg/xdg` (already a dep); cobra CLI.

**Reference:** the Phase 7 brainstorm spec (pruned on ship; see `docs/design/security-and-transport.md`).1 (server TLS), §3.1.1 (auto-gen), §3.1.2 (fingerprint CLI), §3.2 (client modes + TOFU). Decisions #1, #7, #9, #10, #11 locked.

---

## File Structure

**Create:**
- `pkg/server/tls/cert.go` — `Generate(host string) (cert, key []byte, err)`, `Load(certPath, keyPath string) (tls.Certificate, []byte certPEM, err)`, `LoadOrGenerate(certPath, keyPath string, host string) (tls.Certificate, certPEM, fingerprint, err)`, `Fingerprint(certPEM []byte) string` (returns `SHA256:<base64-raw>` form matching SSH).
- `pkg/server/tls/cert_test.go` — testify suite.
- `pkg/client/tls/verify.go` — `Mode` type (`verify` / `tofu` / `insecure`), `BuildConfig(cfg config.TLSConfig, endpoint string) (*tls.Config, error)` constructs the right `tls.Config` for each mode, including `VerifyPeerCertificate` for TOFU + expected-fingerprint pinning.
- `pkg/client/tls/knownhosts.go` — `KnownHosts` struct backed by `$XDG_STATE_HOME/gmountie/known_hosts`; `Lookup(endpoint string) (fingerprint string, ok bool)`, `Pin(endpoint, fingerprint string) error` with O_CREAT|O_APPEND|O_EXCL semantics under a flock so concurrent clients don't race.
- `pkg/client/tls/{verify,knownhosts}_test.go`.
- `cmd/commands/fingerprint.go` — cobra subcommand.
- `test/e2e/utils/tls.go` — helper that generates an ephemeral cert per test context and returns both the server `tls.Config` and the matching client `expected_fingerprint`.

**Modify:**
- `pkg/server/config/server.go` — add `TLSConfig` (cert_file, key_file, client_ca_file, min_version, disabled).
- `pkg/server/grpc/server.go` `NewServer` — build server creds via `pkg/server/tls`, pass via `grpc.Creds()`. Refuse non-loopback bind when `tls.disabled`. Log fingerprint at startup.
- `pkg/server/app.go` — `LoadOrGenerate` early during startup so the path is on disk before the listener binds.
- `pkg/client/config/server.go` — add `TLSConfig` (ca_file, verify, expected_fingerprint, server_name, cert_file, key_file).
- `pkg/client/grpc/factory.go` — build `tls.Config` via `pkg/client/tls` and pass `grpc.WithTransportCredentials(...)`.
- `pkg/client/grpc/auth.go` `BasicAuthCredentials.RequireTransportSecurity()` → `true`.
- `pkg/client/grpc/client.go` — uncomment the TLS dial path.
- `cmd/commands/root.go` — register `fingerprint`.
- `test/e2e/utils/app.go` — `NewAppTestingContext` spins up the TLS helper, wires server creds + client expected_fingerprint.

**Note on dev mode:** when `server.tls.disabled: true` AND `bind` matches `127.*` / `[::1]`, the server starts plaintext. Anything else is a startup error.

---

## Task Granularity

### Task 1: `pkg/server/tls` package — cert helpers

**Files:** `pkg/server/tls/cert.go`, `pkg/server/tls/cert_test.go`

- [ ] **Write failing tests** in a testify suite `CertSuite`:
  - `TestGenerateSelfSignedECDSA` — `Generate("test.example.com")` returns a parseable ECDSA P-256 cert valid 10 years with the expected CN and SAN.
  - `TestFingerprintStable` — same cert input → same `SHA256:<base64>` output. Fingerprint format matches `SHA256:` followed by 43 base64 chars (no padding) — that's the SSH form.
  - `TestLoadFromDisk` — write a known cert to a tempdir, `Load` returns matching parsed cert + raw PEM bytes.
  - `TestLoadOrGenerate_FirstStartCreatesFiles` — empty tempdir, `LoadOrGenerate(certPath, keyPath, "host")` creates both files with mode 0600 on key, 0644 on cert; returns the same fingerprint on disk and in return value.
  - `TestLoadOrGenerate_SubsequentLoad` — second call returns the SAME fingerprint (does not regenerate).
- [ ] **Implement** `Generate`, `Load`, `LoadOrGenerate`, `Fingerprint` in `cert.go`. `LoadOrGenerate` uses `os.OpenFile(O_WRONLY|O_CREATE|O_EXCL, 0600)` for the key so a concurrent serve-start can't clobber. If EEXIST → fall through to Load.
- [ ] **Verify** all tests PASS, `go vet` clean.
- [ ] **Commit:** `feat(server/tls): cert lifecycle helper (generate, load, fingerprint)`

### Task 2: `gmountie fingerprint` CLI subcommand

**Files:** `cmd/commands/fingerprint.go`, `cmd/commands/fingerprint_test.go`, `cmd/commands/root.go`

- [ ] **Failing test** (testscript or direct cobra invocation): when `server.tls.cert_file` is set in test config, `fingerprint` prints `SHA256:...\n` and exits 0. When no cert exists and config has no `cert_file`, exits non-zero with the helpful error message from the spec.
- [ ] **Implement** subcommand: resolves the same path the server would (config `cert_file` wins; otherwise `$XDG_STATE_HOME/gmountie/server.crt`); calls `Load` + `Fingerprint` from `pkg/server/tls`. `--verbose` adds subject, issuer, NotBefore, NotAfter on separate lines.
- [ ] **Register** in `cmd/commands/root.go`.
- [ ] **Commit:** `feat(cmd): gmountie fingerprint subcommand`

### Task 3: Server TLS bootstrap

**Files:** `pkg/server/config/server.go`, `pkg/server/grpc/server.go`, `pkg/server/app.go`, plus tests.

- [ ] **Failing test** (in `pkg/server/grpc/server_test.go` or a new TLS-specific test): build a server with a generated cert; assert `grpc.Creds()` was passed (use a fake `ServerOption` accumulator or inspect via behavior — a dial without TLS gets the right rejection).
- [ ] **Add** `TLSConfig` struct to server config with `CertFile`, `KeyFile`, `ClientCAFile` (left unused in PR 1; see §3.1 — mTLS is PR 3), `MinVersion` (default `"1.3"`), `Disabled` (default `false`).
- [ ] **Wire** `NewServerAppContext` (or wherever cert load happens) to call `tls.LoadOrGenerate` BEFORE the listener binds. Log the fingerprint at INFO with `path` and `fingerprint` fields.
- [ ] **Build** `tls.Config` in `NewServer` with `MinVersion: tls.VersionTLS13`, `NextProtos: []string{"h2"}`, `Certificates: []tls.Certificate{cert}`. Pass `grpc.Creds(credentials.NewTLS(cfg))` to `grpc.NewServer`.
- [ ] **tls.disabled escape hatch**: when true AND bind is loopback (127.x or [::1] including via hostname resolution check), skip TLS setup. When true AND bind is non-loopback, return a startup error.
- [ ] **Verify** server package tests pass. Lint clean.
- [ ] **Commit:** `feat(server): TLS bootstrap with auto-gen cert`

### Task 4: Client TLS dial + verify modes

**Files:** `pkg/client/tls/verify.go`, `pkg/client/tls/knownhosts.go`, plus tests; `pkg/client/config/server.go`; `pkg/client/grpc/factory.go`; `pkg/client/grpc/auth.go`; `pkg/client/grpc/client.go`.

- [ ] **Failing tests** in `pkg/client/tls/*_test.go`:
  - `TestVerifyMode_ValidCert` — server cert chains to a CA bundle; dial succeeds.
  - `TestVerifyMode_WrongCN_Rejected`.
  - `TestTofuMode_FirstConnectPins` — empty `known_hosts`; first connect succeeds and writes the fingerprint; second connect with same cert succeeds.
  - `TestTofuMode_FingerprintChangedRejected` — pin stored, cert rotated → dial fails with the spec's error string.
  - `TestExpectedFingerprintMatch` / `TestExpectedFingerprintMismatch`.
  - `TestInsecureModeSkipsVerification` (also asserts a log line is emitted).
- [ ] **Implement** `verify.go` `BuildConfig` returning the right `*tls.Config` per mode. For `tofu` and `expected_fingerprint`, set `InsecureSkipVerify: true` on the tls.Config and run our OWN check inside `VerifyPeerCertificate` against the stored / configured fingerprint. (`tls.Config.VerifyPeerCertificate` is called BEFORE the default chain verify; with InsecureSkipVerify we replace the chain check with our pin.)
- [ ] **Implement** `knownhosts.go` with file format `<endpoint> <fingerprint>\n` (one line per host). Lookup is a linear scan (file is tiny). `Pin` opens with `O_WRONLY|O_CREATE|O_APPEND` and writes a single line under a `flock`.
- [ ] **Add** `TLSConfig` to client server config: `CAFile`, `Verify` (default `"verify"`), `ExpectedFingerprint`, `ServerName`, `CertFile`, `KeyFile` (last two unused in PR 1; mTLS is PR 3).
- [ ] **Wire** `factory.go` to build the TLS config and pass `grpc.WithTransportCredentials(...)`. Flip `BasicAuthCredentials.RequireTransportSecurity() → true`. Uncomment the TLS dial line in `client.go`.
- [ ] **Verify** all tests pass; lint clean.
- [ ] **Commit:** `feat(client): TLS dial with verify/tofu/insecure modes + fingerprint pin`

### Task 5: Test fixture spins up TLS

**Files:** `test/e2e/utils/tls.go` (new), `test/e2e/utils/app.go` (modify)

- [ ] **Failing state:** existing api + fs e2e tests now fail because the server now requires TLS and the test fixture still uses plaintext.
- [ ] **Add** `test/e2e/utils/tls.go` with `NewEphemeralTLS(t *testing.T, host string) (serverCertPEM, serverKeyPEM []byte, expectedFingerprint string)` that wraps `pkg/server/tls.Generate` + `Fingerprint`.
- [ ] **Modify** `NewAppTestingContext`: generate the cert; write it to a per-test tempdir; populate `server.tls.cert_file/key_file` in the test server config and `server.tls.expected_fingerprint` in the test client config.
- [ ] **Verify** every test in `./test/e2e/api/...` and `./test/e2e/fs/...` still passes on the VM. This is the canary that we haven't broken anything.
- [ ] **Commit:** `test(e2e): wire test fixture through TLS by default`

### Task 6: New e2e tests for the transport behavior

**Files:** `test/e2e/api/tls_test.go` (new)

- [ ] **Failing test** for each spec claim:
  - `TestServeAutoGeneratesCertOnFirstStart` — point `serve` at an empty `$XDG_STATE_HOME`; start, stop; assert files exist with the right modes; assert `fingerprint` subcommand returns the same fingerprint that startup logged.
  - `TestServeReusesExistingCert` — second start uses the same fingerprint (no regeneration).
  - `TestPlaintextClientRejected` — client with no TLS config dialing a TLS server fails (RequireTransportSecurity check).
  - `TestTofuPinsOnFirstConnect` — empty `known_hosts` → connect → fingerprint pinned → known_hosts file contains the entry.
  - `TestTofuRejectsRotatedCert` — pin stored, server cert regenerated → next dial fails with the configured error.
  - `TestExpectedFingerprintMatch` / `TestExpectedFingerprintMismatch` end-to-end.
- [ ] **Run on VM**: `ssh ubuntu@192.168.11.11 'cd /tmp/gm-p7t && go test -count=1 -v -timeout 120s -run TestTLSE2ESuite ./test/e2e/api/'`. All PASS.
- [ ] **Commit:** `test(e2e): TLS auto-gen, TOFU, and fingerprint pin end-to-end`

---

## Self-Review

- **Spec coverage:** §3.1 (server TLS), §3.1.1 (auto-gen), §3.1.2 (fingerprint), §3.2 (client modes), decisions #1, #7, #9, #10, #11. mTLS (#6) is PR 3, deliberately deferred.
- **No placeholders:** every step has the concrete file + behavior.
- **Type consistency:** server `TLSConfig` and client `TLSConfig` use the same field names where the semantics overlap (`CertFile`, `KeyFile`); fingerprint string format is `SHA256:<base64-raw>` everywhere.

## Execution Handoff

Subagent-driven per task. Each subagent gets the relevant Task block + scene-setting context.
