# Phase 7 PR 3 — Identity Tightening (per-user volume ACL + mTLS) Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development.

**Goal:** Restrict which volumes a principal may touch (per-user ACL, fail-closed option), and add mTLS as a first-class auth scheme (client cert CN/SAN = principal). Final PR of the Phase 7 security trio → internet-deployable v1.

**Architecture:** ACL enforcement lives in ONE place — a new `VolumeService.PrincipalCanAccess(ctx, volume) error` called at the top of `BindIdentity` (the single chokepoint every FS op already funnels through) and inside `List`/`WhoAmI`. The ACL reads the authenticated principal from `principal.FromContext` and the per-user `Volumes` list + `auth.default_allow`. mTLS adds an `mtlsAuthService` (dispatched by `auth.type: mtls`) that extracts the principal from the verified peer certificate (CN, or first SAN if CN empty); the cryptographic verification happens at the TLS layer (`ClientAuth: RequireAndVerifyClientCert` + `ClientCAs` from the already-reserved `server.tls.client_ca_file`). The client presents its cert via the already-reserved `cert_file`/`key_file`.

**Tech Stack:** Go `crypto/tls` peer-cert extraction (`google.golang.org/grpc/peer` + `credentials.TLSInfo`), existing principal/ctx plumbing, gRPC auth interceptor, testify, VM FUSE e2e (VM is back: kernel 6.8, Go 1.26.2).

**Reference:** `docs/superpowers/specs/2026-05-29-phase7-security-hardening-design.md` §3.6 (ACL), §3.1/§3.2 (mTLS server/client), decision #6 (mTLS principal = cert CN; SAN if CN empty).

---

## File Structure

**Modify:**
- `pkg/server/config/auth.go` — `BasicAuthConfigUser.Volumes []string`; top-level `auth.default_allow bool` (default true); new `AuthConfigTypeMTLS`.
- `pkg/server/service/volume.go` — `PrincipalCanAccess(ctx, volume) error` on the interface + impl; call it at top of `BindIdentity`; change `List()` → `List(ctx)` and filter; store the per-volume ACL map at construction.
- `pkg/server/service/auth.go` — `mtlsAuthService` + factory dispatch for `AuthConfigTypeMTLS`.
- `pkg/server/grpc/auth.go` — Unary interceptor unchanged (principal still stashed from `UserDetails.Username`); mTLS service fills Username from the cert.
- `pkg/server/app.go` — when `auth.type: mtls`, wire `ClientCAs` + `ClientAuth: RequireAndVerifyClientCert` into the server `tls.Config` from `server.tls.client_ca_file`.
- `pkg/server/controller/volume.go` — `List` handler passes ctx; WhoAmI path (`pkg/server/controller/session.go`) calls `PrincipalCanAccess` before resolving identity.
- `pkg/client/tls/verify.go` — load `cert_file`/`key_file` into `tls.Config.Certificates` (present client cert for mTLS).
- `pkg/client/grpc/factory.go` — thread the client cert paths into `clienttls.Config`.

**Create:**
- `pkg/server/service/acl_test.go` — ACL matrix.
- `pkg/server/service/auth_mtls_test.go` — mtlsAuthService cert→principal extraction.
- `test/e2e/api/acl_test.go` — wire-level ACL deny.
- `test/e2e/fs/mtls_test.go` — mTLS round-trip over a real mount (VM).

---

## Tasks

### Task 1: Per-user volume ACL

**Files:** `pkg/server/config/auth.go`, `pkg/server/service/volume.go`, `pkg/server/service/acl_test.go`, `pkg/server/controller/volume.go` (+ `session.go` WhoAmI), and every `List()` caller.

- [ ] **Failing tests** `acl_test.go` (testify `ACLSuite`), building a `VolumeServiceImpl` with a config of 2 volumes (`photos`, `team`) and these auth users:
  - `alice` → `volumes: [photos, team]`
  - `bob` → `volumes: [team]`
  - `carol` → no `volumes` key (uses default policy)
  Cases, parameterized over `default_allow`:
  - `default_allow: true`: alice→both OK; bob→team OK, photos DENIED; carol→both OK (default).
  - `default_allow: false`: carol→both DENIED (fail-closed); alice/bob unchanged.
  - principal absent from ctx → DENIED (no anonymous volume access).
  - `PrincipalCanAccess` returns a `codes.PermissionDenied` gRPC status error (check via `status.FromError`).
- [ ] **Config**: add `Volumes []string `mapstructure:"volumes"`` to `BasicAuthConfigUser`; add `DefaultAllow *bool `mapstructure:"default_allow"`` on `BasicAuthConfig` (pointer so "unset" defaults to true; nil → true). Document: empty/unset `volumes` = default policy; explicit `[]` = no access.
- [ ] **Implement** `PrincipalCanAccess(ctx, volume)`: read `principal.FromContext`; if absent → PermissionDenied. Look up the principal's `volumes`. If they have an explicit list → membership decides. If no list → `default_allow` decides. The ACL map (`principal → []volume | nil`) + the default-allow bool are built once in `NewVolumeService` from `cfg.Auth` (the VolumeService already takes the full `*config.Config`).
  - **Important — fold into BindIdentity:** call `PrincipalCanAccess` at the very top of `BindIdentity` so all 21 FS-op call sites are covered without edits. Also call it in `List` and in the WhoAmI resolver path.
- [ ] **`List(ctx)`**: change signature to take ctx; filter the returned volumes through `PrincipalCanAccess` (drop denied ones rather than erroring — a user listing volumes sees only theirs). Update the one controller caller.
- [ ] **Non-mapped-auth caveat:** if `cfg.Auth` is not basic/mtls (e.g. a future type), `default_allow` governs and there's no per-user list. Keep the ACL builder tolerant of an auth config with no users.
- [ ] **Verify** unit + `go vet`. **Commit:** `feat(server): per-user volume ACL (default_allow + PrincipalCanAccess)`

### Task 2: mTLS auth scheme (server)

**Files:** `pkg/server/config/auth.go`, `pkg/server/service/auth.go`, `pkg/server/service/auth_mtls_test.go`, `pkg/server/app.go`.

- [ ] **Failing tests** `auth_mtls_test.go`:
  - `mtlsAuthService.Authorize` with a ctx carrying a `peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: [...]}}}` whose leaf CN is `alice` → returns `(true, &UserDetails{Username:"alice"}, nil)`.
  - CN empty but SAN `bob` present → Username `bob`.
  - No peer cert / no TLS info → `(false, …)` fail-closed.
  - (Build the test cert with a CN/SAN via `pkg/server/tls.Generate` extended, or `crypto/x509` directly in the test.)
- [ ] **Config**: add `AuthConfigTypeMTLS AuthConfigType = "mtls"`. An mTLS auth config reuses `BasicAuthConfigUser` for the `volumes:` ACL (no password_hash needed — validation must NOT require password_hash when type is mtls). The principal is matched by `Username` == cert CN.
- [ ] **Implement** `mtlsAuthService.Authorize(ctx, _)`: `peer.FromContext` → `credentials.TLSInfo` → `State.VerifiedChains[0][0]` (verified leaf) → CN or first DNS SAN. The TLS layer already verified the chain (ClientAuth below), so presence of a verified cert ⇒ trusted identity. Return Username = the extracted name. (Volume-level authorization is the ACL's job, called later in BindIdentity — Authorize just establishes identity.)
- [ ] **Factory**: dispatch `AuthConfigTypeMTLS` → `&mtlsAuthService{...}` in `NewAuthServiceFromConfig`.
- [ ] **Server TLS bootstrap** (`app.go`): when `auth.type: mtls`, require `server.tls.client_ca_file` (error if empty), load it into a `*x509.CertPool`, set `tlsCfg.ClientCAs` + `tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert`. (Mutually exclusive with `tls.disabled` — mTLS needs TLS; error if both.)
- [ ] **Verify** unit + vet. **Commit:** `feat(server): mTLS auth scheme — cert CN as principal`

### Task 3: mTLS client (present cert)

**Files:** `pkg/client/tls/verify.go`, `pkg/client/config/server.go` (fields exist), `pkg/client/grpc/factory.go`, `pkg/client/tls/verify_test.go`.

- [ ] **Failing test**: `clienttls.BuildConfig` with `CertFile`/`KeyFile` set loads them into `tls.Config.Certificates` (len 1); unset → empty Certificates.
- [ ] **Implement**: in `BuildConfig`, when both `CertFile` and `KeyFile` are non-empty, `tls.LoadX509KeyPair` and append to `Certificates`. Error if only one is set.
- [ ] **Thread** the two paths from `client/config.TLSConfig` through `factory.go` into `clienttls.Config` (the struct already carries them per PR1, just confirm they're passed).
- [ ] **Verify** unit + vet. **Commit:** `feat(client): present client cert for mTLS`

### Task 4: VM e2e — ACL deny + mTLS round-trip

**Files:** `test/e2e/api/acl_test.go`, `test/e2e/fs/mtls_test.go`, possibly a `test/e2e/utils` helper for an mTLS-configured context.

- [ ] **ACL e2e** (`acl_test.go`, in-process via AppTestingContext, no FUSE needed): two basic-auth principals on a 2-volume server with `default_allow: false`; principal `bob` granted only `team`. Over gRPC, `bob`'s GetAttr on `photos` → `reply.Status == EACCES` (or the call returns `PermissionDenied` — match how PrincipalCanAccess surfaces through the controller). `bob`'s List returns only `team`.
- [ ] **mTLS e2e** (`mtls_test.go`, real FUSE mount on the VM): server with `auth.type: mtls` + a test CA; client presents a cert with CN matching a granted user; mount succeeds and a basic read works. A client with a cert CN that has no volume grant → denied. Add a `utils` helper that generates a CA + server cert + client cert and wires both sides.
- [ ] **Run on VM** via `virtctl ssh ubuntu@vmi/gmountie-dev/gmountie-test -i ~/.ssh/id_rsa -t '-o StrictHostKeyChecking=no' -t '-o UserKnownHostsFile=/dev/null' -c '<cmd>'` after rsync. (See memory `feedback-vm-availability`.)
- [ ] **Commit:** `test(e2e): volume ACL deny + mTLS round-trip`

---

## Self-Review
- **Spec coverage:** §3.6 (ACL, default_allow, PrincipalCanAccess, List filter), §3.1/§3.2 (mTLS server ClientAuth + client cert), decision #6 (CN→SAN principal).
- **Type consistency:** `PrincipalCanAccess(ctx, volume) error` named identically across interface + impl + call sites; `default_allow` is `*bool` (nil→true) everywhere.
- **One-place enforcement:** ACL folded into `BindIdentity` so no FS-op path can bypass it; `List`/`WhoAmI` call it explicitly.

## Execution Handoff
Subagent-driven, T1→T4. T1 (ACL) and T2/T3 (mTLS) are independent; T4 depends on all. The VM is back, so T4 gets real FUSE coverage (don't pre-emptively pivot to in-process for the mTLS-mount case).
