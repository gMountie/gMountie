# Server-Side Revocation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an operator revoke a user or a single lost device without restarting the server — by hot-reloading the per-volume ACL + a durable cert-serial blocklist over an ops endpoint, reaping matching sessions, and rejecting blocked serials at the TLS handshake and per RPC.

**Architecture:** Two atomically-swappable snapshots (ACL in `VolumeService`, serial blocklist in a new `RevocationStore` on `AppContext`). `POST /ops/acl/reload` re-reads the config file, validates it, swaps both snapshots, then reaps sessions whose serial is now blocked or whose principal lost all volume access. A blocked serial is rejected on three paths: TLS handshake (`VerifyPeerCertificate`), per-RPC (auth interceptor), and reap (close open fds). The blocklist lives in config (`auth.revoked_serials`) so a restart re-reads it and stays fail-closed.

**Tech Stack:** Go, `atomic.Pointer`, `crypto/x509`, gRPC, Viper config, testify suites, mockery (`task gen:mocks`).

**Design source:** `docs/superpowers/specs/2026-05-31-oss-revocation-design.md`.

---

## Conventions for this plan

- Run commands from the worktree root:
  `/home/john/git/gMountie/gMountie/.claude/worktrees/oss-revocation`.
- Tests are testify suites (methods on a suite struct), never bare `func TestX`.
- Never hand-edit `internal/mocks/`; regenerate with `task gen:mocks`.
- Commit after each task. Conventional-commit subject + short body; no
  Co-Authored-By / Signed-off-by trailers.
- A canonical **serial key** is used everywhere: `SerialKey(n) = n.Text(16)`
  (lowercase hex, no separators). Config entries in any format (`AB:CD`, `0xabcd`,
  `abcd`) are normalized through `ParseSerialKey` to the same key, so a presented
  cert serial and a config blocklist entry can never miss on formatting.

## File structure

- `pkg/server/config/auth.go` — add `RevokedSerials []string` to `BasicAuthConfig`.
- `pkg/server/config/server.go` — add `OpsTLSConfig`, `OpsConfig.TLS`; allow
  `mtls` in `OpsAuthConfig.Type`.
- `pkg/server/config/config.go` — parse the new keys; add `Config.ConfigPath`.
- `pkg/server/config/reload.go` *(new)* — `ReloadFromFile(path) (*Config, error)`.
- `pkg/server/service/revocation.go` *(new)* — `RevocationStore`, `SerialKey`,
  `ParseSerialKey`.
- `pkg/server/service/auth.go` — add `VerifiedCertSerial(ctx) (string, bool)`.
- `pkg/server/service/volume.go` — `aclSnapshot` + `atomic.Pointer`, `ReloadAuth`.
- `pkg/server/service/session.go` — session stores serial; `Create` takes serial;
  new `ReapIf`.
- `pkg/server/controller/session.go` — thread the serial into `Create`.
- `pkg/server/ops/reload.go` *(new)* — `ReloadHandler` + the reap predicate.
- `pkg/server/ops/server.go` — extend `NewServer` (reload deps + optional mTLS).
- `pkg/server/grpc/server.go` + `pkg/server/grpc/auth.go` — `WithRevocation`,
  per-RPC serial check.
- `pkg/server/app.go` — build `RevocationStore`, `VerifyPeerCertificate` hook,
  wire ops server + grpc option.
- `cmd/commands/serve.go` — set `cfg.ConfigPath`.

---

### Task 1: Config — `auth.revoked_serials`

**Files:**
- Modify: `pkg/server/config/auth.go`
- Test: `pkg/server/config/auth_test.go` (suite already present — add a method)

- [ ] **Step 1: Write the failing test**

Add to `pkg/server/config/auth_test.go` (reuse the existing suite type in that
file; if the suite is named differently, attach the method to it):

```go
func (s *AuthConfigSuite) TestRevokedSerialsParsed() {
	cfg, err := LoadConfigFromString(`
server:
  tls:
    client_ca_file: /etc/ca.pem
auth:
  type: mtls
  revoked_serials: ["ab:cd", "0xEF01"]
  users:
    - username: alice
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`)
	s.Require().NoError(err)
	bac, ok := cfg.Auth.(*BasicAuthConfig)
	s.Require().True(ok)
	s.Equal([]string{"ab:cd", "0xEF01"}, bac.RevokedSerials)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/config/ -run TestRevokedSerialsParsed -v` (or the
suite runner, e.g. `-run TestAuthConfigSuite/TestRevokedSerialsParsed`).
Expected: FAIL — `bac.RevokedSerials` undefined.

- [ ] **Step 3: Implement**

In `pkg/server/config/auth.go`, add the field to `BasicAuthConfig`:

```go
// BasicAuthConfig is a struct that holds the configuration for the basic auth
type BasicAuthConfig struct {
	AuthConfigBase
	Users []BasicAuthConfigUser `validate:"required,dive"`
	// DefaultAllow controls access for principals that have no explicit volumes
	// list. nil (unset) is treated as true for backwards compatibility.
	DefaultAllow *bool `mapstructure:"default_allow"`
	// RevokedSerials is the cert-serial blocklist (any hex format; normalized
	// at load). Read at startup and on /ops/acl/reload — durable across restart,
	// so revocation is fail-closed. Empty for basic-auth deployments.
	RevokedSerials []string `mapstructure:"revoked_serials"`
}
```

Both `NewBasicAuthConfig` and `NewMTLSAuthConfig` already `v.Unmarshal(&conf)`,
so the new mapstructure field is populated automatically for both auth types.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/server/config/ -run TestRevokedSerialsParsed -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/config/auth.go pkg/server/config/auth_test.go
git commit -m "feat(config): auth.revoked_serials cert-serial blocklist field"
```

---

### Task 2: Config — ops mTLS, `ConfigPath`, and `ReloadFromFile`

**Files:**
- Modify: `pkg/server/config/server.go`, `pkg/server/config/config.go`
- Create: `pkg/server/config/reload.go`
- Modify: `cmd/commands/serve.go`
- Test: `pkg/server/config/reload_test.go` (new)

- [ ] **Step 1: Write the failing test**

Create `pkg/server/config/reload_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReloadSuite struct{ suite.Suite }

func TestReloadSuite(t *testing.T) { suite.Run(t, new(ReloadSuite)) }

const reloadYAML = `
server:
  ops:
    addr: 127.0.0.1:9090
    tls:
      cert_file: /etc/ops.crt
      key_file: /etc/ops.key
      client_ca_file: /etc/ops-ca.pem
    auth:
      type: mtls
auth:
  type: mtls
  revoked_serials: ["abcd"]
  users:
    - username: alice
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`

func (s *ReloadSuite) TestOpsTLSParsed() {
	cfg, err := LoadConfigFromString(reloadYAML)
	s.Require().NoError(err)
	s.Equal("/etc/ops.crt", cfg.Server.Ops.TLS.CertFile)
	s.Equal("/etc/ops.key", cfg.Server.Ops.TLS.KeyFile)
	s.Equal("/etc/ops-ca.pem", cfg.Server.Ops.TLS.ClientCAFile)
	s.Equal("mtls", cfg.Server.Ops.Auth.Type)
}

func (s *ReloadSuite) TestReloadFromFile() {
	dir := s.T().TempDir()
	path := filepath.Join(dir, "server.yaml")
	s.Require().NoError(os.WriteFile(path, []byte(reloadYAML), 0o600))

	cfg, err := ReloadFromFile(path)
	s.Require().NoError(err)
	s.Equal(path, cfg.ConfigPath)
	bac, ok := cfg.Auth.(*BasicAuthConfig)
	s.Require().True(ok)
	s.Equal([]string{"abcd"}, bac.RevokedSerials)
}

func (s *ReloadSuite) TestReloadFromFile_BadPath() {
	_, err := ReloadFromFile(filepath.Join(s.T().TempDir(), "nope.yaml"))
	s.Require().Error(err)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/config/ -run TestReloadSuite -v`
Expected: FAIL — `Ops.TLS`, `ReloadFromFile`, `ConfigPath` undefined.

- [ ] **Step 3a: Add the ops TLS struct + allow mtls**

In `pkg/server/config/server.go`, replace the `OpsAuthConfig` / `OpsConfig`
block:

```go
// OpsAuthConfig — Type is "none" (default), "basic", or "mtls".
type OpsAuthConfig struct {
	Type  string                `mapstructure:"type" validate:"omitempty,oneof=none basic mtls"`
	Users []BasicAuthConfigUser `mapstructure:"users"`
}

// OpsTLSConfig enables TLS (and, for type: mtls, client-cert verification) on
// the ops listener. Empty CertFile/KeyFile ⇒ plain HTTP (the default).
type OpsTLSConfig struct {
	CertFile     string `mapstructure:"cert_file"`
	KeyFile      string `mapstructure:"key_file"`
	ClientCAFile string `mapstructure:"client_ca_file"`
}

// OpsConfig controls the operational HTTP endpoint (/metrics, /healthz,
// /readyz, /version, /debug/pprof, /ops/acl/reload).
type OpsConfig struct {
	Addr string        `mapstructure:"addr"`
	Auth OpsAuthConfig `mapstructure:"auth"`
	TLS  OpsTLSConfig  `mapstructure:"tls"`
}
```

- [ ] **Step 3b: Parse the ops TLS keys + add ConfigPath**

In `pkg/server/config/config.go`, add `ConfigPath` to the `Config` struct:

```go
type Config struct {
	Server   *ServerConfig   `validate:"required"`
	Auth     AuthConfig      `validate:"required"`
	Volumes  []*VolumeConfig `validate:"required,dive"`
	Log      *log.LogConfig
	// ConfigPath is the file this config was loaded from, set by the loader
	// (serve.go / ReloadFromFile). Empty when built from a string/defaults.
	// Used by /ops/acl/reload to re-read the file. Not parsed from YAML.
	ConfigPath string `mapstructure:"-"`
}
```

In `parseOpsConfig` (same file), populate `TLS`:

```go
func parseOpsConfig(v *viper.Viper) OpsConfig {
	cfg := OpsConfig{
		Addr: v.GetString("server.ops.addr"),
		Auth: OpsAuthConfig{
			Type: v.GetString("server.ops.auth.type"),
		},
		TLS: OpsTLSConfig{
			CertFile:     v.GetString("server.ops.tls.cert_file"),
			KeyFile:      v.GetString("server.ops.tls.key_file"),
			ClientCAFile: v.GetString("server.ops.tls.client_ca_file"),
		},
	}
	if sub := v.Sub("server.ops.auth"); sub != nil {
		var users []BasicAuthConfigUser
		if err := sub.UnmarshalKey("users", &users); err == nil {
			cfg.Auth.Users = users
		}
	}
	return cfg
}
```

- [ ] **Step 3c: Add `ReloadFromFile`**

Create `pkg/server/config/reload.go`:

```go
package config

import (
	"os"

	"github.com/pkg/errors"
)

// ReloadFromFile re-parses the server config from path and records the path on
// the result (ConfigPath). Used at startup (so the running config knows its own
// source) and by POST /ops/acl/reload to pick up ACL + revoked_serials changes
// without a restart. Validation runs as in normal load; a bad file returns an
// error and the caller keeps the previous good config.
func ReloadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "read config file %q", path)
	}
	cfg, err := LoadConfigFromString(string(data))
	if err != nil {
		return nil, err
	}
	cfg.ConfigPath = path
	return cfg, nil
}
```

- [ ] **Step 3d: Set `ConfigPath` in serve.go**

In `cmd/commands/serve.go`, right after the existing
`cfg, err := serverConfig.LoadConfigFromString(cfgString)` (≈ line 140) and its
error check, add:

```go
		cfg.ConfigPath = configFile
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/server/config/ -run TestReloadSuite -v`
Expected: PASS. Also `go build ./cmd/...` — clean.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/config/server.go pkg/server/config/config.go pkg/server/config/reload.go pkg/server/config/reload_test.go cmd/commands/serve.go
git commit -m "feat(config): ops TLS block, Config.ConfigPath, ReloadFromFile"
```

---

### Task 3: `RevocationStore` + serial keys

**Files:**
- Create: `pkg/server/service/revocation.go`
- Test: `pkg/server/service/revocation_test.go` (new)

- [ ] **Step 1: Write the failing test**

Create `pkg/server/service/revocation_test.go`:

```go
package service

import (
	"math/big"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type RevocationSuite struct{ suite.Suite }

func TestRevocationSuite(t *testing.T) { suite.Run(t, new(RevocationSuite)) }

func (s *RevocationSuite) TestSerialKeyCanonical() {
	n := big.NewInt(0xABCD)
	s.Equal("abcd", SerialKey(n))
}

func (s *RevocationSuite) TestParseSerialKeyNormalizesFormats() {
	for _, in := range []string{"abcd", "ABCD", "ab:cd", "0xABCD", "AB:CD"} {
		key, ok := ParseSerialKey(in)
		s.Require().Truef(ok, "input %q", in)
		s.Equalf("abcd", key, "input %q", in)
	}
	_, ok := ParseSerialKey("nothex!")
	s.False(ok)
}

func (s *RevocationSuite) TestSetAndIsBlocked() {
	store := NewRevocationStore()
	s.False(store.IsBlocked(SerialKey(big.NewInt(0xABCD))))
	store.Set([]string{"AB:CD", "garbage"}) // garbage silently dropped
	s.True(store.IsBlocked(SerialKey(big.NewInt(0xABCD))))
	s.False(store.IsBlocked(SerialKey(big.NewInt(0x1234))))
	store.Set(nil) // empty reload clears the list
	s.False(store.IsBlocked(SerialKey(big.NewInt(0xABCD))))
}

func (s *RevocationSuite) TestConcurrentSetAndRead() {
	store := NewRevocationStore()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); store.Set([]string{"abcd"}) }()
		wg.Add(1)
		go func() { defer wg.Done(); _ = store.IsBlocked("abcd") }()
	}
	wg.Wait()
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/service/ -run TestRevocationSuite -v`
Expected: FAIL — `SerialKey`/`ParseSerialKey`/`NewRevocationStore` undefined.

- [ ] **Step 3: Implement**

Create `pkg/server/service/revocation.go`:

```go
package service

import (
	"math/big"
	"strings"
	"sync/atomic"
)

// SerialKey is the canonical key for a certificate serial: lowercase hex, no
// separators. Used identically for blocklist entries and presented certs so
// formatting can never cause a miss.
func SerialKey(n *big.Int) string {
	if n == nil {
		return ""
	}
	return n.Text(16)
}

// ParseSerialKey normalizes a config-supplied serial in any common hex format
// ("abcd", "AB:CD", "0xABCD") to a SerialKey. Returns ("", false) when the
// value is not valid hex.
func ParseSerialKey(s string) (string, bool) {
	clean := strings.ToLower(strings.TrimSpace(s))
	clean = strings.TrimPrefix(clean, "0x")
	clean = strings.ReplaceAll(clean, ":", "")
	if clean == "" {
		return "", false
	}
	n, ok := new(big.Int).SetString(clean, 16)
	if !ok {
		return "", false
	}
	return SerialKey(n), true
}

// RevocationStore holds the cert-serial blocklist as an atomically-swappable
// snapshot. Writers: the ops reload handler. Readers: the TLS handshake hook,
// the gRPC auth interceptor, and the session reaper. A reader always sees a
// fully-consistent map, never a half-updated one.
type RevocationStore struct {
	blocked atomic.Pointer[map[string]struct{}]
}

// NewRevocationStore returns a store with an empty blocklist.
func NewRevocationStore() *RevocationStore {
	r := &RevocationStore{}
	empty := make(map[string]struct{})
	r.blocked.Store(&empty)
	return r
}

// Set replaces the blocklist with the normalized serials. Unparseable entries
// are dropped. A nil/empty slice clears the list.
func (r *RevocationStore) Set(serials []string) {
	m := make(map[string]struct{}, len(serials))
	for _, s := range serials {
		if key, ok := ParseSerialKey(s); ok {
			m[key] = struct{}{}
		}
	}
	r.blocked.Store(&m)
}

// IsBlocked reports whether the given SerialKey is in the current blocklist.
// An empty key (no client cert) is never blocked.
func (r *RevocationStore) IsBlocked(key string) bool {
	if key == "" {
		return false
	}
	m := r.blocked.Load()
	_, ok := (*m)[key]
	return ok
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/server/service/ -run TestRevocationSuite -race -v`
Expected: PASS (including the `-race` concurrency test).

- [ ] **Step 5: Commit**

```bash
git add pkg/server/service/revocation.go pkg/server/service/revocation_test.go
git commit -m "feat(server): RevocationStore — atomic cert-serial blocklist"
```

---

### Task 4: `VerifiedCertSerial`

**Files:**
- Modify: `pkg/server/service/auth.go`
- Test: `pkg/server/service/auth_test.go` (add a method to the existing suite)

- [ ] **Step 1: Write the failing test**

Add to `pkg/server/service/auth_test.go`:

```go
func (s *AuthServiceSuite) TestVerifiedCertSerial() {
	leaf := &x509.Certificate{SerialNumber: big.NewInt(0xABCD)}
	ti := credentials.TLSInfo{State: tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{leaf}},
	}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: ti})

	key, present := VerifiedCertSerial(ctx)
	s.Require().True(present)
	s.Equal("abcd", key)

	// No client cert (basic-auth): absent.
	_, present = VerifiedCertSerial(context.Background())
	s.False(present)
}
```

Ensure the test file imports `crypto/tls`, `crypto/x509`, `math/big`,
`google.golang.org/grpc/credentials`, `google.golang.org/grpc/peer`,
`context` (mirror the imports already used by the mtls tests in that file).

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/service/ -run TestAuthServiceSuite/TestVerifiedCertSerial -v`
Expected: FAIL — `VerifiedCertSerial` undefined.

- [ ] **Step 3: Implement**

In `pkg/server/service/auth.go`, add after `VerifiedCertPrincipal` (and add
`"math/big"` is **not** needed — `SerialKey` lives in the same package):

```go
// VerifiedCertSerial returns the canonical SerialKey of the verified client
// cert's leaf and true when a verified client certificate is present. Mirrors
// VerifiedCertPrincipal: basic-auth connections (empty VerifiedChains) return
// ("", false). The session records this key so the reaper can match a blocked
// serial out of band; the handshake hook and interceptor compute it live.
func VerifiedCertSerial(ctx context.Context) (serialKey string, present bool) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", false
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", false
	}
	if len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return "", false
	}
	return SerialKey(ti.State.VerifiedChains[0][0].SerialNumber), true
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/server/service/ -run TestAuthServiceSuite/TestVerifiedCertSerial -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/service/auth.go pkg/server/service/auth_test.go
git commit -m "feat(server): VerifiedCertSerial — canonical serial from verified chain"
```

---

### Task 5: ACL atomic snapshot + `ReloadAuth`

**Files:**
- Modify: `pkg/server/service/volume.go`
- Test: `pkg/server/service/acl_test.go` (add methods to the existing suite)
- Regenerate: `internal/mocks/pkg/server/service` (interface gains `ReloadAuth`)

- [ ] **Step 1: Write the failing test**

Add to `pkg/server/service/acl_test.go` (it already has `buildService`,
`ctxFor`, `authWithUsers`, `boolPtr` helpers and volumes "photos"/"team"):

```go
func (s *ACLSuite) TestReloadAuth_GrantsNewAccess() {
	// bob starts with only "team"; default_allow=false.
	svc := buildService(s.T(), authWithUsers(boolPtr(false),
		config.BasicAuthConfigUser{Username: "bob", Volumes: []string{"team"}},
	))
	s.Require().Error(svc.PrincipalCanAccess(ctxFor("bob"), "photos"))

	// Reload with bob now granted "photos" too.
	svc.ReloadAuth(&config.Config{Auth: authWithUsers(boolPtr(false),
		config.BasicAuthConfigUser{Username: "bob", Volumes: []string{"team", "photos"}},
	)})
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("bob"), "photos"))
}

func (s *ACLSuite) TestReloadAuth_RevokesAccess() {
	svc := buildService(s.T(), authWithUsers(boolPtr(false),
		config.BasicAuthConfigUser{Username: "bob", Volumes: []string{"team", "photos"}},
	))
	s.Require().NoError(svc.PrincipalCanAccess(ctxFor("bob"), "photos"))

	svc.ReloadAuth(&config.Config{Auth: authWithUsers(boolPtr(false),
		config.BasicAuthConfigUser{Username: "bob", Volumes: []string{"team"}},
	)})
	s.Require().Error(svc.PrincipalCanAccess(ctxFor("bob"), "photos"))
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/service/ -run TestACLSuite/TestReloadAuth -v`
Expected: FAIL — `ReloadAuth` undefined.

- [ ] **Step 3a: Add the snapshot type + builder; swap the fields**

In `pkg/server/service/volume.go`, add `"sync/atomic"` to the imports. Replace
the three ACL fields on `VolumeServiceImpl`:

```go
	// acl is the atomically-swappable per-principal ACL. Read once per
	// PrincipalCanAccess; swapped by ReloadAuth so a concurrent read always
	// sees a consistent snapshot.
	acl atomic.Pointer[aclSnapshot]
```

(remove `aclByPrincipal`, `defaultAllow`, `aclEnabled`.) Add the type + builder
near the top of the file:

```go
// aclSnapshot is an immutable view of the per-principal ACL, swapped atomically
// on reload. byPrincipal maps principal → allowed-volume set (nil set entry
// only for principals with an explicit list; absence ⇒ defaultAllow applies).
type aclSnapshot struct {
	byPrincipal  map[string]map[string]struct{}
	defaultAllow bool
	enabled      bool
}

func buildACLSnapshot(cfg *config.Config) *aclSnapshot {
	snap := &aclSnapshot{
		byPrincipal:  make(map[string]map[string]struct{}),
		defaultAllow: true,
		enabled:      false,
	}
	if cfg.Auth != nil {
		if bac, ok := cfg.Auth.(*config.BasicAuthConfig); ok {
			snap.enabled = true
			snap.defaultAllow = bac.DefaultAllowOrTrue()
			for _, u := range bac.Users {
				if u.Volumes != nil {
					set := make(map[string]struct{}, len(u.Volumes))
					for _, v := range u.Volumes {
						set[v] = struct{}{}
					}
					snap.byPrincipal[u.Username] = set
				}
			}
		}
	}
	return snap
}
```

- [ ] **Step 3b: Initialize in the constructor**

In `NewVolumeService`, delete the old field initializers
(`aclByPrincipal: ...`, `defaultAllow: true`, `aclEnabled: false`) and the
inline ACL-building block (the `if cfg.Auth != nil { ... }` that filled
`aclByPrincipal`). After the `for _, option := range options { ... }` loop, add:

```go
	svc.acl.Store(buildACLSnapshot(cfg))
```

- [ ] **Step 3c: Read the snapshot in PrincipalCanAccess**

Replace the body of `PrincipalCanAccess`:

```go
func (s *VolumeServiceImpl) PrincipalCanAccess(ctx context.Context, volume string) error {
	snap := s.acl.Load()
	if !snap.enabled {
		return nil
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p == "" {
		return status.Errorf(codes.PermissionDenied, "no authenticated principal for volume %q", volume)
	}
	if set, hasList := snap.byPrincipal[p]; hasList {
		if _, allowed := set[volume]; allowed {
			return nil
		}
		return status.Errorf(codes.PermissionDenied, "principal %q is not granted volume %q", p, volume)
	}
	if snap.defaultAllow {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "principal %q has no volume grants (default_allow=false)", p)
}
```

- [ ] **Step 3d: Add `ReloadAuth` to the interface + impl**

In the `VolumeService` interface, add:

```go
	// ReloadAuth atomically swaps the ACL to reflect cfg.Auth. Only the ACL is
	// affected; volumes and filesystems are untouched. Called by the ops reload
	// path so an operator can change grants without a restart.
	ReloadAuth(cfg *config.Config)
```

And the implementation:

```go
func (s *VolumeServiceImpl) ReloadAuth(cfg *config.Config) {
	s.acl.Store(buildACLSnapshot(cfg))
}
```

- [ ] **Step 4: Run to verify it passes + the existing ACL tests still pass**

Run: `go test ./pkg/server/service/ -run TestACLSuite -v`
Expected: PASS (new `TestReloadAuth_*` and all pre-existing ACL cases).

- [ ] **Step 5: Regenerate mocks (the VolumeService interface gained a method)**

Run: `task gen:mocks`
Then: `go build ./... 2>&1 | grep -v "Package '" ; go vet ./pkg/server/...`
Expected: clean — `MockVolumeService` now has `ReloadAuth`.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/service/volume.go pkg/server/service/acl_test.go internal/mocks/
git commit -m "feat(server): atomic ACL snapshot + VolumeService.ReloadAuth"
```

---

### Task 6: Session carries serial + `ReapIf`

**Files:**
- Modify: `pkg/server/service/session.go`, `pkg/server/controller/session.go`
- Test: `pkg/server/service/session_test.go` (add to the existing suite, or
  create `reap_test.go` in the same package if no suite exists there)
- Regenerate: `internal/mocks/pkg/server/service` (`Create` signature changes,
  `ReapIf` added)

- [ ] **Step 1: Write the failing test**

Create `pkg/server/service/reap_test.go`:

```go
package service

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReapSuite struct{ suite.Suite }

func TestReapSuite(t *testing.T) { suite.Run(t, new(ReapSuite)) }

func (s *ReapSuite) newManager() *sessionManagerImpl {
	return NewSessionManager(SessionManagerOptions{}).(*sessionManagerImpl)
}

func (s *ReapSuite) TestReapIf_BlockedSerialReaped() {
	m := s.newManager()
	keepID, _ := m.Create("alice", "1111")
	reapID, _ := m.Create("alice", "dead") // same principal, revoked device

	n := m.ReapIf(func(_ string, serial string) bool { return serial == "dead" })
	s.Equal(1, n)
	_, err := m.Get(reapID)
	s.Require().Error(err) // reaped
	_, err = m.Get(keepID)
	s.Require().NoError(err) // other device survives
}

func (s *ReapSuite) TestReapIf_AdditiveReapsNothing() {
	m := s.newManager()
	a, _ := m.Create("alice", "1111")
	b, _ := m.Create("bob", "2222")
	// Predicate that never matches (an additive reload: nothing revoked).
	n := m.ReapIf(func(string, string) bool { return false })
	s.Equal(0, n)
	_, errA := m.Get(a)
	_, errB := m.Get(b)
	s.NoError(errA)
	s.NoError(errB)
}

func (s *ReapSuite) TestSessionExposesSerial() {
	m := s.newManager()
	id, _ := m.Create("alice", "abcd")
	sess, err := m.Get(id)
	s.Require().NoError(err)
	s.Equal("abcd", sess.Serial())
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/service/ -run TestReapSuite -v`
Expected: FAIL — `Create` takes 1 arg, `Serial`/`ReapIf` undefined.

- [ ] **Step 3a: Session stores serial**

In `pkg/server/service/session.go`, add `Serial() string` to the `Session`
interface (next to `Principal()`):

```go
	// Serial returns the canonical cert SerialKey bound at Create (empty for
	// basic-auth). Used by the reaper to match a blocked serial out of band.
	Serial() string
```

Add the field + accessor + thread it through `Create`:

```go
type sessionImpl struct {
	id        string
	principal string // set once at Create; never mutated
	serial    string // canonical cert SerialKey; "" for basic-auth
	fdNum     atomic.Uint64
	files     *xsync.MapOf[uint64, *FileEntry]
	replies   *lru.Cache[string, any]
	sf        singleflight.Group
}

func (s *sessionImpl) ID() string        { return s.id }
func (s *sessionImpl) Principal() string { return s.principal }
func (s *sessionImpl) Serial() string    { return s.serial }
```

Update the interface `Create` and the impl:

```go
	// Create creates a new session bound to principal and the canonical cert
	// SerialKey (empty for basic-auth), returning the session id.
	Create(principal, serial string) (string, error)
```

```go
func (m *sessionManagerImpl) Create(principal, serial string) (string, error) {
	id := uuid.NewString()
	replies, err := lru.New[string, any](DefaultIdempotencyCacheSize)
	if err != nil {
		return "", errors.Wrap(err, "create idempotency cache")
	}
	sess := &sessionImpl{
		id:        id,
		principal: principal,
		serial:    serial,
		files:     xsync.NewMapOf[uint64, *FileEntry](),
		replies:   replies,
	}
	m.sessions.Store(id, sess)
	m.metrics.SessionsActiveInc()
	return id, nil
}
```

- [ ] **Step 3b: Add `ReapIf` to the interface + impl**

Interface (add to `SessionManager`):

```go
	// ReapIf releases all fds of, and removes, every session for which pred
	// returns true; returns the count reaped. Called by the ops reload path to
	// evict revoked principals/serials without a restart. pred receives the
	// session's principal and cert SerialKey.
	ReapIf(pred func(principal, serial string) bool) int
```

Impl (place near `Stop`):

```go
func (m *sessionManagerImpl) ReapIf(pred func(principal, serial string) bool) int {
	reaped := 0
	m.sessions.Range(func(id string, sess *sessionImpl) bool {
		if !pred(sess.principal, sess.serial) {
			return true
		}
		if _, ok := m.sessions.LoadAndDelete(id); ok {
			m.metrics.SessionsActiveDec()
			sess.ReleaseAll()
			reaped++
		}
		return true
	})
	return reaped
}
```

- [ ] **Step 3c: Thread the serial in the controller**

In `pkg/server/controller/session.go`, update `Create`:

```go
	p, _ := principal.FromContext(ctx)
	serial, _ := service.VerifiedCertSerial(ctx)
	id, err := c.sessions.Create(p, serial)
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./pkg/server/service/ -run TestReapSuite -v`
Expected: PASS.

- [ ] **Step 5: Fix other `Create` callers + regenerate mocks**

The `SessionManager.Create` signature changed. Find and fix every caller:

```bash
grep -rn "\.Create(" pkg/server cmd test --include=*.go | grep -i "session" | grep -v "_test.go:.*MockSession"
```

For real callers passing one arg (e.g. test servers), add `, ""`. Then
regenerate mocks and rebuild:

```bash
task gen:mocks
go build ./... 2>&1 | grep -v "Package '"
go vet ./pkg/server/... ./cmd/...
```

Expected: clean. `MockSessionManager` now has the 2-arg `Create` + `ReapIf`.
Fix any test that calls the mock's `Create` with the old arity (`.Create(p)` →
`.Create(p, "")` or update `.On("Create", ...)` matchers to two args).

- [ ] **Step 6: Run the full service + controller packages**

Run: `go test ./pkg/server/service/... ./pkg/server/controller/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/server/service/session.go pkg/server/service/reap_test.go pkg/server/controller/session.go internal/mocks/
git commit -m "feat(server): session records cert serial; SessionManager.ReapIf"
```

---

### Task 7: The ops reload endpoint + reap predicate

**Files:**
- Create: `pkg/server/ops/reload.go`
- Modify: `pkg/server/ops/server.go` (extend `NewServer`, register the route)
- Test: `pkg/server/ops/reload_test.go` (new)

- [ ] **Step 1: Write the failing test**

Create `pkg/server/ops/reload_test.go`:

```go
package ops

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
)

type ReloadSuite struct{ suite.Suite }

func TestReloadSuite(t *testing.T) { suite.Run(t, new(ReloadSuite)) }

func (s *ReloadSuite) writeConfig(dir, body string) string {
	path := filepath.Join(dir, "server.yaml")
	s.Require().NoError(os.WriteFile(path, []byte(body), 0o600))
	return path
}

const reloadCfgGranted = `
auth:
  type: mtls
  default_allow: false
  revoked_serials: []
  users:
    - username: alice
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`

const reloadCfgRevoked = `
auth:
  type: mtls
  default_allow: false
  revoked_serials: ["dead"]
  users:
    - username: alice
      volumes: []
volumes:
  - name: photos
    path: /tmp
`

func (s *ReloadSuite) deps(path string) (service.VolumeService, service.SessionManager, *service.RevocationStore, *config.Config) {
	cfg, err := config.ReloadFromFile(path)
	s.Require().NoError(err)
	vs, err := service.NewVolumeService(cfg)
	s.Require().NoError(err)
	return vs, service.NewSessionManager(service.SessionManagerOptions{}), service.NewRevocationStore(), cfg
}

func (s *ReloadSuite) TestReloadAppliesAndReaps() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgGranted)
	vs, sm, rs, cfg := s.deps(path)
	// alice has an open session on the revoked device.
	id, _ := sm.Create("alice", "dead")

	// Operator rewrites the file to revoke alice's volume + block the serial.
	s.Require().NoError(os.WriteFile(path, []byte(reloadCfgRevoked), 0o600))

	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ops/acl/reload", nil))

	s.Equal(http.StatusOK, rec.Code)
	s.True(rs.IsBlocked("dead"))                  // blocklist swapped
	_, err := sm.Get(id)
	s.Require().Error(err)                         // session reaped
}

func (s *ReloadSuite) TestReloadBadConfigKeepsState() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgGranted)
	vs, sm, rs, cfg := s.deps(path)

	// Corrupt the file: invalid auth type fails validation.
	s.Require().NoError(os.WriteFile(path, []byte("auth:\n  type: bogus\n"), 0o600))

	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ops/acl/reload", nil))

	s.Equal(http.StatusBadRequest, rec.Code)
	s.False(rs.IsBlocked("dead")) // nothing swapped — prior state stands
}

func (s *ReloadSuite) TestReloadRejectsGET() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgGranted)
	vs, sm, rs, cfg := s.deps(path)
	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ops/acl/reload", nil))
	s.Equal(http.StatusMethodNotAllowed, rec.Code)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/ops/ -run TestReloadSuite -v`
Expected: FAIL — `ReloadHandler` undefined.

- [ ] **Step 3: Implement the handler + predicate**

Create `pkg/server/ops/reload.go`:

```go
package ops

import (
	"context"
	"encoding/json"
	"net/http"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/principal"
	"go.gmountie.dev/gmountie/pkg/server/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// ReloadHandler handles POST /ops/acl/reload. It re-reads the config file,
// validates it, atomically swaps the ACL + cert-serial blocklist, then reaps
// sessions that the new state revokes. A bad config returns 400 and changes
// nothing (fail-safe). Only auth state is hot-reloaded.
func ReloadHandler(cfg *config.Config, vs service.VolumeService, sm service.SessionManager, rs *service.RevocationStore) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if cfg.ConfigPath == "" {
			http.Error(w, "no config file to reload (config came from defaults/env)", http.StatusBadRequest)
			return
		}
		newCfg, err := config.ReloadFromFile(cfg.ConfigPath)
		if err != nil {
			log.Log.Warn("acl reload rejected: bad config", zap.Error(err))
			http.Error(w, "reload rejected: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Swap auth state.
		vs.ReloadAuth(newCfg)
		rs.Set(revokedSerials(newCfg))

		// Reap sessions the new state revokes.
		reaped := sm.ReapIf(reapPredicate(newCfg, vs, rs))
		log.Log.Info("acl reloaded", zap.Int("reaped", reaped))

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int{"reaped": reaped})
	})
}

// revokedSerials pulls the blocklist out of the auth config (basic + mtls both
// use *BasicAuthConfig). Nil when auth isn't of that type.
func revokedSerials(cfg *config.Config) []string {
	if bac, ok := cfg.Auth.(*config.BasicAuthConfig); ok {
		return bac.RevokedSerials
	}
	return nil
}

// reapPredicate reaps a session iff its serial is now blocked OR its principal
// can no longer access any configured volume. An additive reload (no serial
// blocked, no access removed) matches nothing.
func reapPredicate(cfg *config.Config, vs service.VolumeService, rs *service.RevocationStore) func(principalName, serial string) bool {
	return func(principalName, serial string) bool {
		if rs.IsBlocked(serial) {
			return true
		}
		ctx := principal.WithPrincipal(context.Background(), principalName)
		for _, v := range cfg.Volumes {
			if vs.PrincipalCanAccess(ctx, v.Name) == nil {
				return false // still has at least one volume → keep
			}
		}
		return true // no accessible volume → reap
	}
}
```

- [ ] **Step 4: Register the route (and pass the deps through `NewServer`)**

In `pkg/server/ops/server.go`, extend `NewServer` to accept the reload deps and
register the route. Change the signature and body:

```go
func NewServer(
	addr string,
	readiness ReadinessChecker,
	enablePprof bool,
	auth *BasicAuth,
	reloadCfg *config.Config,
	vs service.VolumeService,
	sm service.SessionManager,
	rs *service.RevocationStore,
) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.Handle("/healthz", LivenessHandler())
	mux.Handle("/readyz", ReadinessHandler(readiness))
	mux.Handle("/version", VersionHandler())
	if reloadCfg != nil && vs != nil && sm != nil && rs != nil {
		mux.Handle("/ops/acl/reload", ReloadHandler(reloadCfg, vs, sm, rs))
	}
	if enablePprof {
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		mux.HandleFunc("/debug/pprof/", pprof.Index)
	}
	return &Server{
		server: &http.Server{Addr: addr, Handler: auth.Wrap(mux)},
	}
}
```

Add the imports `"go.gmountie.dev/gmountie/pkg/server/config"` and
`"go.gmountie.dev/gmountie/pkg/server/service"` to `server.go`.

To keep the whole tree compiling after this task, also update the single
`ops.NewServer(...)` call in `pkg/server/app.go` now, passing the new args with
a **temporary** store (Task 9 revises these to the real `appCtx.*` deps):

```go
	opsServer := ops.NewServer(
		cfg.Server.Ops.Addr,
		readiness,
		cfg.Server.Pprof,
		ops.NewBasicAuth(cfg.Server.Ops.Auth.Users),
		cfg,
		appCtx.VolumeService,
		appCtx.SessionManager,
		service.NewRevocationStore(), // temporary; Task 9 swaps in appCtx.Revocation
	)
```

(`appCtx.VolumeService`/`SessionManager` already exist; only the
`RevocationStore` field is added in Task 9.) Verify this task with
`go test ./pkg/server/ops/ -run TestReloadSuite` and `go build ./... 2>&1 | grep -v "Package '"`.

- [ ] **Step 5: Run to verify it passes**

Run: `go test ./pkg/server/ops/ -run TestReloadSuite -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/ops/reload.go pkg/server/ops/server.go pkg/server/ops/reload_test.go
git commit -m "feat(server): POST /ops/acl/reload — re-read config, swap, reap"
```

---

### Task 8: Optional operator mTLS on the ops listener

**Files:**
- Modify: `pkg/server/ops/server.go`, `pkg/server/app.go` (`validateOpsConfig`)
- Test: `pkg/server/ops/server_test.go` (add to the existing suite)

- [ ] **Step 1: Write the failing test**

Add to `pkg/server/ops/server_test.go` a test that an mTLS-configured ops server
builds a TLS server with client-cert verification. Because wiring a full mTLS
round-trip is heavy, assert the constructed `*http.Server` carries a
`TLSConfig` with `ClientAuth == tls.RequireAndVerifyClientCert` via a new
exported accessor used only in tests:

```go
func (s *ServerSuite) TestOpsMTLSConfigured() {
	dir := s.T().TempDir()
	certPEM, keyPEM, caPEM := genOpsCertKeyCA(s.T()) // helper: self-signed CA+leaf
	certFile := writeFile(s.T(), dir, "ops.crt", certPEM)
	keyFile := writeFile(s.T(), dir, "ops.key", keyPEM)
	caFile := writeFile(s.T(), dir, "ca.pem", caPEM)

	srv, err := NewServerWithTLS("127.0.0.1:0", config.OpsTLSConfig{
		CertFile: certFile, KeyFile: keyFile, ClientCAFile: caFile,
	})
	s.Require().NoError(err)
	tc := srv.tlsConfig() // test-only accessor
	s.Require().NotNil(tc)
	s.Equal(tls.RequireAndVerifyClientCert, tc.ClientAuth)
}
```

`genOpsCertKeyCA`, `writeFile` are small helpers in the test file (generate a
self-signed CA, sign a leaf; reuse the pattern from
`pkg/server/grpc/factory_test.go` / `pkg/server/tls` test helpers — check those
for the exact `x509.CreateCertificate` boilerplate before writing).

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/ops/ -run TestServerSuite/TestOpsMTLS -v`
Expected: FAIL — `NewServerWithTLS` / `tlsConfig` undefined.

- [ ] **Step 3: Implement TLS on the ops server**

In `pkg/server/ops/server.go`, add a TLS path. Keep the existing plain-HTTP
`Start`; add a helper that builds the `tls.Config` and have `Start` use
`ListenAndServeTLS` when a `tls.Config` is set. Concretely:

```go
type Server struct {
	server *http.Server
	tls    *tls.Config // nil = plain HTTP
}

// applyTLS builds an mTLS tls.Config from the ops TLS settings and attaches it.
// Empty CertFile/KeyFile leaves the server plain HTTP.
func (s *Server) applyTLS(cfg config.OpsTLSConfig) error {
	if cfg.CertFile == "" && cfg.KeyFile == "" {
		return nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return errors.Wrap(err, "load ops TLS keypair")
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	if cfg.ClientCAFile != "" {
		caPEM, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return errors.Wrap(err, "read ops client CA")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return errors.New("ops client_ca_file: no valid PEM certificates")
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	s.tls = tc
	s.server.TLSConfig = tc
	return nil
}

// tlsConfig is a test-only accessor for the attached tls.Config (nil = plain).
func (s *Server) tlsConfig() *tls.Config { return s.tls }
```

Update `Start` to branch:

```go
func (s *Server) Start() {
	log.Log.Info("ops server starting", zap.String("addr", s.server.Addr))
	var err error
	if s.tls != nil {
		err = s.server.ListenAndServeTLS("", "") // certs already in TLSConfig
	} else {
		err = s.server.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Log.Error("ops server stopped", zap.Error(err))
	}
}
```

Add a thin `NewServerWithTLS(addr, cfg)` test constructor *or* fold the TLS
wiring into `NewServer` by calling `applyTLS` from `app.go` (Task 9). For the
unit test, the simplest is a small constructor:

```go
// NewServerWithTLS builds a minimal ops Server with TLS applied — used by tests
// and as the building block app.go calls applyTLS on.
func NewServerWithTLS(addr string, tlsCfg config.OpsTLSConfig) (*Server, error) {
	s := &Server{server: &http.Server{Addr: addr, Handler: http.NewServeMux()}}
	if err := s.applyTLS(tlsCfg); err != nil {
		return nil, err
	}
	return s, nil
}
```

Add `crypto/tls`, `crypto/x509`, `os`, and `github.com/pkg/errors` and the
config import to `server.go`.

- [ ] **Step 4: Extend `validateOpsConfig` for mtls**

In `pkg/server/app.go`, update `validateOpsConfig` so `type: mtls` requires the
TLS keypair + client CA:

```go
	if authType == "mtls" {
		if ops.TLS.CertFile == "" || ops.TLS.KeyFile == "" || ops.TLS.ClientCAFile == "" {
			return errors.New("server.ops.auth.type: mtls requires server.ops.tls.cert_file, key_file and client_ca_file")
		}
		return nil
	}
```

(place this branch alongside the existing `none` / `basic` branches; for `mtls`
the loopback-only restriction does not apply — mTLS is safe off-loopback.)

- [ ] **Step 5: Run to verify it passes**

Run: `go test ./pkg/server/ops/ -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/server/ops/server.go pkg/server/ops/server_test.go pkg/server/app.go
git commit -m "feat(server): optional operator mTLS on the ops listener"
```

---

### Task 9: Wire enforcement — AppContext store, handshake hook, per-RPC check, ops server

**Files:**
- Modify: `pkg/server/app.go`, `pkg/server/grpc/server.go`, `pkg/server/grpc/auth.go`
- Test: `pkg/server/grpc/auth_test.go` (per-RPC serial check),
  `pkg/server/app_test.go` (handshake hook — add to existing suite if present,
  else a small new test file)

- [ ] **Step 1: Write the failing test for the per-RPC serial check**

Add to `pkg/server/grpc/auth_test.go` (it already exercises `authorize` via
`info.FullMethod`). The interceptor will gain a `*service.RevocationStore`; a
blocked serial on the peer cert must be denied:

```go
func (s *AuthInterceptorSuite) TestBlockedSerialDenied() {
	rs := service.NewRevocationStore()
	rs.Set([]string{"dead"})
	i := NewAuthInterceptor(s.authService, s.sessions, rs)

	leaf := &x509.Certificate{SerialNumber: big.NewInt(0xdead)}
	ti := credentials.TLSInfo{State: tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{leaf}},
	}}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: ti})

	_, err := i.authorize(ctx, "/gmountie.RpcFile/Read")
	s.Require().Error(err)
	s.Equal(codes.Unauthenticated, status.Code(err))
}
```

(Use whatever the existing suite/helpers in `auth_test.go` are named — match the
field names it already uses for `authService` and `sessions`. If
`NewAuthInterceptor` is called elsewhere in tests, those call sites get the new
third arg in Step 3.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./pkg/server/grpc/ -run TestAuthInterceptorSuite/TestBlockedSerial -v`
Expected: FAIL — `NewAuthInterceptor` takes 2 args; `authorize` doesn't check
serials.

- [ ] **Step 3a: Add the serial check to the interceptor**

In `pkg/server/grpc/auth.go`, add the store to the struct + constructor and a
check at the top of `authorize`:

```go
type AuthInterceptor struct {
	authService service.AuthService
	sessions    service.SessionManager
	revocation  *service.RevocationStore
}

func NewAuthInterceptor(authService service.AuthService, sessions service.SessionManager, revocation *service.RevocationStore) *AuthInterceptor {
	return &AuthInterceptor{authService: authService, sessions: sessions, revocation: revocation}
}
```

At the very start of `authorize` (before Step 1's full-auth branch):

```go
	// Fail-closed: a revoked cert serial is denied on every RPC, including on
	// connections that completed their handshake before the serial was blocked.
	if i.revocation != nil {
		if serial, ok := service.VerifiedCertSerial(ctx); ok && i.revocation.IsBlocked(serial) {
			return nil, status.Errorf(codes.Unauthenticated, "client certificate revoked")
		}
	}
```

- [ ] **Step 3b: Thread the store through the gRPC server**

In `pkg/server/grpc/server.go`: add a `revocation *service.RevocationStore`
field to `Server`, a `WithRevocation` option mirroring `WithSessionManager`, and
pass it to `NewAuthInterceptor`:

```go
// WithRevocation sets the RevocationStore the AuthInterceptor consults to deny
// blocked cert serials per RPC. Always a pointer — RevocationStore holds an
// atomic.Pointer and must never be copied by value.
func WithRevocation(rs *service.RevocationStore) ServerOption { /* see note */ }
```

Note: pass and store a pointer (copying a `RevocationStore` by value is a bug —
`go vet` flags the atomic copy). Mirror `WithSessionManager` exactly — field
`revocation *service.RevocationStore`, setter `s.revocation = rs`, and at the
interceptor build site:

```go
	authInterceptor := NewAuthInterceptor(s.authService, s.sessionManager, s.revocation)
```

- [ ] **Step 3c: Build the store in AppContext + the handshake hook + wire ops**

In `pkg/server/app.go`:

Add to `AppContext`:

```go
	Revocation *service.RevocationStore
```

In `NewServerAppContext`, after building the other services:

```go
	revocation := service.NewRevocationStore()
	revocation.Set(revokedSerialsFromConfig(cfg))
```

and include `Revocation: revocation` in the returned struct. Add the helper:

```go
// revokedSerialsFromConfig extracts the startup blocklist (basic + mtls both
// use *BasicAuthConfig). Loaded at startup so a restart is fail-closed.
func revokedSerialsFromConfig(cfg *config.Config) []string {
	if bac, ok := cfg.Auth.(*config.BasicAuthConfig); ok {
		return bac.RevokedSerials
	}
	return nil
}
```

In `Start`, inside the mTLS branch (after `tlsCfg.ClientAuth = ...`), add the
fail-closed handshake hook:

```go
			tlsCfg.VerifyPeerCertificate = func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
				if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
					return nil // RequireAndVerifyClientCert already guarantees a chain
				}
				key := service.SerialKey(verifiedChains[0][0].SerialNumber)
				if appCtx.Revocation.IsBlocked(key) {
					return errors.New("client certificate revoked")
				}
				return nil
			}
```

Add `grpc.WithRevocation(appCtx.Revocation)` to `grpcOpts` (next to
`WithSessionManager`) — pass the pointer, never a deref/copy.

Update the ops server construction to pass the new deps:

```go
	opsServer := ops.NewServer(
		cfg.Server.Ops.Addr,
		readiness,
		cfg.Server.Pprof,
		ops.NewBasicAuth(cfg.Server.Ops.Auth.Users),
		cfg,
		appCtx.VolumeService,
		appCtx.SessionManager,
		appCtx.Revocation,
	)
	if err := opsServer.ApplyTLS(cfg.Server.Ops.TLS); err != nil {
		return errors.Wrap(err, "configure ops TLS")
	}
```

(Export `applyTLS` as `ApplyTLS` on `*ops.Server` so `app.go` can call it — rename
in Task 8's `server.go` accordingly, keeping the test-only `tlsConfig()` private.)

- [ ] **Step 4: Write the handshake-hook unit test**

Add a focused test for the hook logic. Extract the closure body into a small
named function in `app.go` so it is testable without a live TLS server:

```go
// rejectIfRevoked is the VerifyPeerCertificate body: it rejects a verified
// chain whose leaf serial is blocked.
func rejectIfRevoked(rs *service.RevocationStore, verifiedChains [][]*x509.Certificate) error {
	if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
		return nil
	}
	if rs.IsBlocked(service.SerialKey(verifiedChains[0][0].SerialNumber)) {
		return errors.New("client certificate revoked")
	}
	return nil
}
```

and have the closure call `rejectIfRevoked(appCtx.Revocation, verifiedChains)`.
Test in `pkg/server/app_test.go`:

```go
func (s *AppSuite) TestRejectIfRevoked() {
	rs := service.NewRevocationStore()
	rs.Set([]string{"dead"})
	leaf := &x509.Certificate{SerialNumber: big.NewInt(0xdead)}
	ok := &x509.Certificate{SerialNumber: big.NewInt(0x1234)}
	s.Require().Error(rejectIfRevoked(rs, [][]*x509.Certificate{{leaf}}))
	s.Require().NoError(rejectIfRevoked(rs, [][]*x509.Certificate{{ok}}))
	s.Require().NoError(rejectIfRevoked(rs, nil))
}
```

(If `pkg/server` has no test suite yet, create `app_test.go` with a minimal
`AppSuite`.)

- [ ] **Step 5: Fix `NewAuthInterceptor` call sites + build**

```bash
grep -rn "NewAuthInterceptor(" pkg --include=*.go
```

Update each (prod site is `grpc/server.go`; test sites pass
`service.NewRevocationStore()` or a configured one). Then:

```bash
go build ./... 2>&1 | grep -v "Package '"
go vet ./pkg/server/...
```

Expected: clean.

- [ ] **Step 6: Run to verify it passes**

Run: `go test ./pkg/server/grpc/ ./pkg/server/ -v 2>&1 | tail -20`
Expected: PASS (`TestBlockedSerialDenied`, `TestRejectIfRevoked`, and all
pre-existing tests).

- [ ] **Step 7: Commit**

```bash
git add pkg/server/app.go pkg/server/app_test.go pkg/server/grpc/server.go pkg/server/grpc/auth.go pkg/server/grpc/auth_test.go
git commit -m "feat(server): wire revocation — handshake hook, per-RPC check, ops reload"
```

---

### Task 10: Docs + full verification

**Files:**
- Modify: docs (config reference) if present; otherwise skip the doc edit.

- [ ] **Step 1: Document the new config keys**

If `website/` / `docs/` has a server-config reference page, add `auth.revoked_serials`,
`server.ops.tls.*`, and `server.ops.auth.type: mtls` with one-line descriptions
and the `POST /ops/acl/reload` endpoint (operator-only, mutates authz). Locate it:

```bash
grep -rln "server.ops.addr\|revoked_serials\|ops.auth" website docs 2>/dev/null
```

If none exists, skip — the spec under `docs/superpowers/specs/` is the record.

- [ ] **Step 2: Full local sweep**

```bash
go build ./... 2>&1 | grep -v "Package '"
go vet ./pkg/server/... ./cmd/...
go test $(go list ./... | grep -vE 'pkg/client/mount|test/e2e/fs|test/e2e/api|/ui') -race 2>&1 | grep -E "^(FAIL|ok .*server)" | head
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run ./pkg/server/... ./cmd/...
```

Expected: build/vet clean; server packages green under `-race`; lint 0 issues.
(FUSE-mount suites `pkg/client/mount`, `test/e2e/fs`, `test/e2e/api` and `ui`
are excluded — they need `/dev/fuse` / GTK and run only in CI.)

- [ ] **Step 3: Commit any doc changes**

```bash
git add -A
git commit -m "docs: document revocation config + /ops/acl/reload"
```

(Skip if Step 1 found nothing to edit.)

---

## After all tasks

Use **superpowers:finishing-a-development-branch**: verify tests, push the
branch, open the PR (CI runs the full suite incl. FUSE e2e). Then mark the two
oss-changes.md items — `ops/acl/reload`+reap and cert-serial revocation — ✅ DONE
with the PR number in `gMountie-cloud/docs/design/oss-changes.md`, and update the
`project_cloud_service` memory's S0 status.

## Self-review

- **Spec coverage:** two atomic snapshots → Tasks 3,5. Config blocklist + ops TLS
  + ConfigPath + ReloadFromFile → Tasks 1,2. `/ops/acl/reload` re-read + validate
  + 400-no-swap → Task 7. Three enforcement layers: handshake (Task 9),
  per-RPC (Task 9), reaper (Tasks 6,7). Session serial + reaper predicate
  (additive reaps nothing) → Tasks 6,7. Operator mTLS → Task 8. Fail-closed
  durability via config re-read → Tasks 2,9 (`revokedSerialsFromConfig` at
  startup). Known-gap (Create doesn't gate) is documented, not coded.
- **Placeholder scan:** none — every code step shows real code; the only
  "locate it" steps (grep for call sites / docs) are mechanical sweeps with the
  exact command given.
- **Type consistency:** `SerialKey`/`ParseSerialKey` (Task 3) used by Tasks 4,7,9.
  `RevocationStore.Set([]string)` / `IsBlocked(string)` consistent across Tasks
  3,7,9. `SessionManager.Create(principal, serial string)` and `ReapIf(func(string,string) bool) int`
  consistent across Tasks 6,7. `VolumeService.ReloadAuth(*config.Config)` (Task 5)
  called in Task 7. `ops.NewServer(addr, readiness, pprof, auth, cfg, vs, sm, rs)`
  (Task 7) matches the app.go call (Task 9). `NewAuthInterceptor(authService,
  sessions, revocation)` (Task 9) matches its test + call-site updates.
- **Mock regens:** Task 5 (VolumeService gains ReloadAuth) and Task 6
  (SessionManager.Create arity + ReapIf) each run `task gen:mocks` and fix
  downstream callers — required or dependent packages won't compile.
