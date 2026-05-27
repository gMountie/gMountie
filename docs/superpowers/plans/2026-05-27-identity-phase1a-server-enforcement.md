# Identity Phase 1a — Server-Side Identity & Kernel Enforcement — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the gMountie server resolve each request's *authenticated principal* to a real server-side identity (`uid`, primary `gid`, supplementary `gids`) per the volume's mapping mode, and enforce permissions by assuming that identity's full credentials on the OS thread so the **kernel** does the check.

**Architecture:** A unary gRPC interceptor stashes the authenticated principal on `context.Context`. `VolumeService.BindIdentity(ctx, volume, caller)` resolves `principal + volume mapping → Identity` (via a per-volume `IdentityResolver` + a `{volume,principal}` TTL cache) and returns a per-request **identity-bound `pathfs.FileSystem` wrapper** that, per op, pins the OS thread and applies raw `setgroups` + `setfsgid` + `setfsuid` before delegating to the volume's loopback FS. Controllers call `BindIdentity` for path ops; Read/Write keep using the already-open fd. The old `AssumeUserMiddleware` is deleted (subsumed). `passthrough` derives identity from the wire `proto.Caller` (+ `root_squash`); the mapped modes ignore the wire uid.

**Scope (this plan = Phase 1a):** modes `squash` (default), `static`, `system` (NSS via `getent`/`id`), `passthrough` (`root_squash` both ways); config schema + validation (fail-closed, auth-required); identity cache; `Access` evaluated against the resolved identity. **Out of scope:** `WhoAmI` + client-side rewriting (Phase 1b), capabilities `dac_read`/`dac_override` (Phase 3 — the `Caps` field is carried but unused here), volume confinement (Phase 2).

**Tech Stack:** Go 1.26, `github.com/hanwen/go-fuse/v2 v2.10.1` (`pathfs`), `golang.org/x/sys/unix` v0.45, Viper + `go-playground/validator` v10, testify suites. Module path `gmountie`. Server binary is `CGO_ENABLED=0`.

**Reference spec:** `docs/superpowers/specs/2026-05-27-identity-permissions-design.md` (§3.2 modes, §3.3 resolver, §3.4 enforcement+wiring, §3.11 robustness/validation).

**Testing notes:**
- Pure logic (config parse/validate, resolvers, ctx round-trip, cache, allocs) → unit tests as **testify suites** (project convention), runnable in the sandbox.
- Anything that calls `setfsuid`/`setfsgid`/`setgroups` or real `getent` needs **root on Linux** → run on the kubevirt VM (`ssh ubuntu@192.168.11.11`, passwordless sudo, go installed). These steps say "VM" explicitly.
- Do **not** hand-edit `internal/mocks/`; if a new interface needs a mock, add it to `.mockery.yml` and run `task gen:mocks`.

---

## Task 1: Config — `MappingConfig` schema + parsing

**Files:**
- Modify: `pkg/server/config/volumes.go`
- Test: `pkg/server/config/volumes_test.go`

- [ ] **Step 1: Write the failing test** (testify suite)

```go
package config

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/suite"
)

type VolumeConfigSuite struct{ suite.Suite }

func TestVolumeConfigSuite(t *testing.T) { suite.Run(t, new(VolumeConfigSuite)) }

func (s *VolumeConfigSuite) viperFrom(yaml string) *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")
	s.Require().NoError(v.ReadConfig(stringsNewReader(yaml)))
	return v
}

func (s *VolumeConfigSuite) TestDefaultsToSquash() {
	v := s.viperFrom("name: photos\npath: /srv/photos\n")
	c := NewVolumeConfig(v)
	s.Equal("photos", c.Name)
	s.Equal(MappingModeSquash, c.Mapping.Mode) // default when absent
}

func (s *VolumeConfigSuite) TestParsesStaticTable() {
	v := s.viperFrom(`
name: appliance
path: /srv/app
mapping:
  mode: static
  users:
    alice: {uid: 1001, gid: 1001, groups: [developers]}
  groups:
    developers: 2000
`)
	c := NewVolumeConfig(v)
	s.Equal(MappingModeStatic, c.Mapping.Mode)
	s.Equal(uint32(1001), c.Mapping.Users["alice"].Uid)
	s.Equal([]string{"developers"}, c.Mapping.Users["alice"].Groups)
	s.Equal(uint32(2000), c.Mapping.Groups["developers"])
}

func (s *VolumeConfigSuite) TestParsesPassthroughRootSquash() {
	v := s.viperFrom("name: lan\npath: /srv/lan\nmapping:\n  mode: passthrough\n  root_squash: false\n")
	c := NewVolumeConfig(v)
	s.Equal(MappingModePassthrough, c.Mapping.Mode)
	s.Require().NotNil(c.Mapping.RootSquash)
	s.False(*c.Mapping.RootSquash)
}
```

Add a tiny helper at the bottom of the test file:

```go
import "strings"
func stringsNewReader(sw string) *strings.Reader { return strings.NewReader(sw) }
```

- [ ] **Step 2: Run it, expect FAIL** — `go test ./pkg/server/config/ -run VolumeConfigSuite -v` → fails: `MappingMode*`, `MappingConfig`, `.Mapping` undefined.

- [ ] **Step 3: Implement the schema + parsing** in `pkg/server/config/volumes.go`

```go
package config

import "github.com/spf13/viper"

type MappingMode string

const (
	MappingModeSquash      MappingMode = "squash"
	MappingModeStatic      MappingMode = "static"
	MappingModeSystem      MappingMode = "system"
	MappingModePassthrough MappingMode = "passthrough"
)

// StaticUser is one principal's identity in a `static` mapping table.
type StaticUser struct {
	Uid    uint32   `mapstructure:"uid"`
	Gid    uint32   `mapstructure:"gid"`
	Groups []string `mapstructure:"groups"`
	Caps   []string `mapstructure:"caps"` // Phase 3; parsed but unused in 1a
}

// MappingConfig declares how a volume maps the authenticated principal to a
// server-side identity. See the identity-permissions design doc §3.2.
type MappingConfig struct {
	Mode MappingMode `mapstructure:"mode" validate:"required,oneof=squash static system passthrough"`

	// squash:
	Uid uint32 `mapstructure:"uid"`
	Gid uint32 `mapstructure:"gid"`

	// static:
	Users  map[string]StaticUser `mapstructure:"users"`
	Groups map[string]uint32     `mapstructure:"groups"`

	// passthrough:
	RootSquash *bool  `mapstructure:"root_squash"` // nil => default true
	AnonUid    uint32 `mapstructure:"anon_uid"`
}

type VolumeConfig struct {
	Name    string        `validate:"required"`
	Path    string        `validate:"required"`
	Mapping MappingConfig `validate:"required"`
}

// NewVolumeConfig creates a new VolumeConfig with defaults. An absent or empty
// `mapping` block defaults to squash (the safe default).
func NewVolumeConfig(v *viper.Viper) *VolumeConfig {
	m := MappingConfig{Mode: MappingModeSquash}
	if sub := v.Sub("mapping"); sub != nil {
		_ = sub.Unmarshal(&m)
		if m.Mode == "" {
			m.Mode = MappingModeSquash
		}
	}
	return &VolumeConfig{
		Name:    v.GetString("name"),
		Path:    v.GetString("path"),
		Mapping: m,
	}
}
```

- [ ] **Step 4: Run it, expect PASS** — `go test ./pkg/server/config/ -run VolumeConfigSuite -v`

- [ ] **Step 5: Commit**

```bash
git add pkg/server/config/volumes.go pkg/server/config/volumes_test.go
git commit -m "feat(server/config): per-volume mapping schema (squash/static/system/passthrough)"
```

---

## Task 2: Config — validation (fail-closed, auth-required)

**Files:**
- Modify: `pkg/server/config/config.go` (the `validator.Struct` path, ~line 130) — add a cross-field check
- Create: `pkg/server/config/mapping_validate.go`
- Test: `pkg/server/config/mapping_validate_test.go`

- [ ] **Step 1: Write the failing test**

```go
package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MappingValidateSuite struct{ suite.Suite }

func TestMappingValidateSuite(t *testing.T) { suite.Run(t, new(MappingValidateSuite)) }

func (s *MappingValidateSuite) TestSystemRequiresAuth() {
	err := ValidateMapping(MappingModeSystem, AuthConfigTypeNone)
	s.Require().Error(err)
	s.Contains(err.Error(), "auth")
}

func (s *MappingValidateSuite) TestStaticRequiresAuth() {
	s.Require().Error(ValidateMapping(MappingModeStatic, AuthConfigTypeNone))
}

func (s *MappingValidateSuite) TestSquashAllowsNoAuth() {
	s.Require().NoError(ValidateMapping(MappingModeSquash, AuthConfigTypeNone))
}

func (s *MappingValidateSuite) TestPassthroughAllowsNoAuth() {
	s.Require().NoError(ValidateMapping(MappingModePassthrough, AuthConfigTypeNone))
}
```

(`AuthConfigTypeNone`/`AuthConfigTypeBasic` already exist in this package — confirm the exact identifiers with `grep -n AuthConfigType pkg/server/config/*.go` before writing; adjust the test if the constant name differs.)

- [ ] **Step 2: Run it, expect FAIL** — `go test ./pkg/server/config/ -run MappingValidateSuite -v` → `ValidateMapping` undefined.

- [ ] **Step 3: Implement** `pkg/server/config/mapping_validate.go`

```go
package config

import "github.com/pkg/errors"

// ValidateMapping enforces cross-field rules the struct tags can't express:
// the resolver-backed modes need an authenticated principal, so they are
// invalid with auth disabled. Fail closed at startup rather than silently
// resolving "anonymous".
func ValidateMapping(mode MappingMode, authType AuthConfigType) error {
	switch mode {
	case MappingModeSystem, MappingModeStatic:
		if authType == AuthConfigTypeNone {
			return errors.Errorf("mapping mode %q requires authentication (auth.type must not be none)", mode)
		}
	}
	return nil
}
```

- [ ] **Step 4: Wire it into config load.** In `pkg/server/config/config.go`, after the existing `validator.Struct(result)` call (~line 130), loop volumes and call `ValidateMapping(v.Mapping.Mode, result.Auth.GetType())`, returning the first error. Find the exact field/accessor for the auth type first: `grep -nE 'Auth|GetType|validator.Struct' pkg/server/config/config.go`. Add:

```go
for _, vol := range result.Volumes {
	if err := ValidateMapping(vol.Mapping.Mode, result.Auth.GetType()); err != nil {
		return nil, errors.Wrapf(err, "volume %q", vol.Name)
	}
}
```

Adjust `result.Auth.GetType()` to the real accessor.

- [ ] **Step 5: Run it, expect PASS** — `go test ./pkg/server/config/ -v`

- [ ] **Step 6: Commit**

```bash
git add pkg/server/config/mapping_validate.go pkg/server/config/mapping_validate_test.go pkg/server/config/config.go
git commit -m "feat(server/config): fail-closed validation — system/static modes require auth"
```

---

## Task 3: `Identity` type + `IdentityResolver` interface

**Files:**
- Create: `pkg/server/service/identity.go`
- Test: (covered by resolver tests in Tasks 4–6)

- [ ] **Step 1: Implement** `pkg/server/service/identity.go`

```go
package service

import "github.com/pkg/errors"

// Identity is the resolved server-side identity of a principal on a volume.
// Caps is carried for Phase 3 (admin capabilities); it is unused in Phase 1a.
type Identity struct {
	Principal string
	Uid       uint32
	Gid       uint32   // primary
	Gids      []uint32 // supplementary, MUST include Gid
	Caps      []string // Phase 3 (dac_read/dac_override); empty in 1a
}

// ErrPrincipalNotFound is returned by resolvers when a principal cannot be
// resolved. Callers MUST fail closed (deny), never fall back to a privileged
// identity.
var ErrPrincipalNotFound = errors.New("principal not found")

// IdentityResolver maps an authenticated principal to a server-side Identity
// for one volume. One implementation per mapping mode (squash/static/system).
// passthrough does not implement this — its identity comes from the wire
// caller and is handled in BindIdentity.
type IdentityResolver interface {
	Resolve(principal string) (Identity, error)
}
```

- [ ] **Step 2: Build** — `go build ./pkg/server/service/` (no test yet; resolvers next).

- [ ] **Step 3: Commit**

```bash
git add pkg/server/service/identity.go
git commit -m "feat(server/service): Identity type + IdentityResolver interface"
```

---

## Task 4: `squash` resolver

**Files:**
- Create: `pkg/server/service/resolver_squash.go`
- Test: `pkg/server/service/resolver_squash_test.go`

- [ ] **Step 1: Write the failing test**

```go
package service

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type SquashResolverSuite struct{ suite.Suite }

func TestSquashResolverSuite(t *testing.T) { suite.Run(t, new(SquashResolverSuite)) }

func (s *SquashResolverSuite) TestResolvesFixedIdentityRegardlessOfPrincipal() {
	r := NewSquashResolver(1000, 1000)
	for _, p := range []string{"alice", "bob", "anonymous"} {
		id, err := r.Resolve(p)
		s.Require().NoError(err)
		s.Equal(uint32(1000), id.Uid)
		s.Equal(uint32(1000), id.Gid)
		s.Equal([]uint32{1000}, id.Gids)
	}
}
```

- [ ] **Step 2: Run, expect FAIL** — `go test ./pkg/server/service/ -run SquashResolverSuite -v`

- [ ] **Step 3: Implement**

```go
package service

// squashResolver maps every principal to one fixed identity (NFS all_squash).
type squashResolver struct{ uid, gid uint32 }

func NewSquashResolver(uid, gid uint32) IdentityResolver { return &squashResolver{uid, gid} }

func (r *squashResolver) Resolve(principal string) (Identity, error) {
	return Identity{Principal: principal, Uid: r.uid, Gid: r.gid, Gids: []uint32{r.gid}}, nil
}
```

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `git add pkg/server/service/resolver_squash*.go && git commit -m "feat(server/service): squash identity resolver"`

---

## Task 5: `static` resolver

**Files:**
- Create: `pkg/server/service/resolver_static.go`
- Test: `pkg/server/service/resolver_static_test.go`

- [ ] **Step 1: Write the failing test**

```go
package service

import (
	"testing"

	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type StaticResolverSuite struct{ suite.Suite }

func TestStaticResolverSuite(t *testing.T) { suite.Run(t, new(StaticResolverSuite)) }

func (s *StaticResolverSuite) mapping() config.MappingConfig {
	return config.MappingConfig{
		Mode:   config.MappingModeStatic,
		Users:  map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001, Groups: []string{"developers"}}},
		Groups: map[string]uint32{"developers": 2000},
	}
}

func (s *StaticResolverSuite) TestResolvesUserWithSupplementaryGroups() {
	r := NewStaticResolver(s.mapping())
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
	s.Equal(uint32(1001), id.Gid)
	s.ElementsMatch([]uint32{1001, 2000}, id.Gids) // primary + developers
}

func (s *StaticResolverSuite) TestUnknownPrincipalFailsClosed() {
	r := NewStaticResolver(s.mapping())
	_, err := r.Resolve("mallory")
	s.Require().ErrorIs(err, ErrPrincipalNotFound)
}

func (s *StaticResolverSuite) TestUnknownGroupIsSkipped() {
	m := s.mapping()
	m.Users["alice"] = config.StaticUser{Uid: 1001, Gid: 1001, Groups: []string{"developers", "ghosts"}}
	r := NewStaticResolver(m)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.ElementsMatch([]uint32{1001, 2000}, id.Gids) // "ghosts" has no gid mapping -> skipped
}
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement**

```go
package service

import "gmountie/pkg/server/config"

type staticResolver struct{ m config.MappingConfig }

func NewStaticResolver(m config.MappingConfig) IdentityResolver { return &staticResolver{m} }

func (r *staticResolver) Resolve(principal string) (Identity, error) {
	u, ok := r.m.Users[principal]
	if !ok {
		return Identity{}, ErrPrincipalNotFound
	}
	gids := []uint32{u.Gid}
	for _, g := range u.Groups {
		if gid, ok := r.m.Groups[g]; ok && gid != u.Gid {
			gids = append(gids, gid)
		}
	}
	return Identity{Principal: principal, Uid: u.Uid, Gid: u.Gid, Gids: gids, Caps: u.Caps}, nil
}
```

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `git commit -m "feat(server/service): static identity resolver (config table, fail-closed)"`

---

## Task 6: `system` resolver (NSS via `getent`/`id`, injectable exec)

**Files:**
- Create: `pkg/server/service/resolver_system.go`
- Test: `pkg/server/service/resolver_system_test.go`

**Why shell out:** the binary is `CGO_ENABLED=0`, so pure-Go `os/user` only reads `/etc/passwd`/`/etc/group` and misses LDAP/SSSD. `id`/`getent` consult the full NSS stack. Commands are run via **argv (never a shell string)** with a context timeout; the principal is validated first.

- [ ] **Step 1: Write the failing test** (inject a fake command runner so the unit test needs no real accounts)

```go
package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type SystemResolverSuite struct{ suite.Suite }

func TestSystemResolverSuite(t *testing.T) { suite.Run(t, new(SystemResolverSuite)) }

func (s *SystemResolverSuite) TestResolvesViaIdCommand() {
	fake := func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch {
		case name == "id" && args[0] == "-u":
			return []byte("1001\n"), nil
		case name == "id" && args[0] == "-g":
			return []byte("1001\n"), nil
		case name == "id" && args[0] == "-G":
			return []byte("1001 2000 2001\n"), nil
		}
		return nil, ErrPrincipalNotFound
	}
	r := newSystemResolverWithRunner(fake, time.Second)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
	s.Equal(uint32(1001), id.Gid)
	s.ElementsMatch([]uint32{1001, 2000, 2001}, id.Gids)
}

func (s *SystemResolverSuite) TestUnknownPrincipalFailsClosed() {
	fake := func(context.Context, string, ...string) ([]byte, error) { return nil, ErrPrincipalNotFound }
	r := newSystemResolverWithRunner(fake, time.Second)
	_, err := r.Resolve("mallory")
	s.Require().ErrorIs(err, ErrPrincipalNotFound)
}

func (s *SystemResolverSuite) TestRejectsMalformedPrincipal() {
	r := newSystemResolverWithRunner(func(context.Context, string, ...string) ([]byte, error) {
		s.Fail("runner must not be called for an invalid principal")
		return nil, nil
	}, time.Second)
	_, err := r.Resolve("alice; rm -rf /")
	s.Require().Error(err)
}
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** `pkg/server/service/resolver_system.go`

```go
package service

import (
	"context"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// commandRunner runs an external command (argv, no shell) and returns stdout.
// Injectable for tests.
type commandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

// validPrincipal guards the value we pass as an argv element. Even though argv
// avoids shell injection, we keep principals to a sane charset.
var validPrincipal = regexp.MustCompile(`^[a-zA-Z0-9._@-]{1,64}$`)

type systemResolver struct {
	run     commandRunner
	timeout time.Duration
}

func NewSystemResolver() IdentityResolver { return newSystemResolverWithRunner(execRunner, 5*time.Second) }

func newSystemResolverWithRunner(run commandRunner, timeout time.Duration) *systemResolver {
	return &systemResolver{run: run, timeout: timeout}
}

func (r *systemResolver) Resolve(principal string) (Identity, error) {
	if !validPrincipal.MatchString(principal) {
		return Identity{}, errors.Errorf("invalid principal %q", principal)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	uid, err := r.num(ctx, principal, "-u")
	if err != nil {
		return Identity{}, err // includes ErrPrincipalNotFound mapping below
	}
	gid, err := r.num(ctx, principal, "-g")
	if err != nil {
		return Identity{}, err
	}
	gout, err := r.run(ctx, "id", "-G", principal)
	if err != nil {
		return Identity{}, mapNotFound(err)
	}
	var gids []uint32
	for _, f := range strings.Fields(string(gout)) {
		if g, perr := strconv.ParseUint(f, 10, 32); perr == nil {
			gids = append(gids, uint32(g))
		}
	}
	return Identity{Principal: principal, Uid: uid, Gid: gid, Gids: gids}, nil
}

func (r *systemResolver) num(ctx context.Context, principal, flag string) (uint32, error) {
	out, err := r.run(ctx, "id", flag, principal)
	if err != nil {
		return 0, mapNotFound(err)
	}
	n, perr := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 32)
	if perr != nil {
		return 0, errors.Wrapf(perr, "parse id %s output", flag)
	}
	return uint32(n), nil
}

// mapNotFound: `id` exits non-zero for an unknown user; treat that as
// fail-closed not-found rather than a transient error.
func mapNotFound(err error) error {
	if errors.Is(err, ErrPrincipalNotFound) {
		return ErrPrincipalNotFound
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ErrPrincipalNotFound
	}
	return err
}
```

- [ ] **Step 4: Run, expect PASS** — `go test ./pkg/server/service/ -run SystemResolverSuite -v`

- [ ] **Step 5: VM check (real getent/id).** Copy the worktree to the VM (or `go test` over ssh) and run a tiny program that calls `NewSystemResolver().Resolve("<a real account on the VM, e.g. ubuntu>")`; assert uid/gid/gids match `id ubuntu`. This is a smoke check that the real argv path works; keep it out of the unit suite.

- [ ] **Step 6: Commit** — `git commit -m "feat(server/service): system identity resolver via getent/id (CGO-free NSS)"`

---

## Task 7: identity cache (`{volume,principal}` TTL)

**Files:**
- Create: `pkg/server/service/identity_cache.go`
- Test: `pkg/server/service/identity_cache_test.go`

- [ ] **Step 1: Write the failing test**

```go
package service

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type IdentityCacheSuite struct{ suite.Suite }

func TestIdentityCacheSuite(t *testing.T) { suite.Run(t, new(IdentityCacheSuite)) }

type countingResolver struct{ calls atomic.Int64 }

func (c *countingResolver) Resolve(p string) (Identity, error) {
	c.calls.Add(1)
	return Identity{Principal: p, Uid: 1001, Gid: 1001, Gids: []uint32{1001}}, nil
}

func (s *IdentityCacheSuite) TestCachesWithinTTL() {
	cr := &countingResolver{}
	c := NewCachedResolver(cr, time.Minute)
	for i := 0; i < 3; i++ {
		_, err := c.Resolve("alice")
		s.Require().NoError(err)
	}
	s.Equal(int64(1), cr.calls.Load()) // resolved once, served from cache after
}

func (s *IdentityCacheSuite) TestDoesNotCacheErrors() {
	cr := &countingResolver{}
	failing := resolverFunc(func(string) (Identity, error) { return Identity{}, ErrPrincipalNotFound })
	_ = cr
	c := NewCachedResolver(failing, time.Minute)
	_, e1 := c.Resolve("x")
	_, e2 := c.Resolve("x")
	s.Require().ErrorIs(e1, ErrPrincipalNotFound)
	s.Require().ErrorIs(e2, ErrPrincipalNotFound)
}

type resolverFunc func(string) (Identity, error)

func (f resolverFunc) Resolve(p string) (Identity, error) { return f(p) }
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** `pkg/server/service/identity_cache.go`

```go
package service

import (
	"sync"
	"time"
)

type cacheEntry struct {
	id      Identity
	expires time.Time
}

// cachedResolver wraps a resolver with a per-principal TTL cache. Errors are
// never cached (a transient resolver failure must not pin a denial). Keyed by
// principal; one cachedResolver exists per volume, so the volume is implicit.
type cachedResolver struct {
	inner IdentityResolver
	ttl   time.Duration
	mu    sync.Mutex
	store map[string]cacheEntry
}

func NewCachedResolver(inner IdentityResolver, ttl time.Duration) IdentityResolver {
	return &cachedResolver{inner: inner, ttl: ttl, store: make(map[string]cacheEntry)}
}

func (c *cachedResolver) Resolve(principal string) (Identity, error) {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.store[principal]; ok && now.Before(e.expires) {
		c.mu.Unlock()
		return e.id, nil
	}
	c.mu.Unlock()

	id, err := c.inner.Resolve(principal)
	if err != nil {
		return Identity{}, err // do not cache failures
	}
	c.mu.Lock()
	c.store[principal] = cacheEntry{id: id, expires: now.Add(c.ttl)}
	c.mu.Unlock()
	return id, nil
}
```

- [ ] **Step 4: Run, expect PASS.**
- [ ] **Step 5: Commit** — `git commit -m "feat(server/service): per-volume identity TTL cache (errors uncached)"`

---

## Task 8: stash the authenticated principal on `context.Context`

**Files:**
- Create: `pkg/server/grpc/ctxprincipal.go`
- Modify: `pkg/server/grpc/auth.go` (`Unary`)
- Test: `pkg/server/grpc/ctxprincipal_test.go`

- [ ] **Step 1: Write the failing test**

```go
package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CtxPrincipalSuite struct{ suite.Suite }

func TestCtxPrincipalSuite(t *testing.T) { suite.Run(t, new(CtxPrincipalSuite)) }

func (s *CtxPrincipalSuite) TestRoundTrip() {
	ctx := WithPrincipal(context.Background(), "alice")
	p, ok := PrincipalFromContext(ctx)
	s.True(ok)
	s.Equal("alice", p)
}

func (s *CtxPrincipalSuite) TestMissing() {
	_, ok := PrincipalFromContext(context.Background())
	s.False(ok)
}
```

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** `pkg/server/grpc/ctxprincipal.go`

```go
package grpc

import "context"

type principalKeyT struct{}

var principalKey principalKeyT

// WithPrincipal returns a context carrying the authenticated principal.
func WithPrincipal(ctx context.Context, principal string) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

// PrincipalFromContext returns the authenticated principal, if any.
func PrincipalFromContext(ctx context.Context) (string, bool) {
	p, ok := ctx.Value(principalKey).(string)
	return p, ok
}
```

- [ ] **Step 4: Stash it in the unary interceptor.** In `pkg/server/grpc/auth.go`, change the `Unary` body so that after a successful `Authorize`, it adds the principal to ctx (keep the existing log-field line):

```go
ctx = logging.InjectLogField(ctx, "user", user.Username)
ctx = WithPrincipal(ctx, user.Username)
return handler(ctx, req)
```

- [ ] **Step 5: Run, expect PASS** — `go test ./pkg/server/grpc/ -run CtxPrincipalSuite -v`
- [ ] **Step 6: Commit** — `git commit -m "feat(server/grpc): stash authenticated principal on request context"`

---

## Task 9: `changeIdentity` cred helper + identity-bound FS wrapper

**Files:**
- Create: `pkg/server/io/bound_fs.go` (adapted from `pkg/server/io/middleware/asume_user.go`)
- Test: `pkg/server/io/bound_fs_test.go`

**This wrapper replaces `AssumeUserMiddleware`.** It is structurally identical to `asume_user.go` (same ~21 overridden ops, each: pin thread → set creds → defer restore → delegate), with two changes: it holds an `*Identity` instead of reading `fuse.Context`, and `changeIdentity` adds raw per-thread `setgroups`.

- [ ] **Step 1: Write the failing tests** — allocation cost + the per-thread `setgroups` regression (the §0 proof, promoted). The cred-application assertions run on the **VM** (root); the allocation test runs anywhere.

```go
package io

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
)

type BoundFSSuite struct{ suite.Suite }

func TestBoundFSSuite(t *testing.T) { suite.Run(t, new(BoundFSSuite)) }

func (s *BoundFSSuite) TestBindIsAllocationCheap() {
	base := pathfs.NewLoopbackFileSystem(s.T().TempDir())
	id := &Identity{Uid: 1000, Gid: 1000, Gids: []uint32{1000}}
	allocs := testing.AllocsPerRun(100, func() {
		_ = NewIdentityBoundFS(base, id) // one small struct, no per-method closures
	})
	s.LessOrEqual(allocs, 1.0)
}
```

(`io.Identity` is a thin local mirror of the resolved identity — see Step 3. The service-layer `service.Identity` is converted to `io.Identity` in `BindIdentity`, Task 10, to avoid an import cycle.)

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** `pkg/server/io/bound_fs.go`. Start by copying `pkg/server/io/middleware/asume_user.go` into this file, then:
  1. Rename the type to `identityBoundFS`, package `io`.
  2. Add fields and constructor:

```go
// Identity is the minimal credential set the bound FS applies per op. Mirrors
// service.Identity (kept here to avoid an io->service import cycle).
type Identity struct {
	Uid  uint32
	Gid  uint32
	Gids []uint32
}

type identityBoundFS struct {
	pathfs.FileSystem
	id *Identity
}

// NewIdentityBoundFS wraps fs so every path op runs with id's credentials.
func NewIdentityBoundFS(fs pathfs.FileSystem, id *Identity) pathfs.FileSystem {
	return &identityBoundFS{FileSystem: fs, id: id}
}
```

  3. Replace the `changeUser(context *fuse.Context)` helper with `changeIdentity(id *Identity)`:

```go
// changeIdentity pins the current OS thread and applies the identity's full
// credentials (supplementary groups + primary gid + fsuid). Returns a cleanup
// that restores root creds and unlocks. On any error the thread is left locked
// (tainted) so it dies with the goroutine — same rule as the old changeUser.
func changeIdentity(id *Identity) (func(), error) {
	runtime.LockOSThread()

	origGroups, err := getgroups()
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setGroupsRaw(id.Gids); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsgid(int(id.Gid)); err != nil {
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	if err := setfsuid(int(id.Uid)); err != nil {
		_ = setfsgid(syscall.Getegid())
		_ = setGroupsRaw(origGroups)
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setfsuid(syscall.Geteuid()); err != nil {
			log.Log.Error("restore fsuid failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setfsgid(syscall.Getegid()); err != nil {
			log.Log.Error("restore fsgid failed; leaking OS thread", zap.Error(err))
			return
		}
		if err := setGroupsRaw(origGroups); err != nil {
			log.Log.Error("restore groups failed; leaking OS thread", zap.Error(err))
			return
		}
		runtime.UnlockOSThread()
	}, nil
}
```

  4. Add the raw per-thread groups syscalls (verified in `/tmp/sgtest_main.go`; Go's `syscall.Setgroups` broadcasts across threads via `AllThreadsSyscall`, so we use `RawSyscall`):

```go
func setGroupsRaw(gids []uint32) error {
	var p uintptr
	if len(gids) > 0 {
		p = uintptr(unsafe.Pointer(&gids[0]))
	}
	if _, _, errno := syscall.RawSyscall(syscall.SYS_SETGROUPS, uintptr(len(gids)), p, 0); errno != 0 {
		return errno
	}
	return nil
}

func getgroups() ([]uint32, error) {
	g, err := syscall.Getgroups()
	if err != nil {
		return nil, err
	}
	out := make([]uint32, len(g))
	for i, v := range g {
		out[i] = uint32(v)
	}
	return out, nil
}
```

  5. In every overridden op method (GetAttr, Chmod, Chown, Utimens, Truncate, Access, Link, Mkdir, Mknod, Rename, Rmdir, Unlink, GetXAttr, ListXAttr, RemoveXAttr, SetXAttr, Open, Create, OpenDir, Symlink, Readlink — the same set `asume_user.go` overrides), replace `cleanup, err := changeUser(context)` with `cleanup, err := changeIdentity(a.id)` and keep the rest of each body identical (delegating to `a.FileSystem.<Method>(...)`). The receiver becomes `(a *identityBoundFS)`. Keep `runtime`, `syscall`, `time`, `unsafe`, the go-fuse imports, the `log`/`zap` imports, and the `setfsuid`/`setfsgid` package vars.

- [ ] **Step 4: Run the alloc test, expect PASS** — `go test ./pkg/server/io/ -run BoundFSSuite -v`

- [ ] **Step 5: VM — promote the §0 per-thread proof as a guarded test.** Add `pkg/server/io/bound_fs_creds_linux_test.go` (build tag or `if os.Geteuid()!=0 { t.Skip }`) that, for `Identity{Uid:7000,Gid:8000,Gids:[]uint32{6000}}`, calls `changeIdentity`, opens a `0040` file owned `5000:6000`, and asserts success; a sibling goroutine with `Gids:[]uint32{9999}` gets EACCES (no cross-thread leak); and after cleanup the thread's groups are restored. Run on the VM as root: `sudo go test ./pkg/server/io/ -run Creds -v`.

- [ ] **Step 6: Commit** — `git add pkg/server/io/bound_fs*.go && git commit -m "feat(server/io): identity-bound FS wrapper (setgroups+setfsuid/gid per op)"`

---

## Task 10: `VolumeService.BindIdentity` + per-volume resolver wiring

**Files:**
- Modify: `pkg/server/service/volume.go`
- Test: `pkg/server/service/volume_bind_test.go`

- [ ] **Step 1: Write the failing test** (squash + static + passthrough; principal taken from ctx; passthrough from caller)

```go
package service

import (
	"context"
	"testing"

	"gmountie/pkg/proto"
	servergrpc "gmountie/pkg/server/grpc"
	"gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

type BindIdentitySuite struct{ suite.Suite }

func TestBindIdentitySuite(t *testing.T) { suite.Run(t, new(BindIdentitySuite)) }

func (s *BindIdentitySuite) TestSquashIgnoresPrincipalAndCaller() {
	svc := serviceForVolume(s.T(), config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	ctx := servergrpc.WithPrincipal(context.Background(), "alice")
	id, err := svc.(*VolumeServiceImpl).resolveIdentity(ctx, "v", &proto.Caller{Owner: &proto.Owner{Uid: 4242}})
	s.Require().NoError(err)
	s.Equal(uint32(1000), id.Uid)
}

func (s *BindIdentitySuite) TestStaticUsesCtxPrincipal() {
	m := config.MappingConfig{Mode: config.MappingModeStatic,
		Users:  map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}},
	}
	svc := serviceForVolume(s.T(), m).(*VolumeServiceImpl)
	id, err := svc.resolveIdentity(servergrpc.WithPrincipal(context.Background(), "alice"), "v", nil)
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
}

func (s *BindIdentitySuite) TestStaticNoPrincipalFailsClosed() {
	svc := serviceForVolume(s.T(), config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}}).(*VolumeServiceImpl)
	_, err := svc.resolveIdentity(context.Background(), "v", nil) // no principal on ctx
	s.Require().Error(err)
}

func (s *BindIdentitySuite) TestPassthroughRootSquashDefaultOn() {
	svc := serviceForVolume(s.T(), config.MappingConfig{Mode: config.MappingModePassthrough, AnonUid: 65534}).(*VolumeServiceImpl)
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(65534), id.Uid) // root squashed to anon by default
}

func (s *BindIdentitySuite) TestPassthroughNoRootSquashKeepsRoot() {
	no := false
	svc := serviceForVolume(s.T(), config.MappingConfig{Mode: config.MappingModePassthrough, RootSquash: &no}).(*VolumeServiceImpl)
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(0), id.Uid) // no_root_squash: root stays root
}
```

Add a helper `serviceForVolume(t, mapping) VolumeService` that builds a `*config.Config` with one volume named `v` (Path = `t.TempDir()`) and the given mapping, then `NewVolumeService(cfg)`.

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement** in `pkg/server/service/volume.go`:
  1. Add a `resolvers map[string]IdentityResolver` field and a `mappings map[string]config.MappingConfig` field to `VolumeServiceImpl`.
  2. In `NewVolumeService`, build the resolver per volume from its mapping mode and store the mapping:

```go
for _, v := range cfg.Volumes {
	svc.addFileSystem(v.Name, io.NewLocalFilesystem(v.Path))
	svc.mappings[v.Name] = v.Mapping
	switch v.Mapping.Mode {
	case config.MappingModeSquash:
		svc.resolvers[v.Name] = NewSquashResolver(v.Mapping.Uid, v.Mapping.Gid)
	case config.MappingModeStatic:
		svc.resolvers[v.Name] = NewCachedResolver(NewStaticResolver(v.Mapping), defaultIdentityTTL)
	case config.MappingModeSystem:
		svc.resolvers[v.Name] = NewCachedResolver(NewSystemResolver(), defaultIdentityTTL)
	case config.MappingModePassthrough:
		// no resolver; identity derives from the wire caller
	}
}
```

  with `const defaultIdentityTTL = 60 * time.Second`.

  3. Add `resolveIdentity` (the mode dispatch):

```go
func (s *VolumeServiceImpl) resolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error) {
	m, ok := s.mappings[volume]
	if !ok {
		return Identity{}, errors.Errorf("volume %s not found", volume)
	}
	if m.Mode == config.MappingModePassthrough {
		return passthroughIdentity(m, caller), nil
	}
	principal, ok := servergrpc.PrincipalFromContext(ctx)
	if !ok {
		return Identity{}, errors.Errorf("no authenticated principal for volume %s (mode %s)", volume, m.Mode)
	}
	return s.resolvers[volume].Resolve(principal)
}
```

  4. Add `passthroughIdentity` (wire caller + root_squash):

```go
func passthroughIdentity(m config.MappingConfig, caller *proto.Caller) Identity {
	var uid, gid uint32
	if caller != nil && caller.Owner != nil {
		uid, gid = caller.Owner.Uid, caller.Owner.Gid
	}
	squashRoot := m.RootSquash == nil || *m.RootSquash // default true
	if squashRoot && uid == 0 {
		uid = m.AnonUid
		if gid == 0 {
			gid = m.AnonUid
		}
	}
	return Identity{Uid: uid, Gid: gid, Gids: []uint32{gid}}
}
```

  5. Add `BindIdentity` to the interface and impl:

```go
// in the VolumeService interface:
BindIdentity(ctx context.Context, volume string, caller *proto.Caller) (pathfs.FileSystem, error)

// impl:
func (s *VolumeServiceImpl) BindIdentity(ctx context.Context, volume string, caller *proto.Caller) (pathfs.FileSystem, error) {
	fs, ok := s.filesystems[volume]
	if !ok {
		return nil, errors.Errorf("volume %s not found", volume)
	}
	id, err := s.resolveIdentity(ctx, volume, caller)
	if err != nil {
		return nil, err
	}
	return io.NewIdentityBoundFS(fs, &io.Identity{Uid: id.Uid, Gid: id.Gid, Gids: id.Gids}), nil
}
```

  Add imports: `context`, `gmountie/pkg/proto`, `servergrpc "gmountie/pkg/server/grpc"`, `time`. **Watch for an import cycle:** `service` importing `pkg/server/grpc` — verify `pkg/server/grpc` does not import `pkg/server/service` in a way that cycles. The auth interceptor imports `service` (for `AuthService`). To avoid a cycle, move `WithPrincipal`/`PrincipalFromContext` into a tiny leaf package `pkg/server/grpc/ctxprincipal` or `pkg/server/principal` that both `grpc` and `service` import. **Decide this in Step 3:** if `go build` reports a cycle, relocate `ctxprincipal.go` (Task 8) to `pkg/server/principal/principal.go` and update both importers. Re-run Task 8's test from the new package.

- [ ] **Step 4: Run, expect PASS** — `go test ./pkg/server/service/ -run BindIdentitySuite -v`
- [ ] **Step 5: Regenerate mocks** if `VolumeService` is mocked: `task gen:mocks` (the interface gained `BindIdentity`). Do not hand-edit `internal/mocks/`.
- [ ] **Step 6: Commit** — `git commit -m "feat(server/service): BindIdentity resolves principal+mapping to an identity-bound FS"`

---

## Task 11: controllers call `BindIdentity`; `Access` against the identity

**Files:**
- Modify: `pkg/server/controller/fs.go` (all path-op handlers), `pkg/server/controller/file.go` (`Open`, `Create`)
- Test: `pkg/server/controller/fs_identity_test.go`

- [ ] **Step 1: Write the failing test** — a controller handler resolves identity from ctx and binds before the op. Use the existing controller test harness/mocks; assert that with a static mapping and principal "alice" on ctx, a `GetAttr` is served through the bound FS (e.g., via a fake VolumeService whose `BindIdentity` records the resolved identity). Mirror the style of the existing `pkg/server/controller/*_test.go`.

```go
func (s *FsControllerSuite) TestGetAttrBindsIdentity() {
	// fake VolumeService: BindIdentity returns a sentinel FS and records args
	// assert controller called BindIdentity(ctx, "vol", request.Caller), not GetVolumeFileSystem
}
```

(Match the exact mock/harness the existing controller tests use; if they use `internal/mocks`, regenerate after the interface change in Task 10.)

- [ ] **Step 2: Run, expect FAIL.**

- [ ] **Step 3: Implement.** In every `fs.go` path-op handler, replace:

```go
fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
...
attr, status := fs.GetAttr(request.Path, createContext(ctx, request.Caller))
```

with:

```go
fs, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
if err != nil {
	return nil, err
}
attr, status := fs.GetAttr(request.Path, createContext(ctx, request.Caller))
```

Apply to: `GetAttr`, `OpenDir`, `Mkdir`, `Rmdir`, `Unlink`, `Rename`, `Chmod`, `Chown`, `Utimens`, `Truncate`, `Access`, `GetXAttr`, `StatFs`, `Compound`, and `Open`/`Create` in `file.go`. (`createContext` stays — `fuse.Context` still carries cancel + pid; the bound FS supplies the real creds.)

- [ ] **Step 4: `Access` against the resolved identity.** The loopback `Access` calls `access(2)` (real uid = root) and would always allow. Override `Access` in `identityBoundFS` (Task 9 file) to evaluate the requested mode against `a.id` using the file's `GetAttr` mode/owner bits, returning `fuse.EACCES`/`fuse.OK`. Minimal correct check: `GetAttr` the path (already credentialed), then test the rwx bits for owner/group/other against `a.id.Uid`/`a.id.Gids`. Add a focused unit test for the bit logic (pure function `accessAllowed(attr *fuse.Attr, id *Identity, mode uint32) bool`).

- [ ] **Step 5: Run, expect PASS** — `go test ./pkg/server/controller/ ./pkg/server/io/ -v`
- [ ] **Step 6: Commit** — `git commit -m "feat(server/controller): bind identity per request; Access checks resolved identity"`

---

## Task 12: delete `AssumeUserMiddleware`, rewire `app.go`, full verification

**Files:**
- Delete: `pkg/server/io/middleware/asume_user.go`, `pkg/server/io/middleware/asume_user_test.go`
- Modify: `pkg/server/app.go` (`getVolumeMiddleware`, line ~170)

- [ ] **Step 1:** Remove the `AssumeUserMiddleware` wiring in `app.go`. The bound FS now supplies creds per request, so the construction-time middleware is gone. If `getVolumeMiddleware` becomes empty, drop it and the `WithMiddleware(...)` option from the `NewVolumeService` call (or leave `WithMiddleware` for future middleware but pass nothing). Keep the `linux && uid==0` *log/preconditions* if the server needs to warn when not root (a mapped-mode server that can't `setfsuid` should warn at startup).

- [ ] **Step 2:** `git rm pkg/server/io/middleware/asume_user.go pkg/server/io/middleware/asume_user_test.go`

- [ ] **Step 3: Build + full unit suite** — `task test` (or `go test ./... -count=1`). Fix any references to the deleted symbols.

- [ ] **Step 4: VM end-to-end (real FUSE + root).** On the kubevirt VM, run an e2e that, with a `static` mapping (`alice→1001:1001[developers=2000]`, `bob→1002:1002[developers=2000]`) and basic auth:
  - As principal `alice`, create a file in a setgid `developers` dir → on-disk `1001:2000`.
  - As principal `bob` (member of 2000), read it → **succeeds** (supplementary-group access via `setgroups`).
  - `chmod 0600` as alice; bob read → **EACCES**.
  - `sudo` on bob's client → still evaluated as `bob` (no escalation).
  - `passthrough` volume, `root_squash:false`: client `sudo` write → file owned `0:0`.
  Place under `test/e2e/` following the existing harness; run with `sudo` per `feedback_fuse_test_env`.

- [ ] **Step 5: Commit** — `git commit -m "refactor(server): delete AssumeUserMiddleware (subsumed by identity-bound FS)"`

---

## Self-Review

**Spec coverage (§ → task):** mapping schema §3.2 → T1; validation/fail-closed/auth-required §3.11 → T2; resolver interface §3.3 → T3; squash/static/system resolvers §3.2 → T4/T5/T6 (getent argv + timeout §3.11 → T6); identity cache §3.11 → T7; principal on ctx §3.4 → T8; per-thread setgroups + setfsuid/gid + restore §3.4 → T9; BindIdentity wrapper + passthrough/root_squash §3.4/§3.7 → T10; controller wiring + Access fix §3.4 → T11; delete AssumeUser §6 → T12. **Not in 1a (correct):** WhoAmI/client rewriting (Phase 1b), caps (Phase 3), confinement (Phase 2).

**Type consistency:** `service.Identity{Principal,Uid,Gid,Gids,Caps}` (T3) vs `io.Identity{Uid,Gid,Gids}` (T9, converted in T10's `BindIdentity`) — intentional split to avoid the `io→service` import cycle; conversion is explicit in T10 Step 3.5. `IdentityResolver.Resolve(principal) (Identity,error)` consistent across T3/T4/T5/T6/T7. `BindIdentity(ctx, volume, caller)` consistent T10/T11. `MappingMode*` consts consistent T1/T2/T10.

**Known follow-ups (not blockers for 1a):** the `principal` ctx package may need to live in a leaf package to avoid a `service↔grpc` cycle (flagged in T10 Step 3.5 — resolve empirically at build time). `Access`'s bit-eval is a minimal POSIX check (owner/group/other rwx); ACL-precise `Access` is unnecessary because real ops are kernel-checked.
