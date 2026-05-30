# Client Follow-Referral Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `gmountie mount` client ask the server *where* a volume lives (`VolumeService.Resolve`) before mounting, and connect to the referred location when the server returns one.

**Architecture:** A new `NewClientForVolume(cfg, volume)` factory builds an *un-connected* client to the configured endpoint, calls `Resolve` **pre-session** (the auth interceptor authenticates via mTLS cert-check or basic-auth argon2 with no `session_id` — verified in `pkg/server/grpc/auth.go`), and then completes the session handshake (`Connect()`) on the right connection: the configured endpoint when the location is empty ("served here", the stock-OSS default), or a freshly-dialed connection to the referred location otherwise. Only the single-volume CLI mount path uses it; the multi-volume VFS mounter (UI, deferred) and `ls` keep dialing the configured endpoint.

**Tech Stack:** Go, gRPC, `go.gmountie.dev/gmountie/pkg/proto`, testify suites, mockery mocks (`internal/mocks/pkg/client/grpc`, `internal/mocks/pkg/proto`).

---

## Design notes / decisions

- **Pre-session Resolve is verified, not assumed.** `AuthInterceptor.authorize`
  (`pkg/server/grpc/auth.go:57`) forces full auth only for
  `SessionService/Create` and `/Resume`. Every other method — including
  `VolumeService/Resolve` — either uses the `session_id` fast-path (perf only,
  skips argon2) *or falls through to full `Authorize`* (mTLS cert-check or
  basic-auth argon2). A freshly-dialed connection presenting an mTLS cert or
  basic-auth creds authenticates with **no session**. This is exactly what the
  cloud resolve-only control plane needs.
- **No `Unimplemented` / graceful fallback.** Under the no-backward-compat
  policy every server implements `Resolve` (shipped in PR #68). Silently
  falling back to the configured endpoint on a `Resolve` error is also *wrong*
  for the cloud, where the configured endpoint is the resolver, not a data
  server — a `Resolve` failure must be fatal. So a `Resolve` error fails the
  mount.
- **Referral target is a hostname.** `ServerConfig.Address` is `validate:"ip"`,
  but a referral location is typically `v-id.data.example.com:443`. The data
  client therefore dials the **location string directly** as an explicit
  endpoint, never via `Address`. TLS `ServerName` and the TOFU known-hosts key
  derive from the location host.
- **Referral drops a pinned fingerprint.** `ExpectedFingerprint` pins one
  host's leaf cert; it cannot carry to a different referred host. On referral we
  clear it and rely on CA chain validation (`verify`) or TOFU pinning of the
  referred host (`tofu`).
- **Scope:** only `cmd/commands/mount.go` (single-volume) switches to the new
  factory. `cmd/commands/ls.go` and the multi-volume mounter keep
  `NewClientFromConfig`.

## File structure

- Modify: `pkg/client/grpc/factory.go` — extract `newUnconnectedClient`, add
  `NewClientForVolume`, `resolveLocation`, `tlsConfigForReferral`, and an
  injectable builder var.
- Create: `pkg/client/grpc/referral.go` — the referral helpers
  (`resolveLocation`, `tlsConfigForReferral`) kept separate from the dial
  plumbing so the seam is easy to read and test.
- Create: `pkg/client/grpc/referral_test.go` — unit tests for the pure helpers
  and the `NewClientForVolume` orchestration (mock clients + injected builder).
- Modify: `cmd/commands/mount.go:212` — call `NewClientForVolume(cfg, volumeName)`.

---

### Task 1: Extract `newUnconnectedClient` (pure refactor)

**Files:**
- Modify: `pkg/client/grpc/factory.go`
- Test: `pkg/client/grpc/factory_test.go` (existing — must stay green)

- [ ] **Step 1: Refactor `NewClientFromConfig` to split build from connect**

In `pkg/client/grpc/factory.go`, rename the current body of
`NewClientFromConfig` (everything from the nil-check through `NewClient(...)`,
**excluding** the final `client.Connect()` block) into a new unexported
function that takes an explicit endpoint, and make `NewClientFromConfig` a thin
wrapper. The TLS `Endpoint` and the dial target both come from the `endpoint`
parameter (was `createEndpoint(cfg.Server)`).

```go
// newUnconnectedClient builds a Client dialed at endpoint but does NOT run the
// session handshake. Split out so the referral path (NewClientForVolume) can
// call Resolve on the connection before deciding where to complete the session.
func newUnconnectedClient(cfg *config.Config, endpoint string) (Client, error) {
	if cfg == nil || cfg.Server == nil || cfg.Auth == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	if cfg.Log != nil {
		if err := log.Reconfigure(*cfg.Log, os.Stderr); err != nil {
			return nil, errors.Wrap(err, "configure logger")
		}
	}
	authConfig := cfg.Auth

	opts := make([]ClientOption, 0)

	if cfg.Rpc != nil {
		opts = append(opts, WithTimeouts(cfg.Rpc.TimeoutMeta, cfg.Rpc.TimeoutIO))
		opts = append(opts, WithReadahead(cfg.Rpc.ReadaheadChunkBytes, cfg.Rpc.ReadaheadThreshold, cfg.Rpc.ReadaheadWindow))
		opts = append(opts, WithWriteCoalesce(cfg.Rpc.WriteCoalesceBytes))
		dialOpts := []grpc.DialOption{
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                cfg.Rpc.Keepalive.Time,
				Timeout:             cfg.Rpc.Keepalive.Timeout,
				PermitWithoutStream: cfg.Rpc.Keepalive.PermitWithoutStream,
			}),
			grpc.WithDefaultCallOptions(defaultCallOptions(cfg.Rpc)...),
		}
		opts = append(opts, WithDialOptions(dialOpts))
	}

	tlsCfg, err := clienttls.BuildConfig(clienttls.Config{
		Endpoint:            endpoint,
		Mode:                cfg.Server.TLS.Verify,
		CAFile:              cfg.Server.TLS.CAFile,
		ExpectedFingerprint: cfg.Server.TLS.ExpectedFingerprint,
		ServerName:          cfg.Server.TLS.ServerName,
		KnownHostsPath:      cfg.Server.TLS.KnownHostsPath,
		CertFile:            cfg.Server.TLS.CertFile,
		KeyFile:             cfg.Server.TLS.KeyFile,
	})
	if err != nil {
		return nil, errors.Wrap(err, "build client TLS config")
	}
	opts = append(opts, WithDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
	}))

	m := metrics.NewMetrics()
	if err := m.Register(prometheus.DefaultRegisterer); err != nil {
		return nil, errors.Wrap(err, "register client metrics")
	}
	metrics.RegisterInstance(m)
	opts = append(opts, WithMetrics(m))

	if c, ok := authConfig.(*config.BasicAuthConfig); ok {
		opts = append(opts, WithBasicAuth(c.Username, c.Password))
	}

	return NewClient(endpoint, opts...)
}

// NewClientFromConfig builds a client to the configured endpoint and completes
// the session handshake. Used by paths that don't follow referrals (ls, the
// multi-volume mounter).
func NewClientFromConfig(cfg *config.Config) (Client, error) {
	client, err := newUnconnectedClient(cfg, createEndpoint(cfg.Server))
	if err != nil {
		return nil, err
	}
	if err := client.Connect(); err != nil {
		_ = client.Close()
		return nil, errors.Wrap(err, "session handshake failed; client unusable")
	}
	return client, nil
}
```

- [ ] **Step 2: Run the existing factory tests — must still pass**

Run: `go test ./pkg/client/grpc/ -run TestFactory -v`
Expected: PASS (pure refactor; behavior of `NewClientFromConfig` unchanged).

- [ ] **Step 3: Commit**

```bash
git add pkg/client/grpc/factory.go
git commit -m "refactor(client): split client build from session handshake

Extract newUnconnectedClient(cfg, endpoint) from NewClientFromConfig so
the referral path can call Resolve on the connection before deciding
where to complete the session handshake. No behavior change."
```

---

### Task 2: Referral helpers (`resolveLocation`, `tlsConfigForReferral`)

**Files:**
- Create: `pkg/client/grpc/referral.go`
- Test: `pkg/client/grpc/referral_test.go`

- [ ] **Step 1: Write failing tests for the pure helpers**

Create `pkg/client/grpc/referral_test.go`:

```go
package grpc

import (
	"context"
	"testing"

	clientmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	protomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ReferralSuite struct{ suite.Suite }

func TestReferralSuite(t *testing.T) { suite.Run(t, new(ReferralSuite)) }

// baseConfig returns a minimal client config with a verify-mode TLS block and
// a pinned fingerprint, so tests can assert the referral clears the pin.
func baseConfig() *config.Config {
	return &config.Config{
		Server: &config.ServerConfig{
			Address: "10.0.0.1",
			Port:    9000,
			TLS: config.TLSConfig{
				Verify:              "verify",
				CAFile:              "/etc/ca.pem",
				ExpectedFingerprint: "AA:BB",
				ServerName:          "origin.example.com",
			},
		},
	}
}

func (s *ReferralSuite) TestResolveLocation_ReturnsServerLocation() {
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.MatchedBy(func(r *proto.VolumeResolveRequest) bool {
		return r.GetName() == "photos"
	})).Return(&proto.VolumeResolveReply{Location: "v-abc.data.example.com:443"}, nil)

	client := clientmocks.NewMockClient(s.T())
	client.On("Volume").Return(vc)

	loc, err := resolveLocation(context.Background(), client, "photos")
	s.Require().NoError(err)
	s.Equal("v-abc.data.example.com:443", loc)
}

func (s *ReferralSuite) TestResolveLocation_PropagatesError() {
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return((*proto.VolumeResolveReply)(nil), errors.New("denied"))

	client := clientmocks.NewMockClient(s.T())
	client.On("Volume").Return(vc)

	_, err := resolveLocation(context.Background(), client, "photos")
	s.Require().Error(err)
}

func (s *ReferralSuite) TestTLSConfigForReferral_RetargetsHostAndClearsPin() {
	cfg := baseConfig()
	out, err := tlsConfigForReferral(cfg, "v-abc.data.example.com:443")
	s.Require().NoError(err)

	// ServerName retargeted to the referred host; pinned fingerprint dropped.
	s.Equal("v-abc.data.example.com", out.Server.TLS.ServerName)
	s.Empty(out.Server.TLS.ExpectedFingerprint)
	// CA + verify mode preserved.
	s.Equal("verify", out.Server.TLS.Verify)
	s.Equal("/etc/ca.pem", out.Server.TLS.CAFile)
	// Original config is not mutated.
	s.Equal("origin.example.com", cfg.Server.TLS.ServerName)
	s.Equal("AA:BB", cfg.Server.TLS.ExpectedFingerprint)
}

func (s *ReferralSuite) TestTLSConfigForReferral_RejectsBadLocation() {
	_, err := tlsConfigForReferral(baseConfig(), "not-a-host-port")
	s.Require().Error(err)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/client/grpc/ -run TestReferralSuite -v`
Expected: FAIL — `resolveLocation`/`tlsConfigForReferral` undefined.

- [ ] **Step 3: Implement the helpers**

Create `pkg/client/grpc/referral.go`:

```go
package grpc

import (
	"context"
	"net"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/pkg/errors"
)

// resolveLocation asks the server where the volume lives. An empty location
// means "served here"; a non-empty location is a referral the client should
// reconnect to. Pre-session: the server authenticates the caller from its mTLS
// cert or basic-auth creds, no session_id required.
func resolveLocation(ctx context.Context, client Client, volume string) (string, error) {
	reply, err := client.Volume().Resolve(ctx, &proto.VolumeResolveRequest{Name: volume})
	if err != nil {
		return "", err
	}
	return reply.GetLocation(), nil
}

// tlsConfigForReferral clones cfg with TLS retargeted at the referred host:
// ServerName becomes the location's host (SNI + cert verification target) and
// any pinned ExpectedFingerprint is dropped (a host-specific pin can't transfer
// to a different host; rely on CA chain validation or TOFU of the new host).
// The returned config's dial endpoint is the caller's responsibility — pass the
// raw location string to newUnconnectedClient, since ServerConfig.Address is
// IP-validated and a referral host is usually a name.
func tlsConfigForReferral(cfg *config.Config, location string) (*config.Config, error) {
	host, _, err := net.SplitHostPort(location)
	if err != nil {
		return nil, errors.Wrapf(err, "parse referral location %q", location)
	}
	clone := *cfg
	server := *cfg.Server
	server.TLS = cfg.Server.TLS
	server.TLS.ServerName = host
	server.TLS.ExpectedFingerprint = ""
	clone.Server = &server
	return &clone, nil
}
```

- [ ] **Step 4: Run to verify pass**

Run: `go test ./pkg/client/grpc/ -run TestReferralSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/grpc/referral.go pkg/client/grpc/referral_test.go
git commit -m "feat(client): referral helpers — resolveLocation + tlsConfigForReferral"
```

---

### Task 3: `NewClientForVolume` orchestration (TDD)

**Files:**
- Modify: `pkg/client/grpc/factory.go` (add the injectable builder var + `NewClientForVolume`)
- Modify: `pkg/client/grpc/referral_test.go` (add orchestration tests)

- [ ] **Step 1: Add the orchestration tests**

Append to `pkg/client/grpc/referral_test.go`:

```go
// withStubbedBuilder swaps newUnconnectedClientFn for the duration of fn,
// routing each (cfg,endpoint) build to a caller-supplied factory so the
// referral re-dial branch is exercised without real network dials.
func (s *ReferralSuite) withStubbedBuilder(build func(cfg *config.Config, endpoint string) (Client, error), fn func()) {
	orig := newUnconnectedClientFn
	newUnconnectedClientFn = build
	defer func() { newUnconnectedClientFn = orig }()
	fn()
}

func (s *ReferralSuite) TestNewClientForVolume_LocalConnectsConfigured() {
	cfg := baseConfig()
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return(&proto.VolumeResolveReply{Location: ""}, nil) // served here

	local := clientmocks.NewMockClient(s.T())
	local.On("Volume").Return(vc)
	local.On("Connect").Return(nil)

	var dialedEndpoints []string
	s.withStubbedBuilder(func(_ *config.Config, endpoint string) (Client, error) {
		dialedEndpoints = append(dialedEndpoints, endpoint)
		return local, nil
	}, func() {
		got, err := NewClientForVolume(cfg, "photos")
		s.Require().NoError(err)
		s.Equal(local, got)
	})
	// Only the configured endpoint was dialed; no referral re-dial.
	s.Equal([]string{createEndpoint(cfg.Server)}, dialedEndpoints)
}

func (s *ReferralSuite) TestNewClientForVolume_FollowsReferral() {
	cfg := baseConfig()
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return(&proto.VolumeResolveReply{Location: "v-abc.data.example.com:443"}, nil)

	resolver := clientmocks.NewMockClient(s.T())
	resolver.On("Volume").Return(vc)
	resolver.On("Close").Return(nil) // resolver conn closed before re-dial

	data := clientmocks.NewMockClient(s.T())
	data.On("Connect").Return(nil)

	var dialedEndpoints []string
	s.withStubbedBuilder(func(_ *config.Config, endpoint string) (Client, error) {
		dialedEndpoints = append(dialedEndpoints, endpoint)
		if endpoint == "v-abc.data.example.com:443" {
			return data, nil
		}
		return resolver, nil
	}, func() {
		got, err := NewClientForVolume(cfg, "photos")
		s.Require().NoError(err)
		s.Equal(data, got) // the referred client is returned
	})
	s.Equal([]string{createEndpoint(cfg.Server), "v-abc.data.example.com:443"}, dialedEndpoints)
}

func (s *ReferralSuite) TestNewClientForVolume_ResolveErrorIsFatal() {
	cfg := baseConfig()
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return((*proto.VolumeResolveReply)(nil), errors.New("denied"))

	resolver := clientmocks.NewMockClient(s.T())
	resolver.On("Volume").Return(vc)
	resolver.On("Close").Return(nil)

	s.withStubbedBuilder(func(_ *config.Config, _ string) (Client, error) {
		return resolver, nil
	}, func() {
		_, err := NewClientForVolume(cfg, "photos")
		s.Require().Error(err) // no fallback; resolve failure fails the mount
	})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/client/grpc/ -run TestReferralSuite -v`
Expected: FAIL — `newUnconnectedClientFn` and `NewClientForVolume` undefined.

- [ ] **Step 3: Implement the orchestration**

In `pkg/client/grpc/factory.go`, add after `newUnconnectedClient`:

```go
// resolveTimeout bounds the pre-session Resolve RPC when the config carries no
// explicit meta timeout. A referral lookup is one cheap round-trip.
const resolveTimeout = 5 * time.Second

// newUnconnectedClientFn is the un-connected client builder, indirected through
// a package var so referral orchestration tests can stub the dial.
var newUnconnectedClientFn = newUnconnectedClient

// NewClientForVolume connects to wherever a volume lives. It builds an
// un-connected client to the configured endpoint, calls Resolve (pre-session),
// then completes the session handshake on the configured endpoint when the
// location is empty ("served here") or on a freshly-dialed connection to the
// referred location otherwise. A Resolve failure is fatal — there is no silent
// fallback (the configured endpoint may be a resolve-only control plane).
func NewClientForVolume(cfg *config.Config, volume string) (Client, error) {
	if cfg == nil || cfg.Server == nil {
		return nil, errors.New("config is empty")
	}
	resolver, err := newUnconnectedClientFn(cfg, createEndpoint(cfg.Server))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), resolveMetaTimeout(cfg))
	defer cancel()
	location, err := resolveLocation(ctx, resolver, volume)
	if err != nil {
		_ = resolver.Close()
		return nil, errors.Wrap(err, "resolve volume location")
	}

	if location == "" {
		if err := resolver.Connect(); err != nil {
			_ = resolver.Close()
			return nil, errors.Wrap(err, "session handshake failed; client unusable")
		}
		return resolver, nil
	}

	// Referral: drop the resolver connection and dial the data plane.
	_ = resolver.Close()
	dataCfg, err := tlsConfigForReferral(cfg, location)
	if err != nil {
		return nil, err
	}
	data, err := newUnconnectedClientFn(dataCfg, location)
	if err != nil {
		return nil, err
	}
	if err := data.Connect(); err != nil {
		_ = data.Close()
		return nil, errors.Wrap(err, "session handshake failed; client unusable")
	}
	return data, nil
}

// resolveMetaTimeout picks the per-RPC timeout for the Resolve lookup: the
// configured meta timeout when present, else resolveTimeout.
func resolveMetaTimeout(cfg *config.Config) time.Duration {
	if cfg.Rpc != nil && cfg.Rpc.TimeoutMeta > 0 {
		return cfg.Rpc.TimeoutMeta
	}
	return resolveTimeout
}
```

Add `"context"` and `"time"` to the `factory.go` import block if not present.

- [ ] **Step 4: Run to verify pass**

Run: `go test ./pkg/client/grpc/ -run TestReferralSuite -v`
Expected: PASS (all referral + orchestration tests).

- [ ] **Step 5: Run the whole grpc package + vet + lint**

Run:
```bash
go test ./pkg/client/grpc/...
go vet ./pkg/client/grpc/...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run ./pkg/client/grpc/...
```
Expected: PASS / 0 issues.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/grpc/factory.go pkg/client/grpc/referral_test.go
git commit -m "feat(client): NewClientForVolume — follow VolumeService.Resolve referral

Resolve the volume's location pre-session, then complete the session
handshake on the configured endpoint (empty location = served here) or
on a re-dial to the referred location. Resolve failure is fatal."
```

---

### Task 4: Wire the CLI mount command

**Files:**
- Modify: `cmd/commands/mount.go:212`

- [ ] **Step 1: Switch the mount call site to the referral-aware factory**

Replace:
```go
		c, err := grpc.NewClientFromConfig(cfg)
```
with:
```go
		c, err := grpc.NewClientForVolume(cfg, volumeName)
```
(`volumeName` is already in scope — it's used at `:214` and `:234`.)

- [ ] **Step 2: Build + vet the cmd package**

Run:
```bash
go build ./cmd/...
go vet ./cmd/...
```
Expected: clean.

- [ ] **Step 3: Run the cmd tests that don't require a FUSE mount**

Run: `go test ./cmd/commands/ -run TestMount -v 2>&1 | tail -20`
Expected: PASS or skip (mount-execution tests need `/dev/fuse`, unavailable in
sandbox — they run in CI). A compile failure here is a real failure.

- [ ] **Step 4: Commit**

```bash
git add cmd/commands/mount.go
git commit -m "feat(cmd): gmountie mount follows VolumeService.Resolve referrals"
```

---

### Task 5: Finish the branch

- [ ] **Step 1:** Announce: "I'm using the finishing-a-development-branch skill to complete this work."
- [ ] **Step 2:** Run the full local sweep (`go build ./...`, `go vet ./...`, `go test` excluding `pkg/client/mount` and `test/e2e/fs` which need `/dev/fuse`, and the `ui` package which needs GTK). Touched packages (`pkg/client/grpc`, `cmd/commands`) must be green.
- [ ] **Step 3:** Push the branch and open a PR (CI runs the full suite incl. FUSE e2e).
- [ ] **Step 4:** Update `gMountie-cloud/docs/design/oss-changes.md` — mark the
  "Follow a referral (client)" item ✅ DONE with the PR number. Commit in the
  cloud repo.

---

## Self-review

- **Spec coverage:** Resolve-before-session ✓ (Task 3), referral re-dial ✓
  (Task 3), empty=served-here ✓ (Task 3 local test), TLS retarget + pin-drop ✓
  (Task 2), CLI wiring ✓ (Task 4), no-fallback fatal ✓ (Task 3 error test).
- **Placeholder scan:** none — every code step is concrete.
- **Type consistency:** `newUnconnectedClient(cfg, endpoint)` /
  `newUnconnectedClientFn` / `NewClientForVolume(cfg, volume)` /
  `resolveLocation(ctx, client, volume)` / `tlsConfigForReferral(cfg, location)`
  used identically across tasks. Mock import alias `protomocks` for
  `internal/mocks/pkg/proto`, plain `grpc` mock package for
  `internal/mocks/pkg/client/grpc` (matches existing `factory_test.go` usage —
  verify the alias there before finalizing Task 2).
- **Scope:** single plan, one subsystem (client connect path). Multi-volume and
  `ls` deliberately untouched.
