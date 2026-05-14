# Phase 1b — Client Robustness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every client → server RPC gets a per-call timeout, and every *idempotent* RPC also gets bounded exponential retry on `Unavailable`/`DeadlineExceeded`. A stalled server can no longer hang FUSE forever; a transient network blip no longer surfaces as `EIO`.

**Architecture:** Two small helpers in `pkg/client/io/` (a generic retry-with-classification wrapper and a per-call context-with-timeout shim) get applied to every existing RPC call site in `pkg/client/io/fs.go` and `pkg/client/io/file.go`. Timeouts come from new config keys (`rpc.timeout_meta` default `5s`, `rpc.timeout_io` default `30s`) plumbed through `grpc.Client`. Retry is hardcoded to 3 attempts, 100ms→1s exponential backoff — no config knobs until we have a reason. Mutating RPCs get *timeout only* (not retry); idempotency tokens for safe retry of mutations land in Plan 1d.

**Tech Stack:** Go 1.26, `github.com/avast/retry-go/v4` (already in `go.mod`), `google.golang.org/grpc/status` + `codes` for error classification.

---

## Scope context (read once, then forget)

- This is the second of four plans implementing roadmap Phase 1. Plan 1a (reliability fixes) is done. Plans 1c (sessions) and 1d (idempotency tokens) follow.
- The Phase 1 roadmap items addressed here are: **#4 (context propagation with timeouts)** and **#7 (retry on idempotent I/O)**.
- Item #4 in the roadmap says "Replace `context.Background()` with a context derived from the FUSE op (or a fresh one with a per-RPC timeout)". The `*fuse.Context` from go-fuse v2 already implements `context.Context` and is being passed straight into gRPC calls in `pkg/client/io/fs.go`. So for `fs.go` we **wrap the existing fuse context with a timeout**. For `pkg/client/io/file.go` (`GrpcFile`) there is no fuse context available — those methods are called by go-fuse without one — so we use `context.WithTimeout(context.Background(), ...)`.
- FUSE-touching tests (`pkg/client/mount/...`, `test/e2e/fs/...`) can't run in the Claude sandbox or GoLand's integrated terminal because of fd-inheritance issues — that's expected, validated by user-in-plain-terminal + CI.
- Backwards compatibility is **not** a concern.

## File Structure

Files this plan touches:

- **Create:** `pkg/client/config/rpc.go` — `RpcConfig` struct + defaults.
- **Create:** `pkg/client/io/retry.go` — `retryableCall[T]` generic helper + `isRetryableGrpcError` classifier + `withMetaTimeout`/`withIOTimeout` helpers.
- **Create:** `pkg/client/io/retry_test.go` — unit tests for the retry classifier and the generic wrapper.
- **Modify:** `pkg/client/config/config.go` — parse the new `rpc:` section.
- **Modify:** `pkg/client/config/config_test.go` — test the new defaults and overrides.
- **Modify:** `pkg/client/grpc/client.go` — add `MetaTimeout()` and `IOTimeout()` to the `Client` interface and the `ClientImpl` implementation; add an option to set them.
- **Modify:** `pkg/client/grpc/factory.go` — read timeouts from config, pass them via the new option.
- **Modify:** `pkg/client/grpc/factory_test.go` — assert the timeouts land on the client.
- **Modify:** `pkg/client/io/fs.go` — apply timeout+retry to idempotent ops; timeout only to mutating ops.
- **Modify:** `pkg/client/io/fs_test.go` — assertions still pass (mocks accept any context); a focused regression test that retry fires on `Unavailable` for `GetAttr`.
- **Modify:** `pkg/client/io/file.go` — apply timeout+retry to `Read`; timeout only to the rest.
- **Modify:** `pkg/client/io/file_test.go` — regression test that retry fires on `Unavailable` for `Read`.

**Working-set test command (Claude sandbox / GoLand terminal):**

```
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

**Full validation (user, plain terminal):**

```
task test
task lint
```

---

## Task 1: Add `RpcConfig` to client config

**Why:** Per-call timeouts must come from configuration, not hardcoded. Without a config section there's no place to set them.

**Files:**
- Create: `pkg/client/config/rpc.go`
- Modify: `pkg/client/config/config.go`
- Modify: `pkg/client/config/config_test.go`

- [ ] **Step 1: Create the new config file**

Create `pkg/client/config/rpc.go`:

```go
package config

import (
	"time"

	"github.com/spf13/viper"
)

const (
	// DefaultRpcTimeoutMeta is the default per-RPC timeout for metadata
	// operations (Lookup, GetAttr, Readdir, etc.). Small ops over the network
	// should be cheap.
	DefaultRpcTimeoutMeta = 5 * time.Second
	// DefaultRpcTimeoutIO is the default per-RPC timeout for data operations
	// (Read, Write). Tuned for moderate-sized payloads over an internet link.
	DefaultRpcTimeoutIO = 30 * time.Second
)

// RpcConfig holds per-RPC client-side timeouts and (in future plans) retry
// tuning. Retry parameters are intentionally hardcoded in retry.go for now —
// add config keys here only when we have evidence we need them.
type RpcConfig struct {
	// TimeoutMeta bounds each metadata RPC (Lookup, GetAttr, Readdir, StatFs,
	// Access, *XAttr, and the mutating metadata ops Mkdir/Rmdir/Rename/...).
	TimeoutMeta time.Duration `validate:"required,gte=1ms"`
	// TimeoutIO bounds each data RPC (Read, Write, and file-state ops like
	// Flush/Fsync/Release/locking/Allocate).
	TimeoutIO time.Duration `validate:"required,gte=1ms"`
}

// NewRpcConfig parses an RpcConfig from a viper sub-tree. A nil v yields
// defaults; an empty sub-tree yields defaults; explicit values override.
func NewRpcConfig(v *viper.Viper) (*RpcConfig, error) {
	cfg := &RpcConfig{
		TimeoutMeta: DefaultRpcTimeoutMeta,
		TimeoutIO:   DefaultRpcTimeoutIO,
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("timeout_meta", DefaultRpcTimeoutMeta)
	v.SetDefault("timeout_io", DefaultRpcTimeoutIO)
	if err := v.UnmarshalExact(cfg, viperDurationDecoderOpt); err != nil {
		return nil, err
	}
	return cfg, nil
}

// viperDurationDecoderOpt teaches mapstructure to parse `5s` style strings
// into time.Duration. Without it `UnmarshalExact` returns "expected
// duration; got string".
var viperDurationDecoderOpt = viper.DecodeHook(viperDurationHookFunc())
```

You'll need to import the mapstructure hook. Add at the bottom of the same file:

```go
import "github.com/go-viper/mapstructure/v2"

// viperDurationHookFunc returns a mapstructure decoder hook that converts
// strings into time.Duration via time.ParseDuration. mapstructure's
// StringToTimeDurationHookFunc already exists; we re-export it for clarity.
func viperDurationHookFunc() mapstructure.DecodeHookFunc {
	return mapstructure.StringToTimeDurationHookFunc()
}
```

(Both imports — `time`, `github.com/spf13/viper`, `github.com/go-viper/mapstructure/v2` — go into a single import block at the top of the file. Adjust to match the project's idiomatic single-block import style.)

- [ ] **Step 2: Wire `RpcConfig` into the top-level `Config`**

In `pkg/client/config/config.go`, locate the `Config` struct:

```go
type Config struct {
	Server *ServerConfig `validate:"required"`
	Auth   serverConfig.AuthConfig `validate:"required"`
	Mount  MountConfig `yaml:"mount,omitempty"`
}
```

Add the new field:

```go
type Config struct {
	Server *ServerConfig `validate:"required"`
	Auth   serverConfig.AuthConfig `validate:"required"`
	Mount  MountConfig `yaml:"mount,omitempty"`
	Rpc    *RpcConfig `validate:"required" yaml:"rpc,omitempty"`
}
```

In the same file, find `ParseConfig` and add an rpc-parsing block alongside the existing server/auth/mount parsing. The current end of `ParseConfig` looks like:

```go
	// Parse mount config
	mount := v.Sub("mount")
	if mount != nil {
		if cfg, err := NewMountConfig(v.Sub("mount")); err == nil {
			result.Mount = cfg
		} else {
			return nil, err
		}
	}

	if err := result.Validate(); err != nil {
		return nil, err
	}
	return &result, nil
}
```

Add an rpc block before the validation:

```go
	// Parse rpc config (defaults if absent)
	if cfg, err := NewRpcConfig(v.Sub("rpc")); err == nil {
		result.Rpc = cfg
	} else {
		return nil, err
	}

	if err := result.Validate(); err != nil {
		return nil, err
	}
	return &result, nil
}
```

- [ ] **Step 3: Write a test for defaults**

Append to `pkg/client/config/config_test.go` (place inside the existing `ConfigTestSuite`, mirroring its style — find the suite definition first with `grep -n "ConfigTestSuite" pkg/client/config/config_test.go`).

```go
// TestParse_RpcDefaults verifies that omitting the rpc: section yields
// the documented default timeouts.
func (s *ConfigTestSuite) TestParse_RpcDefaults() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Require().NotNil(result.Rpc)
	s.Assert().Equal(DefaultRpcTimeoutMeta, result.Rpc.TimeoutMeta)
	s.Assert().Equal(DefaultRpcTimeoutIO, result.Rpc.TimeoutIO)
}

// TestParse_RpcOverride verifies explicit rpc values override the defaults.
func (s *ConfigTestSuite) TestParse_RpcOverride() {
	conf := `
server:
  address: 127.0.0.1
  port: 9449
auth:
  type: none
rpc:
  timeout_meta: 2s
  timeout_io: 1m
`
	result, err := LoadConfigFromString(conf)
	s.Require().NoError(err)
	s.Assert().Equal(2*time.Second, result.Rpc.TimeoutMeta)
	s.Assert().Equal(time.Minute, result.Rpc.TimeoutIO)
}
```

Add `"time"` to the test file's imports if not already present.

- [ ] **Step 4: Run the new tests**

```bash
go test -count=1 -run "TestConfigTestSuite/TestParse_RpcDefaults|TestConfigTestSuite/TestParse_RpcOverride" ./pkg/client/config/...
```

Expected: both PASS.

- [ ] **Step 5: Run the full client-config suite**

```bash
go test -count=1 ./pkg/client/config/...
```

Expected: every test PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/config/rpc.go pkg/client/config/config.go pkg/client/config/config_test.go
git commit -m "feat(client/config): add RpcConfig with per-call timeout defaults"
```

---

## Task 2: Plumb timeouts through `grpc.Client`

**Why:** The io layer needs to read the configured timeouts. The natural carrier is the `grpc.Client` (it's already a constructor-time dependency of `LocalFileSystem` and `GrpcFile`).

**Files:**
- Modify: `pkg/client/grpc/client.go`
- Modify: `pkg/client/grpc/factory.go`
- Modify: `pkg/client/grpc/factory_test.go`

- [ ] **Step 1: Extend the `Client` interface**

In `pkg/client/grpc/client.go`, replace the `Client` interface:

```go
// Client is the interface for the gRPC Client.
type Client interface {
	// GetEndpoint returns the gRPC Client endpoint.
	GetEndpoint() string
	// Connect connects to the gRPC server.
	Connect()
	// Close closes the gRPC Client connection.
	Close() error
	// File returns the gRPC File client.
	File() proto.RpcFileClient
	// Fs returns the gRPC Fs client.
	Fs() proto.RpcFsClient
	// Volume returns the gRPC Volume client.
	Volume() proto.VolumeServiceClient
	// MetaTimeout returns the per-RPC timeout for metadata operations.
	MetaTimeout() time.Duration
	// IOTimeout returns the per-RPC timeout for data operations.
	IOTimeout() time.Duration
}
```

Add `"time"` to the imports.

- [ ] **Step 2: Extend `ClientImpl` with fields + accessors**

In the same file, locate the `ClientImpl` struct and add two fields:

```go
type ClientImpl struct {
	endpoint    string
	conn        *grpc.ClientConn
	dialOptions []grpc.DialOption
	fs          proto.RpcFsClient
	file        proto.RpcFileClient
	volume      proto.VolumeServiceClient
	metaTimeout time.Duration
	ioTimeout   time.Duration
}
```

Add accessor methods at the end of the methods section (after `Close`):

```go
// MetaTimeout returns the per-RPC timeout for metadata operations.
func (c *ClientImpl) MetaTimeout() time.Duration {
	return c.metaTimeout
}

// IOTimeout returns the per-RPC timeout for data operations.
func (c *ClientImpl) IOTimeout() time.Duration {
	return c.ioTimeout
}
```

- [ ] **Step 3: Add a `WithTimeouts` constructor option**

Below the existing `WithBasicAuth` option in `pkg/client/grpc/client.go`, add:

```go
// WithTimeouts sets the per-RPC timeouts on the gRPC Client.
func WithTimeouts(meta, io time.Duration) ClientOption {
	return func(c *ClientImpl) {
		c.metaTimeout = meta
		c.ioTimeout = io
	}
}
```

- [ ] **Step 4: Set defaults in `NewClient`**

In the `NewClient` constructor, set safe defaults before the options loop. The constructor currently starts with:

```go
func NewClient(endpoint string, options ...ClientOption) (Client, error) {
	c := ClientImpl{endpoint: endpoint}
	for _, opt := range options {
		opt(&c)
	}
```

Change the first line to seed defaults so callers that don't pass `WithTimeouts` still get sane behaviour:

```go
func NewClient(endpoint string, options ...ClientOption) (Client, error) {
	c := ClientImpl{
		endpoint:    endpoint,
		metaTimeout: 5 * time.Second,
		ioTimeout:   30 * time.Second,
	}
	for _, opt := range options {
		opt(&c)
	}
```

(The hardcoded defaults here intentionally match `DefaultRpcTimeoutMeta`/`DefaultRpcTimeoutIO` in `pkg/client/config/rpc.go` — they apply when `NewClient` is called outside the config-driven path, e.g. tests.)

- [ ] **Step 5: Update `NewClientFromConfig` to pass timeouts**

In `pkg/client/grpc/factory.go`, modify `NewClientFromConfig`:

```go
func NewClientFromConfig(cfg *config.Config) (Client, error) {
	if cfg == nil || cfg.Server == nil || cfg.Auth == nil {
		return nil, errors.New("config is empty or auth config is empty")
	}
	authConfig := cfg.Auth

	opts := make([]ClientOption, 0)

	if cfg.Rpc != nil {
		opts = append(opts, WithTimeouts(cfg.Rpc.TimeoutMeta, cfg.Rpc.TimeoutIO))
	}

	switch c := authConfig.(type) {
	case *serverConfig.NoneAuthConfig:
		// Do nothing
	case *config.BasicAuthConfig:
		opts = append(opts, WithBasicAuth(c.Username, c.Password))
	}
	return NewClient(createEndpoint(cfg.Server), opts...)
}
```

- [ ] **Step 6: Add a test asserting the timeouts land on the client**

Append to `pkg/client/grpc/factory_test.go` (mirror existing test style — find the suite name first with `grep -n "TestSuite\|suite.Run" pkg/client/grpc/factory_test.go`):

```go
// TestNewClientFromConfig_TimeoutsApplied verifies the configured RPC
// timeouts are propagated onto the constructed client.
func (s *FactoryTestSuite) TestNewClientFromConfig_TimeoutsApplied() {
	cfg := &config.Config{
		Server: &config.ServerConfig{Address: "127.0.0.1", Port: 9449},
		Auth:   &serverConfig.NoneAuthConfig{},
		Rpc:    &config.RpcConfig{TimeoutMeta: 2 * time.Second, TimeoutIO: 90 * time.Second},
	}

	c, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	defer c.Close()

	s.Assert().Equal(2*time.Second, c.MetaTimeout())
	s.Assert().Equal(90*time.Second, c.IOTimeout())
}
```

Add the import for `"time"` and verify `config` is already aliased in the test file (it should be — match the existing import style).

- [ ] **Step 7: Run the new and existing client-grpc tests**

```bash
go test -count=1 ./pkg/client/grpc/...
```

Expected: every test PASS, including `TestNewClientFromConfig_TimeoutsApplied`.

- [ ] **Step 8: Commit**

```bash
git add pkg/client/grpc/client.go pkg/client/grpc/factory.go pkg/client/grpc/factory_test.go
git commit -m "feat(client/grpc): expose per-call timeouts on Client interface"
```

---

## Task 3: Add the retry + timeout helpers

**Why:** A small generic helper makes the call-site changes in Tasks 4 and 5 readable. The classifier function lives in one place so we can extend (e.g. to recognise more retryable codes) later without touching every call site.

**Files:**
- Create: `pkg/client/io/retry.go`
- Create: `pkg/client/io/retry_test.go`

- [ ] **Step 1: Write the failing tests for the helpers**

Create `pkg/client/io/retry_test.go`:

```go
package io

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsRetryableGrpcError_Codes(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		want    bool
	}{
		{"nil", nil, false},
		{"non-grpc plain error", errors.New("boom"), false},
		{"Unavailable", status.Error(codes.Unavailable, "down"), true},
		{"DeadlineExceeded", status.Error(codes.DeadlineExceeded, "slow"), true},
		{"NotFound", status.Error(codes.NotFound, "no"), false},
		{"InvalidArgument", status.Error(codes.InvalidArgument, "bad"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isRetryableGrpcError(tt.err))
		})
	}
}

func TestRetryableCall_SucceedsFirstTry(t *testing.T) {
	calls := 0
	res, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 42, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 42, res)
	assert.Equal(t, 1, calls)
}

func TestRetryableCall_RetriesOnRetryableError(t *testing.T) {
	calls := 0
	res, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		if calls < 3 {
			return 0, status.Error(codes.Unavailable, "still down")
		}
		return 7, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, 7, res)
	assert.Equal(t, 3, calls)
}

func TestRetryableCall_GivesUpAfterMaxAttempts(t *testing.T) {
	calls := 0
	_, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "still down")
	})
	assert.Error(t, err)
	assert.Equal(t, 3, calls, "should attempt 3 times then stop")
}

func TestRetryableCall_DoesNotRetryNonRetryableError(t *testing.T) {
	calls := 0
	_, err := retryableCall(context.Background(), "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.NotFound, "missing")
	})
	assert.Error(t, err)
	assert.Equal(t, 1, calls)
}

func TestRetryableCall_RespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := retryableCall(ctx, "test", func(ctx context.Context) (int, error) {
		calls++
		return 0, status.Error(codes.Unavailable, "down")
	})
	assert.Error(t, err)
	// Could be 1 or 2 depending on timing — never the full 3.
	assert.Less(t, calls, 3)
}

func TestWithMetaTimeout_DerivesDeadline(t *testing.T) {
	parent := context.Background()
	ctx, cancel := withMetaTimeout(parent, 100*time.Millisecond)
	defer cancel()
	deadline, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.WithinDuration(t, time.Now().Add(100*time.Millisecond), deadline, 20*time.Millisecond)
}
```

- [ ] **Step 2: Run the tests and confirm they fail to compile**

```bash
go test -count=1 ./pkg/client/io/... 2>&1 | head -20
```

Expected: compile error — `undefined: retryableCall`, `undefined: isRetryableGrpcError`, `undefined: withMetaTimeout`.

- [ ] **Step 3: Implement the helpers**

Create `pkg/client/io/retry.go`:

```go
package io

import (
	"context"
	"time"

	"github.com/avast/retry-go/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryAttempts is the hardcoded number of attempts for idempotent RPCs.
// We keep this in code (not config) until evidence shows we need to tune it.
const (
	retryAttempts     = 3
	retryInitialDelay = 100 * time.Millisecond
	retryMaxDelay     = 1 * time.Second
)

// isRetryableGrpcError reports whether the given error came back from a gRPC
// call with a status code that indicates a transient failure safe to retry
// for an idempotent operation.
func isRetryableGrpcError(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// retryableCall invokes fn up to retryAttempts times with exponential
// backoff, retrying only when isRetryableGrpcError says so. The returned
// value/error is the result of the final attempt. The function is generic
// over the RPC reply type T so call sites read naturally.
//
// Use this ONLY for idempotent operations. Mutating operations (Write,
// Create, Mkdir, ...) must NOT use this until idempotency tokens land in
// Plan 1d, because a server-side success that fails to deliver its reply
// would otherwise be silently duplicated.
func retryableCall[T any](ctx context.Context, op string, fn func(context.Context) (T, error)) (T, error) {
	var result T
	err := retry.Do(
		func() error {
			r, err := fn(ctx)
			if err != nil {
				return err
			}
			result = r
			return nil
		},
		retry.RetryIf(isRetryableGrpcError),
		retry.Attempts(retryAttempts),
		retry.Delay(retryInitialDelay),
		retry.MaxDelay(retryMaxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.Context(ctx),
		retry.LastErrorOnly(true),
	)
	return result, err
}

// withMetaTimeout returns a context bounded by the configured metadata
// timeout. Callers must defer the returned cancel function.
func withMetaTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}

// withIOTimeout returns a context bounded by the configured I/O timeout.
// Callers must defer the returned cancel function.
func withIOTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, timeout)
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
go test -count=1 ./pkg/client/io/...
```

Expected: every test PASS, including the new retry tests. (The existing `pkg/client/io` tests should not be affected; they don't exercise this code path yet.)

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/retry.go pkg/client/io/retry_test.go
git commit -m "feat(client/io): add retryableCall + isRetryableGrpcError helpers"
```

---

## Task 4: Apply timeouts + retry in `pkg/client/io/fs.go`

**Why:** Every metadata RPC in `fs.go` currently passes `*fuse.Context` straight to the gRPC client with no deadline and no retry. After this task, every call has a per-RPC timeout, and idempotent reads survive a transient `Unavailable`.

**Files:**
- Modify: `pkg/client/io/fs.go`
- Modify: `pkg/client/io/fs_test.go` — add one regression test that retry actually fires for `GetAttr`.

This task touches 13 RPC call sites in `fs.go`. They split into two groups:

**Idempotent (timeout + retry):**
- `GetAttr`, `OpenDir`, `Access`, `GetXAttr`, `StatFs`

**Mutating (timeout only, no retry):**
- `Mkdir`, `Rmdir`, `Rename`, `Open`, `Create`, `Unlink`, `Truncate`, `Chmod`, `Chown`

(`Open` and `Create` are listed as mutating because `Create` creates a new file and a retry of `Open` after a partial success would leak fds; safer to treat both as non-retryable until Plan 1c's session work covers fd lifecycle.)

- [ ] **Step 1: Replace `GetAttr` with the timeout+retry pattern**

In `pkg/client/io/fs.go`, replace the existing `GetAttr` method:

```go
// GetAttr returns the attributes of a file.
func (fs *LocalFileSystem) GetAttr(name string, fctx *fuse.Context) (*fuse.Attr, fuse.Status) {
	if fctx == nil {
		// When mounting in another node through the connector, fctx is nil.
		fctx = &fuse.Context{
			Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
			Cancel: make(chan struct{}),
		}
	}
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "GetAttr", func(ctx context.Context) (*proto.GetAttrReply, error) {
		return fs.client.Fs().GetAttr(ctx, &proto.GetAttrRequest{
			Volume: fs.volume,
			Caller: createCaller(fctx),
			Path:   name,
		})
	})
	if err != nil {
		log.Log.Error("error in call: GetAttr", zap.String("path", name), zap.Error(err))
		return &fuse.Attr{}, fuse.EIO
	}
	if res.GetAttributes() == nil {
		return &fuse.Attr{}, fuse.Status(res.Status)
	}
	a := &fuse.Attr{
		Ino:    res.GetAttributes().Ino,
		Size:   res.GetAttributes().Size,
		Blocks: res.GetAttributes().Blocks,
		Atime:  res.GetAttributes().Atime,
		Mtime:  res.GetAttributes().Mtime,
		Ctime:  res.GetAttributes().Ctime,
		Mode:   res.GetAttributes().Mode,
		Nlink:  res.GetAttributes().Nlink,
		Owner: fuse.Owner{
			Uid: res.GetAttributes().Owner.Uid,
			Gid: res.GetAttributes().Owner.Gid,
		},
		Rdev:    res.GetAttributes().Rdev,
		Blksize: res.GetAttributes().Blksize,
		Padding: res.GetAttributes().Padding,
	}
	return a, fuse.Status(res.Status)
}
```

Note the rename of the parameter from `context` to `fctx` — this is necessary because the stdlib `context` package is referenced inside the closure. Apply the rename consistently in every method you edit below.

- [ ] **Step 2: Replace `OpenDir` (idempotent — timeout+retry)**

Replace `OpenDir`:

```go
func (fs *LocalFileSystem) OpenDir(name string, fctx *fuse.Context) (stream []fuse.DirEntry, code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "OpenDir", func(ctx context.Context) (*proto.OpenDirReply, error) {
		return fs.client.Fs().OpenDir(ctx, &proto.OpenDirRequest{
			Volume: fs.volume,
			Caller: createCaller(fctx),
			Path:   name,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: OpenDir", zap.String("path", name), zap.Error(err))
		return nil, fuse.EIO
	}
	var entries []fuse.DirEntry
	for _, entry := range res.Entries {
		entries = append(entries, fuse.DirEntry{
			Mode: entry.Mode,
			Name: entry.Name,
			Ino:  entry.Ino,
			Off:  entry.Off,
		})
	}
	return entries, fuse.Status(res.Status)
}
```

- [ ] **Step 3: Replace `Access` (idempotent — timeout+retry)**

```go
func (fs *LocalFileSystem) Access(name string, mode uint32, fctx *fuse.Context) (code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "Access", func(ctx context.Context) (*proto.AccessReply, error) {
		return fs.client.Fs().Access(ctx, &proto.AccessRequest{
			Volume: fs.volume,
			Caller: createCaller(fctx),
			Path:   name,
			Mode:   mode,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Access", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}
```

- [ ] **Step 4: Replace `GetXAttr` (idempotent — timeout+retry)**

```go
func (fs *LocalFileSystem) GetXAttr(name string, attribute string, fctx *fuse.Context) (data []byte, code fuse.Status) {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "GetXAttr", func(ctx context.Context) (*proto.GetXAttrReply, error) {
		return fs.client.Fs().GetXAttr(ctx, &proto.GetXAttrRequest{
			Volume: fs.volume, Caller: createCaller(fctx), Path: name, Attribute: attribute,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: GetXAttr", zap.String("path", name), zap.Error(err))
		return nil, fuse.EIO
	}
	return res.Data, fuse.Status(res.Status)
}
```

- [ ] **Step 5: Replace `StatFs` (idempotent — timeout+retry, no fuse.Context parent)**

`StatFs` is called by the FUSE kernel without a `*fuse.Context`. Use `context.Background()` as the parent:

```go
func (fs *LocalFileSystem) StatFs(name string) *fuse.StatfsOut {
	ctx, cancel := withMetaTimeout(context.Background(), fs.client.MetaTimeout())
	defer cancel()
	res, err := retryableCall(ctx, "StatFs", func(ctx context.Context) (*proto.StatFsReply, error) {
		return fs.client.Fs().StatFs(ctx, &proto.StatFsRequest{Volume: fs.volume, Path: name})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: StatFs", zap.String("path", name), zap.Error(err))
		return nil
	}
	stats := &fuse.StatfsOut{
		Blocks:  res.Blocks,
		Bfree:   res.Bfree,
		Bavail:  res.Bavail,
		Files:   res.Files,
		Ffree:   res.Ffree,
		Bsize:   res.Bsize,
		NameLen: res.Namelen,
		Frsize:  res.Frsize,
	}
	if len(res.Spare) == 6 {
		stats.Spare = [6]uint32{res.Spare[0], res.Spare[1], res.Spare[2], res.Spare[3], res.Spare[4], res.Spare[5]}
	}
	return stats
}
```

- [ ] **Step 6: Replace `Mkdir` (mutating — timeout only)**

```go
func (fs *LocalFileSystem) Mkdir(name string, mode uint32, fctx *fuse.Context) fuse.Status {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().Mkdir(ctx, &proto.MkdirRequest{
		Volume: fs.volume,
		Caller: createCaller(fctx),
		Path:   name,
		Mode:   mode,
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: MkDir", zap.String("path", name), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}
```

- [ ] **Step 7: Replace the remaining 8 mutating methods**

Apply the same pattern (timeout only, no retry) to:

- `Rmdir` (`proto.RmdirRequest`/`Reply`, error name `RmDir`)
- `Rename` (`proto.RenameRequest`/`Reply`)
- `Open` (`proto.OpenRequest`/`Reply`, returns `nodefs.File` — keep the existing post-call construction of `NewGrpcFile`)
- `Create` (`proto.CreateRequest`/`Reply`, same return-shape note as `Open`)
- `Unlink` (`proto.UnlinkRequest`/`Reply`)
- `Truncate` (`proto.TruncateRequest`/`Reply`)
- `Chmod` (`proto.ChmodRequest`/`Reply`)
- `Chown` (`proto.ChownRequest`/`Reply`)

For each, the shape is exactly:

```go
func (fs *LocalFileSystem) <Name>(<args>, fctx *fuse.Context) <retsig> {
	ctx, cancel := withMetaTimeout(fctx, fs.client.MetaTimeout())
	defer cancel()
	res, err := fs.client.Fs().<Name>(ctx, &proto.<Name>Request{
		Volume: fs.volume,
		Caller: createCaller(fctx),
		// ...the existing request fields
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: <Name>", zap.String("path", name), zap.Error(err))
		return <zero-value error return>
	}
	// ...the existing post-response handling (unchanged)
}
```

Note: `Open` and `Create` keep their existing branching for non-`OK` status. Don't simplify that out.

- [ ] **Step 8: Add the regression test for retry**

In `pkg/client/io/fs_test.go`, locate the existing `LocalFileSystemTestSuite` (find with `grep -n "LocalFileSystemTestSuite" pkg/client/io/fs_test.go`). Add a new test method to that suite. The test wires up the existing mocks so that `GetAttr` returns `Unavailable` once and then succeeds, and verifies the wrapped call returns success and the mock was called twice:

```go
// TestGetAttr_RetriesOnUnavailable verifies that an idempotent metadata
// call survives a single transient Unavailable error via the retry wrapper.
func (s *LocalFileSystemTestSuite) TestGetAttr_RetriesOnUnavailable() {
	// Two expectations on the same method: first call returns Unavailable,
	// second returns OK. testify/mock's Once() in order gives us per-call
	// behaviour.
	s.fs.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fs.EXPECT().GetAttr(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.GetAttrReply{
			Status:     int32(fuse.OK),
			Attributes: &proto.Attr{Mode: 0o644, Owner: &proto.Owner{Uid: 1000, Gid: 1000}},
		}, nil).Once()

	attr, st := s.localFs.GetAttr("file", &fuse.Context{
		Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1000},
		Cancel: make(chan struct{}),
	})

	s.Require().Equal(fuse.OK, st)
	s.NotNil(attr)
	s.fs.AssertNumberOfCalls(s.T(), "GetAttr", 2)
}
```

You may need to add imports `google.golang.org/grpc/codes`, `google.golang.org/grpc/status`, and `github.com/stretchr/testify/mock` to the test file if they're not already present. Adapt the mock variable names (`s.fs`, `s.localFs`) to match what the existing suite uses.

- [ ] **Step 9: Run the io tests**

```bash
go test -count=1 ./pkg/client/io/...
```

Expected: every test PASS, including the new retry regression. The existing tests should pass unchanged because the mocks accept any context (`mock.Anything`).

- [ ] **Step 10: Run the working-set tests to catch indirect breakage**

```bash
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

Expected: every package `ok`.

- [ ] **Step 11: Commit**

```bash
git add pkg/client/io/fs.go pkg/client/io/fs_test.go
git commit -m "feat(client/io): apply timeouts + retry to fs.go RPCs"
```

---

## Task 5: Apply timeouts + retry in `pkg/client/io/file.go`

**Why:** `GrpcFile` methods currently use `context.Background()` everywhere. A stalled server hangs the FUSE kernel thread until the OS gives up. After this task, every RPC has a deadline, and `Read` (the only idempotent op in this file) survives transient `Unavailable`.

**Files:**
- Modify: `pkg/client/io/file.go`
- Modify: `pkg/client/io/file_test.go` — add a regression test that retry fires for `Read`.

This task touches 9 RPC call sites in `file.go`:

**Idempotent (timeout + retry):**
- `Read`

**Mutating / state-changing (timeout only):**
- `Write`, `Release`, `Flush`, `Fsync`, `GetLk`, `SetLk`, `SetLkw`, `Allocate`

`GrpcFile` has no `*fuse.Context` available (the go-fuse `nodefs.File` interface doesn't pass one), so all calls use `context.Background()` as the parent.

- [ ] **Step 1: Replace `Read` with timeout + retry**

In `pkg/client/io/file.go`:

```go
func (f *GrpcFile) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx, "Read", func(ctx context.Context) (*proto.ReadReply, error) {
		return f.fileClient.Read(ctx, &proto.ReadRequest{
			Volume: f.volume,
			Fd:     f.fd,
			Offset: off,
			Size:   uint32(len(dest)),
		},
			grpc.UseCompressor(snappy.Name),
		)
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Read", zap.String("path", f.path), zap.Error(err))
		return nil, fuse.EIO
	}
	return fuse.ReadResultData(res.Bytes), fuse.Status(res.Status)
}
```

Note the use of `f.ioTimeout` — a new field we add in Step 2.

- [ ] **Step 2: Add timeout fields + constructor params to `GrpcFile`**

`GrpcFile` needs access to the I/O timeout (every RPC in `file.go` is a data/state op — none are metadata). Pass it in via the constructor:

```go
type GrpcFile struct {
	fileClient proto.RpcFileClient
	path       string
	volume     string
	fd         uint64
	ioTimeout  time.Duration
	nodefs.File
}

func NewGrpcFile(fileClient proto.RpcFileClient, volume, path string, fd uint64, ioTimeout time.Duration) *GrpcFile {
	return &GrpcFile{
		fileClient: fileClient,
		path:       path,
		volume:     volume,
		fd:         fd,
		ioTimeout:  ioTimeout,
		File:       nodefs.NewDefaultFile(),
	}
}
```

Add `"time"` to the imports.

Then update the two call sites in `fs.go` (`Open` and `Create`) — they currently call `NewGrpcFile(fs.client.File(), fs.volume, name, res.Fd)`; change them to `NewGrpcFile(fs.client.File(), fs.volume, name, res.Fd, fs.client.IOTimeout())`.

Also adjust any other callers of `NewGrpcFile` (grep with `grep -rn "NewGrpcFile" --include="*.go"` to verify the only callers are the two in `fs.go` and the existing test).

- [ ] **Step 3: Replace `Write` (timeout only — mutating)**

```go
func (f *GrpcFile) Write(data []byte, off int64) (written uint32, code fuse.Status) {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.Write(ctx, &proto.WriteRequest{
		Volume: f.volume,
		Fd:     f.fd,
		Offset: off,
		Bytes:  data,
	},
		grpc.UseCompressor(snappy.Name),
	)
	if err != nil || res == nil {
		log.Log.Error("error in call: Write", zap.String("path", f.path), zap.Error(err))
		return 0, fuse.EIO
	}
	return res.Written, fuse.Status(res.Status)
}
```

- [ ] **Step 4: Replace the remaining 7 mutating/state methods**

For each of `Release`, `Flush`, `Fsync`, `GetLk`, `SetLk`, `SetLkw`, `Allocate`: wrap the call in `withIOTimeout(context.Background(), f.ioTimeout)` with a `defer cancel()` and pass the derived `ctx` to the gRPC call instead of `context.Background()`. The body shape is:

```go
func (f *GrpcFile) <Name>(<args>) <retsig> {
	ctx, cancel := withIOTimeout(context.Background(), f.ioTimeout)
	defer cancel()
	res, err := f.fileClient.<Name>(ctx, &proto.<Name>Request{
		Volume: f.volume,
		Fd:     f.fd,
		// ...existing fields
	})
	// ...existing error-handling and response-marshalling, unchanged
}
```

Note: `Release` does not currently return an error; its body just logs on failure. Preserve that behaviour — only add the timeout wrap.

For `GetLk`, `SetLk`, `SetLkw` (locking operations), they're not strictly mutating but they're state-changing and not safe to retry. Treat them as mutating: timeout only.

- [ ] **Step 5: Add a retry regression test for `Read`**

In `pkg/client/io/file_test.go`, find the existing `GrpcFileTestSuite` (or whatever it's called) and add:

```go
// TestRead_RetriesOnUnavailable verifies that Read survives a single
// transient Unavailable.
func (s *GrpcFileTestSuite) TestRead_RetriesOnUnavailable() {
	dest := make([]byte, 10)

	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(nil, status.Error(codes.Unavailable, "down")).Once()
	s.fileClient.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		Return(&proto.ReadReply{
			Bytes:  []byte("0123456789"),
			Status: int32(fuse.OK),
		}, nil).Once()

	result, st := s.grpcFile.Read(dest, 0)

	s.Require().Equal(fuse.OK, st)
	s.NotNil(result)
	s.fileClient.AssertNumberOfCalls(s.T(), "Read", 2)
}
```

Adapt mock variable names (`s.fileClient`, `s.grpcFile`) to whatever the existing suite uses. Add imports for `codes`, `status`, and `mock` if absent.

- [ ] **Step 6: Update existing GrpcFile tests to pass the timeout args**

Any existing constructor call to `NewGrpcFile(...)` in `pkg/client/io/file_test.go` now needs two extra args. The simplest in-test default is `5*time.Second, 30*time.Second`. Grep to find them:

```bash
grep -n "NewGrpcFile" pkg/client/io/file_test.go
```

For each match, append `, 30*time.Second` before the closing paren and add `"time"` to imports if missing.

- [ ] **Step 7: Run the io tests**

```bash
go test -count=1 ./pkg/client/io/...
```

Expected: every test PASS, including the new retry test.

- [ ] **Step 8: Run the working-set tests**

```bash
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

Expected: every package `ok`.

- [ ] **Step 9: Commit**

```bash
git add pkg/client/io/file.go pkg/client/io/file_test.go pkg/client/io/fs.go
git commit -m "feat(client/io): apply timeouts + retry to file.go RPCs"
```

---

## Task 6: Final validation

**Why:** Cross-check that nothing was missed, no `context.Background()` survives in the call paths we touched, and the working-set + build are green before handing off.

- [ ] **Step 1: Confirm no naked `context.Background()` remains in fs.go or file.go RPC paths**

```bash
grep -n "context.Background()" pkg/client/io/fs.go pkg/client/io/file.go
```

Expected: zero matches. (If `StatFs` is grepped, it should be wrapped in `withMetaTimeout(context.Background(), ...)` — that's the legitimate use; the substring still appears but in the right context. Inspect any match by hand to confirm.)

- [ ] **Step 2: Run the working-set tests**

```bash
go test -count=1 ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

Expected: every package `ok`.

- [ ] **Step 3: Build everything**

```bash
go build ./cmd/... ./pkg/server/... ./pkg/client/... ./pkg/common/...
```

Expected: silent. UI build failures (GTK) are environment-only, ignore.

- [ ] **Step 4: Hand off to user for plain-terminal validation**

The FUSE-touching tests (`pkg/client/mount/...`, `test/e2e/fs/...`) cannot be validated from the Claude sandbox or GoLand's terminal. Ask the user to run from a plain terminal:

```
task test
task lint
```

Output to user:

```
Plan 1b complete. Please run from a plain terminal:
    task test
    task lint
to validate the FUSE-mount tests and the lint suite, which I can't run here.
```

- [ ] **Step 5: Verify the commit log**

```bash
git log --oneline 67ae567..HEAD
```

Expected: five new commits (Task 1, Task 2, Task 3, Task 4, Task 5), each with the plain `feat(...)` or `feat(...)` style message and no trailers.

---

## Self-Review Notes (don't modify — for plan reader)

**Spec coverage:** Phase 1 roadmap items #4 (context propagation with timeouts) and #7 (retry on idempotent I/O) are both addressed. Item #4's "FUSE-thread context" requirement is satisfied: `fs.go` calls now wrap the existing `*fuse.Context` (which implements `context.Context`) with a per-call deadline, and `file.go` calls wrap `context.Background()` with a per-call deadline (no fuse context is available at the `nodefs.File` interface boundary).

**Placeholder scan:** No "TBD" / "implement later" / "similar to" wording. Each task has either the full code block or a small mechanical pattern repeated with an explicit list of call sites to apply it to.

**Type consistency:**
- `RpcConfig` has `TimeoutMeta` and `TimeoutIO` (not `TimeoutMetadata`/`TimeoutData`) — used consistently across config, factory, client, fs.go, file.go.
- `Client.MetaTimeout()` / `Client.IOTimeout()` — used consistently from `fs.go` and (via `GrpcFile.metaTimeout`/`ioTimeout` injected at construction time) from `file.go`.
- `retryableCall[T]` is generic; all call sites instantiate it with the gRPC reply type.
- `withMetaTimeout(parent, d)` / `withIOTimeout(parent, d)` — both take an explicit duration so the helpers themselves don't depend on `Client` and stay testable in isolation.

**Scope boundaries kept:**
- No proto changes. No session work. No idempotency tokens. (Plans 1c and 1d cover those.)
- No retry on mutating RPCs (writes/creates/etc.) — would be unsafe without idempotency tokens.
- Hardcoded retry params (3 attempts, 100ms→1s backoff) — config knobs deferred until evidence requires them.
