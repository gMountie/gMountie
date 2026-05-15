# Phase 2 — Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enough instrumentation that the next time gMountie is slow, broken, or weird, the answer is in a log line or a metric. A client log line and the matching server log line can be joined by `request_id`; Prometheus shows per-volume/per-op rates, errors, and latencies; `grpc_health_probe` reports SERVING; `curl /version` returns build info.

**Architecture:** A new `pkg/common/grpc/requestid.go` defines context keys and helpers. Two new gRPC interceptors — `RequestIDInterceptor` (client + server pair) and `LogContextInterceptor` (server, peeks at `session_id`/`volume` getters on the request) — populate context with the values we want in every log line. A small wrapper around the existing `InterceptorLogger` adds `logging.WithFieldsFromContext` so finish-call lines automatically pick those up. The logger init becomes config-driven: `LogConfig{Format, Level}` with default `format` = `console` when stderr is a TTY else `json`. Metrics are split into `pkg/server/metrics/` and `pkg/client/metrics/`; each exposes typed accessors that interceptors and controllers call. The HTTP "ops" server in `pkg/server/grpc/server.go` grows endpoints (`/metrics`, `/healthz`, `/readyz`, `/version`) and its bind address moves to config as `Server.MetricsAddr` (kept named for backwards-compat with the existing `Server.Metrics` toggle). The gRPC health service is registered via `google.golang.org/grpc/health`; readiness flips to NOT_SERVING during graceful shutdown.

**Tech Stack:** Go 1.26, protobuf v1.36.11, `go.uber.org/zap`, `github.com/prometheus/client_golang`, `github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging`, `google.golang.org/grpc/health`, `golang.org/x/term` (for TTY detection).

---

## Scope context (read once, then forget)

- This implements Phase 2 of `docs/superpowers/specs/2026-05-13-roadmap-reliability-and-performance.md`. Phase 1 (1a/1b/1c/1d) is merged on `develop`.
- The 6 in-scope items map to Tasks 1–7 below (request-id and context-aware logging split across Tasks 2a/2b for clarity).
- **Explicitly deferred:** OpenTelemetry tracing (log-correlation IDs cover most of the value at a fraction of complexity); auth on the metrics endpoint (Phase 7 / security).
- The `layering-service-features` skill applies: new HTTP routes (`/healthz`, `/readyz`, `/version`) are thin handlers that should call into small ops-service types. Metrics live in their own `metrics` packages (io-layer) consumed by interceptors/services.
- Tests run in the sandbox for non-FUSE packages. Full validation (`task -t testing/scratch/Taskfile.yml test`) on the kubevirt VM.
- Backwards compatibility is not a concern.
- testify suites; conventional-commit subject + descriptive body; no `Co-Authored-By:` / `Signed-off-by:` / 🤖 trailers; `internal/mocks/` regenerated via `task gen:mocks` only.

## File Structure

**Logger:**
- **Modify:** `pkg/utils/log/log.go` — `LogConfig`, `Reconfigure(cfg)`, TTY detection.
- **Create:** `pkg/utils/log/log_test.go` — TTY/non-TTY/explicit-override + level cases.
- **Modify:** `pkg/server/config/server.go` and `pkg/server/config/config.go` — `Log *LogConfig` field on server `Config`; parser fills it.
- **Modify:** `pkg/client/config/config.go` — same field on client `Config`.
- **Modify:** `pkg/server/app.go` — call `log.Reconfigure(cfg.Log)` in `Start`.
- **Modify:** `pkg/client/grpc/factory.go` — call `log.Reconfigure(cfg.Log)` if non-nil.

**Request-ID + context-aware logging:**
- **Create:** `pkg/common/grpc/requestid.go` — context key, `FromContext`, `NewContext`, metadata key constant `gmountie-request-id`.
- **Create:** `pkg/common/grpc/interceptor_requestid.go` — `ServerUnaryRequestID()` and `ClientUnaryRequestID()` interceptors.
- **Create:** `pkg/common/grpc/log_fields.go` — context keys for `volume`, `session_id`, `user`; `FieldsFromContext(ctx) logging.Fields` adapter for `logging.WithFieldsFromContext`.
- **Create:** `pkg/common/grpc/interceptor_log_fields.go` — server unary interceptor that peeks at `req.GetSessionId()`/`req.GetVolume()` via type assertion and stamps context.
- **Create:** `pkg/common/grpc/requestid_test.go`, `interceptor_requestid_test.go`, `log_fields_test.go`, `interceptor_log_fields_test.go`.
- **Modify:** `pkg/common/grpc/logger.go` — wire `logging.WithFieldsFromContext(FieldsFromContext)` into the InterceptorLogger options builder (or expose option helpers).
- **Modify:** `pkg/server/grpc/server.go` — wire `ServerUnaryRequestID` + `LogContextInterceptor` into the unary chain BEFORE the auth interceptor (auth needs request_id on context).
- **Modify:** `pkg/client/grpc/client.go` — wire `ClientUnaryRequestID` into the client's interceptor chain (uncomment + augment the existing slot).
- **Modify:** `pkg/server/grpc/auth.go` (or wherever auth lives) — stamp `user` onto context.

**Server metrics:**
- **Create:** `pkg/server/metrics/metrics.go` — collectors + accessors (`OpenFiles`, `Bytes`, `RpcErrors`, `RequestDuration`, `SessionsActive`).
- **Create:** `pkg/server/metrics/metrics_test.go` — register/scrape round-trip.
- **Create:** `pkg/server/grpc/interceptor_metrics.go` — unary interceptor that times calls, records error counter per `(volume, op, code)`.
- **Create:** `pkg/server/grpc/interceptor_metrics_test.go`.
- **Modify:** `pkg/server/controller/file.go` — increment `Bytes{out}` after successful Read, `Bytes{in}` after successful Write; increment `OpenFiles` on Open/Create OK, decrement on Release.
- **Modify:** `pkg/server/service/session.go` — `SessionsActive` inc on Create, dec on reap/Stop.

**Client metrics:**
- **Create:** `pkg/client/metrics/metrics.go` — `RetryTotal`, `InFlight` collectors.
- **Create:** `pkg/client/metrics/metrics_test.go`.
- **Modify:** `pkg/client/io/retry.go` — bump `RetryTotal` from `retry.OnRetry` hook.
- **Create:** `pkg/client/grpc/interceptor_metrics.go` — unary interceptor that inc/dec `InFlight{op}`.
- **Modify:** `pkg/client/grpc/client.go` — wire the interceptor.

**Health & version:**
- **Create:** `pkg/server/grpc/health.go` — `grpc/health/v1` registration helper + `SetServing(bool)` shim used by graceful-shutdown.
- **Create:** `pkg/server/ops/server.go` — HTTP server that mounts `/metrics`, `/healthz`, `/readyz`, `/version`. (Owns no business logic — pure routing.)
- **Create:** `pkg/server/ops/handlers.go` — the 4 handlers.
- **Create:** `pkg/server/ops/readiness.go` — a tiny `ReadinessChecker` service interface + default impl that Stats one configured probe path (default first volume root).
- **Create:** `pkg/server/ops/handlers_test.go` and `pkg/server/ops/readiness_test.go`.
- **Modify:** `pkg/server/grpc/server.go` — register health service into the gRPC server; flip to NOT_SERVING in `Stop`. Replace the inline `/metrics`-only goroutine with `ops.NewServer(...)`.
- **Modify:** `pkg/server/app.go` — wire `ReadinessChecker` into the ops server.

**Version proto + handler:**
- **Create:** `api/proto/version.proto` — `VersionService` with `Get(VersionRequest) → VersionReply`.
- **Regen:** `pkg/proto/version.pb.go`, `pkg/proto/version_grpc.pb.go`, `internal/mocks/pkg/proto/*Version*`.
- **Create:** `pkg/server/controller/version.go` — gRPC handler (~6 lines, pure passthrough — see skill exception).
- **Create:** `pkg/server/controller/version_test.go`.
- **Modify:** `pkg/server/app.go` — register the version controller.

**Configurable ops port:**
- **Modify:** `pkg/server/config/server.go` — add `MetricsAddr string` (default `":9090"`, validated as `hostname_port`).
- **Modify:** `pkg/server/config/config.go` — bind/read `server.metrics_addr` env var.

**Working-set test command (sandbox):**
```
go test -count=1 ./pkg/utils/log/... ./pkg/common/grpc/... ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/client/metrics/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

**Full validation (kubevirt VM):**
```
task -t testing/scratch/Taskfile.yml test
```

---

## Task 1: Config-driven logger with TTY detection

**Why:** Today `pkg/utils/log/log.go` hardcodes the `console` encoder in an `init()` and ignores config. JSON logs are the prerequisite for grep-by-request-id; auto-detecting TTY keeps the local dev experience pleasant.

**Files:**
- Modify: `pkg/utils/log/log.go`
- Create: `pkg/utils/log/log_test.go`
- Modify: `pkg/server/config/server.go`, `pkg/server/config/config.go`
- Modify: `pkg/client/config/config.go`
- Modify: `pkg/server/app.go`
- Modify: `pkg/client/grpc/factory.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/utils/log/log_test.go`:

```go
package log

import (
    "bytes"
    "os"
    "testing"

    "github.com/stretchr/testify/suite"
    "go.uber.org/zap/zapcore"
)

type LogTestSuite struct {
    suite.Suite
}

func (s *LogTestSuite) TestDefaultIsConsoleWhenStderrTTY() {
    // We can't fake a real TTY here; just verify the auto-detect helper
    // returns "console" when isTTY=true and "json" when isTTY=false.
    s.Assert().Equal("console", chooseFormat("", true))
    s.Assert().Equal("json", chooseFormat("", false))
}

func (s *LogTestSuite) TestExplicitFormatOverrides() {
    s.Assert().Equal("json", chooseFormat("json", true))
    s.Assert().Equal("console", chooseFormat("console", false))
}

func (s *LogTestSuite) TestReconfigureSwitchesEncoder() {
    var buf bytes.Buffer
    err := Reconfigure(LogConfig{Format: "json", Level: "info"}, &buf)
    s.Require().NoError(err)
    Log.Info("hello", zapcore.Field{}) // any field; the message is what we check
    s.Assert().Contains(buf.String(), `"msg":"hello"`, "JSON encoder must produce structured output")
}

func (s *LogTestSuite) TestReconfigureRejectsUnknownLevel() {
    err := Reconfigure(LogConfig{Format: "json", Level: "shouty"}, os.Stderr)
    s.Require().Error(err)
}

func TestLogTestSuite(t *testing.T) {
    suite.Run(t, new(LogTestSuite))
}
```

- [ ] **Step 2: Run, verify fail**

Run: `go test -v ./pkg/utils/log/`
Expected: build failure — `chooseFormat`, `Reconfigure`, `LogConfig` undefined.

- [ ] **Step 3: Rewrite `pkg/utils/log/log.go`**

```go
package log

import (
    "io"
    "log"
    "os"

    "github.com/pkg/errors"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "golang.org/x/term"
)

// LogConfig controls runtime logger behaviour. Both fields are optional;
// zero values trigger auto-detect / defaults.
type LogConfig struct {
    // Format selects the encoder. "console" (human-friendly), "json"
    // (machine-friendly), or "" to auto-detect: console if stderr is a
    // TTY, json otherwise.
    Format string `mapstructure:"format"`
    // Level is "debug" | "info" | "warn" | "error". Empty → info.
    Level string `mapstructure:"level"`
}

var Log *zap.Logger

func init() {
    // Sensible default until Reconfigure is called: auto-detect against
    // os.Stderr. Tests and callers that want different output should call
    // Reconfigure explicitly.
    if err := Reconfigure(LogConfig{}, os.Stderr); err != nil {
        // Last-ditch fallback so the binary still produces logs.
        Log = zap.NewNop()
        return
    }
}

// chooseFormat resolves the encoder name from config + TTY state.
func chooseFormat(configured string, isTTY bool) string {
    if configured != "" {
        return configured
    }
    if isTTY {
        return "console"
    }
    return "json"
}

func parseLevel(s string) (zapcore.Level, error) {
    if s == "" {
        return zapcore.InfoLevel, nil
    }
    var lvl zapcore.Level
    if err := lvl.UnmarshalText([]byte(s)); err != nil {
        return 0, errors.Wrapf(err, "parse log level %q", s)
    }
    return lvl, nil
}

// Reconfigure rebuilds the package logger from cfg. `sink` is where output
// goes; in production callers pass os.Stderr.
func Reconfigure(cfg LogConfig, sink io.Writer) error {
    lvl, err := parseLevel(cfg.Level)
    if err != nil {
        return err
    }
    isTTY := false
    if f, ok := sink.(*os.File); ok {
        isTTY = term.IsTerminal(int(f.Fd()))
    }
    format := chooseFormat(cfg.Format, isTTY)

    var encCfg zapcore.EncoderConfig
    var enc zapcore.Encoder
    if format == "console" {
        encCfg = zap.NewDevelopmentEncoderConfig()
        enc = zapcore.NewConsoleEncoder(encCfg)
    } else {
        encCfg = zap.NewProductionEncoderConfig()
        encCfg.TimeKey = "ts"
        encCfg.MessageKey = "msg"
        encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
        enc = zapcore.NewJSONEncoder(encCfg)
    }
    core := zapcore.NewCore(enc, zapcore.AddSync(sink), lvl)
    Log = zap.New(core, zap.AddCaller()).Named("gMountie")
    zap.ReplaceGlobals(Log)

    // Redirect stdlib log to zap.
    stdLogger, err := zap.NewStdLogAt(Log.Named("std"), zapcore.DebugLevel)
    if err != nil {
        return errors.Wrap(err, "wire stdlib log")
    }
    log.Default().SetOutput(stdLogger.Writer())
    return nil
}
```

- [ ] **Step 4: Add `golang.org/x/term` if missing**

Run: `go get golang.org/x/term && go mod tidy`

- [ ] **Step 5: Run tests, watch pass**

Run: `go test -v ./pkg/utils/log/`
Expected: 4 cases PASS.

- [ ] **Step 6: Add `Log *LogConfig` to server config**

In `pkg/server/config/server.go`, leave `ServerConfig` alone; the log config lives at the top level next to `Server`, `Auth`, `Volumes`.

In `pkg/server/config/config.go`:

```go
type Config struct {
    Server  *ServerConfig         `validate:"required"`
    Auth    AuthConfig            `validate:"required"`
    Volumes []*VolumeConfig       `validate:"required,dive"`
    Log     *log.LogConfig        // optional; nil → auto-detect defaults
}
```

(Add `"gmountie/pkg/utils/log"` to imports.)

In `ParseConfig`, after the existing server/auth/volume parsing, add:

```go
v.SetDefault("log.format", "")
v.SetDefault("log.level", "")
_ = v.BindEnv("log.format")
_ = v.BindEnv("log.level")
result.Log = &log.LogConfig{
    Format: v.GetString("log.format"),
    Level:  v.GetString("log.level"),
}
```

- [ ] **Step 7: Mirror on client config**

In `pkg/client/config/config.go`, add the same `Log *log.LogConfig` field and parse it from `log.*` keys, with the same defaults and env-binding pattern.

- [ ] **Step 8: Wire `Reconfigure` into `Start`**

In `pkg/server/app.go`, at the top of `Start` (before `NewServerAppContext`):

```go
if cfg.Log != nil {
    if err := log.Reconfigure(*cfg.Log, os.Stderr); err != nil {
        return errors.Wrap(err, "configure logger")
    }
}
```

Add `"os"` and the existing `log` / `errors` imports as needed.

Mirror in `pkg/client/grpc/factory.go::NewClientFromConfig` (early, before construction). Use the same shape.

- [ ] **Step 9: Run all dependent tests**

Run: `go test -count=1 ./pkg/utils/log/... ./pkg/server/... ./pkg/client/config/... ./pkg/client/grpc/...`
Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add pkg/utils/log/ pkg/server/config/ pkg/client/config/ pkg/server/app.go pkg/client/grpc/factory.go go.mod go.sum
git commit -m "$(cat <<'EOF'
feat(log): config-driven logger with TTY auto-detect

LogConfig{Format,Level} drives the encoder selection. Format defaults
to "console" when stderr is a TTY (preserving the local dev
experience) and "json" otherwise — JSON is the prerequisite for
greppable per-RPC log lines that land in the next task. Reconfigure
is called at the top of server.Start and client.NewClientFromConfig
so the binary picks up whatever is in the file/env before any other
package logs.
EOF
)"
```

---

## Task 2a: Request-ID interceptors (server + client)

**Why:** Half of "grep one request_id and see both sides" is generating + propagating the id. The other half (attaching it to log lines) lands in Task 2b.

**Files:**
- Create: `pkg/common/grpc/requestid.go`
- Create: `pkg/common/grpc/requestid_test.go`
- Create: `pkg/common/grpc/interceptor_requestid.go`
- Create: `pkg/common/grpc/interceptor_requestid_test.go`
- Modify: `pkg/server/grpc/server.go` (wire interceptor)
- Modify: `pkg/client/grpc/client.go` (wire interceptor)

- [ ] **Step 1: Write the failing test for the context helpers**

```go
// pkg/common/grpc/requestid_test.go
package grpc

import (
    "context"
    "testing"

    "github.com/stretchr/testify/suite"
)

type RequestIDTestSuite struct{ suite.Suite }

func (s *RequestIDTestSuite) TestFromContextEmptyByDefault() {
    s.Assert().Empty(RequestIDFromContext(context.Background()))
}

func (s *RequestIDTestSuite) TestNewContextRoundTrip() {
    ctx := NewContextWithRequestID(context.Background(), "abc-123")
    s.Assert().Equal("abc-123", RequestIDFromContext(ctx))
}

func TestRequestIDTestSuite(t *testing.T) { suite.Run(t, new(RequestIDTestSuite)) }
```

- [ ] **Step 2: Implement `pkg/common/grpc/requestid.go`**

```go
package grpc

import "context"

// RequestIDMetadataKey is the gRPC metadata key used for the per-RPC
// request id. Lowercased to match gRPC's metadata canonicalization.
const RequestIDMetadataKey = "gmountie-request-id"

type ctxKeyRequestID struct{}

// RequestIDFromContext returns the request id stamped on ctx, or "" if
// none is set.
func RequestIDFromContext(ctx context.Context) string {
    if v, ok := ctx.Value(ctxKeyRequestID{}).(string); ok {
        return v
    }
    return ""
}

// NewContextWithRequestID returns a derived context that carries id.
func NewContextWithRequestID(ctx context.Context, id string) context.Context {
    return context.WithValue(ctx, ctxKeyRequestID{}, id)
}
```

Run: `go test -v ./pkg/common/grpc/ -run TestRequestIDTestSuite` → PASS.

- [ ] **Step 3: Write the interceptor test**

```go
// pkg/common/grpc/interceptor_requestid_test.go
package grpc

import (
    "context"
    "testing"

    "github.com/stretchr/testify/suite"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
)

type RequestIDInterceptorTestSuite struct{ suite.Suite }

func (s *RequestIDInterceptorTestSuite) TestServerGeneratesIDWhenMissing() {
    interceptor := ServerUnaryRequestID()
    info := &grpc.UnaryServerInfo{FullMethod: "/Test/Method"}
    var seen string
    handler := func(ctx context.Context, req any) (any, error) {
        seen = RequestIDFromContext(ctx)
        return nil, nil
    }
    _, err := interceptor(context.Background(), nil, info, handler)
    s.Require().NoError(err)
    s.Assert().NotEmpty(seen, "server must generate a request id when client supplied none")
}

func (s *RequestIDInterceptorTestSuite) TestServerHonoursClientID() {
    interceptor := ServerUnaryRequestID()
    info := &grpc.UnaryServerInfo{FullMethod: "/Test/Method"}
    var seen string
    handler := func(ctx context.Context, req any) (any, error) {
        seen = RequestIDFromContext(ctx)
        return nil, nil
    }
    md := metadata.Pairs(RequestIDMetadataKey, "from-client")
    ctx := metadata.NewIncomingContext(context.Background(), md)
    _, err := interceptor(ctx, nil, info, handler)
    s.Require().NoError(err)
    s.Assert().Equal("from-client", seen)
}

func (s *RequestIDInterceptorTestSuite) TestClientGeneratesAndPropagates() {
    interceptor := ClientUnaryRequestID()
    var outgoingID string
    invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
        md, ok := metadata.FromOutgoingContext(ctx)
        s.Require().True(ok)
        outgoingID = md.Get(RequestIDMetadataKey)[0]
        return nil
    }
    err := interceptor(context.Background(), "/Test/Method", nil, nil, nil, invoker)
    s.Require().NoError(err)
    s.Assert().NotEmpty(outgoingID)
}

func (s *RequestIDInterceptorTestSuite) TestClientPreservesExistingID() {
    interceptor := ClientUnaryRequestID()
    var outgoingID string
    invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
        md, _ := metadata.FromOutgoingContext(ctx)
        outgoingID = md.Get(RequestIDMetadataKey)[0]
        return nil
    }
    ctx := NewContextWithRequestID(context.Background(), "pre-set")
    err := interceptor(ctx, "/Test/Method", nil, nil, nil, invoker)
    s.Require().NoError(err)
    s.Assert().Equal("pre-set", outgoingID)
}

func TestRequestIDInterceptorTestSuite(t *testing.T) {
    suite.Run(t, new(RequestIDInterceptorTestSuite))
}
```

Run → expect build failure.

- [ ] **Step 4: Implement `pkg/common/grpc/interceptor_requestid.go`**

```go
package grpc

import (
    "context"

    "github.com/google/uuid"
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
)

// ServerUnaryRequestID extracts the request id from incoming metadata if
// present, otherwise generates a fresh UUID. The id is stashed on the
// handler's context via RequestIDFromContext.
func ServerUnaryRequestID() grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        id := ""
        if md, ok := metadata.FromIncomingContext(ctx); ok {
            if vals := md.Get(RequestIDMetadataKey); len(vals) > 0 && vals[0] != "" {
                id = vals[0]
            }
        }
        if id == "" {
            id = uuid.NewString()
        }
        return handler(NewContextWithRequestID(ctx, id), req)
    }
}

// ClientUnaryRequestID injects a request id into outgoing metadata. If
// the context already carries one (RequestIDFromContext), it is reused;
// otherwise a fresh UUID is generated.
func ClientUnaryRequestID() grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        id := RequestIDFromContext(ctx)
        if id == "" {
            id = uuid.NewString()
            ctx = NewContextWithRequestID(ctx, id)
        }
        ctx = metadata.AppendToOutgoingContext(ctx, RequestIDMetadataKey, id)
        return invoker(ctx, method, req, reply, cc, opts...)
    }
}
```

Run the suite. All 4 cases PASS.

- [ ] **Step 5: Wire into server**

In `pkg/server/grpc/server.go::getOptions`, prepend `ServerUnaryRequestID()` to the unary chain so it runs first (auth and logging interceptors see request_id):

```go
unaryInterceptors := append(
    []grpc.UnaryServerInterceptor{
        grpc2.ServerUnaryRequestID(),  // FIRST — populate ctx with request_id
        authInterceptor.Unary(),
        unaryLog,
    },
    s.extraUnaryInterceptors...,
)
```

(Where `grpc2` is the existing alias for `gmountie/pkg/common/grpc`.)

- [ ] **Step 6: Wire into client**

In `pkg/client/grpc/client.go::getInterceptors`, replace the commented-out line with:

```go
return []grpc.UnaryClientInterceptor{
    commongrpc.ClientUnaryRequestID(),
}
```

(Add `commongrpc "gmountie/pkg/common/grpc"` to the imports if not present.)

- [ ] **Step 7: Run all tests**

Run: `go test -race -count=2 ./pkg/common/grpc/... ./pkg/server/grpc/... ./pkg/client/grpc/...`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/common/grpc/requestid.go pkg/common/grpc/interceptor_requestid.go pkg/common/grpc/requestid_test.go pkg/common/grpc/interceptor_requestid_test.go pkg/server/grpc/server.go pkg/client/grpc/client.go
git commit -m "$(cat <<'EOF'
feat(grpc): per-RPC request id interceptors + metadata propagation

Server interceptor reads request_id from incoming metadata or generates
a fresh UUID; stashes it on the handler context. Client interceptor
injects request_id into outgoing metadata, preferring any id already
on context so the same logical operation keeps the same id across
retries (idempotency cache hit fast-path).

Wired first in the unary chain on both sides so downstream interceptors
(auth, logging) and handler code can read the id. The log-fields hook
that surfaces this id on every log line lands in the next task.
EOF
)"
```

---

## Task 2b: Context-aware log fields (request_id, session_id, volume, user)

**Why:** Now that request_id is on context, surface it (plus session_id, volume, user) on every log line so the DoD "grep request_id → every line for that RPC, both sides" holds.

**Files:**
- Create: `pkg/common/grpc/log_fields.go`
- Create: `pkg/common/grpc/log_fields_test.go`
- Create: `pkg/common/grpc/interceptor_log_fields.go`
- Create: `pkg/common/grpc/interceptor_log_fields_test.go`
- Modify: `pkg/server/grpc/server.go` — pass `logging.WithFieldsFromContext` to the logging interceptor; wire the LogContext interceptor.
- Modify: `pkg/server/grpc/auth.go` (or wherever the auth interceptor lives) — stamp `user` on context.
- Modify: `pkg/client/grpc/client.go` — same `WithFieldsFromContext` plumbing for the client logger.

- [ ] **Step 1: Write the field-extractor test**

```go
// pkg/common/grpc/log_fields_test.go
package grpc

import (
    "context"
    "testing"

    "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
    "github.com/stretchr/testify/suite"
)

type LogFieldsTestSuite struct{ suite.Suite }

func (s *LogFieldsTestSuite) TestFieldsFromContextEmpty() {
    f := FieldsFromContext(context.Background())
    s.Assert().Empty(f)
}

func (s *LogFieldsTestSuite) TestFieldsFromContextPopulated() {
    ctx := context.Background()
    ctx = NewContextWithRequestID(ctx, "req-1")
    ctx = NewContextWithSessionID(ctx, "sess-1")
    ctx = NewContextWithVolume(ctx, "photos")
    ctx = NewContextWithUser(ctx, "alice")

    f := FieldsFromContext(ctx)
    // logging.Fields is []any of alternating key, value.
    s.Assert().Contains(f, "request_id")
    s.Assert().Contains(f, "req-1")
    s.Assert().Contains(f, "session_id")
    s.Assert().Contains(f, "sess-1")
    s.Assert().Contains(f, "volume")
    s.Assert().Contains(f, "photos")
    s.Assert().Contains(f, "user")
    s.Assert().Contains(f, "alice")
    // sanity — logging.Fields keeps key/value adjacency.
    s.Assert().IsType(logging.Fields{}, f)
}

func TestLogFieldsTestSuite(t *testing.T) { suite.Run(t, new(LogFieldsTestSuite)) }
```

- [ ] **Step 2: Implement `pkg/common/grpc/log_fields.go`**

```go
package grpc

import (
    "context"

    "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
)

type (
    ctxKeySessionID struct{}
    ctxKeyVolume    struct{}
    ctxKeyUser      struct{}
)

func NewContextWithSessionID(ctx context.Context, id string) context.Context {
    return context.WithValue(ctx, ctxKeySessionID{}, id)
}
func SessionIDFromContext(ctx context.Context) string {
    v, _ := ctx.Value(ctxKeySessionID{}).(string)
    return v
}

func NewContextWithVolume(ctx context.Context, v string) context.Context {
    return context.WithValue(ctx, ctxKeyVolume{}, v)
}
func VolumeFromContext(ctx context.Context) string {
    v, _ := ctx.Value(ctxKeyVolume{}).(string)
    return v
}

func NewContextWithUser(ctx context.Context, u string) context.Context {
    return context.WithValue(ctx, ctxKeyUser{}, u)
}
func UserFromContext(ctx context.Context) string {
    v, _ := ctx.Value(ctxKeyUser{}).(string)
    return v
}

// FieldsFromContext extracts the standard log fields from ctx for use
// with logging.WithFieldsFromContext. Fields that are not set on the
// context are omitted entirely (no empty-string noise in logs).
func FieldsFromContext(ctx context.Context) logging.Fields {
    var f logging.Fields
    if id := RequestIDFromContext(ctx); id != "" {
        f = append(f, "request_id", id)
    }
    if id := SessionIDFromContext(ctx); id != "" {
        f = append(f, "session_id", id)
    }
    if v := VolumeFromContext(ctx); v != "" {
        f = append(f, "volume", v)
    }
    if u := UserFromContext(ctx); u != "" {
        f = append(f, "user", u)
    }
    return f
}
```

Run the test → PASS.

- [ ] **Step 3: Write the LogContext-interceptor test**

```go
// pkg/common/grpc/interceptor_log_fields_test.go
package grpc

import (
    "context"
    "testing"

    "github.com/stretchr/testify/suite"
    "google.golang.org/grpc"
)

// Stub request types implementing the getters the interceptor peeks at.
type withSessionAndVolume struct{}

func (withSessionAndVolume) GetSessionId() string { return "sess-X" }
func (withSessionAndVolume) GetVolume() string    { return "vol-Y" }

type onlyVolume struct{}

func (onlyVolume) GetVolume() string { return "vol-only" }

type LogContextInterceptorTestSuite struct{ suite.Suite }

func (s *LogContextInterceptorTestSuite) TestStampsSessionIDAndVolume() {
    interceptor := ServerUnaryLogContext()
    info := &grpc.UnaryServerInfo{FullMethod: "/x/y"}
    var stampedSession, stampedVolume string
    handler := func(ctx context.Context, req any) (any, error) {
        stampedSession = SessionIDFromContext(ctx)
        stampedVolume = VolumeFromContext(ctx)
        return nil, nil
    }
    _, err := interceptor(context.Background(), withSessionAndVolume{}, info, handler)
    s.Require().NoError(err)
    s.Assert().Equal("sess-X", stampedSession)
    s.Assert().Equal("vol-Y", stampedVolume)
}

func (s *LogContextInterceptorTestSuite) TestStampsOnlyAvailableGetters() {
    interceptor := ServerUnaryLogContext()
    info := &grpc.UnaryServerInfo{FullMethod: "/x/y"}
    var stampedSession, stampedVolume string
    handler := func(ctx context.Context, req any) (any, error) {
        stampedSession = SessionIDFromContext(ctx)
        stampedVolume = VolumeFromContext(ctx)
        return nil, nil
    }
    _, err := interceptor(context.Background(), onlyVolume{}, info, handler)
    s.Require().NoError(err)
    s.Assert().Empty(stampedSession)
    s.Assert().Equal("vol-only", stampedVolume)
}

func TestLogContextInterceptorTestSuite(t *testing.T) {
    suite.Run(t, new(LogContextInterceptorTestSuite))
}
```

- [ ] **Step 4: Implement `pkg/common/grpc/interceptor_log_fields.go`**

```go
package grpc

import (
    "context"

    "google.golang.org/grpc"
)

// SessionIDCarrier matches any proto request with a GetSessionId getter.
type SessionIDCarrier interface{ GetSessionId() string }

// VolumeCarrier matches any proto request with a GetVolume getter.
type VolumeCarrier interface{ GetVolume() string }

// ServerUnaryLogContext peeks at the request via the standard proto-
// generated getters and stamps session_id / volume on the context so
// downstream log lines pick them up via FieldsFromContext.
func ServerUnaryLogContext() grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        if r, ok := req.(SessionIDCarrier); ok {
            if id := r.GetSessionId(); id != "" {
                ctx = NewContextWithSessionID(ctx, id)
            }
        }
        if r, ok := req.(VolumeCarrier); ok {
            if v := r.GetVolume(); v != "" {
                ctx = NewContextWithVolume(ctx, v)
            }
        }
        return handler(ctx, req)
    }
}
```

Run → PASS.

- [ ] **Step 5: Stamp `user` from the auth interceptor**

Read `pkg/server/grpc/auth.go` to find where the authenticated principal is determined. After auth succeeds, wrap the context: `ctx = commongrpc.NewContextWithUser(ctx, principal)` before calling the handler. Add a focused test that asserts `UserFromContext` is non-empty after the auth chain. (If the existing auth interceptor file/structure makes this awkward, factor a one-line helper into the same file — it's a 3-line change.)

- [ ] **Step 6: Wire `logging.WithFieldsFromContext` into the server logger**

In `pkg/server/grpc/server.go::getLoggingInterceptor`:

```go
opts := []logging.Option{
    logging.WithLogOnEvents(logging.FinishCall),
    logging.WithFieldsFromContext(grpc2.FieldsFromContext),  // NEW
    logging.WithLevels(func(code codes.Code) logging.Level {
        // ... unchanged ...
    }),
}
```

And in `getOptions`, the unary chain becomes:

```go
unaryInterceptors := append(
    []grpc.UnaryServerInterceptor{
        grpc2.ServerUnaryRequestID(),       // 1. request_id
        grpc2.ServerUnaryLogContext(),      // 2. session_id, volume
        authInterceptor.Unary(),            // 3. user (writes user on ctx)
        unaryLog,                           // 4. finish-call line w/ all fields
    },
    s.extraUnaryInterceptors...,
)
```

- [ ] **Step 7: Same on the client**

In `pkg/client/grpc/client.go`, switch the client's interceptor chain to use the logging library's client interceptor with `WithFieldsFromContext(commongrpc.FieldsFromContext)`. The client side won't have `session_id`/`volume` on its outgoing context unless the IO layer puts them there — for now just `request_id` is enough on the client (the matching server line carries the rest).

- [ ] **Step 8: Wire e2e assertion**

In `test/e2e/api/`, add a focused test that exercises a real RPC through the bufconn harness with the logger pointed at a buffer (via `log.Reconfigure(LogConfig{Format: "json"}, &buf)` in `SetupSuite`), and asserts both client AND server emitted a log line containing the same `request_id`. Suite skeleton:

```go
func (s *LoggingE2ETestSuite) TestRequestIDOnBothSides() {
    // Call something boring like VolumeService.List.
    _, err := s.harness.GetClient().Volume().List(context.Background(), &proto.VolumeListRequest{})
    s.Require().NoError(err)

    out := s.logBuf.String()
    // exactly one request_id, present on both client and server finish-call lines.
    matches := regexp.MustCompile(`"request_id":"([0-9a-f-]+)"`).FindAllStringSubmatch(out, -1)
    s.Require().GreaterOrEqual(len(matches), 2, "both sides must log request_id")
    s.Assert().Equal(matches[0][1], matches[1][1], "both sides must use the same request_id")
}
```

(Adapt to the actual harness/log-redirection pattern — see `test/e2e/api/session_test.go` for the existing in-process harness usage.)

- [ ] **Step 9: Commit**

```bash
git add pkg/common/grpc/log_fields.go pkg/common/grpc/log_fields_test.go pkg/common/grpc/interceptor_log_fields.go pkg/common/grpc/interceptor_log_fields_test.go pkg/server/grpc/server.go pkg/server/grpc/auth.go pkg/client/grpc/client.go test/e2e/api/
git commit -m "$(cat <<'EOF'
feat(grpc): context-aware log fields (request_id, session_id, volume, user)

A new ServerUnaryLogContext interceptor uses the proto-generated
GetSessionId/GetVolume getters to stamp those values on the handler
context; the auth interceptor stamps user. Combined with the request_id
interceptor from the prior commit and logging.WithFieldsFromContext on
the existing grpc-middleware logger, every finish-call line on both
sides now carries request_id, session_id, volume, and user. An e2e
test in test/e2e/api asserts client and server log lines for the same
RPC share the same request_id — closing the Phase 2 DoD on log
correlation.
EOF
)"
```

---

## Task 3: Server metrics

**Why:** Per-volume / per-op visibility into operations the existing prometheus.ServerMetrics doesn't break out — open files, bytes, errors with volume label, duration with volume label, active sessions.

**Files:**
- Create: `pkg/server/metrics/metrics.go`
- Create: `pkg/server/metrics/metrics_test.go`
- Create: `pkg/server/grpc/interceptor_metrics.go`
- Create: `pkg/server/grpc/interceptor_metrics_test.go`
- Modify: `pkg/server/controller/file.go` (call Bytes / OpenFiles accessors)
- Modify: `pkg/server/service/session.go` (call SessionsActive)
- Modify: `pkg/server/app.go` (register the package)

- [ ] **Step 1: Write the metrics-package test**

```go
// pkg/server/metrics/metrics_test.go
package metrics

import (
    "testing"

    "github.com/prometheus/client_golang/prometheus/testutil"
    "github.com/stretchr/testify/suite"
)

type MetricsTestSuite struct {
    suite.Suite
    m *Metrics
}

func (s *MetricsTestSuite) SetupTest() { s.m = NewMetrics() }

func (s *MetricsTestSuite) TestOpenFilesIncDec() {
    s.m.OpenFilesInc("photos", "sess-1")
    s.m.OpenFilesInc("photos", "sess-1")
    s.m.OpenFilesDec("photos", "sess-1")
    s.Assert().Equal(1.0, testutil.ToFloat64(s.m.OpenFiles.WithLabelValues("photos", "sess-1")))
}

func (s *MetricsTestSuite) TestBytesAccumulate() {
    s.m.BytesAdd("photos", "in", 100)
    s.m.BytesAdd("photos", "in", 50)
    s.m.BytesAdd("photos", "out", 200)
    s.Assert().Equal(150.0, testutil.ToFloat64(s.m.Bytes.WithLabelValues("photos", "in")))
    s.Assert().Equal(200.0, testutil.ToFloat64(s.m.Bytes.WithLabelValues("photos", "out")))
}

func (s *MetricsTestSuite) TestRpcErrorsCounter() {
    s.m.RpcErrorInc("photos", "Read", "Unavailable")
    s.Assert().Equal(1.0, testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Read", "Unavailable")))
}

func (s *MetricsTestSuite) TestRequestDurationObserves() {
    s.m.RequestDurationObserve("photos", "Read", 0.123)
    // Histograms expose Sum and Count via the underlying observer; assert
    // count is 1 via testutil.CollectAndCount on the histogram metric.
    s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func (s *MetricsTestSuite) TestSessionsActive() {
    s.m.SessionsActiveInc()
    s.m.SessionsActiveInc()
    s.m.SessionsActiveDec()
    s.Assert().Equal(1.0, testutil.ToFloat64(s.m.SessionsActive))
}

func TestMetricsTestSuite(t *testing.T) { suite.Run(t, new(MetricsTestSuite)) }
```

- [ ] **Step 2: Implement `pkg/server/metrics/metrics.go`**

```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
)

// Metrics is the set of custom server collectors. The struct also satisfies
// prometheus.Collector indirectly via its exported fields, which lets the
// embedding application register them all in one MustRegister call.
type Metrics struct {
    OpenFiles       *prometheus.GaugeVec
    Bytes           *prometheus.CounterVec
    RpcErrors       *prometheus.CounterVec
    RequestDuration *prometheus.HistogramVec
    SessionsActive  prometheus.Gauge
}

// NewMetrics constructs (but does NOT register) the collector set.
// Callers register via MustRegister(registry, m) so tests can use a
// fresh registry without polluting global state.
func NewMetrics() *Metrics {
    return &Metrics{
        OpenFiles: prometheus.NewGaugeVec(prometheus.GaugeOpts{
            Name: "gmountie_server_open_files",
            Help: "Number of file descriptors currently open on the server, per volume and session.",
        }, []string{"volume", "session"}),
        Bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "gmountie_server_bytes_total",
            Help: "Total bytes transferred per volume and direction (in=write, out=read).",
        }, []string{"volume", "direction"}),
        RpcErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "gmountie_server_rpc_errors_total",
            Help: "Count of non-OK gRPC RPCs per volume, op, and grpc status code.",
        }, []string{"volume", "op", "code"}),
        RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
            Name:    "gmountie_server_request_duration_seconds",
            Help:    "Per-RPC handler duration in seconds, per volume and op.",
            Buckets: prometheus.DefBuckets,
        }, []string{"volume", "op"}),
        SessionsActive: prometheus.NewGauge(prometheus.GaugeOpts{
            Name: "gmountie_server_sessions_active",
            Help: "Number of active sessions (created and not yet reaped).",
        }),
    }
}

// MustRegister registers all collectors with r. Callers pass the
// process-wide registry in production and a fresh one in tests.
func (m *Metrics) MustRegister(r prometheus.Registerer) {
    r.MustRegister(m.OpenFiles, m.Bytes, m.RpcErrors, m.RequestDuration, m.SessionsActive)
}

func (m *Metrics) OpenFilesInc(volume, session string) {
    m.OpenFiles.WithLabelValues(volume, session).Inc()
}
func (m *Metrics) OpenFilesDec(volume, session string) {
    m.OpenFiles.WithLabelValues(volume, session).Dec()
}
func (m *Metrics) BytesAdd(volume, direction string, n float64) {
    m.Bytes.WithLabelValues(volume, direction).Add(n)
}
func (m *Metrics) RpcErrorInc(volume, op, code string) {
    m.RpcErrors.WithLabelValues(volume, op, code).Inc()
}
func (m *Metrics) RequestDurationObserve(volume, op string, seconds float64) {
    m.RequestDuration.WithLabelValues(volume, op).Observe(seconds)
}
func (m *Metrics) SessionsActiveInc() { m.SessionsActive.Inc() }
func (m *Metrics) SessionsActiveDec() { m.SessionsActive.Dec() }
```

Run the test → PASS.

- [ ] **Step 3: Write the metrics-interceptor test**

```go
// pkg/server/grpc/interceptor_metrics_test.go
package grpc

import (
    "context"
    "errors"
    "testing"

    "gmountie/pkg/server/metrics"

    "github.com/prometheus/client_golang/prometheus/testutil"
    "github.com/stretchr/testify/suite"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
)

type vReq struct{}

func (vReq) GetVolume() string { return "photos" }

type MetricsInterceptorTestSuite struct {
    suite.Suite
    m  *metrics.Metrics
}

func (s *MetricsInterceptorTestSuite) SetupTest() { s.m = metrics.NewMetrics() }

func (s *MetricsInterceptorTestSuite) TestRecordsDurationAndError() {
    interceptor := UnaryServerMetricsInterceptor(s.m)
    info := &grpc.UnaryServerInfo{FullMethod: "/gmountie.RpcFs/Mkdir"}
    handler := func(ctx context.Context, req any) (any, error) {
        return nil, status.Error(codes.Unavailable, "boom")
    }
    _, err := interceptor(context.Background(), vReq{}, info, handler)
    s.Require().Error(err)
    s.Assert().Equal(1.0, testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Mkdir", "Unavailable")))
    s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func (s *MetricsInterceptorTestSuite) TestNoErrorCounterOnOK() {
    interceptor := UnaryServerMetricsInterceptor(s.m)
    info := &grpc.UnaryServerInfo{FullMethod: "/gmountie.RpcFs/Mkdir"}
    handler := func(ctx context.Context, req any) (any, error) { return nil, nil }
    _, err := interceptor(context.Background(), vReq{}, info, handler)
    s.Require().NoError(err)
    s.Assert().Equal(0.0, testutil.ToFloat64(s.m.RpcErrors.WithLabelValues("photos", "Mkdir", "OK")))
    s.Assert().Equal(1, testutil.CollectAndCount(s.m.RequestDuration))
}

func TestMetricsInterceptorTestSuite(t *testing.T) {
    suite.Run(t, new(MetricsInterceptorTestSuite))
}
```

- [ ] **Step 4: Implement `pkg/server/grpc/interceptor_metrics.go`**

```go
package grpc

import (
    "context"
    "path"
    "time"

    commongrpc "gmountie/pkg/common/grpc"
    "gmountie/pkg/server/metrics"

    "google.golang.org/grpc"
    "google.golang.org/grpc/status"
)

// UnaryServerMetricsInterceptor records per-RPC duration and error
// counters tagged with volume + op + (on error) the gRPC code. The
// volume is read from the request via the GetVolume getter — requests
// without one (e.g. SessionService) are tagged with volume="".
func UnaryServerMetricsInterceptor(m *metrics.Metrics) grpc.UnaryServerInterceptor {
    return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
        start := time.Now()
        volume := ""
        if v, ok := req.(commongrpc.VolumeCarrier); ok {
            volume = v.GetVolume()
        }
        op := path.Base(info.FullMethod) // "/gmountie.RpcFs/Mkdir" → "Mkdir"

        resp, err := handler(ctx, req)
        m.RequestDurationObserve(volume, op, time.Since(start).Seconds())
        if err != nil {
            code := status.Code(err).String()
            m.RpcErrorInc(volume, op, code)
        }
        return resp, err
    }
}
```

Run → PASS.

- [ ] **Step 5: Wire OpenFiles + Bytes from the controller**

In `pkg/server/controller/file.go`, add `metrics *metrics.Metrics` field on `RpcFileServerImpl`. Constructor `NewRpcFileServer(fsService, sessions, metrics)` (signature change — fix the call site in `pkg/server/app.go`).

- After `sess.RegisterFile(...)` in `Open` and `Create` (both inside `withIdempotency`, on the `fuse.OK` branch): `r.metrics.OpenFilesInc(request.Volume, request.SessionId)`.
- In `Release`, after `sess.ReleaseFile(...)`: `r.metrics.OpenFilesDec(request.Volume, request.SessionId)`.
- In `Read`, after a successful read (status == fuse.OK): `r.metrics.BytesAdd(request.Volume, "out", float64(n.Size()))`.
- In `Write`, after a successful write: `r.metrics.BytesAdd(request.Volume, "in", float64(written))`.

Update `pkg/server/controller/file_test.go`: `SetupTest` constructs `metrics.NewMetrics()` and passes it to `NewRpcFileServer`. Most tests don't care about the values — just need the metrics object non-nil.

- [ ] **Step 6: Wire SessionsActive from the SessionManager**

In `pkg/server/service/session.go`:
- Add `metrics MetricsHook` interface (or a function-pointer field) to `sessionManagerImpl`. To keep the package decoupled from the metrics package, define a minimal interface:

```go
type SessionMetrics interface {
    SessionsActiveInc()
    SessionsActiveDec()
}
```

…and accept it as `SessionManagerOptions.Metrics`. Default to a no-op implementation. `Create` increments; the reap goroutine and `Stop` decrement on each session removed.

Update `NewServerAppContext` in `app.go` to construct the metrics package and pass it as the option.

- [ ] **Step 7: Wire the metrics interceptor**

In `pkg/server/grpc/server.go::initMetricsServer`, after the existing `prometheus.ServerMetrics` setup, also create+register the custom `Metrics`:

```go
s.customMetrics = metrics.NewMetrics()
s.customMetrics.MustRegister(prometheus.DefaultRegisterer)
s.extraUnaryInterceptors = append(s.extraUnaryInterceptors,
    UnaryServerMetricsInterceptor(s.customMetrics),
)
```

Expose `s.customMetrics` via `func (s *Server) Metrics() *metrics.Metrics` so the controller layer can fetch it. `app.go` calls `s.Metrics()` after `Serve()` starts — or, cleaner, build the metrics in `app.go` and pass into the server constructor.

**Cleanest shape (apply the layering skill):** construct `*metrics.Metrics` in `AppContext`, register it once, pass it to: the server's interceptor chain, the file controller, the SessionManager. App-context-owns-the-collector; everything else receives it.

- [ ] **Step 8: Run all tests with race**

Run: `go test -race -count=2 ./pkg/server/...`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add pkg/server/metrics/ pkg/server/grpc/interceptor_metrics.go pkg/server/grpc/interceptor_metrics_test.go pkg/server/grpc/server.go pkg/server/controller/file.go pkg/server/controller/file_test.go pkg/server/service/session.go pkg/server/service/session_test.go pkg/server/app.go
git commit -m "$(cat <<'EOF'
feat(server/metrics): per-volume / per-op observability collectors

pkg/server/metrics owns five custom collectors: open files
(volume,session), bytes total (volume,direction), rpc errors
(volume,op,code), request duration histogram (volume,op), and active
sessions gauge. A unary interceptor records duration + error counter
on every server RPC; the file controller bumps open-files and bytes
on the relevant ops; the SessionManager bumps the active-sessions
gauge through a small interface so the service package stays
decoupled from the metrics package.
EOF
)"
```

---

## Task 4: Client metrics

**Why:** Symmetric visibility — how often does the client retry, and how many requests are in flight at any moment?

**Files:**
- Create: `pkg/client/metrics/metrics.go`
- Create: `pkg/client/metrics/metrics_test.go`
- Create: `pkg/client/grpc/interceptor_metrics.go`
- Create: `pkg/client/grpc/interceptor_metrics_test.go`
- Modify: `pkg/client/io/retry.go` — call `RetryInc` from `retry.OnRetry`
- Modify: `pkg/client/grpc/client.go` and `pkg/client/grpc/factory.go` — wire the interceptor and pass the metrics through

- [ ] **Step 1: Write the test**

```go
// pkg/client/metrics/metrics_test.go
package metrics

import (
    "testing"

    "github.com/prometheus/client_golang/prometheus/testutil"
    "github.com/stretchr/testify/suite"
)

type ClientMetricsTestSuite struct {
    suite.Suite
    m *Metrics
}

func (s *ClientMetricsTestSuite) SetupTest() { s.m = NewMetrics() }

func (s *ClientMetricsTestSuite) TestRetryInc() {
    s.m.RetryInc("Read", "Unavailable")
    s.m.RetryInc("Read", "Unavailable")
    s.Assert().Equal(2.0, testutil.ToFloat64(s.m.RetryTotal.WithLabelValues("Read", "Unavailable")))
}

func (s *ClientMetricsTestSuite) TestInFlightIncDec() {
    s.m.InFlightInc("Mkdir")
    s.m.InFlightInc("Mkdir")
    s.m.InFlightDec("Mkdir")
    s.Assert().Equal(1.0, testutil.ToFloat64(s.m.InFlight.WithLabelValues("Mkdir")))
}

func TestClientMetricsTestSuite(t *testing.T) { suite.Run(t, new(ClientMetricsTestSuite)) }
```

- [ ] **Step 2: Implement `pkg/client/metrics/metrics.go`**

```go
package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
    RetryTotal *prometheus.CounterVec
    InFlight   *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
    return &Metrics{
        RetryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
            Name: "gmountie_client_retry_total",
            Help: "Count of client RPC retries per op and grpc status code.",
        }, []string{"op", "code"}),
        InFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
            Name: "gmountie_client_in_flight",
            Help: "Number of in-flight client RPCs per op.",
        }, []string{"op"}),
    }
}

func (m *Metrics) MustRegister(r prometheus.Registerer) {
    r.MustRegister(m.RetryTotal, m.InFlight)
}

func (m *Metrics) RetryInc(op, code string)  { m.RetryTotal.WithLabelValues(op, code).Inc() }
func (m *Metrics) InFlightInc(op string)     { m.InFlight.WithLabelValues(op).Inc() }
func (m *Metrics) InFlightDec(op string)     { m.InFlight.WithLabelValues(op).Dec() }
```

Run → PASS.

- [ ] **Step 3: Hook the retry metric**

In `pkg/client/io/retry.go`, extend `retryableCall` to take a metrics hook (or, simpler, define a small package-level variable that the io package's constructor sets). Cleanest: pass the metrics via a `RetryHook` option threaded through `LocalFileSystem` and `GrpcFile` — but that's a big surface change. Pragmatic path: define a package-private `var metricsHook func(op, code string)` and a setter `SetRetryMetricsHook`. The client factory sets it once at startup. Document the global as deliberate (single client per process is the normal shape).

In `retry.Do(...)` options, add:

```go
retry.OnRetry(func(n uint, err error) {
    if metricsHook == nil { return }
    code := status.Code(err).String()
    metricsHook(op, code)
}),
```

- [ ] **Step 4: Implement the in-flight interceptor**

```go
// pkg/client/grpc/interceptor_metrics.go
package grpc

import (
    "context"
    "path"

    "gmountie/pkg/client/metrics"

    "google.golang.org/grpc"
)

func UnaryClientInFlightInterceptor(m *metrics.Metrics) grpc.UnaryClientInterceptor {
    return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
        op := path.Base(method)
        m.InFlightInc(op)
        defer m.InFlightDec(op)
        return invoker(ctx, method, req, reply, cc, opts...)
    }
}
```

Test mirrors the server-side pattern: invoke the interceptor with a fake invoker that sleeps briefly and assert `m.InFlight.WithLabelValues("Mkdir")` reads 1.0 mid-call, 0.0 after.

- [ ] **Step 5: Wire into the client**

In `pkg/client/grpc/client.go`, accept a `*metrics.Metrics` via a new `WithMetrics(*metrics.Metrics)` option. Construct interceptors from it. The factory builds and registers metrics once.

`io/retry.go`: factory calls `io.SetRetryMetricsHook(metrics.RetryInc)` at startup.

- [ ] **Step 6: Run all tests**

Run: `go test -race -count=2 ./pkg/client/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/client/metrics/ pkg/client/grpc/ pkg/client/io/retry.go
git commit -m "$(cat <<'EOF'
feat(client/metrics): retry + in-flight collectors

gmountie_client_retry_total{op,code} counts every retry the retry-go
wrapper fires (hooked via retry.OnRetry). gmountie_client_in_flight{op}
gauge is bumped from a thin unary interceptor that increments before
the invoker call and decrements via defer. The factory constructs and
registers the collectors once per process; the io package's retry hook
is a package-level setter so the helper stays callable from anywhere.
EOF
)"
```

---

## Task 5: gRPC health protocol + HTTP /healthz + /readyz

**Why:** Standard probes for orchestrators (Kubernetes, systemd, Nomad) without requiring them to learn the gRPC protocol.

**Files:**
- Create: `pkg/server/grpc/health.go`
- Create: `pkg/server/grpc/health_test.go`
- Create: `pkg/server/ops/server.go`
- Create: `pkg/server/ops/handlers.go`
- Create: `pkg/server/ops/handlers_test.go`
- Create: `pkg/server/ops/readiness.go`
- Create: `pkg/server/ops/readiness_test.go`
- Modify: `pkg/server/grpc/server.go` — replace inline `/metrics` goroutine with `ops.NewServer(...)`, flip health status during Stop
- Modify: `pkg/server/app.go` — construct ReadinessChecker, pass to ops server

- [ ] **Step 1: Implement the gRPC health service registration**

```go
// pkg/server/grpc/health.go
package grpc

import (
    "google.golang.org/grpc"
    "google.golang.org/grpc/health"
    healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthService wraps grpc/health's Server with a small SetServing shim
// so the gRPC server can flip status during graceful shutdown.
type HealthService struct {
    srv *health.Server
}

func NewHealthService() *HealthService {
    s := health.NewServer()
    s.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
    return &HealthService{srv: s}
}

func (h *HealthService) Register(s *grpc.Server) {
    healthpb.RegisterHealthServer(s, h.srv)
}

func (h *HealthService) SetNotServing() {
    h.srv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
}
```

Test: in-process gRPC server with HealthService registered; call `health.NewHealthClient(conn).Check(ctx, ...)`; expect SERVING. After `SetNotServing`, expect NOT_SERVING.

- [ ] **Step 2: Implement ReadinessChecker**

```go
// pkg/server/ops/readiness.go
package ops

import (
    "context"
    "os"

    "github.com/pkg/errors"
)

// ReadinessChecker decides whether /readyz returns 200 or 503.
type ReadinessChecker interface {
    Ready(ctx context.Context) error
}

// PathReadinessChecker stats one path. Failing to stat fails readiness.
// Use the root of the first configured volume as the canary.
type PathReadinessChecker struct{ Path string }

func (p PathReadinessChecker) Ready(_ context.Context) error {
    if p.Path == "" {
        return errors.New("no readiness probe path configured")
    }
    if _, err := os.Stat(p.Path); err != nil {
        return errors.Wrap(err, "readiness stat")
    }
    return nil
}
```

Test: PathReadinessChecker against a real temp dir → nil; against `/nonexistent/xyz` → error.

- [ ] **Step 3: Implement the HTTP handlers**

```go
// pkg/server/ops/handlers.go
package ops

import (
    "encoding/json"
    "net/http"

    "gmountie/pkg"
)

// LivenessHandler always returns 200 — the binary is alive enough to
// answer HTTP.
func LivenessHandler() http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        _, _ = w.Write([]byte("ok"))
    })
}

// ReadinessHandler defers to a ReadinessChecker; 200 on Ready, 503
// otherwise.
func ReadinessHandler(rc ReadinessChecker) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if err := rc.Ready(r.Context()); err != nil {
            http.Error(w, err.Error(), http.StatusServiceUnavailable)
            return
        }
        _, _ = w.Write([]byte("ready"))
    })
}

// VersionHandler returns the build info as JSON. Thin passthrough —
// per the layering skill's "no service layer for pure passthrough"
// exception.
func VersionHandler() http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        _ = json.NewEncoder(w).Encode(pkg.GetBuildInfo())
    })
}
```

Tests: `httptest.NewRecorder` + each handler, asserting status code and body for both Ready and NotReady cases (the latter with a fake ReadinessChecker that returns an error).

- [ ] **Step 4: Implement the ops server**

```go
// pkg/server/ops/server.go
package ops

import (
    "context"
    "errors"
    "net/http"

    "gmountie/pkg/utils/log"

    "github.com/prometheus/client_golang/prometheus/promhttp"
    "go.uber.org/zap"
)

// Server is the HTTP "ops" endpoint that mounts /metrics, /healthz,
// /readyz, and /version. It owns no business logic — pure routing over
// supplied handlers.
type Server struct {
    addr      string
    readiness ReadinessChecker
    server    *http.Server
}

func NewServer(addr string, readiness ReadinessChecker) *Server {
    return &Server{addr: addr, readiness: readiness}
}

// Start binds and starts the server. Returns once ListenAndServe returns.
// Callers typically run this in a goroutine.
func (s *Server) Start() {
    mux := http.NewServeMux()
    mux.Handle("/metrics", promhttp.Handler())
    mux.Handle("/healthz", LivenessHandler())
    mux.Handle("/readyz", ReadinessHandler(s.readiness))
    mux.Handle("/version", VersionHandler())

    s.server = &http.Server{Addr: s.addr, Handler: mux}
    log.Log.Info("ops server starting", zap.String("addr", s.addr))
    if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
        log.Log.Error("ops server stopped", zap.Error(err))
    }
}

func (s *Server) Stop(ctx context.Context) error {
    if s.server == nil {
        return nil
    }
    return s.server.Shutdown(ctx)
}
```

- [ ] **Step 5: Wire into `pkg/server/grpc/server.go`**

Replace the inline `/metrics` goroutine in `startMetricsServer` with a call into `ops.NewServer(addr, readinessChecker).Start()` (in its own goroutine). Add a `Stop` path that calls `opsServer.Stop(ctx)`. Register `NewHealthService()` as a gRPC service. On graceful Stop, call `health.SetNotServing()` BEFORE `GracefulStop`.

- [ ] **Step 6: Wire `ReadinessChecker` in `pkg/server/app.go`**

Build a `PathReadinessChecker` pointed at the first configured volume's path (or empty if no volumes). Pass to the server constructor.

- [ ] **Step 7: Run tests**

```
go test -race -count=2 ./pkg/server/...
```

- [ ] **Step 8: Commit**

```bash
git add pkg/server/grpc/health.go pkg/server/grpc/health_test.go pkg/server/ops/ pkg/server/grpc/server.go pkg/server/app.go
git commit -m "$(cat <<'EOF'
feat(server): gRPC health protocol + HTTP /healthz, /readyz, /version

Registers grpc.health.v1.Health so grpc_health_probe reports SERVING.
A new pkg/server/ops package owns the HTTP ops endpoint: /metrics (the
existing prometheus handler), /healthz (always 200 when the process is
alive enough to answer), /readyz (passes a ReadinessChecker — default
implementation Stats the first configured volume's path), and /version
(returns the build-info JSON). The gRPC server flips the health status
to NOT_SERVING before GracefulStop so probes drain in the right order.

The ops HTTP server replaces the inline /metrics goroutine the gRPC
server used to spawn.
EOF
)"
```

---

## Task 6: `Version` gRPC service

**Why:** Symmetry — Phase 2 DoD asks for both an HTTP `/version` (Task 5 covered) and a gRPC `VersionService.Get`. The gRPC version is useful for clients that don't want to scrape the metrics port.

**Files:**
- Create: `api/proto/version.proto`
- Regen: `pkg/proto/version.pb.go`, `pkg/proto/version_grpc.pb.go`, mocks
- Create: `pkg/server/controller/version.go`
- Create: `pkg/server/controller/version_test.go`
- Modify: `pkg/server/app.go` (register the controller)

- [ ] **Step 1: Define the proto**

```protobuf
// api/proto/version.proto
syntax = "proto3";
package gmountie;
option go_package = "pkg/proto";

message VersionRequest {}

message VersionReply {
  string version = 1;
  string commit  = 2;
  string date    = 3;
}

service VersionService {
  rpc Get (VersionRequest) returns (VersionReply);
}
```

Run: `task gen:grpc && task gen:mocks`.

- [ ] **Step 2: Write the controller test**

```go
// pkg/server/controller/version_test.go
package controller

import (
    "context"
    "testing"

    "gmountie/pkg/proto"

    "github.com/stretchr/testify/suite"
)

type VersionControllerTestSuite struct {
    suite.Suite
    c *VersionController
}

func (s *VersionControllerTestSuite) SetupTest() { s.c = NewVersionController() }

func (s *VersionControllerTestSuite) TestGetReturnsBuildInfo() {
    reply, err := s.c.Get(context.Background(), &proto.VersionRequest{})
    s.Require().NoError(err)
    s.Assert().NotEmpty(reply.Version)
    s.Assert().NotEmpty(reply.Commit)
    s.Assert().NotEmpty(reply.Date)
}

func TestVersionControllerTestSuite(t *testing.T) {
    suite.Run(t, new(VersionControllerTestSuite))
}
```

- [ ] **Step 3: Implement the controller**

```go
// pkg/server/controller/version.go
package controller

import (
    "context"

    "gmountie/pkg"
    "gmountie/pkg/proto"

    "google.golang.org/grpc"
)

// VersionController is a thin passthrough over pkg.GetBuildInfo. Per the
// layering-service-features skill: this is the documented "no service
// layer for pure passthrough" exception.
type VersionController struct {
    proto.UnimplementedVersionServiceServer
}

var _ proto.VersionServiceServer = (*VersionController)(nil)

func NewVersionController() *VersionController { return &VersionController{} }

func (c *VersionController) Register(s *grpc.Server) {
    proto.RegisterVersionServiceServer(s, c)
}

func (c *VersionController) Get(_ context.Context, _ *proto.VersionRequest) (*proto.VersionReply, error) {
    bi := pkg.GetBuildInfo()
    return &proto.VersionReply{Version: bi.Version, Commit: bi.Commit, Date: bi.Date}, nil
}
```

Run the test → PASS.

- [ ] **Step 4: Wire into `pkg/server/app.go`**

Add `controller.NewVersionController()` to the `GetGrpcServices` slice.

- [ ] **Step 5: Run**

```
go test ./pkg/server/...
```

- [ ] **Step 6: Commit**

```bash
git add api/proto/version.proto pkg/proto/version.pb.go pkg/proto/version_grpc.pb.go internal/mocks/ pkg/server/controller/version.go pkg/server/controller/version_test.go pkg/server/app.go
git commit -m "$(cat <<'EOF'
feat(server): VersionService gRPC + version controller

Adds a tiny VersionService.Get RPC alongside the HTTP /version
endpoint. Thin passthrough over pkg.GetBuildInfo — the layering skill's
documented exception for pure-passthrough handlers. Lets clients fetch
build info over the same gRPC connection they're already using instead
of opening a second HTTP port to the ops endpoint.
EOF
)"
```

---

## Task 7: Configurable ops port

**Why:** `:9090` is hardcoded; users behind any kind of port-allocation policy can't run gMountie. Trivial fix.

**Files:**
- Modify: `pkg/server/config/server.go`
- Modify: `pkg/server/config/config.go`
- Modify: `pkg/server/app.go` (pass to ops server)

- [ ] **Step 1: Add the field**

```go
// pkg/server/config/server.go
type ServerConfig struct {
    Address     string `validate:"required,ip"`
    Port        uint   `validate:"required"`
    Metrics     bool
    MetricsAddr string `validate:"hostname_port" mapstructure:"metrics_addr"`
}

const DefaultMetricsAddr = ":9090"
```

- [ ] **Step 2: Wire in `ParseConfig`**

Add to the env-binding list and defaults:
```go
v.SetDefault("server.metrics_addr", DefaultMetricsAddr)
_ = v.BindEnv("server.metrics_addr")
result.Server.MetricsAddr = v.GetString("server.metrics_addr")
```

- [ ] **Step 3: Update `pkg/server/config/config_test.go`**

Add a case asserting the default is `:9090` and that `GMOUNTIE_SERVER_METRICS_ADDR=:9999` env-overrides correctly.

- [ ] **Step 4: Pass through in app.go**

```go
ops.NewServer(cfg.Server.MetricsAddr, readiness)
```

- [ ] **Step 5: Run**

```
go test ./pkg/server/...
```

- [ ] **Step 6: Commit**

```bash
git add pkg/server/config/server.go pkg/server/config/config.go pkg/server/config/config_test.go pkg/server/app.go
git commit -m "$(cat <<'EOF'
feat(server/config): make ops port configurable via server.metrics_addr

Default unchanged (:9090). New env-var GMOUNTIE_SERVER_METRICS_ADDR
overrides. Field name keeps "metrics" prefix for backwards-compat with
existing deployments even though the endpoint now serves /healthz,
/readyz, and /version alongside /metrics.
EOF
)"
```

---

## Task 8: Final sweep + opus review + VM validation

- [ ] **Step 1: Run the full working-set tests**

```
go test -count=1 ./pkg/utils/log/... ./pkg/common/grpc/... ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/client/metrics/... ./pkg/common/... ./pkg/server/... ./cmd/... ./test/e2e/api/...
```

- [ ] **Step 2: Race tests on the critical pieces**

```
go test -race -count=2 ./pkg/server/service/... ./pkg/server/controller/... ./pkg/server/grpc/... ./pkg/server/ops/... ./pkg/client/grpc/... ./pkg/client/io/... ./pkg/client/metrics/... ./pkg/common/grpc/...
```

- [ ] **Step 3: `go vet ./...`** — no output expected.

- [ ] **Step 4: VM validation**

```
task -t testing/scratch/Taskfile.yml test
```

- [ ] **Step 5: Manual DoD walkthrough**

The roadmap's Phase 2 DoD lines, each verified via a manual smoke step against a running server (use `task vm:shell` to drop into the VM):

- `grep <one-request-id>` on captured client + server logs returns 2+ lines, one per side. ✅ on test-side via the test added in Task 2b.
- `curl http://localhost:9090/metrics | grep gmountie_server_` returns the five new collectors during a fio run. Document the scrape in `docs/_sidebar.md` if helpful.
- `grpc_health_probe -addr localhost:9449` returns `SERVING`. (Install via `go install github.com/grpc-ecosystem/grpc-health-probe@latest` in the VM if not present.)
- `curl http://localhost:9090/version | jq .` returns the build-info JSON.

- [ ] **Step 6: Dispatch the opus final reviewer**

Pass the full plan range with the same Critical / Important / Minor categorisation used in Plans 1c and 1d. Focus areas:
- Goroutine-leak in the new ops HTTP server on Stop.
- Race correctness of metrics increments under concurrent RPCs (counter/gauge ops are atomic in client_golang — but verify the interceptor doesn't add its own racy state).
- Correctness of `path.Base(info.FullMethod)` for the op label (gRPC full methods look like `/gmountie.RpcFs/Mkdir`).
- Whether the request_id interceptor running BEFORE auth is the right ordering (it is — see Task 2 design notes).
- Whether the auth-context-stamp survives across the chain of interceptors.

- [ ] **Step 7: No commit — verification only**

---

## Self-review notes

**Spec coverage:**
- Item 1 (JSON logger) → Task 1.
- Item 2 (request_id + session_id + user + volume + op fields on logs, both sides) → Tasks 2a + 2b.
- Item 3 (server + client metrics) → Tasks 3 + 4.
- Item 4 (gRPC health + /healthz + /readyz) → Task 5.
- Item 5 (Version gRPC + /version HTTP) → Tasks 5 (HTTP /version) + 6 (gRPC).
- Item 6 (configurable metrics port) → Task 7.

**Type consistency:** `LogConfig`, `Metrics`, `RequestIDFromContext` / `NewContextWithRequestID`, `SessionIDFromContext`, `VolumeFromContext`, `UserFromContext`, `FieldsFromContext`, `ServerUnaryRequestID` / `ClientUnaryRequestID`, `ServerUnaryLogContext`, `UnaryServerMetricsInterceptor`, `UnaryClientInFlightInterceptor`, `HealthService.SetNotServing`, `ReadinessChecker.Ready`, `ops.NewServer`, `VersionController` — used consistently across tasks.

**No placeholders.** Two "read the existing X to find Y" hints (Task 2b step 5 for the auth interceptor location; Task 3 step 5 for the existing fs handler exact shape) are pointers to existing code, not gaps in the plan.

**Out of scope (deliberate):**
- OpenTelemetry tracing (deferred — log correlation covers most of the value).
- Auth on the ops port (Phase 7).
- Per-RPC client-side log lines: the client currently lacks a logging interceptor altogether (only metadata propagation). Adding a full grpc-middleware/logging chain client-side is in scope for Task 2b's step 7; the e2e test in step 8 verifies it works end-to-end.
