# Client Connection Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Open N gRPC connections per mount (sharing one session) and spread Read/Write streams across them (load-aware: least-in-flight, ties→primary), so read/write throughput can exceed the single-connection TCP single-flow ceiling on high-BDP links.

**Architecture:** `ClientImpl` holds a pool of N `*grpc.ClientConn` (conn 0 = primary). Metadata RPCs and the session handshake/keepalive/Subscribe stay on the primary; `DataFileClient()` picks the least-in-flight connection (ties→conn 0) and returns `(RpcFileClient, release)` — used only by the Read/Write stream sites. The server is session-keyed (by `session_id` metadata + principal), so one session works across all connections — no server change.

**Tech Stack:** Go, `google.golang.org/grpc`, Viper config, testify suites, mockery (`task gen:mocks`).

**Reference spec:** `docs/superpowers/specs/2026-06-18-connection-pool-design.md`

**Worktree:** `gMountie/.claude/worktrees/conn-pool` (branch `conn-pool`, off `origin/master`). All paths are relative to that root; run all `git`/`go` from there.

---

## File Structure

| File | Change |
|---|---|
| `pkg/client/config/rpc.go` | add `DefaultRpcConnections`, `Connections` field, default wiring |
| `pkg/client/config/rpc_test.go` | default + validation + round-trip tests |
| `pkg/client/grpc/client.go` | `Client` interface gets `DataFileClient()`; `ClientImpl` holds the pool; `WithConnections` option; dial N; `DataFileClient`; Connect/Close all |
| `pkg/client/grpc/client_test.go` | round-robin + primary-pinning unit tests |
| `internal/mocks/pkg/client/grpc/mock_Client.go` | regenerated via `task gen:mocks` (do NOT hand-edit) |
| `pkg/client/grpc/factory.go` | pass `WithConnections(cfg.Rpc.Connections)` |
| `pkg/client/io/backend_grpc.go` | Read/Write stream sites use `h.client.DataFileClient()` |
| `pkg/client/io/backend_grpc_test.go` | add `DataFileClient()` mock expectation |
| `test/e2e/fs/connpool_test.go` | new: correctness with `connections>1` |
| config reference doc | document `rpc.connections` |

---

### Task 1: `rpc.connections` config knob

**Files:** Modify `pkg/client/config/rpc.go`; Test `pkg/client/config/rpc_test.go`.

Mirror the existing `ReadaheadWindow` int knob exactly (const → `defaultRpcConfig()` literal → `v.SetDefault`). There is NO rpc env-mirror list to update.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/client/config/rpc_test.go` (match the existing suite/style in that file — it tests `NewRPCConfig`):

```go
func (s *RPCConfigSuite) TestConnectionsDefault() {
	cfg, err := NewRPCConfig(nil)
	s.Require().NoError(err)
	s.Equal(DefaultRpcConnections, cfg.Connections)

	cfg, err = NewRPCConfig(viper.New())
	s.Require().NoError(err)
	s.Equal(DefaultRpcConnections, cfg.Connections, "empty viper sub-tree must default connections")
}

func (s *RPCConfigSuite) TestConnectionsOverride() {
	v := viper.New()
	v.Set("connections", 8)
	cfg, err := NewRPCConfig(v)
	s.Require().NoError(err)
	s.Equal(8, cfg.Connections)
}

func (s *RPCConfigSuite) TestConnectionsValidationRejectsZeroAndTooMany() {
	for _, bad := range []int{0, 17} {
		v := viper.New()
		v.Set("connections", bad)
		_, err := NewRPCConfig(v)
		s.Require().Error(err, "connections=%d must be rejected", bad)
	}
}
```

If the suite type/name differs in `rpc_test.go`, attach these as methods on the existing suite and reuse its imports (`viper`). If `NewRPCConfig` does not run the validator, follow whatever the existing tests do to assert validation (e.g. a `Validate()` call); match the file's existing validation-test pattern exactly.

- [ ] **Step 2: Run to verify failure**

Run: `go test -run 'RPCConfig' ./pkg/client/config/ -v`
Expected: FAIL — `cfg.Connections undefined` / `DefaultRpcConnections undefined`.

- [ ] **Step 3: Add the default constant**

In `pkg/client/config/rpc.go`, in the `const (...)` block (next to `DefaultReadaheadWindow`):

```go
	// DefaultRpcConnections is the number of gRPC connections per mount. Each
	// connection is one TCP flow; Read/Write streams round-robin across them so
	// throughput can exceed a single flow's ceiling on high-BDP links. 1 = the
	// historical single-connection behavior.
	DefaultRpcConnections = 4
```

- [ ] **Step 4: Add the struct field**

In `RPCConfig`, next to `ReadaheadWindow`:

```go
	// Connections is the number of gRPC connections in the client pool.
	// Read/Write streams round-robin across them; metadata RPCs and the
	// session-control streams stay on the primary connection.
	Connections int `mapstructure:"connections" validate:"min=1,max=16"`
```

- [ ] **Step 5: Wire the default**

In `defaultRpcConfig()`, in the returned literal (next to `ReadaheadWindow: DefaultReadaheadWindow,`):

```go
		Connections: DefaultRpcConnections,
```

In `NewRPCConfig`, in the `v.SetDefault(...)` block (next to `readahead_window`):

```go
	v.SetDefault("connections", DefaultRpcConnections)
```

- [ ] **Step 6: Run to verify pass**

Run: `go test -run 'RPCConfig' ./pkg/client/config/ -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool add pkg/client/config/rpc.go pkg/client/config/rpc_test.go
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool commit -m "feat(client): add rpc.connections config knob (default 4)"
```

NO AI attribution in any commit message (no Co-Authored-By, no "Generated with Claude Code").

---

### Task 2: Connection pool in `ClientImpl`

**Files:** Modify `pkg/client/grpc/client.go`; regenerate `internal/mocks/...`; Test `pkg/client/grpc/client_test.go`.

- [ ] **Step 1: Write the failing unit tests**

Add to `pkg/client/grpc/client_test.go` (package `grpc`, internal — it can set private fields). These test the picker logic deterministically without real connections, by constructing a `ClientImpl` with a slice of distinct mock `RpcFileClient`s:

```go
func (s *ClientPoolSuite) TestDataFileClientRoundRobins() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	m2 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0, m1, m2}}

	counts := map[proto.RpcFileClient]int{}
	for i := 0; i < 9; i++ {
		counts[c.DataFileClient()]++
	}
	s.Equal(3, counts[m0])
	s.Equal(3, counts[m1])
	s.Equal(3, counts[m2])
}

func (s *ClientPoolSuite) TestDataFileClientSingleConnReturnsPrimary() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0}}
	for i := 0; i < 4; i++ {
		s.Same(m0, c.DataFileClient())
	}
}

func (s *ClientPoolSuite) TestFilePinsToPrimaryRegardlessOfRotation() {
	m0 := protomocks.NewMockRpcFileClient(s.T())
	m1 := protomocks.NewMockRpcFileClient(s.T())
	c := &ClientImpl{file: m0, dataFileClients: []proto.RpcFileClient{m0, m1}}
	_ = c.DataFileClient() // rotate
	s.Same(m0, c.File(), "File() must always return the primary stub")
}

func TestClientPoolSuite(t *testing.T) { suite.Run(t, new(ClientPoolSuite)) }

type ClientPoolSuite struct{ suite.Suite }
```

Use the existing import path for the proto RpcFileClient mock used elsewhere in this package's tests (check another `*_test.go` in `pkg/client/grpc` for the alias, commonly `protomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"`). If `s.Same` on an interface value misbehaves, compare with `s.Equal` on a per-mock sentinel instead.

- [ ] **Step 2: Run to verify failure**

Run: `go test -run 'TestClientPoolSuite' ./pkg/client/grpc/ -v`
Expected: FAIL — `dataFileClients` / `DataFileClient` undefined.

- [ ] **Step 3: Add the interface method**

In `pkg/client/grpc/client.go`, in the `Client` interface, after `File()`:

```go
	// DataFileClient returns an RpcFileClient bound to the next connection in
	// the pool (round-robin). Used only for Read/Write streams so a single
	// file's concurrent streams spread across connections. With one connection
	// it returns the primary File() client.
	DataFileClient() proto.RpcFileClient
```

- [ ] **Step 4: Add pool fields + option**

In the `ClientImpl` struct, after `conn *grpc.ClientConn`:

```go
	// conns is the connection pool; conns[0] == conn is the primary (session
	// handshake, keepalive, Subscribe, metadata RPCs). dataFileClients has one
	// RpcFileClient per connection; DataFileClient round-robins it for
	// Read/Write streams. rrCounter drives the round-robin.
	conns           []*grpc.ClientConn
	dataFileClients []proto.RpcFileClient
	rrCounter       atomic.Uint64
	connections     int
```

Add the option (near the other `With*` options):

```go
// WithConnections sets the gRPC connection-pool size (>=1). Values below 1 are
// clamped to 1. The factory sets this from rpc.connections.
func WithConnections(n int) ClientOption {
	return func(c *ClientImpl) {
		if n < 1 {
			n = 1
		}
		c.connections = n
	}
}
```

- [ ] **Step 5: Dial the pool in `NewClient`**

In `NewClient`, replace the single-dial block:

```go
	conn, err := grpc.NewClient(
		endpoint,
		c.getDialOptions()...,
	)
	if err != nil {
		c.lifeCancel()
		return nil, err
	}
	c.conn = conn
	c.file = proto.NewRpcFileClient(conn)
	c.fs = proto.NewRpcFsClient(conn)
	c.volume = proto.NewVolumeServiceClient(conn)
	c.version = proto.NewVersionServiceClient(conn)
	c.session = proto.NewSessionServiceClient(conn)
	c.handshake = NewSessionHandshake(c.session)
```

with:

```go
	n := c.connections
	if n < 1 {
		n = 1
	}
	conns := make([]*grpc.ClientConn, 0, n)
	fileClients := make([]proto.RpcFileClient, 0, n)
	for i := 0; i < n; i++ {
		conn, err := grpc.NewClient(endpoint, c.getDialOptions()...)
		if err != nil {
			for _, cc := range conns { // close partially-dialed pool
				_ = cc.Close()
			}
			c.lifeCancel()
			return nil, err
		}
		conns = append(conns, conn)
		fileClients = append(fileClients, proto.NewRpcFileClient(conn))
	}
	c.conns = conns
	c.conn = conns[0] // primary
	c.dataFileClients = fileClients
	c.file = fileClients[0] // primary File() stub
	c.fs = proto.NewRpcFsClient(conns[0])
	c.volume = proto.NewVolumeServiceClient(conns[0])
	c.version = proto.NewVersionServiceClient(conns[0])
	c.session = proto.NewSessionServiceClient(conns[0])
	c.handshake = NewSessionHandshake(c.session)
```

- [ ] **Step 6: Add `DataFileClient`**

Add (near `File()`):

```go
// DataFileClient returns an RpcFileClient bound to the next pool connection
// (round-robin). See the Client interface doc. With <=1 connection it returns
// the primary client (no atomic churn).
func (c *ClientImpl) DataFileClient() proto.RpcFileClient {
	n := len(c.dataFileClients)
	if n <= 1 {
		return c.file
	}
	i := c.rrCounter.Add(1) % uint64(n)
	return c.dataFileClients[i]
}
```

- [ ] **Step 7: Connect + Close the whole pool**

Replace `c.conn.Connect()` at the top of `Connect()` with:

```go
	for _, cc := range c.conns {
		cc.Connect()
	}
```

Replace `return c.conn.Close()` at the end of `Close()` with:

```go
	var firstErr error
	for _, cc := range c.conns {
		if err := cc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
```

- [ ] **Step 8: Regenerate the mock**

The `Client` interface changed, so the generated mock must be regenerated (never hand-edit mocks):

Run: `task gen:mocks`
Then confirm `internal/mocks/pkg/client/grpc/mock_Client.go` now has a `DataFileClient` method.

If `task gen:mocks` is unavailable in the environment, STOP and report NEEDS_CONTEXT — do not hand-write the mock method.

- [ ] **Step 9: Run unit tests + build**

Run: `go test -run 'TestClientPoolSuite' ./pkg/client/grpc/ -v` → PASS.
Run: `go build ./...` → builds (the regenerated mock satisfies the interface).

- [ ] **Step 10: Commit**

```bash
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool add pkg/client/grpc/client.go pkg/client/grpc/client_test.go internal/mocks/
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool commit -m "feat(client): gRPC connection pool with round-robin DataFileClient"
```

---

### Task 3: Wire the pool size in the factory

**Files:** Modify `pkg/client/grpc/factory.go`.

- [ ] **Step 1: Pass the option**

In `NewClientFromConfig`, where the other `opts = append(opts, With...())` lines are (next to `WithReadahead`/`WithWriteCoalesce`, around line 133-134), add:

```go
		opts = append(opts, WithConnections(cfg.Rpc.Connections))
```

(`cfg.Rpc` is how the factory already accesses the RPC config — e.g. `cfg.Rpc.TimeoutMeta`.)

- [ ] **Step 2: Build + existing factory tests**

Run: `go build ./pkg/client/grpc/ && go test ./pkg/client/grpc/`
Expected: builds; existing factory tests pass (they dial a bufconn/real endpoint; with the default pool of 4 they dial 4 conns — confirm no test asserted exactly one dial; if one did, update it to accept the configured count).

- [ ] **Step 3: Commit**

```bash
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool add pkg/client/grpc/factory.go
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool commit -m "feat(client): build the gRPC pool from rpc.connections"
```

---

### Task 4: Spread Read/Write streams across the pool

**Files:** Modify `pkg/client/io/backend_grpc.go`; `pkg/client/io/backend_grpc_test.go`.

The Read (two sites) and Write stream-creation sites currently use the per-handle `h.fileClient`. Route them through the pool so each stream (and each retry) picks a connection.

- [ ] **Step 1: Update the Read stream sites**

In `pkg/client/io/backend_grpc.go`, both `h.fileClient.Read(ctx, &proto.ReadRequest{...` sites become:

```go
		stream, err := h.client.DataFileClient().Read(ctx, &proto.ReadRequest{
```

(Only change the receiver `h.fileClient` → `h.client.DataFileClient()`; keep the request args identical.)

- [ ] **Step 2: Update the Write stream site**

In `streamingWrite`, change:

```go
		stream, err := h.fileClient.Write(ctx, grpc.WaitForReady(true))
```

to:

```go
		stream, err := h.client.DataFileClient().Write(ctx, grpc.WaitForReady(true))
```

- [ ] **Step 3: Remove the now-unused `fileClient` if dead**

Run: `go build ./pkg/client/io/ 2>&1`
If the build reports `h.fileClient` (the `grpcFileHandle.fileClient` field) is now unused, remove the field from the struct and its assignment in `newGrpcFileHandle` (and update any caller that passed it). If it is still used elsewhere (e.g. Open/Create/Release/locks via the handle), leave it. Make the build pass.

- [ ] **Step 4: Update the test mock expectation**

In `pkg/client/io/backend_grpc_test.go` `SetupTest` (or wherever `s.client.EXPECT().File().Return(s.fileClient).Maybe()` is), add:

```go
	s.client.EXPECT().DataFileClient().Return(s.fileClient).Maybe()
```

This routes the Read/Write stream calls to the same `s.fileClient` mock the tests already program, so existing Read/Write test expectations keep working.

- [ ] **Step 5: Run the io tests**

Run: `go test ./pkg/client/io/ 2>&1 | tail -20`
Expected: PASS. (If a Read/Write test fails because it asserted `File()` was called for the stream, switch that assertion to `DataFileClient()`.)

- [ ] **Step 6: Commit**

```bash
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool add pkg/client/io/backend_grpc.go pkg/client/io/backend_grpc_test.go
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool commit -m "feat(client): round-robin Read/Write streams across the connection pool"
```

---

### Task 5: e2e correctness with a multi-connection pool

**Files:** Create `test/e2e/fs/connpool_test.go`.

Proves fds opened on the pool work across connections (read + write correctness) with `connections > 1`. Runs in CI (CI has /dev/fuse). Throughput is verified manually on the VM (recorded in the PR), not asserted here.

- [ ] **Step 1: Write the test**

Create `test/e2e/fs/connpool_test.go`. Mirror the harness usage in `test/e2e/fs/simple_test.go`. The pool size is a client RPC-config field; set it via the harness's client-config hook. First check how `test/e2e/utils` sets the client `rpc` config (grep `utils` for an `RPCConfig`/`Rpc`/`WithReadahead`/`connections` setter). If there is an existing option to set rpc config, use it to set `Connections: 4`; if the harness always builds the client from a default config that already defaults `connections` to 4, then a plain mount already exercises the pool — assert that and note it.

Test body (adapt the harness calls to the real `utils` API, mirroring `simple_test.go`):

```go
package fs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// ConnPoolFSSuite verifies that with a multi-connection client pool, fds opened
// on the shared session work for reads and writes regardless of which pooled
// connection a given stream lands on (round-robin). Default config already uses
// connections=4 (rpc.connections), so a normal mount exercises the pool.
type ConnPoolFSSuite struct {
	suite.Suite
	ctx    *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *ConnPoolFSSuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.ctx = ctx
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *ConnPoolFSSuite) TearDownSuite() {
	if s.ctx != nil {
		s.Require().NoError(s.ctx.Close())
	}
}

// TestManyFilesReadWriteAcrossPool writes and reads back enough files that, with
// round-robin per stream, writes/reads land on multiple pooled connections. The
// content check fails if any stream hit a connection where the fd/session was
// not resolvable — the property the pool must preserve.
func (s *ConnPoolFSSuite) TestManyFilesReadWriteAcrossPool() {
	mp := s.volume.GetMountPath()
	for i := 0; i < 24; i++ {
		name := filepath.Join(mp, "f"+string(rune('a'+i%26))+string(rune('0'+i/26)))
		want := bytes.Repeat([]byte{byte('A' + i%26)}, 4096+i)
		s.Require().NoError(os.WriteFile(name, want, 0o644))
		got, err := os.ReadFile(name)
		s.Require().NoError(err)
		s.Require().True(bytes.Equal(want, got), "file %d round-trip mismatch across pool", i)
	}
}

// TestLargeFileReadWriteAcrossPool round-trips one larger file whose reads and
// writes fan out into multiple concurrent streams (and thus connections).
func (s *ConnPoolFSSuite) TestLargeFileReadWriteAcrossPool() {
	mp := s.volume.GetMountPath()
	path := filepath.Join(mp, "large.bin")
	want := bytes.Repeat([]byte("gMountie-pool-0123456789"), 1<<16) // ~1.5 MiB
	s.Require().NoError(os.WriteFile(path, want, 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Require().Equal(len(want), len(got))
	s.Require().True(bytes.Equal(want, got), "large-file round-trip mismatch across pool")
}

func TestConnPoolFSSuite(t *testing.T) { suite.Run(t, new(ConnPoolFSSuite)) }
```

- [ ] **Step 2: Compile-check (no FUSE here)**

Run: `go vet ./test/e2e/fs/`
Expected: clean. (The suite needs /dev/fuse to RUN; the controller runs it on the VM.)

- [ ] **Step 3: Commit**

```bash
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool add test/e2e/fs/connpool_test.go
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool commit -m "test(e2e): read/write correctness across a multi-connection pool"
```

---

### Task 6: Documentation

**Files:** the rpc config reference doc.

- [ ] **Step 1: Locate the rpc-knobs reference**

Run: `grep -rln "readahead_window\|retry_window\|initial_conn_window" docs/ website/ 2>/dev/null`
Pick the user-facing config reference that lists the `rpc.*` knobs (the one documenting `readahead_window` / `retry_window` for end users). Report which file you chose.

- [ ] **Step 2: Document `rpc.connections`**

Add an entry next to the other `rpc.*` knobs, matching the file's exact formatting:

```
rpc.connections (int, default 4, range 1-16): number of gRPC connections per
mount. Each connection is a separate TCP flow; Read/Write streams round-robin
across them so throughput can exceed a single flow's ceiling on high-BDP
(1Gbit/WAN) links. Metadata RPCs and the session keepalive/Subscribe streams use
the primary connection. Set to 1 for the historical single-connection behavior.
```

- [ ] **Step 3: Commit**

```bash
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool add docs/
git -C /home/john/git/gMountie/gMountie/.claude/worktrees/conn-pool commit -m "docs: document rpc.connections"
```

---

## Final verification (controller, after all tasks)

- [ ] **Touched-package gate** (local; `./...` can't run without FUSE):

```bash
go build ./...
go test ./pkg/client/config/ ./pkg/client/grpc/ ./pkg/client/io/
go vet ./pkg/client/config/ ./pkg/client/grpc/ ./pkg/client/io/ ./test/e2e/fs/
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2 run ./pkg/client/config/... ./pkg/client/grpc/... ./pkg/client/io/... ./test/e2e/fs/...
```
Expected: all green.

- [ ] **VM verification (gmountie-perf VM — has /dev/fuse + netem):**
  - `go test -race ./pkg/client/grpc/ ./pkg/client/io/`
  - `go test -run 'TestConnPoolFSSuite' ./test/e2e/fs/` → PASS (fds work across the pool)
  - Existing `test/e2e/fs` + `test/e2e/api` → green with default pool of 4
  - Throughput (record in PR): at **1Gbit** netem, single-file write and a sequential read with `connections=4` vs `connections=1` — expect the 4-conn numbers to exceed the 1-conn single-flow ceiling. At **100Mbit**, `connections=4` vs `1` — expect no regression.

- [ ] **Open the PR** summarizing the design, the 1Gbit lift + 100Mbit no-regression numbers, and the default of 4. No AI attribution.

---

## Self-Review (completed by plan author)

- **Spec coverage:** config knob → Task 1; pool + DataFileClient + interface/mock → Task 2; factory wiring → Task 3; data-plane spread (3 sites) → Task 4; primary pinning (File/Fs/Volume/Version + handshake/keepalive/Subscribe unchanged) → Task 2 (they keep using `conns[0]` stubs); error handling via retryOp → unchanged, exercised by existing io tests + Task 5; testing → Tasks 2/5 + final verification; docs → Task 6. Covered.
- **Placeholders:** none — every code step has complete code; the two "locate/adapt" steps (Task 5 harness API, Task 6 doc file) are explicit grep-and-select instructions, not vague placeholders.
- **Type/name consistency:** `Connections` / `DefaultRpcConnections` / `connections` (mapstructure) / `cfg.Rpc.Connections`; `DataFileClient()` / `dataFileClients` / `rrCounter` / `WithConnections` used consistently across Tasks 1–4. `conns[0]` is the primary everywhere.
