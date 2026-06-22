# Client Architecture — Extensible Layering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the client's `FileSystemBackend` decorator seam into a clean, composable, well-contracted layer stack, proven by a first real consumer (an op-level metrics observer layer) and a `Recorder` seam that removes the global-dispatcher metrics debt.

**Architecture:** Evolve, don't rewrite. Add (a) a `PassthroughBackend` embeddable base + a documented behavioral contract + a conformance/forwarding test (the safety foundation); (b) constrained, named-position composition replacing the inline `if`-ladder, with capabilities resolved once into a `MountParams` value before the stack is built; (c) a metrics **observer layer** plus a `metrics.Recorder` interface injected per-client, migrating the 21 package-global fan-out call sites onto it and deleting the global dispatcher.

**Tech Stack:** Go, `go.gmountie.dev/gmountie/pkg/client/...`, testify suites, mockery mocks (`internal/mocks`), Prometheus client, go-fuse v2.

## Global Constraints

- Module path: `go.gmountie.dev/gmountie` (vanity); internal imports `go.gmountie.dev/gmountie/pkg/...`.
- **Behavior-preserving.** Every task keeps existing behavior and the existing Prometheus series names identical. No new wire/proto surface. No server changes.
- Logging: `go.gmountie.dev/gmountie/pkg/utils/log` (`log.Log`). Errors: `github.com/pkg/errors`.
- Tests are testify suites (`func (s *XSuite) TestX()`), never bare `func TestX`. Mocks live in `internal/mocks/` — never hand-edit; regenerate with `task gen:mocks` only if an interface/ctor signature changes.
- `proto.FsError` (not `error`) is the backend status type; `proto.FsError_FS_OK == 0`.
- Build/test: `task test` → `go test -failfast -v ./...`. Single test: `go test -v -run TestName ./pkg/client/io/...`. Lint: `task lint`. Touched-package gate: run the union of touched packages locally (can't run `./...` — FUSE).
- Commit style: conventional-commit subject + short body; **no AI attribution** anywhere.

---

## File Structure

- `pkg/client/io/passthrough.go` (new) — `PassthroughBackend` observer base.
- `pkg/client/io/passthrough_test.go` (new) — forwarding conformance + semantic-no-embed guard.
- `pkg/client/io/backend.go` (modify) — add the behavioral contract doc comment to `FileSystemBackend`.
- `pkg/client/metrics/recorder.go` (new) — `Recorder` interface + op-level collectors; `*Metrics` satisfies it.
- `pkg/client/io/metricslayer.go` (new) — the metrics observer layer (embeds `PassthroughBackend`).
- `pkg/client/io/metricslayer_test.go` (new).
- `pkg/client/mount/params.go` (new) — `MountParams` + `negotiateMountParams`.
- `pkg/client/mount/compose.go` (new) — named-position layer composition.
- `pkg/client/mount/compose_test.go` (new).
- `pkg/client/mount/single.go` (modify) — `Mount` uses `negotiateMountParams` + `composeBackend`.
- `pkg/client/cache/backend.go` + `store.go` + `data.go` + `subscriber.go` (modify) — accept an injected `Recorder`; replace global `metrics.*` calls.
- `pkg/client/io/retry.go` (modify) — emit retries via `client.Metrics()` instead of the global.
- `pkg/client/metrics/metrics.go` (modify) — delete the global dispatcher (`instances`, `RegisterInstance`, `UnregisterInstance`, `sameCollectors`, and the fan-out funcs).
- `pkg/client/grpc/factory.go` + `client.go` (modify) — drop `RegisterInstance`/`UnregisterInstance`.

---

## Task 1: `PassthroughBackend` observer base

**Files:**
- Create: `pkg/client/io/passthrough.go`
- Test: `pkg/client/io/passthrough_test.go`

**Interfaces:**
- Consumes: `io.FileSystemBackend`, `io.FileHandle`, `io.Attr`, `io.SetAttrIn`, `io.DirEntryPlus`, `io.StatFs` (all in `pkg/client/io/backend.go`); `proto.FsError`; `fuse.FileLock`.
- Produces: `type PassthroughBackend struct { Inner io.FileSystemBackend }` implementing `io.FileSystemBackend` by forwarding every method to `Inner`. Embed in observer layers.

- [ ] **Step 1: Write the failing test**

`pkg/client/io/passthrough_test.go`:
```go
package io

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/stretchr/testify/suite"
)

// recordingBackend records the last method called and returns canned values.
type recordingBackend struct {
	FileSystemBackend // embed so unimplemented methods panic loudly if hit unexpectedly
	lastCall string
	closed   bool
}

func (r *recordingBackend) Stat(_ context.Context, path string) (*Attr, proto.FsError) {
	r.lastCall = "Stat:" + path
	return &Attr{Ino: 7}, proto.FsError_FS_OK
}
func (r *recordingBackend) Close() error { r.closed = true; return nil }

type PassthroughSuite struct {
	suite.Suite
	inner *recordingBackend
	pt    *PassthroughBackend
}

func (s *PassthroughSuite) SetupTest() {
	s.inner = &recordingBackend{}
	s.pt = &PassthroughBackend{Inner: s.inner}
}

func (s *PassthroughSuite) TestForwardsStat() {
	attr, st := s.pt.Stat(context.Background(), "/x")
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal(uint64(7), attr.Ino)
	s.Equal("Stat:/x", s.inner.lastCall)
}

func (s *PassthroughSuite) TestForwardsClose() {
	s.NoError(s.pt.Close())
	s.True(s.inner.closed)
}

// Compile-time assertion: PassthroughBackend implements the full interface.
var _ FileSystemBackend = (*PassthroughBackend)(nil)

func TestPassthroughSuite(t *testing.T) { suite.Run(t, new(PassthroughSuite)) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -v -run TestPassthroughSuite ./pkg/client/io/...`
Expected: FAIL — `undefined: PassthroughBackend`.

- [ ] **Step 3: Write the implementation**

`pkg/client/io/passthrough.go`:
```go
package io

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// PassthroughBackend is an embeddable base for OBSERVER layers — layers that
// add side effects (metrics, tracing, audit) without changing behavior. Embed
// it, set Inner, and override only the ops you observe; all other ops forward
// to Inner unchanged.
//
// DO NOT embed this in a SEMANTIC layer (cache, write-batcher, WAL). A new
// interface method would forward silently and bypass the layer's behavior — a
// stale-data bug. Semantic layers must implement FileSystemBackend explicitly
// so the compiler forces a decision on every method. (Enforced by
// TestSemanticLayersDoNotEmbedPassthrough.)
type PassthroughBackend struct {
	Inner FileSystemBackend
}

func (p *PassthroughBackend) Stat(ctx context.Context, path string) (*Attr, proto.FsError) {
	return p.Inner.Stat(ctx, path)
}
func (p *PassthroughBackend) GetAttrIfChanged(ctx context.Context, path string, knownVersion uint64) (*Attr, bool, proto.FsError) {
	return p.Inner.GetAttrIfChanged(ctx, path, knownVersion)
}
func (p *PassthroughBackend) Lookup(ctx context.Context, parent, name string) (*Attr, proto.FsError) {
	return p.Inner.Lookup(ctx, parent, name)
}
func (p *PassthroughBackend) ListDir(ctx context.Context, path string) ([]DirEntryPlus, proto.FsError) {
	return p.Inner.ListDir(ctx, path)
}
func (p *PassthroughBackend) Access(ctx context.Context, path string, mode uint32) proto.FsError {
	return p.Inner.Access(ctx, path, mode)
}
func (p *PassthroughBackend) StatFs(ctx context.Context, path string) (*StatFs, proto.FsError) {
	return p.Inner.StatFs(ctx, path)
}
func (p *PassthroughBackend) GetXAttr(ctx context.Context, path, attr string) ([]byte, proto.FsError) {
	return p.Inner.GetXAttr(ctx, path, attr)
}
func (p *PassthroughBackend) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) proto.FsError {
	return p.Inner.SetXAttr(ctx, path, attr, data, flags)
}
func (p *PassthroughBackend) RemoveXAttr(ctx context.Context, path, attr string) proto.FsError {
	return p.Inner.RemoveXAttr(ctx, path, attr)
}
func (p *PassthroughBackend) ListXAttr(ctx context.Context, path string) ([]string, proto.FsError) {
	return p.Inner.ListXAttr(ctx, path)
}
func (p *PassthroughBackend) Open(ctx context.Context, path string, flags uint32) (FileHandle, proto.FsError) {
	return p.Inner.Open(ctx, path, flags)
}
func (p *PassthroughBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (FileHandle, *Attr, proto.FsError) {
	return p.Inner.Create(ctx, parent, name, flags, mode)
}
func (p *PassthroughBackend) Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, proto.FsError) {
	return p.Inner.Read(ctx, fh, off, dest)
}
func (p *PassthroughBackend) Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	return p.Inner.Write(ctx, fh, off, data)
}
func (p *PassthroughBackend) Release(ctx context.Context, fh FileHandle) proto.FsError {
	return p.Inner.Release(ctx, fh)
}
func (p *PassthroughBackend) Flush(ctx context.Context, fh FileHandle) proto.FsError {
	return p.Inner.Flush(ctx, fh)
}
func (p *PassthroughBackend) Fsync(ctx context.Context, fh FileHandle, flags int64) proto.FsError {
	return p.Inner.Fsync(ctx, fh, flags)
}
func (p *PassthroughBackend) Allocate(ctx context.Context, fh FileHandle, off, size uint64, mode uint32) proto.FsError {
	return p.Inner.Allocate(ctx, fh, off, size, mode)
}
func (p *PassthroughBackend) GetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) proto.FsError {
	return p.Inner.GetLk(ctx, fh, owner, lk, flags, out)
}
func (p *PassthroughBackend) SetLk(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return p.Inner.SetLk(ctx, fh, owner, lk, flags)
}
func (p *PassthroughBackend) SetLkw(ctx context.Context, fh FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) proto.FsError {
	return p.Inner.SetLkw(ctx, fh, owner, lk, flags)
}
func (p *PassthroughBackend) CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, fhOut FileHandle, offOut uint64, length, flags uint64) (uint64, proto.FsError) {
	return p.Inner.CopyFileRange(ctx, fhIn, offIn, fhOut, offOut, length, flags)
}
func (p *PassthroughBackend) Lseek(ctx context.Context, fh FileHandle, offset uint64, whence uint32) (uint64, proto.FsError) {
	return p.Inner.Lseek(ctx, fh, offset, whence)
}
func (p *PassthroughBackend) Mkdir(ctx context.Context, path string, mode uint32) (*Attr, proto.FsError) {
	return p.Inner.Mkdir(ctx, path, mode)
}
func (p *PassthroughBackend) Rmdir(ctx context.Context, path string) proto.FsError {
	return p.Inner.Rmdir(ctx, path)
}
func (p *PassthroughBackend) Unlink(ctx context.Context, path string) proto.FsError {
	return p.Inner.Unlink(ctx, path)
}
func (p *PassthroughBackend) Rename(ctx context.Context, oldPath, newPath string) proto.FsError {
	return p.Inner.Rename(ctx, oldPath, newPath)
}
func (p *PassthroughBackend) Readlink(ctx context.Context, path string) (string, proto.FsError) {
	return p.Inner.Readlink(ctx, path)
}
func (p *PassthroughBackend) Symlink(ctx context.Context, target, linkPath string) (*Attr, proto.FsError) {
	return p.Inner.Symlink(ctx, target, linkPath)
}
func (p *PassthroughBackend) SetAttr(ctx context.Context, path string, in SetAttrIn) (*Attr, proto.FsError) {
	return p.Inner.SetAttr(ctx, path, in)
}
func (p *PassthroughBackend) Close() error { return p.Inner.Close() }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -v -run TestPassthroughSuite ./pkg/client/io/...`
Expected: PASS. (If a method is missing, the `var _ FileSystemBackend` assertion fails the build — that's the intended guard.)

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/passthrough.go pkg/client/io/passthrough_test.go
git commit -m "feat(client/io): add PassthroughBackend observer base

Embeddable full-surface forwarder for observer layers (metrics/trace/audit).
A compile-time interface assertion keeps it complete; semantic layers must NOT
embed it (enforced in a later task)."
```

---

## Task 2: Behavioral contract + semantic-no-embed guard

**Files:**
- Modify: `pkg/client/io/backend.go` (doc comment on `FileSystemBackend`, ~line 110)
- Test: `pkg/client/io/passthrough_test.go` (append)

**Interfaces:**
- Consumes: `PassthroughBackend` (Task 1); `reflect`.
- Produces: a documented contract on `FileSystemBackend`; `TestSemanticLayersDoNotEmbedPassthrough` guarding that semantic backend types never embed `PassthroughBackend`.

- [ ] **Step 1: Write the failing test (append to `passthrough_test.go`)**

```go
import "reflect" // add to the existing import block

// semanticBackendTypes lists every SEMANTIC layer (changes behavior). Add new
// ones here; the test fails if any embeds PassthroughBackend (silent-forward
// hazard). Observer layers (metrics/trace/audit) are intentionally absent.
func semanticBackendTypes() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(BackendClient{}),
	}
}

func (s *PassthroughSuite) TestSemanticLayersDoNotEmbedPassthrough() {
	ptName := reflect.TypeOf(PassthroughBackend{}).Name()
	for _, t := range semanticBackendTypes() {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			s.Falsef(f.Anonymous && f.Type.Name() == ptName,
				"%s embeds PassthroughBackend; semantic layers must implement the full interface explicitly", t.Name())
		}
	}
}
```

- [ ] **Step 2: Run to verify it passes immediately (guard, not red-first)**

Run: `go test -v -run TestPassthroughSuite ./pkg/client/io/...`
Expected: PASS (`BackendClient` does not embed `PassthroughBackend`). This is a regression guard; it has no red phase.

- [ ] **Step 3: Add the contract doc comment**

In `pkg/client/io/backend.go`, replace the doc comment immediately above `type FileSystemBackend interface {` with:
```go
// FileSystemBackend is the client's filesystem operation surface and the
// decorator seam: layers wrap an inner FileSystemBackend.
//
// Behavioral contract (all layers must honor):
//   - Status: every op returns proto.FsError; FS_OK (0) means success. Layers
//     propagate the inner status unchanged unless they deliberately handle it.
//   - Retry ownership: ONLY the transport leaf retries (see retryOp). No other
//     layer re-attempts a failed op; layers above propagate errors upward.
//   - Idempotency: mutating ops carry a request_id at the transport so the
//     server dedups a retried attempt. Layers must not duplicate a mutating op.
//   - Ordering/durability: writes may be buffered and acked optimistically;
//     durability is established on Flush/Fsync/Release. A layer that defers a
//     write must drain at these boundaries (today only the transport does).
//   - Handles: Open/Create return a FileHandle; a layer that wraps handles must
//     implement FileHandle.Unwrap() returning its inner so resolveHandle reaches
//     the transport leaf.
//   - Invalidation flows UP: the transport owns the Subscribe stream; events
//     propagate outward (transport -> ... -> cache -> node).
//   - Observer vs semantic: observer layers (metrics/trace/audit) may embed
//     PassthroughBackend; semantic layers (cache, write-batcher, WAL) must
//     implement every method explicitly.
type FileSystemBackend interface {
```

- [ ] **Step 4: Verify build + test**

Run: `go test -v -run TestPassthroughSuite ./pkg/client/io/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend.go pkg/client/io/passthrough_test.go
git commit -m "docs(client/io): document FileSystemBackend behavioral contract + guard semantic layers

Lift the ordering/retry-ownership/idempotency/handle/invalidation semantics
into a written contract on the interface, and add a reflection guard asserting
semantic layers never embed PassthroughBackend."
```

---

## Task 3: `MountParams` — resolve capabilities once, before the stack

**Files:**
- Create: `pkg/client/mount/params.go`
- Test: `pkg/client/mount/params_test.go`

**Interfaces:**
- Consumes: `grpc.Client` (mount's `clientgrpc`/`grpc` alias — same `pkg/client/grpc.Client`), `config.FUSEConfig`, `negotiateMaxWriteBytes` (`common.go`), `proto`, `mappingModeSquash` (`single.go:25`).
- Produces:
  ```go
  type MountParams struct {
      MaxWriteBytes      int
      DefaultPermissions bool
  }
  func negotiateMountParams(client grpc.Client, fuseCfg *config.FUSEConfig, rawIDs bool, volume string) (MountParams, *io.IDRewriter)
  ```
  `negotiateMountParams` runs the existing `Version` (via `negotiateMaxWriteBytes`) and `WhoAmI` calls and returns the resolved params + rewriter, to be called BEFORE the backend is built.

- [ ] **Step 1: Write the failing test**

`pkg/client/mount/params_test.go`:
```go
package mount

import (
	"testing"
	"time"

	grpcmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/stretchr/testify/suite"
)

type MountParamsSuite struct {
	suite.Suite
	client *grpcmocks.MockClient
}

func (s *MountParamsSuite) SetupTest() {
	s.client = grpcmocks.NewMockClient(s.T())
	s.client.EXPECT().MetaTimeout().Return(2 * time.Second).Maybe()
}

func (s *MountParamsSuite) TestRawIDsSkipsWhoAmI() {
	ver := grpcmocks.NewMockVersionServiceClient(s.T())
	ver.EXPECT().Get(mock.Anything, mock.Anything).
		Return(&proto.VersionReply{FrameSizeBytes: 0}, nil).Once()
	s.client.EXPECT().Version().Return(ver).Once()

	params, rewriter := negotiateMountParams(s.client, &config.FUSEConfig{MaxWriteBytes: 1 << 20}, true, "vol")
	s.Equal(1<<20, params.MaxWriteBytes)
	s.False(params.DefaultPermissions)
	s.Nil(rewriter)
}
```
(Import `github.com/stretchr/testify/mock` and `grpc`-version mock as needed; mirror `backend_grpc_test.go` setup.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test -v -run TestMountParamsSuite ./pkg/client/mount/...`
Expected: FAIL — `undefined: negotiateMountParams`.

- [ ] **Step 3: Implement `params.go`**

`pkg/client/mount/params.go`:
```go
package mount

import (
	"context"
	"os"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// MountParams is the set of capabilities resolved ONCE, before the backend
// stack is built, from the existing Version + WhoAmI RPCs. Threading it as one
// value (rather than scattered positional args) fixes the old ordering bug
// where the backend was constructed before WhoAmI ran, and gives future layers
// a single place to read negotiated capabilities. No new wire surface.
type MountParams struct {
	MaxWriteBytes      int
	DefaultPermissions bool
}

// negotiateMountParams runs version negotiation and (unless rawIDs) WhoAmI,
// returning the resolved params and an optional IDRewriter. Soft-fails to
// configured/raw defaults exactly as the prior inline code did.
func negotiateMountParams(client grpc.Client, fuseCfg *config.FUSEConfig, rawIDs bool, volume string) (MountParams, *io.IDRewriter) {
	params := MountParams{MaxWriteBytes: negotiateMaxWriteBytes(client, fuseCfg)}
	if rawIDs {
		return params, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.MetaTimeout())
	defer cancel()
	idResp, err := client.WhoAmI(ctx, volume)
	if err != nil {
		log.Log.Warn("WhoAmI failed, mounting with raw IDs", zap.String("volume", volume), zap.Error(err))
		return params, nil
	}
	rewriter := io.NewIDRewriter(identityFromProto(idResp), uint32(os.Getuid()), uint32(os.Getgid()))
	params.DefaultPermissions = idResp.GetMappingMode() == mappingModeSquash
	return params, rewriter
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test -v -run TestMountParamsSuite ./pkg/client/mount/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/params.go pkg/client/mount/params_test.go
git commit -m "feat(client/mount): resolve MountParams (Version+WhoAmI) as one pre-build value

Bundle the existing version + WhoAmI negotiation into MountParams + the
rewriter, computed before the backend is built. No new wire surface; behavior
identical to the prior inline negotiation."
```

---

## Task 4: Named-position composition + wire `Mount` to it

**Files:**
- Create: `pkg/client/mount/compose.go`
- Test: `pkg/client/mount/compose_test.go`
- Modify: `pkg/client/mount/single.go` (`Mount`, lines ~84-167)

**Interfaces:**
- Consumes: `io.FileSystemBackend`, `MountParams` (Task 3).
- Produces:
  ```go
  type layerPos int
  const ( posObserver layerPos = iota; posCache; posWritePath; posTransport )
  type backendLayer struct { pos layerPos; build func(inner io.FileSystemBackend) io.FileSystemBackend }
  func composeBackend(transport io.FileSystemBackend, layers []backendLayer) io.FileSystemBackend
  ```
  `composeBackend` folds layers from innermost (highest pos) to outermost (lowest pos) around the transport leaf. A layer is assigned to a *named* position, never an index — misordering is impossible.

- [ ] **Step 1: Write the failing test**

`pkg/client/mount/compose_test.go`:
```go
package mount

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

type ComposeSuite struct{ suite.Suite }

func (s *ComposeSuite) TestFoldsOutermostFirstRegardlessOfSliceOrder() {
	var order []string
	mk := func(name string) func(io.FileSystemBackend) io.FileSystemBackend {
		return func(inner io.FileSystemBackend) io.FileSystemBackend {
			order = append(order, name)
			return inner // identity wrapper for the test
		}
	}
	transport := &PassthroughCounter{} // see helper below
	// Deliberately out-of-order slice: cache before observer.
	layers := []backendLayer{
		{pos: posCache, build: mk("cache")},
		{pos: posObserver, build: mk("observer")},
	}
	got := composeBackend(transport, layers)
	s.NotNil(got)
	// Innermost built first: cache (closer to transport) before observer (outermost).
	s.Equal([]string{"cache", "observer"}, order)
}

// PassthroughCounter is a minimal io.FileSystemBackend for composition tests.
type PassthroughCounter struct{ io.PassthroughBackend }

func TestComposeSuite(t *testing.T) { suite.Run(t, new(ComposeSuite)) }
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -v -run TestComposeSuite ./pkg/client/mount/...`
Expected: FAIL — `undefined: composeBackend` / `backendLayer`.

- [ ] **Step 3: Implement `compose.go`**

`pkg/client/mount/compose.go`:
```go
package mount

import (
	"sort"

	"go.gmountie.dev/gmountie/pkg/client/io"
)

// layerPos orders backend layers from outermost (closest to FUSE) to innermost
// (the transport leaf). Layers declare a NAMED position; the stack cannot be
// misordered by an index. The writeBatcher/WAL slot (posWritePath) is reserved
// and unused today.
type layerPos int

const (
	posObserver  layerPos = iota // metrics / tracing / audit — outermost
	posCache                     // read/attr/dir/data cache
	posWritePath                 // writeBatcher / WAL slot (reserved; empty now)
	posTransport                 // the gRPC leaf — innermost, always present
)

// backendLayer is one optional layer at a named position.
type backendLayer struct {
	pos   layerPos
	build func(inner io.FileSystemBackend) io.FileSystemBackend
}

// composeBackend wraps transport (innermost) with each layer, innermost-first,
// so the result is node -> observer -> cache -> [writePath] -> transport.
func composeBackend(transport io.FileSystemBackend, layers []backendLayer) io.FileSystemBackend {
	sorted := make([]backendLayer, len(layers))
	copy(sorted, layers)
	// Build innermost-first: higher pos value = closer to transport = built earlier.
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].pos > sorted[j].pos })
	result := transport
	for _, l := range sorted {
		result = l.build(result)
	}
	return result
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `go test -v -run TestComposeSuite ./pkg/client/mount/...`
Expected: PASS.

- [ ] **Step 5: Rewire `Mount` to use `negotiateMountParams` + `composeBackend`**

In `pkg/client/mount/single.go`, replace the body from `maxWrite := negotiateMaxWriteBytes(...)` through the `establishMount(...)` call (the block quoted in the design's ground-truth) with:
```go
	params, rewriter := negotiateMountParams(m.client, m.fuse, m.rawIDs, volume)

	backendOpts := []io.BackendOption{
		io.WithPlusListings(m.cache.Enabled),
		io.WithXattrListings(m.cache.Enabled),
	}
	if m.cache.Enabled {
		backendOpts = append(backendOpts, io.WithoutReadahead())
	}
	transport := io.NewBackendClient(m.client, volume, backendOpts...)

	var layers []backendLayer
	if m.cache.Enabled {
		root := filepath.Join(m.cache.Path, volume)
		var gcMetrics persist.GCMetrics
		if cm := m.client.Metrics(); cm != nil {
			gcMetrics = persistGCMetrics{cm}
		}
		p, err := persist.Open(persist.Options{Root: root, DiskMaxBytes: int64(m.cache.DiskMaxBytes), Metrics: gcMetrics})
		if err != nil {
			return errors.Wrap(err, "open cache persist")
		}
		m.persists.Store(volume, p)
		client := m.client // capture for the closure
		cacheCfg := cache.ConfigFromClient(m.cache)
		layers = append(layers, backendLayer{pos: posCache, build: func(inner io.FileSystemBackend) io.FileSystemBackend {
			return cache.NewCachedBackend(inner, cacheCfg, p, client.Fs(), volume)
		}})
	}

	backend := composeBackend(transport, layers)
	m.backends.Store(volume, backend)

	handle, err := establishMount(mountPath, volume, m.client.GetEndpoint(), backend, rewriter, m.fuse, params.MaxWriteBytes, m.client.MetaTimeout(), params.DefaultPermissions)
	if err != nil {
		return err
	}
```
Remove the now-unused inline `maxWrite`, `useDefaultPermissions`, and the inline WhoAmI block (moved into `negotiateMountParams`). Keep imports `context`/`os` only if still used elsewhere in the file; otherwise drop them (the linter will flag unused).

- [ ] **Step 6: Run the mount package tests + lint**

Run: `go test -v ./pkg/client/mount/...`
Expected: PASS (existing mount tests still green — behavior preserved).
Run: `task lint`
Expected: no new findings (fix any unused-import on `single.go`).

- [ ] **Step 7: Commit**

```bash
git add pkg/client/mount/compose.go pkg/client/mount/compose_test.go pkg/client/mount/single.go
git commit -m "refactor(client/mount): named-position layer composition replaces the if-ladder

Mount now resolves MountParams before building the stack, builds the transport
leaf, and folds optional layers (cache today) via composeBackend at named
positions. Behavior-preserving; reserves the writePath slot for a future
write-batcher/WAL."
```

---

## Task 5: `metrics.Recorder` interface + op-level collectors

**Files:**
- Create: `pkg/client/metrics/recorder.go`
- Modify: `pkg/client/metrics/metrics.go` (add op-level collectors + their setters; register them)
- Test: `pkg/client/metrics/recorder_test.go`

**Interfaces:**
- Consumes: `*metrics.Metrics` (existing).
- Produces:
  ```go
  type Recorder interface {
      RetryInc(op, code string)
      CacheHitInc(tier, cacheType string)
      CacheMissInc(cacheType string)
      CacheDedupeHitInc()
      CachePersistDroppedInc()
      CacheRevalidationInc(result string)
      SubscribeEventReceivedInc(kind string)
      SubscribeStreamStateSet(up bool)
      CacheUnverifiedAdd(seconds float64)
      InFlightInc(op string)
      InFlightDec(op string)
      ObserveOp(op string, seconds float64, code string) // op-level boundary signal
  }
  var _ Recorder = (*Metrics)(nil)
  ```

- [ ] **Step 1: Write the failing test**

`pkg/client/metrics/recorder_test.go`:
```go
package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/suite"
)

type RecorderSuite struct {
	suite.Suite
	reg *prometheus.Registry
	m   *Metrics
}

func (s *RecorderSuite) SetupTest() {
	s.reg = prometheus.NewRegistry()
	s.m = NewMetrics()
	s.Require().NoError(s.m.Register(s.reg))
}

func (s *RecorderSuite) TestMetricsSatisfiesRecorder() {
	var r Recorder = s.m
	r.ObserveOp("Read", 0.005, "FS_OK")
	mf, err := s.reg.Gather()
	s.NoError(err)
	var found bool
	for _, f := range mf {
		if f.GetName() == "gmountie_client_op_seconds" {
			found = true
		}
	}
	s.True(found, "op latency histogram should be registered and observed")
	_ = dto.MetricFamily{} // ensure dto import used
}

func TestRecorderSuite(t *testing.T) { suite.Run(t, new(RecorderSuite)) }
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -v -run TestRecorderSuite ./pkg/client/metrics/...`
Expected: FAIL — `undefined: Recorder` / `ObserveOp`.

- [ ] **Step 3: Add op-level collectors to `metrics.go`**

In `pkg/client/metrics/metrics.go`, add to the `Metrics` struct:
```go
	// Op-level boundary metrics, emitted by the metrics observer layer.
	OpSeconds *prometheus.HistogramVec
```
In `NewMetrics()`, add:
```go
		OpSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "gmountie_client_op_seconds",
			Help:    "FileSystemBackend op latency in seconds, by op and FsError code.",
			Buckets: prometheus.DefBuckets,
		}, []string{"op", "code"}),
```
Add `m.OpSeconds` to both `MustRegister(...)` and the `Register(...)` collector slice (and add a `*prometheus.HistogramVec` case to the `Register` adoption switch mirroring `CounterVec`). Add the setter:
```go
// ObserveOp records one op's latency, labelled by op name and FsError code.
func (m *Metrics) ObserveOp(op string, seconds float64, code string) {
	m.OpSeconds.WithLabelValues(op, code).Observe(seconds)
}
```

- [ ] **Step 4: Create `recorder.go`**

`pkg/client/metrics/recorder.go`:
```go
package metrics

// Recorder is the per-client metrics sink injected into the layers (cache,
// the metrics observer) and used by the transport retry path. *Metrics is the
// default Prometheus implementation; OTel/audit backends implement Recorder
// later without touching the layers. Defining it here (a leaf package with no
// io/grpc deps) removes the old import cycle that the package-global dispatcher
// existed to dodge.
type Recorder interface {
	RetryInc(op, code string)
	CacheHitInc(tier, cacheType string)
	CacheMissInc(cacheType string)
	CacheDedupeHitInc()
	CachePersistDroppedInc()
	CacheRevalidationInc(result string)
	SubscribeEventReceivedInc(kind string)
	SubscribeStreamStateSet(up bool)
	CacheUnverifiedAdd(seconds float64)
	InFlightInc(op string)
	InFlightDec(op string)
	ObserveOp(op string, seconds float64, code string)
}

var _ Recorder = (*Metrics)(nil)
```
(All listed methods except `ObserveOp` already exist on `*Metrics`.)

- [ ] **Step 5: Run to verify it passes**

Run: `go test -v -run TestRecorderSuite ./pkg/client/metrics/...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/metrics/recorder.go pkg/client/metrics/recorder_test.go pkg/client/metrics/metrics.go
git commit -m "feat(client/metrics): add Recorder interface + op-level latency collector

Recorder is the per-client sink the layers will depend on (Prometheus *Metrics
satisfies it; OTel/audit later). Adds gmountie_client_op_seconds for the
upcoming metrics observer layer."
```

---

## Task 6: Metrics observer layer

**Files:**
- Create: `pkg/client/io/metricslayer.go`
- Test: `pkg/client/io/metricslayer_test.go`

**Interfaces:**
- Consumes: `PassthroughBackend` (Task 1), `metrics.Recorder` (Task 5), `proto.FsError`.
- Produces:
  ```go
  func NewMetricsLayer(inner FileSystemBackend, rec metrics.Recorder) FileSystemBackend
  ```
  An observer layer embedding `PassthroughBackend`, timing a representative set of ops and emitting via `rec.ObserveOp`. New ops added to the interface forward untimed (acceptable for an observer) until someone opts them in.

- [ ] **Step 1: Write the failing test**

`pkg/client/io/metricslayer_test.go`:
```go
package io

import (
	"context"
	"testing"

	metricsmocks "go.gmountie.dev/gmountie/internal/mocks/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type MetricsLayerSuite struct {
	suite.Suite
	inner *recordingBackend // from passthrough_test.go (same package)
	rec   *metricsmocks.MockRecorder
}

func (s *MetricsLayerSuite) SetupTest() {
	s.inner = &recordingBackend{}
	s.rec = metricsmocks.NewMockRecorder(s.T())
}

func (s *MetricsLayerSuite) TestStatRecordsOpLatency() {
	s.rec.EXPECT().ObserveOp("Stat", mock.AnythingOfType("float64"), "FS_OK").Once()
	layer := NewMetricsLayer(s.inner, s.rec)
	_, st := layer.Stat(context.Background(), "/x")
	s.Equal(proto.FsError_FS_OK, st)
	s.Equal("Stat:/x", s.inner.lastCall) // still forwarded
}

func TestMetricsLayerSuite(t *testing.T) { suite.Run(t, new(MetricsLayerSuite)) }
```
(Requires a generated mock for `metrics.Recorder`. Add `Recorder` to `.mockery.yml` under the `pkg/client/metrics` package and run `task gen:mocks` as Step 3a below.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test -v -run TestMetricsLayerSuite ./pkg/client/io/...`
Expected: FAIL — `undefined: NewMetricsLayer` (and missing `MockRecorder`).

- [ ] **Step 3a: Register the Recorder mock and regenerate**

Add `Recorder` to the `pkg/client/metrics` entry in `.mockery.yml` (mirror an existing interface entry). Run:
```bash
task gen:mocks
```
Expected: `internal/mocks/pkg/client/metrics/MockRecorder.go` created.

- [ ] **Step 3b: Implement `metricslayer.go`**

`pkg/client/io/metricslayer.go`:
```go
package io

import (
	"context"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/proto"
)

// metricsLayer is an OBSERVER layer: it times a representative set of ops and
// emits via the injected Recorder, forwarding everything else unchanged. It
// embeds PassthroughBackend (observer base) so ops it does not time still work;
// it adds NO retry and changes NO behavior (contract: observers are transparent).
type metricsLayer struct {
	PassthroughBackend
	rec metrics.Recorder
}

// NewMetricsLayer wraps inner with op-level boundary metrics.
func NewMetricsLayer(inner FileSystemBackend, rec metrics.Recorder) FileSystemBackend {
	return &metricsLayer{PassthroughBackend: PassthroughBackend{Inner: inner}, rec: rec}
}

func (l *metricsLayer) Stat(ctx context.Context, path string) (*Attr, proto.FsError) {
	start := time.Now()
	attr, st := l.Inner.Stat(ctx, path)
	l.rec.ObserveOp("Stat", time.Since(start).Seconds(), st.String())
	return attr, st
}

func (l *metricsLayer) Read(ctx context.Context, fh FileHandle, off int64, dest []byte) (int, proto.FsError) {
	start := time.Now()
	n, st := l.Inner.Read(ctx, fh, off, dest)
	l.rec.ObserveOp("Read", time.Since(start).Seconds(), st.String())
	return n, st
}

func (l *metricsLayer) Write(ctx context.Context, fh FileHandle, off int64, data []byte) (uint32, proto.FsError) {
	start := time.Now()
	n, st := l.Inner.Write(ctx, fh, off, data)
	l.rec.ObserveOp("Write", time.Since(start).Seconds(), st.String())
	return n, st
}

func (l *metricsLayer) Lookup(ctx context.Context, parent, name string) (*Attr, proto.FsError) {
	start := time.Now()
	attr, st := l.Inner.Lookup(ctx, parent, name)
	l.rec.ObserveOp("Lookup", time.Since(start).Seconds(), st.String())
	return attr, st
}

func (l *metricsLayer) ListDir(ctx context.Context, path string) ([]DirEntryPlus, proto.FsError) {
	start := time.Now()
	es, st := l.Inner.ListDir(ctx, path)
	l.rec.ObserveOp("ListDir", time.Since(start).Seconds(), st.String())
	return es, st
}
```
(`proto.FsError` has a `String()` method via the generated enum; confirm and use it for the `code` label.)

- [ ] **Step 4: Run to verify it passes**

Run: `go test -v -run TestMetricsLayerSuite ./pkg/client/io/...`
Expected: PASS.

- [ ] **Step 5: Wire the observer layer into composition (mount)**

In `pkg/client/mount/single.go` `Mount`, after building `layers` and before `composeBackend`, prepend the observer when a recorder exists:
```go
	if rec := m.client.Metrics(); rec != nil {
		layers = append(layers, backendLayer{pos: posObserver, build: func(inner io.FileSystemBackend) io.FileSystemBackend {
			return io.NewMetricsLayer(inner, rec)
		}})
	}
```
(`m.client.Metrics()` returns `*metrics.Metrics`, which satisfies `metrics.Recorder`.)

- [ ] **Step 6: Run mount + io tests**

Run: `go test -v ./pkg/client/io/... ./pkg/client/mount/...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/client/io/metricslayer.go pkg/client/io/metricslayer_test.go pkg/client/mount/single.go .mockery.yml internal/mocks/pkg/client/metrics/
git commit -m "feat(client/io): op-level metrics observer layer (first stack consumer)

An observer layer (PassthroughBackend base) timing Stat/Read/Write/Lookup/
ListDir via the injected Recorder, slotted at posObserver in the composition.
Proves the layer seam end-to-end with a real consumer."
```

---

## Task 7: Inject `Recorder` into cache/io; delete the global dispatcher

**Files:**
- Modify: `pkg/client/cache/backend.go` (ctor takes `Recorder`; replace 8 `metrics.*` calls), `pkg/client/cache/store.go` (3 calls), `pkg/client/cache/data.go` (1 call), `pkg/client/cache/subscriber.go` (6 calls)
- Modify: `pkg/client/io/retry.go` (1 call → via `client.Metrics()`)
- Modify: `pkg/client/mount/single.go` (pass recorder to `NewCachedBackend`)
- Modify: `pkg/client/metrics/metrics.go` (delete dispatcher), `pkg/client/grpc/factory.go` + `client.go` (drop RegisterInstance/UnregisterInstance)

**Interfaces:**
- Consumes: `metrics.Recorder` (Task 5); `client.Metrics()` (returns `*metrics.Metrics`).
- Produces: `cache.NewCachedBackend(inner io.FileSystemBackend, cfg Config, p *persist.Persist, client invalidationSource, volume string, rec metrics.Recorder) io.FileSystemBackend` — one new trailing param threaded to `store`/`data`/`subscriber` sub-components.

- [ ] **Step 1: Thread `Recorder` into the cache constructor (write the failing test)**

Add to a cache test (e.g. `pkg/client/cache/backend_test.go`) a case asserting a cache miss calls `rec.CacheMissInc`:
```go
func (s *CacheBackendSuite) TestAttrMissRecordsViaInjectedRecorder() {
	rec := metricsmocks.NewMockRecorder(s.T())
	rec.EXPECT().CacheMissInc("attr").Once()
	rec.EXPECT().CacheHitInc(mock.Anything, mock.Anything).Maybe()
	// build cache with rec injected (see NewCachedBackend new signature) and
	// drive an uncached Stat through a fake inner that returns an Attr.
	// ... (mirror existing cache test fixtures; Subscribe disabled)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -v -run TestCacheBackendSuite/TestAttrMissRecordsViaInjectedRecorder ./pkg/client/cache/...`
Expected: FAIL — signature mismatch / `metrics.CacheMiss` still global.

- [ ] **Step 3: Add the `rec` field + param and replace cache call sites**

In `pkg/client/cache/backend.go`: add `rec metrics.Recorder` to the `cachedBackend` struct; add the trailing `rec metrics.Recorder` param to `NewCachedBackend`; store it; pass it into the `store`/`data`/`subscriber` constructors (add a `rec` field to those sub-structs). Then replace, verbatim by the inventory:

| file:line | from | to |
|---|---|---|
| `cache/store.go:111` | `metrics.CacheHit("memory", s.cacheType)` | `s.rec.CacheHitInc("memory", s.cacheType)` |
| `cache/store.go:140` | `metrics.CacheHit("disk", s.cacheType)` | `s.rec.CacheHitInc("disk", s.cacheType)` |
| `cache/store.go:179` | `metrics.CachePersistDropped()` | `s.rec.CachePersistDroppedInc()` |
| `cache/backend.go:175` | `metrics.CacheRevalidation("error")` | `b.rec.CacheRevalidationInc("error")` |
| `cache/backend.go:180` | `metrics.CacheRevalidation("not_modified")` | `b.rec.CacheRevalidationInc("not_modified")` |
| `cache/backend.go:189` | `metrics.CacheRevalidation("enoent")` | `b.rec.CacheRevalidationInc("enoent")` |
| `cache/backend.go:192` | `metrics.CacheRevalidation("changed")` | `b.rec.CacheRevalidationInc("changed")` |
| `cache/backend.go:258` | `metrics.CacheMiss("attr")` | `b.rec.CacheMissInc("attr")` |
| `cache/backend.go:277` | `metrics.CacheMiss("attr")` | `b.rec.CacheMissInc("attr")` |
| `cache/backend.go:328` | `metrics.CacheMiss("dir")` | `b.rec.CacheMissInc("dir")` |
| `cache/backend.go:456` | `metrics.CacheMiss("data")` | `b.rec.CacheMissInc("data")` |
| `cache/data.go:242` | `metrics.CacheDedupeHit()` | `d.rec.CacheDedupeHitInc()` |
| `cache/subscriber.go:69` | `metrics.SubscribeStreamStateChanged(false)` | `c.rec.SubscribeStreamStateSet(false)` |
| `cache/subscriber.go:82` | `metrics.SubscribeStreamStateChanged(false)` | `c.rec.SubscribeStreamStateSet(false)` |
| `cache/subscriber.go:120` | `metrics.CacheUnverifiedElapsed(...)` | `c.rec.CacheUnverifiedAdd(...)` |
| `cache/subscriber.go:123` | `metrics.SubscribeStreamStateChanged(true)` | `c.rec.SubscribeStreamStateSet(true)` |
| `cache/subscriber.go:156` | `metrics.SubscribeEventReceived("mutated")` | `c.rec.SubscribeEventReceivedInc("mutated")` |
| `cache/subscriber.go:159` | `metrics.SubscribeEventReceived("deleted")` | `c.rec.SubscribeEventReceivedInc("deleted")` |
| `cache/subscriber.go:167` | `metrics.SubscribeEventReceived("renamed")` | `c.rec.SubscribeEventReceivedInc("renamed")` |
| `cache/subscriber.go:174` | `metrics.SubscribeEventReceived("heartbeat")` | `c.rec.SubscribeEventReceivedInc("heartbeat")` |

Update `mount/single.go` to pass the recorder:
```go
		rec := client.Metrics() // *metrics.Metrics satisfies metrics.Recorder; may be nil
		layers = append(layers, backendLayer{pos: posCache, build: func(inner io.FileSystemBackend) io.FileSystemBackend {
			return cache.NewCachedBackend(inner, cacheCfg, p, client.Fs(), volume, rec)
		}})
```
If `rec` can be nil (no metrics), have `NewCachedBackend` substitute a `metrics.NopRecorder{}` (add a no-op `NopRecorder` implementing `Recorder` in `recorder.go`) so call sites never nil-check.

- [ ] **Step 4: Migrate the retry path**

In `pkg/client/io/retry.go:123`, replace `metrics.OnRetry(op, status.Code(err).String())` with emission via the client the retry already holds:
```go
		if m := client.Metrics(); m != nil {
			m.RetryInc(op, status.Code(err).String())
		}
```
(`retryOp` already receives `client`; `client.Metrics()` returns `*metrics.Metrics`. No new threading.)

- [ ] **Step 5: Delete the global dispatcher**

In `pkg/client/metrics/metrics.go`, delete: `instancesMu`, `instances`, `instanceEntry`, `sameCollectors`, `RegisterInstance`, `UnregisterInstance`, and the package-level fan-out funcs (`OnRetry`, `CacheHit`, `CacheMiss`, `CacheDedupeHit`, `CachePersistDropped`, `CacheRevalidation`, `SubscribeEventReceived`, `SubscribeStreamStateChanged`, `CacheUnverifiedElapsed`). Keep the per-instance `*Metrics` methods (they're the `Recorder` impl). In `pkg/client/grpc/factory.go` remove the `metrics.RegisterInstance(m)` line; in `pkg/client/grpc/client.go` remove the `metrics.UnregisterInstance(c.metrics)` block in `Close`.

- [ ] **Step 6: Build, run the union of touched packages, lint**

Run:
```bash
go build ./...
go test -v ./pkg/client/cache/... ./pkg/client/io/... ./pkg/client/mount/... ./pkg/client/grpc/... ./pkg/client/metrics/...
task lint
```
Expected: PASS; no references to the deleted global funcs remain (`go build` fails loudly if any were missed — that is the safety net).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor(client/metrics): inject Recorder per-client; delete the global dispatcher

Cache, the subscriber, the persist store and the retry path now emit through an
injected metrics.Recorder instead of package-global fan-out. Removes the
instances slice + RegisterInstance/UnregisterInstance/sameCollectors that
existed only to dodge the io->grpc->metrics import cycle. Same Prometheus
series; one clean mechanism. Sets up OTel/audit as drop-in Recorder impls."
```

---

## Self-Review notes (for the executor)

- **Spec coverage:** Task 1 = PassthroughBackend (#move passthrough); Task 2 = written contract (#9) + retry-ownership invariant (documented) + semantic-no-embed guard (#8 reflection half); Task 4 = constrained composition (#1) + layer order (#2); Task 3 = MountParams (#5, no new wire); Tasks 5–6 = metrics observer layer + Recorder (#6/#7 stage 1); Task 7 = migration + dispatcher deletion (#7 stage 2). Conformance-suite (#8 behavioral half): the PassthroughBackend forwarding test + the `var _ FileSystemBackend` assertions are the executable contract; extending it to run against the cache/transport over fakes is a follow-up (note in PR). **Deferred by design (NOT in this plan):** writeBatcher extraction (#3), invalidation-up-the-chain routing (#4).
- **`proto.FsError.String()`**: confirm the generated enum exposes `String()`; if the type is an alias without it, use `proto.FsError_name[int32(st)]` for the `code` label.
- **`.mockery.yml`**: only Task 6 adds an interface (`Recorder`) — regenerate once; do not hand-edit mocks.
- **Behavior preservation**: every Prometheus series name is unchanged; the only new series is `gmountie_client_op_seconds`. No proto/server changes.
