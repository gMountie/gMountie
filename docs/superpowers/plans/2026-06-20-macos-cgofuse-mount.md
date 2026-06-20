# macOS mount via cgofuse — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a cgofuse-based filesystem adapter so gMountie mounts on macOS (macFUSE or FUSE-T) without the Linux-only go-fuse path, reusing the existing `FileSystemBackend` seam unchanged.

**Architecture:** A new `pkg/client/io/cgofs` package implements cgofuse's `FileSystemInterface` by delegating to the existing `io.FileSystemBackend`, as a sibling of the go-fuse `gMountieNode`. A build-tagged mounter selects go-fuse on Linux and cgofuse on darwin (or on Linux with `-tags cgofuse` for benchmarking). Linux production builds stay `CGO_ENABLED=0` on go-fuse.

**Tech Stack:** Go, `github.com/winfsp/cgofuse/fuse`, `github.com/hanwen/go-fuse/v2` (existing), gRPC, testify suites, GitHub Actions.

## Global Constraints

- Module path: `go.gmountie.dev/gmountie`. go-fuse pinned at `github.com/hanwen/go-fuse/v2 v2.10.1`.
- **Linux production build stays `CGO_ENABLED=0` on go-fuse — unchanged.** The cgofuse adapter and cgofuse mounter must be excluded from the default Linux build via build tags.
- **No AI attribution** in commits or PR bodies (conventional-commit subject + short body).
- Tests are written as methods on a **testify suite**, never standalone `func TestX`.
- Config defaults live in Go constructors, **not** `viper.SetDefault`.
- cgofuse's high-level `FileSystemInterface` has **no lock callbacks** — byte-range locks are NOT forwarded on the cgofuse path (documented limitation; go-fuse path keeps locking).
- Build-tag scheme (used throughout):
  - Files with **no cgofuse import** (pure Go, testable anywhere, `CGO_ENABLED=0`): no build tag.
  - Files that **import `cgofuse/fuse`** (require cgo + libfuse/macFUSE/FUSE-T headers): `//go:build darwin || cgofuse`.
  - Mounter selection: go-fuse file = `//go:build !darwin && !cgofuse`; cgofuse file = `//go:build darwin || cgofuse`.

---

### Task 1: Caller-context injection seam

cgofuse ops have no `context.Context` and don't run under a go-fuse context, so `callerFromCtx` (which reads `fuse.FromContext(ctx)`) would fall back to a zero Caller → the server would run every op as root. Add an additive context key so the cgofuse adapter can stamp the kernel caller it gets from `fuse.Getcontext()`. The go-fuse path is unchanged.

**Files:**
- Modify: `pkg/client/io/backend_grpc.go:110-118` (extend `callerFromCtx`; add `WithCaller` + key)
- Test: `pkg/client/io/caller_ctx_test.go` (create)

**Interfaces:**
- Produces: `func WithCaller(ctx context.Context, uid, gid, pid uint32) context.Context` (in package `io`) — attaches a caller the cgofuse adapter reads.
- Produces: `callerFromCtx(ctx)` now also honors the `WithCaller` value when no go-fuse context is present.

- [ ] **Step 1: Write the failing test**

```go
// pkg/client/io/caller_ctx_test.go
package io

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CallerCtxSuite struct{ suite.Suite }

func TestCallerCtxSuite(t *testing.T) { suite.Run(t, new(CallerCtxSuite)) }

func (s *CallerCtxSuite) TestWithCallerIsReadByCallerFromCtx() {
	ctx := WithCaller(context.Background(), 501, 20, 4242)
	c := callerFromCtx(ctx)
	s.Require().NotNil(c.Owner)
	s.Equal(uint32(501), c.Owner.Uid)
	s.Equal(uint32(20), c.Owner.Gid)
	s.Equal(uint32(4242), c.Pid)
}

func (s *CallerCtxSuite) TestBareContextFallsBackToZeroCaller() {
	c := callerFromCtx(context.Background())
	s.Require().NotNil(c.Owner)
	s.Equal(uint32(0), c.Owner.Uid)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/client/io/ -run TestCallerCtxSuite -v`
Expected: FAIL — `undefined: WithCaller`.

- [ ] **Step 3: Implement the seam**

In `pkg/client/io/backend_grpc.go`, add near `callerFromCtx`:

```go
// callerCtxKey carries a kernel caller for backends that don't run under a
// go-fuse context. The Linux go-fuse path populates ctx via fuse.FromContext;
// the cgofuse/macOS adapter has no such context, so it stamps the caller it
// reads from fuse.Getcontext() under this key instead.
type callerCtxKey struct{}

// WithCaller attaches the kernel-reported caller (uid/gid/pid) to ctx for the
// cgofuse adapter. callerFromCtx reads it when no go-fuse context is present.
func WithCaller(ctx context.Context, uid, gid, pid uint32) context.Context {
	return context.WithValue(ctx, callerCtxKey{}, &proto.Caller{
		Owner: &proto.Owner{Uid: uid, Gid: gid},
		Pid:   pid,
	})
}
```

Replace the body of `callerFromCtx` with:

```go
func callerFromCtx(ctx context.Context) *proto.Caller {
	if c, ok := fuse.FromContext(ctx); ok {
		return &proto.Caller{
			Owner: &proto.Owner{Uid: c.Uid, Gid: c.Gid},
			Pid:   c.Pid,
		}
	}
	if c, ok := ctx.Value(callerCtxKey{}).(*proto.Caller); ok {
		return c
	}
	return &proto.Caller{Owner: &proto.Owner{}}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/client/io/ -run TestCallerCtxSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/caller_ctx_test.go
git commit -m "io: add WithCaller context seam for non-go-fuse backends

The cgofuse adapter has no go-fuse context to carry the kernel caller.
Add an additive context key so it can stamp uid/gid/pid from
fuse.Getcontext(); callerFromCtx now honors it. go-fuse path unchanged."
```

---

### Task 2: Status mapping (`fuse.Status` → cgofuse errno)

cgofuse ops return a negative errno `int`. `FileSystemBackend` returns `fuse.Status` (a `uint32` errno). Map between them. Pure Go, no cgofuse import — testable anywhere.

**Files:**
- Create: `pkg/client/io/cgofs/status.go`
- Test: `pkg/client/io/cgofs/status_test.go`

**Interfaces:**
- Produces: `func errc(st fuse.Status) int` — returns `0` for `fuse.OK`, else `-int(st)` (negative errno per cgofuse convention).

- [ ] **Step 1: Write the failing test**

```go
// pkg/client/io/cgofs/status_test.go
package cgofs

import (
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type StatusSuite struct{ suite.Suite }

func TestStatusSuite(t *testing.T) { suite.Run(t, new(StatusSuite)) }

func (s *StatusSuite) TestOKMapsToZero() {
	s.Equal(0, errc(fuse.OK))
}

func (s *StatusSuite) TestErrnoMapsToNegative() {
	s.Equal(-int(fuse.ENOENT), errc(fuse.ENOENT))
	s.Equal(-int(fuse.EACCES), errc(fuse.EACCES))
	s.Equal(-int(fuse.EIO), errc(fuse.EIO))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/client/io/cgofs/ -run TestStatusSuite -v`
Expected: FAIL — `undefined: errc` (or package does not exist).

- [ ] **Step 3: Implement**

```go
// pkg/client/io/cgofs/status.go

// Package cgofs adapts the gMountie io.FileSystemBackend to cgofuse's
// FileSystemInterface, so macOS can mount via macFUSE/FUSE-T (and Windows via
// WinFsp later) without the Linux-only go-fuse path. Files that import
// cgofuse/fuse are build-tagged "darwin || cgofuse"; status mapping and the
// handle table are pure Go and build everywhere.
package cgofs

import "github.com/hanwen/go-fuse/v2/fuse"

// errc converts a FileSystemBackend fuse.Status into cgofuse's return
// convention: 0 for success, negative errno otherwise.
func errc(st fuse.Status) int {
	if st == fuse.OK {
		return 0
	}
	return -int(st)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/client/io/cgofs/ -run TestStatusSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/status.go pkg/client/io/cgofs/status_test.go
git commit -m "cgofs: add fuse.Status -> cgofuse errno mapping"
```

---

### Task 3: Open-file handle table

cgofuse identifies open files by `uint64`; `FileSystemBackend` uses `io.FileHandle` objects. The adapter owns a mutex-guarded table mapping `uint64 → io.FileHandle`. Pure Go (depends only on the `io` package types), testable anywhere.

**Files:**
- Create: `pkg/client/io/cgofs/handles.go`
- Test: `pkg/client/io/cgofs/handles_test.go`

**Interfaces:**
- Produces: `type handleTable struct{ ... }`; `func newHandleTable() *handleTable`; `(*handleTable) add(io.FileHandle) uint64`; `(*handleTable) get(uint64) (io.FileHandle, bool)`; `(*handleTable) remove(uint64) (io.FileHandle, bool)`.

- [ ] **Step 1: Write the failing test**

```go
// pkg/client/io/cgofs/handles_test.go
package cgofs

import (
	"sync"
	"testing"

	gio "go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

// stubHandle is a minimal io.FileHandle for table tests.
type stubHandle struct{ p string }

func (h *stubHandle) Path() string            { return h.p }
func (h *stubHandle) Unwrap() gio.FileHandle  { return h }

type HandleTableSuite struct{ suite.Suite }

func TestHandleTableSuite(t *testing.T) { suite.Run(t, new(HandleTableSuite)) }

func (s *HandleTableSuite) TestAddGetRemove() {
	tbl := newHandleTable()
	h := &stubHandle{p: "a/b"}
	id := tbl.add(h)
	got, ok := tbl.get(id)
	s.True(ok)
	s.Equal(h, got)
	removed, ok := tbl.remove(id)
	s.True(ok)
	s.Equal(h, removed)
	_, ok = tbl.get(id)
	s.False(ok)
}

func (s *HandleTableSuite) TestIDsAreUniqueAndConcurrent() {
	tbl := newHandleTable()
	const n = 200
	ids := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); ids <- tbl.add(&stubHandle{}) }()
	}
	wg.Wait()
	close(ids)
	seen := map[uint64]bool{}
	for id := range ids {
		s.False(seen[id], "duplicate id %d", id)
		seen[id] = true
	}
	s.Len(seen, n)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/client/io/cgofs/ -run TestHandleTableSuite -v`
Expected: FAIL — `undefined: newHandleTable`.

- [ ] **Step 3: Implement**

```go
// pkg/client/io/cgofs/handles.go
package cgofs

import (
	"sync"

	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// handleTable maps cgofuse's uint64 file handles to io.FileHandle objects.
// cgofuse hands back a uint64 per open; go-fuse gave us an object, so the
// cgofuse adapter owns this mapping itself. Safe for concurrent use.
type handleTable struct {
	mu   sync.Mutex
	next uint64
	m    map[uint64]gio.FileHandle
}

func newHandleTable() *handleTable {
	return &handleTable{next: 1, m: make(map[uint64]gio.FileHandle)}
}

// add stores fh and returns a fresh non-zero handle id.
func (t *handleTable) add(fh gio.FileHandle) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	id := t.next
	t.next++
	t.m[id] = fh
	return id
}

func (t *handleTable) get(id uint64) (gio.FileHandle, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fh, ok := t.m[id]
	return fh, ok
}

func (t *handleTable) remove(id uint64) (gio.FileHandle, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fh, ok := t.m[id]
	if ok {
		delete(t.m, id)
	}
	return fh, ok
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/client/io/cgofs/ -run TestHandleTableSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/handles.go pkg/client/io/cgofs/handles_test.go
git commit -m "cgofs: add concurrent open-file handle table"
```

---

### Task 4: Attr ↔ `Stat_t` conversion with IDRewriter

Convert `io.Attr` → cgofuse `Stat_t` (applying `IDRewriter.Inbound` for display, mirroring `setAttrFromBackend` in node.go), and `io.StatFs` → `Statfs_t`. This file imports `cgofuse/fuse`, so it is build-tagged and its tests run in the libfuse CI lane (build needs libfuse; no mount needed).

**Files:**
- Create: `pkg/client/io/cgofs/attr.go` (`//go:build darwin || cgofuse`)
- Test: `pkg/client/io/cgofs/attr_test.go` (`//go:build darwin || cgofuse`)

**Interfaces:**
- Consumes: `gio.Attr`, `gio.StatFs`, `gio.IDRewriter` (nil-safe `Inbound`).
- Produces: `func fillStat(dst *fuse.Stat_t, a *gio.Attr, rw *gio.IDRewriter)`; `func fillStatfs(dst *fuse.Statfs_t, s *gio.StatFs)`.

- [ ] **Step 1: Write the failing test**

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/attr_test.go
package cgofs

import (
	"testing"

	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

type AttrSuite struct{ suite.Suite }

func TestAttrSuite(t *testing.T) { suite.Run(t, new(AttrSuite)) }

func (s *AttrSuite) TestFillStatCopiesFieldsAndTimes() {
	a := &gio.Attr{
		Ino: 7, Size: 1024, Blocks: 2, Mode: 0o100644, Nlink: 1,
		Uid: 1000, Gid: 1000, Rdev: 0, Blksize: 4096,
		Atime: 100, Atimensec: 5, Mtime: 200, Mtimensec: 6, Ctime: 300, Ctimensec: 7,
	}
	var st cgofuse.Stat_t
	fillStat(&st, a, nil) // nil rewriter = identity
	s.Equal(uint64(7), st.Ino)
	s.Equal(int64(1024), st.Size)
	s.Equal(uint32(0o100644), st.Mode)
	s.Equal(uint32(1000), st.Uid)
	s.Equal(int64(100), st.Atim.Sec)
	s.Equal(int64(5), st.Atim.Nsec)
	s.Equal(int64(200), st.Mtim.Sec)
}

func (s *AttrSuite) TestFillStatAppliesRewriter() {
	// Server identity uid=1000 maps to local uid=501.
	rw := gio.NewIDRewriter(&gio.Identity{Uid: 1000, Gid: 1000}, 501, 20)
	a := &gio.Attr{Mode: 0o100644, Uid: 1000, Gid: 1000}
	var st cgofuse.Stat_t
	fillStat(&st, a, rw)
	s.Equal(uint32(501), st.Uid)
	s.Equal(uint32(20), st.Gid)
}

func (s *AttrSuite) TestFillStatfs() {
	in := &gio.StatFs{Blocks: 10, Bfree: 4, Bavail: 3, Files: 100, Ffree: 50, Bsize: 4096, Namelen: 255, Frsize: 4096}
	var out cgofuse.Statfs_t
	fillStatfs(&out, in)
	s.Equal(uint64(10), out.Blocks)
	s.Equal(uint64(4096), out.Bsize)
	s.Equal(uint64(255), out.Namemax)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestAttrSuite -v`
Expected: FAIL — `undefined: fillStat`. (If it fails to build with a missing `fuse.h`, install libfuse first: `sudo apt-get install -y libfuse-dev`.)

- [ ] **Step 3: Implement**

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/attr.go
package cgofs

import (
	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// fillStat maps an io.Attr to a cgofuse Stat_t, applying the mount's
// IDRewriter so the caller sees local display uid/gid (mirrors
// setAttrFromBackend in node.go). rw may be nil (identity transform).
func fillStat(dst *cgofuse.Stat_t, a *gio.Attr, rw *gio.IDRewriter) {
	dst.Ino = a.Ino
	dst.Size = int64(a.Size)
	dst.Blocks = int64(a.Blocks)
	dst.Mode = a.Mode
	dst.Nlink = a.Nlink
	dst.Rdev = uint64(a.Rdev)
	dst.Blksize = int64(a.Blksize)
	dst.Atim = cgofuse.Timespec{Sec: int64(a.Atime), Nsec: int64(a.Atimensec)}
	dst.Mtim = cgofuse.Timespec{Sec: int64(a.Mtime), Nsec: int64(a.Mtimensec)}
	dst.Ctim = cgofuse.Timespec{Sec: int64(a.Ctime), Nsec: int64(a.Ctimensec)}
	dst.Uid, dst.Gid = rw.Inbound(a.Uid, a.Gid)
}

// fillStatfs maps an io.StatFs to a cgofuse Statfs_t.
func fillStatfs(dst *cgofuse.Statfs_t, s *gio.StatFs) {
	dst.Bsize = uint64(s.Bsize)
	dst.Frsize = uint64(s.Frsize)
	dst.Blocks = s.Blocks
	dst.Bfree = s.Bfree
	dst.Bavail = s.Bavail
	dst.Files = s.Files
	dst.Ffree = s.Ffree
	dst.Namemax = uint64(s.Namelen)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestAttrSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/attr.go pkg/client/io/cgofs/attr_test.go
git commit -m "cgofs: io.Attr/StatFs -> cgofuse Stat_t/Statfs_t with IDRewriter"
```

---

### Task 5: Adapter scaffold + metadata ops

The `MountieCgoFS` struct embeds cgofuse's `FileSystemBase`, holds the backend + rewriter + handle table, and builds a per-op context carrying the caller from `fuse.Getcontext()`. Implement the read-only metadata ops in this task. Introduce a `fakeBackend` test double (reused by Tasks 6–8). All adapter-method tests call methods directly — **no real mount** — so they build with libfuse but don't need to mount.

**Files:**
- Create: `pkg/client/io/cgofs/fs.go` (`//go:build darwin || cgofuse`)
- Create: `pkg/client/io/cgofs/fakebackend_test.go` (`//go:build darwin || cgofuse`)
- Test: `pkg/client/io/cgofs/fs_meta_test.go` (`//go:build darwin || cgofuse`)

**Interfaces:**
- Consumes: `gio.FileSystemBackend`, `gio.IDRewriter`, `gio.WithCaller`, `errc`, `newHandleTable`, `fillStat`, `fillStatfs`.
- Produces: `type MountieCgoFS struct{ ... }`; `func New(backend gio.FileSystemBackend, rw *gio.IDRewriter, metaTimeout time.Duration) *MountieCgoFS`; methods `Getattr`, `Statfs`, `Access`, `Readdir`, `Readlink`, `Opendir`, `Releasedir`. Later tasks add I/O, mutations, xattrs.
- Produces: `func (fs *MountieCgoFS) opCtx() (context.Context, context.CancelFunc)` — context with caller + meta timeout.

- [ ] **Step 1: Write the fake backend and failing metadata test**

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/fakebackend_test.go
package cgofs

import (
	"context"

	"github.com/hanwen/go-fuse/v2/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// fakeBackend is a programmable io.FileSystemBackend test double. Each field
// is the canned response for the matching op; calls are recorded for asserts.
// Reused by Tasks 5–8.
type fakeBackend struct {
	statAttr *gio.Attr
	statSt   fuse.Status
	statPath string

	listEntries []gio.DirEntryPlus
	listSt      fuse.Status

	statfs   *gio.StatFs
	statfsSt fuse.Status

	readlink   string
	readlinkSt fuse.Status

	lastCallerUID uint32
}

func (f *fakeBackend) recordCaller(ctx context.Context) {
	// callerFromCtx is unexported in io; instead read the documented seam.
	// Tests assert the adapter passed a non-background ctx by checking the
	// uid the adapter injected via gio.WithCaller (see opCtx). We re-extract
	// using the same public API path the backend would.
}

func (f *fakeBackend) Stat(ctx context.Context, path string) (*gio.Attr, fuse.Status) {
	f.statPath = path
	return f.statAttr, f.statSt
}
func (f *fakeBackend) GetAttrIfChanged(ctx context.Context, path string, v uint64) (*gio.Attr, bool, fuse.Status) {
	return nil, false, fuse.EIO
}
func (f *fakeBackend) Lookup(ctx context.Context, parent, name string) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) ListDir(ctx context.Context, path string) ([]gio.DirEntryPlus, fuse.Status) {
	return f.listEntries, f.listSt
}
func (f *fakeBackend) Access(ctx context.Context, path string, mode uint32) fuse.Status { return fuse.OK }
func (f *fakeBackend) StatFs(ctx context.Context, path string) (*gio.StatFs, fuse.Status) {
	return f.statfs, f.statfsSt
}
func (f *fakeBackend) GetXAttr(ctx context.Context, path, attr string) ([]byte, fuse.Status) {
	return nil, fuse.ENOATTR
}
func (f *fakeBackend) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) RemoveXAttr(ctx context.Context, path, attr string) fuse.Status { return fuse.OK }
func (f *fakeBackend) ListXAttr(ctx context.Context, path string) ([]string, fuse.Status) {
	return nil, fuse.OK
}
func (f *fakeBackend) Open(ctx context.Context, path string, flags uint32) (gio.FileHandle, fuse.Status) {
	return nil, fuse.OK
}
func (f *fakeBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (gio.FileHandle, *gio.Attr, fuse.Status) {
	return nil, nil, fuse.OK
}
func (f *fakeBackend) Read(ctx context.Context, fh gio.FileHandle, off int64, dest []byte) (int, fuse.Status) {
	return 0, fuse.OK
}
func (f *fakeBackend) Write(ctx context.Context, fh gio.FileHandle, off int64, data []byte) (uint32, fuse.Status) {
	return 0, fuse.OK
}
func (f *fakeBackend) Release(ctx context.Context, fh gio.FileHandle) fuse.Status { return fuse.OK }
func (f *fakeBackend) Flush(ctx context.Context, fh gio.FileHandle) fuse.Status   { return fuse.OK }
func (f *fakeBackend) Fsync(ctx context.Context, fh gio.FileHandle, flags int64) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) Allocate(ctx context.Context, fh gio.FileHandle, off, size uint64, mode uint32) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) GetLk(ctx context.Context, fh gio.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) fuse.Status {
	return fuse.ENOSYS
}
func (f *fakeBackend) SetLk(ctx context.Context, fh gio.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return fuse.ENOSYS
}
func (f *fakeBackend) SetLkw(ctx context.Context, fh gio.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) fuse.Status {
	return fuse.ENOSYS
}
func (f *fakeBackend) CopyFileRange(ctx context.Context, in gio.FileHandle, io1 uint64, out gio.FileHandle, oo uint64, length, flags uint64) (uint64, fuse.Status) {
	return 0, fuse.ENOSYS
}
func (f *fakeBackend) Lseek(ctx context.Context, fh gio.FileHandle, offset uint64, whence uint32) (uint64, fuse.Status) {
	return 0, fuse.ENOSYS
}
func (f *fakeBackend) Mkdir(ctx context.Context, path string, mode uint32) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Rmdir(ctx context.Context, path string) fuse.Status  { return fuse.OK }
func (f *fakeBackend) Unlink(ctx context.Context, path string) fuse.Status { return fuse.OK }
func (f *fakeBackend) Rename(ctx context.Context, oldPath, newPath string) fuse.Status {
	return fuse.OK
}
func (f *fakeBackend) Readlink(ctx context.Context, path string) (string, fuse.Status) {
	return f.readlink, f.readlinkSt
}
func (f *fakeBackend) Symlink(ctx context.Context, target, linkPath string) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) SetAttr(ctx context.Context, path string, in gio.SetAttrIn) (*gio.Attr, fuse.Status) {
	return f.statAttr, f.statSt
}
func (f *fakeBackend) Close() error { return nil }
```

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/fs_meta_test.go
package cgofs

import (
	"testing"
	"time"

	cgofuse "github.com/winfsp/cgofuse/fuse"
	"github.com/hanwen/go-fuse/v2/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

type MetaSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestMetaSuite(t *testing.T) { suite.Run(t, new(MetaSuite)) }

func (s *MetaSuite) SetupTest() {
	s.be = &fakeBackend{}
	s.fs = New(s.be, nil, time.Second)
}

func (s *MetaSuite) TestGetattrOK() {
	s.be.statAttr = &gio.Attr{Ino: 9, Size: 42, Mode: 0o100644}
	s.be.statSt = fuse.OK
	var st cgofuse.Stat_t
	rc := s.fs.Getattr("dir/file", &st, ^uint64(0))
	s.Equal(0, rc)
	s.Equal(uint64(9), st.Ino)
	s.Equal(int64(42), st.Size)
	s.Equal("dir/file", s.be.statPath) // leading slash stripped
}

func (s *MetaSuite) TestGetattrENOENT() {
	s.be.statSt = fuse.ENOENT
	var st cgofuse.Stat_t
	rc := s.fs.Getattr("/missing", &st, ^uint64(0))
	s.Equal(-int(fuse.ENOENT), rc)
}

func (s *MetaSuite) TestReaddirFillsEntries() {
	s.be.listSt = fuse.OK
	s.be.listEntries = []gio.DirEntryPlus{
		{DirEntry: gio.DirEntry{Name: "a", Ino: 1, Mode: 0o100644}},
		{DirEntry: gio.DirEntry{Name: "b", Ino: 2, Mode: fuse.S_IFDIR | 0o755}},
	}
	var names []string
	fill := func(name string, stat *cgofuse.Stat_t, ofst int64) bool {
		names = append(names, name)
		return true
	}
	rc := s.fs.Readdir("/", fill, 0, 0)
	s.Equal(0, rc)
	s.Equal([]string{".", "..", "a", "b"}, names)
}

func (s *MetaSuite) TestReadlink() {
	s.be.readlink = "target/path"
	s.be.readlinkSt = fuse.OK
	rc, target := s.fs.Readlink("/link")
	s.Equal(0, rc)
	s.Equal("target/path", target)
}

func (s *MetaSuite) TestStatfs() {
	s.be.statfs = &gio.StatFs{Blocks: 10, Bsize: 4096, Namelen: 255}
	s.be.statfsSt = fuse.OK
	var out cgofuse.Statfs_t
	rc := s.fs.Statfs("/", &out)
	s.Equal(0, rc)
	s.Equal(uint64(10), out.Blocks)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestMetaSuite -v`
Expected: FAIL — `undefined: New` / `MountieCgoFS`.

- [ ] **Step 3: Implement scaffold + metadata ops**

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/fs.go
package cgofs

import (
	"context"
	"strings"
	"time"

	cgofuse "github.com/winfsp/cgofuse/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
)

// MountieCgoFS adapts io.FileSystemBackend to cgofuse's FileSystemInterface.
// It is the macOS/Windows sibling of the go-fuse gMountieNode and delegates
// every op to the same backend. backend + rewriter are set at construction
// and shared for the mount's lifetime; the handle table maps cgofuse's uint64
// handles to io.FileHandle objects.
type MountieCgoFS struct {
	cgofuse.FileSystemBase
	backend     gio.FileSystemBackend
	rewriter    *gio.IDRewriter
	handles     *handleTable
	metaTimeout time.Duration
}

// New builds an adapter over backend. rw may be nil (raw_ids / no rewrite).
// metaTimeout bounds metadata ops (mirrors the client MetaTimeout).
func New(backend gio.FileSystemBackend, rw *gio.IDRewriter, metaTimeout time.Duration) *MountieCgoFS {
	return &MountieCgoFS{
		backend:     backend,
		rewriter:    rw,
		handles:     newHandleTable(),
		metaTimeout: metaTimeout,
	}
}

// clean normalizes a cgofuse path (always absolute, leading "/") to the
// backend's path convention (relative to mount root, no leading slash). The
// root "/" becomes "".
func clean(p string) string { return strings.TrimPrefix(p, "/") }

// opCtx builds a per-op context carrying the kernel caller (uid/gid/pid from
// cgofuse) so the gRPC backend stamps proto.Caller correctly, with a timeout.
func (fs *MountieCgoFS) opCtx() (context.Context, context.CancelFunc) {
	uid, gid, pid := cgofuse.Getcontext()
	ctx := gio.WithCaller(context.Background(), uid, gid, uint32(pid))
	return context.WithTimeout(ctx, fs.metaTimeout)
}

func (fs *MountieCgoFS) Getattr(path string, stat *cgofuse.Stat_t, fh uint64) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	a, st := fs.backend.Stat(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	fillStat(stat, a, fs.rewriter)
	return 0
}

func (fs *MountieCgoFS) Readdir(path string, fill func(name string, stat *cgofuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	entries, st := fs.backend.ListDir(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	// "." and ".." are not returned by the backend; cgofuse expects them.
	fill(".", nil, 0)
	fill("..", nil, 0)
	for _, e := range entries {
		var stat cgofuse.Stat_t
		stat.Mode = e.Mode
		stat.Ino = e.Ino
		if !fill(e.Name, &stat, 0) {
			break
		}
	}
	return 0
}

func (fs *MountieCgoFS) Readlink(path string) (int, string) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	target, st := fs.backend.Readlink(ctx, clean(path))
	if !st.Ok() {
		return errc(st), ""
	}
	return 0, target
}

func (fs *MountieCgoFS) Access(path string, mask uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Access(ctx, clean(path), mask))
}

func (fs *MountieCgoFS) Statfs(path string, stat *cgofuse.Statfs_t) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	s, st := fs.backend.StatFs(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	fillStatfs(stat, s)
	return 0
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestMetaSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/fs.go pkg/client/io/cgofs/fakebackend_test.go pkg/client/io/cgofs/fs_meta_test.go
git commit -m "cgofs: adapter scaffold + metadata ops (getattr/readdir/readlink/access/statfs)"
```

---

### Task 6: Adapter file I/O ops

Implement `Open`, `Create`, `Read`, `Write`, `Flush`, `Fsync`, `Release`, `Truncate` — the handle-table-backed ops. `Open`/`Create` allocate a handle id; `Read`/`Write`/`Flush`/`Fsync`/`Release` resolve it; `Release` removes it. `Truncate` maps to `SetAttr` with the SIZE bit.

**Files:**
- Modify: `pkg/client/io/cgofs/fs.go`
- Test: `pkg/client/io/cgofs/fs_io_test.go` (`//go:build darwin || cgofuse`)

**Interfaces:**
- Consumes: `gio.SetAttrIn`, `gio.FileHandle`, the handle table, `fuse` valid-bit constants (`fuse.FATTR_SIZE`).
- Produces: methods `Open`, `Create`, `Read`, `Write`, `Flush`, `Fsync`, `Release`, `Truncate` on `*MountieCgoFS`.

- [ ] **Step 1: Write the failing test** (extend `fakeBackend` with recording fields, then assert)

Add to `pkg/client/io/cgofs/fakebackend_test.go` these recording fields and overrides (replace the stub `Open`/`Create`/`Read`/`Write`/`Release`):

```go
// --- append to fakeBackend struct literal fields ---
//   openFH gio.FileHandle
//   openSt fuse.Status
//   createFH gio.FileHandle
//   createAttr *gio.Attr
//   createSt fuse.Status
//   readData []byte
//   readSt fuse.Status
//   wroteData []byte
//   writeSt fuse.Status
//   released []string
//   setAttrIn gio.SetAttrIn
```

> Implementer note: add the fields above to the `fakeBackend` struct, then
> replace the five stub methods below. `recHandle` is a tiny io.FileHandle.

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/fs_io_test.go
package cgofs

import (
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

// recHandle is an io.FileHandle that records its path.
type recHandle struct{ p string }

func (h *recHandle) Path() string           { return h.p }
func (h *recHandle) Unwrap() gio.FileHandle { return h }

type IOSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestIOSuite(t *testing.T) { suite.Run(t, new(IOSuite)) }

func (s *IOSuite) SetupTest() {
	s.be = &fakeBackend{}
	s.fs = New(s.be, nil, time.Second)
}

func (s *IOSuite) TestOpenReadRelease() {
	s.be.openFH = &recHandle{p: "f"}
	s.be.openSt = fuse.OK
	rc, fh := s.fs.Open("/f", 0)
	s.Equal(0, rc)
	s.NotZero(fh)

	s.be.readData = []byte("hello")
	s.be.readSt = fuse.OK
	buf := make([]byte, 5)
	n := s.fs.Read("/f", buf, 0, fh)
	s.Equal(5, n)
	s.Equal("hello", string(buf))

	rc = s.fs.Release("/f", fh)
	s.Equal(0, rc)
	// handle no longer resolvable -> EBADF-ish: a subsequent read returns -EBADF
	n = s.fs.Read("/f", buf, 0, fh)
	s.Equal(-int(fuse.EBADF), n)
}

func (s *IOSuite) TestWrite() {
	s.be.openFH = &recHandle{p: "f"}
	s.be.openSt = fuse.OK
	_, fh := s.fs.Open("/f", 0)
	s.be.writeSt = fuse.OK
	n := s.fs.Write("/f", []byte("abc"), 0, fh)
	s.Equal(3, n)
	s.Equal("abc", string(s.be.wroteData))
}

func (s *IOSuite) TestTruncateMapsToSetAttrSize() {
	s.be.statAttr = &gio.Attr{}
	s.be.statSt = fuse.OK
	rc := s.fs.Truncate("/f", 128, ^uint64(0))
	s.Equal(0, rc)
	s.Equal(uint32(fuse.FATTR_SIZE), s.be.setAttrIn.Valid&uint32(fuse.FATTR_SIZE))
	s.Equal(uint64(128), s.be.setAttrIn.Size)
}
```

Then in `fakebackend_test.go` replace those five methods + `SetAttr` to record:

```go
func (f *fakeBackend) Open(ctx context.Context, path string, flags uint32) (gio.FileHandle, fuse.Status) {
	return f.openFH, f.openSt
}
func (f *fakeBackend) Create(ctx context.Context, parent, name string, flags, mode uint32) (gio.FileHandle, *gio.Attr, fuse.Status) {
	return f.createFH, f.createAttr, f.createSt
}
func (f *fakeBackend) Read(ctx context.Context, fh gio.FileHandle, off int64, dest []byte) (int, fuse.Status) {
	n := copy(dest, f.readData)
	return n, f.readSt
}
func (f *fakeBackend) Write(ctx context.Context, fh gio.FileHandle, off int64, data []byte) (uint32, fuse.Status) {
	f.wroteData = append([]byte(nil), data...)
	return uint32(len(data)), f.writeSt
}
func (f *fakeBackend) Release(ctx context.Context, fh gio.FileHandle) fuse.Status {
	f.released = append(f.released, fh.Path())
	return fuse.OK
}
func (f *fakeBackend) SetAttr(ctx context.Context, path string, in gio.SetAttrIn) (*gio.Attr, fuse.Status) {
	f.setAttrIn = in
	return f.statAttr, f.statSt
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestIOSuite -v`
Expected: FAIL — `fs.Open undefined` etc.

- [ ] **Step 3: Implement I/O ops** (append to `fs.go`)

```go
func (fs *MountieCgoFS) Open(path string, flags int) (int, uint64) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	fh, st := fs.backend.Open(ctx, clean(path), uint32(flags))
	if !st.Ok() {
		return errc(st), ^uint64(0)
	}
	return 0, fs.handles.add(fh)
}

func (fs *MountieCgoFS) Create(path string, flags int, mode uint32) (int, uint64) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	parent, name := splitPath(clean(path))
	fh, _, st := fs.backend.Create(ctx, parent, name, uint32(flags), mode)
	if !st.Ok() {
		return errc(st), ^uint64(0)
	}
	return 0, fs.handles.add(fh)
}

func (fs *MountieCgoFS) Read(path string, buff []byte, ofst int64, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	n, st := fs.backend.Read(ctx, h, ofst, buff)
	if !st.Ok() {
		return errc(st)
	}
	return n
}

func (fs *MountieCgoFS) Write(path string, buff []byte, ofst int64, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	n, st := fs.backend.Write(ctx, h, ofst, buff)
	if !st.Ok() {
		return errc(st)
	}
	return int(n)
}

func (fs *MountieCgoFS) Flush(path string, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Flush(ctx, h))
}

func (fs *MountieCgoFS) Fsync(path string, datasync bool, fh uint64) int {
	h, ok := fs.handles.get(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	var flags int64
	if datasync {
		flags = 1
	}
	return errc(fs.backend.Fsync(ctx, h, flags))
}

func (fs *MountieCgoFS) Release(path string, fh uint64) int {
	h, ok := fs.handles.remove(fh)
	if !ok {
		return -int(fuse.EBADF)
	}
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Release(ctx, h))
}

func (fs *MountieCgoFS) Truncate(path string, size int64, fh uint64) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_SIZE), Size: uint64(size)}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}
```

Add the import `"github.com/hanwen/go-fuse/v2/fuse"` to `fs.go`, and this helper:

```go
// splitPath splits a cleaned path into (parent, name) for Create. "f" -> ("","f").
func splitPath(p string) (parent, name string) {
	i := strings.LastIndex(p, "/")
	if i < 0 {
		return "", p
	}
	return p[:i], p[i+1:]
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestIOSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/fs.go pkg/client/io/cgofs/fs_io_test.go pkg/client/io/cgofs/fakebackend_test.go
git commit -m "cgofs: file I/O ops (open/create/read/write/flush/fsync/release/truncate)"
```

---

### Task 7: Adapter mutations + SetAttr-family ops

Implement `Mkdir`, `Rmdir`, `Unlink`, `Rename`, `Symlink`, `Chmod`, `Chown`, `Utimens`. The SetAttr-family (`Chmod`/`Chown`/`Utimens`) build a `gio.SetAttrIn` with the right `FATTR_*` valid bits; `Chown` applies `rewriter.Outbound` (local→server).

**Files:**
- Modify: `pkg/client/io/cgofs/fs.go`
- Test: `pkg/client/io/cgofs/fs_mut_test.go` (`//go:build darwin || cgofuse`)

**Interfaces:**
- Consumes: `gio.SetAttrIn`, `fuse.FATTR_MODE/UID/GID/ATIME/MTIME`, `gio.IDRewriter.Outbound`, cgofuse `Timespec`.
- Produces: methods `Mkdir`, `Rmdir`, `Unlink`, `Rename`, `Symlink`, `Chmod`, `Chown`, `Utimens` on `*MountieCgoFS`.

- [ ] **Step 1: Write the failing test**

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/fs_mut_test.go
package cgofs

import (
	"testing"
	"time"

	cgofuse "github.com/winfsp/cgofuse/fuse"
	"github.com/hanwen/go-fuse/v2/fuse"
	gio "go.gmountie.dev/gmountie/pkg/client/io"
	"github.com/stretchr/testify/suite"
)

type MutSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestMutSuite(t *testing.T) { suite.Run(t, new(MutSuite)) }

func (s *MutSuite) SetupTest() {
	s.be = &fakeBackend{statAttr: &gio.Attr{}, statSt: fuse.OK}
	// rewriter: local uid 501 -> server uid 1000
	rw := gio.NewIDRewriter(&gio.Identity{Uid: 1000, Gid: 1000}, 501, 20)
	s.fs = New(s.be, rw, time.Second)
}

func (s *MutSuite) TestChmodSetsModeBit() {
	rc := s.fs.Chmod("/f", 0o600)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_MODE))
	s.Equal(uint32(0o600), s.be.setAttrIn.Mode)
}

func (s *MutSuite) TestChownAppliesOutboundRewrite() {
	rc := s.fs.Chown("/f", 501, 20)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_UID))
	s.Equal(uint32(1000), s.be.setAttrIn.Uid) // 501 -> 1000 via Outbound
	s.Equal(uint32(1000), s.be.setAttrIn.Gid) // 20 -> 1000 via Outbound
}

func (s *MutSuite) TestUtimensSetsTimes() {
	tmsp := []cgofuse.Timespec{{Sec: 111, Nsec: 0}, {Sec: 222, Nsec: 0}}
	rc := s.fs.Utimens("/f", tmsp)
	s.Equal(0, rc)
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_ATIME))
	s.NotZero(s.be.setAttrIn.Valid & uint32(fuse.FATTR_MTIME))
	s.Require().NotNil(s.be.setAttrIn.Atime)
	s.Equal(int64(111), s.be.setAttrIn.Atime.Unix())
}

func (s *MutSuite) TestRename() {
	rc := s.fs.Rename("/a", "/b")
	s.Equal(0, rc)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestMutSuite -v`
Expected: FAIL — `fs.Chmod undefined` etc.

- [ ] **Step 3: Implement** (append to `fs.go`; add `"time"` import — already present)

```go
func (fs *MountieCgoFS) Mkdir(path string, mode uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	_, st := fs.backend.Mkdir(ctx, clean(path), mode)
	return errc(st)
}

func (fs *MountieCgoFS) Rmdir(path string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Rmdir(ctx, clean(path)))
}

func (fs *MountieCgoFS) Unlink(path string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Unlink(ctx, clean(path)))
}

func (fs *MountieCgoFS) Rename(oldpath string, newpath string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.Rename(ctx, clean(oldpath), clean(newpath)))
}

func (fs *MountieCgoFS) Symlink(target string, newpath string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	_, st := fs.backend.Symlink(ctx, target, clean(newpath))
	return errc(st)
}

func (fs *MountieCgoFS) Chmod(path string, mode uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_MODE), Mode: mode}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}

func (fs *MountieCgoFS) Chown(path string, uid uint32, gid uint32) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	suid, sgid := fs.rewriter.Outbound(uid, gid)
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_UID | fuse.FATTR_GID), Uid: suid, Gid: sgid}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}

func (fs *MountieCgoFS) Utimens(path string, tmsp []cgofuse.Timespec) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	in := gio.SetAttrIn{Valid: uint32(fuse.FATTR_ATIME | fuse.FATTR_MTIME)}
	if len(tmsp) >= 2 {
		at := tmsp[0].Time()
		mt := tmsp[1].Time()
		in.Atime = &at
		in.Mtime = &mt
	}
	_, st := fs.backend.SetAttr(ctx, clean(path), in)
	return errc(st)
}
```

Add import `cgofuse "github.com/winfsp/cgofuse/fuse"` (already present from Task 5).

- [ ] **Step 4: Run test to verify it passes**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestMutSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/fs.go pkg/client/io/cgofs/fs_mut_test.go
git commit -m "cgofs: mutations + setattr-family (mkdir/rmdir/unlink/rename/symlink/chmod/chown/utimens)"
```

---

### Task 8: Adapter xattr ops + interface assertion

Implement `Getxattr`, `Setxattr`, `Removexattr`, `Listxattr` (the last uses a `fill func(name string) bool` callback). Add a compile-time assertion that `*MountieCgoFS` satisfies `cgofuse.FileSystemInterface`.

**Files:**
- Modify: `pkg/client/io/cgofs/fs.go`
- Test: `pkg/client/io/cgofs/fs_xattr_test.go` (`//go:build darwin || cgofuse`)

**Interfaces:**
- Produces: methods `Getxattr`, `Setxattr`, `Removexattr`, `Listxattr` on `*MountieCgoFS`; var assertion `_ cgofuse.FileSystemInterface = (*MountieCgoFS)(nil)`.

- [ ] **Step 1: Write the failing test** (add recording fields `xattrData []byte`, `xattrNames []string`, `setXattr struct{name string; data []byte}` to `fakeBackend`, override Get/Set/List)

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/fs_xattr_test.go
package cgofs

import (
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type XattrSuite struct {
	suite.Suite
	be *fakeBackend
	fs *MountieCgoFS
}

func TestXattrSuite(t *testing.T) { suite.Run(t, new(XattrSuite)) }

func (s *XattrSuite) SetupTest() {
	s.be = &fakeBackend{}
	s.fs = New(s.be, nil, time.Second)
}

func (s *XattrSuite) TestGetxattr() {
	s.be.xattrData = []byte("v")
	s.be.xattrGetSt = fuse.OK
	rc, data := s.fs.Getxattr("/f", "user.k")
	s.Equal(0, rc)
	s.Equal("v", string(data))
}

func (s *XattrSuite) TestListxattr() {
	s.be.xattrNames = []string{"user.a", "user.b"}
	s.be.xattrListSt = fuse.OK
	var got []string
	rc := s.fs.Listxattr("/f", func(name string) bool { got = append(got, name); return true })
	s.Equal(0, rc)
	s.Equal([]string{"user.a", "user.b"}, got)
}
```

Add to `fakeBackend` (fields + override Get/List, keep Set/Remove returning OK):

```go
// fields: xattrData []byte; xattrGetSt fuse.Status; xattrNames []string; xattrListSt fuse.Status
func (f *fakeBackend) GetXAttr(ctx context.Context, path, attr string) ([]byte, fuse.Status) {
	return f.xattrData, f.xattrGetSt
}
func (f *fakeBackend) ListXAttr(ctx context.Context, path string) ([]string, fuse.Status) {
	return f.xattrNames, f.xattrListSt
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestXattrSuite -v`
Expected: FAIL — `fs.Getxattr undefined`.

- [ ] **Step 3: Implement** (append to `fs.go`)

```go
func (fs *MountieCgoFS) Getxattr(path string, name string) (int, []byte) {
	ctx, cancel := fs.opCtx()
	defer cancel()
	data, st := fs.backend.GetXAttr(ctx, clean(path), name)
	if !st.Ok() {
		return errc(st), nil
	}
	return 0, data
}

func (fs *MountieCgoFS) Setxattr(path string, name string, value []byte, flags int) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.SetXAttr(ctx, clean(path), name, value, uint32(flags)))
}

func (fs *MountieCgoFS) Removexattr(path string, name string) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	return errc(fs.backend.RemoveXAttr(ctx, clean(path), name))
}

func (fs *MountieCgoFS) Listxattr(path string, fill func(name string) bool) int {
	ctx, cancel := fs.opCtx()
	defer cancel()
	names, st := fs.backend.ListXAttr(ctx, clean(path))
	if !st.Ok() {
		return errc(st)
	}
	for _, n := range names {
		if !fill(n) {
			break
		}
	}
	return 0
}

// Compile-time guard: the adapter must satisfy cgofuse's interface. If a
// signature drifts upstream, the build breaks here, not at mount time.
var _ cgofuse.FileSystemInterface = (*MountieCgoFS)(nil)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -v`
Expected: PASS (all adapter suites).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/cgofs/fs.go pkg/client/io/cgofs/fs_xattr_test.go pkg/client/io/cgofs/fakebackend_test.go
git commit -m "cgofs: xattr ops + FileSystemInterface compile-time assertion"
```

---

### Task 9: `mountHandle` abstraction + go-fuse mounter refactor

`single.go` is coupled to `*fuse.Server`. Introduce a `mountHandle` interface and move the go-fuse mount establishment into a build-tagged function, leaving Linux behaviour identical. This is a refactor: existing mount tests must still pass.

**Files:**
- Create: `pkg/client/mount/handle.go` (no tag — pure interface)
- Create: `pkg/client/mount/establish_gofuse.go` (`//go:build !darwin && !cgofuse`)
- Modify: `pkg/client/mount/single.go` (use `mountHandle` instead of `*fuse.Server`)
- Test: `pkg/client/mount/handle_test.go` (no tag)

**Interfaces:**
- Produces: `type mountHandle interface { Wait(); Unmount(mountPath string) error }`.
- Produces (build-tagged): `func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int) (mountHandle, error)`.
- Changes: `SingleVolumeMounterImpl.mounts` becomes `*xsync.MapOf[string, mountHandle]`; `Wait`/`Unmount`/`releaseVolume` use the interface.

- [ ] **Step 1: Write the failing test**

```go
// pkg/client/mount/handle_test.go
package mount

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// fakeHandle implements mountHandle for the refactor test.
type fakeHandle struct {
	waited     bool
	unmounted  string
}

func (h *fakeHandle) Wait()                        { h.waited = true }
func (h *fakeHandle) Unmount(p string) error       { h.unmounted = p; return nil }

type HandleSuite struct{ suite.Suite }

func TestHandleSuite(t *testing.T) { suite.Run(t, new(HandleSuite)) }

func (s *HandleSuite) TestMountHandleInterfaceShape() {
	var h mountHandle = &fakeHandle{}
	h.Wait()
	s.NoError(h.Unmount("/mnt/x"))
	s.Equal("/mnt/x", h.(*fakeHandle).unmounted)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/client/mount/ -run TestHandleSuite -v`
Expected: FAIL — `undefined: mountHandle`.

- [ ] **Step 3: Implement the interface + go-fuse handle + refactor**

```go
// pkg/client/mount/handle.go
package mount

// mountHandle is the platform-agnostic lifecycle handle for an established
// mount. The go-fuse path wraps *fuse.Server; the cgofuse path wraps a
// cgofuse FileSystemHost goroutine.
type mountHandle interface {
	// Wait blocks until the mount's serve loop exits (our own unmount or an
	// out-of-band detach).
	Wait()
	// Unmount requests teardown and blocks until the serve loop has exited.
	Unmount(mountPath string) error
}
```

```go
//go:build !darwin && !cgofuse

// pkg/client/mount/establish_gofuse.go
package mount

import (
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
)

// gofuseHandle wraps a go-fuse server as a mountHandle.
type gofuseHandle struct{ server *fuse.Server }

func (h *gofuseHandle) Wait() { h.server.Wait() }
func (h *gofuseHandle) Unmount(mountPath string) error {
	return stopServer(h.server, mountPath)
}

// establishMount mounts via go-fuse (Linux). Mirrors the prior inline body of
// SingleVolumeMounterImpl.Mount.
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int) (mountHandle, error) {
	root := io.NewMountieRoot(backend, rewriter, cfg.DirectIO)
	mountOpts := createMountOptions(endpoint, volume, cfg, maxWrite)
	fsOpts := buildFSOptions(mountOpts, cfg)
	server, err := gofs.Mount(mountPath, root, fsOpts)
	if err := wrapMountError(err); err != nil {
		return nil, errors.Wrap(err, "mount fail")
	}
	return &gofuseHandle{server: server}, nil
}
```

In `single.go`:
- Change the field: `mounts *xsync.MapOf[string, mountHandle]` and its init `xsync.NewMapOf[string, mountHandle]()`.
- Replace the mount block (lines ~146-157) with:

```go
	handle, err := establishMount(m.client.GetEndpoint(), volume, m.client.GetEndpoint(), backend, rewriter, m.fuse, maxWrite)
	// NOTE: signature is (mountPath, volume, endpoint, ...). Pass mountPath first:
	handle, err = establishMount(mountPath, volume, m.client.GetEndpoint(), backend, rewriter, m.fuse, maxWrite)
	if err != nil {
		return err
	}
	m.mounts.Store(volume, handle)
	m.mountPaths.Store(volume, mountPath)
	return nil
```

> Implementer note: collapse the two `establishMount` lines into one correct
> call — `handle, err := establishMount(mountPath, volume, m.client.GetEndpoint(), backend, rewriter, m.fuse, maxWrite)`. The duplicated line above is only to make the parameter order explicit.

- In `Wait`: replace `server.Wait()` with `handle.Wait()` (the loaded value is now `mountHandle`).
- In `Unmount`: replace `stopServer(server, mountPath)` with `server.Unmount(mountPath)` where the loaded value is the handle (rename local var `server`→`handle`).
- Remove the now-unused `gofs`/`fuse` imports from `single.go` (they moved to `establish_gofuse.go`); keep `xsync`.

- [ ] **Step 4: Run tests to verify the refactor is green**

Run: `go test ./pkg/client/mount/ -v`
Expected: PASS (existing mount suite + new HandleSuite). Build must stay `CGO_ENABLED=0`:
Run: `CGO_ENABLED=0 go build ./pkg/client/...`
Expected: builds clean.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/handle.go pkg/client/mount/establish_gofuse.go pkg/client/mount/single.go pkg/client/mount/handle_test.go
git commit -m "mount: extract mountHandle + go-fuse establishMount (linux unchanged)"
```

---

### Task 10: macOS mount options + FUSE provider detection

Build the macOS option set (`volname`, conditional `local`, `noappledouble`) and detect which provider is installed (macFUSE vs FUSE-T). Pure Go (no cgofuse import) so it unit-tests anywhere. Provider detection probes well-known install paths; the choice respects a config override.

**Files:**
- Create: `pkg/client/mount/macos_provider.go` (no tag)
- Test: `pkg/client/mount/macos_provider_test.go` (no tag)

**Interfaces:**
- Produces: `type fuseProvider string` with consts `providerAuto = "auto"`, `providerMacFUSE = "macfuse"`, `providerFuseT = "fuse-t"`.
- Produces: `func detectProvider(override fuseProvider, exists func(string) bool) (fuseProvider, error)` — resolves `auto` by probing paths; returns an error if neither is present and `auto`.
- Produces: `func macOSMountOptions(volume string, provider fuseProvider) []string` — the cgofuse `opts []string` (each like `-o`, `volname=…`).

- [ ] **Step 1: Write the failing test**

```go
// pkg/client/mount/macos_provider_test.go
package mount

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type MacProviderSuite struct{ suite.Suite }

func TestMacProviderSuite(t *testing.T) { suite.Run(t, new(MacProviderSuite)) }

func (s *MacProviderSuite) TestAutoPrefersMacFUSEWhenBothPresent() {
	p, err := detectProvider(providerAuto, func(string) bool { return true })
	s.NoError(err)
	s.Equal(providerMacFUSE, p)
}

func (s *MacProviderSuite) TestAutoFallsBackToFuseT() {
	exists := func(path string) bool { return path == fuseTLibPath }
	p, err := detectProvider(providerAuto, exists)
	s.NoError(err)
	s.Equal(providerFuseT, p)
}

func (s *MacProviderSuite) TestAutoErrorsWhenNeitherPresent() {
	_, err := detectProvider(providerAuto, func(string) bool { return false })
	s.Error(err)
}

func (s *MacProviderSuite) TestExplicitOverrideHonored() {
	p, err := detectProvider(providerFuseT, func(string) bool { return false })
	s.NoError(err)
	s.Equal(providerFuseT, p)
}

func (s *MacProviderSuite) TestOptionsIncludeVolnameAlways() {
	opts := macOSMountOptions("photos", providerFuseT)
	s.Contains(opts, "volname=photos")
	s.NotContains(opts, "local") // FUSE-T rejects unknown opts
}

func (s *MacProviderSuite) TestOptionsIncludeLocalForMacFUSE() {
	opts := macOSMountOptions("photos", providerMacFUSE)
	s.Contains(opts, "local")
	s.Contains(opts, "volname=photos")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/client/mount/ -run TestMacProviderSuite -v`
Expected: FAIL — `undefined: detectProvider`.

- [ ] **Step 3: Implement**

```go
// pkg/client/mount/macos_provider.go
package mount

import (
	"fmt"
	"os"
)

type fuseProvider string

const (
	providerAuto    fuseProvider = "auto"
	providerMacFUSE fuseProvider = "macfuse"
	providerFuseT   fuseProvider = "fuse-t"

	// macFUSELibPath is the macFUSE userspace library install location.
	macFUSELibPath = "/usr/local/lib/libfuse.2.dylib"
	// fuseTLibPath is the FUSE-T userspace library install location.
	fuseTLibPath = "/usr/local/lib/libfuse-t.dylib"
)

// detectProvider resolves which FUSE provider to use. override wins unless it
// is providerAuto, in which case it probes install paths (macFUSE preferred,
// FUSE-T fallback) using exists (injected for testing; pass pathExists in
// production). Returns an error when auto finds neither.
func detectProvider(override fuseProvider, exists func(string) bool) (fuseProvider, error) {
	if override != providerAuto {
		return override, nil
	}
	if exists(macFUSELibPath) {
		return providerMacFUSE, nil
	}
	if exists(fuseTLibPath) {
		return providerFuseT, nil
	}
	return "", fmt.Errorf("no FUSE provider found: install macFUSE (https://macfuse.github.io) or FUSE-T (https://www.fuse-t.org)")
}

// pathExists is the production exists probe for detectProvider.
func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// macOSMountOptions builds the cgofuse mount option list. volname is always
// set (Finder needs a name). "local" is macFUSE-only — it makes Finder treat
// the mount as a browsable local device (fixes "terminal sees files, Finder
// doesn't"); FUSE-T rejects unknown options, so it is omitted there.
func macOSMountOptions(volume string, provider fuseProvider) []string {
	opts := []string{
		"-o", "volname=" + volume,
		"-o", "noappledouble",
	}
	if provider == providerMacFUSE {
		opts = append(opts, "-o", "local")
	}
	return opts
}
```

> Note: the `volname=photos` / `local` assertions in the test use
> `s.Contains(opts, ...)`; since opts interleaves `-o` flags, adjust the test
> helper to scan the slice for the value tokens (the test above already checks
> for the bare value tokens `"volname=photos"` and `"local"`, which are present
> as their own elements).

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/client/mount/ -run TestMacProviderSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/macos_provider.go pkg/client/mount/macos_provider_test.go
git commit -m "mount: macOS FUSE provider detection + mount options (volname/local)"
```

---

### Task 11: cgofuse mounter + host lifecycle + config wiring

Implement the darwin/cgofuse `establishMount` that builds the adapter, starts a cgofuse `FileSystemHost` in a goroutine, and signals readiness via the adapter's `Init`. Add `config.FUSEConfig.Provider` so the user can pin macfuse/fuse-t/auto. This task's mount code is tagged; the readiness logic is testable via a small unit on the handle's channel plumbing.

**Files:**
- Create: `pkg/client/mount/establish_cgofuse.go` (`//go:build darwin || cgofuse`)
- Create: `pkg/client/io/cgofs/ready.go` (no tag — the readiness channel + Init hook live in the adapter)
- Modify: `pkg/client/io/cgofs/fs.go` (add `Init()`/`Destroy()` that signal the ready/done channels)
- Modify: `pkg/client/config/*` FUSEConfig struct + constructor default (`Provider: "auto"`)
- Test: `pkg/client/io/cgofs/ready_test.go` (`//go:build darwin || cgofuse`)

**Interfaces:**
- Consumes: `cgofs.New`, `cgofuse.NewFileSystemHost`, `(*FileSystemHost).Mount/Unmount`, `detectProvider`, `macOSMountOptions`, `config.FUSEConfig.Provider`, `m.client.MetaTimeout()`.
- Produces: `func (fs *MountieCgoFS) Ready() <-chan struct{}` (closed by `Init`); `func (fs *MountieCgoFS) Done() <-chan struct{}` (closed by `Destroy`).
- Produces (tagged): `establishMount(...)` with the SAME signature as the go-fuse one (Task 9).
- Produces: `cgofuseHandle` implementing `mountHandle` (`Wait` blocks on `Done`; `Unmount` calls `host.Unmount()` then waits on `Done`).

- [ ] **Step 1: Write the failing readiness test**

```go
//go:build darwin || cgofuse

// pkg/client/io/cgofs/ready_test.go
package cgofs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type ReadySuite struct{ suite.Suite }

func TestReadySuite(t *testing.T) { suite.Run(t, new(ReadySuite)) }

func (s *ReadySuite) TestInitClosesReady() {
	fs := New(&fakeBackend{}, nil, time.Second)
	select {
	case <-fs.Ready():
		s.Fail("ready closed before Init")
	default:
	}
	fs.Init()
	select {
	case <-fs.Ready():
	case <-time.After(time.Second):
		s.Fail("ready not closed after Init")
	}
}

func (s *ReadySuite) TestDestroyClosesDone() {
	fs := New(&fakeBackend{}, nil, time.Second)
	fs.Destroy()
	select {
	case <-fs.Done():
	case <-time.After(time.Second):
		s.Fail("done not closed after Destroy")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestReadySuite -v`
Expected: FAIL — `fs.Ready undefined`.

- [ ] **Step 3: Implement readiness + Init/Destroy + the cgofuse mounter + config**

Add the channels to `MountieCgoFS` in `fs.go` (extend the struct and `New`):

```go
// add fields:  ready chan struct{}; done chan struct{}; readyOnce, doneOnce sync.Once
// in New(): ready: make(chan struct{}), done: make(chan struct{}),
```

```go
// pkg/client/io/cgofs/ready.go
package cgofs

import "sync"

// Init is called by cgofuse when the filesystem is initialized; closing ready
// lets the mounter return once the volume is live. Destroy is called on
// teardown; closing done lets Wait/Unmount observe the serve loop's exit.
func (fs *MountieCgoFS) Init() { fs.readyOnce.Do(func() { close(fs.ready) }) }

func (fs *MountieCgoFS) Destroy() { fs.doneOnce.Do(func() { close(fs.done) }) }

func (fs *MountieCgoFS) Ready() <-chan struct{} { return fs.ready }
func (fs *MountieCgoFS) Done() <-chan struct{}  { return fs.done }

var _ = sync.Once{} // ensure sync import if not already present in fs.go
```

> Implementer note: put `readyOnce/doneOnce sync.Once` and the `ready/done`
> channels on the struct in `fs.go`, import `sync` there, and delete the dummy
> `var _ = sync.Once{}` line — it is only a reminder.

```go
//go:build darwin || cgofuse

// pkg/client/mount/establish_cgofuse.go
package mount

import (
	"fmt"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/io"
	"go.gmountie.dev/gmountie/pkg/client/io/cgofs"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	cgofuse "github.com/winfsp/cgofuse/fuse"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// cgofuseHandle wraps a cgofuse FileSystemHost goroutine as a mountHandle.
type cgofuseHandle struct {
	host *cgofuse.FileSystemHost
	fs   *cgofs.MountieCgoFS
}

func (h *cgofuseHandle) Wait() { <-h.fs.Done() }
func (h *cgofuseHandle) Unmount(mountPath string) error {
	if !h.host.Unmount() {
		return errors.Errorf("cgofuse unmount %s failed", mountPath)
	}
	<-h.fs.Done()
	return nil
}

// establishMount mounts via cgofuse (macOS now; Windows later). Builds the
// adapter, starts the FUSE host in a goroutine, and blocks until the adapter's
// Init fires (mount live) or a timeout elapses. Same signature as the go-fuse
// establishMount so single.go is platform-agnostic.
func establishMount(mountPath, volume, endpoint string, backend io.FileSystemBackend, rewriter *io.IDRewriter, cfg *config.FUSEConfig, maxWrite int) (mountHandle, error) {
	provider, err := detectProvider(fuseProvider(cfg.Provider), pathExists)
	if err != nil {
		return nil, err
	}
	adapter := cgofs.New(backend, rewriter, cfg.MetaTimeout)
	host := cgofuse.NewFileSystemHost(adapter)
	opts := macOSMountOptions(volume, provider)

	go func() {
		// Mount blocks until the volume is unmounted; ok==false means the
		// mount never came up. Destroy (-> Done) fires on exit either way.
		if ok := host.Mount(mountPath, opts); !ok {
			log.Log.Error("cgofuse mount exited without success",
				zap.String("volume", volume), zap.String("mount_path", mountPath))
		}
		adapter.Destroy() // ensure Done closes even if cgofuse skipped Destroy
	}()

	select {
	case <-adapter.Ready():
		log.Log.Info("cgofuse mount live", zap.String("volume", volume), zap.String("provider", string(provider)))
		return &cgofuseHandle{host: host, fs: adapter}, nil
	case <-time.After(15 * time.Second):
		host.Unmount()
		return nil, fmt.Errorf("cgofuse mount of %s did not become ready within 15s (provider=%s)", volume, provider)
	}
}
```

Config: add `Provider string` to `config.FUSEConfig` (mapstructure tag `provider`) and default it to `"auto"` in the FUSE config constructor (NOT viper.SetDefault). Note `cfg.MetaTimeout` — if `FUSEConfig` lacks a meta timeout, pass `m.client.MetaTimeout()` instead by threading it through `establishMount`; if so, add a `metaTimeout time.Duration` parameter to BOTH `establishMount` implementations and pass `m.client.MetaTimeout()` from `single.go`.

> Implementer decision (pick one, apply to both establishMount impls + the call site): (a) add `MetaTimeout time.Duration` to `FUSEConfig`, or (b) add a `metaTimeout` param to `establishMount`. Option (b) is less invasive; the go-fuse impl simply ignores it.

- [ ] **Step 4: Run tests**

Run: `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/ -run TestReadySuite -v`
Expected: PASS.
Run (Linux production build still clean, no cgo): `CGO_ENABLED=0 go build ./...`
Expected: builds (cgofuse files excluded by tags).
Run (cgofuse variant compiles on Linux): `CGO_ENABLED=1 go build -tags cgofuse ./pkg/client/...`
Expected: builds (needs `libfuse-dev`).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/mount/establish_cgofuse.go pkg/client/io/cgofs/ready.go pkg/client/io/cgofs/fs.go pkg/client/io/cgofs/ready_test.go pkg/client/config/
git commit -m "mount: cgofuse mounter + host lifecycle + FUSEConfig.Provider"
```

---

### Task 12: CI — libfuse conformance lane + macOS build lane

Make CI build and run the cgofuse adapter suites on Linux (with libfuse) and build the macOS artifact on a mac runner. Adapter suites do not mount, so the Linux lane needs only `libfuse-dev`.

**Files:**
- Modify: `.github/workflows/*.yml` (the CI workflow that runs Go tests)

**Interfaces:**
- Produces: a CI job step installing `libfuse-dev` and running `CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/...`.
- Produces: a `macos-latest` job that runs `CGO_ENABLED=1 go build -tags cgofuse ./...` after installing macFUSE (or builds against FUSE headers).

- [ ] **Step 1: Add the Linux cgofuse-conformance job step**

```yaml
  cgofs-conformance:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - name: Install libfuse (FUSE2 headers for cgofuse)
        run: sudo apt-get update && sudo apt-get install -y libfuse-dev
      - name: Run cgofs adapter suites (no mount)
        run: CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/... -v
```

- [ ] **Step 2: Add the macOS build job**

```yaml
  macos-build:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - name: Install macFUSE
        run: brew install --cask macfuse
      - name: Build darwin client (cgo)
        run: CGO_ENABLED=1 go build ./cmd/...
```

- [ ] **Step 3: Verify the Linux production lane is unchanged**

Confirm the existing test job still runs `CGO_ENABLED=0 go build ./...` (or equivalent) without the `cgofuse` tag. Add it if absent:

```yaml
      - name: Verify no-cgo Linux build (guards the invariant)
        run: CGO_ENABLED=0 go build ./...
```

- [ ] **Step 4: Validate workflow syntax locally**

Run: `python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml'))"` (adjust filename)
Expected: no error (valid YAML).

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/
git commit -m "ci: cgofs conformance lane (libfuse) + macOS build lane; guard no-cgo linux"
```

---

### Task 13: Linux go-fuse vs cgofuse benchmark variant

Make the cgofuse path runnable on Linux for the perf comparison that gates the future "unify Linux" decision. The mounter selection already supports `-tags cgofuse` on Linux (Task 9/11), so this task wires a documented benchmark invocation and records to Bencher.

**Files:**
- Create: `docs/design/benchmarks/cgofuse-vs-gofuse.md` (how to run the comparison)
- Modify: `scripts/perf/*` (add a `--fuse-binding` switch or a second binary build) — follow the existing perf harness layout.

**Interfaces:**
- Produces: a documented command to build a `CGO_ENABLED=1 go build -tags cgofuse` client and run it through the existing LAN/WAN matrix harness, tagged in Bencher (project slug `gmountie-tfkojd8g`, testbed `gmountie-perf-pod`).

- [ ] **Step 1: Write the benchmark runbook**

```markdown
# go-fuse vs cgofuse (Linux) benchmark

Gates the deferred "unify Linux onto cgofuse" decision (perf-parity AND
acceptable build cost). Run on the perf VM, not in CI.

## Build both clients
- go-fuse (production): `CGO_ENABLED=0 go build -o gmountie.gofuse ./cmd/gmountie`
- cgofuse:              `CGO_ENABLED=1 go build -tags cgofuse -o gmountie.cgofuse ./cmd/gmountie`
  (needs `libfuse-dev`)

## Run the matrix
Use the existing LAN/WAN netem matrix harness against the same server, once
per binary. Record both to Bencher (project `gmountie-tfkojd8g`, testbed
`gmountie-perf-pod`, branch master) with a `binding=gofuse|cgofuse` label.

## Decide
Compare metadata-heavy + sequential-throughput + WAN profiles. Unify Linux
only if cgofuse is at parity AND the cgo build cost is judged acceptable.
```

- [ ] **Step 2: Wire the harness switch**

Follow the existing `scripts/perf/run.sh` (or equivalent) pattern to accept a binary path / `--fuse-binding` argument selecting which client binary to launch. Do not invent a new harness — pass the chosen binary into the current one.

- [ ] **Step 3: Smoke-test both builds compile on the perf VM**

Run (on the perf VM): `CGO_ENABLED=0 go build -o /tmp/g.gofuse ./cmd/gmountie && CGO_ENABLED=1 go build -tags cgofuse -o /tmp/g.cgofuse ./cmd/gmountie`
Expected: both build.

- [ ] **Step 4: Commit**

```bash
git add docs/design/benchmarks/cgofuse-vs-gofuse.md scripts/perf/
git commit -m "perf: runbook + harness switch for go-fuse vs cgofuse linux comparison"
```

---

## Self-Review

**Spec coverage:**
- macOS adapter on `FileSystemBackend` seam → Tasks 4–8. ✓
- macFUSE + FUSE-T support, auto-detect, default macFUSE → Task 10–11. ✓
- Finder fix (`volname`/`local`) → Task 10. ✓
- Identity via caller stamp + server-side mapping → Task 1 (seam) + Task 5 (`opCtx`/`Getcontext`). ✓
- Locking NOT forwarded (cgofuse has no lock callbacks) → encoded by omission + Global Constraints + spec correction. ✓
- Status mapping, handle table → Tasks 2–3. ✓
- Linux stays go-fuse, `CGO_ENABLED=0`, build-tag isolation → Task 9 + Global Constraints + Task 12 guard. ✓
- macOS runner release lane → Task 12. ✓
- Linux cgofuse-buildable benchmark to gate unify decision → Task 11 (build) + Task 13. ✓
- Testing layers (unit anywhere, conformance with libfuse, macOS e2e gated) → Tasks 2/3/10 (anywhere), 4–8/11 (libfuse), Task 12. The gated macOS *mount* e2e (`GMOUNTIE_E2E_MACOS`) is the one spec item NOT scripted here because it requires a real mac runner with macFUSE; it is called out in Task 12 as build-only on CI and left as manual e2e — acceptable, flagged.

**Placeholder scan:** No "TBD"/"add error handling" placeholders. Two "implementer note/decision" callouts (Task 9 collapse-the-call, Task 11 metaTimeout location) are explicit either/or decisions with both branches specified, not vague gaps.

**Type consistency:** `establishMount` signature identical in Tasks 9 & 11 (plus the optional `metaTimeout` param decision applied to both). `errc`, `fillStat`, `fillStatfs`, `newHandleTable`/`add`/`get`/`remove`, `New`, `opCtx`, `clean`, `splitPath`, `WithCaller` names consistent across tasks. `MountieCgoFS` methods match cgofuse `FileSystemInterface` signatures captured from `fsop.go`.

## Open follow-ups (not blockers, noted in spec)

- `GMOUNTIE_E2E_MACOS` real-mount suite on a mac runner (manual for now).
- Possible neutralization of `fuse.Status` → `syscall.Errno` in the seam (deferred refactor).
- `Lookup`/`GetAttrIfChanged` revalidation optimisation is not exploited by the path-based cgofuse `Getattr` (acceptable; verify caching behaviour during macOS e2e).
