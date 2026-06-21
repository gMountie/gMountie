# Canonical FS/file error codes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the raw Linux-numbered `int32 status` errno on `RpcFs`/`RpcFile` replies with an OS-neutral `FsError` proto enum, mapped per-endpoint, so the macFUSE client gets correct errno and the wire stops assuming a Linux server.

**Architecture:** A new `FsError` proto enum is the canonical wire error. A shared `pkg/common/fserr` package converts between `FsError` and the **host** kernel's errno using Go's `syscall.Errno` (whose `E*` constants are already per-GOOS-correct), so one `ToErrno` serves both the go-fuse and cgofuse adapters host-correctly and `FromErrno` serves the server; only the name-divergent `ENODATA`/`ENOATTR` xattr case needs a build-tagged shim. The `FileSystemBackend` client seam is changed to return `proto.FsError` instead of go-fuse's `fuse.Status`.

**Tech Stack:** Go, protobuf (`task gen:grpc`), mockery (`task gen:mocks`), testify suites, go-fuse v2, winfsp/cgofuse.

## Global Constraints

- Module path is `go.gmountie.dev/gmountie`; proto package is `gmountie`, `option go_package = "pkg/proto"`; generated Go in `pkg/proto/`.
- Tests are methods on a testify suite, never standalone `func TestX`.
- Errors wrapped with `github.com/pkg/errors`; logging via `pkg/utils/log`.
- Do **not** hand-edit `internal/mocks/` or `pkg/proto/` — regenerate (`task gen:mocks`, `task gen:grpc`).
- No backwards compatibility: this is a breaking wire change; both ends release together. Don't add transitional dual-encoding.
- Scope is the `RpcFs`/`RpcFile` errno path only. Do **not** touch `SessionService`/`VolumeService`/`VersionService` error reporting (they use gRPC status codes).
- Can't run `go test ./...` locally (FUSE); gate on the union of touched packages and let CI run the FUSE e2e.

## File Structure

- `api/proto/common.proto` — **add** `enum FsError` (shared; fs/file already import common for `Attr`/`Caller`).
- `api/proto/fs.proto`, `api/proto/file.proto` — **modify**: every reply `int32 status` → `FsError status` (26 fields).
- `pkg/proto/*` — **regenerated**.
- `pkg/common/fserr/fserr.go` — **create**: `ToErrno`/`FromErrno`/`FromGRPCCode` + the same-name code table.
- `pkg/common/fserr/fserr_linux.go`, `fserr_darwin.go` — **create**: build-tagged shim for the name-divergent xattr code.
- `pkg/common/fserr/fserr_test.go` — **create**: round-trip + totality tests.
- `pkg/server/controller/fs.go`, `file.go` — **modify**: `Status: int32(s)` → `Status: fserr.FromErrno(syscall.Errno(s))`.
- `pkg/client/io/backend.go` — **modify**: the `FileSystemBackend` interface, `fuse.Status` → `proto.FsError` (~30 sigs).
- `pkg/client/io/backend_grpc.go` — **modify**: `BackendClient` impl + `statusFromRPCError`/`fdOpStatus`/`reclaimError` to `proto.FsError`.
- `pkg/client/cache/backend.go` — **modify**: the cache decorator's `FileSystemBackend` methods, `fuse.Status` → `proto.FsError`.
- `pkg/client/io/node.go` — **modify**: go-fuse adapter, `syscall.Errno(st)` → `fserr.ToErrno(st)`; `st.Ok()` → `st == proto.FsError_FS_OK`.
- `pkg/client/io/cgofs/status.go`, `fs.go` — **modify**: `errc` + the 5 inline `-int(fuse.EBADF)`.
- `internal/mocks/pkg/client/io/mock_FileSystemBackend.go` — **regenerated**.

---

### Task 1: `FsError` proto enum

**Files:**
- Modify: `api/proto/common.proto`
- Modify: `api/proto/fs.proto` (14 status fields), `api/proto/file.proto` (12 status fields)
- Regenerate: `pkg/proto/`

**Interfaces:**
- Produces: `enum FsError` in proto package `gmountie` → Go `proto.FsError` with values `proto.FsError_FS_OK` (=0), `proto.FsError_FS_ENOENT`, … Every reply message's `status` field becomes type `FsError`.

This task only *adds* the enum and flips the field declarations; downstream Go won't compile until Task 3, so we verify via `protoc`/`buf` codegen + a generated-constant check, not a full build.

- [ ] **Step 1: Add the enum to `common.proto`.** Append (proto3, prefixed values, `0 = FS_OK`):

```proto
// FsError is the canonical, OS-neutral filesystem error for RpcFs/RpcFile
// replies. The wire never carries a raw OS errno: the server maps its native
// errno to FsError, and each client maps FsError back to its host errno
// (pkg/common/fserr). 0 == FS_OK (success), matching proto3 zero-default.
enum FsError {
  FS_OK = 0;
  FS_EPERM = 1;
  FS_ENOENT = 2;
  FS_EIO = 3;
  FS_ENXIO = 4;
  FS_EBADF = 5;
  FS_EAGAIN = 6;
  FS_EACCES = 7;
  FS_EBUSY = 8;
  FS_EEXIST = 9;
  FS_EXDEV = 10;
  FS_ENOTDIR = 11;
  FS_EISDIR = 12;
  FS_EINVAL = 13;
  FS_EMFILE = 14;
  FS_ENFILE = 15;
  FS_EFBIG = 16;
  FS_ENOSPC = 17;
  FS_EROFS = 18;
  FS_EMLINK = 19;
  FS_ERANGE = 20;
  FS_ENAMETOOLONG = 21;
  FS_ENOSYS = 22;
  FS_ENOTEMPTY = 23;
  FS_ELOOP = 24;
  FS_EOVERFLOW = 25;
  FS_EDQUOT = 26;
  FS_ESTALE = 27;
  FS_ENOTSUP = 28;
  FS_ENO_XATTR = 29;  // missing xattr: Linux ENODATA, Darwin ENOATTR
  FS_EINTR = 30;
  FS_ETXTBSY = 31;
}
```

- [ ] **Step 2: Flip every reply `status` field type.** In `fs.proto` and `file.proto`, change each `int32 status = N;` to `FsError status = N;` (keep the field number). The 26 messages (verbatim list to find): `GetAttrReply`, `UnlinkReply`, `AccessReply`, `ReadlinkReply`, `SetAttrReply`, `ReadDirBatch`, `MkdirReply`, `RmdirReply`, `RenameReply`, `GetXAttrReply`, `SetXAttrReply`, `RemoveXAttrReply`, `ListXAttrReply` (fs.proto); `OpenReply`, `CreateReply`, `ReadFrame`, `WriteReply`, `FsyncReply`, `FlushReply`, `WriteAndFlushReply`, `GetLkReply`, `SetLkReply`, `SetLkwReply`, `AllocateReply`, `CopyFileRangeReply`, `LseekReply` (file.proto).

- [ ] **Step 3: Regenerate stubs.**

Run: `task gen:grpc`
Expected: regenerates `pkg/proto/*.pb.go`; `git status` shows `common.pb.go`, `fs.pb.go`, `file.pb.go` changed; the reply structs now have `Status proto.FsError` fields and a new `FsError` type with the constants.

- [ ] **Step 4: Verify the generated enum.** Confirm the constants exist:

Run: `grep -E "FsError_FS_OK FsError = 0|FsError_FS_ENO_XATTR|FsError_FS_ENOTEMPTY" pkg/proto/common.pb.go`
Expected: matches (the enum and its values generated). `CGO_ENABLED=0 go build ./pkg/proto/...` succeeds (proto package alone still builds).

- [ ] **Step 5: Commit.**

```bash
git add api/proto/ pkg/proto/
git commit -m "proto: add OS-neutral FsError enum; type RpcFs/RpcFile status fields"
```

---

### Task 2: `pkg/common/fserr` mapping package

**Files:**
- Create: `pkg/common/fserr/fserr.go`, `fserr_linux.go`, `fserr_darwin.go`
- Test: `pkg/common/fserr/fserr_test.go`

**Interfaces:**
- Consumes: `proto.FsError` (Task 1).
- Produces:
  - `func ToErrno(e proto.FsError) syscall.Errno` — `FS_OK`→0; host-correct errno otherwise.
  - `func FromErrno(e syscall.Errno) proto.FsError` — 0→`FS_OK`; unmapped→`FS_EIO`.
  - `func FromGRPCCode(c codes.Code) proto.FsError` — `NotFound`→`FS_ENOENT`, `PermissionDenied`/`Unauthenticated`→`FS_EACCES`, else `FS_EIO`.

- [ ] **Step 1: Write the failing test** (`pkg/common/fserr/fserr_test.go`). Round-trip + totality; assert Linux numbers on the default (linux) build:

```go
package fserr

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

type FserrSuite struct{ suite.Suite }

func TestFserrSuite(t *testing.T) { suite.Run(t, new(FserrSuite)) }

// allErrs is every FsError value the mapping must handle.
var allErrs = []proto.FsError{
	proto.FsError_FS_EPERM, proto.FsError_FS_ENOENT, proto.FsError_FS_EIO,
	proto.FsError_FS_ENXIO, proto.FsError_FS_EBADF, proto.FsError_FS_EAGAIN,
	proto.FsError_FS_EACCES, proto.FsError_FS_EBUSY, proto.FsError_FS_EEXIST,
	proto.FsError_FS_EXDEV, proto.FsError_FS_ENOTDIR, proto.FsError_FS_EISDIR,
	proto.FsError_FS_EINVAL, proto.FsError_FS_EMFILE, proto.FsError_FS_ENFILE,
	proto.FsError_FS_EFBIG, proto.FsError_FS_ENOSPC, proto.FsError_FS_EROFS,
	proto.FsError_FS_EMLINK, proto.FsError_FS_ERANGE, proto.FsError_FS_ENAMETOOLONG,
	proto.FsError_FS_ENOSYS, proto.FsError_FS_ENOTEMPTY, proto.FsError_FS_ELOOP,
	proto.FsError_FS_EOVERFLOW, proto.FsError_FS_EDQUOT, proto.FsError_FS_ESTALE,
	proto.FsError_FS_ENOTSUP, proto.FsError_FS_ENO_XATTR, proto.FsError_FS_EINTR,
	proto.FsError_FS_ETXTBSY,
}

func (s *FserrSuite) TestOKIsZero() {
	s.Equal(syscall.Errno(0), ToErrno(proto.FsError_FS_OK))
	s.Equal(proto.FsError_FS_OK, FromErrno(0))
}

func (s *FserrSuite) TestEveryErrorMapsNonZeroAndRoundTrips() {
	for _, e := range allErrs {
		errno := ToErrno(e)
		s.NotEqualf(syscall.Errno(0), errno, "%v mapped to 0", e)
		s.Equalf(e, FromErrno(errno), "round-trip failed for %v", e)
	}
}

func (s *FserrSuite) TestUnknownErrnoIsEIO() {
	s.Equal(proto.FsError_FS_EIO, FromErrno(syscall.Errno(0x7fff)))
}

func (s *FserrSuite) TestLinuxNumbers() { // default build is linux
	s.Equal(syscall.Errno(39), ToErrno(proto.FsError_FS_ENOTEMPTY)) // ENOTEMPTY=39 on Linux
	s.Equal(syscall.Errno(syscall.ENODATA), ToErrno(proto.FsError_FS_ENO_XATTR))
}

func (s *FserrSuite) TestGRPCCodes() {
	s.Equal(proto.FsError_FS_ENOENT, FromGRPCCode(codes.NotFound))
	s.Equal(proto.FsError_FS_EACCES, FromGRPCCode(codes.PermissionDenied))
	s.Equal(proto.FsError_FS_EACCES, FromGRPCCode(codes.Unauthenticated))
	s.Equal(proto.FsError_FS_EIO, FromGRPCCode(codes.Internal))
}
```

- [ ] **Step 2: Run to verify it fails.**

Run: `go test ./pkg/common/fserr/`
Expected: FAIL — `ToErrno`/`FromErrno`/`FromGRPCCode` undefined.

- [ ] **Step 3: Implement `fserr.go`** (same-name codes; the table is the single source — `FromErrno` is its inverse):

```go
// Package fserr maps the OS-neutral proto.FsError to and from the host kernel's
// errno. syscall.E* constants are per-GOOS, so ToErrno yields the correct number
// on whatever platform this is built for; the server uses FromErrno, each client
// adapter uses ToErrno. Codes whose NAME differs across OSes (the xattr case)
// live in the build-tagged fserr_<goos>.go files via toErrnoExtra.
package fserr

import (
	"syscall"

	"google.golang.org/grpc/codes"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// toErrno is the same-name table; platform files add toErrnoExtra in init().
var toErrno = map[proto.FsError]syscall.Errno{
	proto.FsError_FS_EPERM:        syscall.EPERM,
	proto.FsError_FS_ENOENT:       syscall.ENOENT,
	proto.FsError_FS_EIO:          syscall.EIO,
	proto.FsError_FS_ENXIO:        syscall.ENXIO,
	proto.FsError_FS_EBADF:        syscall.EBADF,
	proto.FsError_FS_EAGAIN:       syscall.EAGAIN,
	proto.FsError_FS_EACCES:       syscall.EACCES,
	proto.FsError_FS_EBUSY:        syscall.EBUSY,
	proto.FsError_FS_EEXIST:       syscall.EEXIST,
	proto.FsError_FS_EXDEV:        syscall.EXDEV,
	proto.FsError_FS_ENOTDIR:      syscall.ENOTDIR,
	proto.FsError_FS_EISDIR:       syscall.EISDIR,
	proto.FsError_FS_EINVAL:       syscall.EINVAL,
	proto.FsError_FS_EMFILE:       syscall.EMFILE,
	proto.FsError_FS_ENFILE:       syscall.ENFILE,
	proto.FsError_FS_EFBIG:        syscall.EFBIG,
	proto.FsError_FS_ENOSPC:       syscall.ENOSPC,
	proto.FsError_FS_EROFS:        syscall.EROFS,
	proto.FsError_FS_EMLINK:       syscall.EMLINK,
	proto.FsError_FS_ERANGE:       syscall.ERANGE,
	proto.FsError_FS_ENAMETOOLONG: syscall.ENAMETOOLONG,
	proto.FsError_FS_ENOSYS:       syscall.ENOSYS,
	proto.FsError_FS_ENOTEMPTY:    syscall.ENOTEMPTY,
	proto.FsError_FS_ELOOP:        syscall.ELOOP,
	proto.FsError_FS_EOVERFLOW:    syscall.EOVERFLOW,
	proto.FsError_FS_EDQUOT:       syscall.EDQUOT,
	proto.FsError_FS_ESTALE:       syscall.ESTALE,
	proto.FsError_FS_ENOTSUP:      syscall.ENOTSUP,
	proto.FsError_FS_EINTR:        syscall.EINTR,
	proto.FsError_FS_ETXTBSY:      syscall.ETXTBSY,
}

var fromErrno = map[syscall.Errno]proto.FsError{}

func init() {
	for fe, en := range toErrnoExtra { // platform-specific (xattr)
		toErrno[fe] = en
	}
	for fe, en := range toErrno {
		fromErrno[en] = fe
	}
}

// ToErrno maps a canonical FsError to the host kernel's errno. FS_OK -> 0.
func ToErrno(e proto.FsError) syscall.Errno {
	if e == proto.FsError_FS_OK {
		return 0
	}
	if en, ok := toErrno[e]; ok {
		return en
	}
	return syscall.EIO
}

// FromErrno maps a host errno to the canonical FsError. 0 -> FS_OK; unmapped -> FS_EIO.
func FromErrno(e syscall.Errno) proto.FsError {
	if e == 0 {
		return proto.FsError_FS_OK
	}
	if fe, ok := fromErrno[e]; ok {
		return fe
	}
	return proto.FsError_FS_EIO
}

// FromGRPCCode maps a transport-level gRPC code to FsError (mirrors what the
// server produces; see backend_grpc.go statusFromRPCError history).
func FromGRPCCode(c codes.Code) proto.FsError {
	switch c {
	case codes.NotFound:
		return proto.FsError_FS_ENOENT
	case codes.PermissionDenied, codes.Unauthenticated:
		return proto.FsError_FS_EACCES
	default:
		return proto.FsError_FS_EIO
	}
}
```

- [ ] **Step 4: Implement the build-tagged xattr shim.**

`pkg/common/fserr/fserr_linux.go`:

```go
//go:build linux

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// Linux reports a missing xattr as ENODATA.
var toErrnoExtra = map[proto.FsError]syscall.Errno{
	proto.FsError_FS_ENO_XATTR: syscall.ENODATA,
}
```

`pkg/common/fserr/fserr_darwin.go`:

```go
//go:build darwin

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// Darwin reports a missing xattr as ENOATTR.
var toErrnoExtra = map[proto.FsError]syscall.Errno{
	proto.FsError_FS_ENO_XATTR: syscall.ENOATTR,
}
```

- [ ] **Step 5: Run tests to verify they pass.**

Run: `go test ./pkg/common/fserr/ -v`
Expected: PASS (all suite methods). `CGO_ENABLED=0 GOOS=darwin go build ./pkg/common/fserr/` also succeeds.
Fallback: if the darwin build fails with "undefined: syscall.EXXX", that constant's name diverges on darwin too — move that `proto.FsError_FS_*` entry out of the shared `toErrno` map and into both `toErrnoExtra` shims with each platform's constant (same pattern as `FS_ENO_XATTR`). The shared map must only hold names that exist on every target OS.

- [ ] **Step 6: Add the darwin-number build-tagged test.** `pkg/common/fserr/fserr_darwin_test.go`:

```go
//go:build darwin

package fserr

import (
	"syscall"

	proto "go.gmountie.dev/gmountie/pkg/proto"
)

func (s *FserrSuite) TestDarwinNumbers() {
	s.Equal(syscall.Errno(66), ToErrno(proto.FsError_FS_ENOTEMPTY)) // Darwin ENOTEMPTY
	s.Equal(syscall.ENOATTR, ToErrno(proto.FsError_FS_ENO_XATTR))
	s.Equal(syscall.Errno(35), ToErrno(proto.FsError_FS_EAGAIN)) // Darwin EAGAIN
}
```

Note: this test only runs on a darwin build (mac runner / `GOOS=darwin go vet`); it is the macOS-correctness assertion. `TestLinuxNumbers` covers the linux side on normal CI.

- [ ] **Step 7: Commit.**

```bash
git add pkg/common/fserr/
git commit -m "fserr: FsError <-> host errno via per-GOOS syscall, with xattr shim"
```

---

### Task 3: Flip the seam + wire to `FsError`

This is the atomic type change: the `FileSystemBackend` seam (interface + both implementors + both consumers + mocks) and the wire (server producers + client wire-reader) must change together — the proto field type forces it. Work top-down, build at the end.

**Files:** `pkg/server/controller/fs.go`, `pkg/server/controller/file.go`, `pkg/client/io/backend.go`, `pkg/client/io/backend_grpc.go`, `pkg/client/cache/backend.go`, `pkg/client/io/node.go`, `pkg/client/io/cgofs/status.go`, `pkg/client/io/cgofs/fs.go`, regenerated `internal/mocks/...`, plus touched tests.

**Interfaces:**
- Consumes: `fserr.FromErrno`/`ToErrno`/`FromGRPCCode` (Task 2), `proto.FsError` (Task 1).
- Produces: `FileSystemBackend` methods return `proto.FsError` (was `fuse.Status`). `BackendClient`, the cache decorator, and the mock implement the new signatures.

- [ ] **Step 1: Server controllers — emit `FsError`.** In `pkg/server/controller/fs.go` and `file.go`, the loopback FS returns `fuse.Status`; the reply field is now `FsError`. Replace each `Status: int32(s)` / `int32(status)` / `int32(fuse.OK)` / `int32(fuse.EBADF)` with `fserr.FromErrno(syscall.Errno(s))` (and `proto.FsError_FS_OK` / `proto.FsError_FS_EBADF` for the literals). Add imports `syscall` and `fserr "go.gmountie.dev/gmountie/pkg/common/fserr"`. Example (fs.go GetAttr, lines ~52–62):

```go
attr, status := fs.GetAttr(request.Path, createContext(ctx, request.Caller))
if attr == nil {
	return &proto.GetAttrReply{Status: fserr.FromErrno(syscall.Errno(status))}, nil
}
return &proto.GetAttrReply{
	Attributes: toProtoAttr(attr, &id),
	Status:     fserr.FromErrno(syscall.Errno(status)),
}, nil
```

And the literal cases: `int32(fuse.OK)` → `proto.FsError_FS_OK`; `int32(fuse.EBADF)` (file.go ReadFrame ~line 190) → `proto.FsError_FS_EBADF`.

- [ ] **Step 2: Change the seam interface.** In `pkg/client/io/backend.go`, change every `fuse.Status` return to `proto.FsError` across the `FileSystemBackend` interface (the ~30 methods listed below — verbatim from the current interface; only the return type changes). Add import `proto "go.gmountie.dev/gmountie/pkg/proto"`; drop the `fuse` import if now unused except for `fuse.FileLock` (it's still used by `GetLk/SetLk/SetLkw` params, so keep it). The signatures become:

```
Stat(ctx, path) (*Attr, proto.FsError)
GetAttrIfChanged(ctx, path, knownVersion) (*Attr, bool, proto.FsError)
Lookup(ctx, parent, name) (*Attr, proto.FsError)
ListDir(ctx, path) ([]DirEntryPlus, proto.FsError)
Access(ctx, path, mode) proto.FsError
StatFs(ctx, path) (*StatFs, proto.FsError)
GetXAttr(ctx, path, attr) ([]byte, proto.FsError)
SetXAttr(ctx, path, attr, data, flags) proto.FsError
RemoveXAttr(ctx, path, attr) proto.FsError
ListXAttr(ctx, path) ([]string, proto.FsError)
Open(ctx, path, flags) (FileHandle, proto.FsError)
Create(ctx, parent, name, flags, mode) (FileHandle, *Attr, proto.FsError)
Read(ctx, fh, off, dest) (int, proto.FsError)
Write(ctx, fh, off, data) (uint32, proto.FsError)
Release(ctx, fh) proto.FsError
Flush(ctx, fh) proto.FsError
Fsync(ctx, fh, flags) proto.FsError
Allocate(ctx, fh, off, size, mode) proto.FsError
GetLk(ctx, fh, owner, lk *fuse.FileLock, flags, out *fuse.FileLock) proto.FsError
SetLk(ctx, fh, owner, lk *fuse.FileLock, flags) proto.FsError
SetLkw(ctx, fh, owner, lk *fuse.FileLock, flags) proto.FsError
CopyFileRange(ctx, fhIn, offIn, fhOut, offOut, length, flags) (uint64, proto.FsError)
Lseek(ctx, fh, offset, whence) (uint64, proto.FsError)
Mkdir(ctx, path, mode) (*Attr, proto.FsError)
Rmdir(ctx, path) proto.FsError
Unlink(ctx, path) proto.FsError
Rename(ctx, oldPath, newPath) proto.FsError
Readlink(ctx, path) (string, proto.FsError)
Symlink(ctx, target, linkPath) (*Attr, proto.FsError)
SetAttr(ctx, path, in) (*Attr, proto.FsError)
```

(`Close() error` is unchanged.)

- [ ] **Step 3: `BackendClient` (`backend_grpc.go`).** Three changes:
  1. `reclaimError.st` field type `fuse.Status` → `proto.FsError`.
  2. `statusFromRPCError` and `fdOpStatus` return `proto.FsError`: replace the `fuse.*` returns with `fserr.FromGRPCCode(s.Code())`, the `ok/nil` default with `proto.FsError_FS_EIO`, and `fdOpStatus`'s NotFound special-case with `proto.FsError_FS_ESTALE`. New bodies:

```go
func statusFromRPCError(err error) proto.FsError {
	var re reclaimError
	if errors.As(err, &re) {
		return re.st
	}
	s, ok := grpcstatus.FromError(err)
	if !ok || s == nil {
		return proto.FsError_FS_EIO
	}
	return fserr.FromGRPCCode(s.Code())
}

func fdOpStatus(err error) proto.FsError {
	var re reclaimError
	if errors.As(err, &re) {
		return re.st
	}
	if s, ok := grpcstatus.FromError(err); ok && s != nil && s.Code() == codes.NotFound {
		return proto.FsError_FS_ESTALE
	}
	return statusFromRPCError(err)
}
```

  3. Each method: `fuse.Status(res.Status)` → `res.Status` (already `proto.FsError`), and the success guard `if !st.Ok()` → `if st != proto.FsError_FS_OK`. Example (Stat):

```go
if err != nil || res == nil {
	return nil, statusFromRPCError(err)
}
if res.GetAttributes() == nil {
	return nil, res.Status
}
return attrFromProto(res.GetAttributes()), res.Status
```

Remove the now-unused `syscall` import if `fuse.Status(syscall.ESTALE)` was its only use.

- [ ] **Step 4: Cache decorator (`pkg/client/cache/backend.go`).** Change its `FileSystemBackend` method signatures `fuse.Status` → `proto.FsError` to match the interface, and update any internal success checks from `st.Ok()` / `== fuse.OK` to `== proto.FsError_FS_OK`. It mostly passes status through from the wrapped backend; keep the pass-through, just retype. (Inspect for any place it constructs a status literal, e.g. `fuse.ENOENT` for a negative-cache hit → `proto.FsError_FS_ENOENT`.)

- [ ] **Step 5: go-fuse adapter (`node.go`).** It returns `syscall.Errno` to go-fuse. Replace `syscall.Errno(st)` with `fserr.ToErrno(st)` and `!st.Ok()` with `st != proto.FsError_FS_OK`. Example (Getattr):

```go
func (n *gMountieNode) Getattr(ctx context.Context, _ fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	a, st := n.backend.Stat(ctx, n.path())
	if st != proto.FsError_FS_OK {
		return fserr.ToErrno(st)
	}
	setAttrFromBackend(&out.Attr, a, n.rewriter)
	return 0
}
```

Add imports `fserr` and `proto`. Apply to every method that checks/returns the backend status.

- [ ] **Step 6: cgofuse adapter (`cgofs/status.go` + `fs.go`).** Rewrite `errc` to take `proto.FsError` and go through `fserr.ToErrno` (host-correct on darwin); fix the 5 inline `-int(fuse.EBADF)` (fs.go lines ~162/176/190/200/214) to `-int(fserr.ToErrno(proto.FsError_FS_EBADF))` (or a local `ebadf` const). New `status.go`:

```go
package cgofs

import (
	fserr "go.gmountie.dev/gmountie/pkg/common/fserr"
	proto "go.gmountie.dev/gmountie/pkg/proto"
)

// errc converts a backend proto.FsError into cgofuse's return convention: 0 for
// success, negative host errno otherwise. fserr.ToErrno is per-GOOS, so on a
// darwin build this yields Darwin errno numbers (what macFUSE expects).
func errc(st proto.FsError) int {
	if st == proto.FsError_FS_OK {
		return 0
	}
	return -int(fserr.ToErrno(st))
}

// ebadf is the cgofuse return for an unknown/closed handle.
func ebadf() int { return -int(fserr.ToErrno(proto.FsError_FS_EBADF)) }
```

In `fs.go`, replace each `return -int(fuse.EBADF)` with `return ebadf()`, and drop the `github.com/hanwen/go-fuse/v2/fuse` import there if it's now only used for those (check: `fuse.FATTR_*`, `fuse.Timespec` etc. may still be used — keep the import if so).

- [ ] **Step 7: Regenerate mocks.**

Run: `task gen:mocks`
Expected: `internal/mocks/pkg/client/io/mock_FileSystemBackend.go` regenerates with `proto.FsError` return types. `git status` shows it changed.

- [ ] **Step 8: Fix compilation across touched packages + tests.** Build each variant and fix references (tests that construct/assert `fuse.Status` from the backend, e.g. `node_test.go`, `cache/backend_test.go`, `cgofs/*_test.go`, any controller test):

Run:
```
CGO_ENABLED=0 go build ./... 2>&1 | head
CGO_ENABLED=1 go build -tags cgofuse ./pkg/client/... 2>&1 | head
CGO_ENABLED=0 GOOS=darwin go build ./pkg/client/... 2>&1 | head
```
Expected: all succeed. Update test fixtures: a backend returning an error now returns `proto.FsError_FS_*` (e.g. `fuse.ENOENT` → `proto.FsError_FS_ENOENT`); assertions on adapter output compare the resulting errno via `fserr.ToErrno`.

- [ ] **Step 9: Add the adapter divergent-case test (cgofs).** In a `//go:build darwin || cgofuse`-tagged cgofs test, assert a backend `FS_ENOTEMPTY`/`FS_ENO_XATTR` surfaces as the right host errc through `errc`:

```go
func (s *MetaSuite) TestErrcMapsHostErrno() {
	s.Equal(0, errc(proto.FsError_FS_OK))
	s.Equal(-int(fserr.ToErrno(proto.FsError_FS_ENOTEMPTY)), errc(proto.FsError_FS_ENOTEMPTY))
	s.Negative(errc(proto.FsError_FS_ENO_XATTR))
}
```

- [ ] **Step 10: Run the touched-package tests.**

Run:
```
CGO_ENABLED=0 go test ./pkg/common/fserr/ ./pkg/client/io/ ./pkg/client/cache/ ./pkg/server/controller/
CGO_ENABLED=1 go test -tags cgofuse ./pkg/client/io/cgofs/
gofmt -l pkg/ api/ && go vet ./pkg/client/... ./pkg/server/controller/...
```
Expected: PASS, gofmt clean, vet clean. (FUSE e2e runs in CI.)

- [ ] **Step 11: Commit.**

```bash
git add api/ pkg/ internal/mocks/
git commit -m "fs: carry OS-neutral FsError on the wire and the client seam"
```

---

## Notes for the executor

- After Task 3, the macFUSE errno bug is fixed structurally; the **real macOS confirmation** is a mount on the cloud Mac (`johnbuluba@45.74.241.139`, FUSE-T/macFUSE) asserting e.g. `rmdir` of a non-empty dir reports "Directory not empty", not a garbage error. Do this once before the PR.
- Gate the local PR run on the union of touched packages (`fserr`, `client/io`, `client/cache`, `client/io/cgofs`, `server/controller`, `pkg/proto`); a missed mock call-site only shows under `-tags cgofuse`.
- The CI `cgofs-conformance` lane (libfuse on Linux) exercises `errc` with Linux numbers; the darwin numbers are covered by the build-tagged tests on the `macos-build` lane.
