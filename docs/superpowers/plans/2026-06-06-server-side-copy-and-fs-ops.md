# Server-Side Copy + Missing FS Ops Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `copy_file_range`, `lseek` (SEEK_DATA/SEEK_HOLE), and the xattr write side (`setxattr`/`removexattr`/`listxattr`) end-to-end so intra-volume copies happen server-side instead of streaming every byte through the client.

**Architecture:** Five explicit RPCs following the existing vertical: proto → controller (session/identity/idempotency/events) → `ConfinedLoopbackFileSystem` → `FileSystemBackend` (BackendClient + cachedBackend) → go-fuse node adapters. Spec: `docs/design/server-side-copy-and-fs-ops.md`.

**Tech Stack:** Go, gRPC/protobuf (`task gen:grpc`), mockery (`task gen:mocks`), go-fuse v2, testify suites.

**Spec amendments discovered during planning** (apply them as you go; they refine, not contradict, the spec):
1. The client has deferred dirty data after all: `grpcFileHandle.coalescer` holds small writes until flush. `CopyFileRange` must drain the coalescer on **both** handles, and `Lseek` on its handle, before issuing the RPC — exactly like `Fsync` does (`backend_grpc.go:888`).
2. `CopyFileRangeReply`/`LseekReply` carry `int32 status` (the `Allocate` convention the spec pins).
3. The server file-handle table stores opaque `nodefs.File`; raw-fd access requires a wrapper (`RawFdFile`, Task 2). Non-fd-backed files (tests, future backends) take the interface-based fallback loop.

**Conventions reminder (repo):** conventional commits, body explains *why*, NO `Co-Authored-By` trailer. Logging via `go.gmountie.dev/gmountie/pkg/utils/log`. All work happens in `~/git/gMountie/gMountie` (the OSS repo), not gMountie-cloud.

**Before Task 1:** create an isolated branch — `git switch -c feat/server-side-copy` (or a worktree via superpowers:using-git-worktrees). All implementation commits land there, NOT on master; integration choice (merge/PR) comes at the end via superpowers:finishing-a-development-branch.

---

### Task 1: Wire protocol — five new RPCs

**Files:**
- Modify: `api/proto/file.proto` (after `AllocateReply`, line 177; service block line 179)
- Modify: `api/proto/fs.proto` (after `GetXAttrReply`, line 275; service block at end of file)

- [ ] **Step 1: Add messages + RPCs to `api/proto/file.proto`**

Insert after `AllocateReply` (before `service RpcFile`):

```proto
message CopyFileRangeRequest {
  string volume = 1;
  Caller caller = 2;
  uint64 fd_in = 3;       // open server handle, source
  string path_in = 4;
  uint64 off_in = 5;
  uint64 fd_out = 6;      // open server handle, destination
  string path_out = 7;
  uint64 off_out = 8;
  uint64 length = 9;
  uint64 flags = 10;      // must be 0 (copy_file_range(2) contract)
  string session_id = 11;
}

message CopyFileRangeReply {
  uint64 bytes_copied = 1;
  int32 status = 2;
}

message LseekRequest {
  string volume = 1;
  Caller caller = 2;
  uint64 fd = 3;
  string path = 4;
  uint64 offset = 5;
  uint32 whence = 6;      // SEEK_DATA | SEEK_HOLE only
  string session_id = 7;
}

message LseekReply {
  uint64 offset = 1;
  int32 status = 2;
}
```

Append inside `service RpcFile { ... }`:

```proto
  rpc CopyFileRange (CopyFileRangeRequest) returns (CopyFileRangeReply);
  rpc Lseek (LseekRequest) returns (LseekReply);
```

- [ ] **Step 2: Add messages + RPCs to `api/proto/fs.proto`**

Insert after `GetXAttrReply` (line 275):

```proto
message SetXAttrRequest {
  string volume = 1;
  Caller caller = 2;
  string path = 3;
  string attribute = 4;
  bytes data = 5;
  uint32 flags = 6;       // XATTR_CREATE / XATTR_REPLACE
  string request_id = 7;
  string session_id = 8;
}

message SetXAttrReply {
  int32 status = 1;
}

message RemoveXAttrRequest {
  string volume = 1;
  Caller caller = 2;
  string path = 3;
  string attribute = 4;
  string request_id = 5;
  string session_id = 6;
}

message RemoveXAttrReply {
  int32 status = 1;
}

message ListXAttrRequest {
  string volume = 1;
  Caller caller = 2;
  string path = 3;
}

message ListXAttrReply {
  repeated string attributes = 1;
  int32 status = 2;
}
```

Append inside `service RpcFs { ... }` (next to the existing `GetXAttr` rpc):

```proto
  rpc SetXAttr (SetXAttrRequest) returns (SetXAttrReply);
  rpc RemoveXAttr (RemoveXAttrRequest) returns (RemoveXAttrReply);
  rpc ListXAttr (ListXAttrRequest) returns (ListXAttrReply);
```

- [ ] **Step 3: Regenerate + build**

Run: `cd ~/git/gMountie/gMountie && task gen:grpc && go build ./...`
Expected: regenerated `pkg/proto/*.pb.go`; build succeeds. (`RpcFsServer`/`RpcFileServer` interfaces grow, but the controllers embed `proto.Unimplemented*Server`, so they still compile.)

- [ ] **Step 4: Commit**

```bash
git add api/proto pkg/proto
git commit -m "feat(proto): add CopyFileRange, Lseek, and xattr-write RPCs

Wire protocol for server-side copy and the remaining FUSE ops per
docs/design/server-side-copy-and-fs-ops.md. Handle-based ops follow
Allocate's conventions (session_id, embedded status); xattr writes
follow Chmod's (request_id for idempotent replay)."
```

---

### Task 2: Server FS — `RawFdFile` wrapper + copy engine + lseek

**Files:**
- Create: `pkg/server/io/copy_range.go`
- Create: `pkg/server/io/copy_range_test.go`
- Modify: `pkg/server/io/confined.go:400` (Open) and `:424` (Create)

- [ ] **Step 1: Write the failing tests** (`pkg/server/io/copy_range_test.go`, note: `package io` — internal, like `confined_xattr_test.go`)

```go
package io

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type CopyRangeSuite struct {
	suite.Suite
	dir string
}

func TestCopyRangeSuite(t *testing.T) { suite.Run(t, new(CopyRangeSuite)) }

func (s *CopyRangeSuite) SetupTest() { s.dir = s.T().TempDir() }

// rawFile creates path with content and opens it read-write as a RawFdFile.
// Closed via t.Cleanup through the loopback's Release.
func (s *CopyRangeSuite) rawFile(name string, content []byte) *RawFdFile {
	p := filepath.Join(s.dir, name)
	s.Require().NoError(os.WriteFile(p, content, 0o644))
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	rf := NewRawFdFile(f)
	s.T().Cleanup(func() { rf.Release() })
	return rf
}

func (s *CopyRangeSuite) readBack(name string) []byte {
	b, err := os.ReadFile(filepath.Join(s.dir, name))
	s.Require().NoError(err)
	return b
}

func (s *CopyRangeSuite) TestCopyWholeFile() {
	src := s.rawFile("src", []byte("hello copy_file_range"))
	dst := s.rawFile("dst", nil)
	n, st := CopyFileRange(src, dst, 0, 0, 21)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(21), n)
	s.Equal([]byte("hello copy_file_range"), s.readBack("dst"))
}

func (s *CopyRangeSuite) TestCopyAtOffsets() {
	src := s.rawFile("src", []byte("0123456789"))
	dst := s.rawFile("dst", []byte("XXXXXXXXXX"))
	n, st := CopyFileRange(src, dst, 2, 5, 3) // "234" -> dst@5
	s.Equal(fuse.OK, st)
	s.Equal(uint64(3), n)
	s.Equal([]byte("XXXXX234XX"), s.readBack("dst"))
}

func (s *CopyRangeSuite) TestCopyShortAtEOF() {
	src := s.rawFile("src", []byte("short"))
	dst := s.rawFile("dst", nil)
	n, st := CopyFileRange(src, dst, 0, 0, 4096) // ask for more than src has
	s.Equal(fuse.OK, st)
	s.Equal(uint64(5), n)
}

func (s *CopyRangeSuite) TestCopyZeroLength() {
	src := s.rawFile("src", []byte("abc"))
	dst := s.rawFile("dst", nil)
	n, st := CopyFileRange(src, dst, 0, 0, 0)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(0), n)
}

// Overlapping ranges within one file must surface EINVAL (kernel contract),
// not silently "succeed" via the fallback loop.
func (s *CopyRangeSuite) TestCopyOverlapSameFile_EINVAL() {
	p := filepath.Join(s.dir, "same")
	s.Require().NoError(os.WriteFile(p, make([]byte, 8192), 0o644))
	f1, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	f2, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	src, dst := NewRawFdFile(f1), NewRawFdFile(f2)
	s.T().Cleanup(func() { src.Release(); dst.Release() })

	_, st := CopyFileRange(src, dst, 0, 1024, 4096) // [0,4096) vs [1024,5120) overlap
	s.Equal(fuse.Status(syscall.EINVAL), st)
}

// The generic loop must produce identical results to the fd path and apply
// its own overlap check (it can't rely on the kernel's).
func (s *CopyRangeSuite) TestCopyGenericFallback() {
	src := s.rawFile("gsrc", []byte("generic fallback data"))
	dst := s.rawFile("gdst", nil)
	n, st := copyRangeGeneric(src, dst, 8, 0, 8) // "fallback"
	s.Equal(fuse.OK, st)
	s.Equal(uint64(8), n)
	s.Equal([]byte("fallback"), s.readBack("gdst"))
}

func (s *CopyRangeSuite) TestCopyGenericOverlap_EINVAL() {
	p := filepath.Join(s.dir, "gsame")
	s.Require().NoError(os.WriteFile(p, make([]byte, 8192), 0o644))
	f1, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	f2, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	src, dst := NewRawFdFile(f1), NewRawFdFile(f2)
	s.T().Cleanup(func() { src.Release(); dst.Release() })

	_, st := copyRangeGeneric(src, dst, 0, 1024, 4096)
	s.Equal(fuse.Status(syscall.EINVAL), st)
}

func (s *CopyRangeSuite) TestLseekDataAndHole() {
	f := s.rawFile("lseek", []byte("0123456789"))
	off, st := Lseek(f, 0, unix.SEEK_DATA)
	s.Equal(fuse.OK, st)
	s.Equal(uint64(0), off)
	off, st = Lseek(f, 0, unix.SEEK_HOLE)
	s.Equal(fuse.OK, st)
	s.GreaterOrEqual(off, uint64(10)) // implicit hole at EOF
}

func (s *CopyRangeSuite) TestLseekPastEOF_ENXIO() {
	f := s.rawFile("lseek2", []byte("abc"))
	_, st := Lseek(f, 100, unix.SEEK_DATA)
	s.Equal(fuse.Status(syscall.ENXIO), st)
}

func (s *CopyRangeSuite) TestLseekNonRawFile_ENOTSUP() {
	_, st := Lseek(nodefs.NewDefaultFile(), 0, unix.SEEK_DATA)
	s.Equal(fuse.ENOTSUP, st)
}

// Confined Open/Create must hand back fd-backed files so the controller's
// copy/lseek paths get the fast path.
func (s *CopyRangeSuite) TestConfinedOpenReturnsRawFdFile() {
	s.Require().NoError(os.WriteFile(filepath.Join(s.dir, "x"), []byte("x"), 0o644))
	cfs, err := NewConfinedLoopbackFileSystem(s.dir)
	s.Require().NoError(err)
	s.T().Cleanup(func() { unix.Close(cfs.rootFd) })

	f, st := cfs.Open("x", uint32(os.O_RDONLY), nil)
	s.Require().Equal(fuse.OK, st)
	s.T().Cleanup(func() { f.Release() })
	_, ok := f.(*RawFdFile)
	s.True(ok, "confined Open should return *RawFdFile")

	g, st := cfs.Create("y", uint32(os.O_RDWR), 0o644, nil)
	s.Require().Equal(fuse.OK, st)
	s.T().Cleanup(func() { g.Release() })
	_, ok = g.(*RawFdFile)
	s.True(ok, "confined Create should return *RawFdFile")
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/server/io/ -run TestCopyRangeSuite -v`
Expected: FAIL — `undefined: NewRawFdFile`, `undefined: CopyFileRange`, etc.

- [ ] **Step 3: Implement** (`pkg/server/io/copy_range.go`)

```go
package io

import (
	"os"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"golang.org/x/sys/unix"
)

const (
	// copyChunk caps a single copy_file_range(2) call.
	copyChunk = 1 << 30
	// copyBufSize is the buffer for the interface-based fallback loop.
	copyBufSize = 1 << 20
)

// RawFdFile couples the loopback nodefs.File with its backing *os.File so
// fd-level server ops (copy_file_range, lseek) can reach the raw
// descriptor. All nodefs.File behavior — including Release closing the
// fd — is delegated to the embedded loopback file.
type RawFdFile struct {
	nodefs.File
	Raw *os.File
}

// NewRawFdFile wraps an already-open *os.File. The loopback takes
// ownership of f; its Release closes the descriptor, after which Raw must
// not be used.
func NewRawFdFile(f *os.File) *RawFdFile {
	return &RawFdFile{File: nodefs.NewLoopbackFile(f), Raw: f}
}

// CopyFileRange copies up to length bytes from src@offIn to dst@offOut
// entirely on the server. Fast path: copy_file_range(2) between raw fds
// (reflink-fast on capable filesystems). Falls back to an interface-based
// read/write loop when the syscall reports it can't operate (EXDEV,
// EOPNOTSUPP, ENOSYS) or when either file isn't fd-backed. EINVAL is NOT
// a fallback trigger — per copy_file_range(2) it signals overlapping
// ranges within one file and must propagate. Short copies (source EOF)
// return the partial count with OK; callers reissue.
func CopyFileRange(src, dst nodefs.File, offIn, offOut, length uint64) (uint64, fuse.Status) {
	if length == 0 {
		return 0, fuse.OK
	}
	sf, sok := src.(*RawFdFile)
	df, dok := dst.(*RawFdFile)
	if sok && dok {
		n, st, fallback := copyRangeFd(sf, df, offIn, offOut, length)
		if !fallback {
			return n, st
		}
	}
	return copyRangeGeneric(src, dst, offIn, offOut, length)
}

// copyRangeFd drives copy_file_range(2). fallback=true means "syscall
// unsupported here, try the generic loop" and is only ever reported
// before any bytes moved — once data has been copied, partial progress
// is returned as a short success instead.
func copyRangeFd(src, dst *RawFdFile, offIn, offOut, length uint64) (copied uint64, st fuse.Status, fallback bool) {
	in, out := int64(offIn), int64(offOut)
	for copied < length {
		chunk := length - copied
		if chunk > copyChunk {
			chunk = copyChunk
		}
		n, err := unix.CopyFileRange(int(src.Raw.Fd()), &in, int(dst.Raw.Fd()), &out, int(chunk), 0)
		if err != nil {
			if copied > 0 {
				return copied, fuse.OK, false
			}
			switch err {
			case unix.EXDEV, unix.EOPNOTSUPP, unix.ENOSYS:
				return 0, fuse.OK, true
			}
			// NOTE: must precede errnoToStatus — that maps EXDEV to
			// EACCES for path-resolution escapes, which is wrong here.
			return 0, errnoToStatus(err), false
		}
		if n == 0 { // source EOF
			break
		}
		copied += uint64(n)
	}
	return copied, fuse.OK, false
}

// rangesOverlap reports whether [offIn, offIn+length) and
// [offOut, offOut+length) intersect.
func rangesOverlap(offIn, offOut, length uint64) bool {
	return offIn < offOut+length && offOut < offIn+length
}

// copyRangeGeneric is the interface-based fallback: read from src, write
// to dst, all server-side. Replicates the kernel's same-file overlap
// check, which the fd path gets for free. Ino==0 means GetAttr gave us
// nothing usable — skip the check rather than false-positive (volumes
// are confined to one filesystem via RESOLVE_NO_XDEV, so Ino equality
// implies same inode).
func copyRangeGeneric(src, dst nodefs.File, offIn, offOut, length uint64) (uint64, fuse.Status) {
	var sa, da fuse.Attr
	if src.GetAttr(&sa).Ok() && dst.GetAttr(&da).Ok() &&
		sa.Ino != 0 && sa.Ino == da.Ino && rangesOverlap(offIn, offOut, length) {
		return 0, fuse.EINVAL
	}
	buf := make([]byte, copyBufSize)
	var copied uint64
	for copied < length {
		chunk := uint64(len(buf))
		if rem := length - copied; rem < chunk {
			chunk = rem
		}
		res, st := src.Read(buf[:chunk], int64(offIn+copied))
		if !st.Ok() {
			return copied, st
		}
		data, st := res.Bytes(buf[:chunk])
		if !st.Ok() {
			return copied, st
		}
		if len(data) == 0 { // source EOF
			break
		}
		written := 0
		for written < len(data) {
			n, wst := dst.Write(data[written:], int64(offOut+copied)+int64(written))
			if !wst.Ok() {
				return copied + uint64(written), wst
			}
			if n == 0 {
				return copied + uint64(written), fuse.EIO
			}
			written += int(n)
		}
		copied += uint64(len(data))
	}
	return copied, fuse.OK
}

// Lseek probes hole geometry (SEEK_DATA/SEEK_HOLE) on an open file. Only
// fd-backed files can answer; anything else reports ENOTSUP. ENXIO
// (offset at/past EOF) passes through — it's the protocol's functional
// "no more data/hole" signal, not an error.
func Lseek(f nodefs.File, offset uint64, whence uint32) (uint64, fuse.Status) {
	rf, ok := f.(*RawFdFile)
	if !ok {
		return 0, fuse.ENOTSUP
	}
	off, err := unix.Seek(int(rf.Raw.Fd()), int64(offset), int(whence))
	if err != nil {
		return 0, errnoToStatus(err)
	}
	return uint64(off), fuse.OK
}
```

- [ ] **Step 4: Wire `RawFdFile` into `confined.go`**

At `confined.go:400` (Open), replace:
```go
	// os.NewFile takes ownership of fd; LoopbackFile closes it via Release.
	return nodefs.NewLoopbackFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
```
with:
```go
	// os.NewFile takes ownership of fd; the embedded LoopbackFile closes it
	// via Release. RawFdFile keeps the raw fd reachable for copy/lseek.
	return NewRawFdFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
```

At `confined.go:424` (Create), replace:
```go
	return nodefs.NewLoopbackFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
```
with:
```go
	return NewRawFdFile(os.NewFile(uintptr(fd), leaf)), fuse.OK
```

- [ ] **Step 5: Run tests**

Run: `go test ./pkg/server/io/ -v`
Expected: PASS (whole package — confirms no regression in confined/eventbus suites).

- [ ] **Step 6: Commit**

```bash
git add pkg/server/io/copy_range.go pkg/server/io/copy_range_test.go pkg/server/io/confined.go
git commit -m "feat(server/io): copy engine + lseek on fd-backed files

copy_file_range(2) between raw fds with an interface-based fallback for
non-fd files; EINVAL (overlap) propagates, only EXDEV/EOPNOTSUPP/ENOSYS
trigger fallback. RawFdFile keeps the raw descriptor reachable from the
session handle table without forking nodefs.LoopbackFile."
```

---

### Task 3: Controller — `CopyFileRange` + `Lseek` handlers

**Files:**
- Modify: `pkg/server/controller/file.go` (append after `Allocate`, line 446)
- Modify: `pkg/server/controller/file_test.go` (append tests)

- [ ] **Step 1: Write the failing tests** (append to `file_test.go`; the suite + helpers at the top of the file already exist — reuse `s.sessionMgr`, `s.sessionID`, `s.bus`)

```go
// registerRawFile creates a real temp file with content and registers it
// in the test session, returning the wire fd. Exercises the same
// RawFdFile type the confined FS hands out in production.
func (s *RpcFileServerTestSuite) registerRawFile(name string, content []byte) uint64 {
	p := filepath.Join(s.T().TempDir(), name)
	s.Require().NoError(os.WriteFile(p, content, 0o644))
	f, err := os.OpenFile(p, os.O_RDWR, 0)
	s.Require().NoError(err)
	rf := serverio.NewRawFdFile(f)
	s.T().Cleanup(func() { rf.Release() })
	sess, err := s.sessionMgr.Get(s.sessionID)
	s.Require().NoError(err)
	return sess.RegisterFile(name, rf)
}

func (s *RpcFileServerTestSuite) TestCopyFileRange_Happy() {
	// versionAfterPath consults GetVolumeFileSystem; failing it just
	// yields version 0 on the event, which is fine here.
	s.fsService.On("GetVolumeFileSystem", mock.Anything).Return(nil, status.Error(codes.NotFound, "no fs")).Maybe()
	events, cancel := s.bus.Subscribe("testVolume")
	defer cancel()

	srcFd := s.registerRawFile("src", []byte("0123456789"))
	dstFd := s.registerRawFile("dst", []byte("XXXXXXXXXX"))

	reply, err := s.server.CopyFileRange(context.Background(), &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, OffIn: 2, FdOut: dstFd, OffOut: 5,
		Length: 3, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal(uint64(3), reply.BytesCopied)

	select {
	case ev := <-events:
		s.Equal("dst", ev.Path)
	case <-time.After(time.Second):
		s.Fail("expected a mutation event for the copy destination")
	}
}

func (s *RpcFileServerTestSuite) TestCopyFileRange_BadFd() {
	srcFd := s.registerRawFile("src2", []byte("data"))
	reply, err := s.server.CopyFileRange(context.Background(), &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: 9999, Length: 4, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EBADF), reply.Status)
}

func (s *RpcFileServerTestSuite) TestCopyFileRange_NonzeroFlags_EINVAL() {
	srcFd := s.registerRawFile("src3", []byte("data"))
	reply, err := s.server.CopyFileRange(context.Background(), &proto.CopyFileRangeRequest{
		Volume: "testVolume", FdIn: srcFd, FdOut: srcFd, Length: 1, Flags: 1, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EINVAL), reply.Status)
}

func (s *RpcFileServerTestSuite) TestLseek_DataAndPastEOF() {
	fd := s.registerRawFile("lf", []byte("0123456789"))

	reply, err := s.server.Lseek(context.Background(), &proto.LseekRequest{
		Volume: "testVolume", Fd: fd, Offset: 0, Whence: uint32(unix.SEEK_DATA), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal(uint64(0), reply.Offset)

	reply, err = s.server.Lseek(context.Background(), &proto.LseekRequest{
		Volume: "testVolume", Fd: fd, Offset: 100, Whence: uint32(unix.SEEK_DATA), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.Status(syscall.ENXIO)), reply.Status)
}

func (s *RpcFileServerTestSuite) TestLseek_BadWhence_EINVAL() {
	fd := s.registerRawFile("lf2", []byte("x"))
	reply, err := s.server.Lseek(context.Background(), &proto.LseekRequest{
		Volume: "testVolume", Fd: fd, Whence: 0 /* SEEK_SET — kernel never sends it */, SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EINVAL), reply.Status)
}

func (s *RpcFileServerTestSuite) TestLseek_BadFd() {
	reply, err := s.server.Lseek(context.Background(), &proto.LseekRequest{
		Volume: "testVolume", Fd: 9999, Whence: uint32(unix.SEEK_DATA), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EBADF), reply.Status)
}
```

Add to `file_test.go` imports: `"os"`, `"path/filepath"`, `"syscall"`, `"golang.org/x/sys/unix"` (others are present).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/server/controller/ -run RpcFileServerTestSuite -v`
Expected: FAIL — `s.server.CopyFileRange undefined` (only the generated `Unimplemented` stub exists, which returns a gRPC error, so even partial wiring fails the asserts).

- [ ] **Step 3: Implement handlers** (append to `file.go`)

```go
// CopyFileRange copies length bytes between two open handles entirely on
// the server — the whole point of the RPC is that no file data crosses
// the wire. Errnos travel in-reply (Allocate convention): EBADF for a
// handle the session doesn't own, EINVAL for nonzero flags (per
// copy_file_range(2), flags must be 0). No identity re-bind: permission
// was checked at Open and the fds carry their access rights, same trust
// model as Read/Write/Allocate.
func (r *RpcFileServerImpl) CopyFileRange(ctx context.Context, request *proto.CopyFileRangeRequest) (*proto.CopyFileRangeReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if request.Flags != 0 {
		return &proto.CopyFileRangeReply{Status: int32(fuse.EINVAL)}, nil
	}
	srcEntry, ok := sess.GetFile(request.FdIn)
	if !ok {
		return &proto.CopyFileRangeReply{Status: int32(fuse.EBADF)}, nil
	}
	dstEntry, ok := sess.GetFile(request.FdOut)
	if !ok {
		return &proto.CopyFileRangeReply{Status: int32(fuse.EBADF)}, nil
	}
	copied, st := serverio.CopyFileRange(srcEntry.File, dstEntry.File, request.OffIn, request.OffOut, request.Length)
	if st == fuse.OK && copied > 0 {
		path := dstEntry.Path
		if path == "" {
			path = request.PathOut
		}
		ver := r.versionAfterPath(ctx, request.Volume, path, request.Caller)
		r.bus.Emit(request.Volume, path, ver, serverio.KindMutated)
	}
	return &proto.CopyFileRangeReply{BytesCopied: copied, Status: int32(st)}, nil
}

// Lseek probes hole geometry on an open handle. Only SEEK_DATA/SEEK_HOLE
// are valid on the wire — the kernel resolves SET/CUR/END itself and
// never sends them through FUSE. Safe on the shared server fd: Read and
// Write are pread/pwrite-based (offset-less), and each lseek(2) call
// atomically returns its result.
func (r *RpcFileServerImpl) Lseek(ctx context.Context, request *proto.LseekRequest) (*proto.LseekReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if request.Whence != uint32(unix.SEEK_DATA) && request.Whence != uint32(unix.SEEK_HOLE) {
		return &proto.LseekReply{Status: int32(fuse.EINVAL)}, nil
	}
	entry, ok := sess.GetFile(request.Fd)
	if !ok {
		return &proto.LseekReply{Status: int32(fuse.EBADF)}, nil
	}
	off, st := serverio.Lseek(entry.File, request.Offset, request.Whence)
	return &proto.LseekReply{Offset: off, Status: int32(st)}, nil
}
```

Add to `file.go` imports: `"golang.org/x/sys/unix"`.

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/server/controller/ -v`
Expected: PASS (full package).

- [ ] **Step 5: Commit**

```bash
git add pkg/server/controller/file.go pkg/server/controller/file_test.go
git commit -m "feat(server): CopyFileRange and Lseek controllers

Handle-table lookups surface EBADF in-reply so the errno reaches the
FUSE layer; a successful copy emits a mutation event for the destination
path so subscribed clients invalidate."
```

---

### Task 4: Controller — xattr write side + namespace policy

**Files:**
- Modify: `pkg/server/controller/fs.go` (extend the `// ----- Extended attributes -----` section, line 312)
- Modify: `pkg/server/controller/fs_test.go` (append tests)

- [ ] **Step 1: Write the failing tests** (append to `fs_test.go`; mirror the existing suite's `BindIdentity` mock pattern from `TestOpen`-style tests in the same file — `s.fsService.On("BindIdentity", ...)` returning a `pathfs2.MockFileSystem`)

```go
func TestXattrWriteAllowed(t *testing.T) {
	cases := map[string]bool{
		"user.foo":                true,
		"user.":                   true,
		"system.posix_acl_access": true,
		"system.posix_acl_default": true,
		"trusted.foo":             false,
		"security.capability":     false,
		"security.selinux":        false,
		"system.other":            false,
		"":                        false,
	}
	for attr, want := range cases {
		if got := xattrWriteAllowed(attr); got != want {
			t.Errorf("xattrWriteAllowed(%q) = %v, want %v", attr, got, want)
		}
	}
}

func (s *RpcServerTestSuite) TestSetXAttr_Happy() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().SetXAttr("/f", "user.k", []byte("v"), 0, mock.Anything).Return(fuse.OK)
	// event version stat
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1}, fuse.OK).Maybe()

	reply, err := s.server.SetXAttr(context.Background(), &proto.SetXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "user.k", Data: []byte("v"),
		SessionId: s.sessionID, RequestId: "req-setx-1",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
}

func (s *RpcServerTestSuite) TestSetXAttr_DisallowedNamespace_EPERM() {
	// No BindIdentity expectation: policy must reject BEFORE touching the FS.
	reply, err := s.server.SetXAttr(context.Background(), &proto.SetXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "trusted.evil", Data: []byte("v"),
		SessionId: s.sessionID, RequestId: "req-setx-2",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EPERM), reply.Status)
}

func (s *RpcServerTestSuite) TestSetXAttr_IdempotentReplay() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().SetXAttr("/f", "user.k", []byte("v"), 0, mock.Anything).Return(fuse.OK).Once()
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1}, fuse.OK).Maybe()

	req := &proto.SetXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "user.k", Data: []byte("v"),
		SessionId: s.sessionID, RequestId: "req-setx-replay",
	}
	for i := 0; i < 2; i++ {
		reply, err := s.server.SetXAttr(context.Background(), req)
		s.Require().NoError(err)
		s.Equal(int32(fuse.OK), reply.Status)
	}
	mockFs.AssertExpectations(s.T()) // .Once() proves the replay was deduped
}

func (s *RpcServerTestSuite) TestRemoveXAttr_HappyAndDisallowed() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().RemoveXAttr("/f", "user.k", mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/f", mock.Anything).Return(&fuse.Attr{Ino: 1}, fuse.OK).Maybe()

	reply, err := s.server.RemoveXAttr(context.Background(), &proto.RemoveXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "user.k",
		SessionId: s.sessionID, RequestId: "req-rmx-1",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)

	reply, err = s.server.RemoveXAttr(context.Background(), &proto.RemoveXAttrRequest{
		Volume: "testVolume", Path: "/f", Attribute: "security.capability",
		SessionId: s.sessionID, RequestId: "req-rmx-2",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.EPERM), reply.Status)
}

func (s *RpcServerTestSuite) TestListXAttr_Happy() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("BindIdentity", mock.Anything, "testVolume", mock.Anything).Return(mockFs, service.Identity{}, nil)
	mockFs.EXPECT().ListXAttr("/f", mock.Anything).Return([]string{"user.a", "user.b"}, fuse.OK)

	reply, err := s.server.ListXAttr(context.Background(), &proto.ListXAttrRequest{
		Volume: "testVolume", Path: "/f",
	})
	s.Require().NoError(err)
	s.Equal(int32(fuse.OK), reply.Status)
	s.Equal([]string{"user.a", "user.b"}, reply.Attributes)
}
```

(`RpcServerTestSuite` is the existing suite at `fs_test.go:23`; its `SetupTest` already provides `s.sessionID`, `s.bus`, and the permissive `ResolveIdentity` stub — no harness changes needed.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/server/controller/ -run 'RpcServerTestSuite|TestXattrWriteAllowed' -v`
Expected: FAIL — `undefined: xattrWriteAllowed`, `s.server.SetXAttr` resolves to the Unimplemented stub.

- [ ] **Step 3: Implement** (extend the xattr section of `fs.go`)

```go
// xattrWriteAllowed reports whether attr may be WRITTEN through the wire
// protocol. The server process may run privileged and setfsuid does not
// drop capabilities, so a client must not be able to plant trusted.* or
// security.* (e.g. security.capability — file capabilities) xattrs on
// server-side files. Only namespaces a regular user could safely own are
// writable: user.* and the POSIX ACL pair. Reads stay unfiltered.
func xattrWriteAllowed(attr string) bool {
	return strings.HasPrefix(attr, "user.") ||
		attr == "system.posix_acl_access" ||
		attr == "system.posix_acl_default"
}

func (r *RpcServerImpl) SetXAttr(ctx context.Context, request *proto.SetXAttrRequest) (*proto.SetXAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if !xattrWriteAllowed(request.Attribute) {
		return &proto.SetXAttrReply{Status: int32(fuse.EPERM)}, nil
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.SetXAttrReply, error) {
		s := fs.SetXAttr(request.Path, request.Attribute, request.Data, int(request.Flags), createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.SetXAttrReply{Status: int32(s)}, nil
	})
}

func (r *RpcServerImpl) RemoveXAttr(ctx context.Context, request *proto.RemoveXAttrRequest) (*proto.RemoveXAttrReply, error) {
	sess, err := resolveSession(ctx, r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	if !xattrWriteAllowed(request.Attribute) {
		return &proto.RemoveXAttrReply{Status: int32(fuse.EPERM)}, nil
	}
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.RemoveXAttrReply, error) {
		s := fs.RemoveXAttr(request.Path, request.Attribute, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.RemoveXAttrReply{Status: int32(s)}, nil
	})
}

// ListXAttr is read-only and identity-bound like GetXAttr — no session or
// idempotency machinery.
func (r *RpcServerImpl) ListXAttr(ctx context.Context, request *proto.ListXAttrRequest) (*proto.ListXAttrReply, error) {
	fs, _, err := r.fsService.BindIdentity(ctx, request.Volume, request.Caller)
	if err != nil {
		return nil, err
	}
	attrs, st := fs.ListXAttr(request.Path, createContext(ctx, request.Caller))
	return &proto.ListXAttrReply{Attributes: attrs, Status: int32(st)}, nil
}
```

Add to `fs.go` imports: `"strings"`.

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/server/controller/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/controller/fs.go pkg/server/controller/fs_test.go
git commit -m "feat(server): xattr write RPCs behind a namespace allowlist

SetXAttr/RemoveXAttr/ListXAttr complete the existing GetXAttr. Writes
are policy-gated to user.* + POSIX ACLs before identity binding: the
server may run privileged and setfsuid does not drop capabilities, so
trusted.*/security.* must never be client-writable."
```

---

### Task 5: Client backend — interface, BackendClient, cachedBackend, mocks

**Files:**
- Modify: `pkg/client/io/backend.go` (interface, after `GetLk`/xattr blocks)
- Modify: `pkg/client/io/backend_grpc.go` (after `Allocate`, line 938; xattr next to `GetXAttr`, line 335)
- Modify: `pkg/client/cache/backend.go` (next to `Write`, line 450, and `GetXAttr`, line 417)
- Modify: `pkg/client/cache/backend_test.go` (append invalidation test)
- Regenerate: `internal/mocks/` via `task gen:mocks`

- [ ] **Step 1: Extend `FileSystemBackend`** (in `backend.go`: first three after the `GetLk/SetLk/SetLkw` block; xattr pair after `GetXAttr`)

```go
	// CopyFileRange copies length bytes server-side from fhIn@offIn to
	// fhOut@offOut without the data crossing the wire. flags must be 0.
	// Short counts are legal (source EOF); callers reissue.
	CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, fhOut FileHandle, offOut uint64, length, flags uint64) (uint64, fuse.Status)
	// Lseek probes hole geometry (SEEK_DATA/SEEK_HOLE) on an open handle.
	Lseek(ctx context.Context, fh FileHandle, offset uint64, whence uint32) (uint64, fuse.Status)
```

```go
	// SetXAttr stores an extended attribute (flags: XATTR_CREATE/REPLACE).
	SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) fuse.Status
	// RemoveXAttr deletes an extended attribute.
	RemoveXAttr(ctx context.Context, path, attr string) fuse.Status
	// ListXAttr returns the extended-attribute names of path.
	ListXAttr(ctx context.Context, path string) ([]string, fuse.Status)
```

- [ ] **Step 2: Regenerate mocks so dependent test code compiles**

Run: `task gen:mocks && go vet ./internal/... 2>/dev/null; go build ./internal/...`
Expected: `MockFileSystemBackend` gains the five methods. (Client packages won't compile until Steps 3–4 are done — that's expected mid-task; don't commit yet.)

- [ ] **Step 3: Implement on `BackendClient`** (`backend_grpc.go`; copy/lseek after `Allocate`, xattr after `GetXAttr`)

```go
// CopyFileRange asks the server to copy length bytes from fhIn@offIn to
// fhOut@offOut entirely server-side. Pending coalesced writes on BOTH
// handles are drained first (same reason Fsync drains: the server must
// see current bytes). No retry, mirroring Allocate — replay semantics of
// a partially-applied copy are murky and the kernel reissues anyway.
func (b *BackendClient) CopyFileRange(ctx context.Context, fhIn FileHandle, offIn uint64, fhOut FileHandle, offOut uint64, length, flags uint64) (uint64, fuse.Status) {
	src := resolveHandle(fhIn)
	dst := resolveHandle(fhOut)
	if src == nil || dst == nil {
		return 0, fuse.EBADF
	}
	if st := b.drainCoalescer(src); !st.Ok() {
		return 0, st
	}
	if st := b.drainCoalescer(dst); !st.Ok() {
		return 0, st
	}
	ctx2, cancel := ioCtx(ctx, dst.ioTimeout)
	defer cancel()
	res, err := dst.fileClient.CopyFileRange(ctx2, &proto.CopyFileRangeRequest{
		Volume:    dst.volume,
		Caller:    callerFromCtx(ctx),
		FdIn:      src.fd,
		PathIn:    src.path,
		OffIn:     offIn,
		FdOut:     dst.fd,
		PathOut:   dst.path,
		OffOut:    offOut,
		Length:    length,
		Flags:     flags,
		SessionId: dst.sessionID,
	})
	if err != nil {
		log.Log.Error("error in call: CopyFileRange", zap.String("path", dst.path), zap.Error(err))
		return 0, fuse.EIO
	}
	st := fuse.Status(res.Status)
	if st.Ok() && res.BytesCopied > 0 {
		dst.dirty.Store(true) // close() should still Flush the destination
	}
	return res.BytesCopied, st
}

// Lseek probes hole geometry on the server fd. Idempotent — retried.
// The coalescer is drained first so pending writes shape the answer.
func (b *BackendClient) Lseek(ctx context.Context, fh FileHandle, offset uint64, whence uint32) (uint64, fuse.Status) {
	h := resolveHandle(fh)
	if h == nil {
		return 0, fuse.EBADF
	}
	if st := b.drainCoalescer(h); !st.Ok() {
		return 0, st
	}
	ctx2, cancel := ioCtx(ctx, h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx2, "Lseek", func(ctx context.Context) (*proto.LseekReply, error) {
		return h.fileClient.Lseek(ctx, &proto.LseekRequest{
			Volume:    h.volume,
			Caller:    callerFromCtx(ctx),
			Fd:        h.fd,
			Path:      h.path,
			Offset:    offset,
			Whence:    whence,
			SessionId: h.sessionID,
		})
	})
	if err != nil {
		log.Log.Error("error in call: Lseek", zap.String("path", h.path), zap.Error(err))
		return 0, fuse.EIO
	}
	return res.Offset, fuse.Status(res.Status)
}
```

```go
// SetXAttr stores an extended attribute. Mutating — request_id stamped
// outside retry for idempotency, like Chmod.
func (b *BackendClient) SetXAttr(ctx context.Context, path, attr string, data []byte, flags uint32) fuse.Status {
	return mutatePath(ctx, "SetXAttr", b.metaCtx,
		func(ctx context.Context, requestID string) (*proto.SetXAttrReply, error) {
			return b.client.Fs().SetXAttr(ctx, &proto.SetXAttrRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Attribute: attr,
				Data:      data,
				Flags:     flags,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			})
		},
		func(r *proto.SetXAttrReply) int32 { return r.Status },
	)
}

// RemoveXAttr deletes an extended attribute. Mutating — see SetXAttr.
func (b *BackendClient) RemoveXAttr(ctx context.Context, path, attr string) fuse.Status {
	return mutatePath(ctx, "RemoveXAttr", b.metaCtx,
		func(ctx context.Context, requestID string) (*proto.RemoveXAttrReply, error) {
			return b.client.Fs().RemoveXAttr(ctx, &proto.RemoveXAttrRequest{
				Volume:    b.volume,
				Caller:    callerFromCtx(ctx),
				Path:      path,
				Attribute: attr,
				SessionId: b.client.SessionID(),
				RequestId: requestID,
			})
		},
		func(r *proto.RemoveXAttrReply) int32 { return r.Status },
	)
}

// ListXAttr returns extended-attribute names. Idempotent.
func (b *BackendClient) ListXAttr(ctx context.Context, path string) ([]string, fuse.Status) {
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	res, err := retryableCall(ctx2, "ListXAttr", func(ctx context.Context) (*proto.ListXAttrReply, error) {
		return b.client.Fs().ListXAttr(ctx, &proto.ListXAttrRequest{
			Volume: b.volume, Caller: callerFromCtx(ctx), Path: path,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: ListXAttr", zap.String("path", path), zap.Error(err))
		return nil, fuse.EIO
	}
	return res.Attributes, fuse.Status(res.Status)
}
```

- [ ] **Step 4: Implement on `cachedBackend`** (`pkg/client/cache/backend.go`)

```go
// CopyFileRange passes through, then invalidates the DESTINATION like a
// Write of the copied range: the data cache for [offOut, offOut+n) and
// the attr entry (size/mtime moved). Source is untouched (atime only).
func (b *cachedBackend) CopyFileRange(ctx context.Context, fhIn io.FileHandle, offIn uint64, fhOut io.FileHandle, offOut uint64, length, flags uint64) (uint64, fuse.Status) {
	n, st := b.inner.CopyFileRange(ctx, unwrapHandle(fhIn), offIn, unwrapHandle(fhOut), offOut, length, flags)
	if st != fuse.OK {
		return n, st
	}
	if ch, ok := fhOut.(*cachedHandle); ok && n > 0 {
		b.data.invalidateRange(ch.path, int64(offOut), int64(n))
		b.attr.invalidate(ch.path)
	}
	return n, fuse.OK
}

// Lseek is a pure pass-through — hole geometry isn't cached.
func (b *cachedBackend) Lseek(ctx context.Context, fh io.FileHandle, offset uint64, whence uint32) (uint64, fuse.Status) {
	return b.inner.Lseek(ctx, unwrapHandle(fh), offset, whence)
}

// Xattr ops are pass-throughs like GetXAttr: xattrs are not cached and
// don't affect the stat-shaped attr cache.
func (b *cachedBackend) SetXAttr(ctx context.Context, p, attr string, data []byte, flags uint32) fuse.Status {
	return b.inner.SetXAttr(ctx, p, attr, data, flags)
}

func (b *cachedBackend) RemoveXAttr(ctx context.Context, p, attr string) fuse.Status {
	return b.inner.RemoveXAttr(ctx, p, attr)
}

func (b *cachedBackend) ListXAttr(ctx context.Context, p string) ([]string, fuse.Status) {
	return b.inner.ListXAttr(ctx, p)
}
```

- [ ] **Step 5: Write the cache invalidation test** (append to `pkg/client/cache/backend_test.go`; the suite's `SetupTest` uses `ChunkSizeBytes: 1024` and `openCachedHandle` at the top of the file provides handle plumbing)

```go
// --- CopyFileRange ---

// CopyFileRange must invalidate the destination's cached data range and
// attr entry exactly like a Write of [offOut, offOut+n). A chunk outside
// the copied range stays cached; the source is untouched.
// NOTE: if the seeding Read expectations below don't match the read
// path's chunk-aligned inner calls exactly, mirror the offsets used by
// the existing data-cache tests in this file.
func (s *CachedBackendTestSuite) TestCopyFileRangeInvalidatesDestination() {
	srcH, innerSrc := s.openCachedHandle("/src")
	dstH, innerDst := s.openCachedHandle("/dst")

	// Seed dst attr cache.
	s.inner.EXPECT().Stat(mock.Anything, "/dst").Return(&io.Attr{Ino: 2, Size: 4096}, fuse.OK).Once()
	_, st := s.b.Stat(context.Background(), "/dst")
	s.Require().Equal(fuse.OK, st)

	// Seed dst data cache: chunk 0 ([0,1024)) and chunk 2 ([2048,3072)).
	buf := make([]byte, 1024)
	s.inner.EXPECT().Read(mock.Anything, innerDst, int64(0), mock.Anything).Return(1024, fuse.OK).Once()
	_, st = s.b.Read(context.Background(), dstH, 0, buf)
	s.Require().Equal(fuse.OK, st)
	s.inner.EXPECT().Read(mock.Anything, innerDst, int64(2048), mock.Anything).Return(1024, fuse.OK).Once()
	_, st = s.b.Read(context.Background(), dstH, 2048, buf)
	s.Require().Equal(fuse.OK, st)

	// Copy 100 bytes into dst@0 — overlaps chunk 0 only.
	s.inner.EXPECT().CopyFileRange(mock.Anything, innerSrc, uint64(0), innerDst, uint64(0), uint64(100), uint64(0)).
		Return(uint64(100), fuse.OK).Once()
	n, cst := s.b.CopyFileRange(context.Background(), srcH, 0, dstH, 0, 100, 0)
	s.Require().Equal(fuse.OK, cst)
	s.Require().Equal(uint64(100), n)

	// Chunk 0 must MISS (re-fetch from inner)...
	s.inner.EXPECT().Read(mock.Anything, innerDst, int64(0), mock.Anything).Return(1024, fuse.OK).Once()
	_, st = s.b.Read(context.Background(), dstH, 0, buf)
	s.Require().Equal(fuse.OK, st)
	// ...chunk 2 must still HIT (no new EXPECT: served from cache).
	_, st = s.b.Read(context.Background(), dstH, 2048, buf)
	s.Require().Equal(fuse.OK, st)

	// Attr must MISS after the copy (size/mtime moved).
	s.inner.EXPECT().Stat(mock.Anything, "/dst").Return(&io.Attr{Ino: 2, Size: 4096}, fuse.OK).Once()
	_, st = s.b.Stat(context.Background(), "/dst")
	s.Require().Equal(fuse.OK, st)
}

// Lseek and the xattr trio are pure pass-throughs — one delegation test
// each keeps the interface honest without over-testing.
func (s *CachedBackendTestSuite) TestLseekAndXattrPassThrough() {
	h, innerH := s.openCachedHandle("/f")
	s.inner.EXPECT().Lseek(mock.Anything, innerH, uint64(5), uint32(3)).Return(uint64(9), fuse.OK).Once()
	off, st := s.b.Lseek(context.Background(), h, 5, 3)
	s.Require().Equal(fuse.OK, st)
	s.Equal(uint64(9), off)

	s.inner.EXPECT().SetXAttr(mock.Anything, "/f", "user.k", []byte("v"), uint32(0)).Return(fuse.OK).Once()
	s.Equal(fuse.OK, s.b.SetXAttr(context.Background(), "/f", "user.k", []byte("v"), 0))

	s.inner.EXPECT().RemoveXAttr(mock.Anything, "/f", "user.k").Return(fuse.OK).Once()
	s.Equal(fuse.OK, s.b.RemoveXAttr(context.Background(), "/f", "user.k"))

	s.inner.EXPECT().ListXAttr(mock.Anything, "/f").Return([]string{"user.k"}, fuse.OK).Once()
	names, st := s.b.ListXAttr(context.Background(), "/f")
	s.Require().Equal(fuse.OK, st)
	s.Equal([]string{"user.k"}, names)
}
```

- [ ] **Step 6: Run client tests**

Run: `go build ./... && go test ./pkg/client/... -v`
Expected: PASS (node_test still green — mocks regenerated; cache tests pass).

- [ ] **Step 7: Commit**

```bash
git add pkg/client/io/backend.go pkg/client/io/backend_grpc.go pkg/client/cache internal/mocks
git commit -m "feat(client): backend plumbing for copy, lseek, xattr writes

CopyFileRange/Lseek drain the write coalescer before issuing (the server
must see current bytes — same reason Fsync drains). The cache layer
invalidates the copy destination exactly like a Write of that range."
```

---

### Task 6: Client node adapters

**Files:**
- Modify: `pkg/client/io/node.go` (assertions block line 71; methods after Getxattr section line 518 and file-ops section line 539)
- Modify: `pkg/client/io/node_test.go` (append tests)

- [ ] **Step 1: Write the failing tests** (append to `node_test.go`; reuse `s.childNode`, `rootAs`)

```go
// openFileOn opens a file node with a distinct mock handle and returns
// the fs.FileHandle (a *gMountieFile) plus the mock leaf.
func (s *NodeAdapterTestSuite) openFileOn(node fs.InodeEmbedder, path string) (fs.FileHandle, *iomocks.MockFileHandle) {
	mfh := iomocks.NewMockFileHandle(s.T())
	s.backend.EXPECT().Open(mock.Anything, path, uint32(0)).Return(mfh, fuse.OK).Once()
	fh, _, errno := node.(fs.NodeOpener).Open(context.Background(), 0)
	s.Require().Equal(syscall.Errno(0), errno)
	return fh, mfh
}

func (s *NodeAdapterTestSuite) TestNodeCopyFileRange() {
	n1 := s.childNode("f1", 21)
	n2 := s.childNode("f2", 22)
	fh1, m1 := s.openFileOn(n1, "f1")
	fh2, m2 := s.openFileOn(n2, "f2")

	s.backend.EXPECT().CopyFileRange(mock.Anything, m1, uint64(0), m2, uint64(100), uint64(4096), uint64(0)).
		Return(uint64(4096), fuse.OK).Once()
	n, errno := n1.(fs.NodeCopyFileRanger).CopyFileRange(context.Background(), fh1, 0, nil, fh2, 100, 4096, 0)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Equal(uint32(4096), n)
}

func (s *NodeAdapterTestSuite) TestNodeCopyFileRange_ForeignHandle_EBADF() {
	n1 := s.childNode("f3", 23)
	fh1, _ := s.openFileOn(n1, "f3")
	_, errno := n1.(fs.NodeCopyFileRanger).CopyFileRange(context.Background(), fh1, 0, nil, nil, 0, 1, 0)
	s.Equal(syscall.EBADF, errno)
}

func (s *NodeAdapterTestSuite) TestFileLseek() {
	n1 := s.childNode("f4", 24)
	fh1, m1 := s.openFileOn(n1, "f4")
	s.backend.EXPECT().Lseek(mock.Anything, m1, uint64(7), uint32(3) /* SEEK_DATA */).
		Return(uint64(42), fuse.OK).Once()
	off, errno := fh1.(fs.FileLseeker).Lseek(context.Background(), 7, 3)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Equal(uint64(42), off)
}

func (s *NodeAdapterTestSuite) TestSetxattr_RootAndNode() {
	s.backend.EXPECT().SetXAttr(mock.Anything, "", "user.k", []byte("v"), uint32(0)).Return(fuse.OK).Once()
	errno := rootAs[fs.NodeSetxattrer](s).Setxattr(context.Background(), "user.k", []byte("v"), 0)
	s.Equal(syscall.Errno(0), errno)

	child := s.childNode("d", 31)
	s.backend.EXPECT().SetXAttr(mock.Anything, "d", "user.k", []byte("v"), uint32(0)).Return(fuse.OK).Once()
	errno = child.(fs.NodeSetxattrer).Setxattr(context.Background(), "user.k", []byte("v"), 0)
	s.Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRemovexattr() {
	child := s.childNode("d2", 32)
	s.backend.EXPECT().RemoveXAttr(mock.Anything, "d2", "user.k").Return(fuse.OK).Once()
	errno := child.(fs.NodeRemovexattrer).Removexattr(context.Background(), "user.k")
	s.Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestListxattr_FitsAndERANGE() {
	child := s.childNode("d3", 33)
	s.backend.EXPECT().ListXAttr(mock.Anything, "d3").Return([]string{"user.a", "user.b"}, fuse.OK).Twice()

	dest := make([]byte, 64)
	n, errno := child.(fs.NodeListxattrer).Listxattr(context.Background(), dest)
	s.Require().Equal(syscall.Errno(0), errno)
	s.Equal(uint32(14), n) // "user.a\0user.b\0"
	s.Equal([]byte("user.a\x00user.b\x00"), dest[:n])

	small := make([]byte, 4)
	n, errno = child.(fs.NodeListxattrer).Listxattr(context.Background(), small)
	s.Equal(syscall.ERANGE, errno)
	s.Equal(uint32(14), n) // needed size still reported
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./pkg/client/io/ -run NodeAdapterTestSuite -v`
Expected: FAIL — type-assert failures (`does not implement fs.NodeCopyFileRanger`, etc.).

- [ ] **Step 3: Implement** (in `node.go`)

Add to the assertions block:

```go
	_ fs.NodeSetxattrer    = (*gMountieRoot)(nil)
	_ fs.NodeRemovexattrer = (*gMountieRoot)(nil)
	_ fs.NodeListxattrer   = (*gMountieRoot)(nil)

	_ fs.NodeSetxattrer    = (*gMountieNode)(nil)
	_ fs.NodeRemovexattrer = (*gMountieNode)(nil)
	_ fs.NodeListxattrer   = (*gMountieNode)(nil)
	_ fs.NodeCopyFileRanger = (*gMountieNode)(nil)

	_ fs.FileLseeker = (*gMountieFile)(nil)
```

After the Getxattr section:

```go
// --- Setxattr / Removexattr / Listxattr ---

func (r *gMountieRoot) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return syscall.Errno(r.backend.SetXAttr(ctx, "", attr, data, flags))
}

func (n *gMountieNode) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	return syscall.Errno(n.backend.SetXAttr(ctx, n.path(), attr, data, flags))
}

func (r *gMountieRoot) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return syscall.Errno(r.backend.RemoveXAttr(ctx, "", attr))
}

func (n *gMountieNode) Removexattr(ctx context.Context, attr string) syscall.Errno {
	return syscall.Errno(n.backend.RemoveXAttr(ctx, n.path(), attr))
}

func (r *gMountieRoot) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return listxattrAt(ctx, r.backend, "", dest)
}

func (n *gMountieNode) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	return listxattrAt(ctx, n.backend, n.path(), dest)
}

// listxattrAt marshals names into the kernel's NUL-joined buffer format.
// go-fuse contract: if dest is too small, return ERANGE AND the needed
// size so the caller can re-issue with a bigger buffer.
func listxattrAt(ctx context.Context, backend FileSystemBackend, p string, dest []byte) (uint32, syscall.Errno) {
	names, st := backend.ListXAttr(ctx, p)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	sz := 0
	for _, name := range names {
		sz += len(name) + 1
	}
	if sz > len(dest) {
		return uint32(sz), syscall.ERANGE
	}
	off := 0
	for _, name := range names {
		off += copy(dest[off:], name)
		dest[off] = 0
		off++
	}
	return uint32(sz), 0
}
```

In the file-handle ops section (and `// --- CopyFileRange ---` after Rename):

```go
// --- CopyFileRange ---

// CopyFileRange forwards the kernel's copy request to the server so the
// bytes never transit the client. Both handles are ours by construction
// (same bridge ⇒ same mount); a failed assert is EBADF, not EXDEV. The
// reply is capped at the 32-bit width of the FUSE_COPY_FILE_RANGE reply
// (the kernel caps the request below 4 GiB anyway).
func (n *gMountieNode) CopyFileRange(ctx context.Context, fhIn fs.FileHandle, offIn uint64, _ *fs.Inode, fhOut fs.FileHandle, offOut uint64, length uint64, flags uint64) (uint32, syscall.Errno) {
	src, ok := fhIn.(*gMountieFile)
	if !ok {
		return 0, syscall.EBADF
	}
	dst, ok := fhOut.(*gMountieFile)
	if !ok {
		return 0, syscall.EBADF
	}
	copied, st := n.backend.CopyFileRange(ctx, src.fh, offIn, dst.fh, offOut, length, flags)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	if copied > math.MaxUint32 {
		copied = math.MaxUint32
	}
	return uint32(copied), 0
}

func (f *gMountieFile) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	o, st := f.backend.Lseek(ctx, f.fh, off, whence)
	if !st.Ok() {
		return 0, syscall.Errno(st)
	}
	return o, 0
}
```

Add to `node.go` imports: `"math"`.

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/client/io/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/node.go pkg/client/io/node_test.go
git commit -m "feat(client): node adapters for copy_file_range, lseek, xattr writes

gMountieNode gains NodeCopyFileRanger (handle type-asserts, EBADF on
foreign handles), gMountieFile gains FileLseeker, and both node types
gain the xattr write trio with kernel-format Listxattr marshalling."
```

---

### Task 7: End-to-end tests (real mount)

**Files:**
- Create: `test/e2e/fs/copyrange_test.go`

- [ ] **Step 1: Write the e2e suite** (mirror `simple_test.go`'s harness exactly)

```go
package fs

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sys/unix"
)

type CopyRangeE2ESuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func TestCopyRangeE2ESuite(t *testing.T) { suite.Run(t, new(CopyRangeE2ESuite)) }

func (s *CopyRangeE2ESuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(true),
	)
	s.Require().NoError(err)
	utils.Must0(s.T(), ctx.Start())
	s.testAppCtx = ctx
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	ctx.MountVolume(s.volume)
}

func (s *CopyRangeE2ESuite) TearDownSuite() {
	s.Require().NoError(s.testAppCtx.Close())
}

// serverBytesOut reads the server's streaming-Read byte counter for the
// test volume. Only the streaming Read handler increments it
// (controller/file.go BytesAdd(volume, "out")), so a delta of ~0 across a
// copy proves no file data crossed the wire — the spec's acceptance
// criterion, measured deterministically (RPC byte counts, not timing).
func (s *CopyRangeE2ESuite) serverBytesOut() float64 {
	m := s.testAppCtx.GetServerApp().Metrics
	return testutil.ToFloat64(m.Bytes.WithLabelValues(s.volume.Name, "out"))
}

// TestCopyFileRangeSyscall drives copy_file_range(2) directly against the
// mount (no libc/GIO fallback in the way), verifies content fidelity, AND
// asserts the copy was server-side. The latter is the assertion that
// matters: the syscall succeeds even on a broken wiring (the kernel falls
// back to generic_copy_file_range), so success alone proves nothing.
func (s *CopyRangeE2ESuite) TestCopyFileRangeSyscall() {
	mount := s.volume.GetMountPath()
	content := make([]byte, 4<<20)
	_, err := rand.Read(content)
	s.Require().NoError(err)
	srcPath := filepath.Join(mount, "cfr-src.bin")
	dstPath := filepath.Join(mount, "cfr-dst.bin")
	s.Require().NoError(os.WriteFile(srcPath, content, 0o644))

	src, err := os.Open(srcPath)
	s.Require().NoError(err)
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_RDWR|os.O_CREATE, 0o644)
	s.Require().NoError(err)
	defer dst.Close()

	outBefore := s.serverBytesOut()

	var total int
	for total < len(content) {
		n, err := unix.CopyFileRange(int(src.Fd()), nil, int(dst.Fd()), nil, len(content)-total, 0)
		s.Require().NoError(err, "copy_file_range through the mount")
		s.Require().Greater(n, 0)
		total += n
	}

	// Acceptance: server-side copy streams (almost) nothing to the client.
	// A fallback that round-trips the data would show a ~4 MiB delta;
	// allow 64 KiB slack for incidental kernel reads around the copy.
	outAfter := s.serverBytesOut()
	s.Require().Less(outAfter-outBefore, float64(64<<10),
		"copy must not stream file data through the client (delta=%v)", outAfter-outBefore)

	got, err := os.ReadFile(dstPath) // after the metrics check — this read legitimately streams
	s.Require().NoError(err)
	s.Require().True(bytes.Equal(content, got), "destination content must match source")
}

// TestSeekDataHole exercises FUSE_LSEEK through the mount using
// FS-agnostic invariants (hole reporting granularity varies by backing
// filesystem, so don't assert exact hole offsets for punched ranges).
func (s *CopyRangeE2ESuite) TestSeekDataHole() {
	p := filepath.Join(s.volume.GetMountPath(), "sparse.bin")
	s.Require().NoError(os.WriteFile(p, []byte("0123456789"), 0o644))
	f, err := os.Open(p)
	s.Require().NoError(err)
	defer f.Close()

	off, err := unix.Seek(int(f.Fd()), 0, unix.SEEK_DATA)
	s.Require().NoError(err)
	s.Equal(int64(0), off)

	off, err = unix.Seek(int(f.Fd()), 0, unix.SEEK_HOLE)
	s.Require().NoError(err)
	s.GreaterOrEqual(off, int64(10)) // implicit EOF hole

	_, err = unix.Seek(int(f.Fd()), 100, unix.SEEK_DATA)
	s.Require().ErrorIs(err, unix.ENXIO)
}

// TestXattrRoundTrip exercises set/get/list/remove through the mount and
// the server-side namespace policy.
func (s *CopyRangeE2ESuite) TestXattrRoundTrip() {
	p := filepath.Join(s.volume.GetMountPath(), "xattr.txt")
	s.Require().NoError(os.WriteFile(p, []byte("x"), 0o644))

	if err := unix.Setxattr(p, "user.e2e", []byte("v1"), 0); err == unix.ENOTSUP {
		s.T().Skip("backing filesystem has no xattr support")
	} else {
		s.Require().NoError(err)
	}

	buf := make([]byte, 64)
	n, err := unix.Getxattr(p, "user.e2e", buf)
	s.Require().NoError(err)
	s.Equal([]byte("v1"), buf[:n])

	n, err = unix.Listxattr(p, buf)
	s.Require().NoError(err)
	s.Contains(string(buf[:n]), "user.e2e")

	// Policy: trusted.* writes must be rejected by the SERVER (EPERM),
	// regardless of local privileges.
	err = unix.Setxattr(p, "trusted.e2e", []byte("v"), 0)
	s.Require().ErrorIs(err, unix.EPERM)

	s.Require().NoError(unix.Removexattr(p, "user.e2e"))
	_, err = unix.Getxattr(p, "user.e2e", buf)
	s.Require().ErrorIs(err, unix.ENODATA)
}
```

- [ ] **Step 2: Run the e2e suite**

Run: `go test ./test/e2e/fs/ -run CopyRangeE2ESuite -v -count=1`
Expected: PASS. (If the harness needs root/fusermount, run the repo's usual e2e entrypoint — check `task --list` for an `e2e` target and use it with `-run CopyRangeE2ESuite`.)

Notes:
- Add to the suite's imports: `"github.com/prometheus/client_golang/prometheus/testutil"` (client_golang is already a dependency; `AppTestingContext.GetServerApp().Metrics` is the public `*metrics.Metrics` from `pkg/server/app.go:39`).
- The metrics-delta assertion in `TestCopyFileRangeSyscall` IS the spec's "no Read stream reaches the client" acceptance criterion — deterministic (byte counters), not timing-based. Do not weaken it to a plain content check: the syscall succeeds even when the wiring is broken (kernel falls back to a generic in-kernel copy that streams everything).
- `TestSeekDataHole` doubles as the production-chain guard for `RawFdFile`: both `identityBoundFS.Open` (`bound_fs.go:747`) and `resolverBoundFS.Open` (`bound_fs.go:308`) pass the confined FS's file through unwrapped, and if any future wrapper breaks that, server `Lseek` returns ENOTSUP and this test fails loudly (copy would silently lose only reflink, but lseek can't hide).

- [ ] **Step 3: Commit**

```bash
git add test/e2e/fs/copyrange_test.go
git commit -m "test(e2e): copy_file_range, SEEK_DATA/HOLE, xattr round-trip via real mount

Drives the raw syscalls against a mounted volume; xattr policy check
asserts the server rejects trusted.* with EPERM end-to-end."
```

---

### Task 8: Full verification + changelog

- [ ] **Step 1: Full build, vet, and test sweep**

Run: `go build ./... && go vet ./... && go test ./... -count=1`
Expected: all green. Fix anything that surfaces before proceeding.

- [ ] **Step 2: Update `CHANGELOG.md`**

Read the file's existing format first; add entries under the unreleased section for: server-side `copy_file_range` (one RPC instead of streaming bytes through the client), `lseek` SEEK_DATA/SEEK_HOLE, xattr write support with `user.*`/ACL allowlist.

- [ ] **Step 3: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: changelog for server-side copy, lseek, and xattr writes"
```

---

## Self-review checklist (run before handoff)

- Spec coverage: protocol ✅ (Task 1), copy engine + fallback + EINVAL ✅ (Task 2), controllers + events + policy ✅ (Tasks 3–4), client backend + coalescer drain + cache invalidation ✅ (Task 5), node adapters ✅ (Task 6), e2e ✅ (Task 7). Non-goals untouched.
- Type consistency: `serverio.CopyFileRange(src, dst nodefs.File, offIn, offOut, length uint64)` and `serverio.Lseek(f, offset, whence)` used identically in Tasks 2–3; backend signature `CopyFileRange(ctx, fhIn, offIn, fhOut, offOut, length, flags)` identical in Tasks 5–6.
- Known judgment calls an executor must NOT "fix": EINVAL is not a fallback trigger; policy check sits before `BindIdentity`; coalescer drains on both copy handles; `RawFdFile` type-assert failures degrade to the generic loop (copy) / ENOTSUP (lseek).
