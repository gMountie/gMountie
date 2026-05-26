# Writeback Hardening: Utimens RPC + Close-Tail Error Coverage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `Utimens` RPC end-to-end so `utimensat`/`touch` persists atime/mtime to the server, and add two e2e tests hardening the SP4-A writeback opt-in (timestamp persistence + ENOSPC delivered to the app under writeback).

**Architecture:** Mirror the existing path-based metadata-op pattern (`Chmod`/`Chown`/`Truncate`) at every layer: proto message + `RpcFs` rpc → server controller handler (idempotency + MUTATED emit, delegating to the loopback FS which already implements `Utimens`) → client `BackendClient` (retryable) → `cachedBackend` (invalidate attr) → `FileSystemBackend` interface → FUSE node `setattrAt` dispatch. Timestamps carried as nil-able `FileTime` messages (nil = `UTIME_OMIT`); `UTIME_NOW` is already resolved to a concrete time by go-fuse's `SetAttrIn` getters.

**Tech Stack:** Go, gRPC (`task gen:grpc`), mockery v3 (`task gen:mocks`), go-fuse v2.10.1 (`pathfs.FileSystem`), testify suites. FUSE-mount e2e runs on the kubevirt VM (the sandbox cannot mount FUSE).

**Spec:** `docs/superpowers/specs/2026-05-26-writeback-utimens-hardening-design.md`

**Conventions (from CLAUDE.md + project memory):**
- Module path is `gmountie`; imports are `gmountie/pkg/...`.
- Tests are methods on a testify `suite`, never standalone `func TestX` (except the one `func TestXxxSuite(t) { suite.Run(...) }` runner per suite).
- Never hand-edit `internal/mocks/` or `pkg/proto/` — regenerate with `task gen:mocks` / `task gen:grpc`.
- Errors wrapped with `github.com/pkg/errors`; logging via `gmountie/pkg/utils/log`.
- Commit conventional-commit subject + short body; NO `Co-Authored-By`/`Signed-off-by` trailers.
- For RPC/retry tests, assert the property the fix protects (e.g. ctx non-cancelled), not a guessed error-value shape.

**Run a single unit test suite:** `go test -v -run TestNameSuite ./pkg/<path>/...`
**Run the full unit suite:** `task test`  •  **Lint:** `task lint`
**FUSE e2e:** must run on the VM (root + FUSE). The sandbox/GoLand cannot mount FUSE.

---

## Task 0: VM probe — does the kernel push FATTR_MTIME on writeback flush? (investigative, throwaway)

**Why:** The spec's motivation §2 claims the kernel *may* push `FATTR_MTIME` to the FS on a writeback flush. This is unverified. The probe decides only the spec's wording — the code is identical either way. Do this first so the spec is accurate; it requires nothing from later tasks (the writeback mount already exists on master).

**Files:** none committed. A throwaway log inspection.

- [ ] **Step 1: Build the current client/server and mount with writeback on the VM**

On the VM (FUSE available), start a server over a temp dir and mount a volume with `writeback_cache: true`. Enable go-fuse mount debug so SETATTR ops are logged: in `pkg/client/mount/common.go` the mount sets `Debug: debug` (currently `false`). For the probe only, temporarily flip `debug = true` (or set the FUSE `Debug` option) so the kernel↔client op stream is logged. Do NOT commit this flip.

- [ ] **Step 2: Trigger a writeback flush and capture the SETATTR Valid bits**

```bash
# on the VM, in the mount:
dd if=/dev/zero of=/path/to/mount/probe.bin bs=1M count=4 conv=fsync
# then unmount the client and inspect the FUSE debug log for SETATTR ops
```
Look at the `SETATTR` entries the kernel sent during/after the write+flush. Note whether the `Valid` mask includes `FATTR_MTIME` (0x20) and/or only `FATTR_SIZE` (0x8).

- [ ] **Step 3: Record the finding in the spec**

Edit `docs/superpowers/specs/2026-05-26-writeback-utimens-hardening-design.md` §Motivation point 2:
- If `FATTR_MTIME` IS set on flush: change "*may* push `FATTR_MTIME`" to "pushes `FATTR_MTIME`" and note it was confirmed on Linux <kernel version> on <date>.
- If only `FATTR_SIZE` is set: change point 2 to state the writeback path pushes only size, so the mtime motivation is purely explicit-`utimensat`/`touch` correctness; honoring `FATTR_MTIME` remains correct for explicit ops.

- [ ] **Step 4: Revert the debug flip and commit the spec update only**

```bash
git checkout -- pkg/client/mount/common.go   # discard the debug=true flip
git add docs/superpowers/specs/2026-05-26-writeback-utimens-hardening-design.md
git commit -m "docs(spec): record writeback-flush SETATTR probe result"
```

---

## Task 1: Wire — add the Utimens RPC to the proto

**Files:**
- Modify: `api/proto/fs.proto`
- Regenerate: `pkg/proto/fs.pb.go`, `pkg/proto/fs_grpc.pb.go` (via `task gen:grpc`)
- Regenerate: `internal/mocks/**` (via `task gen:mocks`)

- [ ] **Step 1: Add the messages and the rpc**

In `api/proto/fs.proto`, add these messages (place `FileTime` near the top with `Attr`, and the request/reply near `ChmodRequest`):

```proto
// FileTime is an optional absolute timestamp. Field types match Attr's
// mtime/mtimensec so conversions are lossless. A nil FileTime message means
// UTIME_OMIT — leave that timestamp unchanged.
message FileTime {
  uint64 sec  = 1;
  uint32 nsec = 2;
}

message UtimensRequest {
  string   volume     = 1;
  Caller   caller     = 2;
  string   path       = 3;
  FileTime atime      = 4;   // nil => leave atime unchanged (UTIME_OMIT)
  FileTime mtime      = 5;   // nil => leave mtime unchanged (UTIME_OMIT)
  string   request_id = 6;
  string   session_id = 7;
}

message UtimensReply {
  int32 status = 1;
}
```

In `service RpcFs { ... }`, add (after `Chmod`):

```proto
rpc Utimens (UtimensRequest) returns (UtimensReply);
```

- [ ] **Step 2: Regenerate the gRPC stubs**

Run: `task gen:grpc`
Expected: `pkg/proto/fs.pb.go` gains `FileTime`/`UtimensRequest`/`UtimensReply`; `pkg/proto/fs_grpc.pb.go` gains `Utimens` on `RpcFsClient`, `RpcFsServer`, and `UnimplementedRpcFsServer`.

- [ ] **Step 3: Regenerate mocks**

Run: `task gen:mocks`
Expected: `internal/mocks` `MockRpcFsClient` and `MockRpcFsServer` gain `Utimens`. (`MockFileSystemBackend` does NOT change yet — the interface gains the method in Task 3.)

- [ ] **Step 4: Verify the build compiles**

Run: `go build ./pkg/... ./internal/...`
Expected: success. `RpcServerImpl` still compiles because it embeds `proto.UnimplementedRpcFsServer` (which now supplies a default `Utimens` returning Unimplemented — replaced by the real handler in Task 5).

- [ ] **Step 5: Commit**

```bash
git add api/proto/fs.proto pkg/proto internal/mocks
git commit -m "feat(proto): add Utimens RPC to RpcFs

FileTime + UtimensRequest/UtimensReply and the Utimens rpc, mirroring
the Chmod/Chown path-op shape. nil FileTime = UTIME_OMIT. Regenerated
gRPC stubs and mocks."
```

---

## Task 2: Client — BackendClient.Utimens (gRPC backend)

**Files:**
- Modify: `pkg/client/io/backend_grpc.go` (add `Utimens` + `timeToFileTime`, after `Chown` ~line 438)
- Test: `pkg/client/io/backend_grpc_test.go`

`backend_grpc.go` already imports `context`, `time`, `gmountie/pkg/proto`, `github.com/google/uuid`, `github.com/hanwen/go-fuse/v2/fuse`, `go.uber.org/zap`, and the log pkg — no new imports needed.

- [ ] **Step 1: Write the failing tests**

Add to `pkg/client/io/backend_grpc_test.go` (mirroring `TestStat` / `TestStat_CancelledParentDoesNotAbortRPC`):

```go
func (s *BackendClientTestSuite) TestUtimens() {
	mtime := time.Unix(1577836800, 500) // 2020-01-01, 500ns
	s.fsClient.EXPECT().Utimens(mock.Anything, mock.MatchedBy(func(req *proto.UtimensRequest) bool {
		return req.Volume == "testVolume" && req.Path == "/test" &&
			req.Atime == nil && // UTIME_OMIT
			req.Mtime != nil && req.Mtime.Sec == 1577836800 && req.Mtime.Nsec == 500
	})).Return(&proto.UtimensReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Utimens(context.Background(), "/test", nil, &mtime)
	s.Require().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestUtimens_BothTimes() {
	atime := time.Unix(100, 1)
	mtime := time.Unix(200, 2)
	s.fsClient.EXPECT().Utimens(mock.Anything, mock.MatchedBy(func(req *proto.UtimensRequest) bool {
		return req.Atime != nil && req.Atime.Sec == 100 && req.Atime.Nsec == 1 &&
			req.Mtime != nil && req.Mtime.Sec == 200 && req.Mtime.Nsec == 2
	})).Return(&proto.UtimensReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Utimens(context.Background(), "/test", &atime, &mtime)
	s.Require().Equal(fuse.OK, st)
}

func (s *BackendClientTestSuite) TestUtimens_Error() {
	s.fsClient.EXPECT().Utimens(mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)
	st := s.backend.Utimens(context.Background(), "/test", nil, nil)
	s.Assert().Equal(fuse.EIO, st)
}

// Protective property (see Test protective property memory): a cancelled FUSE
// request ctx must NOT abort the in-flight idempotent metadata RPC. Assert the
// RPC receives a non-cancelled ctx rather than a specific error value.
func (s *BackendClientTestSuite) TestUtimens_CancelledParentDoesNotAbortRPC() {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	s.fsClient.EXPECT().Utimens(
		mock.MatchedBy(func(ctx context.Context) bool { return ctx.Err() == nil }),
		mock.Anything,
	).Return(&proto.UtimensReply{Status: int32(fuse.OK)}, nil)

	st := s.backend.Utimens(parent, "/test", nil, nil)
	s.Require().Equal(fuse.OK, st)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -v -run TestBackendClientTestSuite/TestUtimens ./pkg/client/io/`
Expected: compile error / FAIL — `s.backend.Utimens` undefined.

- [ ] **Step 3: Implement Utimens + timeToFileTime**

In `pkg/client/io/backend_grpc.go`, after `Chown` (~line 438):

```go
// timeToFileTime maps a Go time to the wire FileTime. A nil input yields a
// nil FileTime — UTIME_OMIT (leave that timestamp unchanged).
func timeToFileTime(t *time.Time) *proto.FileTime {
	if t == nil {
		return nil
	}
	return &proto.FileTime{Sec: uint64(t.Unix()), Nsec: uint32(t.Nanosecond())}
}

// Utimens sets atime and/or mtime. A nil pointer leaves that timestamp
// unchanged (UTIME_OMIT).
func (b *BackendClient) Utimens(ctx context.Context, path string, atime, mtime *time.Time) fuse.Status {
	ctx2, cancel := b.metaCtx(ctx)
	defer cancel()
	requestID := uuid.NewString()
	res, err := retryableCall(ctx2, "Utimens", func(ctx context.Context) (*proto.UtimensReply, error) {
		return b.client.Fs().Utimens(ctx, &proto.UtimensRequest{
			Volume:    b.volume,
			Caller:    callerFromCtx(ctx),
			Path:      path,
			Atime:     timeToFileTime(atime),
			Mtime:     timeToFileTime(mtime),
			SessionId: b.client.SessionID(),
			RequestId: requestID,
		})
	})
	if err != nil || res == nil {
		log.Log.Error("error in call: Utimens", zap.String("path", path), zap.Error(err))
		return fuse.EIO
	}
	return fuse.Status(res.Status)
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -v -run TestBackendClientTestSuite/TestUtimens ./pkg/client/io/`
Expected: PASS (all four).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/backend_grpc_test.go
git commit -m "feat(client/io): BackendClient.Utimens over the new RPC

Mirrors Chmod/Chown: retryable metadata call, nil time = UTIME_OMIT
on the wire. Tests cover omit, both-times, error mapping, and the
cancelled-parent-does-not-abort-RPC property."
```

---

## Task 3: Backend interface + cache decorator

**Files:**
- Modify: `pkg/client/io/backend.go` (add `Utimens` to `FileSystemBackend`, after `Chown` ~line 142)
- Modify: `pkg/client/cache/backend.go` (add `cachedBackend.Utimens`)
- Regenerate: `internal/mocks/**` (via `task gen:mocks`) — `MockFileSystemBackend` gains `Utimens`
- Test: `pkg/client/cache/backend_test.go`

- [ ] **Step 1: Add the interface method**

In `pkg/client/io/backend.go`, after the `Chown` line (~142):

```go
	// Utimens sets atime and/or mtime. A nil pointer leaves that timestamp
	// unchanged (UTIME_OMIT semantics).
	Utimens(ctx context.Context, path string, atime, mtime *time.Time) fuse.Status
```

Ensure `backend.go` imports `"time"` (add it if not already present).

- [ ] **Step 2: Implement the cache decorator method**

In `pkg/client/cache/backend.go`, next to `Chmod` (mirroring it exactly):

```go
func (b *cachedBackend) Utimens(ctx context.Context, p string, atime, mtime *time.Time) fuse.Status {
	st := b.inner.Utimens(ctx, p, atime, mtime)
	if st != fuse.OK {
		return st
	}
	b.attr.invalidate(p)
	return fuse.OK
}
```

Ensure `cache/backend.go` imports `"time"` (add if missing).

- [ ] **Step 3: Regenerate mocks**

Run: `task gen:mocks`
Expected: `MockFileSystemBackend` gains `Utimens`. Build now requires it; `BackendClient` already satisfies it (Task 2), `cachedBackend` satisfies it (Step 2).

- [ ] **Step 4: Verify build**

Run: `go build ./pkg/... ./internal/...`
Expected: success.

- [ ] **Step 5: Write the failing cache test**

Add to `pkg/client/cache/backend_test.go` (mirroring `TestChmodInvalidatesAttrOnly`):

```go
func (s *CachedBackendTestSuite) TestUtimensInvalidatesAttrOnly() {
	mtime := time.Unix(1577836800, 0)
	s.b.attr.putPositive("/f", &io.Attr{Mtime: 1})
	s.b.data.put("/f", 0, []byte("DATA"))
	s.inner.EXPECT().Utimens(mock.Anything, "/f", (*time.Time)(nil), &mtime).
		Return(fuse.OK).Once()

	st := s.b.Utimens(context.Background(), "/f", nil, &mtime)
	s.Require().Equal(fuse.OK, st)

	_, hit, _ := s.b.attr.get("/f")
	s.Assert().False(hit)                       // attr invalidated
	s.Assert().NotNil(s.b.data.get("/f", 0))    // data untouched
}

func (s *CachedBackendTestSuite) TestUtimensFailureDoesNotInvalidate() {
	s.b.attr.putPositive("/f", &io.Attr{Mtime: 1})
	s.inner.EXPECT().Utimens(mock.Anything, "/f", mock.Anything, mock.Anything).
		Return(fuse.EPERM).Once()

	st := s.b.Utimens(context.Background(), "/f", nil, nil)
	s.Require().Equal(fuse.EPERM, st)

	_, hit, _ := s.b.attr.get("/f")
	s.Assert().True(hit) // not invalidated on failure
}
```

Ensure `cache/backend_test.go` imports `"time"` (add if missing).

- [ ] **Step 6: Run the cache tests to verify they pass**

Run: `go test -v -run TestCachedBackendTestSuite/TestUtimens ./pkg/client/cache/`
Expected: PASS (both).

- [ ] **Step 7: Commit**

```bash
git add pkg/client/io/backend.go pkg/client/cache/backend.go pkg/client/cache/backend_test.go internal/mocks
git commit -m "feat(client/cache): Utimens in backend interface + cache invalidation

Add Utimens to FileSystemBackend; cachedBackend invalidates the attr
slice on success (mtime change => attr/version change), leaving data
untouched. Regenerated mocks. Tests cover invalidate-on-OK and
no-invalidate-on-failure."
```

---

## Task 4: FUSE node — setattrAt dispatches atime/mtime to Utimens

**Files:**
- Modify: `pkg/client/io/node.go` (`setattrAt`, ~lines 215-255; comment ~215-218)
- Test: `pkg/client/io/node_test.go`

- [ ] **Step 1: Write the failing node tests**

Add to `pkg/client/io/node_test.go` (mirroring `TestRootSetattr_TruncateAndChmod`). The mount root path is `""`:

```go
func (s *NodeAdapterTestSuite) TestRootSetattr_MtimeOnly() {
	// FATTR_MTIME set, FATTR_ATIME unset => Utimens(nil atime, mtime set).
	s.backend.EXPECT().Utimens(
		mock.Anything, "",
		(*time.Time)(nil),
		mock.MatchedBy(func(t *time.Time) bool { return t != nil && t.Unix() == 1577836800 }),
	).Return(fuse.OK)
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Mtime: 1577836800}, fuse.OK,
	)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MTIME
	in.Mtime = 1577836800
	in.Mtimensec = 0
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootSetattr_AtimeAndMtime() {
	s.backend.EXPECT().Utimens(
		mock.Anything, "",
		mock.MatchedBy(func(t *time.Time) bool { return t != nil && t.Unix() == 100 }),
		mock.MatchedBy(func(t *time.Time) bool { return t != nil && t.Unix() == 200 }),
	).Return(fuse.OK)
	s.backend.EXPECT().Stat(mock.Anything, "").Return(&clientio.Attr{Ino: 1}, fuse.OK)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_ATIME | fuse.FATTR_MTIME
	in.Atime = 100
	in.Mtime = 200
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
}

func (s *NodeAdapterTestSuite) TestRootSetattr_NoTimeBitsNoUtimens() {
	// Only mode set => Utimens must NOT be called (no EXPECT() => mockery fails
	// the test if it is called).
	s.backend.EXPECT().Chmod(mock.Anything, "", uint32(0o600)).Return(fuse.OK)
	s.backend.EXPECT().Stat(mock.Anything, "").Return(
		&clientio.Attr{Ino: 1, Mode: fuse.S_IFREG | 0o600}, fuse.OK,
	)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0o600
	out := &fuse.AttrOut{}
	errno := rootAs[fs.NodeSetattrer](s).Setattr(context.Background(), nil, in, out)
	s.Require().Equal(syscall.Errno(0), errno)
}
```

Ensure `node_test.go` imports `"time"` (add if missing).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -v -run TestNodeAdapterTestSuite/TestRootSetattr ./pkg/client/io/`
Expected: FAIL — `TestRootSetattr_MtimeOnly`/`_AtimeAndMtime` fail because `Utimens` is never called (no dispatch yet). `TestRootSetattr_NoTimeBitsNoUtimens` already passes (no time bits) — that's fine.

- [ ] **Step 3: Add the dispatch in setattrAt**

In `pkg/client/io/node.go`, inside `setattrAt`, after the chown block and before the final `Stat` (~line 248):

```go
	atime, aok := in.GetATime()
	mtime, mok := in.GetMTime()
	if aok || mok {
		var ap, mp *time.Time
		if aok {
			ap = &atime
		}
		if mok {
			mp = &mtime
		}
		if st := backend.Utimens(ctx, p, ap, mp); !st.Ok() {
			return syscall.Errno(st)
		}
	}
```

Add `"time"` to `node.go`'s imports.

Replace the stale `setattrAt` doc comment (currently: "The backend has no atime/mtime mutation today, so those bits are silently no-op'd (same as the legacy code).") with:

```go
// setattrAt dispatches on SetAttrIn.Valid flags: size -> Truncate, mode ->
// Chmod, uid/gid -> Chown, atime/mtime -> Utimens. For Chown with only one of
// uid/gid set, we Stat first to read the unset side so we don't overwrite it.
// in.GetATime()/GetMTime() return the resolved concrete time (UTIME_NOW is
// already resolved to time.Now() by go-fuse); a false ok means the bit was
// unset (UTIME_OMIT), so that timestamp is passed as nil and left unchanged.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -v -run TestNodeAdapterTestSuite/TestRootSetattr ./pkg/client/io/`
Expected: PASS (all three, including the existing TruncateAndChmod / ChownPartial).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/node.go pkg/client/io/node_test.go
git commit -m "feat(client/io): honor FATTR_ATIME/MTIME in setattrAt via Utimens

setattrAt no longer drops timestamp bits — it dispatches atime/mtime to
backend.Utimens (nil = UTIME_OMIT; UTIME_NOW resolved by go-fuse).
Updated the stale 'silently no-op'd' comment. Tests cover mtime-only,
atime+mtime, and the no-time-bits path."
```

---

## Task 5: Server — RpcServerImpl.Utimens handler

**Files:**
- Modify: `pkg/server/controller/fs.go` (add `Utimens` handler + `fileTimeToTime`, after `Chown` ~line 257; add `"time"` import)
- Test: `pkg/server/controller/fs_test.go`

- [ ] **Step 1: Write the failing controller test**

Add to `pkg/server/controller/fs_test.go` (mirroring `TestChmod`). The volume FS mock is `pathfs2.MockFileSystem`; its `Utimens(name, *time.Time, *time.Time, *fuse.Context)` exists because the real `pathfs.FileSystem` interface has it:

```go
func (s *RpcServerTestSuite) TestUtimens() {
	mockFs := new(pathfs2.MockFileSystem)
	s.fsService.On("GetVolumeFileSystem", "testVolume").Return(mockFs, nil)
	ctx := context.Background()
	mockFs.EXPECT().Utimens("/test/path", mock.Anything, mock.Anything, mock.Anything).Return(fuse.OK)
	mockFs.EXPECT().GetAttr("/test/path", mock.Anything).Return(&fuse.Attr{}, fuse.OK).Maybe()

	request := &proto.UtimensRequest{
		Volume: "testVolume", Path: "/test/path",
		Mtime:     &proto.FileTime{Sec: 1577836800, Nsec: 0},
		Caller:    CreateCaller(0, 0, 0),
		SessionId: s.sessionID,
		RequestId: "test-req-utimens",
	}
	reply, err := s.server.Utimens(ctx, request)

	s.Require().NoError(err)
	s.Assert().NotNil(reply)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -v -run TestRpcServerTestSuite/TestUtimens ./pkg/server/controller/`
Expected: FAIL — `s.server.Utimens` resolves to the embedded `UnimplementedRpcFsServer.Utimens` (returns `Unimplemented` error), so `err` is non-nil → test fails.

- [ ] **Step 3: Implement the handler + helper**

In `pkg/server/controller/fs.go`, add `"time"` to the imports, then after `Chown` (~line 257):

```go
// fileTimeToTime maps a wire FileTime to a Go time pointer. A nil input yields
// nil (UTIME_OMIT — leave that timestamp unchanged).
func fileTimeToTime(ft *proto.FileTime) *time.Time {
	if ft == nil {
		return nil
	}
	t := time.Unix(int64(ft.Sec), int64(ft.Nsec))
	return &t
}

func (r *RpcServerImpl) Utimens(ctx context.Context, request *proto.UtimensRequest) (*proto.UtimensReply, error) {
	sess, err := resolveSession(r.sessions, request.SessionId)
	if err != nil {
		return nil, err
	}
	fs, err := r.fsService.GetVolumeFileSystem(request.Volume)
	if err != nil {
		return nil, err
	}
	return withIdempotency(sess, request.RequestId, func() (*proto.UtimensReply, error) {
		atime := fileTimeToTime(request.Atime)
		mtime := fileTimeToTime(request.Mtime)
		s := fs.Utimens(request.Path, atime, mtime, createContext(ctx, request.Caller))
		if s == fuse.OK {
			r.bus.Emit(request.Volume, request.Path, r.versionAfter(ctx, fs, request.Path, request.Caller), serverio.KindMutated)
		}
		return &proto.UtimensReply{Status: int32(s)}, nil
	})
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `go test -v -run TestRpcServerTestSuite/TestUtimens ./pkg/server/controller/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/server/controller/fs.go pkg/server/controller/fs_test.go
git commit -m "feat(server/controller): Utimens handler delegating to loopback FS

Mirrors Chmod/Chown: session resolve, idempotency, MUTATED emit on OK.
Converts wire FileTime to *time.Time (nil = UTIME_OMIT) and calls the
volume FS Utimens. Test asserts OK round-trips."
```

---

## Task 6: Full unit suite + lint gate

**Files:** none (verification task).

- [ ] **Step 1: Run the whole unit suite**

Run: `task test`
Expected: PASS, no failures. (If the UI/Wails packages fail to build for lack of `libwebkit2gtk`/`gtk+-3.0`, that is a pre-existing environment limitation unrelated to this change — confirm the failure is only the `ui/` packages and the `gmountie/pkg/...` + `test/...` packages pass.)

- [ ] **Step 2: Lint**

Run: `task lint`
Expected: clean. Fix any `testifylint`/`revive` findings in the new code (e.g. prefer `Require().ErrorIs` over separate `Error`+`ErrorIs`).

- [ ] **Step 3: Commit any lint fixups (if needed)**

```bash
git add -A
git commit -m "chore: lint fixups for Utimens"
```
(Skip if nothing changed.)

---

## Task 7: e2e — Utimens persistence (VM)

**Files:**
- Create: `test/e2e/fs/utimens_test.go`

**Run location:** the kubevirt VM (real FUSE). The sandbox cannot mount FUSE.

**Design:** Write a file through the mount, set its mtime via `os.Chtimes`, then `Stat` the **backing source file directly** (`volume.GetSrcPath()/<name>`, a plain local file). Statting the backing file bypasses gMountie entirely, proving the mtime persisted server-side and is not merely a client cache hit.

- [ ] **Step 1: Write the test**

```go
package fs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	clientConfig "gmountie/pkg/client/config"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// UtimensSuite verifies that utimensat/touch through a gMountie mount persists
// atime/mtime to the server's backing file. It stats the backing source file
// directly (bypassing the mount) so a client-side cache hit cannot mask a
// missing server-side write. Requires a real FUSE mount — runs on the VM.
type UtimensSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	mnt        string
}

func (s *UtimensSuite) SetupSuite() {
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithFUSEConfig(clientConfig.FUSEConfig{
			MaxWriteBytes:  clientConfig.DefaultFUSEMaxWriteBytes,
			MaxBackground:  64,
			WritebackCache: false,
		}),
	)
	if err != nil {
		s.T().Fatal(err)
	}
	utils.Must0(s.T(), testAppCtx.Start())
	s.testAppCtx = testAppCtx
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.testAppCtx.MountVolume(s.volume)
	s.mnt = s.volume.GetMountPath()
}

func (s *UtimensSuite) TearDownSuite() {
	if err := s.testAppCtx.Close(); err != nil {
		s.T().Fatal(err)
	}
}

func (s *UtimensSuite) TestSetMtimePersistsToBackingFile() {
	name := "ut.bin"
	mountPath := filepath.Join(s.mnt, name)
	s.Require().NoError(os.WriteFile(mountPath, []byte("hello"), 0o644))

	want := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s.Require().NoError(os.Chtimes(mountPath, want, want))

	// Stat the backing source file directly — proves the change reached the
	// server, not just the client cache.
	srcPath := filepath.Join(s.volume.GetSrcPath(), name)
	fi, err := os.Stat(srcPath)
	s.Require().NoError(err)
	s.Assert().Equal(want.Unix(), fi.ModTime().Unix())
}

func TestUtimensSuite(t *testing.T) {
	suite.Run(t, new(UtimensSuite))
}
```

- [ ] **Step 2: Run on the VM**

Run (on the VM): `go test -v -run TestUtimensSuite ./test/e2e/fs/`
Expected: PASS — `TestSetMtimePersistsToBackingFile` shows the backing file's mtime equals 2020-01-01.

- [ ] **Step 3: Commit**

```bash
git add test/e2e/fs/utimens_test.go
git commit -m "test(e2e): Utimens persists mtime to the server backing file

Sets mtime via the mount, then stats the backing source file directly
(bypassing gMountie) to prove server-side persistence rather than a
client cache hit. Runs on the VM (real FUSE)."
```

---

## Task 8: e2e — write failure is delivered to the app under writeback (VM)

**Files:**
- Create: `test/e2e/fs/writeback_error_test.go`

**Run location:** the VM. Needs root to mount a small tmpfs as the server's backing dir.

**Design:** Back the volume with a ≈2 MiB tmpfs (`WithExistingVolume`). Mount the client with `writeback_cache: true`. Write well past tmpfs capacity. Assert `ENOSPC` is delivered to the application — at `write()` or at `Close()`. Per the SP4 design and the advisor's note, under writeback the kernel may flush dirty pages mid-write (surfacing ENOSPC at `write()`) or only at close (surfacing at `Close()`); the property under test is that a deferred/cached write failure is **not silently swallowed** but reaches the app on the close-tail flush path. The test accepts either delivery point and fails only if the data silently "succeeded."

- [ ] **Step 1: Write the test**

```go
package fs

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	clientConfig "gmountie/pkg/client/config"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// WritebackErrorSuite verifies that a write failure on the server's backing
// store (here: ENOSPC from a tiny tmpfs) is delivered to the application under
// the writeback page cache rather than being silently lost. The error may
// surface at write() (kernel flushes dirty pages early) or at Close() (the
// close-tail flush). Requires root (tmpfs mount) and real FUSE — runs on the VM.
type WritebackErrorSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
	mnt        string
	tmpfsDir   string
}

func (s *WritebackErrorSuite) SetupSuite() {
	s.tmpfsDir = s.T().TempDir()
	out, err := exec.Command("mount", "-t", "tmpfs", "-o", "size=2m", "tmpfs", s.tmpfsDir).CombinedOutput()
	if err != nil {
		s.T().Skipf("cannot mount tmpfs (need root): %v: %s", err, out)
	}

	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithExistingVolume("wberr", s.tmpfsDir),
		utils.WithFUSEConfig(clientConfig.FUSEConfig{
			MaxWriteBytes:  clientConfig.DefaultFUSEMaxWriteBytes,
			MaxBackground:  64,
			WritebackCache: true,
		}),
	)
	if err != nil {
		_ = exec.Command("umount", s.tmpfsDir).Run()
		s.T().Fatal(err)
	}
	utils.Must0(s.T(), testAppCtx.Start())
	s.testAppCtx = testAppCtx
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.testAppCtx.MountVolume(s.volume)
	s.mnt = s.volume.GetMountPath()
}

func (s *WritebackErrorSuite) TearDownSuite() {
	if s.testAppCtx != nil {
		_ = s.testAppCtx.Close()
	}
	_ = exec.Command("umount", s.tmpfsDir).Run()
}

func (s *WritebackErrorSuite) TestWritePastCapacityDeliversENOSPC() {
	f, err := os.OpenFile(filepath.Join(s.mnt, "big.bin"), os.O_CREATE|os.O_WRONLY, 0o644)
	s.Require().NoError(err)

	// Write 8 MiB into a 2 MiB tmpfs.
	buf := bytes.Repeat([]byte("z"), 1<<20)
	var writeErr error
	for i := 0; i < 8 && writeErr == nil; i++ {
		_, writeErr = f.Write(buf)
	}
	closeErr := f.Close()

	s.Require().True(
		errors.Is(writeErr, syscall.ENOSPC) || errors.Is(closeErr, syscall.ENOSPC),
		"expected ENOSPC delivered at write or close; got write=%v close=%v", writeErr, closeErr,
	)
}

func TestWritebackErrorSuite(t *testing.T) {
	suite.Run(t, new(WritebackErrorSuite))
}
```

- [ ] **Step 2: Run on the VM (as root)**

Run (on the VM, root): `go test -v -run TestWritebackErrorSuite ./test/e2e/fs/`
Expected: PASS — ENOSPC delivered at write or close. If it is SKIPPED, you are not root / tmpfs unavailable; re-run with privileges.

- [ ] **Step 3: Confirm where the error surfaced (informational)**

Note in the PR description whether ENOSPC surfaced at `write()` or `Close()` (the test log prints both). This documents the observed behavior for the close-tail path; no code change required.

- [ ] **Step 4: Commit**

```bash
git add test/e2e/fs/writeback_error_test.go
git commit -m "test(e2e): writeback delivers backing-store ENOSPC to the app

Backs a volume with a 2 MiB tmpfs, writes past capacity under
writeback_cache, and asserts ENOSPC reaches the app at write() or
Close() (not silently swallowed). Skips when not root. Runs on the VM."
```

---

## Final review

After all tasks:
- [ ] Run `task test` (unit) and `task lint` once more — clean.
- [ ] On the VM, run the full fs e2e: `go test -v ./test/e2e/fs/` — `WritebackSuite`, `UtimensSuite`, `WritebackErrorSuite`, and existing suites all pass.
- [ ] Dispatch a final code review over the whole branch.
- [ ] Use superpowers:finishing-a-development-branch to open the PR.

---

## Self-review notes (plan vs spec)

- **Spec coverage:** proto (Task 1), server handler (Task 5), client backend (Task 2), cache (Task 3), interface (Task 3), node dispatch (Task 4), mocks regen (Tasks 1, 3), Utimens persistence e2e (Task 7), error-at-close e2e (Task 8), probe (Task 0). All spec sections mapped.
- **Type consistency:** `Utimens(ctx, path string, atime, mtime *time.Time) fuse.Status` is identical across interface (Task 3), `BackendClient` (Task 2), `cachedBackend` (Task 3), and mock expectations (Tasks 2-4). `FileTime{Sec uint64, Nsec uint32}` and `fileTimeToTime`/`timeToFileTime` are used consistently. Server uses `pathfs.FileSystem.Utimens(name string, *time.Time, *time.Time, *fuse.Context)` (verified present).
- **Deviation from spec, intentional:** the persistence test stats the backing source file directly instead of a sibling mount — simpler and a stronger proof of server-side persistence. The error-at-close test asserts ENOSPC at write-OR-close (non-flaky) rather than close-only, since under writeback the kernel may flush dirty pages before close; the property protected (deferred failure delivered to the app, not swallowed) is preserved. Both noted here for the reviewer.
