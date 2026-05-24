# Compound-for-writes (SP3): WriteAndFlush — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fuse the close-tail `Write`+`Flush` into one `WriteAndFlush` RPC and return the new file's `Attr` from `Create`, cutting a small-file create-write-close from ~7 critical-path RTTs toward ~5.

**Architecture:** A new unary `WriteAndFlush(fd, offset, data) → (written, status, final_attr)` RPC on `RpcFile`; the client's `Flush` drains the coalescer in-memory and sends one `WriteAndFlush` instead of a streaming `Write` + a separate `Flush`. `CreateReply` gains `Attr` so the kernel's post-create `EntryOut` is filled from the reply, dropping the client's post-create `Stat`. RELEASE is untouched (already async).

**Tech Stack:** Go, gRPC/protobuf (`protoc` via `task gen:grpc`), go-fuse, testify suites, mockery (`task gen:mocks`), golangci-lint.

**Spec:** `docs/superpowers/specs/2026-05-25-compound-writes-design.md`

---

## File structure

- `api/proto/file.proto` — add `WriteAndFlush` RPC + `WriteAndFlushRequest`/`WriteAndFlushReply`; add `attributes` to `CreateReply`. Source of the generated stubs.
- `pkg/server/controller/utils.go` — extract a reusable `toProtoAttr` (the attr→`proto.Attr` mapping currently inlined in `fs.go`).
- `pkg/server/controller/file.go` — `WriteAndFlush` handler; populate `CreateReply.Attributes`.
- `pkg/client/io/backend_grpc.go` — `Flush` → one `WriteAndFlush` (+ clean-handle no-op skip); `Create` returns the reply `Attr`.
- Tests live beside each (`*_test.go`, testify suites). `internal/mocks` regenerated.

---

## Task 1: Proto — `WriteAndFlush` + `CreateReply.attributes`

**Files:**
- Modify: `api/proto/file.proto`
- Regenerate: `pkg/proto/file*.pb.go` (via `task gen:grpc`)

- [ ] **Step 1: Add the messages + RPC + CreateReply field**

In `api/proto/file.proto`, add `attributes` to `CreateReply` (note `file.proto`
imports `common.proto`; `Attr` lives in `fs.proto` — add `import "api/proto/fs.proto";`
at the top alongside the existing `common.proto` import):

```proto
message CreateReply {
  uint64 fd = 1;
  int32 status = 2;
  Attr attributes = 3;   // new: lets the client fill EntryOut without a post-create GetAttr
}
```

Add the new messages (after `FlushReply`):

```proto
// WriteAndFlush fuses the deferred coalesced write and the flush into one RPC
// at FUSE FLUSH time (the errno reaches the app's close()). data may be empty
// (a pure flush). Unary: only used for buffers <= WriteCoalesceBytes.
message WriteAndFlushRequest {
  string volume     = 1;
  uint64 fd         = 2;
  int64  offset     = 3;
  bytes  data       = 4;
  string session_id = 5;
}

message WriteAndFlushReply {
  uint32 written    = 1;
  int32  status     = 2;   // write errno if the write failed, else the flush errno
  Attr   final_attr = 3;
}
```

Add to the `RpcFile` service (after `Flush`):

```proto
  rpc WriteAndFlush (WriteAndFlushRequest) returns (WriteAndFlushReply);
```

- [ ] **Step 2: Regenerate stubs**

Run: `task gen:grpc`
Expected: exit 0; `git status` shows `pkg/proto/file.pb.go` and `pkg/proto/file_grpc.pb.go` modified.

- [ ] **Step 3: Verify the generated symbols exist**

Run: `go build ./pkg/proto/ && grep -rl 'WriteAndFlushRequest\|WriteAndFlush(' pkg/proto/`
Expected: builds; `WriteAndFlush` appears in the generated client/server interfaces and `CreateReply.Attributes` exists.

- [ ] **Step 4: Commit**

```bash
git add api/proto/file.proto pkg/proto/
git commit -m "feat(proto): add WriteAndFlush RPC + CreateReply.attributes

Unary WriteAndFlush(fd,offset,data) -> (written,status,final_attr) fuses
the close-tail write+flush; CreateReply carries the new file's Attr."
```

---

## Task 2: Server — `toProtoAttr` helper, `WriteAndFlush` handler, `Create` attrs

**Files:**
- Modify: `pkg/server/controller/utils.go` (add `toProtoAttr`)
- Modify: `pkg/server/controller/fs.go` (use the helper in `GetAttr`/`GetAttrIfChanged`)
- Modify: `pkg/server/controller/file.go` (`WriteAndFlush`, `Create` attrs)
- Test: `pkg/server/controller/file_test.go`

- [ ] **Step 1: Extract `toProtoAttr`**

The attr→`proto.Attr` mapping is inlined in `fs.go` `GetAttr` (and
`GetAttrIfChanged`). Add to `pkg/server/controller/utils.go`:

```go
// toProtoAttr maps a server-side FUSE Attr to the wire Attr, including the
// derived version. nil in → nil out.
func toProtoAttr(a *fuse.Attr) *proto.Attr {
	if a == nil {
		return nil
	}
	return &proto.Attr{
		Ino: a.Ino, Size: a.Size, Blocks: a.Blocks,
		Atime: a.Atime, Mtime: a.Mtime, Ctime: a.Ctime,
		Atimensec: a.Atimensec, Mtimensec: a.Mtimensec, Ctimensec: a.Ctimensec,
		Mode: a.Mode, Nlink: a.Nlink,
		Owner:   &proto.Owner{Uid: a.Uid, Gid: a.Gid},
		Rdev:    a.Rdev, Blksize: a.Blksize, Padding: a.Padding,
		Version: serverio.VersionFromAttr(a),
	}
}
```

(Confirm the server attr type used by `fs.GetAttr` — it returns `*fuse.Attr`;
match the field set already mapped in `fs.go:GetAttr`. Replace the two inlined
mappings in `fs.go` with `toProtoAttr(attr)` to keep it DRY.)

- [ ] **Step 2: Write the failing server test for `WriteAndFlush`**

In `pkg/server/controller/file_test.go` (mirror the existing `RpcFileServerImpl`
test suite + `resolveSession`/`RegisterFile` setup used by the `Write`/`Flush`
tests):

```go
func (s *RpcFileServerSuite) TestWriteAndFlushWritesThenFlushesAndReturnsAttr() {
	// open a file on the test fs, register an fd in the session (mirror the
	// Write/Flush test setup in this file)
	fd := s.registerWritableFile("/waf.txt") // helper used by existing Write tests
	reply, err := s.srv.WriteAndFlush(s.ctx, &proto.WriteAndFlushRequest{
		Volume: s.volume, Fd: fd, Offset: 0, Data: []byte("hello"), SessionId: s.sessionID,
	})
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
	s.Assert().Equal(uint32(5), reply.Written)
	s.Require().NotNil(reply.FinalAttr)
	s.Assert().Equal(uint64(5), reply.FinalAttr.Size)
}

func (s *RpcFileServerSuite) TestWriteAndFlushEmptyDataIsPureFlush() {
	fd := s.registerWritableFile("/waf2.txt")
	reply, err := s.srv.WriteAndFlush(s.ctx, &proto.WriteAndFlushRequest{
		Volume: s.volume, Fd: fd, SessionId: s.sessionID, // no Data
	})
	s.Require().NoError(err)
	s.Assert().Equal(int32(fuse.OK), reply.Status)
	s.Assert().Equal(uint32(0), reply.Written)
	s.Require().NotNil(reply.FinalAttr)
}
```

(If `registerWritableFile` doesn't exist, mirror the fd-registration the
existing `Write`/`Flush`/`Read` tests in this file already do — reuse their
setup verbatim rather than inventing a new harness.)

- [ ] **Step 3: Run to verify it fails**

Run: `go test -run 'RpcFileServerSuite/TestWriteAndFlush' ./pkg/server/controller/`
Expected: FAIL — `WriteAndFlush` undefined on `RpcFileServerImpl`.

- [ ] **Step 4: Implement the handler**

In `pkg/server/controller/file.go`, model resolve+fd-lookup on the existing
`Write`/`Flush` handlers and the attr stat on `versionAfterPath`:

```go
// WriteAndFlush writes data at offset (if any) then flushes the fd, in one RPC.
// A write error short-circuits the flush and is what close() will see. The
// reply carries the post-op Attr so the client needn't re-stat.
func (r *RpcFileServerImpl) WriteAndFlush(ctx context.Context, req *proto.WriteAndFlushRequest) (*proto.WriteAndFlushReply, error) {
	sess, err := resolveSession(r.sessions, req.SessionId)
	if err != nil {
		return nil, err
	}
	entry, ok := sess.GetFile(req.Fd)
	if !ok {
		return &proto.WriteAndFlushReply{Status: int32(fuse.EBADF)}, nil
	}
	fs, err := r.fsService.GetVolumeFileSystem(req.Volume)
	if err != nil {
		return nil, err
	}
	reply := &proto.WriteAndFlushReply{}
	if len(req.Data) > 0 {
		n, st := entry.File.Write(req.Data, req.Offset)
		if st != fuse.OK {
			reply.Status = int32(st)
			return reply, nil // write error: skip flush, surface at close()
		}
		reply.Written = n
	}
	st := entry.File.Flush()
	reply.Status = int32(st)
	// best-effort post-op attr (errors leave final_attr nil — client falls back)
	if attr, gst := fs.GetAttr(entry.Path, createContext(ctx, nil)); gst.Ok() {
		reply.FinalAttr = toProtoAttr(attr)
	}
	if st == fuse.OK {
		ver := r.versionAfterPath(ctx, req.Volume, entry.Path, nil)
		r.bus.Emit(req.Volume, entry.Path, ver, serverio.KindMutated)
	}
	return reply, nil
}
```

(Confirm `entry.Path`/`entry.File` field names against the session entry type
used elsewhere in `file.go`; `entry.File.Write`/`Flush` signatures match the
existing `Write`/`Flush` handlers.)

- [ ] **Step 5: Populate `CreateReply.Attributes`**

In the existing `Create` handler, after a successful `fs.Create` (where it
already calls `versionAfterPath`), stat once and set attrs:

```go
		reply.Fd = sess.RegisterFile(request.Path, file)
		r.metrics.OpenFilesInc(request.Volume, request.SessionId)
		if attr, gst := fs.GetAttr(request.Path, createContext(ctx, request.Caller)); gst.Ok() {
			reply.Attributes = toProtoAttr(attr)
			r.bus.Emit(request.Volume, request.Path, serverio.VersionFromAttr(attr), serverio.KindMutated)
		} else {
			ver := r.versionAfterPath(ctx, request.Volume, request.Path, request.Caller)
			r.bus.Emit(request.Volume, request.Path, ver, serverio.KindMutated)
		}
```

(Keeps the existing bus.Emit; reuses the stat for both attrs and version.)

- [ ] **Step 6: Run server tests**

Run: `go test -count=1 ./pkg/server/controller/`
Expected: `ok` (new WriteAndFlush tests pass; existing Create/Write/Flush tests unaffected).

- [ ] **Step 7: Commit**

```bash
git add pkg/server/controller/
git commit -m "feat(server): WriteAndFlush handler + Create returns Attr

Write-then-flush in one handler (write error skips flush), returns the
post-op Attr; Create populates CreateReply.Attributes from the stat it
already takes. Extract toProtoAttr to DRY the attr->proto mapping."
```

---

## Task 3: Client — `Flush` issues one `WriteAndFlush`

**Files:**
- Modify: `pkg/client/io/backend_grpc.go` (`Flush`)
- Test: `pkg/client/io/backend_grpc_test.go`

- [ ] **Step 1: Write the failing client test**

Model on the existing `backend_grpc_test.go` suite (it builds a `BackendClient`
over a mocked `proto.RpcFileClient` — reuse that harness). Assert that a Flush
after a small buffered write issues exactly one `WriteAndFlush` and no streaming
`Write`/`Flush`:

```go
func (s *BackendGrpcSuite) TestFlushFusesWriteAndFlush() {
	h := s.openWritableHandle("/f") // existing helper / mirror Create+handle setup
	// buffer a small write (below coalesce threshold -> stays in coalescer)
	s.backend.Write(s.ctx, h, 0, []byte("hi"))

	s.fileMock.EXPECT().WriteAndFlush(mock.Anything, mock.MatchedBy(func(r *proto.WriteAndFlushRequest) bool {
		return r.Fd == h.fd && r.Offset == 0 && string(r.Data) == "hi"
	})).Return(&proto.WriteAndFlushReply{Status: int32(fuse.OK), Written: 2}, nil).Once()
	// no separate streaming Write, no separate Flush:
	// (mockery strict mode / AssertExpectations will fail if Write or Flush is called)

	st := s.backend.Flush(s.ctx, h)
	s.Assert().Equal(fuse.OK, st)
}

func (s *BackendGrpcSuite) TestFlushCleanHandleSkipsRPC() {
	h := s.openWritableHandle("/clean")
	// nothing written since last flush -> Flush must issue NO RPC
	st := s.backend.Flush(s.ctx, h)
	s.Assert().Equal(fuse.OK, st)
	// AssertExpectations: WriteAndFlush/Write/Flush never called
}
```

(Use the mock-construction and handle-open helpers already in
`backend_grpc_test.go`; match their naming.)

- [ ] **Step 2: Run to verify it fails**

Run: `go test -run 'BackendGrpcSuite/TestFlush(FusesWriteAndFlush|CleanHandleSkipsRPC)' ./pkg/client/io/`
Expected: FAIL — `Flush` still calls streaming `Write`+`Flush`; `WriteAndFlush` mock unmet.

- [ ] **Step 3: Implement `Flush` over `WriteAndFlush`**

Replace `BackendClient.Flush` (currently `drainCoalescer` → streaming Write, then
`Flush` RPC) with a single `WriteAndFlush`. Add a per-handle `dirtySinceFlush`
bool (set in `Write`, cleared here) for the clean-handle skip:

```go
func (b *BackendClient) Flush(ctx context.Context, fh FileHandle) fuse.Status {
	h := resolveHandle(fh)
	if h == nil {
		return fuse.EBADF
	}
	pending := h.coalescer.Drain() // in-memory, no RPC
	if pending == nil && !h.dirtySinceFlush() {
		return fuse.OK // clean handle: nothing to write, already flushed
	}
	var off int64
	var data []byte
	if pending != nil {
		off, data = pending.Offset, pending.Data
	}
	ctx2, cancel := withIOTimeout(ctx, h.ioTimeout)
	defer cancel()
	res, err := retryableCall(ctx2, "WriteAndFlush", func(ctx context.Context) (*proto.WriteAndFlushReply, error) {
		return h.fileClient.WriteAndFlush(ctx, &proto.WriteAndFlushRequest{
			Volume: h.volume, Fd: h.fd, Offset: off, Data: data, SessionId: h.sessionID,
		})
	})
	if err != nil {
		log.Log.Error("error in call: WriteAndFlush", zap.String("path", h.path), zap.Error(err))
		return fuse.EIO
	}
	h.clearDirty()
	h.applyFinalAttr(res.FinalAttr) // store attrs for the node to read; no-op if nil
	return fuse.Status(res.Status)
}
```

(Add `dirtySinceFlush()`/`clearDirty()` and a `dirty` field to the handle struct
at `backend_grpc.go:897`; set dirty in `Write`. `applyFinalAttr` may cache the
attr on the handle — wiring it into the inode is Task 4's concern; a minimal
store + getter is enough here. If `coalescer` is nil for this handle config,
treat `pending` as nil.)

- [ ] **Step 4: Run client tests**

Run: `go test -count=1 ./pkg/client/io/`
Expected: `ok` (new Flush tests pass; existing Write/Release/Fsync/coalesce tests unaffected — `Fsync` still uses its own drain+`Fsync`).

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/backend_grpc_test.go
git commit -m "perf(client/io): Flush issues one WriteAndFlush, skips clean handles

Drain the coalescer in-memory and fuse the pending write with the flush
into one RPC (was streaming Write + separate Flush = 2 RTTs). A handle
with nothing written since the last flush issues no RPC. Fsync/Release
unchanged."
```

---

## Task 4: Client — `Create` returns `Attr`, node drops the post-create Stat

**Files:**
- Modify: `pkg/client/io/backend_grpc.go` (`Create` returns reply attr)
- Modify: `pkg/client/io/node.go` (`Create` uses the returned attr; drop the extra Stat)
- Test: `pkg/client/io/backend_grpc_test.go`, `pkg/client/io/node_test.go`

- [ ] **Step 1: Write the failing test**

```go
func (s *BackendGrpcSuite) TestCreateReturnsAttrFromReply() {
	s.fileMock.EXPECT().Create(mock.Anything, mock.Anything).Return(&proto.CreateReply{
		Status: int32(fuse.OK), Fd: 7,
		Attributes: &proto.Attr{Ino: 42, Size: 0, Mode: 0o100644},
	}, nil).Once()
	// Create must NOT call GetAttr/Stat to fill the attr:
	_, attr, st := s.backend.Create(s.ctx, "/", "new.txt", uint32(os.O_CREATE|os.O_WRONLY), 0o644)
	s.Require().Equal(fuse.OK, st)
	s.Require().NotNil(attr)
	s.Assert().Equal(uint64(42), attr.Ino)
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test -run 'BackendGrpcSuite/TestCreateReturnsAttrFromReply' ./pkg/client/io/`
Expected: FAIL — `Create` returns `nil` attr today (see the comment at `backend_grpc.go` `Create`).

- [ ] **Step 3: Implement**

In `BackendClient.Create`, build the `*Attr` from `res.Attributes` and return it
(replace the `nil` return + the stale comment):

```go
	var attr *Attr
	if res.Attributes != nil {
		attr = protoAttrToIO(res.Attributes) // reuse the existing reply->Attr mapper used by GetAttr/Lookup
	}
	return h, attr, fuse.OK
```

(Find the existing wire-`proto.Attr` → io-`Attr` mapper the client already uses
in its `GetAttr`/`Lookup` paths and reuse it; do not hand-roll a new mapping.)

In `pkg/client/io/node.go` `Create`: it currently issues a `Stat` after
`backend.Create` to fill `out *fuse.EntryOut` (per the backend comment). Use the
returned `attr` directly when non-nil; only fall back to `Stat` when it's nil.

- [ ] **Step 4: Run tests**

Run: `go test -count=1 ./pkg/client/io/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/io/backend_grpc.go pkg/client/io/node.go pkg/client/io/*_test.go
git commit -m "perf(client/io): Create fills EntryOut from reply Attr, drops post-create Stat"
```

---

## Task 5: Regenerate mocks + full gate

**Files:**
- Regenerate: `internal/mocks/`

- [ ] **Step 1: Regenerate mocks for the new RPC**

Run: `task gen:mocks`
Expected: exit 0; `internal/mocks` now has `WriteAndFlush` on the `RpcFileClient`/`RpcFileServer` mocks. (If the io tests referenced `WriteAndFlush` on the mock before regen, they only compile now — that's expected ordering.)

- [ ] **Step 2: Full lint + test**

Run: `task lint && task test`
Expected: `0 issues`; all non-FUSE packages pass. (The `pkg/client/mount` + `ui` FUSE/GTK packages fail only in a sandbox without FUSE — run on the VM or accept those two as environment-gated, same as the cache work.)

- [ ] **Step 3: Commit**

```bash
git add internal/mocks/
git commit -m "chore(mocks): regenerate for WriteAndFlush"
```

---

## Task 6: VM netem acceptance re-bench (blocking)

**Files:** none (measurement); record results in the spec.

- [ ] **Step 1: Build a static client + server for the VM**

Run: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/gm-waf ./cmd`
(Server changed too — deploy the same binary as both `serve` and `mount`.)

- [ ] **Step 2: Deploy to the kubevirt VM and re-run W1 under netem**

scp `/tmp/gm-waf` to the VM; run it as the server (`gm-waf serve`) and the client
mount (cache enabled, `subscribe_enabled: false`). At ~50ms and ~100ms netem RTT,
run 150× `echo x > f` and record files/s + per-RPC server metrics. **Prefer the
`test/e2e/perf` harness** (`task perf:bench:tcp` / its metadata bench) if it has
merged to `master` and into this branch's base by now; otherwise the ad-hoc
netem bench used in the spec study.

- [ ] **Step 3: Verify acceptance criteria**

- Server metrics: per file, `WriteAndFlush` = 1, separate `Write`+`Flush` = 0, post-create `GetAttr` gone.
- W1 critical-path RTTs/file dropped from ~7 toward ~5.
- W2 (128 MiB streamed write) throughput unchanged.

- [ ] **Step 4: Record + flip spec status**

Edit `docs/superpowers/specs/2026-05-25-compound-writes-design.md`: set status to
`implemented`, append the before/after W1 numbers under acceptance criterion #5.
Commit.

---

## Self-review notes

- **Spec coverage:** Goal 1 (fuse Write+Flush) → Tasks 1,3; Goal 2 (Create/WriteAndFlush return Attr) → Tasks 1,2,4; Goal 3 (no large-write/Release regression) → Task 3 (Fsync/Release untouched) + Task 6 W2 check; Goal 4 (minimal surface) → no general compound anywhere. All acceptance criteria map to a task (1→T3/T6, 2→T2/T4/T6, 3→T3/T6, 4→T2, 5→T6, 6→T5).
- **Type/name consistency:** `toProtoAttr` (T2) reused in T2 handler + Create; `WriteAndFlushRequest/Reply`/`FinalAttr`/`Attributes` (T1) used in T2/T3/T4; handle `dirty`/`dirtySinceFlush()`/`clearDirty()`/`applyFinalAttr` introduced and used within T3; `protoAttrToIO` (T4) flagged as "reuse existing mapper, don't hand-roll".
- **No placeholders:** proto and the server handler are complete; tests are concrete against the existing mock/suite harnesses (with explicit "mirror existing helper X" where a test fixture already exists, per the codebase's established patterns); the two spots that say "confirm field name against existing handler" are verification steps, not deferred work.
