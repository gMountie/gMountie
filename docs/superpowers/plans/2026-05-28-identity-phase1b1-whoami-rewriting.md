# Identity Phase 1b-1 — WhoAmI + client UID/GID rewriting — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the client learn its server-side identity via a `WhoAmI` RPC, then rewrite UIDs/GIDs at the FUSE boundary so a mapped-mode mount renders like a local filesystem (the caller's own files show as the local user; others as `nobody`).

**Architecture:** Add `WhoAmI(volume, caller) → Identity` to the existing `SessionService`; the server resolves the request's identity with the Phase-1a resolver. The client calls `WhoAmI` once per mount, caches the `Identity` on the per-volume `BackendClient`, and applies an `IDRewriter` (built from that identity + the local mounting uid/gid) at the inbound attr-return points and the outbound `Chown`/`Caller` points. A `raw_ids` mount option installs a no-op rewriter (used for backups / passthrough).

**Tech Stack:** Go 1.26, gRPC (`task gen:grpc`), go-fuse v2 (`pkg/client/io`), testify suites. Module `gmountie`. Server runs as root in prod (enforcement from Phase 1a).

**Reference (historical):** the brainstorm spec that drove this plan has been pruned now that the identity feature has shipped; see `docs/design/identity-and-permissions.md` for the durable as-shipped record.

**Scope (this plan = 1b-1):** WhoAmI RPC + Identity proto (uid/gid/gids — **no names yet**); server resolve; client identity cache; inbound + outbound UID/GID rewriting; `raw_ids`. **Deferred to 1b-2:** symbolic names (`Owner.user_name`/`group_name`, resolver name population, the `toProtoAttr` seam, client name display). **Out of scope:** Phase 2 (confinement), Phase 3 (caps).

**Testing notes:** unit logic (rewriter, resolver, WhoAmI handler with a fake VolumeService, proto round-trips) runs in the sandbox as testify suites. FUSE-mount e2e runs **as root on the kubevirt VM** (`ssh ubuntu@192.168.11.11`); sandbox can't FUSE-mount. Do NOT hand-edit `internal/mocks/` — regen via `task gen:mocks` after interface changes.

---

## Task 1: Proto — `WhoAmI` RPC + `Identity` message

**Files:** Modify `api/proto/session.proto`. Regenerate `pkg/proto/session*.pb.go` via `task gen:grpc`.

- [ ] **Step 1: Add to `api/proto/session.proto`** (the `Caller` type is in `common.proto`, package `gmountie`; import it):

```proto
import "common.proto";

message WhoAmIRequest {
  string volume = 1;
  Caller caller = 2;  // wire identity; used by passthrough mode (advisory in mapped modes)
}

message Identity {
  string principal     = 1;
  uint32 uid           = 2;
  uint32 primary_gid   = 3;
  repeated uint32 gids = 4;
  // user_name (5) + group_names (6) are added in Phase 1b-2.
}
```

Add to `service SessionService`:
```proto
  rpc WhoAmI (WhoAmIRequest) returns (Identity);
```

Confirm `common.proto` is importable (check existing imports in `fs.proto`/`file.proto` for the exact `import` path used — mirror it; the generated Go is the same package `proto`).

- [ ] **Step 2: Regenerate** — `task gen:grpc`. Verify `grep -n 'WhoAmI' pkg/proto/session_grpc.pb.go` shows the new client+server methods and `pkg/proto/session.pb.go` has `WhoAmIRequest`/`Identity`.

- [ ] **Step 3: Regenerate mocks** — `task gen:mocks` (the `SessionServiceClient`/`Server` mocks gain `WhoAmI`). Do not hand-edit `internal/mocks/`.

- [ ] **Step 4: Build** — `go build ./pkg/proto/... ./internal/...` clean.

- [ ] **Step 5: Commit**
```bash
git add api/proto/session.proto pkg/proto/ internal/mocks/
git commit -m "feat(proto): WhoAmI RPC + Identity message on SessionService"
```

---

## Task 2: Server — export `VolumeService.ResolveIdentity`

**Files:** Modify `pkg/server/service/volume.go` (interface + impl). Test: `pkg/server/service/volume_bind_test.go`.

Context: `resolveIdentity(ctx, volume, caller)` is unexported (used by `BindIdentity`). The `WhoAmI` controller needs it. Export a thin wrapper.

- [ ] **Step 1: Write the failing test** (append to `volume_bind_test.go`):

```go
func (s *BindIdentitySuite) TestResolveIdentityExported() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	id, err := svc.ResolveIdentity(context.Background(), "v", nil)
	s.Require().NoError(err)
	s.Equal(uint32(1000), id.Uid)
}
```

- [ ] **Step 2: Run** `go test ./pkg/server/service/ -run TestBindIdentitySuite/TestResolveIdentityExported -v` → FAIL (undefined `ResolveIdentity`).

- [ ] **Step 3: Implement.** Add to the `VolumeService` interface:
```go
	// ResolveIdentity resolves the request's server-side identity for a volume
	// (principal from ctx for mapped modes; wire caller for passthrough).
	ResolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error)
```
And the impl (delegates to the existing unexported method):
```go
func (s *VolumeServiceImpl) ResolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error) {
	return s.resolveIdentity(ctx, volume, caller)
}
```

- [ ] **Step 4: Run** the test → PASS; `go test ./pkg/server/service/ -count=1` green. **Regen mocks** (`VolumeService` gained a method): `task gen:mocks`; `go build ./...` (server) clean.

- [ ] **Step 5: Commit**
```bash
git add pkg/server/service/volume.go pkg/server/service/volume_bind_test.go internal/mocks/
git commit -m "feat(server/service): export ResolveIdentity for the WhoAmI handler"
```

---

## Task 3: Server — `WhoAmI` controller handler

**Files:** Modify `pkg/server/controller/session.go` (add `volSvc` dep + `WhoAmI`), `pkg/server/app.go` (constructor wiring). Test: `pkg/server/controller/session_test.go`.

- [ ] **Step 1: Write the failing test.** In `session_test.go`, with a fake/mock `VolumeService` whose `ResolveIdentity` returns `service.Identity{Principal:"alice",Uid:1001,Gid:1001,Gids:[]uint32{1001,2000}}`, and the principal "alice" on ctx (`principal.WithPrincipal(ctx,"alice")`), assert `WhoAmI(ctx, &proto.WhoAmIRequest{Volume:"v"})` returns `&proto.Identity{Principal:"alice",Uid:1001,PrimaryGid:1001,Gids:[]uint32{1001,2000}}`. (Match the existing session_test harness/mocks; the `VolumeService` mock is in `internal/mocks`.)

```go
func (s *SessionControllerSuite) TestWhoAmI() {
	s.volSvc.EXPECT().ResolveIdentity(mock.Anything, "v", mock.Anything).
		Return(service.Identity{Principal: "alice", Uid: 1001, Gid: 1001, Gids: []uint32{1001, 2000}}, nil)
	ctx := principal.WithPrincipal(context.Background(), "alice")
	reply, err := s.controller.WhoAmI(ctx, &proto.WhoAmIRequest{Volume: "v"})
	s.Require().NoError(err)
	s.Equal("alice", reply.Principal)
	s.Equal(uint32(1001), reply.Uid)
	s.Equal(uint32(1001), reply.PrimaryGid)
	s.ElementsMatch([]uint32{1001, 2000}, reply.Gids)
}
```

- [ ] **Step 2: Run** → FAIL (WhoAmI undefined / constructor mismatch).

- [ ] **Step 3: Implement.** In `session.go`:
  - Add field `volSvc service.VolumeService` to `SessionController`; change `NewSessionController(mgr service.SessionManager, volSvc service.VolumeService)`.
  - Add the handler:
```go
func (c *SessionController) WhoAmI(ctx context.Context, req *proto.WhoAmIRequest) (*proto.Identity, error) {
	id, err := c.volSvc.ResolveIdentity(ctx, req.Volume, req.Caller)
	if err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "whoami: %v", err)
	}
	return &proto.Identity{
		Principal:  id.Principal,
		Uid:        id.Uid,
		PrimaryGid: id.Gid,
		Gids:       id.Gids,
	}, nil
}
```
  (Import `codes`/`status` if not already.) In `app.go:69`, update the call to `controller.NewSessionController(c.SessionManager, c.VolumeService)`.

- [ ] **Step 4: Run** the test → PASS; `go test ./pkg/server/controller/ -count=1`; `go build ./pkg/server/...` clean.

- [ ] **Step 5: Commit**
```bash
git add pkg/server/controller/session.go pkg/server/controller/session_test.go pkg/server/app.go
git commit -m "feat(server/controller): WhoAmI handler resolves the caller's identity"
```

---

## Task 4: Client — call `WhoAmI` at mount + cache the identity

**Files:** Modify `pkg/client/grpc/session.go` (call WhoAmI in `Establish` + `tryReattach`), `pkg/client/grpc/client.go` (expose `Identity()`), `pkg/client/io/backend_grpc.go` (`BackendClient` stores identity). Test: `pkg/client/grpc/session_test.go`.

- [ ] **Step 1: Write the failing test.** With a mock `SessionServiceClient` whose `Create` returns a session id and `WhoAmI` returns `&proto.Identity{Uid:1001,PrimaryGid:1001,Gids:[]uint32{1001}}`, assert that after `Establish(ctx, "v", caller)` the handshake exposes that identity via `Identity()`. (Mirror existing `session_test.go` setup.)

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement.** In `session.go`:
  - `SessionHandshake` gains `volume string` and `identity *proto.Identity`.
  - In `Establish`, after `Create` succeeds, call `resp, err := h.client.WhoAmI(ctx, &proto.WhoAmIRequest{Volume: h.volume, Caller: localCaller()})` and store `h.identity = resp` (on error, log + leave nil — degrade to no rewrite). `localCaller()` builds a `*proto.Caller` from `os.Getuid()/os.Getgid()`.
  - In `tryReattach`, re-fetch `WhoAmI` after a successful resume/recreate.
  - Add `func (h *SessionHandshake) Identity() *proto.Identity { return h.identity }`.
  - Thread the volume name into the handshake (it's mount-scoped). Expose `Client.Identity() *proto.Identity` on `ClientImpl` (delegates to the handshake).
  - In `backend_grpc.go`, `BackendClient` gains `identity *proto.Identity`, set at construction from `client.Identity()`, with accessor `Identity() *proto.Identity`.

- [ ] **Step 4: Run** the test → PASS; `go build ./pkg/client/...` clean; `go test ./pkg/client/grpc/ -count=1`.

- [ ] **Step 5: Commit**
```bash
git add pkg/client/grpc/ pkg/client/io/backend_grpc.go internal/mocks/
git commit -m "feat(client): fetch and cache server identity via WhoAmI at mount"
```

---

## Task 5: Client — the `IDRewriter` (pure logic)

**Files:** Create `pkg/client/io/idrewrite.go`. Test: `pkg/client/io/idrewrite_test.go`.

- [ ] **Step 1: Write the failing test** (testify suite):

```go
package io

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type IDRewriteSuite struct{ suite.Suite }

func TestIDRewriteSuite(t *testing.T) { suite.Run(t, new(IDRewriteSuite)) }

// local user 500:500; server identity 1001:1001 (gids 1001,2000); nobody 65534.
func (s *IDRewriteSuite) rw() *IDRewriter {
	return NewIDRewriter(&Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001, 2000}}, 500, 500)
}

func (s *IDRewriteSuite) TestInboundOwnFilesMapToLocal() {
	uid, gid := s.rw().Inbound(1001, 1001)
	s.Equal(uint32(500), uid)
	s.Equal(uint32(500), gid)
}

func (s *IDRewriteSuite) TestInboundOtherUserMapsToNobody() {
	uid, _ := s.rw().Inbound(1002, 9999)
	s.Equal(uint32(65534), uid)
}

func (s *IDRewriteSuite) TestOutboundLocalMapsToServer() {
	uid, gid := s.rw().Outbound(500, 500)
	s.Equal(uint32(1001), uid)
	s.Equal(uint32(1001), gid)
}

func (s *IDRewriteSuite) TestNilRewriterIsIdentity() {
	var r *IDRewriter // raw_ids / no identity
	uid, gid := r.Inbound(1234, 5678)
	s.Equal(uint32(1234), uid)
	s.Equal(uint32(5678), gid)
}
```

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement** `pkg/client/io/idrewrite.go`:

```go
package io

// nobodyID is the conventional unprivileged display id for files the mounting
// user does not own (and whose group it does not share).
const nobodyID uint32 = 65534

// Identity mirrors the server-resolved identity the client learns via WhoAmI.
type Identity struct {
	Uid  uint32
	Gid  uint32
	Gids []uint32
}

// IDRewriter maps between the server's identity namespace and the local
// mounting user's namespace for display. A nil *IDRewriter is the identity
// transform (used for raw_ids mounts and when WhoAmI returned nothing).
type IDRewriter struct {
	id            *Identity
	localUID      uint32
	localGID      uint32
}

func NewIDRewriter(id *Identity, localUID, localGID uint32) *IDRewriter {
	if id == nil {
		return nil
	}
	return &IDRewriter{id: id, localUID: localUID, localGID: localGID}
}

// Inbound rewrites server uid/gid → local display uid/gid (server→client).
func (r *IDRewriter) Inbound(uid, gid uint32) (uint32, uint32) {
	if r == nil {
		return uid, gid
	}
	outUID, outGID := nobodyID, nobodyID
	if uid == r.id.Uid {
		outUID = r.localUID
	}
	if gid == r.id.Gid {
		outGID = r.localGID
	}
	return outUID, outGID
}

// Outbound rewrites local uid/gid → server uid/gid (client→server), for chown.
// IDs that are not the mounting user map to the value unchanged (the server
// will reject what the principal can't set).
func (r *IDRewriter) Outbound(uid, gid uint32) (uint32, uint32) {
	if r == nil {
		return uid, gid
	}
	if uid == r.localUID {
		uid = r.id.Uid
	}
	if gid == r.localGID {
		gid = r.id.Gid
	}
	return uid, gid
}
```

(`Gids` is carried for 1b-2 group-name display; the inbound rule here is uid/primary-gid only, per §3.8. Group display refinement is 1b-2.)

- [ ] **Step 4: Run** → PASS.

- [ ] **Step 5: Commit**
```bash
git add pkg/client/io/idrewrite.go pkg/client/io/idrewrite_test.go
git commit -m "feat(client/io): IDRewriter — server<->local id mapping (nil = passthrough)"
```

---

## Task 6: Client — apply the rewriter (inbound attrs + outbound chown)

**Files:** Modify `pkg/client/io/node.go` (construct root/node with a rewriter; apply at the 5 `setAttrFromBackend` call sites; apply outbound in `setattrAt` before `Chown`). Test: `pkg/client/io/node_idrewrite_test.go` (or extend existing node tests).

- [ ] **Step 1: Write the failing test.** Using the existing `pkg/client/io` test harness (in-memory/fake backend), mount a node whose backend reports a file owned by the server identity uid (1001) and assert `Getattr` returns the local uid (500) after rewriting; and that a `Setattr` with the local uid chowns to the server uid (1001) on the backend. (Mirror the existing node test style; the fake backend records `Chown` args.)

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement.**
  - `gMountieRoot`/`gMountieNode` gain a `*IDRewriter` (set in `NewMountieRoot`; children inherit the root's). Build it in the mounter from the cached identity + `os.Getuid()/os.Getgid()` (Task 7 threads `raw_ids`).
  - At each `setAttrFromBackend(dst, a)` call site (`lookupAt` node.go:151, `getattrAt` :202, `createAt` :318, `mkdirAt` :356, `setattrAt` trailing stat :266), after `setAttrFromBackend`, apply: `dst.Uid, dst.Gid = rewriter.Inbound(dst.Uid, dst.Gid)`. **Do this in `setAttrFromBackend` itself** by passing the rewriter in — cleaner than 5 call sites: change `setAttrFromBackend(dst *fuse.Attr, a *Attr)` → `setAttrFromBackend(dst *fuse.Attr, a *Attr, rw *IDRewriter)` and rewrite at the end (nil-safe). Update the 5 callers to pass the node's rewriter.
  - In `setattrAt`, before `backend.Chown(ctx, p, uid, gid)`: `uid, gid = rewriter.Outbound(uid, gid)`.

- [ ] **Step 4: Run** the test → PASS; `go test ./pkg/client/io/ -count=1` green.

- [ ] **Step 5: Commit**
```bash
git add pkg/client/io/node.go pkg/client/io/node_idrewrite_test.go
git commit -m "feat(client/io): rewrite ids on attr return and outbound chown"
```

---

## Task 7: Client — `raw_ids` mount option

**Files:** Modify `pkg/client/config/mount.go` (`SingleMountConfig` gains `RawIDs`), `pkg/client/mount/single.go` (thread to `NewMountieRoot`: `raw_ids` ⇒ nil rewriter), and the mount CLI flag in `cmd/commands/mount.go`. Test: `pkg/client/config/mount_test.go`.

- [ ] **Step 1: Write the failing test** — parse a mount config with `raw_ids: true` and assert `SingleMountConfig.RawIDs == true`; default false.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement.**
  - `SingleMountConfig` gains `RawIDs bool \`mapstructure:"raw_ids"\``.
  - `SingleVolumeMounterImpl` gains `rawIDs bool` (constructor option); when building the root, if `rawIDs` OR the cached identity is nil, pass a nil `*IDRewriter`; else `NewIDRewriter(identityFromProto(client.Identity()), uint32(os.Getuid()), uint32(os.Getgid()))`. Add a tiny `identityFromProto(*proto.Identity) *io.Identity` converter.
  - Add a `--raw-ids` bool flag to `cmd/commands/mount.go` mapped to `mount.raw_ids` (default false), documented "expose server-side uids/gids unchanged (backups/admin)".

- [ ] **Step 4: Run** → PASS; `go build ./pkg/client/... ./cmd/...` clean.

- [ ] **Step 5: Commit**
```bash
git add pkg/client/config/mount.go pkg/client/mount/single.go cmd/commands/mount.go pkg/client/config/mount_test.go
git commit -m "feat(client): raw_ids mount option (disable id rewriting)"
```

---

## Task 8: e2e on the VM — mapped-mode local feel

**Files:** Create `test/e2e/fs/identity_rewrite_test.go` (testify suite, runs on the VM as root). Harness: `test/e2e/utils`.

- [ ] **Step 1: Write the test.** Configure a `static`-mode volume mapping principal `alice → uid 1001` and basic auth as alice; mount (no `raw_ids`). Create a file via the mount; `Lstat` it and assert the displayed `Uid` equals the **local** mounting uid (`os.Getuid()`), not 1001 — proving inbound rewrite. Then a second mount with `raw_ids: true` and assert the same file shows `Uid == 1001` (raw). Use `utils.WithBasicAuth` + a static mapping (extend the harness `VolumeConfig` builder to accept a mapping, or add `utils.WithStaticVolume`).

- [ ] **Step 2: Build the test binary on the VM** (`go test -c`), run as root (`sudo ./fs.test -test.run TestIdentityRewrite -test.v`). Expected PASS. (Per the FUSE-test-env memory; sandbox can't mount.)

- [ ] **Step 3: Commit**
```bash
git add test/e2e/fs/identity_rewrite_test.go test/e2e/utils/
git commit -m "test(e2e): mapped-mode mount rewrites ids to the local user; raw_ids shows server ids"
```

---

## Self-Review

**Spec coverage (§ → task):** WhoAmI RPC §3.5 → T1+T3; client identity cache §3.5 → T4; client UID/GID rewriting §3.8 → T5+T6; raw_ids §3.8 → T7; per-volume identity scoping → T4 (identity on per-volume BackendClient); e2e local-feel → T8. **Deferred to 1b-2 (intentional, noted):** symbolic names `Owner.user_name`/`group_name` §3.6, resolver name population, `toProtoAttr` seam, group-name display.

**Type consistency:** `proto.Identity{Principal,Uid,PrimaryGid,Gids}` (T1) used consistently in T3 (server fill) + T4 (client read). `io.Identity{Uid,Gid,Gids}` (T5) is the client-side mirror; `identityFromProto` (T7) converts `proto.Identity`→`io.Identity` (PrimaryGid→Gid). `IDRewriter.Inbound/Outbound` (T5) used in T6. `service.Identity` unchanged in 1b-1 (names are 1b-2). `ResolveIdentity` (T2) consumed by T3.

**Open notes for the implementer:** (1) `WhoAmIRequest.Caller` lets passthrough resolve the wire identity → the client's rewrite becomes a no-op there (server uid == local uid). (2) `GetAttrIfChanged` nil-caller is unchanged (spec §11). (3) For passthrough, the cached identity equals the local uid, so `Inbound`/`Outbound` are no-ops — correct.
