# Identity Phase 1b-2 — Symbolic names + shared-group rendering — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add server-side identity name resolution so `WhoAmI` returns the caller's `user_name` + `group_names`, and so attr returns carry `Owner.user_name` (caller's own) + `Owner.group_name` (groups the caller is in) — and let the client preserve gids for groups the caller is in (no more spurious "nogroup" on shared-group files).

**Architecture:** Each resolver mode populates `service.Identity.UserName` + `GroupNames` (squash: one-time `getent`; static: from the config table; system: `id -un` / `id -nG`). The `WhoAmI` handler maps them to the proto `Identity`. `toProtoAttr` gains the caller's resolved `*Identity` and fills `Owner.user_name` only when `attr.Uid == identity.Uid` and `Owner.group_name` only when `attr.Gid ∈ identity.Gids` (the §3.6 hybrid rule — caller's own user only; group names for groups the caller is in). Attr-returning controller handlers fetch the identity via the (already-exported) `VolumeService.ResolveIdentity` — cheap, served from the per-volume identity TTL cache. Client-side, `IDRewriter.Inbound` is extended so gids in the caller's group set pass through unchanged instead of mapping to `nobody`.

**Tech Stack:** Same as 1b-1. testify suites.

**Reference spec:** `docs/superpowers/specs/2026-05-27-identity-permissions-design.md` §3.5 (Identity message), §3.6 (hybrid display fidelity, **locked**), §3.8 (client rewriting).

**Scope (this plan):** server-side name plumbing end-to-end (resolvers → Identity → WhoAmI → Owner via `toProtoAttr`), plus the small client `Inbound` refinement for shared-group gids. **Out of scope:** name-aware UI display (clients that want server group names beyond raw gids can read `Owner.group_name`; FUSE itself only carries numeric ids), VFS multi-volume rewriter (still deferred), identity refresh on resume.

**Testing notes:** unit testify suites in the sandbox cover resolvers (with the injectable `commandRunner`), `toProtoAttr` (pure), the `Inbound` extension, and the WhoAmI handler with mocked `VolumeService`. The full integration is exercised by the existing `TestIdentityRewriteSuite` (squash) on the VM — extend it to also assert the new `Owner.user_name` field surfaces.

---

## Task 1: Proto — `Owner.user_name`/`group_name` + `Identity.user_name`/`group_names`

**Files:** Modify `api/proto/common.proto` and `api/proto/session.proto`. Regenerate `pkg/proto/` via `task gen:grpc` and `internal/mocks/` via `task gen:mocks`.

- [ ] **Step 1:** In `api/proto/common.proto`, extend `Owner`:
```proto
message Owner {
  uint32 uid        = 1;
  uint32 gid        = 2;
  string user_name  = 3;  // caller's own uid only (hybrid display)
  string group_name = 4;  // groups the caller is a member of
}
```

- [ ] **Step 2:** In `api/proto/session.proto`, extend `Identity` (currently has fields 1–4; the comment already reserves 5 + 6):
```proto
message Identity {
  string principal                = 1;
  uint32 uid                      = 2;
  uint32 primary_gid              = 3;
  repeated uint32 gids            = 4;
  string user_name                = 5;
  map<uint32, string> group_names = 6;
}
```

- [ ] **Step 3:** `task gen:grpc`; then `task gen:mocks`. Verify the generated Go fields exist: `proto.Owner.UserName`, `proto.Owner.GroupName`, `proto.Identity.UserName`, `proto.Identity.GroupNames map[uint32]string`. Build clean.

- [ ] **Step 4:** Commit.
```bash
git add api/proto/common.proto api/proto/session.proto pkg/proto/ internal/mocks/
git commit -m "feat(proto): Owner.user_name/group_name + Identity name fields"
```

---

## Task 2: `service.Identity` carries names

**Files:** Modify `pkg/server/service/identity.go`.

- [ ] **Step 1:** Extend the struct (additive — existing call sites stay valid; the new fields default to empty):
```go
type Identity struct {
	Principal  string
	Uid        uint32
	Gid        uint32
	Gids       []uint32
	Caps       []string
	UserName   string            // populated by resolvers in 1b-2 (empty in 1a/1b-1)
	GroupNames map[uint32]string // gid -> name, for groups the caller is in
}
```

- [ ] **Step 2:** Build clean. Commit.
```bash
git add pkg/server/service/identity.go
git commit -m "feat(server/service): Identity gains UserName + GroupNames"
```

---

## Task 3: Resolver name population — `static` and `system`

**Files:** Modify `pkg/server/service/resolver_static.go`, `pkg/server/service/resolver_system.go`. Tests: extend `resolver_static_test.go`, `resolver_system_test.go`.

The static resolver knows everything from config; the system resolver shells out for names too.

- [ ] **Step 1 (static, failing test).** Extend `StaticResolverSuite` — after the existing "supplementary groups" test, add:
```go
func (s *StaticResolverSuite) TestPopulatesNames() {
	r := NewStaticResolver(s.mapping())
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal("alice", id.UserName)
	s.Equal(map[uint32]string{2000: "developers"}, id.GroupNames)
}
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3:** Implement in `resolver_static.go`. In `Resolve`, set:
```go
	groupNames := map[uint32]string{}
	for _, g := range u.Groups {
		if gid, ok := r.m.Groups[g]; ok {
			groupNames[gid] = g
		}
	}
	return Identity{
		Principal:  principal,
		Uid:        u.Uid, Gid: u.Gid, Gids: gids, Caps: u.Caps,
		UserName:   principal,
		GroupNames: groupNames,
	}, nil
```

- [ ] **Step 4:** PASS. Commit (intermediate ok, but easier to batch with system below).

- [ ] **Step 5 (system, failing test).** Extend `SystemResolverSuite`:
```go
func (s *SystemResolverSuite) TestPopulatesNames() {
	fake := func(_ context.Context, name string, args ...string) ([]byte, error) {
		switch args[0] {
		case "-u":  return []byte("1001\n"), nil
		case "-g":  return []byte("1001\n"), nil
		case "-G":  return []byte("1001 2000\n"), nil
		case "-un": return []byte("alice\n"), nil
		case "-nG": return []byte("alice developers\n"), nil
		}
		return nil, ErrPrincipalNotFound
	}
	r := newSystemResolverWithRunner(fake, time.Second)
	id, err := r.Resolve("alice")
	s.Require().NoError(err)
	s.Equal("alice", id.UserName)
	s.Equal(map[uint32]string{1001: "alice", 2000: "developers"}, id.GroupNames)
}
```

- [ ] **Step 6:** Run → FAIL.

- [ ] **Step 7:** Implement in `resolver_system.go`. After parsing `gids` and before returning Identity:
```go
	// Names: id -un <principal>; id -nG <principal>
	uname, err := r.text(ctx, principal, "-un")
	if err != nil {
		return Identity{}, err
	}
	nGOut, err := r.run(ctx, "id", "-nG", principal)
	if err != nil {
		return Identity{}, mapNotFound(err)
	}
	groupNames := map[uint32]string{}
	for i, name := range strings.Fields(string(nGOut)) {
		if i < len(gids) {
			groupNames[gids[i]] = name
		}
	}
```
and include them in the returned `Identity`. Add the small helper:
```go
func (r *systemResolver) text(ctx context.Context, principal, flag string) (string, error) {
	out, err := r.run(ctx, "id", flag, principal)
	if err != nil {
		return "", mapNotFound(err)
	}
	return strings.TrimSpace(string(out)), nil
}
```

- [ ] **Step 8:** Run both suites → PASS. Build clean.

- [ ] **Step 9:** Commit.
```bash
git add pkg/server/service/resolver_static.go pkg/server/service/resolver_static_test.go pkg/server/service/resolver_system.go pkg/server/service/resolver_system_test.go
git commit -m "feat(server/service): static + system resolvers populate UserName + GroupNames"
```

---

## Task 4: `squash` resolver name population (one-time lookup, injectable)

**Files:** Modify `pkg/server/service/resolver_squash.go`. Test: extend `resolver_squash_test.go`.

The squash uid/gid is fixed at startup, so look up its name once (via the same injectable runner used by `system`).

- [ ] **Step 1 (failing test):**
```go
func (s *SquashResolverSuite) TestPopulatesNamesFromRunner() {
	fake := func(_ context.Context, name string, args ...string) ([]byte, error) {
		// Cheap mapping for the test: getent passwd 1000 -> name; getent group 1000 -> name.
		// Match by argv shape; whitespace-trimmed bytes are returned.
		switch {
		case name == "getent" && args[0] == "passwd" && args[1] == "1000":
			return []byte("appuser:x:1000:1000:App:/:/usr/sbin/nologin\n"), nil
		case name == "getent" && args[0] == "group" && args[1] == "1000":
			return []byte("appgroup:x:1000:\n"), nil
		}
		return nil, nil
	}
	r := newSquashResolverWithRunner(1000, 1000, fake, time.Second)
	id, err := r.Resolve("anyone")
	s.Require().NoError(err)
	s.Equal("appuser", id.UserName)
	s.Equal(map[uint32]string{1000: "appgroup"}, id.GroupNames)
}
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3:** Implement. Mirror the system resolver's runner pattern (extract `commandRunner` to a shared spot if it isn't already package-level — it is in `resolver_system.go`; reuse it). `NewSquashResolver(uid, gid)` keeps its current signature for production and looks names up once via `execRunner` (errors → empty names, log a warn but don't fail). Expose a test-only `newSquashResolverWithRunner(uid, gid, run commandRunner, timeout time.Duration) *squashResolver`. Parse `getent passwd <uid>` and `getent group <gid>` for the first colon-delimited field.

- [ ] **Step 4:** Test → PASS. Commit:
```bash
git add pkg/server/service/resolver_squash.go pkg/server/service/resolver_squash_test.go
git commit -m "feat(server/service): squash resolver looks up names via getent"
```

---

## Task 5: `WhoAmI` handler maps names; `toProtoAttr` fills hybrid Owner names

**Files:** Modify `pkg/server/controller/session.go` (extend the WhoAmI mapping), `pkg/server/controller/utils.go` (extend `toProtoAttr`), and EVERY attr-returning handler in `fs.go`/`file.go` (resolve identity → pass to `toProtoAttr`). Tests: extend `session_test.go` for WhoAmI; new `utils_owner_test.go` for the hybrid rule.

- [ ] **Step 1 (WhoAmI test, append to existing TestWhoAmI):** mock `ResolveIdentity` to return `UserName:"alice"` + `GroupNames: map[uint32]string{2000:"developers"}` and assert the reply mirrors them.

- [ ] **Step 2:** Implement: in `session.go` extend the reply construction to copy `id.UserName`/`id.GroupNames`. Run → PASS.

- [ ] **Step 3 (toProtoAttr test, pure):** new `pkg/server/controller/utils_owner_test.go`:
```go
package controller

import (
	"testing"

	"gmountie/pkg/server/service"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type ToProtoAttrSuite struct{ suite.Suite }

func TestToProtoAttrSuite(t *testing.T) { suite.Run(t, new(ToProtoAttrSuite)) }

func attr(uid, gid uint32) *fuse.Attr {
	a := &fuse.Attr{Mode: 0o644}
	a.Uid = uid
	a.Gid = gid
	return a
}

func (s *ToProtoAttrSuite) id() *service.Identity {
	return &service.Identity{Uid: 1001, Gid: 1001, Gids: []uint32{1001, 2000},
		UserName: "alice", GroupNames: map[uint32]string{1001: "alice", 2000: "developers"}}
}

func (s *ToProtoAttrSuite) TestFillsUserNameForOwnFiles() {
	p := toProtoAttr(attr(1001, 1001), s.id())
	s.Equal("alice", p.Owner.UserName)
	s.Equal("alice", p.Owner.GroupName)
}

func (s *ToProtoAttrSuite) TestUserNameEmptyForOthers() {
	p := toProtoAttr(attr(9999, 2000), s.id())
	s.Equal("", p.Owner.UserName)            // hide other users' names
	s.Equal("developers", p.Owner.GroupName) // group caller IS in
}

func (s *ToProtoAttrSuite) TestGroupNameEmptyWhenNotInGids() {
	p := toProtoAttr(attr(9999, 3000), s.id())
	s.Equal("", p.Owner.UserName)
	s.Equal("", p.Owner.GroupName)
}

func (s *ToProtoAttrSuite) TestNilIdentityYieldsNoNames() {
	p := toProtoAttr(attr(1001, 1001), nil)
	s.Equal("", p.Owner.UserName)
	s.Equal("", p.Owner.GroupName)
}
```

- [ ] **Step 4:** Run → FAIL (signature mismatch).

- [ ] **Step 5:** Implement. Change `toProtoAttr(a *fuse.Attr) *proto.Attr` → `toProtoAttr(a *fuse.Attr, id *service.Identity) *proto.Attr`. After building `Owner{Uid, Gid}`, if `id != nil`:
```go
	owner := &proto.Owner{Uid: a.Uid, Gid: a.Gid}
	if id != nil {
		if a.Uid == id.Uid {
			owner.UserName = id.UserName
		}
		// group_name only if the file's gid is in the caller's group set
		if name, ok := id.GroupNames[a.Gid]; ok {
			for _, g := range id.Gids {
				if g == a.Gid {
					owner.GroupName = name
					break
				}
			}
		}
	}
```

- [ ] **Step 6:** Update every existing `toProtoAttr(attr)` call to `toProtoAttr(attr, id)`. The handlers in `fs.go` (`GetAttr`, `GetAttrIfChanged`, `OpenDir`?, `StatFs`?, `Compound` — grep `grep -n 'toProtoAttr(' pkg/server/controller/`) and `file.go` (`Open`, `Create`) gain an `id, _ := r.fsService.ResolveIdentity(ctx, request.Volume, request.Caller)` immediately after the existing `BindIdentity` call (resolving twice is cheap — the cachedResolver makes the second hit free). On error, pass `nil` (best-effort, never block the attr return). For `GetAttrIfChanged`'s nil-caller path, this is still fine (passthrough degrades to anon — pre-existing spec §11 follow-up).

- [ ] **Step 7:** Run `go test ./pkg/server/controller/ -count=1` → PASS. Commit:
```bash
git add pkg/server/controller/
git commit -m "feat(server/controller): fill Owner user_name/group_name per hybrid display rule"
```

---

## Task 6: Client `IDRewriter` — keep gids in the caller's group set

**Files:** Modify `pkg/client/io/idrewrite.go` and the mounter (`pkg/client/mount/single.go`'s `identityFromProto`) to carry `Gids`. Tests: extend `idrewrite_test.go`.

The current `Inbound` rule maps a non-primary gid to `nobody` even when the file's group is one the caller is a member of. Spec §3.8: a gid in the caller's `Gids` should pass through (numeric — the kernel returns numeric and ls(1) does local /etc/group lookup; if the server's gid isn't local the user sees a number, which is correct).

- [ ] **Step 1 (failing test):** extend `IDRewriteSuite`:
```go
func (s *IDRewriteSuite) TestInboundSharedGroupKeepsGid() {
	// Identity gid=1001 (primary) + 2000 (developers). File owned by alice (1001)
	// but group developers (2000). Inbound: uid -> local (own file), gid stays
	// 2000 (the caller IS in 2000), NOT nobody.
	uid, gid := s.rw().Inbound(1001, 2000)
	s.Equal(uint32(500), uid)
	s.Equal(uint32(2000), gid)
}
```

- [ ] **Step 2:** Run → FAIL.

- [ ] **Step 3:** Implement. In `Inbound`, after the existing primary-gid check, add a fallback:
```go
	switch {
	case gid == r.id.Gid:
		outGID = r.localGID
	default:
		outGID = nobodyID
		for _, g := range r.id.Gids {
			if g == gid {
				outGID = gid // keep as-is for shared groups
				break
			}
		}
	}
```

- [ ] **Step 4:** Run → PASS. Existing tests (`TestInboundOwnFilesMapToLocal`, `TestInboundOtherUserMapsToNobody`) still pass — confirm. (Note `TestInboundOtherUserMapsToNobody` uses gid 9999 which is NOT in `Gids`, so behavior is unchanged.)

- [ ] **Step 5:** No change needed in `identityFromProto` if `Gids` is already carried (it is — see `pkg/client/mount/single.go`). Build + full `pkg/client/io` suite green.

- [ ] **Step 6:** Commit:
```bash
git add pkg/client/io/idrewrite.go pkg/client/io/idrewrite_test.go
git commit -m "feat(client/io): keep gids in the caller's group set (don't map to nobody)"
```

---

## Task 7: VM e2e — extend the squash test to assert `Owner.user_name` surfaces

**Files:** Extend `test/e2e/fs/identity_rewrite_test.go`. The existing squash test already mounts and stats a file; add a second assertion that, via the gRPC layer, `Owner.user_name` is filled. The simplest end-to-end check is to extend `WithSquashVolume` to expose the configured name (via getent on the VM, the squash uid 1000 has a known name) and assert the displayed local user's local lookup matches — or, more directly, add a small server-side log assertion that the resolved identity has a non-empty `UserName` (skipping if `getent passwd 1000` returns nothing on the VM).

Pragmatic minimum: extend the test to also issue a raw RPC `WhoAmI` via the existing client and assert `reply.UserName != ""`. The client has a `Client` interface with `WhoAmI(ctx, volume) (*proto.Identity, error)` — call it after mount, assert the name is set (or skip-if-no-system-name, since the VM's user database determines whether uid 1000 has a name).

- [ ] **Step 1:** Append a `TestSquashWhoAmIReturnsUserName` method to the existing suite that calls `c.client.WhoAmI(ctx, volume.Name)` and asserts `reply.UserName != ""` if the VM has a name for uid 1000 (skip otherwise).

- [ ] **Step 2:** Build → compiles in sandbox; SKIPs as non-root.

- [ ] **Step 3:** Run on the VM as root: `sudo /tmp/fs1b2.test -test.run TestIdentityRewriteSuite -test.v` → PASS.

- [ ] **Step 4:** Commit:
```bash
git add test/e2e/fs/identity_rewrite_test.go
git commit -m "test(e2e): assert WhoAmI surfaces the squash uid's name"
```

---

## Self-Review

**Spec coverage (§ → task):** §3.5 Identity name fields → T1+T2; §3.6 Owner.user_name/group_name + hybrid rule → T1 (proto) + T5 (toProtoAttr); resolver name population → T3 (static/system) + T4 (squash); client shared-group rendering refinement (§3.8 spirit) → T6; e2e proof → T7.

**Type consistency:** `service.Identity.UserName`/`GroupNames` (T2) → `proto.Identity.UserName`/`GroupNames` (T1, T5 WhoAmI) → `proto.Owner.UserName`/`GroupName` (T1, T5 toProtoAttr). `io.Identity` already carries `Gids` from 1b-1; `IDRewriter` only reads `Gids` in T6 (no proto field change needed client-side beyond the existing carry).

**Known acceptable limitations:** `passthrough` mode populates no names (the server has no account lookup for arbitrary wire IDs) — `UserName` stays empty for passthrough, correct per §3.6. `GetAttrIfChanged` still passes nil caller (spec §11 deferred); under name-filling it just gets empty names, fail-safe.
