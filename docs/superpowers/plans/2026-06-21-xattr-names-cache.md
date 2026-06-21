# xattr-names Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ls -la` fast by eliminating the per-file `ListXAttr` RPC storm — fold per-entry xattr *names* into the `ListDir` reply and serve repeats from a TTL'd client cache.

**Architecture:** The server's readdirplus loop (already stats every entry) also `listxattr`s each entry when the client opts in via a new `with_xattr` request flag. The client primes a new advisory `xattrCache` from that one `ListDir` RPC, so the kernel's subsequent per-file `listxattr` is a local hit. The cache rides the existing attr-cache Subscribe invalidation; it is display-only (ACL enforcement stays server-side kernel-native), so it serves on TTL + invalidation without per-path revalidation.

**Provider note (post-cgofuse-refactor):** Master now has two client FUSE adapters — go-fuse (`pkg/client/io/node.go`, Linux) and cgofuse (`pkg/client/io/cgofs/fs.go`, macOS). Both call the **same** `FileSystemBackend`, and `single.go` builds the backend + cache decorator **once** before `establishMount` picks the provider. So all cache-layer work (Tasks 4–7) and the single mount wiring (Task 8) are provider-agnostic: the prime benefits macOS too (cgofuse `Listxattr` at `cgofs/fs.go:314` calls `backend.ListXAttr`, served from the primed cache). The server (`pkg/server/...`) is Linux-only and was untouched by the refactor — Tasks 1–2 are unaffected. Do **not** add per-provider xattr logic; the caching lives in the decorator.

**Tech Stack:** Go, `github.com/hanwen/go-fuse/v2`, gRPC/protobuf, `go-task`, testify suites, mockery v3.

## Global Constraints

- Module path is `go.gmountie.dev/gmountie` (vanity import).
- Logging via `go.gmountie.dev/gmountie/pkg/utils/log` (`log.Log`); errors wrapped with `github.com/pkg/errors`.
- Tests are methods on a **testify suite**, not standalone `func TestX`.
- `internal/mocks/` is generated — never hand-edit; regenerate with `task gen:mocks`.
- After any `.proto` change run `task gen:grpc`; after any `FileSystemBackend` interface change run `task gen:mocks`.
- No backwards-compat shims — we control both ends; document wire changes in release notes.
- Cannot run `go test ./...` locally (FUSE); run the **union of touched packages** per task. CI (`/dev/fuse` available) is the arbiter for e2e.
- Names-only: do **not** cache `getxattr` values in this plan.

---

## File map

| File | Responsibility | Action |
|---|---|---|
| `api/proto/fs.proto` | add `with_xattr`, `xattr_listed`, `xattr_names` | Modify |
| `pkg/server/io/readdirplus.go` | per-entry `listxattr` in `ReadDirPlus`; `ReadDirPlusser` gains `withXattr` arg | Modify |
| `pkg/server/io/bound_fs.go:471` | thread `withXattr` through `resolverBoundFS.ReadDirPlus` | Modify |
| `pkg/server/controller/fs.go:174` | pass `req.WithXattr`; copy names into proto | Modify |
| `pkg/client/io/backend.go` | `io.DirEntryPlus` gains `XattrNames`/`XattrListed` (struct only) | Modify |
| `pkg/client/io/backend_grpc.go` | `WithXattrListings` option + `xattrListings` field (~line 49-68); set `WithXattr` on request (~line 313); map reply fields (`ListDir` stream loop) | Modify |
| `pkg/client/config/cache.go` | `XAttrTTL` knob + default | Modify |
| `pkg/client/cache/config.go` | `Config.XAttrTTL` + `ConfigFromClient` | Modify |
| `pkg/client/cache/xattr.go` | new `xattrCache` sub-cache | Create |
| `pkg/client/cache/backend.go` | construct `xattr`, read-through `ListXAttr`, prime, local invalidation | Modify |
| `pkg/client/cache/subscriber.go` | `invalidateXAttr` in interface + `invalidatePathAndParent` | Modify |
| `pkg/client/mount/single.go:108` | wire `WithXattrListings(m.cache.Enabled)` | Modify |
| `test/e2e/fs/...` | e2e: 1 ListDir, 0 per-file ListXAttr | Create/Modify |

---

### Task 1: Proto fields + regen

**Files:**
- Modify: `api/proto/fs.proto` (`ReadDirRequest` ~line 177, `DirEntryPlus` ~line 184)
- Regenerated: `pkg/proto/fs.pb.go`, `internal/mocks/`

**Interfaces:**
- Produces: `proto.ReadDirRequest.WithXattr bool`; `proto.DirEntryPlus.XattrListed bool`, `proto.DirEntryPlus.XattrNames []string`.

- [ ] **Step 1: Edit `ReadDirRequest`** — add field 5:

```proto
message ReadDirRequest {
  string volume = 1;
  Caller caller = 2;
  string path   = 3;
  bool   plus   = 4;
  bool   with_xattr = 5;  // ask the server to listxattr each entry (cache-only)
}
```

- [ ] **Step 2: Edit `DirEntryPlus`** — add fields 3 and 4:

```proto
message DirEntryPlus {
  DirEntry entry      = 1;
  Attr     attributes = 2;  // set when plus=true and the per-entry stat succeeded
  // xattr_listed is true when with_xattr was requested AND the per-entry
  // listxattr succeeded. It is the prime signal: proto3 cannot distinguish an
  // empty repeated field (no xattrs) from "not requested", so the client primes
  // its xattr cache only when this bool is set.
  bool             xattr_listed = 3;
  repeated string  xattr_names  = 4;  // entry's xattr names; empty == no xattrs
}
```

- [ ] **Step 3: Regenerate stubs and mocks**

Run: `task gen:grpc && task gen:mocks`
Expected: `pkg/proto/fs.pb.go` now has `GetWithXattr()`, `GetXattrListed()`, `GetXattrNames()`; build clean.

- [ ] **Step 4: Verify it compiles**

Run: `go build ./pkg/proto/... ./api/...`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add api/proto/fs.proto pkg/proto/ internal/mocks/
git commit -m "proto(fs): add with_xattr request flag and per-entry xattr_names to readdir"
```

---

### Task 2: Server — per-entry listxattr in ReadDirPlus

**Files:**
- Modify: `pkg/server/io/readdirplus.go`
- Modify: `pkg/server/io/bound_fs.go` (~line 471)
- Modify: `pkg/server/controller/fs.go` (~line 174)
- Test: `pkg/server/io/readdirplus_test.go`

**Interfaces:**
- Consumes: `proto.ReadDirRequest.WithXattr` (Task 1).
- Produces: `serverio.DirEntryPlus.XattrNames []string`, `serverio.DirEntryPlus.XattrListed bool`; `ReadDirPlusser.ReadDirPlus(name string, withXattr bool, context *fuse.Context) ([]DirEntryPlus, fuse.Status)`.

- [ ] **Step 1: Write the failing test** in `pkg/server/io/readdirplus_test.go` (add to the existing suite). It creates a file with a `user.test` xattr and asserts ReadDirPlus surfaces it:

```go
func (s *ReadDirPlusSuite) TestReadDirPlusWithXattrNames() {
	// s.dir is the volume root used by the suite's fs (see existing setup).
	path := filepath.Join(s.dir, "withxattr.txt")
	s.Require().NoError(os.WriteFile(path, []byte("x"), 0o644))
	s.Require().NoError(unix.Setxattr(path, "user.test", []byte("v"), 0))

	entries, st := s.fs.ReadDirPlus("", true, nil)
	s.Require().Equal(fuse.OK, st)

	var got *DirEntryPlus
	for i := range entries {
		if entries[i].Entry.Name == "withxattr.txt" {
			got = &entries[i]
		}
	}
	s.Require().NotNil(got)
	s.True(got.XattrListed)
	s.Contains(got.XattrNames, "user.test")
}

func (s *ReadDirPlusSuite) TestReadDirPlusWithoutXattrSkipsListing() {
	path := filepath.Join(s.dir, "plain.txt")
	s.Require().NoError(os.WriteFile(path, []byte("x"), 0o644))

	entries, st := s.fs.ReadDirPlus("", false, nil)
	s.Require().Equal(fuse.OK, st)
	for i := range entries {
		s.False(entries[i].XattrListed, "xattr must not be listed when withXattr=false")
		s.Nil(entries[i].XattrNames)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/server/io/ -run ReadDirPlusSuite -v`
Expected: FAIL — `ReadDirPlus` takes 2 args / `XattrListed` undefined.

- [ ] **Step 3: Extend the struct and interface** in `pkg/server/io/readdirplus.go`:

```go
type DirEntryPlus struct {
	Entry       fuse.DirEntry
	Attr        *fuse.Attr
	XattrNames  []string // populated when withXattr and listxattr succeeded
	XattrListed bool     // true == listxattr ran successfully for this entry
}

type ReadDirPlusser interface {
	ReadDirPlus(name string, withXattr bool, context *fuse.Context) ([]DirEntryPlus, fuse.Status)
}
```

- [ ] **Step 4: Implement per-entry listxattr** in `ReadDirPlus` (same file). Change the signature and, inside the entry loop after the `fstatatFn` block, add the xattr fetch. Add the helper below the function:

```go
func (c *ConfinedLoopbackFileSystem) ReadDirPlus(name string, withXattr bool, _ *fuse.Context) ([]DirEntryPlus, fuse.Status) {
	f, entries, st := c.openConfinedDir(name)
	if st != fuse.OK {
		return nil, st
	}
	defer func() { _ = f.Close() }()
	dirFd := int(f.Fd())
	out := make([]DirEntryPlus, 0, len(entries))
	for _, e := range entries {
		d := DirEntryPlus{Entry: fuse.DirEntry{Name: e.Name()}}
		var stat unix.Stat_t
		if serr := fstatatFn(dirFd, e.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); serr == nil {
			a := &fuse.Attr{}
			a.FromStat((*syscall.Stat_t)(unsafe.Pointer(&stat)))
			d.Attr = a
			d.Entry.Mode = stat.Mode
			d.Entry.Ino = stat.Ino
		} else {
			d.Entry.Mode = direntTypeToMode(e.Type())
		}
		if withXattr {
			// Best-effort: a listxattr failure (entry vanished, ENOTSUP) leaves
			// XattrListed=false so the client falls back to a direct ListXAttr.
			if names, ok := listXattrAt(dirFd, e.Name()); ok {
				d.XattrNames = names
				d.XattrListed = true
			}
		}
		out = append(out, d)
	}
	return out, fuse.OK
}

// listXattrAt lists the xattr names of an immediate child of dirFd. The entry
// name never contains a path separator, so an fd-relative openat2 of the child
// cannot escape confinement (same reasoning as the per-entry fstatat). O_PATH +
// O_NOFOLLOW lists the entry's own xattrs (symlinks included), matching the
// AT_SYMLINK_NOFOLLOW stat above. Returns ok=false on any syscall error.
func listXattrAt(dirFd int, name string) ([]string, bool) {
	fd, err := unix.Openat2(dirFd, name, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC,
		Resolve: resolveHow,
	})
	if err != nil {
		return nil, false
	}
	defer func() { _ = unix.Close(fd) }()
	size, err := unix.Listxattr(procFdPath(fd), nil)
	if err != nil {
		return nil, false
	}
	if size == 0 {
		return nil, true // listed successfully; no xattrs
	}
	buf := make([]byte, size)
	n, err := unix.Listxattr(procFdPath(fd), buf)
	if err != nil {
		return nil, false
	}
	return splitNullTerminated(buf[:n]), true
}
```

- [ ] **Step 5: Thread `withXattr` through `resolverBoundFS.ReadDirPlus`** in `pkg/server/io/bound_fs.go:471` — change its signature to `ReadDirPlus(name string, withXattr bool, context *fuse.Context)` and pass `withXattr` to the inner `rdp.ReadDirPlus(name, withXattr, context)` call.

- [ ] **Step 6: Pass the flag and copy names in the controller** in `pkg/server/controller/fs.go:174`. Update the `ReadDirPlusser` call and the proto build:

```go
	if rdp, ok := fs.(serverio.ReadDirPlusser); ok && req.Plus {
		entries, st = rdp.ReadDirPlus(req.Path, req.WithXattr, fctx)
	} else {
```

and inside the batch-build loop, extend the per-entry proto:

```go
			batch.Entries[i] = &proto.DirEntryPlus{
				Entry: &proto.DirEntry{
					Mode: e.Entry.Mode,
					Name: e.Entry.Name,
					Ino:  e.Entry.Ino,
					Off:  e.Entry.Off,
				},
				Attributes:  toProtoAttr(e.Attr, &id),
				XattrListed: e.XattrListed,
				XattrNames:  e.XattrNames,
			}
```

- [ ] **Step 7: Fix the other `ReadDirPlus` call sites in tests** — existing suite calls (`readdirplus_test.go`, `resolver_bound_fs_exec_test.go`) pass two args; add `false` as the `withXattr` arg so they compile.

- [ ] **Step 8: Run the tests**

Run: `go test ./pkg/server/io/ ./pkg/server/controller/ -run 'ReadDirPlus|Fs' -v`
Expected: PASS (new xattr tests + existing readdirplus tests green).

- [ ] **Step 9: Commit**

```bash
git add pkg/server/io/ pkg/server/controller/fs.go
git commit -m "feat(server): listxattr per entry in ReadDirPlus when with_xattr is set"
```

---

### Task 3: Client io layer — request flag + reply mapping

**Files:**
- Modify: `pkg/client/io/backend.go` (`DirEntryPlus` struct ~line 65; `BackendOption` area ~line 49-68)
- Modify: `pkg/client/io/backend_grpc.go` (`ListDir` ~line 288)
- Test: `pkg/client/io/backend_grpc_test.go` (existing suite) or a focused mapping test

**Interfaces:**
- Consumes: `proto.DirEntryPlus.XattrListed/XattrNames`, `proto.ReadDirRequest.WithXattr` (Task 1).
- Produces: `io.DirEntryPlus.XattrNames []string`, `io.DirEntryPlus.XattrListed bool`; `io.WithXattrListings(enabled bool) BackendOption`.

- [ ] **Step 1: Write the failing test** in `pkg/client/io/backend_grpc_test.go`. Drive `ListDir` against a fake `RpcFs` ReadDir stream that returns one entry with `XattrListed:true, XattrNames:["user.a"]`, assert the mapping:

```go
func (s *BackendClientSuite) TestListDirMapsXattrNames() {
	// s.fakeFs is the suite's fake RpcFs; configure its ReadDir stream to yield:
	//   &proto.DirEntryPlus{Entry: &proto.DirEntry{Name: "f"}, XattrListed: true,
	//                       XattrNames: []string{"user.a"}}
	s.fakeFs.setReadDirEntries([]*proto.DirEntryPlus{{
		Entry:       &proto.DirEntry{Name: "f", Mode: 0o644},
		XattrListed: true,
		XattrNames:  []string{"user.a"},
	}})
	entries, st := s.backend.ListDir(s.ctx, "")
	s.Require().Equal(fuse.OK, st)
	s.Require().Len(entries, 1)
	s.True(entries[0].XattrListed)
	s.Equal([]string{"user.a"}, entries[0].XattrNames)
}
```

(Match the suite's existing fake-stream helper; if none exists, follow the pattern already used by the suite's `ListDir`/`ReadDir` tests.)

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/client/io/ -run BackendClientSuite/TestListDirMapsXattrNames -v`
Expected: FAIL — `XattrListed` undefined on `io.DirEntryPlus`.

- [ ] **Step 3: Extend `io.DirEntryPlus`** in `pkg/client/io/backend.go` (the struct lives here, ~line 65):

```go
type DirEntryPlus struct {
	DirEntry
	Attr        *Attr
	XattrNames  []string // entry's xattr names when the server listed them
	XattrListed bool     // true == server ran listxattr for this entry
}
```

- [ ] **Step 4: Add the backend option** in `pkg/client/io/backend_grpc.go` (next to `WithPlusListings`, ~line 62-68 — the option and the `BackendClient` field were moved here by the cgofuse refactor), plus the field on `BackendClient` (~line 51):

```go
// xattrListings makes ListDir request per-entry xattr names (set via
// WithXattrListings; default false). Enabled only when the client cache is on.
xattrListings bool
```

```go
// WithXattrListings makes ListDir ask the server to listxattr each entry so the
// cache can prime per-file xattr names from one ReadDir RPC.
func WithXattrListings(enabled bool) BackendOption {
	return func(b *BackendClient) { b.xattrListings = enabled }
}
```

- [ ] **Step 5: Set the request flag and map the reply** in `pkg/client/io/backend_grpc.go` `ListDir`. In the `&proto.ReadDirRequest{...}` literal add `WithXattr: b.xattrListings,`. In the per-entry append, add the two fields:

```go
				out.entries = append(out.entries, DirEntryPlus{
					DirEntry: DirEntry{
						Ino:  entry.Ino,
						Mode: entry.Mode,
						Name: entry.Name,
					},
					Attr:        attrFromProto(e.GetAttributes()),
					XattrNames:  e.GetXattrNames(),
					XattrListed: e.GetXattrListed(),
				})
```

- [ ] **Step 6: Run the test**

Run: `go test ./pkg/client/io/ -run BackendClientSuite -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/client/io/
git commit -m "feat(client/io): plumb with_xattr request flag and map xattr_names reply"
```

---

### Task 4: Config knob `xattr_ttl`

**Files:**
- Modify: `pkg/client/config/cache.go`
- Modify: `pkg/client/cache/config.go`
- Test: `pkg/client/config/cache_test.go` (`CacheConfigSuite`)

**Interfaces:**
- Produces: `clientconfig.CacheConfig.XAttrTTL time.Duration` (default 5m); `cache.Config.XAttrTTL time.Duration` (mapped in `ConfigFromClient`); `DefaultCacheXAttrTTL`.

- [ ] **Step 1: Write the failing test** in `pkg/client/config/cache_test.go`:

```go
func (s *CacheConfigSuite) TestXAttrTTLDefault() {
	c, err := NewCacheConfig(nil)
	s.Require().NoError(err)
	s.Equal(5*time.Minute, c.XAttrTTL)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/client/config/ -run CacheConfigSuite/TestXAttrTTLDefault -v`
Expected: FAIL — `XAttrTTL` undefined.

- [ ] **Step 3: Add the default + field + viper default** in `pkg/client/config/cache.go`:

In the `const` block:
```go
	// DefaultCacheXAttrTTL is the per-entry lifetime for cached xattr-name
	// lists. Advisory/display-only (ACL enforcement is server-side), so it
	// mirrors the attr TTL and TTL+invalidation are the only freshness signals.
	DefaultCacheXAttrTTL = 5 * time.Minute
```

In `CacheConfig`:
```go
	// XAttrTTL is the per-entry lifetime for cached xattr-name lists. Zero
	// disables time-based expiry for this tier.
	XAttrTTL time.Duration `mapstructure:"xattr_ttl"`
```

In `defaultCacheConfig()`: add `XAttrTTL: DefaultCacheXAttrTTL,`.
In `NewCacheConfig`'s SetDefault block: add `v.SetDefault("xattr_ttl", DefaultCacheXAttrTTL)`.

- [ ] **Step 4: Map it into the runtime `cache.Config`** in `pkg/client/cache/config.go` — add `XAttrTTL time.Duration` to `Config` and `XAttrTTL: cfg.XAttrTTL,` to `ConfigFromClient`.

- [ ] **Step 5: Run the test**

Run: `go test ./pkg/client/config/ -run CacheConfigSuite -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/client/config/cache.go pkg/client/cache/config.go
git commit -m "feat(config): add cache.xattr_ttl (default 5m)"
```

---

### Task 5: `xattrCache` sub-cache

**Files:**
- Create: `pkg/client/cache/xattr.go`
- Test: `pkg/client/cache/xattr_test.go`

**Interfaces:**
- Consumes: `newAccountant`, `newStore` (existing); `cache.Config.XAttrTTL` (Task 4).
- Produces: `newXAttrCache(acct *accountant, ttl time.Duration, now func() time.Time) *xattrCache`; methods `get(path) ([]string, bool)`, `put(path string, names []string)`, `invalidate(path string)`.

- [ ] **Step 1: Write the failing test** in `pkg/client/cache/xattr_test.go`:

```go
package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type XAttrCacheSuite struct{ suite.Suite }

func TestXAttrCacheSuite(t *testing.T) { suite.Run(t, new(XAttrCacheSuite)) }

func (s *XAttrCacheSuite) newCache(ttl time.Duration, now func() time.Time) *xattrCache {
	return newXAttrCache(newAccountant(0, 0), ttl, now)
}

func (s *XAttrCacheSuite) TestPutGetHit() {
	c := s.newCache(time.Minute, nil)
	c.put("a/b", []string{"user.x"})
	got, hit := c.get("a/b")
	s.True(hit)
	s.Equal([]string{"user.x"}, got)
}

func (s *XAttrCacheSuite) TestEmptyListIsPositiveHit() {
	c := s.newCache(time.Minute, nil)
	c.put("a/b", []string{}) // "no xattrs" is a cacheable fact
	got, hit := c.get("a/b")
	s.True(hit)
	s.Empty(got)
}

func (s *XAttrCacheSuite) TestMiss() {
	c := s.newCache(time.Minute, nil)
	_, hit := c.get("nope")
	s.False(hit)
}

func (s *XAttrCacheSuite) TestTTLExpiry() {
	t0 := time.Unix(1000, 0)
	cur := t0
	c := s.newCache(time.Minute, func() time.Time { return cur })
	c.put("a/b", []string{"user.x"})
	cur = t0.Add(2 * time.Minute)
	_, hit := c.get("a/b")
	s.False(hit)
}

func (s *XAttrCacheSuite) TestZeroTTLNeverExpiresOnTime() {
	t0 := time.Unix(1000, 0)
	cur := t0
	c := s.newCache(0, func() time.Time { return cur })
	c.put("a/b", []string{"user.x"})
	cur = t0.Add(99 * time.Hour)
	_, hit := c.get("a/b")
	s.True(hit)
}

func (s *XAttrCacheSuite) TestInvalidateDrops() {
	c := s.newCache(time.Minute, nil)
	c.put("a/b", []string{"user.x"})
	c.invalidate("a/b")
	_, hit := c.get("a/b")
	s.False(hit)
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/client/cache/ -run XAttrCacheSuite -v`
Expected: FAIL — `newXAttrCache` undefined.

- [ ] **Step 3: Implement `xattr.go`** (mirrors `dir.go`, memory-only — names are cheap to re-prime, so no persist tier in v1):

```go
package cache

import (
	"time"
)

// xattrEntry is the value stored in xattrCache. An empty (non-nil) names
// slice is a valid positive entry meaning "this path has no xattrs".
type xattrEntry struct {
	names     []string
	expiresAt time.Time
}

// xattrCache is a TTL wrapper over a store holding per-path xattr-name lists.
// It is advisory/display-only (ACL enforcement is server-side), so it carries
// no negative entries and no validity-tracker gating — TTL plus explicit/Subscribe
// invalidation are the only freshness signals.
type xattrCache struct {
	st  *store
	now func() time.Time
	ttl time.Duration
}

func newXAttrCache(acct *accountant, ttl time.Duration, now func() time.Time) *xattrCache {
	if now == nil {
		now = time.Now
	}
	return &xattrCache{st: newStore(acct, "xattr"), now: now, ttl: ttl}
}

// get returns (names, true) on a fresh hit, (nil, false) on miss or expiry.
// The returned slice is a copy; callers may not mutate the cached view.
func (c *xattrCache) get(path string) ([]string, bool) {
	e := c.st.get(path)
	if e == nil {
		return nil, false
	}
	xe, _ := e.value.(*xattrEntry) // xattr store only holds *xattrEntry
	// ttl=0 means "never expire on time alone"; invalidation is the only signal.
	if c.ttl > 0 && c.now().After(xe.expiresAt) {
		c.st.remove(path)
		return nil, false
	}
	out := make([]string, len(xe.names))
	copy(out, xe.names)
	return out, true
}

// put stores names for path (copying the slice). A nil names slice is stored
// as an empty list so it still reads back as a positive "no xattrs" hit.
func (c *xattrCache) put(path string, names []string) {
	stored := make([]string, len(names))
	copy(stored, names)
	xe := &xattrEntry{names: stored, expiresAt: c.now().Add(c.ttl)}
	c.st.put(path, xe, xattrEntrySize(path, xe))
}

func (c *xattrCache) invalidate(path string) { c.st.remove(path) }

// xattrEntrySize estimates the in-memory footprint: the path (stored twice —
// map key + entry key copy) plus struct overhead plus each name's bytes.
func xattrEntrySize(path string, xe *xattrEntry) int {
	n := 2*len(path) + 64
	for i := range xe.names {
		n += len(xe.names[i]) + 16
	}
	return n
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./pkg/client/cache/ -run XAttrCacheSuite -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/client/cache/xattr.go pkg/client/cache/xattr_test.go
git commit -m "feat(cache): add advisory TTL'd xattrCache sub-cache"
```

---

### Task 6: Wire xattrCache into cachedBackend (read-through + prime + local invalidation)

**Files:**
- Modify: `pkg/client/cache/backend.go`
- Test: `pkg/client/cache/backend_test.go` (existing suite)

**Interfaces:**
- Consumes: `xattrCache` (Task 5); `cache.Config.XAttrTTL` (Task 4); `io.DirEntryPlus.XattrNames/XattrListed` (Task 3).
- Produces: read-through `cachedBackend.ListXAttr`; prime in `listDirFromInner`; `xattr.invalidate` on `SetXAttr`/`RemoveXAttr`.

- [ ] **Step 1: Write the failing tests** in `pkg/client/cache/backend_test.go`. Use the suite's existing mock `io.FileSystemBackend` (the suite already builds a `cachedBackend` around one):

```go
func (s *BackendSuite) TestListXAttrServesFromCacheAfterFirstCall() {
	s.inner.On("ListXAttr", mock.Anything, "f").Return([]string{"user.a"}, fuse.OK).Once()
	n1, st1 := s.b.ListXAttr(s.ctx, "f")
	s.Require().Equal(fuse.OK, st1)
	s.Equal([]string{"user.a"}, n1)
	// Second call must NOT hit inner (Once() above would fail on a 2nd call).
	n2, st2 := s.b.ListXAttr(s.ctx, "f")
	s.Require().Equal(fuse.OK, st2)
	s.Equal([]string{"user.a"}, n2)
	s.inner.AssertExpectations(s.T())
}

func (s *BackendSuite) TestListDirPrimesXattrCache() {
	s.inner.On("ListDir", mock.Anything, "d").Return([]io.DirEntryPlus{{
		DirEntry:    io.DirEntry{Name: "child"},
		XattrListed: true,
		XattrNames:  []string{"user.k"},
	}}, fuse.OK).Once()
	_, st := s.b.ListDir(s.ctx, "d")
	s.Require().Equal(fuse.OK, st)
	// ListXAttr on the primed child must be served from cache (no inner call).
	names, xst := s.b.ListXAttr(s.ctx, "d/child")
	s.Require().Equal(fuse.OK, xst)
	s.Equal([]string{"user.k"}, names)
	s.inner.AssertExpectations(s.T()) // no inner.ListXAttr expectation set → fails if called
}

func (s *BackendSuite) TestSetXAttrInvalidatesXattrAndAttr() {
	// Prime both caches.
	s.inner.On("ListXAttr", mock.Anything, "f").Return([]string{"user.a"}, fuse.OK).Twice()
	_, _ = s.b.ListXAttr(s.ctx, "f")
	s.inner.On("SetXAttr", mock.Anything, "f", "user.b", []byte("v"), uint32(0)).Return(fuse.OK).Once()
	s.Require().Equal(fuse.OK, s.b.SetXAttr(s.ctx, "f", "user.b", []byte("v"), 0))
	// Next ListXAttr must re-hit inner (Twice() allows the second call).
	_, _ = s.b.ListXAttr(s.ctx, "f")
	s.inner.AssertExpectations(s.T())
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./pkg/client/cache/ -run BackendSuite -v`
Expected: FAIL — `ListXAttr` still passes through; no `xattr` field on `cachedBackend`.

- [ ] **Step 3: Add the field and construct it.** In the `cachedBackend` struct (top of `backend.go`) add `xattr *xattrCache`. In `NewCachedBackend`, after the `data:` line, add:

```go
		xattr:    newXAttrCache(acct, cfg.XAttrTTL, nil),
```

- [ ] **Step 4: Replace the `ListXAttr` pass-through** (read-through, no validity gating — advisory cache):

```go
// ListXAttr is read-through against the advisory xattr cache. It serves on
// TTL + invalidation only (no GetAttrIfChanged revalidation): a stale names
// list is at worst a wrong ls "+" indicator, never an enforcement decision.
func (b *cachedBackend) ListXAttr(ctx context.Context, p string) ([]string, fuse.Status) {
	if names, hit := b.xattr.get(p); hit {
		return names, fuse.OK
	}
	names, st := b.inner.ListXAttr(ctx, p)
	if st == fuse.OK {
		b.xattr.put(p, names)
	}
	return names, st
}
```

- [ ] **Step 5: Prime in `listDirFromInner`.** In the existing entry loop that strips entries and primes the attr cache, add the xattr prime:

```go
		for i, e := range entries {
			stripped[i] = e.DirEntry
			if e.Attr != nil {
				b.attr.putPositive(joinPath(p, e.Name), e.Attr)
			}
			if e.XattrListed {
				// Cache the names (empty == "no xattrs"), so the kernel's per-file
				// listxattr after this readdir is a local hit — the cold-pass win.
				b.xattr.put(joinPath(p, e.Name), e.XattrNames)
			}
		}
```

- [ ] **Step 6: Invalidate on local mutation.** Update `SetXAttr` and `RemoveXAttr` (currently pure pass-throughs):

```go
// SetXAttr stores an extended attribute, then drops the cached names list and
// the attr entry: an xattr write bumps the inode ctime, so the cached attr
// version is now stale too.
func (b *cachedBackend) SetXAttr(ctx context.Context, p, attr string, data []byte, flags uint32) fuse.Status {
	st := b.inner.SetXAttr(ctx, p, attr, data, flags)
	if st == fuse.OK {
		b.xattr.invalidate(p)
		b.attr.invalidate(p)
	}
	return st
}

// RemoveXAttr deletes an extended attribute; same invalidation as SetXAttr.
func (b *cachedBackend) RemoveXAttr(ctx context.Context, p, attr string) fuse.Status {
	st := b.inner.RemoveXAttr(ctx, p, attr)
	if st == fuse.OK {
		b.xattr.invalidate(p)
		b.attr.invalidate(p)
	}
	return st
}
```

- [ ] **Step 7: Run the tests**

Run: `go test ./pkg/client/cache/ -run BackendSuite -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add pkg/client/cache/backend.go pkg/client/cache/backend_test.go
git commit -m "feat(cache): read-through ListXAttr, prime from ListDir, invalidate on set/remove"
```

---

### Task 7: Remote (Subscribe) invalidation

**Files:**
- Modify: `pkg/client/cache/subscriber.go`
- Modify: `pkg/client/cache/backend.go` (`subscribeBackendAdapter`)
- Test: `pkg/client/cache/subscriber_test.go`

**Interfaces:**
- Consumes: `subscribeBackendOps` (existing), `cachedBackend.xattr` (Task 6).
- Produces: `subscribeBackendOps.invalidateXAttr(path string)`; `invalidatePathAndParent` also drops xattr for the path.

- [ ] **Step 1: Write the failing test** in `pkg/client/cache/subscriber_test.go`. The suite's `fakeBackendForSubscriber` already records invalidations; add a recorder and assert a `MUTATED` event drops xattr:

```go
func (s *SubscriberSuite) TestMutatedInvalidatesXAttr() {
	c := s.newConsumer() // existing helper building a subscribeConsumer over the fake
	c.handle(&proto.SubscribeEvent{Kind: proto.SubscribeEvent_MUTATED, Path: "a/b"})
	s.Contains(s.fake.xattrInvalidated, "a/b")
}
```

Add the field + method to the fake:
```go
func (b *fakeBackendForSubscriber) invalidateXAttr(p string) {
	b.xattrInvalidated = append(b.xattrInvalidated, p)
}
```
(declare `xattrInvalidated []string` on the fake struct).

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./pkg/client/cache/ -run SubscriberSuite/TestMutatedInvalidatesXAttr -v`
Expected: FAIL — `invalidateXAttr` not in `subscribeBackendOps` / not called.

- [ ] **Step 3: Add to the interface** in `pkg/client/cache/subscriber.go`:

```go
type subscribeBackendOps interface {
	invalidateAttr(path string)
	invalidateData(path string)
	invalidateDir(path string)
	invalidateXAttr(path string)
	putNegative(path string)
}
```

- [ ] **Step 4: Call it in `invalidatePathAndParent`** (same file). Add after the `invalidateData(p)` line:

```go
	c.cache.invalidateXAttr(p)
```

(The path's own xattrs change on MUTATED/DELETED/RENAMED; the parent dir has no xattr entry of interest, so only `p` is dropped.)

- [ ] **Step 5: Implement on the real adapter** in `pkg/client/cache/backend.go`:

```go
func (a *subscribeBackendAdapter) invalidateXAttr(p string) { a.b.xattr.invalidate(p) }
```

- [ ] **Step 6: Run the subscriber tests**

Run: `go test ./pkg/client/cache/ -run SubscriberSuite -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/client/cache/subscriber.go pkg/client/cache/backend.go pkg/client/cache/subscriber_test.go
git commit -m "feat(cache): drop xattr cache on Subscribe MUTATED/DELETED/RENAMED"
```

---

### Task 8: Wire the request flag at mount

**Files:**
- Modify: `pkg/client/mount/single.go` (~line 106 — the `backendOpts` slice; a `WithoutReadahead()` append already follows it under `if m.cache.Enabled`)

**Interfaces:**
- Consumes: `io.WithXattrListings` (Task 3); `m.cache.Enabled`.

- [ ] **Step 1: Add the option** to the existing `backendOpts` slice at `single.go:106`:

```go
	backendOpts := []io.BackendOption{
		io.WithPlusListings(m.cache.Enabled),
		io.WithXattrListings(m.cache.Enabled),
	}
	if m.cache.Enabled {
		backendOpts = append(backendOpts, io.WithoutReadahead())
	}
```

This backend (and decorator) is shared across both FUSE providers, so this one edit covers Linux and macOS.

- [ ] **Step 2: Build the binary to verify wiring compiles**

Run: `go build ./pkg/client/... ./cmd/...`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add pkg/client/mount/single.go
git commit -m "feat(mount): request per-entry xattr names when the cache is enabled"
```

---

### Task 9: End-to-end — one ListDir, zero per-file ListXAttr

**Files:**
- Create/Modify: `test/e2e/fs/xattr_cache_test.go` (env-gated like the suite's other VM/largefile gates only if it needs FUSE; CI has `/dev/fuse`)

**Interfaces:**
- Consumes: the full mounted stack (server + FUSE client + cache enabled).

- [ ] **Step 1: Write the e2e test.** Mount a volume with the cache enabled, create N files (one with a `user.*` xattr), then walk the directory issuing a `listxattr` per entry and assert the wire cost. Use the suite's existing call-counting/interceptor hook on the gRPC client if present; otherwise count via the server's Prometheus `ListXAttr`/`ReadDir` counters exposed on the ops endpoint. Pseudocode shape:

```go
func (s *FsE2ESuite) TestLsLaPrimesXattrFromReadDir() {
	dir := s.mountSubdir("lsla")
	for i := 0; i < 8; i++ {
		s.Require().NoError(os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%d", i)), []byte("x"), 0o644))
	}
	s.Require().NoError(unix.Setxattr(filepath.Join(dir, "f0"), "user.tag", []byte("v"), 0))

	before := s.serverRPCCount("ListXAttr")
	// Simulate `ls -la`: readdir then listxattr each entry (what coreutils does).
	ents, err := os.ReadDir(dir)
	s.Require().NoError(err)
	for _, e := range ents {
		sz, _ := unix.Listxattr(filepath.Join(dir, e.Name()), nil)
		if sz > 0 {
			buf := make([]byte, sz)
			_, _ = unix.Listxattr(filepath.Join(dir, e.Name()), buf)
		}
	}
	after := s.serverRPCCount("ListXAttr")
	s.Equal(int64(0), after-before, "readdir must prime xattr; no per-file ListXAttr RPC")
}

func (s *FsE2ESuite) TestSetXAttrThenListReflectsChange() {
	dir := s.mountSubdir("setget")
	p := filepath.Join(dir, "f")
	s.Require().NoError(os.WriteFile(p, []byte("x"), 0o644))
	_, _ = unix.Listxattr(p, nil) // prime (empty)
	s.Require().NoError(unix.Setxattr(p, "user.new", []byte("v"), 0))
	sz, err := unix.Listxattr(p, nil)
	s.Require().NoError(err)
	buf := make([]byte, sz)
	n, err := unix.Listxattr(p, buf)
	s.Require().NoError(err)
	s.Contains(string(buf[:n]), "user.new")
}
```

(Adapt `mountSubdir` / `serverRPCCount` to the suite's actual helpers — check `test/e2e/fs` setup for the mount root and any existing metrics/counter accessor.)

- [ ] **Step 2: Run the e2e suite**

Run: `go test ./test/e2e/fs/ -run FsE2ESuite -v`
Expected: PASS locally if `/dev/fuse` is available; otherwise CI is the arbiter (note in the PR).

- [ ] **Step 3: Commit**

```bash
git add test/e2e/fs/xattr_cache_test.go
git commit -m "test(e2e): ls -la primes xattr from readdir; setxattr invalidates"
```

---

### Task 10: Final gate + docs

**Files:**
- Modify: `docs/design/` (whichever doc covers the cache, if the readdirplus/cache design is documented there) — one paragraph on xattr-names caching.

- [ ] **Step 1: Run the union gate** over every touched package:

Run:
```bash
go build ./... && go test \
  ./pkg/proto/... ./pkg/server/io/ ./pkg/server/controller/ \
  ./pkg/client/io/ ./pkg/client/cache/ ./pkg/client/config/ -v
```
Expected: PASS (FUSE-dependent e2e excluded — runs in CI).

- [ ] **Step 2: Lint**

Run: `task lint`
Expected: clean.

- [ ] **Step 3: Add a short design note** (if a cache design doc exists) describing the xattr-names prime, the advisory/no-revalidation choice, and the `with_xattr` gate. Reference the spec at `docs/superpowers/specs/2026-06-21-xattr-names-cache-design.md`.

- [ ] **Step 4: Commit**

```bash
git add docs/
git commit -m "docs: note xattr-names caching via readdirplus prime"
```

---

## Self-review notes

- **Spec coverage:** proto+gate (Task 1, 8), server prime (Task 2), client mapping (Task 3), config (Task 4), cache + read-through + prime + local invalidation (Task 5, 6), remote invalidation (Task 7), advisory/no-revalidation choice (Task 6 Step 4 comment), names-only (no value cache anywhere), e2e cold-pass + invalidation (Task 9), bloat gate `with_xattr`=cache-enabled (Task 8). All spec sections map to a task.
- **Empty-vs-unset:** resolved with the explicit `xattr_listed` bool (Task 1) rather than slice emptiness — primed empties are positive hits (Task 5/6 tests).
- **Type consistency:** `XattrNames`/`XattrListed` used identically across proto (Task 1), serverio (Task 2), io (Task 3), and prime (Task 6); `XAttrTTL` consistent across config layers (Task 4) and construction (Task 6).
