# Design: xattr-names caching via readdirplus prime

**Status:** approved (brainstorm 2026-06-21)
**Branch:** `worktree-xattr-cache`
**Problem:** `ls -la` is materially slower than `ls` because it triggers one
`ListXAttr` RPC per directory entry.

## Background

`ls -la` (GNU coreutils via gnulib `file_has_acl`) issues a `listxattr(2)`
syscall per file to decide whether to print the ACL `+` indicator after the
mode bits. Plain `ls` skips this entirely.

gMountie's client cache already serves the per-file `LOOKUP`/`getattr` from the
readdirplus attr-prime (`listDirFromInner` primes the attr cache from
`DirEntryPlus.Attr`). After that optimization, **`listxattr` is the last
un-primed per-file RPC in `ls -la`** — N files = N `ListXAttr` round-trips, each
returning (almost always) an empty list. On a WAN link this is the dominant
cost.

Today xattr ops are explicit pass-throughs in `cachedBackend`
(`pkg/client/cache/backend.go` `GetXAttr`/`SetXAttr`/`RemoveXAttr`/`ListXAttr`)
— nothing is cached.

### Why a client cache alone is insufficient

A plain client xattr cache only helps the **2nd+** run: on a single cold
`ls -la`, each path is queried exactly once, so there is nothing cached yet. The
workload spans both cold and repeated/recursive listing, so the design must
cover the **cold pass** — which means priming xattr names from the single
`ListDir` RPC, not just caching direct calls.

## Goals

- A cold `ls -la` over a directory of N files costs **1 `ListDir` RPC and 0
  per-file `ListXAttr` RPCs** (down from N).
- Repeated / recursive (`ls -laR`) and re-stat workloads served from cache.
- Reuse the existing attr-cache freshness machinery (version + Subscribe
  invalidation); add no new staleness class.

## Non-goals (YAGNI)

- **Caching `getxattr` *values*.** Names-only. Most files have no xattrs →
  `listxattr` returns empty → `ls` never calls `getxattr`. A file *with* an ACL
  pays one `getxattr` RPC (rare). Value-caching carries the data-cache
  staleness contract and is deferred unless a concrete need appears.
- New kernel-level xattr timeout (go-fuse has none; not available).

## Design

### 1. Wire protocol (`api/proto/fs.proto`, regen via `task gen:grpc`)

- `ReadDirRequest`: add `bool with_xattr = 5;` — opt-in, set by the client only
  when its cache is enabled.
- `DirEntryPlus`: add `repeated string xattr_names = 3;` — populated only when
  `with_xattr=true` and the per-entry `listxattr` succeeded. Empty slice means
  "no xattrs" (a cacheable positive fact, distinct from "not requested" which is
  the whole field being unset because `with_xattr=false`).

No backwards-compat shim (per project policy: we control both ends; release
notes document the break).

### 2. Server (`pkg/server/controller/fs.go` ReadDir handler + `pkg/server/io`)

When `with_xattr=true`, the readdirplus loop that already stats each entry also
calls the bound FS `ListXAttr` for that entry and sets `xattr_names`. One extra
local syscall per entry — no network. A per-entry `listxattr` error is
non-fatal: leave `xattr_names` empty for that entry (the client will fall back
to a direct `ListXAttr` on demand, same as a cache miss).

### 3. Client cache (`pkg/client/cache`)

**New sub-cache `xattrCache`** (`xattr.go`): `path → []string` (names), TTL-based,
sharing the existing `accountant` byte-budget. Mirrors `attrCache`'s shape but
simpler — positive entries only (an empty `[]string` is a valid positive entry;
there is no negative/ENOENT concept for a names list). TTL from new config knob
`cache.xattr_ttl`, defaulting to the attr TTL.

**`cachedBackend.ListXAttr`** becomes read-through: hit → serve; miss → call
`inner.ListXAttr`, populate on `fuse.OK`, return.

**Prime in `listDirFromInner`**: alongside the existing attr prime, for each
entry carrying `xattr_names`, set `xattrCache[joinPath(p, name)] = names`. This
is the cold-pass win — the single `ListDir` RPC primes every child's
`listxattr`.

The client sets `with_xattr=true` on the `ReadDir` request when the cache is
enabled (plumbed through the io-layer `ListDir` → `ReadDir` call).

### 4. Freshness gating — deliberate choice

The xattr-names cache is **advisory / display-only**: a stale entry yields at
worst a wrong `+` indicator in `ls`. It is **never** a security or data-integrity
issue — ACL enforcement is server-side kernel-native (the client cache is not in
the enforcement path). Therefore it serves on **TTL + invalidation alone**, and
does **NOT** go through the validity tracker's per-path `GetAttrIfChanged`
revalidation.

This is load-bearing: if `ListXAttr` were gated like `cachedAttrLookup`, an
unverified primed entry would trigger one revalidation RTT per file and the
cold-pass prime would buy nothing. Serving primed names directly is what makes
the cold pass fast.

### 5. Invalidation (reuses existing machinery)

- **Local mutation** — `cachedBackend.SetXAttr` / `RemoveXAttr` wrappers, on
  `fuse.OK`: `xattr.invalidate(path)` **and** `attr.invalidate(path)`. (An xattr
  write bumps the inode's ctime, so the cached attr's version is now stale too —
  invalidating attr here also closes a small pre-existing gap where the old
  pass-through left a stale attr version cached.)
- **Remote mutation** — extend `subscribeConsumer.invalidatePathAndParent` to
  also call `invalidateXAttr(path)`. Covers `MUTATED` / `DELETED` / `RENAMED`.
  `SetXAttr` / `RemoveXAttr` already emit via `mutateEmit` (verified
  `fs.go:429`, `:449`), so remote xattr changes already fire a path event.
  - Add `invalidateXAttr(path string)` to the `subscribeBackendOps` interface
    (`subscriber.go`) and the `subscribeBackendAdapter` (`backend.go`).
- **TTL** is the backstop when Subscribe is disabled (`cache.xattr_ttl`).

### 6. Config (`pkg/common/config`)

- Add `cache.xattr_ttl` (duration), default = attr TTL. Default lives in the Go
  constructor (not `viper.SetDefault`), per project convention.

## Bloat tradeoff (resolved)

Folding xattr into readdir taxes every consumer (plain `ls`, `find`, globs) with
a per-entry `listxattr` syscall + usually-empty wire bytes. Resolution: gate it
behind `with_xattr`, set **only when the client cache is enabled**. Non-caching
mounts pay nothing; caching mounts accept one cheap local syscall per entry per
readdir in exchange for eliminating the per-file `ListXAttr` storm.

## Testing

- **Unit (`pkg/client/cache`)** — testify suites:
  - `xattrCache`: put/get, TTL expiry, accountant eviction, empty-list is a hit.
  - `cachedBackend.ListXAttr`: hit serves without inner call; miss calls inner
    once and populates.
  - prime: `ListDir` with `xattr_names` populates `xattrCache` for each child;
    subsequent `ListXAttr` is a hit with zero inner calls.
  - invalidation: local `SetXAttr`/`RemoveXAttr` drop the entry (+ attr);
    Subscribe `MUTATED`/`DELETED`/`RENAMED` drop it.
- **e2e (`test/e2e/fs`, CI has `/dev/fuse`)** — counting backend asserts a
  `readdir` + per-entry `listxattr` sweep over N files issues **1 `ListDir`, 0
  per-file `ListXAttr`**; a `setxattr` followed by re-list reflects the new
  value (invalidation end-to-end).
- **Server (`pkg/server/controller`)** — `ReadDir` carries `xattr_names` when
  `with_xattr=true`, omits when false; per-entry `listxattr` error leaves that
  entry's names empty without failing the batch.

## Touched packages (local gate must test their union)

`api/proto` (+ regen `pkg/proto`), `pkg/server/controller`, `pkg/server/io`,
`pkg/client/io`, `pkg/client/cache`, `pkg/common/config`, `test/e2e/fs`.
Regenerate mocks (`task gen:mocks`) after the `FileSystemBackend` / proto
surface changes.

## Rollout / risk

- Risk is low: advisory cache, server-side enforcement unchanged, invalidation
  rides proven attr-cache machinery.
- Proto change is additive but we ship no compat shim; bump the OSS version and
  note the readdir field in release notes. Cloud consumes via its usual OSS dep
  bump.
