# Changelog

All notable changes to gMountie. Format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). gMountie does not maintain backwards compatibility across alpha releases — wire format, config keys, and on-disk layout are versioned but not migrated. Release notes call out every break.

## Unreleased

The work that will ship in the next `v0.3.0-alpha.x` cut. Tracks `develop`.

### Headline features

- **Persistent client-side cache (Phase 4).** Per-volume `<cache.path>/<volume>/{LOCK, meta.db, chunks/}` survives client restarts. Read of a file already in the cache hits the network only for a tiny `GetAttrIfChanged` revalidation; with the Subscribe stream healthy, zero RPCs.
- **Push-driven invalidation.** Server emits `MUTATED`/`DELETED`/`RENAMED` events from every mutating handler; clients subscribe per volume and apply invalidations within tens of milliseconds. Heartbeat every 10 s confirms stream health.
- **Per-connection session + idempotency (Phase 1c).** Reconnects don't re-execute mutations — each one carries a `request_id`; the server's per-session LRU + singleflight cache deduplicates. `Write` is now safely retryable.
- **Streaming Read/Write + Compound metadata batching (Phase 3).** `Read` and `Write` are now streaming RPCs (no unary frame cap); `Compound` batches 100 `GetAttr` into one RTT.
- **Observability foundations (Phase 2).** Prometheus metrics on server and client, request-id-propagating log fields, gRPC health protocol, `/healthz` / `/readyz` / `/version` HTTP endpoints.
- **Sequential readahead + small-write coalescing (Phase 3).** Detected per-fd, contiguous-only — no speculation, no write-back semantics.
- **Server TLS leaf live-reload.** Both the gRPC and ops listeners pick up a renewed cert+key from disk at the next handshake (stat-stamped `GetCertificate`, fail-open to the last good pair) — cert-manager-style rotation no longer needs a restart, and existing sessions are never disturbed. Note for fingerprint-pinning setups: replacing the cert files changes the fingerprint clients must pin; nothing changes unless you replace the files.

### ⚠ Breaking changes

Operators who explicitly set any of these in `~/.config/gmountie/client.yaml` need to update:

- **`mount.type: vfs` removed.** The VFS multi-volume mounter and `VFSMountConfig` have been extracted to a separate future desktop repo. Only `type: single` is valid. Any config with `type: vfs` will get "invalid mount type: vfs" at startup — change to `type: single` with an explicit `volume:` field.
- **`NewAppContext` signature changed (library users).** The `multiMountPath string` positional argument has been removed. Callers must drop that argument.
- **`github.com/wailsapp/wails/v3` and `github.com/samber/slog-zap/v2` removed from `go.mod`.** Any downstream code that imported these transitively through this module must now take them directly.

- **`cache.max_size_bytes` removed.** Replaced by two independent caps: `cache.memory_max_bytes` (256 MiB default) and `cache.disk_max_bytes` (10 GiB default). Sub-spec C made the cache a two-tier memory + disk structure with separate budgets — one cap can no longer express both.
- **`cache.enabled` default flipped to `true`.** Anyone running `gmountie mount` with no cache config now gets the cache turned on. To opt out: set `cache.enabled: false` explicitly. Cache directory defaults to `$XDG_CACHE_HOME/gmountie`.
- **`cache.attr_ttl` / `cache.dir_ttl` / `cache.negative_ttl` defaults relaxed.** Previously 5 s / 5 s / 2 s when TTL was the only freshness signal; now 5 min / 5 min / 30 s with Subscribe push handling fast invalidation. Zero on any TTL disables that tier for operators who fully trust Subscribe.
- **Wire protocol additions, no removals.** `Attr.version` is new (server-packed from mtime+size+ctime). `GetAttrIfChanged` and `Subscribe` are new RPCs on `RpcFs`. Older clients ignore the new field and don't open the new streams — they still work, just without push-driven invalidation.

### Added

#### Server-side copy, lseek, and xattr writes

- **Server-side `copy_file_range`**. Intra-volume file copies now execute entirely on the server — one RPC instead of streaming all bytes through the client. On filesystems that support `copy_file_range(2)` (Btrfs, XFS, NFS 4.2), the server falls through to a reflink, making large copies near-instant.
- **`lseek` SEEK_DATA / SEEK_HOLE support**. Server exposes a `Lseek` RPC backed by the real syscall, so sparse-aware tools (`cp --sparse`, `rsync --sparse`, `tar -S`) can skip holes efficiently.
- **xattr write support** (`setxattr` / `removexattr`; `listxattr` already existed). A server-side allowlist restricts writes to `user.*` keys and the four POSIX-ACL names (`system.posix_acl_access`, `system.posix_acl_default`, `trusted.posix_acl_access`, `trusted.posix_acl_default`); all other namespaces are rejected with `EPERM`.

#### Phase 1c — Session + idempotency

- `SessionService` gRPC (`Create`, `Resume`, `Keepalive`) with per-session fd tables and grace-period reap on disconnect.
- `session_id` field on every fd-carrying file RPC; `request_id` on every mutating RPC.
- Per-session idempotency cache via golang-lru + singleflight; safely deduplicates Write retries.
- Self-healing client `Keepalive` loop with Resume/Create fallback.
- Per-RPC request-id interceptors and context-aware log fields (`request_id`, `session_id`, `volume`, `user`).

#### Phase 1d — Server reliability fixes

- Graceful shutdown on `SIGTERM` / `SIGINT`.
- Metrics listener failure is non-fatal (won't kill the server process).
- `nodefs.NewServer` / mount errors return up the stack instead of `log.Fatal`.
- Middleware `AssumeUser` failure returns EPERM instead of fatal-erroring.
- Nil-`Caller`/nil-`Owner` guards in `createContext`.
- Non-fatal-`StatFs` reply path returns the error instead of swallowing.
- Viper config now binds nested env vars; `NewFromConfig` errors on nil viper.

#### Phase 2 — Observability

- Server-side per-volume / per-op Prometheus collectors.
- Client-side retry + in-flight collectors.
- gRPC health protocol + HTTP `/healthz` `/readyz` `/version`.
- `VersionService` gRPC + version controller.
- Configurable ops port via `server.metrics_addr`.

#### Phase 3 — Performance

- Streaming `Read` (server-streaming) and `Write` (client-streaming) — removed unary frame cap.
- `Compound` RPC batches metadata ops; 100 `GetAttr` in one RTT.
- Configurable gRPC keepalive and max message size.
- Tunable client FUSE `MaxWrite` + background depth, server-negotiated.
- Sequential per-fd readahead (single-chunk window, no speculation).
- Small-write coalescing per fd, contiguous-only.
- `PerFileConfig` bundle of per-file knobs.
- Perf benchmark harness (`test/e2e/perf/`) with localhost + slow-30ms-loopback variants; benchstat-comparable.

#### Phase 4 — Persistent client-side cache

**Sub-spec A — pathfs → go-fuse/v2/fs migration:**
- New `FileSystemBackend` interface seam in `pkg/client/io`.
- `BackendClient` gRPC implementation of `FileSystemBackend`.
- go-fuse `fs.NodeXXX` adapters delegating to `FileSystemBackend`.
- Single + VFS mounters switched to the new API; legacy `pathfs.FileSystem` deleted.

**Sub-spec B — in-memory cache + TTL:**
- `cachedBackend` decorator wrapping the gRPC client.
- Three sub-caches: `attrCache` (positive + negative TTL), `dirCache`, `dataCache` (chunked, no TTL).
- Shared `accountant` with global LRU eviction under a single byte budget.
- Per-op invalidation contract (Write/Truncate/Unlink/Rename drop the right slices).

**Sub-spec C — disk persistence:**
- New `pkg/client/cache/persist` package with bbolt index + content-addressable chunk storage.
- xxh3-128 content addressing — rename and copy are free in the cache; dedupe across files.
- Refcounted chunk lifecycle in `chunk_refs` bbolt bucket.
- Per-volume flock-based `LOCK` file — refuses dual-mount with `ErrCacheLocked`.
- Format-versioned schema; wipe-on-mismatch (no migration code).
- Disk accountant with FIFO eviction under `disk_max_bytes`.
- Sampled ghost sweep + async orphan sweep on startup.
- Cache hit metrics split by tier (`memory|disk`) + chunk dedupe counter.

**Sub-spec D — Subscribe + version push:**
- `Attr.version` field — server packs (mtime_ns, size, ctime_ns) into uint64.
- `GetAttrIfChanged(volume, path, known_version) → (not_modified | new_attrs | ENOENT)` lightweight revalidation RPC.
- `Subscribe(volume) → stream SubscribeEvent` server-streaming RPC. Per-volume fan-out, bounded subscriber buffers, drop-on-overflow, 10 s heartbeat.
- Client `validityTracker` (verified | unverified) gates cached reads on `GetAttrIfChanged` when the Subscribe stream is unhealthy.
- `subscribeConsumer` goroutine with exp-backoff reconnect; first heartbeat flips the cache to verified.
- Client + server Prometheus counters for revalidations, events received/emitted, stream state, slow-subscriber drops.

### Performance

- Phase 4 / Sub-spec D adds zero measurable overhead on the metadata hot path (OpenStatClose / Lookup / Readdir100 deltas within noise; allocations flat). Geomean shift -1.33 % latency, -0.10 % allocs, +1.23 % throughput. Two benchstat-flagged streaming rows (`SeqRead64MiB +23.64 %` and `SeqWrite16MiB -14.20 %`) are VM scheduling variance (flat B/op confirms; deltas contradict each other on adjacent sizes). Details: `docs/perf/phase4d-2026-05-18.md`.

### Testing

- E2E suites added on the kubevirt VM: `CacheEnabledFSSuite` (push-/pop-/dir-cache correctness), `CachePersistentFSSuite` (restart, dual-mount lock, disk cap), `CacheSubscribeFSSuite` (push across clients, restart-revalidates, deleted-while-offline, subscribe-disabled-falls-back-to-TTL).
- New unit suites for the persist package (Open/Close, ChunkIO, Refcount, DataIdx, KV, Sweep) and the cache validity tracker + subscriber.
- Standalone test functions migrated to testify suites across the tree.
- Mockery upgraded to v3.7.0; mocks regenerated for the new `FileSystemBackend` interface.

### Infrastructure

- Go 1.26 toolchain; protoc plugins pinned via `go.mod`'s `tool` directive.
- CI now runs on `develop` in addition to `master`. Test job timeout bumped to 20 min.
- Release workflow requires a green CI run for the same SHA before allowing a `production` or `alpha` build.
- New roadmap appendix: **Cache proxy / edge tier** as a future capability — Subscribe protocol is already shaped to support a proxy node re-broadcasting events to N downstream clients in the same AZ.

---

## [v0.2.0-alpha.0] — 2026-05-13

Last release before the Phase 1c-4d cycle. Tagged from `master` with the pathfs-based client and unary `Read`/`Write` RPCs.

## [v0.1.0-alpha.0] — earlier alpha

## [v0.0.3], [v0.0.2], [v0.0.1] — initial development tags
