# Phase 4 / Sub-spec D: Subscribe + Version Push

**Status:** Design approved 2026-05-18.

**Builds on:**
- Sub-spec A (`docs/superpowers/specs/2026-05-17-phase-4a-pathfs-to-fs-migration.md`) — FileSystemBackend decorator seam.
- Sub-spec B (`docs/superpowers/specs/2026-05-17-phase-4b-in-memory-cache-ttl.md`) — in-memory cache, TTL primitives.
- Sub-spec C (`docs/superpowers/specs/2026-05-17-phase-4c-cache-persistence-design.md`) — disk persistence. ChunkRef.Version slot is already provisioned in the bbolt schema, waiting to be wired.

**Closes:** Phase 4 of the roadmap (persistent client-side cache). After this sub-spec, the headline user promise holds end-to-end: cached files cost zero RPCs in steady state; remote mutations propagate via push; reboot+revalidate is cheap.

## Goal

Make cache freshness push-driven instead of TTL-driven. Three protocol additions and one client behavior change:

1. **`Attr.version`** — server-populated freshness token packed from `(mtime_ns, size, ctime_ns)`.
2. **`GetAttrIfChanged(volume, path, known_version)`** — lightweight revalidation RPC: returns `NotModified` (tiny RTT, no bytes) or new attrs.
3. **`Subscribe(volume)` server-streaming RPC** — pushes `(path, new_version, kind)` mutation events from the server to subscribed clients.
4. **Client cache validation states**: every cached path is either *verified* or *unverified*. Subscribe healthy = verified; disconnect or restart = unverified. Reads of unverified entries gate on a `GetAttrIfChanged` round-trip before serving cached bytes.

User-visible promise: open mount → cache populates → close laptop for a week → boot → open file → one tiny RTT to revalidate, then served from disk. No file re-downloads unless something actually changed.

## Non-goals

- **Per-range invalidation** (per-chunk version tracking) — documented in "Future work". Low priority.
- **Out-of-band change detection** (inotify / fanotify) — documented in "Future work". The self-emit model catches every change made by any gMountie client; out-of-band edits to the underlying directory bypass Subscribe entirely and rely on the TTL safety net.
- **Per-volume event replay ring buffer** — defer until profiling shows the revalidation RPC cost matters.
- **Subscribe stream multiplexing across volumes** — one stream per mounted volume, matches the mount-per-volume model.

## Architecture

Sub-spec D adds three protocol surfaces and one new client goroutine. No new packages; everything threads through existing seams.

**Server-side new wiring:**

- `pkg/server/io/eventbus.go` — `EventBus` interface with `Emit(volume, path, version, kind)` and `Subscribe(volume) <-chan Event`. `localEventBus` owns a `sync.Map[volume → []*subscriber]` with bounded per-subscriber channels. Non-blocking fan-out: full channel → drop the subscriber (its stream closes; client reconnects in revalidation mode).
- `pkg/server/io/version.go` — single helper `VersionFromAttr(attr)` packs the composite version.
- `pkg/server/controller/subscribe.go` — new gRPC streaming controller. One per `SubscribeService` request.
- Existing mutating handlers (Write, Truncate, Unlink, Rmdir, Rename, Chmod, Chown, Allocate, Create, Mkdir) call `eventBus.Emit` after success — one line each.
- `GetAttrIfChangedController` is a fast-path on `VolumeService`: Stat the path; compare versions; reply with `not_modified=true` or the fresh attrs.

**Client-side new wiring:**

- `pkg/client/cache/subscriber.go` — `subscribeConsumer` goroutine started once per `cachedBackend`. Opens the Subscribe stream, drains events into per-path cache invalidations. On stream error: flips cache to unverified, sleeps with exponential backoff (1 s → 30 s cap), reconnects.
- `pkg/client/cache/validity.go` — `validityTracker` owns `state atomic.Int32 (verified|unverified)` plus `verifiedPaths sync.Map[path]bool` for per-path tracking after partial revalidations.
- `pkg/client/cache/backend.go` — extends `cachedBackend.Stat/Lookup/ListDir/Read` to consult the validity tracker. Unverified path → revalidate via `GetAttrIfChanged` before serving cached data.
- Existing `attrCache`, `dirCache`, `dataCache` from Sub-spec B/C stay; revalidation hooks slot in at the cachedBackend layer (not inside the sub-caches).

## Wire protocol changes

`api/proto/fs.proto`:

```proto
message Attr {
  // ... existing fields 1..17 ...
  uint64 version = 18;  // NEW: server-packed via VersionFromAttr.
}

message GetAttrIfChangedRequest {
  string volume = 1;
  string path = 2;
  uint64 known_version = 3;
}
message GetAttrIfChangedReply {
  bool not_modified = 1;
  Attr attrs = 2;             // populated only when not_modified=false
}

message SubscribeRequest {
  string volume = 1;
}
message SubscribeEvent {
  enum Kind {
    KIND_UNSPECIFIED = 0;
    MUTATED = 1;              // Write / Truncate / Chmod / Chown / Allocate / Create / Mkdir
    DELETED = 2;              // Unlink / Rmdir
    RENAMED = 3;              // Rename — new_path populated
    HEARTBEAT = 4;            // periodic; no path, signals "I am alive and you have seen everything up to this point"
  }
  Kind kind = 1;
  string path = 2;
  string new_path = 3;        // populated only for RENAMED
  uint64 new_version = 4;     // ignored for DELETED / HEARTBEAT
}

service RpcFs {
  // ... existing RPCs ...
  rpc GetAttrIfChanged (GetAttrIfChangedRequest) returns (GetAttrIfChangedReply);
  rpc Subscribe (SubscribeRequest) returns (stream SubscribeEvent);
}
```

**Version packing.** `VersionFromAttr(mtime_ns, size, ctime_ns) = mtime_ns ^ (size << 16) ^ ctime_ns`. 64 bits, captures every observable change (content, size, permission/ownership via ctime). Collision is possible in principle but requires three identical-ns events plus the size shift to align — not a real-world failure mode. The single source of truth lives in `pkg/server/io/version.go` and gets unit-tested for the few obvious distinguishing cases (size change → version change; ctime change → version change; same triple → same version).

## Server-side: self-emit event bus

For Sub-spec D, change detection is **self-emit only**: every mutation made through this gMountie server emits an event. Out-of-band changes to the underlying filesystem (e.g. SSHing in and editing directly, or another process writing to the same dir) are NOT captured — those rely on the TTL safety net. Hybrid inotify is a documented follow-up (see "Future work").

This choice matches the headline use case: user mounts remote storage from a laptop; the server is gMountie-owned and not multi-tenant. It also keeps the server capability-free (no `CAP_SYS_ADMIN` requirement, no per-directory inotify watch sprawl).

**`pkg/server/io/eventbus.go`** (new):

```go
package io

type EventKind int8
const (
    KindMutated EventKind = iota
    KindDeleted
    KindRenamed
    KindHeartbeat
)

type Event struct {
    Path       string
    NewPath    string  // KindRenamed only
    NewVersion uint64  // 0 for KindDeleted / KindHeartbeat
    Kind       EventKind
}

type EventBus interface {
    Emit(volume, path string, newVersion uint64, kind EventKind)
    EmitRename(volume, oldPath, newPath string, newVersion uint64)
    Subscribe(volume string) (events <-chan Event, cancel func())
}
```

`localEventBus` owns `sync.Map[volume → []*subscriber]`. Each subscriber has a buffered channel (size `server.subscribe_buffer_size`, default 256). Send is non-blocking: full channel → close the channel → `SubscribeController` sees the close, tears down the stream, increments `gmountie_subscribe_dropped_slow_total`.

**Heartbeat** runs as a per-volume ticker inside the bus (interval = `server.subscribe_heartbeat_interval`, default 10 s). On each tick, fan out a `KindHeartbeat` event to every subscriber. The heartbeat doubles as both keepalive and as the "you have seen everything up to this point" signal the client uses to flip its cache fully verified.

**Wiring into existing handlers**. After every successful mutation in `pkg/server/controller/file.go` and `pkg/server/controller/fs.go`, call `eventBus.Emit(volume, path, attr.Version, kind)`. The handler already has the post-mutation attrs in the reply, so no extra Stat call. One added line per handler.

**`SubscribeController`** (`pkg/server/controller/subscribe.go`, new):

```go
func (c *SubscribeController) Subscribe(req *pb.SubscribeRequest, stream pb.RpcFs_SubscribeServer) error {
    // Authn already done by interceptor; volume access check below.
    if err := c.volumes.CheckAccess(stream.Context(), req.Volume); err != nil {
        return err
    }
    events, cancel := c.bus.Subscribe(req.Volume)
    defer cancel()
    for {
        select {
        case <-stream.Context().Done():
            return stream.Context().Err()
        case ev, ok := <-events:
            if !ok { return status.Error(codes.ResourceExhausted, "subscriber lagged") }
            if err := stream.Send(toPB(ev)); err != nil { return err }
        }
    }
}
```

**`GetAttrIfChangedController`** is a one-handler addition on the existing `VolumeService` (or `RpcFs`):

```go
func (c *VolumeController) GetAttrIfChanged(ctx context.Context, req *pb.GetAttrIfChangedRequest) (*pb.GetAttrIfChangedReply, error) {
    fs, err := c.volumes.Get(ctx, req.Volume)
    if err != nil { return nil, err }
    attr, st := fs.Stat(ctx, req.Path)
    if !st.Ok() {
        return nil, statusFromFuse(st)
    }
    v := VersionFromAttr(attr)
    if v == req.KnownVersion {
        return &pb.GetAttrIfChangedReply{NotModified: true}, nil
    }
    return &pb.GetAttrIfChangedReply{NotModified: false, Attrs: toPBAttr(attr, v)}, nil
}
```

## Client-side: Subscribe consumer + validation gating

### validityTracker

`pkg/client/cache/validity.go` (new):

```go
type validityState int32
const (
    stateUnverified validityState = 0  // zero-value: safe default after restart / new construction
    stateVerified   validityState = 1
)

type validityTracker struct {
    state          atomic.Int32       // whole-cache state
    verifiedPaths  sync.Map           // path → struct{}; populated by per-path GetAttrIfChanged successes during unverified periods
}

func (v *validityTracker) globalState() validityState
func (v *validityTracker) markGlobalVerified()           // called after first HEARTBEAT after stream-up
func (v *validityTracker) markGlobalUnverified()         // called on stream drop / startup
func (v *validityTracker) isPathVerified(path string) bool
func (v *validityTracker) markPathVerified(path string)  // called by per-path revalidation success
```

The default state (zero-value) is `stateUnverified`. New cachedBackend construction starts unverified; the Subscribe consumer flips to verified only after one HEARTBEAT event has been received successfully, which guarantees the server's emit-then-broadcast pipeline is healthy.

### subscribeConsumer

`pkg/client/cache/subscriber.go` (new):

```go
type subscribeConsumer struct {
    client    grpc.Client
    volume    string
    cache     *cachedBackend
    validity  *validityTracker
}

func (s *subscribeConsumer) run(ctx context.Context) {
    backoff := time.Second
    for ctx.Err() == nil {
        if err := s.runOnce(ctx); err != nil {
            log.Warn("subscribe stream error", "err", err)
            s.validity.markGlobalUnverified()
            select {
            case <-ctx.Done(): return
            case <-time.After(backoff):
            }
            if backoff < 30*time.Second { backoff *= 2 }
            continue
        }
        backoff = time.Second
    }
}

func (s *subscribeConsumer) runOnce(ctx context.Context) error {
    stream, err := s.client.RpcFs().Subscribe(ctx, &pb.SubscribeRequest{Volume: s.volume})
    if err != nil { return err }
    sawHeartbeat := false
    for {
        ev, err := stream.Recv()
        if err != nil { return err }
        s.handle(ev)
        if !sawHeartbeat && ev.Kind == pb.SubscribeEvent_HEARTBEAT {
            s.validity.markGlobalVerified()
            sawHeartbeat = true
        }
    }
}

func (s *subscribeConsumer) handle(ev *pb.SubscribeEvent) {
    switch ev.Kind {
    case pb.SubscribeEvent_MUTATED:
        s.cache.attr.invalidate(ev.Path)
        s.cache.data.invalidatePath(ev.Path)
        s.cache.dir.invalidate(pathParent(ev.Path))
    case pb.SubscribeEvent_DELETED:
        s.cache.attr.invalidate(ev.Path)
        s.cache.data.invalidatePath(ev.Path)
        s.cache.dir.invalidate(pathParent(ev.Path))
        s.cache.attr.putNegative(ev.Path)
    case pb.SubscribeEvent_RENAMED:
        for _, p := range []string{ev.Path, ev.NewPath} {
            s.cache.attr.invalidate(p)
            s.cache.data.invalidatePath(p)
            s.cache.dir.invalidate(pathParent(p))
        }
        s.cache.attr.putNegative(ev.Path)
    case pb.SubscribeEvent_HEARTBEAT:
        // no-op; only meaningful to runOnce above
    }
}
```

### Read-path gating

`cachedBackend.Stat`, `Lookup`, `ListDir`, `Read` consult the validity tracker BEFORE serving cache hits. The single arbiter is the attr cache — data and dir trust it transitively. Single-handler atomicity in revalidation prevents Read-after-Stat races:

```go
// inside cachedBackend, used by every read path
func (b *cachedBackend) revalidate(ctx context.Context, path string, cachedVersion uint64) revalidateResult {
    reply, err := b.client.RpcFs().GetAttrIfChanged(ctx, &pb.GetAttrIfChangedRequest{
        Volume: b.volume, Path: path, KnownVersion: cachedVersion,
    })
    if err != nil {
        return revalidateResult{ok: false, fallback: true}
    }
    if reply.NotModified {
        b.validity.markPathVerified(path)
        return revalidateResult{ok: true, notModified: true}
    }
    // Version changed OR ENOENT. Atomic invalidation of all three caches BEFORE returning,
    // so a fast-following Read cannot race past us and hit stale data.
    b.attr.invalidate(path)
    b.data.invalidatePath(path)
    b.dir.invalidate(pathParent(path))
    if reply.Attrs == nil {
        b.attr.putNegative(path)
        return revalidateResult{ok: true, enoent: true}
    }
    return revalidateResult{ok: true, freshAttrs: reply.Attrs}
}
```

In each read method:

```go
func (b *cachedBackend) Stat(ctx context.Context, p string) (*io.Attr, fuse.Status) {
    if cached, hit, pos := b.attr.get(p); hit {
        if b.validity.globalState() == stateVerified || b.validity.isPathVerified(p) {
            if pos { return cached, fuse.OK }
            return nil, fuse.ENOENT
        }
        // Unverified: revalidate before serving the cache hit.
        result := b.revalidate(ctx, p, cached.Version)
        if result.notModified { return cached, fuse.OK }
        if result.enoent { return nil, fuse.ENOENT }
        if result.freshAttrs != nil {
            // cache was already invalidated inside revalidate; populate with fresh attrs and return.
            b.attr.putPositive(p, result.freshAttrs)
            return result.freshAttrs, fuse.OK
        }
        // fallback path: GetAttrIfChanged failed (network or RPC error). Fall through to inner.
    }
    return b.statFromInner(ctx, p)
}
```

The `Read` path uses the *attr* cache's verified state to decide whether to trust data chunks (data chunks themselves carry no independent version; they're locked to the file's attr version transitively). This is correct under whole-file invalidation: if attr version matches, every chunk for the file is current; if it doesn't, all chunks are invalidated together. Per-chunk independent versioning would only matter under per-range invalidation (future work).

## TTL becomes the safety net

TTL semantics from Sub-spec B stay; two policy changes:

1. **Relaxed defaults** in `pkg/client/config/cache.go`:
   - `attr_ttl`: 5 s → **5 min** (300 s)
   - `dir_ttl`: 5 s → **5 min**
   - `negative_ttl`: 2 s → **30 s**

2. **Zero disables a TTL tier.** `attrCache.get` / `dirCache.get` skip the expiry check when their configured TTL is zero. Operators who fully trust Subscribe (single-tenant servers, no out-of-band edits expected) can set all three to `0` and rely purely on Subscribe + revalidation.

The defaults change because TTL is no longer the primary freshness signal. It's the safety net for two cases the Subscribe machinery cannot cover:

- **Out-of-band edits** to the underlying directory (SSH in, write directly). Subscribe never fires; TTL eventually catches it on next access after expiry.
- **Emit-path bugs** in the server (handler forgot to fire an event). TTL bounds the staleness window.

## Config additions

`pkg/client/config/cache.go`:

```yaml
cache:
  subscribe_enabled: true        # NEW. false disables the consumer entirely; cache runs pure TTL+revalidation mode.
  attr_ttl: 5m                   # CHANGED default (was 5s). 0 disables.
  dir_ttl: 5m                    # CHANGED default. 0 disables.
  negative_ttl: 30s              # CHANGED default. 0 disables.
```

`pkg/server/config/server.go`:

```yaml
server:
  subscribe_buffer_size: 256              # NEW. per-subscriber channel buffer; full → drop subscriber.
  subscribe_heartbeat_interval: 10s       # NEW. tick at which the per-volume bus broadcasts HEARTBEAT.
```

## Metrics

Extend Phase 2 + Phase 4 metrics with Subscribe and revalidation counters:

- `gmountie_cache_revalidations_total{result="not_modified|changed|enoent|error"}` (client)
- `gmountie_subscribe_events_received_total{kind="mutated|deleted|renamed|heartbeat"}` (client)
- `gmountie_subscribe_stream_state{state="up|down"}` (client gauge)
- `gmountie_cache_unverified_duration_seconds_total` (client; how long the cache spends in unverified mode)
- `gmountie_subscribe_events_emitted_total{kind}` (server)
- `gmountie_subscribe_subscribers{volume}` (server gauge)
- `gmountie_subscribe_dropped_slow_total{volume}` (server; subscriber buffer overflows)

## Testing strategy

**Unit (testify suites):**

- `pkg/server/io/version_test.go` — VersionFromAttr distinguishes content/size/perm changes; same triple → same version.
- `pkg/server/io/eventbus_test.go` — subscriber gets exactly the events emitted; full-channel drop closes the channel; heartbeat ticks at configured interval; multiple subscribers fan out independently.
- `pkg/server/controller/subscribe_test.go` — auth check; ctx cancel tears down stream; stream send error returns from RPC.
- `pkg/server/controller/getattrifchanged_test.go` — version match → not_modified; version mismatch → fresh attrs; ENOENT → typed gRPC error.
- `pkg/client/cache/validity_test.go` — state transitions; per-path marking; concurrent global-state-flip race smoke test (`-race`).
- `pkg/client/cache/subscriber_test.go` — handle() drives the right invalidations per event kind; runOnce flips verified on first heartbeat; stream error resets to unverified; backoff caps at 30 s.
- `pkg/client/cache/backend_test.go` — extended: read-after-disconnect calls GetAttrIfChanged; not_modified serves cached bytes; version_changed invalidates all three caches atomically; ENOENT invalidates + putNegative.

**E2E** (`test/e2e/api/cache_subscribe_test.go`, new):

`CacheSubscribeFSSuite`:
- `TestPushInvalidatesAcrossClients` — two clients on same volume; client A writes; client B's cached entry for the file disappears within a handful of milliseconds (subscribe push, no TTL involvement).
- `TestRestartRevalidatesViaGetAttrIfChanged` — client mounts, caches file, closes. Restart; the file is still cached on disk; first read does one GetAttrIfChanged (not a full Stat), bytes round-trip from disk. Assert via metrics counters.
- `TestDeletedWhileOfflineSurfacesENOENT` — same restart shape, but the file is deleted on the server between close and restart. First read after restart returns ENOENT (not stale bytes).
- `TestSlowSubscriberGetsDropped` — fill a client's subscribe buffer; server drops it; client reconnects in unverified mode within backoff window.
- `TestSubscribeDisabledFallsBackToTTL` — set `cache.subscribe_enabled: false`; verify no Subscribe stream is opened; TTL still works.

## Definition of done

- Steady-state read of a cached file with healthy Subscribe = zero RPCs (verified via metrics: hits go up, GetAttrIfChanged stays flat).
- Client A writes → client B sees new bytes on next read; if Subscribe is up, within heartbeat interval (~ms), via push event. If Subscribe is down, on the read itself, via GetAttrIfChanged.
- Computer-off-then-reboot scenario: cached files round-trip ONE small GetAttrIfChanged per accessed path; no chunk re-downloads when nothing changed.
- Files deleted while client was offline return ENOENT on first access after boot.
- Stream drop (network blip / server restart / slow consumer): cache stays usable; falls into revalidation mode; recovers automatically when Subscribe reconnects + first heartbeat arrives.
- `task test` + the new e2e suite pass on the VM.

## Future work

Documented now so the next phase has a head start when telemetry justifies these.

### Per-range invalidation

Subscribe event currently carries only `(path, new_version)`. The server's Write handler also knows `(offset, length)` of the mutation. With per-chunk version tracking, the client could invalidate only the chunks that overlap the modified range and keep cached bytes for the rest.

Cost to ship:
- Add `modified_offset` and `modified_length` fields to `SubscribeEvent`.
- Promote `ChunkRef.Version` from "always = file version" to "version at which this chunk was populated".
- Client's per-chunk read: compare `chunkRef.Version` against the file's current attr version; if different but the chunk is outside the modified range from the latest event, keep the chunk (its bytes are still correct).
- Real bookkeeping in the client: maintain a per-path "modified-ranges-since-last-full-validation" set.

Wins only on the big-file-tiny-write workload. Most edits are open-modify-save = whole-file rewrites, which would still invalidate everything. Defer until profiling shows this is a measurable hot path.

### Hybrid inotify for out-of-band changes

Self-emit catches every change through the gMountie server but misses direct edits to the underlying directory (SSH in and edit; another process writes; rsync push to the data dir). When this matters, ship a hybrid:

- Add `server.subscribe_watch_external: true` config flag.
- When enabled, spawn an `inotify` watcher per volume root. On filesystem events, do an `os.Stat` to get fresh attrs and emit a `MUTATED` event onto the same bus that self-emit feeds.
- Watch sprawl (one watch per directory) is bounded by the volume size — practical for typical workloads, would need fanotify for huge trees.

After this lands, the TTL safety net becomes truly redundant for watched filesystems and can default to `0`.

### Subscribe event replay ring buffer

For short disconnects (network blip), the server could keep a per-volume ring of recent events and replay missed events on reconnect. Removes the GetAttrIfChanged-per-cached-path cost during reconnection.

Defer until profiling shows the revalidation cost matters. The fallback path already handles this case correctly, just at higher RPC count.
