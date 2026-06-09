# Changelog

All notable changes to gMountie. Format is loosely based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). gMountie does not maintain backwards compatibility across alpha releases — wire format, config keys, and on-disk layout are versioned but not migrated. Release notes call out every break.

## Unreleased

### Fixes

- **fd reap race eliminated.** A grace-expiry reap, revocation, or `Release` RPC could call `File.Release()` while a `Read`/`Write`/`CopyFileRange` handler was mid-`pread`/`pwrite` on the same fd — `EBADF` at best, fd-number reuse reading the wrong file at worst. `FileEntry` now carries an atomic refcount: the fd table holds one ref, each in-flight op holds one, and the underlying file closes only when the last ref drops. Removing an entry from the table is still immediate (so the open-files metric and the fd slot are freed promptly); only the `os.File.Close` is deferred.
- **`WaitForReady` on streaming Read/Write.** A server restart or transient network drop no longer burns the retry window: the stream-open call parks and waits for the connection to come back instead of returning `Unavailable` immediately.
- **Bounded `Connect()` during mount.** Session establishment in `gmountie mount` is now capped at 3× the metadata timeout; a half-open or unresponsive server at connect time can no longer hang the process indefinitely.
- **Per-call timeout on session recovery.** Each reattach attempt inside the reconnect loop runs its `Resume`/`Create` calls under a shared 5 s budget; a TCP-reachable but unresponsive server can no longer stall the recovery path for the full retry window.
- **Parent-directory attr invalidation on mutations.** Remote mutation events (`MUTATED`/`DELETED`/`RENAMED`) and local `unlink`/`rmdir`/`rename` ops now eagerly invalidate the parent directory's cached attributes, fixing stale mtime (up to the 5 min attr TTL) that broke mtime-sensitive tools like `make` and `rsync`.

### Improvements

- **`server.session.idempotency_cache_size` config knob; default raised 256 → 4096.** With kernel writeback the client keeps up to 64 `WRITE`s in flight per session, each retried with a stable `request_id`. At the old size the LRU could evict an in-flight entry and cause the retry to re-execute the write. 4096 entries ≈ 400 KiB/session; raise further when sustained per-session `WRITE` concurrency approaches a few thousand. Set via `server.session.idempotency_cache_size` (env `GMOUNTIE_SERVER_SESSION_IDEMPOTENCY_CACHE_SIZE`); `0` or unset delegates to the default.
- **`volumePeekStream` per-frame mutex fast-path.** Once a streaming RPC's volume is pinned, the per-`RecvMsg` mutex acquisition is skipped for the rest of the stream — no lock contention on the hot read path after the first frame.

---

## [v0.16.0-alpha.0] — 2026-06-09

### Headline features

- **Resilient mount retry.** Transient FS-RPC failures retry inside a wall-clock window (`rpc.retry_window`, default 60 s; `0` = fail-fast) with a fresh per-attempt deadline, exponential backoff, and `WaitForReady` dialing — a network blip no longer surfaces `EIO` to applications. A session-change guard keeps idempotent reads retrying across a reconnect while fd ops and path mutations stop immediately when the session was reaped.
- **`get.gmountie.dev` installer.** `curl -fsSL https://get.gmountie.dev | sh` installs the CLI from GitHub Releases (`website/static/install.sh`): resolves stable-then-newest-prerelease, verifies the archive against `checksums.txt`, optional cosign verification; covered by shellcheck + unit tests + a live smoke job in CI.

### ⚠ Breaking changes

- **`server.session.grace_period` default raised 30 s → 60 s** (and is now configurable). The grace period must stay ≥ the client's `rpc.retry_window` for transparent resume.
- Client retry behaviour changed: operations that previously failed on the first transient error now retry for up to `rpc.retry_window`. Set `rpc.retry_window: 0` for the old fail-fast behaviour.

## [v0.15.0-alpha.0] — 2026-06-07

### Headline features

- **Server-side `copy_file_range`.** Intra-volume copies execute entirely on the server — one RPC instead of streaming bytes through the client; reflink-capable filesystems (Btrfs, XFS, NFS 4.2) make large copies near-instant.
- **`lseek` `SEEK_DATA`/`SEEK_HOLE`.** Sparse-aware tools (`cp --sparse`, `rsync --sparse`, `tar -S`) can skip holes efficiently.
- **xattr writes** (`setxattr`/`removexattr`) behind a server-side namespace allowlist (`user.*` plus the four POSIX-ACL names); other namespaces get `EPERM`.
- **Server TLS leaf live-reload.** Both the gRPC and ops listeners pick up a renewed cert+key from disk at the next handshake (stat-stamped `GetCertificate`, fail-open to the last good pair) — cert-manager-style rotation no longer needs a restart, and existing sessions are never disturbed. Fingerprint-pinning setups: replacing the cert files changes the fingerprint clients must pin; nothing changes unless you replace the files.

## [v0.14.0-alpha.0] — 2026-06-04

- **Single-blob credential mount.** `gmountie mount` (and `ls`) accept a `--credentials` file or the `$GMOUNTIE_CREDENTIALS` env var carrying cert/key/CA/endpoint in one blob — cert-only (mTLS) auth with nothing else on the command line.
- `--volume` may be omitted: the client auto-resolves the volume when the server exposes exactly one.
- Go toolchain bumped to 1.26.4 for stdlib security fixes.

## [v0.13.0-alpha.0] — 2026-06-02

- **Config profiles.** Named client configs under `~/.config/gmountie/profiles/<name>.yaml`, selected with `--profile/-P` on `mount`/`ls`/`config show`; `gmountie profile list`. Secrets can come from `auth.password_command` or a 0600 `auth.password_file`.
- **CLI hardening.** New `gmountie status` and `gmountie unmount`; friendly config-validation errors; `config show` prints the file verbatim with secrets redacted, `--effective` shows the merged config; a foreground mount exits on external unmount.
- `/ops/acl/reload` echoes the loaded `revoked_serials`; release ldflags fixed after the module rename; SECURITY.md, CONTRIBUTING.md, and issue/PR templates added.

## [v0.12.0-alpha.0] — 2026-06-01

- **macOS client.** `gmountie mount` / `ls` build and run on darwin (via macFUSE or FUSE-T). The server stays Linux-only.

## [v0.11.0-alpha.2] — 2026-06-01

- Client accepts a hostname (not just an IP) as the server address; spurious keepalive-recovery noise fixed; chart probes default to a TCP dial of the gRPC port and `appProtocol` became configurable.

## [v0.11.0-alpha.1] — 2026-06-01

- Helm chart made deployable against the v0.11 server.

## [v0.11.0-alpha.0] — 2026-06-01

- **Inline TLS PEM config.** `tls.ca_pem` / `tls.cert_pem` / `tls.key_pem` accepted as alternatives to the `*_file` paths — container-native credential injection.

## [v0.10.0-alpha.0] — 2026-05-31

### Headline features

- **Mount-time referrals.** New `VolumeService.Resolve` RPC (ACL-checked, local by default); the client follows a referral to another endpoint at mount time.
- **Cert revocation without restart.** `auth.revoked_serials` cert-serial blocklist, `POST /ops/acl/reload` re-reads config / swaps the ACL snapshot / reaps revoked sessions, optional operator mTLS on the ops listener.

### ⚠ Breaking changes

- **Module renamed** `gmountie` → `go.gmountie.dev/gmountie` (vanity import). All library imports change.
- **`mount.type: vfs` removed.** The VFS multi-volume mounter and the desktop UI left the repo (future separate desktop repo). Only `type: single` is valid; `type: vfs` now fails with "invalid mount type".
- **`NewAppContext` signature changed (library users):** the `multiMountPath string` argument is gone.
- **`github.com/wailsapp/wails/v3` and `github.com/samber/slog-zap/v2` dropped from `go.mod`.** Downstream code that relied on them transitively must import them directly.

## [v0.9.0-alpha.0] — 2026-05-31

### Headline features

- **CLI/config UX overhaul.** sshfs-style shorthand `gmountie mount [user@]host[:port]/volume mountpoint`; interactive no-echo password prompt; `--daemon` background mounts; error remediation hints; new `gmountie ls` and `gmountie config show`; `gmountie fingerprint` emits a paste-ready client snippet; systemd units for serve and mount.
- Server/identity hot-path refactor (bound-FS wrapper cache, O(1) ACL, singleflight identity misses) plus a client reliability + cache-performance sweep (per-instance metrics dispatcher, binary chunk encoding, batched eviction).

### ⚠ Breaking changes (behavior)

- `gmountie serve` first run now **binds 0.0.0.0** (was 127.0.0.1), ships a working `shared` volume at `$XDG_DATA_HOME/gmountie/shared`, and generates a **random admin password printed once** (was the fixed `admin`). Rotate with `gmountie genpass`.
- The server **validates volume paths at startup** and refuses to start when a configured path is missing or not a directory (was: failed lazily at first I/O).
- Config files are written with 0600 permissions; `host:port` parsing is IPv6-correct.

## [v0.8.0-alpha.0] — 2026-05-30

- **Session ownership binding.** Sessions are bound to the caller identity at create; `Resume`/`Keepalive`/data RPCs from a different principal are rejected (closes cross-user `session_id` reuse under mTLS). Raw `session_id` values are redacted in logs (`session_fp` fingerprint instead).

## [v0.7.0-alpha.0] — 2026-05-29

- **Session-scoped authentication.** The argon2id password verify runs once per session; later RPCs authorize by `session_id` (fail-closed). Fixes the per-request-hashing throughput collapse under load; the client also stops re-sending basic-auth credentials once a session is live.

## [v0.6.0-alpha.0] — 2026-05-29

### Headline features

- **TLS everywhere (Phase 7).** Server TLS on every connection; zero-config self-signed cert auto-generation with fingerprint logging and `gmountie fingerprint`; client verification modes `verify` / `tofu` / `insecure` + `expected_fingerprint` pin.
- **Identity & kernel-native permission enforcement (Phase 1a/1b/2/3).** The authenticated principal — not the client-supplied uid/gid — is resolved through per-volume mapping modes (`squash` default / `static` / `system` / `passthrough`) and enforced by the kernel via per-op credential switching; `WhoAmI` RPC + client-side uid/gid rewriting (`--raw-ids` opts out); volume confinement via `openat2(RESOLVE_BENEATH)`; opt-in admin capabilities (`dac_read_search`, `dac_override`).
- **Per-user volume ACL** (`auth.users[].volumes`, `auth.default_allow`) and **mTLS auth** (`auth.type: mtls`, client-cert CN as principal).
- Failure-mode e2e suite (auth rejection, server killed mid-I/O, reconnect with open fds, ≥4 GiB files, concurrent clients); docs site migrated to Docusaurus; race-detector CI job + scheduled Trivy image scan.

### ⚠ Breaking changes

- **`auth: none` removed** — authentication is mandatory; unknown auth types fail closed.
- **Plaintext `auth.users[].password` removed** — only argon2id `password_hash` (PHC string) is accepted; generate with `gmountie genpass`. Startup fails on plaintext.
- **Client-supplied uid/gid is no longer trusted** for permission decisions (wire field is advisory); `AssumeUserMiddleware` deleted in favour of identity-bound filesystems.
- gRPC reflection is now opt-in (default off); ops endpoints bind loopback by default; per-connection DoS limits enforced.

## [v0.5.0-alpha.1] — 2026-05-27

- Release pipeline fix: upload SBOMs + signatures in a cosign-verifiable format.

## [v0.5.0-alpha.0] — 2026-05-27

- **Phase 6 — operations & packaging.** Hardened non-root container image (pinned base, healthcheck); compose credential hygiene; Helm `fsGroup` + default resources; SBOMs + keyless cosign signing on releases.
- **SP5 partial-consume pipelined readahead** — ≈2× sequential-read throughput on high-RTT links; new defaults `readahead_chunk_bytes: 1 MiB`, `readahead_window: 4`.
- **`Utimens` RPC** — `FATTR_ATIME`/`FATTR_MTIME` honored, writeback mtime fidelity.
- Server frame-buffer pooling on the streaming read path; declarative Bencher dashboard plots.

## [v0.4.0-alpha.0] — 2026-05-25

- **`WriteAndFlush` RPC (SP3)** fuses the small-file close tail (create-write-close 8→5 RPCs, ~1.9× faster); `Create` returns attributes so the post-create `Stat` is dropped; `Fsync` clears the dirty flag so a following close-`Flush` is a no-op.

## [v0.3.0-alpha.1] — 2026-05-25

- Interrupted FUSE metadata ops return `EINTR` (not `EIO`); idempotent metadata RPCs detached from FUSE-interrupt cancellation; redundant self-copy removed from streaming `Read`.

## [v0.3.0-alpha.0] — 2026-05-25

- **In-repo perf pipeline.** `perfbmf` CLI (bench parsing, substrate probes, BMF emit), release-gated Bencher upload, fio substrate job + named netem profiles, dedicated runner image. Fix: persist cache `Close` joins background sweeps.

## [v0.2.0-alpha.1] — 2026-05-25

- **Cache fsync reduction** — `meta.db` opened NoSync with periodic + close-time sync; no-op invalidation transactions skipped (~3× persistent-cache write throughput).

## [v0.2.0-alpha.0] — 2026-05-24

The Phase 1c–4d cycle, in one tag.

### Headline features

- **Persistent client-side cache (Phase 4).** Two-tier (memory + disk) cache that survives client restarts; bbolt index + content-addressable chunks; per-volume `LOCK` against dual mounts; LRU eviction under `cache.memory_max_bytes` / `cache.disk_max_bytes`. Client FUSE layer migrated from `pathfs` to go-fuse v2 `fs` (inode-based) behind a unified `FileSystemBackend` seam.
- **Push-driven invalidation.** Server emits `MUTATED`/`DELETED`/`RENAMED` events from every mutating handler; clients `Subscribe` per volume; `Attr.version` + `GetAttrIfChanged` give cheap revalidation when the stream is unhealthy.
- **Per-connection session + idempotency (Phase 1c).** `SessionService` (`Create`/`Resume`/`Keepalive`), per-session fd tables with grace-period reap, `request_id` idempotency LRU + singleflight — `Write` is safely retryable.
- **Streaming Read/Write + `Compound` batching (Phase 3).** No more unary frame cap; 100 metadata ops in one RTT; sequential readahead + small-write coalescing; compression became opt-in.
- **Observability (Phase 2).** Prometheus metrics on both sides, request-id log correlation, gRPC health protocol, `/healthz` `/readyz` `/version`.

### ⚠ Breaking changes

- **`cache.max_size_bytes` removed** — replaced by `cache.memory_max_bytes` (256 MiB default) and `cache.disk_max_bytes` (10 GiB default).
- **`cache.enabled` default flipped to `true`** (opt out explicitly); cache dir defaults to `$XDG_CACHE_HOME/gmountie`.
- **TTL defaults relaxed** — `attr_ttl`/`dir_ttl`/`negative_ttl` now 5 m / 5 m / 30 s with Subscribe push handling fast invalidation.
- Wire additions only: `Attr.version`, `GetAttrIfChanged`, `Subscribe`.

## [v0.1.0-alpha.0], [v0.1.0-alpha.1] — earlier alphas

## [v0.0.3], [v0.0.2], [v0.0.1] — initial development tags
