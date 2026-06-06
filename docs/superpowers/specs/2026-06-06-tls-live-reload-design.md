# Server TLS Leaf Live-Reload — Design

**Date:** 2026-06-06
**Status:** Approved (owner, 2026-06-06). Scope locked: server leaf cert+key
only — no client-CA pool reload, no client-side reload.

## Problem

Both server listeners load their TLS certificate exactly once:

- the main gRPC listener (`pkg/server/app.go` — `Certificates:
  []tls.Certificate{cert}` from `servertls.LoadOrGenerate`), and
- the ops listener (`pkg/server/ops/server.go` — `tls.LoadX509KeyPair` at
  construction).

A renewed certificate written to the same paths (the cert-manager /
Kubernetes-Secret rotation model: the file is replaced at ~2/3 lifetime via an
atomic symlink swap) never reaches the running process. The in-memory leaf
eventually expires and every new handshake fails — a deterministic outage for
any long-running server whose deployer rotates certs, with no workaround short
of restarting the process (which kills every live session on the volume).

This is generally useful to any self-hoster running behind cert-manager,
ACME, or any rotation scheme — not a hosting-specific hook.

## Decision

**Reload the leaf on change, at handshake time** — `tls.Config.GetCertificate`
backed by a stat-checked cached pair. Rejected alternatives:

- **fsnotify watcher:** new dependency, goroutine lifecycle, and kubelet's
  projected-volume `..data` symlink dance is fiddly to watch correctly. The
  per-handshake `stat` (~µs, and handshakes are rare — connections are
  long-lived) buys the same result with none of that.
- **Explicit trigger (SIGHUP / ops endpoint):** pushes the rotation burden
  onto every deployer; defeats "rotation just works".

Always-on; no config knob. Reload-on-change is strictly better than the
current behavior and the files only change when the operator changes them.

## Component

`pkg/server/tls.Reloader` — the package already owns the server cert
lifecycle (`Generate`/`Load`/`LoadOrGenerate`/`Fingerprint`); the reloader
sits beside them.

```go
// NewReloader loads the initial cert+key pair (failing fast, exactly like
// the current startup path) and caches the cert file's identity stamp.
func NewReloader(certPath, keyPath string) (*Reloader, error)

// GetCertificate is a tls.Config.GetCertificate callback: it serves the
// cached pair, reloading it first when the cert file's stamp has changed.
func (r *Reloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error)
```

- **Stamp** = the cert file's `(mtime, size, inode)`. Inode catches the
  kubelet atomic symlink swap (new `..data` target = new inode) and
  same-second mtime granularity; `os.Stat` follows symlinks so the projected
  layout is transparent.
- **Cached pair** in an `atomic.Pointer[tls.Certificate]` — the unchanged
  path is a stat + pointer load, no locks.
- **Check-and-reload** is mutex-guarded: concurrent handshakes during a
  rotation trigger at most one reload; the rest serve the cached cert
  without blocking on the loader.
- **Fail-open to the last good pair:** any reload failure — cert/key
  mismatch from catching a non-atomic swap mid-write, briefly missing file,
  unparsable PEM — keeps the cached pair and returns it. A failure streak
  logs ONE warning (state-transition logging, no per-handshake spam); the
  next successful reload logs recovery. `GetCertificate` never fails a
  handshake the old cert could serve. (The old pair can itself expire while
  reloads keep failing — at that point handshakes fail with certificate
  errors exactly as today, and the warn log says why.)
- **Successful swap** logs old → new SSH-style fingerprint
  (`servertls.Fingerprint`) at Info.

## Wiring

Two call sites:

- `pkg/server/app.go`: keep `LoadOrGenerate` (generate-on-first-boot, the
  startup fingerprint log, fail-fast validation all unchanged), then build
  the reloader from the same resolved paths and set
  `GetCertificate: reloader.GetCertificate` instead of `Certificates`.
- `pkg/server/ops/server.go`: same substitution for the ops listener's
  `cfg.CertFile`/`cfg.KeyFile`. Two independent `Reloader` instances even if
  the paths coincide — no shared state to get wrong.

Everything else in both `tls.Config`s (MinVersion, NextProtos, ClientCAs,
ClientAuth, the revocation `VerifyPeerCertificate`) is untouched.

## Behavior notes

- **Existing connections are untouched.** TLS never re-handshakes an open
  session; rotation affects new handshakes only. Zero disruption by
  construction.
- **Fingerprint pinning:** clients pinning a self-signed server fingerprint
  are unaffected unless the operator replaces the cert files — which is the
  explicit opt-in. CA-verified deployments (where rotation actually happens)
  never see the change. Changelog notes this.

## Testing

Testify suites (per repo convention), run with `-race`:

1. **Reloader unit suite** (`pkg/server/tls`): initial load; rotate files →
   next `GetCertificate` returns the new leaf (assert by serial); cert/key
   mismatch mid-swap → cached pair, no handshake error; cert file deleted →
   cached pair; stamp unchanged → no reload (assert the returned
   `*tls.Certificate` is pointer-identical across calls); concurrent
   `GetCertificate` calls racing one rotation.
2. **Listener integration test**: a real `tls.Server` using the callback —
   dial, rotate the files on disk, dial again, assert the served leaf serial
   changed and the first connection stayed usable.

No FUSE involvement; runs everywhere including CI.

## Out of scope

- Client-CA pool reload (`ClientCAs` rebuild via `GetConfigForClient`) —
  CA rotation is an operator-driven event with a trust-bundle rollout;
  revisit if that ever needs to be hot.
- Client-side certificate reload — device/client certs are long-lived by
  design here.
- Any reload trigger surface (signal, ops endpoint).
