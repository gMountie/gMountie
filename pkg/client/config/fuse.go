package config

import (
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const (
	// DefaultFUSEProvider is the default FUSE provider selection. "auto"
	// probes known install paths and picks macFUSE if present, falling back
	// to FUSE-T. Set to "macfuse" or "fuse-t" to skip auto-detection.
	DefaultFUSEProvider = "auto"
	// DefaultFUSETBackend selects FUSE-T's mount backend:
	//   "auto"  (default) — use Apple FSKit when the FskitSrvModule extension is
	//           present, otherwise NFS. If the FSKit mount fails (e.g. the
	//           extension is installed but not enabled in System Settings) the
	//           mount transparently falls back to NFS.
	//   "fskit" — force FSKit; the mount fails with a clear error if the module
	//           is missing or the FSKit mount does not succeed (no fallback).
	//   "nfs"   — force FUSE-T's NFSv4 backend (universally available).
	// FSKit (macOS 15+) is a native mount that avoids the NFS backend's metadata
	// amplification (free-space polling, getattr-during-write, AppleDouble
	// sidecars). FUSE-T only — ignored on the macFUSE (go-fuse) path.
	DefaultFUSETBackend = "auto"
	// DefaultFUSEMaxWriteBytes is the default ceiling for FUSE MaxWrite.
	// go-fuse sets the kernel's max_read to the same value, so this knob
	// drives both directions of FUSE-kernel transfer size. 1 MiB matches
	// the server's default FrameSizeBytes; the version handshake caps
	// this down further when the server advertises a smaller frame.
	DefaultFUSEMaxWriteBytes = 1 << 20
	// DefaultFUSEMaxBackground is the upper bound on async background
	// requests in flight from the kernel. go-fuse's library default is
	// 12; 64 gives the streaming Read/Write path room to overlap.
	DefaultFUSEMaxBackground = 64
	// DefaultFUSEWritebackCache leaves the kernel writeback cache off by
	// default. It is a validated opt-in WAN-throughput mode: with it on, the
	// kernel buffers writes and flushes them asynchronously (up to
	// MaxBackground WRITEs in flight), letting the client saturate a
	// high-RTT link instead of one synchronous WRITE per round-trip. Wired
	// through ExtraCapabilities (CAP_WRITEBACK_CACHE) at mount time when true.
	DefaultFUSEWritebackCache = false
	// DefaultFUSEDirectIO leaves global direct-IO off by default: ordinary
	// files keep the kernel page cache (and the readahead/caching the perf
	// path relies on). It is an opt-in escape hatch for mmap-heavy workloads
	// (LMDB, SQLite with a large mmap_size) that would otherwise SIGBUS on a
	// shared writable mmap. Note SQLite WAL -shm sidecars are opened direct-IO
	// unconditionally regardless of this flag (see pkg/client/backend node.go).
	DefaultFUSEDirectIO = false
	// DefaultFUSEAttrTimeout is the default kernel dentry-attribute cache
	// lifetime. 1 s matches go-fuse's built-in default and is a safe
	// starting point: it cuts per-op GETATTR chatter while keeping the
	// kernel-side staleness window short enough that attribute changes (e.g.
	// size after a remote write) become visible within a second.
	//
	// Raising this value reduces kernel GETATTR traffic at the cost of a
	// wider staleness window. Note that the Subscribe push-invalidation
	// stream CANNOT reach inside the kernel's attribute cache — only VFS
	// operations that bypass or expire the cache will see fresh data — so
	// values above ~1 s explicitly trade coherence for chatter reduction.
	DefaultFUSEAttrTimeout = 1 * time.Second
	// DefaultFUSEEntryTimeout is the default kernel dentry (name→inode)
	// cache lifetime. The same coherence tradeoff as AttrTimeout applies:
	// raising it cuts LOOKUP round-trips on repeated path traversals but
	// widens the window for stale name→inode mappings (e.g. a file renamed
	// or unlinked by another client).
	//
	// The Subscribe push-invalidation stream CANNOT reach inside the
	// kernel's dentry cache, so values above ~1 s trade coherence for
	// fewer LOOKUP RPCs.
	DefaultFUSEEntryTimeout = 1 * time.Second
	// DefaultFUSEHandleKillPriv advertises CAP_HANDLE_KILLPRIV_V2 to the
	// kernel by default. Without it the kernel issues a security.capability
	// getxattr on EVERY write (to strip setuid/setgid/file-caps on modify),
	// which a FUSE mount forwards as one GetXattr RPC per write — a
	// round-trip that caps single-file write throughput on high-RTT links
	// (~+75% measured once removed). With the cap set the kernel marks the
	// inode S_NOSEC and instead performs the suid/sgid/cap strip via a
	// setattr the server applies, so the per-write getxattr disappears with
	// no loss of privilege-stripping. Older kernels that do not support the
	// cap simply ignore it. Opt out with fuse.handle_kill_priv: false.
	DefaultFUSEHandleKillPriv = true
)

// FUSEConfig holds the FUSE-kernel-side tuning knobs surfaced through
// gmountie. Values are passed straight to fuse.MountOptions when building
// the mount; max_write_bytes is additionally capped at the server's
// advertised frame size during the version handshake.
//
// go-fuse v2.10.1 does not expose a separate MaxRead field — the kernel's
// max_read is set equal to MaxWrite by the library, so one knob covers
// both directions. There is also no CongestionThreshold field in v2.10.1.
type FUSEConfig struct {
	// MaxWriteBytes bounds each FUSE WRITE (and effectively each READ —
	// see note above) the kernel issues. Pinned to [4 KiB, 16 MiB] so
	// values below a page size or above gRPC's default ceiling are
	// rejected up-front.
	MaxWriteBytes int `validate:"min=4096,max=16777216" mapstructure:"max_write_bytes"`
	// MaxBackground caps async background requests in flight. Pinned to
	// [1, 1024]; the upper bound is a sanity ceiling, not a tuned value.
	MaxBackground int `validate:"min=1,max=1024" mapstructure:"max_background"`
	// WritebackCache toggles the kernel's writeback page cache for the
	// mount. Off by default (synchronous writes); set true to opt into
	// async writeback for WAN throughput — write errors then surface at
	// flush/close, and write visibility to other clients is close-to-open.
	WritebackCache bool `mapstructure:"writeback_cache"`
	// HandleKillPriv advertises CAP_HANDLE_KILLPRIV_V2 to the kernel when
	// true (the default). It removes a per-write security.capability getxattr
	// round-trip; the kernel still strips setuid/setgid/file-caps on modify
	// via a setattr the server applies. Set false only to fall back to the
	// kernel's per-write getxattr behavior (e.g. to debug a backing FS that
	// mishandles the capability).
	HandleKillPriv bool `mapstructure:"handle_kill_priv"`
	// DirectIO forces FOPEN_DIRECT_IO on every file handle when true: the
	// kernel bypasses its page cache for reads/writes and refuses a shared
	// writable mmap with a clean error instead of letting the page fault die
	// with SIGBUS. Off by default — it disables kernel-side read caching, so
	// only enable it for mmap-heavy workloads (LMDB, SQLite mmap_size>0).
	// SQLite WAL -shm sidecars are always opened direct-IO independently of
	// this flag, so plain WAL databases do not need it (see issue #111).
	DirectIO bool `mapstructure:"direct_io"`
	// AttrTimeout controls how long the kernel caches inode attributes
	// (size, mode, timestamps) before issuing a GETATTR. Raising this value
	// cuts GETATTR round-trips on attribute-heavy workloads (e.g. repeated
	// stat calls on the same path) at the cost of a wider kernel-side
	// staleness window. The Subscribe push-invalidation stream CANNOT reach
	// inside the kernel's attribute cache, so values above ~1 s explicitly
	// trade coherence for reduced chatter.
	AttrTimeout time.Duration `validate:"gte=0" mapstructure:"attr_timeout"`
	// EntryTimeout controls how long the kernel caches dentry (name→inode)
	// mappings before issuing a LOOKUP. Raising this value reduces LOOKUP
	// RPCs on repeated path traversals at the cost of stale name→inode
	// mappings being visible longer (e.g. a file renamed or unlinked by
	// another client). The Subscribe push-invalidation stream CANNOT reach
	// inside the kernel's dentry cache, so values above ~1 s trade
	// coherence for fewer LOOKUP RPCs.
	EntryTimeout time.Duration `validate:"gte=0" mapstructure:"entry_timeout"`
	// Provider selects the macOS FUSE implementation: "auto" (default)
	// probes known install paths and picks macFUSE if present, falling back
	// to FUSE-T. Pin to "macfuse" or "fuse-t" to skip auto-detection.
	// Has no effect on Linux without -tags cgofuse.
	Provider string `mapstructure:"provider"`
	// FuseTBackend selects FUSE-T's backend: "auto" (default), "nfs", or
	// "fskit". Only applies when the resolved provider is FUSE-T; ignored for
	// macFUSE and on Linux. "auto" prefers FSKit when available and falls back
	// to NFS; "fskit" forces it (macOS 15+ with the FskitSrvModule extension
	// installed and enabled, else the mount fails clearly).
	FuseTBackend string `validate:"omitempty,oneof=auto nfs fskit" mapstructure:"fuset_backend"`
}

// NewFUSEConfig parses a FUSEConfig from a viper sub-tree. A nil v
// yields defaults; an empty sub-tree yields defaults; explicit values
// override.
func NewFUSEConfig(v *viper.Viper) (*FUSEConfig, error) {
	cfg := &FUSEConfig{
		MaxWriteBytes:  DefaultFUSEMaxWriteBytes,
		MaxBackground:  DefaultFUSEMaxBackground,
		WritebackCache: DefaultFUSEWritebackCache,
		HandleKillPriv: DefaultFUSEHandleKillPriv,
		DirectIO:       DefaultFUSEDirectIO,
		AttrTimeout:    DefaultFUSEAttrTimeout,
		EntryTimeout:   DefaultFUSEEntryTimeout,
		Provider:       DefaultFUSEProvider,
		FuseTBackend:   DefaultFUSETBackend,
	}
	if v == nil {
		return cfg, nil
	}
	v.SetDefault("max_write_bytes", DefaultFUSEMaxWriteBytes)
	v.SetDefault("max_background", DefaultFUSEMaxBackground)
	v.SetDefault("writeback_cache", DefaultFUSEWritebackCache)
	v.SetDefault("handle_kill_priv", DefaultFUSEHandleKillPriv)
	v.SetDefault("direct_io", DefaultFUSEDirectIO)
	v.SetDefault("attr_timeout", DefaultFUSEAttrTimeout)
	v.SetDefault("entry_timeout", DefaultFUSEEntryTimeout)
	v.SetDefault("provider", DefaultFUSEProvider)
	v.SetDefault("fuset_backend", DefaultFUSETBackend)
	if err := v.UnmarshalExact(cfg, viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())); err != nil {
		return nil, err
	}
	return cfg, nil
}
