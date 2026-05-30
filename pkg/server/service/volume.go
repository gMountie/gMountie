package service

import (
	"context"
	"runtime"
	"syscall"
	"time"

	"go.gmountie.dev/gmountie/pkg/common"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/principal"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/pkg/errors"
	"github.com/puzpuzpuz/xsync/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultIdentityTTL = 60 * time.Second

// VolumeService is a service that manages volumes.
type VolumeService interface {
	// List lists all volumes accessible to the principal in ctx.
	List(ctx context.Context) ([]common.Volume, error)
	// GetVolumeFileSystem gets the filesystem for a volume.
	GetVolumeFileSystem(name string) (pathfs.FileSystem, error)
	// BindIdentity resolves the request's identity for a volume and returns a
	// per-request identity-bound filesystem wrapping the volume's loopback,
	// together with the resolved Identity so callers need not re-resolve it.
	BindIdentity(ctx context.Context, volume string, caller *proto.Caller) (pathfs.FileSystem, Identity, error)
	// ResolveIdentity resolves the request's server-side identity for a volume
	// (principal from ctx for mapped modes; wire caller for passthrough).
	ResolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error)
	// PrincipalCanAccess returns nil if the principal in ctx is allowed to
	// access the named volume, or a PermissionDenied status error otherwise.
	PrincipalCanAccess(ctx context.Context, volume string) error
	// Resolve returns the location where the named volume is served, after
	// checking the principal in ctx may access it. An empty string means "this
	// server serves it locally". Errors: PermissionDenied (ACL) or NotFound.
	Resolve(ctx context.Context, volume string) (string, error)
}

type VolumeServiceOptions func(*VolumeServiceImpl)

func WithMiddleware(middleware ...io.Middleware) VolumeServiceOptions {
	return func(s *VolumeServiceImpl) {
		s.middleware = append(s.middleware, middleware...)
	}
}

// volumeEntry consolidates all per-volume state, eliminating the partial-write
// hazard from maintaining multiple parallel maps.
type volumeEntry struct {
	fs       pathfs.FileSystem
	resolver IdentityResolver // nil for passthrough
	mapping  config.MappingConfig
}

// boundFSCacheKey identifies a cached bound-FS wrapper for mapped modes.
// Passthrough is never cached (identity depends on per-RPC wire Caller).
type boundFSCacheKey struct {
	volume    string
	principal string
}

// VolumeServiceImpl is an implementation of the VolumeService interface.
type VolumeServiceImpl struct {
	config     *config.Config
	volumes    map[string]*volumeEntry
	middleware []io.Middleware

	// aclByPrincipal is the O(1) ACL lookup table: principal → set of allowed
	// volumes. A nil value in the map means the principal has no explicit list
	// (falls through to defaultAllow). An entry with an empty set means the
	// principal has an explicit empty list (all denied).
	aclByPrincipal map[string]map[string]struct{}
	defaultAllow   bool // effective default_allow from auth config
	aclEnabled     bool // true when an auth config with users is present

	// boundFSCache caches per-(volume,principal) resolver-bound FS wrappers for
	// mapped modes. Entries are safe to reuse indefinitely because the wrapper
	// holds a reference to the TTL-cached resolver and resolves identity fresh on
	// every op — staleness is bounded by the resolver's own TTL.
	boundFSCache *xsync.MapOf[boundFSCacheKey, pathfs.FileSystem]
}

// NewVolumeService creates a new VolumeService.
func NewVolumeService(cfg *config.Config, options ...VolumeServiceOptions) (VolumeService, error) {
	svc := &VolumeServiceImpl{
		config:         cfg,
		volumes:        make(map[string]*volumeEntry),
		middleware:     make([]io.Middleware, 0),
		aclByPrincipal: make(map[string]map[string]struct{}),
		defaultAllow:   true,
		aclEnabled:     false,
		boundFSCache:   xsync.NewMapOf[boundFSCacheKey, pathfs.FileSystem](),
	}
	for _, option := range options {
		option(svc)
	}
	// Build O(1) ACL model from auth config when available.
	if cfg.Auth != nil {
		if bac, ok := cfg.Auth.(*config.BasicAuthConfig); ok {
			svc.aclEnabled = true
			svc.defaultAllow = bac.DefaultAllowOrTrue()
			for _, u := range bac.Users {
				if u.Volumes != nil {
					set := make(map[string]struct{}, len(u.Volumes))
					for _, v := range u.Volumes {
						set[v] = struct{}{}
					}
					svc.aclByPrincipal[u.Username] = set
				}
				// Users without an explicit Volumes list get no entry →
				// defaultAllow policy applies at check time.
			}
		}
	}
	for _, v := range cfg.Volumes {
		localFS, err := io.NewLocalFilesystem(v.Path)
		if err != nil {
			return nil, errors.Wrapf(err, "open volume %q", v.Name)
		}
		entry := &volumeEntry{mapping: v.Mapping}
		entry.fs = svc.applyMiddleware(localFS)
		switch v.Mapping.Mode {
		case config.MappingModeSquash:
			entry.resolver = NewSquashResolver(v.Mapping.Uid, v.Mapping.Gid)
		case config.MappingModeStatic:
			entry.resolver = NewCachedResolver(NewStaticResolver(v.Mapping), defaultIdentityTTL)
		case config.MappingModeSystem:
			entry.resolver = NewCachedResolver(NewSystemResolver(v.Mapping), defaultIdentityTTL)
		case config.MappingModePassthrough:
			// no resolver; identity derives from the wire caller
		}
		svc.volumes[v.Name] = entry
	}
	return svc, nil
}

// List lists all volumes accessible to the principal in ctx.
// Volumes for which PrincipalCanAccess returns an error are silently excluded
// so that a caller with restricted access sees only their volumes, not an error.
func (s *VolumeServiceImpl) List(ctx context.Context) ([]common.Volume, error) {
	volumes := make([]common.Volume, 0)
	for _, v := range s.config.Volumes {
		if s.PrincipalCanAccess(ctx, v.Name) == nil {
			volumes = append(volumes, common.Volume{Name: v.Name})
		}
	}
	return volumes, nil
}

// GetVolumeFileSystem gets the filesystem for a volume.
func (s *VolumeServiceImpl) GetVolumeFileSystem(name string) (pathfs.FileSystem, error) {
	entry, ok := s.volumes[name]
	if !ok {
		return nil, errors.Errorf("volume %s not found", name)
	}
	return entry.fs, nil
}

// PrincipalCanAccess returns nil if the authenticated principal in ctx is
// permitted to access the named volume. When no auth config is present (ACL
// not enabled), all accesses are allowed regardless of principal. When ACL is
// enabled and the principal has an explicit volume set, membership is checked
// in O(1); otherwise the default_allow policy applies.
func (s *VolumeServiceImpl) PrincipalCanAccess(ctx context.Context, volume string) error {
	// No auth configured → no restrictions; allow everything.
	if !s.aclEnabled {
		return nil
	}
	p, ok := principal.FromContext(ctx)
	if !ok || p == "" {
		return status.Errorf(codes.PermissionDenied, "no authenticated principal for volume %q", volume)
	}
	if set, hasList := s.aclByPrincipal[p]; hasList {
		if _, allowed := set[volume]; allowed {
			return nil
		}
		return status.Errorf(codes.PermissionDenied, "principal %q is not granted volume %q", p, volume)
	}
	// No explicit list → default policy.
	if s.defaultAllow {
		return nil
	}
	return status.Errorf(codes.PermissionDenied, "principal %q has no volume grants (default_allow=false)", p)
}

// Resolve checks access then reports where the volume lives. The OSS server
// always serves its configured volumes locally, so it returns "" on success.
func (s *VolumeServiceImpl) Resolve(ctx context.Context, volume string) (string, error) {
	// ACL first — do not leak existence to an unauthorized principal.
	if err := s.PrincipalCanAccess(ctx, volume); err != nil {
		return "", err
	}
	if _, ok := s.volumes[volume]; !ok {
		return "", status.Errorf(codes.NotFound, "volume %q not found", volume)
	}
	return "", nil
}

// BindIdentity resolves the request's identity for a volume and returns both
// the identity-bound filesystem and the resolved Identity, eliminating the
// double-resolve pattern in attr-returning handlers.
//
// For mapped modes (squash/static/system) the returned FS is a resolver-bound
// wrapper that re-resolves identity on every op via the volume's TTL-cached
// resolver; the wrapper itself is cached per (volume, principal) so repeated
// RPCs skip the allocation. For passthrough, identity derives from the per-RPC
// wire Caller and the wrapper is always built fresh.
//
// When identityEnforceable returns false (non-root, dev/CI) the bare loopback
// is returned alongside a best-effort identity (zero on resolve error) so
// attr-returning handlers can still populate Owner names without changing creds.
func (s *VolumeServiceImpl) BindIdentity(ctx context.Context, volume string, caller *proto.Caller) (pathfs.FileSystem, Identity, error) {
	if err := s.PrincipalCanAccess(ctx, volume); err != nil {
		return nil, Identity{}, err
	}
	entry, ok := s.volumes[volume]
	if !ok {
		return nil, Identity{}, errors.Errorf("volume %s not found", volume)
	}
	if !identityEnforceable() {
		// Not privileged to change thread credentials (non-root dev/CI): run as
		// the server's own user, with no identity layer. Best-effort resolve so
		// attr-returning handlers can still populate Owner names.
		id, _ := s.resolveIdentity(ctx, volume, caller) // swallow error; zero Identity is safe
		return entry.fs, id, nil
	}
	id, err := s.resolveIdentity(ctx, volume, caller)
	if err != nil {
		return nil, Identity{}, err
	}

	var boundFS pathfs.FileSystem
	if entry.mapping.Mode == config.MappingModePassthrough {
		// Passthrough: identity is per-RPC (wire Caller uid/gid) — never cache.
		boundFS = io.NewIdentityBoundFS(entry.fs, &io.Identity{Uid: id.Uid, Gid: id.Gid, Gids: id.Gids, Caps: id.Caps})
	} else {
		// Mapped modes: wrapper re-resolves via the TTL-cached resolver on every
		// op, so caching the wrapper is safe regardless of TTL expiry.
		p, _ := principal.FromContext(ctx)
		key := boundFSCacheKey{volume: volume, principal: p}
		if cached, hit := s.boundFSCache.Load(key); hit {
			boundFS = cached
		} else {
			resolver := entry.resolver
			resolveFn := io.IdentityResolveFunc(func(principal string) (io.Identity, error) {
				id, err := resolver.Resolve(principal)
				if err != nil {
					return io.Identity{}, err
				}
				return io.Identity{Uid: id.Uid, Gid: id.Gid, Gids: id.Gids, Caps: id.Caps}, nil
			})
			w := io.NewResolverBoundFS(entry.fs, resolveFn, p)
			s.boundFSCache.Store(key, w)
			boundFS = w
		}
	}
	return boundFS, id, nil
}

// ResolveIdentity resolves the request's server-side identity for a volume.
// It is the exported counterpart of resolveIdentity, for use by the WhoAmI handler.
func (s *VolumeServiceImpl) ResolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error) {
	return s.resolveIdentity(ctx, volume, caller)
}

// identityEnforceable reports whether the process can change thread credentials
// (setfsuid/setfsgid/setgroups) — i.e. it is root on Linux. Without it the
// bound FS would EPERM on every op, so BindIdentity skips it. Overridable in
// tests.
var identityEnforceable = func() bool {
	return runtime.GOOS == "linux" && syscall.Geteuid() == 0
}

// resolveIdentity determines the effective identity for a request against a
// volume. Mapped modes resolve the authenticated principal (from ctx) through
// the volume's resolver; passthrough derives the identity from the wire caller.
func (s *VolumeServiceImpl) resolveIdentity(ctx context.Context, volume string, caller *proto.Caller) (Identity, error) {
	entry, ok := s.volumes[volume]
	if !ok {
		return Identity{}, errors.Errorf("volume %s not found", volume)
	}
	m := entry.mapping
	if m.Mode == config.MappingModePassthrough {
		return passthroughIdentity(m, caller), nil
	}
	if m.Mode == config.MappingModeSquash {
		// Squash maps every caller to one fixed identity, independent of the
		// authenticated principal, so no principal is required.
		return entry.resolver.Resolve("")
	}
	p, ok := principal.FromContext(ctx)
	if !ok {
		return Identity{}, errors.Errorf("no authenticated principal for volume %s (mode %s)", volume, m.Mode)
	}
	return entry.resolver.Resolve(p)
}

// passthroughIdentity derives the identity directly from the wire caller,
// applying root_squash (default on) so an incoming uid 0 maps to AnonUid unless
// no_root_squash is configured.
func passthroughIdentity(m config.MappingConfig, caller *proto.Caller) Identity {
	var uid, gid uint32
	if caller != nil && caller.Owner != nil {
		uid, gid = caller.Owner.Uid, caller.Owner.Gid
	}
	squashRoot := m.RootSquash == nil || *m.RootSquash // default true
	if squashRoot && uid == 0 {
		anon := m.AnonUid
		if anon == 0 {
			// Never squash root TO root. The server runs as root, so its own
			// uid (0) is not a safe anon; fall back to nobody, matching NFS.
			anon = defaultAnonUid
		}
		uid = anon
		if gid == 0 {
			gid = anon
		}
	}
	return Identity{Uid: uid, Gid: gid, Gids: []uint32{gid}}
}

// defaultAnonUid is the "nobody" id used for root_squash when anon_uid is not
// configured. Guarantees root is never squashed to a privileged identity.
const defaultAnonUid = 65534

// applyMiddleware wraps fs through all registered middleware layers.
func (s *VolumeServiceImpl) applyMiddleware(fs pathfs.FileSystem) pathfs.FileSystem {
	for _, mw := range s.middleware {
		fs = mw(fs)
	}
	return fs
}
