package service

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/io"
	"go.gmountie.dev/gmountie/pkg/server/principal"

	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"github.com/stretchr/testify/suite"
)

type BindIdentitySuite struct{ suite.Suite }

func TestBindIdentitySuite(t *testing.T) { suite.Run(t, new(BindIdentitySuite)) }

// serviceForVolume builds a VolumeService with a single volume "v" using the
// given mapping. Path is a real temp dir so the loopback constructs cleanly.
func (s *BindIdentitySuite) serviceForVolume(m config.MappingConfig) *VolumeServiceImpl {
	cfg := &config.Config{Volumes: []*config.VolumeConfig{{Name: "v", Path: s.T().TempDir(), Mapping: m}}}
	svc, err := NewVolumeService(cfg)
	s.Require().NoError(err)
	return svc.(*VolumeServiceImpl)
}

func (s *BindIdentitySuite) TestSquashIgnoresPrincipalAndCaller() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	ctx := principal.WithPrincipal(context.Background(), "alice")
	id, err := svc.resolveIdentity(ctx, "v", &proto.Caller{Owner: &proto.Owner{Uid: 4242}})
	s.Require().NoError(err)
	s.Equal(uint32(1000), id.Uid)
}

func (s *BindIdentitySuite) TestStaticUsesCtxPrincipal() {
	m := config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}}
	svc := s.serviceForVolume(m)
	id, err := svc.resolveIdentity(principal.WithPrincipal(context.Background(), "alice"), "v", nil)
	s.Require().NoError(err)
	s.Equal(uint32(1001), id.Uid)
}

func (s *BindIdentitySuite) TestStaticNoPrincipalFailsClosed() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}})
	_, err := svc.resolveIdentity(context.Background(), "v", nil)
	s.Require().Error(err)
}

func (s *BindIdentitySuite) TestPassthroughRootSquashDefaultOn() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough, AnonUid: 65534})
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(65534), id.Uid)
}

func (s *BindIdentitySuite) TestPassthroughRootSquashUnsetAnonUsesNobody() {
	// root_squash default-on, anon_uid unset (0): root MUST squash to nobody,
	// never to 0 (which would make root_squash a silent no-op).
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough})
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(65534), id.Uid)
	s.Equal(uint32(65534), id.Gid)
}

func (s *BindIdentitySuite) TestPassthroughNoRootSquashKeepsRoot() {
	no := false
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough, RootSquash: &no})
	id, err := svc.resolveIdentity(context.Background(), "v", &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}})
	s.Require().NoError(err)
	s.Equal(uint32(0), id.Uid)
}

func (s *BindIdentitySuite) TestBindIdentityReturnsFS() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	fs, _, err := svc.BindIdentity(context.Background(), "v", nil)
	s.Require().NoError(err)
	s.NotNil(fs)
}

func (s *BindIdentitySuite) TestBindIdentityPrivilegedWrapsIdentity() {
	orig := identityEnforceable
	defer func() { identityEnforceable = orig }()
	identityEnforceable = func() bool { return true }

	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	bare, err := svc.GetVolumeFileSystem("v")
	s.Require().NoError(err)
	bound, _, err := svc.BindIdentity(context.Background(), "v", nil)
	s.Require().NoError(err)
	s.NotSame(bare, bound) // wrapped with the identity-bound FS
}

func (s *BindIdentitySuite) TestResolveIdentityExported() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	id, err := svc.ResolveIdentity(context.Background(), "v", nil)
	s.Require().NoError(err)
	s.Equal(uint32(1000), id.Uid)
	s.Equal("squash", id.Mode, "ResolveIdentity must stamp the volume's mapping mode for WhoAmI")
}

func (s *BindIdentitySuite) TestBindIdentityUnprivilegedReturnsBareFS() {
	orig := identityEnforceable
	defer func() { identityEnforceable = orig }()
	identityEnforceable = func() bool { return false }

	// static + no principal would normally fail closed, but when the process
	// can't change creds, BindIdentity skips identity entirely and hands back
	// the bare loopback so the unprivileged path stays usable (dev/CI).
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}})
	bare, err := svc.GetVolumeFileSystem("v")
	s.Require().NoError(err)
	bound, _, err := svc.BindIdentity(context.Background(), "v", nil)
	s.Require().NoError(err)
	s.Same(bare, bound)
}

// TestBoundFSCachedForMappedMode verifies that repeated BindIdentity calls for
// the same (volume, principal) return the same wrapper instance when the process
// is privileged and the mapping mode is not passthrough.
func (s *BindIdentitySuite) TestBoundFSCachedForMappedMode() {
	orig := identityEnforceable
	defer func() { identityEnforceable = orig }()
	identityEnforceable = func() bool { return true }

	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	ctx := principal.WithPrincipal(context.Background(), "alice")

	fs1, _, err := svc.BindIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	fs2, _, err := svc.BindIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	s.Same(fs1, fs2, "repeated BindIdentity for the same principal must return the cached wrapper")
}

// TestBoundFSNotCachedForPassthrough verifies that passthrough mode never
// caches the wrapper — each call returns a fresh snapshot because identity
// derives from the per-RPC wire Caller.
func (s *BindIdentitySuite) TestBoundFSNotCachedForPassthrough() {
	orig := identityEnforceable
	defer func() { identityEnforceable = orig }()
	identityEnforceable = func() bool { return true }

	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough})
	caller1 := &proto.Caller{Owner: &proto.Owner{Uid: 1001, Gid: 1001}}
	caller2 := &proto.Caller{Owner: &proto.Owner{Uid: 1002, Gid: 1002}}
	ctx := context.Background()

	fs1, _, err := svc.BindIdentity(ctx, "v", caller1)
	s.Require().NoError(err)
	fs2, _, err := svc.BindIdentity(ctx, "v", caller2)
	s.Require().NoError(err)
	s.NotSame(fs1, fs2, "passthrough wrappers must never be shared across different callers")
}

// TestBindIdentityReturnsIdentity verifies that the Identity returned by
// BindIdentity matches the one from ResolveIdentity (no double-resolve needed).
func (s *BindIdentitySuite) TestBindIdentityReturnsIdentity() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	ctx := context.Background()

	_, id, err := svc.BindIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	// In unprivileged path, resolveIdentity is still called for best-effort names.
	resolved, err := svc.ResolveIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	s.Equal(resolved.Uid, id.Uid)
	s.Equal(resolved.Gid, id.Gid)
}

// TestResolverConstancyByMode pins which mapping modes are eligible for the
// pre-credentialed executor fast path. This is the construction-level
// guarantee that per-principal mutable modes never get an executor:
// BindIdentity only calls the constant-FS constructor when
// resolverIsConstant is true.
func (s *BindIdentitySuite) TestResolverConstancyByMode() {
	squash := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	s.True(resolverIsConstant(squash.volumes["v"].resolver),
		"squash resolves to one frozen identity — constant")

	static := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{"alice": {Uid: 1001, Gid: 1001}}})
	s.True(resolverIsConstant(static.volumes["v"].resolver),
		"static identities are frozen in config — constant (even behind the TTL cache)")

	system := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSystem})
	s.False(resolverIsConstant(system.volumes["v"].resolver),
		"system identities come from mutable getent state — must keep the per-op path")

	passthrough := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModePassthrough})
	s.Nil(passthrough.volumes["v"].resolver,
		"passthrough has no resolver at all; identity is per-RPC wire state")
}

// TestExecutorWorkersFromConfig: the knob flows from server.identity config;
// a nil Server (minimal test configs) means 0 = executor disabled (default).
func (s *BindIdentitySuite) TestExecutorWorkersFromConfig() {
	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	s.Equal(0, svc.executorWorkers(), "nil Server config yields 0 = executor disabled")

	svc.config.Server = &config.ServerConfig{Identity: config.IdentityConfig{ExecutorWorkers: 8}}
	s.Equal(8, svc.executorWorkers())
}

// TestDefaultConfigNeverRoutesExecutor is the mutation check for the opt-in
// default: with the default config (executor_workers unset = 0), BindIdentity
// must NEVER construct an executor-routed FS — io.NewConstantResolverBoundFS
// (and through it executorFor) is never invoked, even for a constant-identity
// (squash) volume. Deleting the `s.executorWorkers() > 0` gate in BindIdentity
// makes this fail.
func (s *BindIdentitySuite) TestDefaultConfigNeverRoutesExecutor() {
	origEnforceable, origCtor := identityEnforceable, newConstantResolverBoundFS
	defer func() { identityEnforceable, newConstantResolverBoundFS = origEnforceable, origCtor }()
	identityEnforceable = func() bool { return true }
	calls := 0
	newConstantResolverBoundFS = func(fs pathfs.FileSystem, fn io.IdentityResolveFunc, p string, id io.Identity, workers int) pathfs.FileSystem {
		calls++
		return origCtor(fs, fn, p, id, workers)
	}

	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	ctx := principal.WithPrincipal(context.Background(), "alice")
	bound, _, err := svc.BindIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	s.Require().NotNil(bound)
	s.Equal(0, calls, "default config (executor_workers=0) must never invoke the executor constructor")
}

// TestExplicitWorkersRoutesExecutor: executor_workers=4 ⇒ BindIdentity routes
// a constant-identity volume through the executor constructor with the
// configured pool size. (That the constructor actually builds and wires the
// pool is proven at the io level — TestConstantConstructorWiresExecutor; the
// stub below avoids the process-wide registry, whose real worker startup
// needs root.)
func (s *BindIdentitySuite) TestExplicitWorkersRoutesExecutor() {
	origEnforceable, origCtor := identityEnforceable, newConstantResolverBoundFS
	defer func() { identityEnforceable, newConstantResolverBoundFS = origEnforceable, origCtor }()
	identityEnforceable = func() bool { return true }
	calls, gotWorkers := 0, 0
	newConstantResolverBoundFS = func(fs pathfs.FileSystem, fn io.IdentityResolveFunc, p string, id io.Identity, workers int) pathfs.FileSystem {
		calls++
		gotWorkers = workers
		return io.NewResolverBoundFS(fs, fn, p)
	}

	svc := s.serviceForVolume(config.MappingConfig{Mode: config.MappingModeSquash, Uid: 1000, Gid: 1000})
	svc.config.Server = &config.ServerConfig{Identity: config.IdentityConfig{ExecutorWorkers: 4}}
	ctx := principal.WithPrincipal(context.Background(), "alice")
	bound, _, err := svc.BindIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	s.Require().NotNil(bound)
	s.Equal(1, calls, "executor_workers=4 must route construction through the executor constructor")
	s.Equal(4, gotWorkers, "the configured pool size must pass through")
}

func (s *BindIdentitySuite) TestBindIdentityStaticCapsCarriedThrough() {
	orig := identityEnforceable
	defer func() { identityEnforceable = orig }()
	identityEnforceable = func() bool { return true }

	svc := s.serviceForVolume(config.MappingConfig{
		Mode: config.MappingModeStatic,
		Users: map[string]config.StaticUser{
			"alice": {Uid: 1001, Gid: 1001, Caps: []string{"dac_read_search"}},
		},
	})
	ctx := principal.WithPrincipal(context.Background(), "alice")

	// Verify that the service.Identity resolved for alice carries the cap.
	id, err := svc.resolveIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	s.Equal([]string{"dac_read_search"}, id.Caps, "service.Identity must carry the configured cap")

	// Verify that BindIdentity succeeds (i.e. Caps does not break the construction path).
	boundFS, _, err := svc.BindIdentity(ctx, "v", nil)
	s.Require().NoError(err)
	s.NotNil(boundFS, "bound FS must be non-nil")
}
