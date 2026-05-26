package utils

import (
	"context"
	"gmountie/pkg/client"
	clientConfig "gmountie/pkg/client/config"
	grpcClient "gmountie/pkg/client/grpc"
	"gmountie/pkg/server"
	"gmountie/pkg/server/config"
	grpcServer "gmountie/pkg/server/grpc"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/thanhpk/randstr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

// AppTestingContext is a struct that holds the testing context
type AppTestingContext struct {
	// cfg is the configuration for the server.
	cfg config.Config
	// serverCtx is the application context.
	serverCtx *server.AppContext
	// clientCtx is the client context.
	clientCtx *client.AppContext
	// userClientOptions holds only the caller-supplied client options
	// (auth, interceptors, timeouts). Does NOT include the bufconn/TCP
	// dialer added by setupTransport. NewSiblingClient builds its own
	// option list from this + a fresh dialer to avoid double-stacking.
	userClientOptions []grpcClient.ClientOption
	// clientOptions is the client options (user options + transport dialer).
	clientOptions []grpcClient.ClientOption
	// serverOptions is the server options.
	serverOptions []grpcServer.ServerOption
	// useTCP toggles between the default in-memory bufconn transport
	// and a real loopback TCP listener. TCP is enabled via
	// WithTCPTransport so perf benches can exercise tc netem shaping.
	useTCP bool
	// listener is the in-memory bufconn listener used in the default
	// configuration; nil when useTCP is set.
	listener *bufconn.Listener
	// tcpListener is the TCP listener used when useTCP is set.
	tcpListener net.Listener
	// server is the gRPC server.
	server *grpcServer.Server
	// client is the gRPC client.
	client grpcClient.Client
	// volumes are the test volumes.
	volumes []*TestVolume
	// cacheCfg is the client-side cache configuration. Defaulted to
	// the package-level disabled defaults in NewAppTestingContext;
	// WithCache overrides this before NewAppContext sees it.
	cacheCfg *clientConfig.CacheConfig
	// fuseCfg is the FUSE kernel-side tuning configuration. Defaulted
	// to the package-level defaults in NewAppTestingContext;
	// WithFUSEConfig overrides this before NewAppContext sees it.
	fuseCfg *clientConfig.FUSEConfig
}

// TestOptions is a type that defines the TestOptions function.
type TestOptions func(*AppTestingContext)

// WithBasicAuth sets the basic authentication for the testing context.
func WithBasicAuth(username, password string) TestOptions {
	return func(c *AppTestingContext) {
		// Set the server basic auth
		c.cfg.Auth = &config.BasicAuthConfig{
			Users: []config.BasicAuthConfigUser{
				{
					Username: username, Password: password,
				},
			},
		}
		// Append the client options to both lists: userClientOptions
		// (auth only, no transport) is used by NewSiblingClient to
		// build a second gRPC client without double-stacking the dialer.
		c.userClientOptions = append(c.userClientOptions, grpcClient.WithBasicAuth(username, password))
		c.clientOptions = append(c.clientOptions, grpcClient.WithBasicAuth(username, password))
	}
}

// WithHeartbeatInterval overrides the server-side Subscribe heartbeat
// interval. Tests that need to control when stateVerified flips (e.g.
// restart-revalidate tests where a heartbeat before the first Stat
// would surface stale cached attrs) should call this with a large
// duration to prevent the heartbeat from racing the test's first access.
func WithHeartbeatInterval(d time.Duration) TestOptions {
	return func(c *AppTestingContext) {
		if c.cfg.Server == nil {
			return
		}
		c.cfg.Server.SubscribeHeartbeatInterval = d
	}
}

// WithServerStreamInterceptors installs extra stream server interceptors on
// the test gRPC server. Useful for tests that need to count or inspect
// streaming RPCs (e.g. verifying client-side write coalescing reduces the
// number of Write streams).
func WithServerStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) TestOptions {
	return func(c *AppTestingContext) {
		c.serverOptions = append(c.serverOptions, grpcServer.WithExtraStreamInterceptors(interceptors...))
	}
}

// WithTCPTransport switches the test harness from the default
// bufconn (in-process pipe) transport to a real loopback TCP
// listener on 127.0.0.1:0. Perf benches set this so tc netem
// shaping on lo actually affects the gRPC path; bufconn never
// touches lo so it cannot be shaped. Functional tests can
// usually leave this off.
func WithTCPTransport() TestOptions {
	return func(c *AppTestingContext) {
		c.useTCP = true
	}
}

// WithCache enables and configures the client-side cache decorator for
// this test harness. Mirrors how the operator-facing CacheConfig is
// wired through SingleVolumeMounter at mount time; tests opt in by
// passing this option, otherwise the harness keeps the cache disabled
// (matching the production default).
func WithCache(cfg clientConfig.CacheConfig) TestOptions {
	return func(c *AppTestingContext) {
		c.cacheCfg = &cfg
	}
}

// WithFUSEConfig overrides the FUSE kernel tuning for the mount built
// by NewAppTestingContext. The default is the package-level off/default
// values (WritebackCache: false); pass a FUSEConfig with
// WritebackCache: true to exercise the kernel writeback path.
func WithFUSEConfig(cfg clientConfig.FUSEConfig) TestOptions {
	return func(c *AppTestingContext) {
		c.fuseCfg = &cfg
	}
}

// WithReadahead configures the client-side readahead window for the mount
// (chunk size, sequential-read arming threshold, and in-flight window depth).
func WithReadahead(chunkBytes, threshold, window int) TestOptions {
	return func(c *AppTestingContext) {
		c.clientOptions = append(c.clientOptions, grpcClient.WithReadahead(chunkBytes, threshold, window))
	}
}

// WithRandomTestVolume creates random test volume.
func WithRandomTestVolume(randomfiles bool) TestOptions {
	return func(c *AppTestingContext) {
		v, err := NewTestVolume(randstr.String(10), randomfiles)
		if err != nil {
			panic(err)
		}
		c.volumes = append(c.volumes, v)
		// Add in server config.
		c.cfg.Volumes = append(c.cfg.Volumes, &config.VolumeConfig{
			Name: v.Name,
			Path: v.GetSrcPath(),
		})
	}
}

// WithExistingVolume adds a volume with a pinned name and an existing
// server-side source directory. Both name and srcPath are caller-owned
// (e.g. via t.TempDir()) so that multiple AppTestingContext instances can
// point at the same backing data and the same per-volume cache directory
// (cache.Path/<name>). Each context gets its own mount subdirectory.
// Use this in restart and dual-mount tests instead of WithRandomTestVolume.
func WithExistingVolume(name, srcPath string) TestOptions {
	return func(c *AppTestingContext) {
		v, err := NewTestVolumeWithExistingSrc(name, srcPath)
		if err != nil {
			panic(err)
		}
		c.volumes = append(c.volumes, v)
		c.cfg.Volumes = append(c.cfg.Volumes, &config.VolumeConfig{
			Name: v.Name,
			Path: v.GetSrcPath(),
		})
	}
}

// NewAppTestingContext creates a new AppTestingContext.
func NewAppTestingContext(options ...TestOptions) (*AppTestingContext, error) {
	appCtx := &AppTestingContext{}
	appCtx.cfg.Server = &config.ServerConfig{
		Metrics:                    false,
		FrameSizeBytes:             config.DefaultFrameSizeBytes,
		CompoundMaxParallel:        config.DefaultCompoundMaxParallel,
		MaxMessageBytes:            config.DefaultMaxMessageBytes,
		SubscribeBufferSize:        config.DefaultServerSubscribeBufferSize,
		SubscribeHeartbeatInterval: config.DefaultServerSubscribeHeartbeatInterval,
		Keepalive: config.ServerKeepaliveConfig{
			Time:                config.DefaultKeepaliveTime,
			Timeout:             config.DefaultKeepaliveTimeout,
			MinTime:             config.DefaultKeepaliveMinTime,
			PermitWithoutStream: config.DefaultKeepalivePermitWithoutStream,
		},
	}
	// Default to the disabled-cache config; WithCache may override
	// before NewAppContext is called below.
	appCtx.cacheCfg = &clientConfig.CacheConfig{
		Enabled:        false, // e2e default: disable cache; WithCache overrides
		MemoryMaxBytes: clientConfig.DefaultCacheMemoryMaxBytes,
		DiskMaxBytes:   clientConfig.DefaultCacheDiskMaxBytes,
		ChunkSizeBytes: clientConfig.DefaultCacheChunkSizeBytes,
		AttrTTL:        clientConfig.DefaultCacheAttrTTL,
		DirTTL:         clientConfig.DefaultCacheDirTTL,
		NegativeTTL:    clientConfig.DefaultCacheNegativeTTL,
	}
	// Default to the standard FUSE tuning; WithFUSEConfig may override
	// before NewAppContext is called below.
	appCtx.fuseCfg = &clientConfig.FUSEConfig{
		MaxWriteBytes:  clientConfig.DefaultFUSEMaxWriteBytes,
		MaxBackground:  clientConfig.DefaultFUSEMaxBackground,
		WritebackCache: clientConfig.DefaultFUSEWritebackCache,
	}
	// Apply the options
	for _, opt := range options {
		opt(appCtx)
	}
	// Create a new server app context
	appCtx.serverCtx = server.NewServerAppContext(&appCtx.cfg)

	dialTarget, err := appCtx.setupTransport()
	if err != nil {
		return nil, err
	}
	appCtx.server = grpcServer.NewServer(
		&appCtx.cfg,
		appCtx.serverCtx.AuthService,
		appCtx.serverCtx.GetGrpcServices(),
		appCtx.serverOptions...,
	)
	c, err := grpcClient.NewClient(dialTarget, appCtx.clientOptions...)
	if err != nil {
		return nil, err
	}
	appCtx.client = c
	appCtx.clientCtx = client.NewAppContext(c, "", appCtx.fuseCfg, appCtx.cacheCfg)
	return appCtx, nil
}

// GetServerApp returns the server app context.
func (c *AppTestingContext) GetServerApp() *server.AppContext {
	return c.serverCtx
}

// setupTransport wires the listener + dial options based on useTCP and
// returns the dial target string for grpcClient.NewClient. The default
// transport is bufconn (in-memory pipe) so existing tests run without
// network sockets; WithTCPTransport switches to a real 127.0.0.1
// listener so loopback shaping (tc netem) takes effect on the gRPC
// path.
func (c *AppTestingContext) setupTransport() (string, error) {
	if c.useTCP {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return "", errors.Wrap(err, "test harness TCP listener")
		}
		c.tcpListener = lis
		c.serverOptions = append(c.serverOptions, grpcServer.WithListener(lis))
		// passthrough:///TARGET (three slashes; passthrough resolver
		// treats whatever follows as the literal dial address).
		return "passthrough:///" + lis.Addr().String(), nil
	}
	c.listener = bufconn.Listen(1024 * 1024)
	c.serverOptions = append(c.serverOptions, grpcServer.WithListener(c.listener))
	c.clientOptions = append(c.clientOptions, grpcClient.WithDialOptions([]grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return c.listener.Dial()
		}),
	}))
	return "passthrough://bufnet", nil
}

// GetClientApp returns the client app context.
func (c *AppTestingContext) GetClientApp() *client.AppContext {
	return c.clientCtx
}

// GetClient returns the gRPC client.
func (c *AppTestingContext) GetClient() grpcClient.Client {
	return c.client
}

// GetVolumes returns the test volumes.
func (c *AppTestingContext) GetVolumes() []*TestVolume {
	return c.volumes
}

// NewSiblingClient creates an independent gRPC client that connects to
// the same server as this AppTestingContext. This enables two-client
// tests where both clients share the server's event bus (so Subscribe
// push events from one client's writes reach the other). The sibling
// client uses cacheCfg independently of c.cacheCfg; pass a different
// cache.Path so the two clients don't contend for the same lock file.
//
// The caller is responsible for closing the returned *client.AppContext
// (via clientCtx.Close()) before calling c.Close(). The returned
// *client.AppContext can mount volumes via SingleVolumeMounter.Mount.
//
// Only bufconn transport is supported for siblings today; TCP-transport
// contexts share a listener address, which can be used directly.
func (c *AppTestingContext) NewSiblingClient(cacheCfg *clientConfig.CacheConfig) (*client.AppContext, error) {
	if cacheCfg == nil {
		cacheCfg = &clientConfig.CacheConfig{Enabled: false}
	}
	// Build sibling opts from user-supplied auth options only; do NOT
	// copy c.clientOptions because setupTransport already appended the
	// bufconn dialer there. We attach a fresh dialer for the same listener.
	siblingOpts := make([]grpcClient.ClientOption, 0, len(c.userClientOptions)+1)
	siblingOpts = append(siblingOpts, c.userClientOptions...)
	if c.listener != nil {
		// bufconn: inject a fresh ContextDialer pointing at the same listener.
		siblingOpts = append(siblingOpts, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return c.listener.Dial()
			}),
		}))
	}
	// TCP transport: no extra dialer needed; the existing dial target
	// (passthrough:///host:port) routes correctly to the live listener.
	siblingClient, err := grpcClient.NewClient(c.client.GetEndpoint(), siblingOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "sibling gRPC client")
	}
	siblingClient.Connect()
	if siblingClient.SessionID() == "" {
		return nil, errors.New("sibling client session handshake failed")
	}
	return client.NewAppContext(siblingClient, "", c.fuseCfg, cacheCfg), nil
}

// MountVolumeErr mounts the test volume and returns any error. Callers
// that want test-fatal behaviour on error should use MountVolume instead.
func (c *AppTestingContext) MountVolumeErr(v *TestVolume) error {
	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		ch <- result{err: c.clientCtx.SingleVolumeMounter.Mount(v.Name, v.GetMountPath())}
	}()
	r := <-ch
	return r.err
}

// MountVolume mounts the test volume and panics on error.
func (c *AppTestingContext) MountVolume(v *TestVolume) {
	if err := c.MountVolumeErr(v); err != nil {
		panic(err)
	}
}

// UnmountVolume unmounts the test volume.
func (c *AppTestingContext) UnmountVolume(v *TestVolume) error {
	return c.clientCtx.SingleVolumeMounter.Unmount(v.Name)
}

// Start starts the gRPC server.
func (c *AppTestingContext) Start() error {
	go func() {
		if err := c.server.Serve(); err != nil {
			panic(err)
		}
	}()
	// Wait for the server to start
	time.Sleep(1 * time.Second)
	c.client.Connect()
	if c.client.SessionID() == "" {
		return errors.New("client session handshake failed; test harness cannot proceed")
	}
	return nil
}

// Close closes the gRPC server.
func (c *AppTestingContext) Close() error {
	// Close the volumes
	err := c.clientCtx.Close()
	if err != nil {
		return err
	}
	// Close the server
	c.server.Stop(true)
	return nil
}
