package utils

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"gmountie/pkg/client"
	clientConfig "gmountie/pkg/client/config"
	grpcClient "gmountie/pkg/client/grpc"
	clienttls "gmountie/pkg/client/tls"
	"gmountie/pkg/common/passhash"
	"gmountie/pkg/proto"
	"gmountie/pkg/server"
	"gmountie/pkg/server/config"
	grpcServer "gmountie/pkg/server/grpc"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/thanhpk/randstr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	// tls is the per-context ephemeral TLS cert/key used by the test server.
	// Generated in NewAppTestingContext; T6 uses ExpectedFingerprint for TOFU tests.
	tls *EphemeralTLS
	// clientTLSConfig holds the caller's TLS verification policy for the client.
	// Default is {Verify: ModeInsecure}; WithClientTLS overrides before
	// NewAppTestingContext builds the gRPC dial credentials.
	clientTLSConfig clienttls.Config
	// userClientOptions holds only the caller-supplied client options
	// (auth, interceptors, timeouts). Does NOT include the bufconn/TCP
	// dialer added by setupTransport. NewSiblingClient builds its own
	// option list from this + a fresh dialer to avoid double-stacking.
	userClientOptions []grpcClient.ClientOption
	// clientOptions is the client options (user options + transport dialer).
	clientOptions []grpcClient.ClientOption
	// serverOptions holds server options WITHOUT a listener. buildServer
	// appends the listener separately so StartServer can re-serve on a
	// fresh listener without accumulating stale WithListener options.
	serverOptions []grpcServer.ServerOption
	// useTCP toggles between the default in-memory bufconn transport
	// and a real loopback TCP listener. TCP is enabled via
	// WithTCPTransport so perf benches can exercise tc netem shaping.
	useTCP bool
	// useProxy, when true, inserts a TCPProxy between the client and the
	// server's TCP listener. The client dials the proxy, which forwards to
	// the server. WithProxy implies useTCP. Used in T4 reconnect tests.
	useProxy bool
	// proxy is the TCPProxy instance when useProxy is set; nil otherwise.
	// Close shuts it down after the client and server are torn down.
	proxy *TCPProxy
	// listener is the in-memory bufconn listener used in the default
	// configuration; nil when useTCP is set.
	listener *bufconn.Listener
	// tcpListener is the TCP listener used when useTCP is set.
	tcpListener net.Listener
	// server is the gRPC server.
	server *grpcServer.Server
	// serveErrCh receives the error (or nil) returned by server.Serve()
	// so StopServer can join the serve goroutine before rebuilding.
	serveErrCh chan error
	// client is the gRPC client.
	client grpcClient.Client
	// mtlsCreds, when non-nil, overrides the default ephemeral server cert
	// with a CA-signed leaf and configures the server to require client certs
	// from that CA. Set via WithMTLS.
	mtlsCreds *mtlsCredsConfig
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
// The password is hashed with argon2id before being stored in the server config;
// the plaintext password is passed through to the client options unchanged.
func WithBasicAuth(username, password string) TestOptions {
	return func(c *AppTestingContext) {
		h, err := passhash.HashFast(password)
		if err != nil {
			panic("WithBasicAuth: hash password: " + err.Error())
		}
		// Set the server basic auth. AuthConfigBase.Type must be set
		// explicitly so NewAuthServiceFromConfig selects the basic-auth branch.
		c.cfg.Auth = &config.BasicAuthConfig{
			AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeBasic},
			Users: []config.BasicAuthConfigUser{
				{
					Username: username, PasswordHash: h,
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

// WithProxy inserts a TCPProxy between the client and the server's TCP
// listener. The client dials the proxy, which forwards to the server.
// WithProxy implies WithTCPTransport (proxy requires a real TCP listener
// to forward to). The proxy is accessible via Proxy() for Sever/Restore
// calls in failure-mode tests. Close shuts down the proxy automatically.
func WithProxy() TestOptions {
	return func(c *AppTestingContext) {
		c.useTCP = true
		c.useProxy = true
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

// WithClientTLS overrides the client-side TLS verification policy for this
// test context. The default is {Verify: "insecure"} so existing tests keep
// working; pass a TLSConfig with Verify: "tofu" or "verify" to exercise the
// Phase 7 PR 1 TLS verification paths. The option is applied before
// NewAppTestingContext builds the gRPC dial credentials, so it wins.
func WithClientTLS(cfg clientConfig.TLSConfig) TestOptions {
	return func(c *AppTestingContext) {
		c.clientTLSConfig = clienttls.Config{
			Mode:                cfg.Verify,
			CAFile:              cfg.CAFile,
			ExpectedFingerprint: cfg.ExpectedFingerprint,
			ServerName:          cfg.ServerName,
			KnownHostsPath:      cfg.KnownHostsPath,
		}
	}
}

// mtlsCredsConfig holds the PEM materials for an in-process mTLS test context.
type mtlsCredsConfig struct {
	caCertPEM  []byte
	serverCert []byte
	serverKey  []byte
	clientCert []byte // primary client cert (default client / alice)
	clientKey  []byte
}

// ACLUser describes a single principal for WithACLUsers.
type ACLUser struct {
	Username string
	Password string   // plaintext; hashed internally
	Volumes  []string // explicit volume grant list; nil means use default_allow
}

// WithACLUsers sets a multi-user BasicAuth config with per-user volume ACLs and
// default_allow:false. The first user's credentials are wired as the default
// client; use NewClientAs to build a second client authenticating as another
// user. Volume names in the grants must match the names used in WithExistingVolume
// or other volume helpers.
func WithACLUsers(users ...ACLUser) TestOptions {
	return func(c *AppTestingContext) {
		f := false
		bac := &config.BasicAuthConfig{
			AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeBasic},
			DefaultAllow:   &f,
		}
		for _, u := range users {
			h, err := passhash.HashFast(u.Password)
			if err != nil {
				panic("WithACLUsers: hash password for " + u.Username + ": " + err.Error())
			}
			bac.Users = append(bac.Users, config.BasicAuthConfigUser{
				Username:     u.Username,
				PasswordHash: h,
				Volumes:      u.Volumes,
			})
		}
		c.cfg.Auth = bac
		// Wire the first user as the default gRPC client.
		if len(users) > 0 {
			u := users[0]
			c.userClientOptions = append(c.userClientOptions, grpcClient.WithBasicAuth(u.Username, u.Password))
			c.clientOptions = append(c.clientOptions, grpcClient.WithBasicAuth(u.Username, u.Password))
		}
	}
}

// WithMTLS configures the server to use the supplied CA-signed server cert and
// require client certs from that CA (RequireAndVerifyClientCert). The primary
// client authenticates using clientCert/clientKey. Auth config is set to
// auth.type:mtls with the supplied users; default_allow is false.
// Use NewClientAs or build a raw gRPC client to connect as a different principal.
func WithMTLS(ca *TestCA, primaryPrincipal string, users ...ACLUser) TestOptions {
	return func(c *AppTestingContext) {
		c.mtlsCreds = &mtlsCredsConfig{
			caCertPEM:  ca.CACertPEM,
			serverCert: ca.ServerCert,
			serverKey:  ca.ServerKey,
			clientCert: ca.ClientCerts[primaryPrincipal],
			clientKey:  ca.ClientKeys[primaryPrincipal],
		}
		// Build auth config (mtls-typed; no password hashes).
		f := false
		bac := &config.BasicAuthConfig{
			AuthConfigBase: config.AuthConfigBase{Type: config.AuthConfigTypeMTLS},
			DefaultAllow:   &f,
		}
		for _, u := range users {
			bac.Users = append(bac.Users, config.BasicAuthConfigUser{
				Username: u.Username,
				Volumes:  u.Volumes,
			})
		}
		c.cfg.Auth = bac
		// Client TLS stays insecure (server cert SAN is 127.0.0.1 but the bufconn
		// dial target is "bufnet", so server-name verification would fail). The
		// client cert is still presented and validated by the server.
		c.clientTLSConfig = clienttls.Config{
			Mode:     clienttls.ModeInsecure,
			CertFile: "", // populated in NewAppTestingContext from mtlsCreds
			KeyFile:  "",
		}
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
// e2ePassthroughMapping returns the mapping used for e2e volumes: passthrough
// with no_root_squash, i.e. the server assumes the wire caller's uid/gid
// verbatim. This reproduces the pre-identity behaviour (the old
// AssumeUserMiddleware assumed the wire uid when running as root), so existing
// e2e assertions about ownership still hold. The e2e server runs as root.
func e2ePassthroughMapping() config.MappingConfig {
	noRootSquash := false
	return config.MappingConfig{Mode: config.MappingModePassthrough, RootSquash: &noRootSquash}
}

// e2eSquashMapping returns a squash mapping that pins every caller to the
// given uid/gid on the server side, regardless of the authenticated principal.
func e2eSquashMapping(uid, gid uint32) config.MappingConfig {
	return config.MappingConfig{Mode: config.MappingModeSquash, Uid: uid, Gid: gid}
}

// WithSquashVolume appends a randomly named test volume whose Mapping is a
// squash mapping pinned to uid/gid. No random files are pre-created; tests
// create their own content. Use this in identity-rewrite tests where you
// need the server to own files as a specific uid/gid.
func WithSquashVolume(uid, gid uint32) TestOptions {
	return func(c *AppTestingContext) {
		v, err := NewTestVolume(randstr.String(10), false)
		if err != nil {
			panic(err)
		}
		// The server squashes every caller to uid/gid and writes to the volume
		// source dir as that identity. The harness builds the temp tree 0700
		// root-owned, so the squash user can neither traverse the parent nor
		// write the src dir — make the parent traversable and give src to the
		// squash identity (else it hits EACCES).
		if err := os.Chmod(filepath.Dir(v.GetSrcPath()), 0o755); err != nil {
			panic(err)
		}
		if err := os.Chown(v.GetSrcPath(), int(uid), int(gid)); err != nil {
			panic(err)
		}
		c.volumes = append(c.volumes, v)
		c.cfg.Volumes = append(c.cfg.Volumes, &config.VolumeConfig{
			Name:    v.Name,
			Path:    v.GetSrcPath(),
			Mapping: e2eSquashMapping(uid, gid),
		})
	}
}

func WithRandomTestVolume(randomfiles bool) TestOptions {
	return func(c *AppTestingContext) {
		v, err := NewTestVolume(randstr.String(10), randomfiles)
		if err != nil {
			panic(err)
		}
		c.volumes = append(c.volumes, v)
		// Add in server config.
		c.cfg.Volumes = append(c.cfg.Volumes, &config.VolumeConfig{
			Name:    v.Name,
			Path:    v.GetSrcPath(),
			Mapping: e2ePassthroughMapping(),
		})
	}
}

// WithStaticVolume adds a volume whose Mapping is static-mode, backed by an
// existing on-disk directory (srcPath). The caller owns srcPath's lifecycle.
// users and groups are mapped directly into MappingConfig.Users/Groups so the
// static resolver can populate uid/gid/caps for each principal.
//
// Use this in identity/caps tests where you need per-principal caps and the
// server must authenticate via basic-auth. Each AppTestingContext should carry
// exactly one principal in both WithBasicAuth and WithStaticVolume.users.
func WithStaticVolume(srcPath string, users map[string]config.StaticUser, groups map[string]uint32) TestOptions {
	return func(c *AppTestingContext) {
		name := randstr.String(10)
		v, err := NewTestVolumeWithExistingSrc(name, srcPath)
		if err != nil {
			panic(err)
		}
		c.volumes = append(c.volumes, v)
		c.cfg.Volumes = append(c.cfg.Volumes, &config.VolumeConfig{
			Name: v.Name,
			Path: v.GetSrcPath(),
			Mapping: config.MappingConfig{
				Mode:   config.MappingModeStatic,
				Users:  users,
				Groups: groups,
			},
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
			Name:    v.Name,
			Path:    v.GetSrcPath(),
			Mapping: e2ePassthroughMapping(),
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
	// Default client TLS to insecure so existing tests keep working.
	// WithClientTLS overrides this before options are applied.
	appCtx.clientTLSConfig = clienttls.Config{
		Mode: clienttls.ModeInsecure,
	}
	// Apply the options; WithClientTLS may replace clientTLSConfig.
	for _, opt := range options {
		opt(appCtx)
	}
	// Create a new server app context
	serverCtx, err := server.NewServerAppContext(&appCtx.cfg)
	if err != nil {
		return nil, errors.Wrap(err, "build server app context")
	}
	appCtx.serverCtx = serverCtx

	dialTarget, err := appCtx.setupTransport()
	if err != nil {
		return nil, err
	}

	// PHASE 7 PR 1: every test server terminates TLS. T5 wires this in by
	// default so existing tests run unmodified; T6 covers the TLS-specific
	// behaviour (auto-gen, TOFU, fingerprint pin).
	// When WithMTLS is used, override the self-signed ephemeral cert with the
	// CA-signed server cert and require client certificates from the test CA.
	if appCtx.mtlsCreds != nil {
		mc := appCtx.mtlsCreds
		serverLeaf, err := tls.X509KeyPair(mc.serverCert, mc.serverKey)
		if err != nil {
			return nil, errors.Wrap(err, "parse mTLS server cert/key")
		}
		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(mc.caCertPEM) {
			return nil, errors.New("mTLS: no CA cert found in CA PEM")
		}
		serverTLSCfg := &tls.Config{
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{"h2"},
			Certificates: []tls.Certificate{serverLeaf},
			ClientCAs:    caPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
		}
		appCtx.serverOptions = append(appCtx.serverOptions, grpcServer.WithCredentials(credentials.NewTLS(serverTLSCfg)))
		// Client TLS: insecure server-cert verification (bufconn target ≠ IP SAN)
		// but present the primary client cert.
		clientLeaf, err := tls.X509KeyPair(mc.clientCert, mc.clientKey)
		if err != nil {
			return nil, errors.Wrap(err, "parse mTLS client cert/key")
		}
		clientTLSCfg := &tls.Config{
			MinVersion:         tls.VersionTLS13,
			InsecureSkipVerify: true, //nolint:gosec // intentional for in-process test
			Certificates:       []tls.Certificate{clientLeaf},
		}
		appCtx.clientOptions = append(appCtx.clientOptions, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSCfg)),
		}))
		appCtx.userClientOptions = append(appCtx.userClientOptions, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSCfg)),
		}))
	} else {
		ephemeral, err := NewEphemeralTLS("127.0.0.1")
		if err != nil {
			return nil, errors.Wrap(err, "generate test TLS cert")
		}
		appCtx.tls = ephemeral
		serverCreds := credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS13,
			NextProtos:   []string{"h2"},
			Certificates: []tls.Certificate{ephemeral.ServerCreds},
		})
		appCtx.serverOptions = append(appCtx.serverOptions, grpcServer.WithCredentials(serverCreds))

		// Build client TLS credentials from clientTLSConfig (defaulted to insecure;
		// WithClientTLS may have replaced it with tofu/verify). The endpoint for TOFU
		// key resolution is the real TCP listener address when useTCP is set, so that
		// known_hosts entries written in tests use the same key the verifier will look
		// up. For bufconn contexts the endpoint is a synthetic string (the TOFU key is
		// unused because the cert is per-test and the known_hosts path is a tempdir).
		tlsCfgForClient := appCtx.clientTLSConfig
		if tlsCfgForClient.Endpoint == "" {
			if appCtx.tcpListener != nil {
				tlsCfgForClient.Endpoint = appCtx.tcpListener.Addr().String()
			} else {
				tlsCfgForClient.Endpoint = "127.0.0.1:0"
			}
		}
		clientTLSCfg, err := clienttls.BuildConfig(tlsCfgForClient)
		if err != nil {
			return nil, errors.Wrap(err, "build client TLS config for test harness")
		}
		appCtx.clientOptions = append(appCtx.clientOptions, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSCfg)),
		}))
		appCtx.userClientOptions = append(appCtx.userClientOptions, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithTransportCredentials(credentials.NewTLS(clientTLSCfg)),
		}))
	}

	// Determine which listener to pass to buildServer.
	var initialLis net.Listener
	if appCtx.useTCP {
		initialLis = appCtx.tcpListener
	} else {
		initialLis = appCtx.listener
	}
	appCtx.server = appCtx.buildServer(initialLis)
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
//
// The listener is NOT appended to c.serverOptions here — buildServer
// adds it fresh each time so StartServer can re-serve on a new
// listener without accumulating stale WithListener options.
func (c *AppTestingContext) setupTransport() (string, error) {
	if c.useTCP {
		lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			return "", errors.Wrap(err, "test harness TCP listener")
		}
		c.tcpListener = lis
		// If a proxy is requested, insert it between the client and the
		// server's TCP listener. The client dials the proxy; the proxy
		// forwards to the server listener. The TLS endpoint for
		// known-hosts/TOFU resolution still keys off tcpListener.Addr()
		// (where the server actually sits), not the proxy addr — which is
		// correct because insecure mode (test default) skips that lookup.
		if c.useProxy {
			px, pxErr := NewTCPProxy(lis.Addr().String())
			if pxErr != nil {
				return "", errors.Wrap(pxErr, "test harness TCP proxy")
			}
			c.proxy = px
			return "passthrough:///" + px.Addr(), nil
		}
		// passthrough:///TARGET (three slashes; passthrough resolver
		// treats whatever follows as the literal dial address).
		return "passthrough:///" + lis.Addr().String(), nil
	}
	c.listener = bufconn.Listen(1024 * 1024)
	c.clientOptions = append(c.clientOptions, grpcClient.WithDialOptions([]grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return c.listener.Dial()
		}),
	}))
	return "passthrough://bufnet", nil
}

// buildServer constructs a fresh *grpcServer.Server over lis, combining
// c.serverOptions (interceptors, TLS creds, etc.) with a new WithListener.
// Each call returns an independent server; the caller is responsible for
// launching Serve and sending its error to c.serveErrCh.
func (c *AppTestingContext) buildServer(lis net.Listener) *grpcServer.Server {
	// Copy c.serverOptions so we never mutate the stored slice, then append
	// the concrete listener for this server instance.
	opts := append([]grpcServer.ServerOption(nil), c.serverOptions...)
	opts = append(opts, grpcServer.WithListener(lis))
	return grpcServer.NewServer(
		&c.cfg,
		c.serverCtx.AuthService,
		c.serverCtx.GetGrpcServices(),
		opts...,
	)
}

// serveBackground launches c.server.Serve in a goroutine that sends the
// result to c.serveErrCh. GracefulStop makes grpc return nil, so a nil
// result is expected on an intentional stop — the caller reads from
// c.serveErrCh to join the goroutine.
func (c *AppTestingContext) serveBackground() {
	c.serveErrCh = make(chan error, 1)
	go func() {
		c.serveErrCh <- c.server.Serve()
	}()
}

// waitHealthy polls c.client.Version().Get until the server replies or
// deadline elapses. No session or volume is required for the Version RPC;
// it probes the gRPC transport being up and accepting TLS + auth without
// any per-session state. The poll cadence is 25 ms.
func (c *AppTestingContext) waitHealthy(deadline time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	for {
		_, err := c.client.Version().Get(ctx, &proto.VersionRequest{})
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "server did not become healthy within deadline")
		case serveErr := <-c.serveErrCh:
			// Serve returned early (bind failure, etc.) — re-queue the error
			// so StopServer's drain still works, then propagate.
			c.serveErrCh <- serveErr
			return errors.Wrap(serveErr, "server Serve() returned before becoming healthy")
		case <-time.After(25 * time.Millisecond):
		}
	}
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

// GetTLSFingerprint returns the SHA256 fingerprint of the server's ephemeral
// TLS certificate (format "SHA256:<43 base64 raw chars>"). Used by TLS e2e
// tests to build explicit pin assertions without re-reading the cert from disk.
func (c *AppTestingContext) GetTLSFingerprint() string {
	if c.tls == nil {
		return ""
	}
	return c.tls.ExpectedFingerprint
}

// Proxy returns the TCPProxy used by this context, or nil if WithProxy was
// not passed. Tests use Proxy().Sever() / Proxy().Restore() to simulate
// a transient network outage while the server stays up.
func (c *AppTestingContext) Proxy() *TCPProxy {
	return c.proxy
}

// GetEndpoint returns the raw "host:port" TCP address for the test server.
// Only meaningful when useTCP is set (WithTCPTransport); returns "" for
// in-memory bufconn contexts. TLS e2e tests that need to pre-seed a
// known_hosts entry at a specific address use this value as the key.
func (c *AppTestingContext) GetEndpoint() string {
	if c.tcpListener != nil {
		return c.tcpListener.Addr().String()
	}
	return ""
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

// NewClientAs builds a raw gRPC client that authenticates as the named
// principal using basic auth. It connects to the same server as this context.
// Use this in ACL tests where you need a second caller with different
// credentials. The caller must close the returned client when done.
func (c *AppTestingContext) NewClientAs(username, password string) (grpcClient.Client, error) {
	// Collect transport credentials from userClientOptions (which already includes
	// the TLS dial option appended after options apply) and replace auth.
	// Build a fresh option slice: no auth yet, then add this principal's basic-auth.
	opts := make([]grpcClient.ClientOption, 0, len(c.userClientOptions)+2)
	for _, o := range c.userClientOptions {
		opts = append(opts, o)
	}
	opts = append(opts, grpcClient.WithBasicAuth(username, password))
	// Wire the transport dialer (bufconn or TCP).
	if c.listener != nil {
		opts = append(opts, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return c.listener.Dial()
			}),
		}))
	}
	cl, err := grpcClient.NewClient(c.client.GetEndpoint(), opts...)
	if err != nil {
		return nil, errors.Wrap(err, "NewClientAs: dial")
	}
	cl.Connect()
	if cl.SessionID() == "" {
		_ = cl.Close()
		return nil, errors.New("NewClientAs: session handshake failed")
	}
	return cl, nil
}

// NewRawClientWithTLS builds a raw gRPC client using caller-supplied TLS
// credentials. It connects to the same server as this context (bufconn or TCP).
// The caller is responsible for closing the returned client. This is used in
// mTLS tests to connect with an alternative principal cert without going through
// the WithBasicAuth path.
func (c *AppTestingContext) NewRawClientWithTLS(tlsCfg *tls.Config) (grpcClient.Client, error) {
	creds := credentials.NewTLS(tlsCfg)
	var opts []grpcClient.ClientOption
	opts = append(opts, grpcClient.WithDialOptions([]grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}))
	if c.listener != nil {
		opts = append(opts, grpcClient.WithDialOptions([]grpc.DialOption{
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return c.listener.Dial()
			}),
		}))
	}
	cl, err := grpcClient.NewClient(c.client.GetEndpoint(), opts...)
	if err != nil {
		return nil, errors.Wrap(err, "NewRawClientWithTLS: dial")
	}
	cl.Connect()
	if cl.SessionID() == "" {
		_ = cl.Close()
		return nil, errors.New("NewRawClientWithTLS: session handshake failed")
	}
	return cl, nil
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

// Start starts the gRPC server and waits until it is healthy before
// completing the client session handshake. The readiness gate polls
// c.client.Version().Get with a 5-second deadline so there is no
// fixed sleep and startup is deterministic.
func (c *AppTestingContext) Start() error {
	c.serveBackground()
	if err := c.waitHealthy(5 * time.Second); err != nil {
		return errors.Wrap(err, "server readiness")
	}
	c.client.Connect()
	if c.client.SessionID() == "" {
		return errors.New("client session handshake failed; test harness cannot proceed")
	}
	return nil
}

// StopServer gracefully stops the running gRPC server and waits for
// the Serve goroutine to return. The client's keepalive loop will
// observe the connection drop and begin its recovery backoff.
func (c *AppTestingContext) StopServer() {
	c.server.Stop(true)
	// Drain the channel so serveBackground can be called again.
	<-c.serveErrCh
}

// KillServer immediately stops the running gRPC server (non-graceful) and
// waits for the Serve goroutine to return. Unlike StopServer (which uses
// GracefulStop and waits for in-flight RPCs to drain), KillServer drops all
// active connections immediately — semantically equivalent to killing the
// server process mid-transfer. This is the correct primitive for kill-mid-read
// and kill-mid-write failure tests where GracefulStop would block indefinitely
// on long-lived Subscribe/session streams. T3 (ServerKilledSuite) uses this
// instead of StopServer.
func (c *AppTestingContext) KillServer() {
	c.server.Stop(false)
	// Drain the channel so serveBackground can be called again.
	<-c.serveErrCh
}

// StartServer rebuilds and re-serves the gRPC server over the same TCP
// address as the previous listener. Only supported under WithTCPTransport;
// returns an error when called on a bufconn context (bufconn listeners
// cannot rebind). The client is left as-is: its keepalive recovery loop
// will reattach once the server accepts again.
func (c *AppTestingContext) StartServer() error {
	if c.tcpListener == nil {
		return errors.New("StartServer is only supported with WithTCPTransport (bufconn cannot rebind)")
	}
	addr := c.tcpListener.Addr().String()
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "StartServer: rebind TCP listener")
	}
	c.tcpListener = lis
	c.server = c.buildServer(lis)
	c.serveBackground()
	if err := c.waitHealthy(5 * time.Second); err != nil {
		return errors.Wrap(err, "StartServer: server did not become healthy")
	}
	return nil
}

// RestartServer is a convenience wrapper for StopServer followed by StartServer.
func (c *AppTestingContext) RestartServer() error {
	c.StopServer()
	return c.StartServer()
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
	// Drain the serve goroutine if Start was called.
	if c.serveErrCh != nil {
		<-c.serveErrCh
	}
	// Shut down the proxy if one was created by WithProxy.
	if c.proxy != nil {
		_ = c.proxy.Close()
	}
	return nil
}
