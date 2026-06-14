package grpc

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/metrics"
	"go.gmountie.dev/gmountie/pkg/common"
	commongrpc "go.gmountie.dev/gmountie/pkg/common/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Client is the interface for the gRPC Client.
type Client interface {
	// GetEndpoint returns the gRPC Client endpoint.
	GetEndpoint() string
	// Connect connects to the gRPC server and completes the session handshake.
	// Returns an error if the handshake fails; callers should treat a non-nil
	// error as fatal and close the client.
	Connect() error
	// Close closes the gRPC Client connection.
	Close() error
	// File returns the gRPC File client.
	File() proto.RpcFileClient
	// Fs returns the gRPC Fs client.
	Fs() proto.RpcFsClient
	// Volume returns the gRPC Volume client.
	Volume() proto.VolumeServiceClient
	// Version returns the gRPC Version client. Used at mount time to
	// negotiate the FUSE frame ceiling with the server.
	Version() proto.VersionServiceClient
	// MetaTimeout returns the per-RPC timeout for metadata operations.
	MetaTimeout() time.Duration
	// IOTimeout returns the per-RPC timeout for data operations.
	IOTimeout() time.Duration
	// RetryWindow returns the wall-clock budget for retrying a single FS op
	// through transient failures. 0 means fail-fast (single attempt).
	RetryWindow() time.Duration
	// Lifetime returns a context cancelled when the client is closed/unmounted.
	// Long retries derive from it so they abort promptly on teardown.
	Lifetime() context.Context
	// SessionID returns the server-assigned session id obtained during Connect.
	SessionID() string
	// BootEpoch returns the server's per-process boot epoch received during the
	// most recent successful session Create. Empty before Connect succeeds or if
	// the server does not send one. A change in value across re-connections
	// indicates a server restart; callers may use this to gate session reclaim.
	BootEpoch() string
	// SessionLive reports whether the keepalive-backed session is currently
	// healthy (a keepalive stream is open and draining). False before Connect,
	// during recovery, and after Close. `gmountie mount` heartbeats this into
	// its on-disk record so `gmountie status` can distinguish a working mount
	// from one whose session is locked out (revoked/expired cert).
	SessionLive() bool
	// PerFileConfig returns the runtime knobs each newly-opened GrpcFile
	// inherits from the Client. Bundling them keeps the interface from
	// widening on every new per-file feature.
	PerFileConfig() PerFileConfig
	// WhoAmI returns the server-side identity the caller maps to on the given
	// volume (used by the mount layer to set up id rewriting).
	WhoAmI(ctx context.Context, volume string) (*proto.Identity, error)
	// Metrics returns this client's collector set (retries, in-flight RPCs,
	// cache hits/misses, subscribe-stream state). NewClientFromConfig
	// registers the collectors on prometheus.DefaultRegisterer, so library
	// consumers embedding the client in a long-running process can expose
	// them by serving promhttp.Handler() (which gathers from
	// prometheus.DefaultGatherer) on an endpoint of their choosing. Returns
	// nil when the client was built without WithMetrics (the factory paths
	// always provide one).
	Metrics() *metrics.Metrics
}

// PerFileConfig bundles the runtime tuning knobs that each newly-opened
// GrpcFile inherits from the Client. Zero values mean "feature off".
type PerFileConfig struct {
	// ReadaheadChunkBytes is the prefetch fetch size. Zero disables
	// readahead on the opened file.
	ReadaheadChunkBytes int
	// ReadaheadThreshold is the number of strictly-sequential reads
	// required before the client arms a prefetch.
	ReadaheadThreshold int
	// ReadaheadWindow is how many ReadaheadChunkBytes chunks to keep in
	// flight or ready ahead of the read cursor. Zero is treated as 1 by
	// NewReadahead. A window > 1 saturates the WAN link on sequential
	// reads by issuing multiple concurrent prefetch RPCs.
	ReadaheadWindow int
	// WriteCoalesceBytes is the per-fd small-write coalescing threshold.
	// Zero disables coalescing; small contiguous writes flow straight
	// through to the streaming Write RPC.
	WriteCoalesceBytes int
}

// ClientImpl is a struct that holds the gRPC ClientImpl
type ClientImpl struct {
	endpoint          string
	conn              *grpc.ClientConn
	dialOptions       []grpc.DialOption
	extraInterceptors []grpc.UnaryClientInterceptor
	fs                proto.RpcFsClient
	file              proto.RpcFileClient
	volume            proto.VolumeServiceClient
	version           proto.VersionServiceClient
	session           proto.SessionServiceClient
	handshake         *SessionHandshake
	metaTimeout       time.Duration
	ioTimeout         time.Duration
	retryWindow       time.Duration
	lifeCtx           context.Context
	lifeCancel        context.CancelFunc
	perFile           PerFileConfig
	// metrics is the per-client collector set. Set via WithMetrics; nil
	// means no in-flight interceptor is wired (the factory always provides one).
	metrics *metrics.Metrics
	// closed makes Close idempotent: the metrics dispatcher registration is
	// refcounted, so a double Close must not drop another live client's
	// reference.
	closed atomic.Bool
	// backgroundTasks are launched on lifeCtx once construction succeeds (e.g.
	// the certificate-renewal loop). Close cancels lifeCtx so each returns.
	backgroundTasks []func(ctx context.Context)
}

// -------------------- ClientImpl Options --------------------

// ClientOption is a type that defines the ClientImplOption function
type ClientOption func(*ClientImpl)

// WithDialOptions sets the dial options for the gRPC ClientImpl
func WithDialOptions(dialOptions []grpc.DialOption) ClientOption {
	return func(c *ClientImpl) {
		// Append the dial options
		c.dialOptions = append(c.dialOptions, dialOptions...)
	}
}

// WithBasicAuth sets the basic authentication for the gRPC ClientImpl.
// The credentials are wired with a sessionLive gate: once the keepalive-backed
// session is healthy, basic-auth metadata is omitted on steady-state RPCs and
// the per-RPC session_id (injected by sessionIDUnaryInterceptor) authorises
// the call instead. Basic-auth is still sent for Create/Resume/recovery because
// those run while healthy=false.
func WithBasicAuth(username, password string) ClientOption {
	return func(c *ClientImpl) {
		creds := NewBasicAuthCredentials(username, password)
		creds.sessionLive = c.SessionLive
		c.dialOptions = append(c.dialOptions, grpc.WithPerRPCCredentials(creds))
	}
}

// WithUnaryInterceptors appends extra unary client interceptors to the
// chain alongside the built-in request-id and logging interceptors.
// Order matters: extras run after the built-ins, closest to the invoker.
func WithUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) ClientOption {
	return func(c *ClientImpl) {
		c.extraInterceptors = append(c.extraInterceptors, interceptors...)
	}
}

// WithTimeouts sets the per-RPC timeouts on the gRPC Client.
func WithTimeouts(meta, io time.Duration) ClientOption {
	return func(c *ClientImpl) {
		c.metaTimeout = meta
		c.ioTimeout = io
	}
}

// WithRetryWindow sets the per-op transient-retry window on the gRPC Client.
func WithRetryWindow(window time.Duration) ClientOption {
	return func(c *ClientImpl) { c.retryWindow = window }
}

// WithReadahead sets the per-fd readahead parameters used when opening
// GrpcFile instances. chunkBytes of 0 disables readahead. window
// controls how many chunks are kept in flight or ready ahead of the
// read cursor; values < 1 are treated as 1 by NewReadahead.
func WithReadahead(chunkBytes, threshold, window int) ClientOption {
	return func(c *ClientImpl) {
		c.perFile.ReadaheadChunkBytes = chunkBytes
		c.perFile.ReadaheadThreshold = threshold
		c.perFile.ReadaheadWindow = window
	}
}

// WithWriteCoalesce sets the per-fd small-write coalescing threshold used
// when opening GrpcFile instances. bytes of 0 disables coalescing.
func WithWriteCoalesce(bytes int) ClientOption {
	return func(c *ClientImpl) {
		c.perFile.WriteCoalesceBytes = bytes
	}
}

// WithBackgroundTask registers fn to run on the client's lifecycle context
// once construction succeeds; fn must return when ctx is done. Close cancels
// the context. Used by the factory to run the certificate-renewal loop.
func WithBackgroundTask(fn func(ctx context.Context)) ClientOption {
	return func(c *ClientImpl) { c.backgroundTasks = append(c.backgroundTasks, fn) }
}

// WithMetrics attaches a pre-built *metrics.Metrics to the client. The
// factory (NewClientFromConfig) uses this to avoid overwriting the package-
// level metric hooks when multiple clients are constructed in the same process
// (e.g. in tests). If not supplied, NewClient itself does not register or
// wire any metrics — the factory is the only production code path that does.
func WithMetrics(m *metrics.Metrics) ClientOption {
	return func(c *ClientImpl) {
		c.metrics = m
	}
}

// ---------------------- Constructor ----------------------

// NewClient creates a new gRPC ClientImpl
func NewClient(endpoint string, options ...ClientOption) (Client, error) {
	c := ClientImpl{
		endpoint:    endpoint,
		metaTimeout: config.DefaultRpcTimeoutMeta,
		ioTimeout:   config.DefaultRpcTimeoutIO,
		retryWindow: config.DefaultRpcRetryWindow,
		perFile: PerFileConfig{
			ReadaheadChunkBytes: config.DefaultReadaheadChunkBytes,
			ReadaheadThreshold:  config.DefaultReadaheadThreshold,
			ReadaheadWindow:     config.DefaultReadaheadWindow,
			WriteCoalesceBytes:  config.DefaultWriteCoalesceBytes,
		},
	}
	c.lifeCtx, c.lifeCancel = context.WithCancel(context.Background())
	for _, opt := range options {
		opt(&c)
	}
	conn, err := grpc.NewClient(
		endpoint,
		c.getDialOptions()...,
	)
	if err != nil {
		c.lifeCancel()
		return nil, err
	}
	c.conn = conn
	c.file = proto.NewRpcFileClient(conn)
	c.fs = proto.NewRpcFsClient(conn)
	c.volume = proto.NewVolumeServiceClient(conn)
	c.version = proto.NewVersionServiceClient(conn)
	c.session = proto.NewSessionServiceClient(conn)
	c.handshake = NewSessionHandshake(c.session)
	// Launch registered background tasks on the client lifecycle context.
	// Close cancels lifeCtx, so each task returns on teardown.
	for _, fn := range c.backgroundTasks {
		go fn(c.lifeCtx)
	}
	return &c, nil
}

// launchRenewal starts fn on the client's lifecycle context after
// construction, the same way WithBackgroundTask would at construction time.
// The referral path uses it to start the certificate-renewal loop on whichever
// client survives as the final, returned one — only after it knows which leg
// that is — so a discarded resolver leg never owns the loop. Close cancels
// lifeCtx, so fn returns on teardown.
//
// It is a no-op for any Client implementation that is not *ClientImpl (e.g. the
// referral tests' in-package fake), keeping it usable through the Client seam.
func launchRenewal(c Client, fn func(ctx context.Context)) {
	if impl, ok := c.(*ClientImpl); ok {
		go fn(impl.lifeCtx)
	}
}

// ---------------------- Methods -----------------------

// GetEndpoint returns the gRPC ClientImpl endpoint
func (c *ClientImpl) GetEndpoint() string {
	return c.endpoint
}

// File returns the gRPC File client
func (c *ClientImpl) File() proto.RpcFileClient {
	return c.file
}

// Fs returns the gRPC Fs client
func (c *ClientImpl) Fs() proto.RpcFsClient {
	return c.fs
}

// Volume returns the gRPC Volume client
func (c *ClientImpl) Volume() proto.VolumeServiceClient {
	return c.volume
}

// Version returns the gRPC Version client.
func (c *ClientImpl) Version() proto.VersionServiceClient {
	return c.version
}

// Connect connects to the gRPC server and completes the session handshake.
// Returns the handshake error so callers can fail fast instead of discovering
// the broken state via SessionID() == "".
func (c *ClientImpl) Connect() error {
	c.conn.Connect()
	// Bound session establishment: 3×metaTimeout covers TLS negotiation and the
	// unary SessionService/Create RPC against a slow or half-open server —
	// without letting gmountie mount hang forever. The Keepalive stream is
	// opened non-blocking on its own long-lived context inside Establish, so
	// cancelling this ctx only bounds the unary Create; the stream is not torn
	// down when this deadline fires.
	ctx, cancel := context.WithTimeout(c.lifeCtx, 3*c.metaTimeout)
	defer cancel()
	if err := c.handshake.Establish(ctx); err != nil {
		log.Log.Error("session handshake failed", zap.Error(err))
		return err
	}
	return nil
}

// Close closes the gRPC ClientImpl connection. It also unregisters this
// client's metrics from the package-level dispatcher — without that, closed
// clients keep receiving OnRetry/cache fan-out forever and the dispatcher
// double-counts events once a second client registers. Idempotent: only the
// first Close tears down.
func (c *ClientImpl) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	if c.metrics != nil {
		metrics.UnregisterInstance(c.metrics)
	}
	if c.lifeCancel != nil {
		c.lifeCancel()
	}
	if c.handshake != nil {
		_ = c.handshake.Close()
	}
	return c.conn.Close()
}

// SessionID returns the server-assigned session id obtained during Connect.
// Returns "" if the handshake has not completed (Connect not called or failed),
// or if c.handshake is nil (pre-construction guard).
func (c *ClientImpl) SessionID() string {
	if c.handshake == nil {
		return ""
	}
	return c.handshake.SessionID()
}

// BootEpoch returns the server's per-process boot epoch received during the
// most recent successful session Create. Returns "" if Connect has not been
// called, failed, or if c.handshake is nil.
func (c *ClientImpl) BootEpoch() string {
	if c.handshake == nil {
		return ""
	}
	return c.handshake.BootEpoch()
}

// SessionLive reports whether the keepalive-backed session is currently healthy
// (i.e. a keepalive stream is open and draining). Returns false before Connect
// is called, during session recovery, and after Close. Used by BasicAuthCredentials
// to omit redundant basic-auth metadata on steady-state RPCs.
func (c *ClientImpl) SessionLive() bool {
	if c.handshake == nil {
		return false
	}
	return c.handshake.IsHealthy()
}

// MetaTimeout returns the per-RPC timeout for metadata operations.
func (c *ClientImpl) MetaTimeout() time.Duration {
	return c.metaTimeout
}

// IOTimeout returns the per-RPC timeout for data operations.
func (c *ClientImpl) IOTimeout() time.Duration {
	return c.ioTimeout
}

// RetryWindow returns the wall-clock budget for retrying a single FS op
// through transient failures. 0 means fail-fast (single attempt).
func (c *ClientImpl) RetryWindow() time.Duration { return c.retryWindow }

// Lifetime returns a context cancelled when the client is closed/unmounted.
// Long retries derive from it so they abort promptly on teardown.
func (c *ClientImpl) Lifetime() context.Context { return c.lifeCtx }

// PerFileConfig returns the bundled per-file knobs newly-opened GrpcFile
// instances inherit from this Client.
func (c *ClientImpl) PerFileConfig() PerFileConfig {
	return c.perFile
}

// Metrics returns the per-client collector set attached via WithMetrics
// (nil if none was attached). See the Client interface doc for how library
// consumers expose these via promhttp.
func (c *ClientImpl) Metrics() *metrics.Metrics {
	return c.metrics
}

// WhoAmI asks the server which identity the current OS user maps to on the
// given volume. The Caller is populated from os.Getuid / os.Getgid; this
// method is called at mount time, outside any FUSE op, so there is no
// FUSE context available.
func (c *ClientImpl) WhoAmI(ctx context.Context, volume string) (*proto.Identity, error) {
	return c.session.WhoAmI(ctx, &proto.WhoAmIRequest{
		Volume: volume,
		Caller: &proto.Caller{Owner: &proto.Owner{
			Uid: uint32(os.Getuid()),
			Gid: uint32(os.Getgid()),
		}},
	})
}

// sessionIDUnaryInterceptor injects the current session_id into outgoing unary
// RPC metadata. It is a no-op when SessionID() is empty (i.e. during the
// initial SessionService/Create call) so Create hits full argon2 auth.
func (c *ClientImpl) sessionIDUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if id := c.SessionID(); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataSessionID, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// sessionIDStreamInterceptor injects the current session_id into outgoing
// stream metadata. Read/Write/Subscribe/Keepalive are streams and must carry
// the session_id so they hit the fast path on the server.
func (c *ClientImpl) sessionIDStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if id := c.SessionID(); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, common.MetadataSessionID, id)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// getInterceptors returns the ClientImpl unary interceptors, including any
// extras registered via WithUnaryInterceptors. Built-ins (request-id,
// logging, session-id) run first; the per-instance in-flight interceptor
// (when metrics are attached via WithMetrics) runs next; extras run closest
// to the invoker.
func (c *ClientImpl) getInterceptors() []grpc.UnaryClientInterceptor {
	opts := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
	}
	base := []grpc.UnaryClientInterceptor{
		commongrpc.ClientUnaryRequestID(),
		logging.UnaryClientInterceptor(commongrpc.InterceptorLogger(log.Log), opts...),
		c.sessionIDUnaryInterceptor(),
	}
	if c.metrics != nil {
		base = append(base, UnaryClientInFlightInterceptor(c.metrics))
	}
	return append(base, c.extraInterceptors...)
}

// getDialOptions returns the dial options. Transport credentials must be
// provided via WithDialOptions (see factory.go); there is no insecure fallback.
func (c *ClientImpl) getDialOptions() []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(
			c.getInterceptors()...,
		),
		grpc.WithChainStreamInterceptor(
			c.sessionIDStreamInterceptor(),
		),
	}

	// Append the dial options (includes transport credentials from factory).
	opts = append(opts, c.dialOptions...)
	return opts
}
