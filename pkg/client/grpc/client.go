package grpc

import (
	"context"
	"time"

	commongrpc "gmountie/pkg/common/grpc"
	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is the interface for the gRPC Client.
type Client interface {
	// GetEndpoint returns the gRPC Client endpoint.
	GetEndpoint() string
	// Connect connects to the gRPC server.
	Connect()
	// Close closes the gRPC Client connection.
	Close() error
	// File returns the gRPC File client.
	File() proto.RpcFileClient
	// Fs returns the gRPC Fs client.
	Fs() proto.RpcFsClient
	// Volume returns the gRPC Volume client.
	Volume() proto.VolumeServiceClient
	// MetaTimeout returns the per-RPC timeout for metadata operations.
	MetaTimeout() time.Duration
	// IOTimeout returns the per-RPC timeout for data operations.
	IOTimeout() time.Duration
	// SessionID returns the server-assigned session id obtained during Connect.
	SessionID() string
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
	session           proto.SessionServiceClient
	handshake         *SessionHandshake
	metaTimeout       time.Duration
	ioTimeout         time.Duration
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

// WithBasicAuth sets the basic authentication for the gRPC ClientImpl
func WithBasicAuth(username, password string) ClientOption {
	return func(c *ClientImpl) {
		c.dialOptions = append(c.dialOptions, grpc.WithPerRPCCredentials(NewBasicAuthCredentials(username, password)))
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

// ---------------------- Constructor ----------------------

// NewClient creates a new gRPC ClientImpl
func NewClient(endpoint string, options ...ClientOption) (Client, error) {
	c := ClientImpl{
		endpoint:    endpoint,
		metaTimeout: 5 * time.Second,
		ioTimeout:   30 * time.Second,
	}
	for _, opt := range options {
		opt(&c)
	}
	conn, err := grpc.NewClient(
		endpoint,
		c.getDialOptions()...,
	)
	if err != nil {
		return nil, err
	}
	c.conn = conn
	c.file = proto.NewRpcFileClient(conn)
	c.fs = proto.NewRpcFsClient(conn)
	c.volume = proto.NewVolumeServiceClient(conn)
	c.session = proto.NewSessionServiceClient(conn)
	c.handshake = NewSessionHandshake(c.session)
	return &c, nil
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

// Connect connects to the gRPC server
func (c *ClientImpl) Connect() {
	c.conn.Connect()
	if err := c.handshake.Establish(context.Background()); err != nil {
		log.Log.Error("session handshake failed", zap.Error(err))
	}
}

// Close closes the gRPC ClientImpl connection
func (c *ClientImpl) Close() error {
	if c.handshake != nil {
		_ = c.handshake.Close()
	}
	return c.conn.Close()
}

// SessionID returns the server-assigned session id obtained during Connect.
// Returns "" if the handshake has not completed (Connect not called or failed).
func (c *ClientImpl) SessionID() string {
	return c.handshake.SessionID()
}

// MetaTimeout returns the per-RPC timeout for metadata operations.
func (c *ClientImpl) MetaTimeout() time.Duration {
	return c.metaTimeout
}

// IOTimeout returns the per-RPC timeout for data operations.
func (c *ClientImpl) IOTimeout() time.Duration {
	return c.ioTimeout
}

// getInterceptors returns the ClientImpl interceptors, including any
// extras registered via WithUnaryInterceptors. Built-ins (request-id,
// logging) run first; extras run closer to the invoker.
func (c *ClientImpl) getInterceptors() []grpc.UnaryClientInterceptor {
	opts := []logging.Option{
		logging.WithLogOnEvents(logging.FinishCall),
	}
	base := []grpc.UnaryClientInterceptor{
		commongrpc.ClientUnaryRequestID(),
		logging.UnaryClientInterceptor(commongrpc.InterceptorLogger(log.Log), opts...),
	}
	return append(base, c.extraInterceptors...)
}

// getDialOptions returns the dial options
func (c *ClientImpl) getDialOptions() []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithChainUnaryInterceptor(
			c.getInterceptors()...,
		),
	}

	// Append the dial options
	opts = append(opts, c.dialOptions...)
	return opts
}
