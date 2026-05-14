package grpc

import (
	"time"

	"gmountie/pkg/proto"

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
}

// ClientImpl is a struct that holds the gRPC ClientImpl
type ClientImpl struct {
	endpoint    string
	conn        *grpc.ClientConn
	dialOptions []grpc.DialOption
	fs          proto.RpcFsClient
	file        proto.RpcFileClient
	volume      proto.VolumeServiceClient
	metaTimeout time.Duration
	ioTimeout   time.Duration
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
}

// Close closes the gRPC ClientImpl connection
func (c *ClientImpl) Close() error {
	return c.conn.Close()
}

// MetaTimeout returns the per-RPC timeout for metadata operations.
func (c *ClientImpl) MetaTimeout() time.Duration {
	return c.metaTimeout
}

// IOTimeout returns the per-RPC timeout for data operations.
func (c *ClientImpl) IOTimeout() time.Duration {
	return c.ioTimeout
}

// GetInterceptors returns the ClientImpl interceptors
func getInterceptors() []grpc.UnaryClientInterceptor {
	return []grpc.UnaryClientInterceptor{
		//logging.UnaryClientImplInterceptor(grpc2.InterceptorLogger(log.Log)),
	}
}

// getDialOptions returns the dial options
func (c *ClientImpl) getDialOptions() []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		//grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithChainUnaryInterceptor(
			getInterceptors()...,
		),
	}

	// Append the dial options
	opts = append(opts, c.dialOptions...)
	return opts
}
