package grpc

import (
	"context"
	"crypto/tls"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/controller"
	"go.gmountie.dev/gmountie/pkg/server/service"
	servertls "go.gmountie.dev/gmountie/pkg/server/tls"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

type FactoryTestSuite struct {
	suite.Suite

	// addr is the address of a real in-process gRPC server that has the
	// SessionService registered. Built once in SetupSuite. NewClientFromConfig
	// now invokes Connect() and requires a working handshake, so unit tests
	// can no longer point at a dead address.
	addr   string
	server *grpc.Server
}

func (s *FactoryTestSuite) SetupSuite() {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)

	// Phase 7 PR 1: the client always dials TLS. Spin up an ephemeral
	// self-signed cert and configure the test server with it; the client
	// uses Mode=insecure so it skips chain verification. Test/e2e/utils
	// gets the full fixture treatment in T5; this is the minimum to keep
	// pkg/client/grpc unit tests passing.
	certPEM, keyPEM, err := servertls.Generate("127.0.0.1")
	s.Require().NoError(err)
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	s.Require().NoError(err)
	creds := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2"},
		Certificates: []tls.Certificate{cert},
	})

	srv := grpc.NewServer(grpc.Creds(creds))
	sessMgr := service.NewSessionManager(service.SessionManagerOptions{})
	proto.RegisterSessionServiceServer(srv, controller.NewSessionController(sessMgr, nil, "test-epoch", nil))

	go func() {
		_ = srv.Serve(lis)
	}()

	s.addr = lis.Addr().String()
	s.server = srv
}

func (s *FactoryTestSuite) TearDownSuite() {
	if s.server != nil {
		s.server.Stop()
	}
}

// hostPort splits the suite's listener address into host and integer port.
func (s *FactoryTestSuite) hostPort() (string, int) {
	host, portStr, err := net.SplitHostPort(s.addr)
	s.Require().NoError(err)
	port, err := strconv.Atoi(portStr)
	s.Require().NoError(err)
	return host, port
}

func (s *FactoryTestSuite) endpoint() string {
	host, port := s.hostPort()
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func (s *FactoryTestSuite) TestNewClientFromConfig_NilConfig() {
	// Test with nil config
	client, err := NewClientFromConfig(nil)
	s.Require().Error(err)
	s.Nil(client)

	// Test with empty config
	client, err = NewClientFromConfig(&config.Config{})
	s.Require().Error(err)
	s.Nil(client)
}

func (s *FactoryTestSuite) TestNewClientFromConfig_BasicAuth() {
	host, port := s.hostPort()
	cfg := &config.Config{
		Server: &config.ServerConfig{
			Address: host,
			Port:    uint(port),
			TLS:     config.TLSConfig{Verify: "insecure"},
		},
		Auth: &config.BasicAuthConfig{
			BasicAuthConfigUser: config.BasicAuthConfigUser{
				Username: "testuser",
				Password: "testpass",
			},
		},
	}

	client, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	s.Require().NotNil(client)
	defer client.Close()
	s.Equal(s.endpoint(), client.GetEndpoint())
	s.NotEmpty(client.SessionID(), "factory must establish a session")
}

func (s *FactoryTestSuite) TestCreateEndpoint() {
	cfg := &config.ServerConfig{
		Address: "localhost",
		Port:    9449,
	}

	endpoint := createEndpoint(cfg)
	s.Equal("localhost:9449", endpoint)
}

// TestNewClientFromConfig_TimeoutsApplied verifies the configured RPC
// timeouts are propagated onto the constructed client.
func (s *FactoryTestSuite) TestNewClientFromConfig_TimeoutsApplied() {
	host, port := s.hostPort()
	cfg := &config.Config{
		Server: &config.ServerConfig{Address: host, Port: uint(port), TLS: config.TLSConfig{Verify: "insecure"}},
		Auth: &config.BasicAuthConfig{
			BasicAuthConfigUser: config.BasicAuthConfigUser{
				Username: "testuser",
				Password: "testpass",
			},
		},
		Rpc: &config.RpcConfig{
			TimeoutMeta:     2 * time.Second,
			TimeoutIO:       90 * time.Second,
			MaxMessageBytes: config.DefaultMaxMessageBytes,
			Keepalive: config.ClientKeepaliveConfig{
				Time:                config.DefaultKeepaliveTime,
				Timeout:             config.DefaultKeepaliveTimeout,
				PermitWithoutStream: config.DefaultKeepalivePermitWithoutStream,
			},
		},
	}

	c, err := NewClientFromConfig(cfg)
	s.Require().NoError(err)
	defer c.Close()

	s.Assert().Equal(2*time.Second, c.MetaTimeout())
	s.Assert().Equal(90*time.Second, c.IOTimeout())
}

// TestNewClientFromConfig_HandshakeFailureReturnsError verifies that when the
// session handshake fails (no server at the address), the factory returns an
// error and a nil client rather than handing back a half-built client.
func (s *FactoryTestSuite) TestNewClientFromConfig_HandshakeFailureReturnsError() {
	// Bind a port then release it — high chance nothing is listening there.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	port := lis.Addr().(*net.TCPAddr).Port
	lis.Close()

	cfg := &config.Config{
		Server: &config.ServerConfig{Address: "127.0.0.1", Port: uint(port), TLS: config.TLSConfig{Verify: "insecure"}},
		Auth: &config.BasicAuthConfig{
			BasicAuthConfigUser: config.BasicAuthConfigUser{
				Username: "testuser",
				Password: "testpass",
			},
		},
	}

	client, err := NewClientFromConfig(cfg)
	s.Require().Error(err)
	s.Nil(client)
}

// silentConn wraps a net.Conn so that, once the test calls goSilent(),
// all subsequent reads block until `done` is closed by the test's
// teardown. We also poke SetReadDeadline so any already-pending Read
// returns immediately and the next iteration parks. The peer still
// believes the TCP connection is up (no FIN/RST), so the only signal
// it has is the HTTP/2 keepalive PING going unacked.
type silentConn struct {
	net.Conn
	silent *atomic.Bool
	done   <-chan struct{}
}

func (c *silentConn) Read(b []byte) (int, error) {
	if c.silent.Load() {
		<-c.done
		return 0, net.ErrClosed
	}
	n, err := c.Conn.Read(b)
	if err != nil && c.silent.Load() {
		// Pending Read was woken by SetReadDeadline. Park instead of
		// surfacing the deadline error, otherwise the server would tear
		// the conn down via its own read-error path and the test would
		// never exercise the keepalive timeout.
		<-c.done
		return 0, net.ErrClosed
	}
	return n, err
}

// silentListener hands out silentConn wrappers and gives the test a
// goSilent() hook that flips the shared `silent` flag and wakes any
// pending Read on every accepted connection.
type silentListener struct {
	net.Listener
	silent *atomic.Bool
	done   <-chan struct{}

	mu    sync.Mutex
	conns []*silentConn
}

func (l *silentListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	sc := &silentConn{Conn: c, silent: l.silent, done: l.done}
	l.mu.Lock()
	l.conns = append(l.conns, sc)
	l.mu.Unlock()
	return sc, nil
}

func (l *silentListener) goSilent() {
	l.silent.Store(true)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		// Wake any in-flight Read. Past read will return a deadline
		// error, the wrapper sees silent==true and parks instead.
		_ = c.SetReadDeadline(time.Now())
	}
}

// TestKeepalive_DetectsDeadConnectionWithinTimeoutBudget verifies that a
// gRPC client built with the same keepalive dial options the factory
// installs surfaces a dead connection as Unavailable within
// ~Time+Timeout instead of hanging on the OS TCP timeout. Uses a
// wrapping listener whose Read() blocks once flipped, so the only signal
// of "dead network" is the HTTP/2 keepalive PING timing out. This
// bypasses NewClientFromConfig to keep the assertion focused on the
// keepalive wiring rather than the session-handshake path.
func (s *FactoryTestSuite) TestKeepalive_DetectsDeadConnectionWithinTimeoutBudget() {
	const (
		kaTime    = 200 * time.Millisecond
		kaTimeout = 200 * time.Millisecond
	)

	tcpLis, err := net.Listen("tcp", "127.0.0.1:0")
	s.Require().NoError(err)
	silent := &atomic.Bool{}
	done := make(chan struct{})
	var doneOnce sync.Once
	closeDone := func() { doneOnce.Do(func() { close(done) }) }
	defer closeDone()
	lis := &silentListener{Listener: tcpLis, silent: silent, done: done}

	// Real gRPC server with matching short keepalive params + a permissive
	// MinTime so the client's aggressive pings don't get GOAWAY'd.
	srv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    kaTime,
			Timeout: kaTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             50 * time.Millisecond,
			PermitWithoutStream: true,
		}),
	)
	sessMgr := service.NewSessionManager(service.SessionManagerOptions{})
	proto.RegisterSessionServiceServer(srv, controller.NewSessionController(sessMgr, nil, "test-epoch", nil))
	go func() { _ = srv.Serve(lis) }()
	defer func() {
		// Wake any parked silentConn.Read first, then stop the server so
		// it can shed open connections without us waiting on a TCP RST.
		closeDone()
		stopped := make(chan struct{})
		go func() {
			srv.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
		}
	}()

	// Dial with matching aggressive keepalive — same dial options the
	// factory would build given a short-keepalive RpcConfig.
	conn, err := grpc.NewClient(
		tcpLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                kaTime,
			Timeout:             kaTimeout,
			PermitWithoutStream: true,
		}),
	)
	s.Require().NoError(err)
	defer conn.Close()

	client := proto.NewSessionServiceClient(conn)

	// Warm the connection: a real RPC succeeds, proving the wiring works.
	warmCtx, warmCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer warmCancel()
	_, err = client.Create(warmCtx, &proto.SessionCreateRequest{})
	s.Require().NoError(err, "warm RPC must succeed before we sever the wire")

	// Cut the wire. Server-side reads are now parked, so the server can
	// neither receive client PINGs nor ACK them. The client should detect
	// the dead conn via its own keepalive timeout.
	lis.goSilent()

	// Give the client a generous wall-clock budget but assert it actually
	// fails within kaTime+kaTimeout plus a small slack — not after the OS
	// TCP timeout.
	deadline := time.Now().Add(kaTime + kaTimeout + 2*time.Second)
	rpcCtx, rpcCancel := context.WithDeadline(context.Background(), deadline)
	defer rpcCancel()

	start := time.Now()
	_, err = client.Create(rpcCtx, &proto.SessionCreateRequest{})
	elapsed := time.Since(start)
	s.Require().Error(err, "expected RPC to fail on dead connection (elapsed %s)", elapsed)
	st, ok := status.FromError(err)
	s.Require().Truef(ok, "expected gRPC status error, got %v (type %T)", err, err)
	// Either Unavailable (transport closed by keepalive) or DeadlineExceeded
	// is acceptable as a "detected the dead conn" signal. We additionally
	// require that we didn't hang for the full OS TCP timeout (~30s+).
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded:
	default:
		s.Failf("unexpected status code", "expected Unavailable or DeadlineExceeded, got %s: %v", st.Code(), err)
	}
	maxBudget := kaTime + kaTimeout + time.Second
	s.LessOrEqualf(elapsed, maxBudget, "dead-conn detection took %s, want <= %s", elapsed, maxBudget)
}

// TestResolveMetaTimeout_FloorAndOverride proves the pre-session connect/resolve
// budget never drops below resolveConnectFloor (a cold mTLS dial over a slow link
// needs more than the per-op metadata timeout), yet an explicitly larger
// timeout_meta still wins.
func (s *FactoryTestSuite) TestResolveMetaTimeout_FloorAndOverride() {
	// nil rpc: floor.
	s.Equal(resolveConnectFloor, resolveMetaTimeout(&config.Config{}))
	// default-sized per-op meta (below the floor): floor wins so the cold dial
	// isn't starved.
	s.Equal(resolveConnectFloor, resolveMetaTimeout(&config.Config{
		Rpc: &config.RpcConfig{TimeoutMeta: config.DefaultRpcTimeoutMeta},
	}))
	// zero is treated as unset: floor.
	s.Equal(resolveConnectFloor, resolveMetaTimeout(&config.Config{Rpc: &config.RpcConfig{}}))
	// an explicit larger value wins (user widened it for an even slower link).
	s.Equal(45*time.Second, resolveMetaTimeout(&config.Config{
		Rpc: &config.RpcConfig{TimeoutMeta: 45 * time.Second},
	}))
}

func TestFactoryTestSuite(t *testing.T) {
	suite.Run(t, new(FactoryTestSuite))
}
