package utils

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// TCPProxySuite tests TCPProxy behaviour in isolation: bidirectional
// forwarding, connection drop on Sever, and reconnect after Restore.
type TCPProxySuite struct {
	suite.Suite

	echoLis net.Listener // trivial echo upstream
	proxy   *TCPProxy
}

// startEchoServer starts a trivial echo upstream and returns its listener.
// Each accepted connection copies its input back until closed.
func startEchoServer(t *testing.T) net.Listener {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo server listen: %v", err)
	}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return // listener closed
			}
			go func() {
				_, _ = io.Copy(c, c)
				_ = c.Close()
			}()
		}
	}()
	return lis
}

func (s *TCPProxySuite) SetupSuite() {
	s.echoLis = startEchoServer(s.T())
	p, err := NewTCPProxy(s.echoLis.Addr().String())
	s.Require().NoError(err, "NewTCPProxy")
	s.proxy = p
}

func (s *TCPProxySuite) TearDownSuite() {
	if s.proxy != nil {
		_ = s.proxy.Close()
	}
	if s.echoLis != nil {
		_ = s.echoLis.Close()
	}
}

// TestForwardsThenSeverDropsConn verifies that traffic flows through the
// proxy before Sever, and that the active connection receives an error
// immediately after Sever is called.
func (s *TCPProxySuite) TestForwardsThenSeverDropsConn() {
	conn, err := net.DialTimeout("tcp", s.proxy.Addr(), 2*time.Second)
	s.Require().NoError(err, "dial proxy")
	defer conn.Close()

	// Write a small payload and read it back (echo works before Sever).
	payload := []byte("hello gMountie")
	_, err = conn.Write(payload)
	s.Require().NoError(err, "write before sever")

	buf := make([]byte, len(payload))
	_, err = io.ReadFull(conn, buf)
	s.Require().NoError(err, "read echo before sever")
	s.Require().Equal(payload, buf, "echo content")

	// Sever all active connections.
	s.proxy.Sever()
	defer s.proxy.Restore()

	// The in-flight connection must now return an error on the next read.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(buf)
	s.Require().Error(err, "expected error after Sever; got nil")
}

// TestRestoreAcceptsNewConn verifies that after Sever+Restore, a fresh
// dial to the proxy succeeds and echo traffic flows again.
func (s *TCPProxySuite) TestRestoreAcceptsNewConn() {
	s.proxy.Sever()
	s.proxy.Restore()

	conn, err := net.DialTimeout("tcp", s.proxy.Addr(), 2*time.Second)
	s.Require().NoError(err, "dial proxy after Restore")
	defer conn.Close()

	payload := []byte("recovered!")
	_, err = conn.Write(payload)
	s.Require().NoError(err, "write after Restore")

	buf := make([]byte, len(payload))
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	s.Require().NoError(err, "read echo after Restore")
	s.Require().Equal(payload, buf, "echo content after Restore")
}

func TestTCPProxySuite(t *testing.T) {
	suite.Run(t, new(TCPProxySuite))
}
