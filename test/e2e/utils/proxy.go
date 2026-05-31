package utils

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/pkg/errors"
)

// connPair holds the two ends of a proxied connection so Sever can close both.
type connPair struct {
	client   net.Conn
	upstream net.Conn
}

// TCPProxy is a controllable loopback forwarder for failure-mode tests. It
// listens on 127.0.0.1:0 and forwards bidirectionally to upstream. Sever
// drops all active connections and pauses accepting new ones (simulating a
// network drop); Restore resumes accepting. The server behind upstream stays
// up the whole time, so a drop within the session grace period is
// reclaimable by the client's keepalive recovery loop.
type TCPProxy struct {
	upstream string
	lis      net.Listener

	mu      sync.Mutex
	severed bool
	closed  bool
	conns   map[*connPair]struct{}
}

// NewTCPProxy creates a TCPProxy that listens on 127.0.0.1:0 and forwards
// traffic to upstream (host:port). The proxy begins accepting connections
// immediately; call Addr to learn the listen address.
func NewTCPProxy(upstream string) (*TCPProxy, error) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, errors.Wrap(err, "TCPProxy: listen")
	}
	p := &TCPProxy{
		upstream: upstream,
		lis:      lis,
		conns:    make(map[*connPair]struct{}),
	}
	go p.acceptLoop()
	return p, nil
}

// Addr returns the proxy's listen address (e.g. "127.0.0.1:12345").
func (p *TCPProxy) Addr() string {
	return p.lis.Addr().String()
}

// Sever drops all active connections and stops forwarding new ones. Any
// in-flight reads or writes on client connections will receive an immediate
// error (EOF or reset). Subsequent dials to the proxy are accepted and then
// closed without forwarding until Restore is called.
func (p *TCPProxy) Sever() {
	p.mu.Lock()
	p.severed = true
	pairs := make([]*connPair, 0, len(p.conns))
	for cp := range p.conns {
		pairs = append(pairs, cp)
	}
	p.mu.Unlock()

	for _, cp := range pairs {
		_ = cp.client.Close()
		_ = cp.upstream.Close()
	}
}

// Restore resumes accepting and forwarding new connections. Connections that
// were severed are not recovered; callers must dial the proxy again.
func (p *TCPProxy) Restore() {
	p.mu.Lock()
	p.severed = false
	p.mu.Unlock()
}

// Close shuts down the proxy listener and drops all active connections.
// The accept loop exits when the listener close returns an error.
func (p *TCPProxy) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	pairs := make([]*connPair, 0, len(p.conns))
	for cp := range p.conns {
		pairs = append(pairs, cp)
	}
	p.mu.Unlock()

	for _, cp := range pairs {
		_ = cp.client.Close()
		_ = cp.upstream.Close()
	}
	return p.lis.Close()
}

// acceptLoop runs until the listener is closed. It accepts connections and,
// if not severed, dials upstream and starts bidirectional forwarding.
func (p *TCPProxy) acceptLoop() {
	for {
		client, err := p.lis.Accept()
		if err != nil {
			// Listener closed — exit cleanly.
			return
		}
		p.mu.Lock()
		sev := p.severed
		p.mu.Unlock()

		if sev {
			// While severed: accept but immediately close so the dialer gets
			// a clean connection-refused-equivalent rather than hanging.
			_ = client.Close()
			continue
		}

		up, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.upstream)
		if err != nil {
			_ = client.Close()
			continue
		}

		cp := &connPair{client: client, upstream: up}
		p.mu.Lock()
		p.conns[cp] = struct{}{}
		p.mu.Unlock()

		go p.forward(cp)
	}
}

// forward runs two io.Copy goroutines between the client and upstream
// connections, then removes the pair from the tracking map when both
// directions finish.
func (p *TCPProxy) forward(cp *connPair) {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = io.Copy(cp.upstream, cp.client)
		// Signal the other direction to stop.
		_ = cp.upstream.Close()
	}()

	go func() {
		defer wg.Done()
		_, _ = io.Copy(cp.client, cp.upstream)
		// Signal the other direction to stop.
		_ = cp.client.Close()
	}()

	wg.Wait()

	p.mu.Lock()
	delete(p.conns, cp)
	p.mu.Unlock()
}
