package tls

import (
	"crypto/tls"
	"sync/atomic"
)

// CertSource supplies the client certificate dynamically at each TLS
// handshake (the tls.Config.GetClientCertificate contract). Implementations
// must never return (nil, nil): returning a pointer to an empty
// tls.Certificate declines client auth without failing the handshake.
type CertSource interface {
	GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error)
}

// ManagedSource is an in-memory, atomically swappable CertSource. A
// refresher swaps renewed cert+key pairs in with Set; handshakes read the
// current pair. The zero value is an empty source: handshakes send no client
// certificate until the first Set. Renewed material never touches disk.
type ManagedSource struct {
	current atomic.Pointer[tls.Certificate]
}

// Set atomically replaces the served certificate. The new pair is used from
// the next TLS handshake; established connections are not affected.
func (m *ManagedSource) Set(cert *tls.Certificate) { m.current.Store(cert) }

// Current returns the currently served certificate, or nil when none is set.
func (m *ManagedSource) Current() *tls.Certificate { return m.current.Load() }

// GetClientCertificate implements CertSource.
func (m *ManagedSource) GetClientCertificate(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	if c := m.current.Load(); c != nil {
		return c, nil
	}
	return &tls.Certificate{}, nil
}
