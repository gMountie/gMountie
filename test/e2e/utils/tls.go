package utils

import (
	"crypto/tls"

	commontls "go.gmountie.dev/gmountie/pkg/common/tls"
	servertls "go.gmountie.dev/gmountie/pkg/server/tls"
)

// EphemeralTLS wraps an in-memory self-signed cert for one test app context.
type EphemeralTLS struct {
	ServerCreds         tls.Certificate // pass into credentials.NewTLS on the server
	ExpectedFingerprint string          // the SHA256: pin the client should expect (unused in PR 1; T6 uses it)
}

// NewEphemeralTLS generates a fresh ECDSA P-256 cert valid for `host`. Each
// AppTestingContext gets its own so tests are isolated from one another.
func NewEphemeralTLS(host string) (*EphemeralTLS, error) {
	certPEM, keyPEM, err := servertls.Generate(host)
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	fp, err := commontls.Fingerprint(certPEM)
	if err != nil {
		return nil, err
	}
	return &EphemeralTLS{ServerCreds: cert, ExpectedFingerprint: fp}, nil
}
