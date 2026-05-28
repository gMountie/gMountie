// Package tls owns the server-side certificate lifecycle: generating a
// self-signed ECDSA P-256 cert on first startup (SSH host-key pattern),
// loading an existing cert from disk, and computing the SSH-style SHA-256
// fingerprint used in client config and the `gmountie fingerprint` CLI.
package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"time"
)

// Generate creates a fresh self-signed ECDSA P-256 certificate for host.
// host may be a bare hostname/IP or a host:port pair; the port is stripped.
// Returns the PEM-encoded certificate and private key.
func Generate(host string) (certPEM, keyPEM []byte, err error) {
	// Strip port if present.
	bare, _, splitErr := net.SplitHostPort(host)
	if splitErr != nil {
		bare = host
	}

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	// Serial number: random 128-bit value.
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: bare},
		NotBefore:    now,
		NotAfter:     now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyAgreement,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Populate SAN: IP address literal or DNS name.
	if ip := net.ParseIP(bare); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{bare}
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	return certPEM, keyPEM, nil
}

// Load reads a certificate and key from disk and returns the parsed
// tls.Certificate together with the raw PEM bytes of the certificate
// (needed for Fingerprint without a second file read).
func Load(certPath, keyPath string) (tls.Certificate, []byte, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	return cert, certPEM, nil
}

// LoadOrGenerate atomically loads the certificate from disk if both files
// already exist, or generates a new self-signed cert for host, writes it
// (key 0600, cert 0644), and returns the result.
//
// The key is written with O_EXCL so that two concurrent server-starts
// cannot clobber each other: if EEXIST is returned the caller that lost
// the race falls through to Load.
func LoadOrGenerate(certPath, keyPath, host string) (tls.Certificate, []byte, string, error) {
	// Fast path: both files already exist.
	cert, certPEM, err := Load(certPath, keyPath)
	if err == nil {
		fp, fpErr := Fingerprint(certPEM)
		if fpErr != nil {
			return tls.Certificate{}, nil, "", fpErr
		}
		return cert, certPEM, fp, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return tls.Certificate{}, nil, "", err
	}

	// Generate a fresh cert.
	certPEM, keyPEM, err := Generate(host)
	if err != nil {
		return tls.Certificate{}, nil, "", err
	}

	// Write key (O_EXCL — first writer wins).
	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Another process won the race; load what it wrote.
			cert, certPEM, loadErr := Load(certPath, keyPath)
			if loadErr != nil {
				return tls.Certificate{}, nil, "", loadErr
			}
			fp, fpErr := Fingerprint(certPEM)
			if fpErr != nil {
				return tls.Certificate{}, nil, "", fpErr
			}
			return cert, certPEM, fp, nil
		}
		return tls.Certificate{}, nil, "", err
	}
	if _, err = keyFile.Write(keyPEM); err != nil {
		keyFile.Close()
		return tls.Certificate{}, nil, "", err
	}
	if err = keyFile.Close(); err != nil {
		return tls.Certificate{}, nil, "", err
	}

	// Write cert (not exclusive — if the key existed the cert might too,
	// but since we took the O_EXCL lock on the key we're the sole writer).
	if err = os.WriteFile(certPath, certPEM, 0o644); err != nil {
		return tls.Certificate{}, nil, "", err
	}

	// Parse what we just wrote.
	parsedCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, "", err
	}

	fp, err := Fingerprint(certPEM)
	if err != nil {
		return tls.Certificate{}, nil, "", err
	}

	return parsedCert, certPEM, fp, nil
}

// LoadCertOnly reads only the cert PEM file. Used by callers (e.g. the
// `gmountie fingerprint` subcommand) that need to fingerprint without
// also requiring the private key on disk.
func LoadCertOnly(certPath string) ([]byte, error) {
	return os.ReadFile(certPath)
}

// Fingerprint returns the SSH-style SHA-256 fingerprint of a PEM-encoded
// certificate: "SHA256:<base64-raw-no-padding>".
//
// This matches the output of:
//
//	openssl x509 -in cert.pem -outform DER | openssl dgst -sha256 -binary | base64
//
// (minus the trailing "=" padding and the "SHA256:" prefix that openssl
// does not print).
func Fingerprint(certPEM []byte) (string, error) {
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return "", errors.New("tls: no PEM block found in cert data")
	}
	sum := sha256.Sum256(block.Bytes)
	return "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:]), nil
}
