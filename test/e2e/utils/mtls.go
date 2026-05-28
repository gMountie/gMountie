package utils

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"time"
)

// TestCA holds the PEM-encoded artifacts from GenerateTestCA.
// All certs are signed by the same in-memory CA, so RequireAndVerifyClientCert
// accepts every client cert and the ACL layer is the gating mechanism.
type TestCA struct {
	CACertPEM   []byte            // CA cert (pass to server client_ca_file / client RootCAs)
	ServerCert  []byte            // server leaf cert PEM (SAN: 127.0.0.1)
	ServerKey   []byte            // server leaf key PEM
	ClientCerts map[string][]byte // CN → client leaf cert PEM
	ClientKeys  map[string][]byte // CN → client leaf key PEM
}

// GenerateTestCA creates a fresh in-memory CA, a server leaf cert/key (SAN
// 127.0.0.1), and one or more client leaf certs (one per principal name).
// Client certs use ExtKeyUsageClientAuth; server cert uses ExtKeyUsageServerAuth.
// All certs are ECDSA P-256 and valid for 24 hours.
func GenerateTestCA(principals ...string) (*TestCA, error) {
	// --- CA key + cert ---
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	caSerial, err := randSerial()
	if err != nil {
		return nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{CommonName: "gmountie-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return nil, err
	}
	caCertPEM := certPEM(caDER)

	// --- server leaf ---
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serverSerial, err := randSerial()
	if err != nil {
		return nil, err
	}
	serverTmpl := &x509.Certificate{
		SerialNumber: serverSerial,
		Subject:      pkix.Name{CommonName: "gmountie-test-server"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyAgreement,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	serverKeyPEM, err := ecKeyPEM(serverKey)
	if err != nil {
		return nil, err
	}

	// --- client leafs ---
	clientCerts := make(map[string][]byte, len(principals))
	clientKeys := make(map[string][]byte, len(principals))
	for _, cn := range principals {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, err
		}
		serial, err := randSerial()
		if err != nil {
			return nil, err
		}
		tmpl := &x509.Certificate{
			SerialNumber: serial,
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			return nil, err
		}
		keyPEM, err := ecKeyPEM(key)
		if err != nil {
			return nil, err
		}
		clientCerts[cn] = certPEM(der)
		clientKeys[cn] = keyPEM
	}

	return &TestCA{
		CACertPEM:   caCertPEM,
		ServerCert:  certPEM(serverDER),
		ServerKey:   serverKeyPEM,
		ClientCerts: clientCerts,
		ClientKeys:  clientKeys,
	}, nil
}

// randSerial returns a random 128-bit serial number for a certificate.
func randSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

// certPEM encodes a DER certificate to PEM.
func certPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// ecKeyPEM encodes an ECDSA private key to PEM.
func ecKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}), nil
}
