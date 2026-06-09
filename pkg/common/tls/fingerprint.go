// Package tls holds the TLS vocabulary shared by client and server: the
// SSH-style certificate fingerprint both sides must compute identically
// (the server prints it via `gmountie fingerprint` / at startup; the client
// pins and verifies it in `verify`/`tofu` modes).
package tls

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/pem"

	"github.com/pkg/errors"
)

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
