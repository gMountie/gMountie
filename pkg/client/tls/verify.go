// Package tls owns the client's TLS verification policy: strict chain
// verification, trust-on-first-use pinning, an explicit fingerprint pin,
// or skip-verification for prototyping. See Phase 7 spec §3.2.
package tls

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"os"

	servertls "gmountie/pkg/server/tls"
)

// Mode constants for TLS verification policy.
const (
	ModeVerify   = "verify"
	ModeTOFU     = "tofu"
	ModeInsecure = "insecure"
)

// Config is the inputs the client provides; mirrors the YAML TLSConfig.
// Endpoint is "host:port" — used as the known_hosts key for TOFU and as the
// server name when ServerName is empty.
type Config struct {
	Endpoint            string
	Mode                string // "verify" | "tofu" | "insecure" — empty = "verify"
	CAFile              string
	ExpectedFingerprint string
	ServerName          string
	KnownHostsPath      string // overridable; default $XDG_STATE_HOME/gmountie/known_hosts
	CertFile            string // client cert for mTLS; both CertFile and KeyFile required together, or neither
	KeyFile             string // client key for mTLS; both CertFile and KeyFile required together, or neither
}

// BuildConfig returns a *tls.Config wired with the right verification policy
// for cfg.Mode and cfg.ExpectedFingerprint.
func BuildConfig(cfg Config) (*tls.Config, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = ModeVerify
	}
	serverName := cfg.ServerName
	if serverName == "" {
		host, _ := splitHostPort(cfg.Endpoint)
		serverName = host
	}
	out := &tls.Config{
		MinVersion: tls.VersionTLS13,
		ServerName: serverName,
	}
	switch mode {
	case ModeVerify:
		if cfg.CAFile != "" {
			pool := x509.NewCertPool()
			ca, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("read ca_file %q: %w", cfg.CAFile, err)
			}
			if !pool.AppendCertsFromPEM(ca) {
				return nil, fmt.Errorf("no certs found in ca_file %q", cfg.CAFile)
			}
			out.RootCAs = pool
		}
		// Also enforce ExpectedFingerprint when set, ALONGSIDE chain verify.
		if cfg.ExpectedFingerprint != "" {
			out.VerifyPeerCertificate = pinVerifier(cfg.ExpectedFingerprint, nil, cfg.Endpoint)
		}
	case ModeTOFU:
		out.InsecureSkipVerify = true // we replace chain verify with the pin check
		kh, err := openKnownHosts(cfg.KnownHostsPath)
		if err != nil {
			return nil, err
		}
		// ExpectedFingerprint short-circuits TOFU: explicit pins win. Otherwise
		// the callback resolves the pin per-connection via known_hosts so the
		// second connect after a first-connect Pin actually sees the stored
		// value (capturing the lookup result here would freeze it at "missing"
		// for the lifetime of the *tls.Config).
		out.VerifyPeerCertificate = pinVerifier(cfg.ExpectedFingerprint, kh, cfg.Endpoint)
	case ModeInsecure:
		out.InsecureSkipVerify = true
	default:
		return nil, fmt.Errorf("unknown tls.verify mode %q", mode)
	}

	// mTLS: present a client certificate when configured. Both paths required
	// together — one without the other is a config error.
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return nil, fmt.Errorf("client mTLS requires both cert_file and key_file (got cert_file=%q key_file=%q)", cfg.CertFile, cfg.KeyFile)
	}
	if cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		out.Certificates = append(out.Certificates, cert)
	}

	return out, nil
}

// pinVerifier returns a tls.Config.VerifyPeerCertificate callback that
// compares the server's leaf-cert SHA-256 to the pinned fingerprint, with
// three resolution paths:
//
//   - When `expected` is non-empty, that's the static pin — mismatch fails.
//   - Otherwise when `kh` is non-nil (TOFU), each call resolves the pin via
//     `kh.Lookup(endpoint)`. A hit + match → OK; hit + mismatch → "fingerprint
//     changed"; miss → first-connect, `kh.Pin` it and accept.
//   - When `expected` is empty AND `kh` is nil this callback should not be
//     set by BuildConfig (it's the ModeVerify-without-pin path).
func pinVerifier(expected string, kh *KnownHosts, endpoint string) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return fmt.Errorf("server presented no certificate")
		}
		leafPEM := derToPEM(rawCerts[0])
		got, err := servertls.Fingerprint(leafPEM)
		if err != nil {
			return fmt.Errorf("fingerprint leaf: %w", err)
		}
		// Static pin (verify mode with ExpectedFingerprint, or TOFU with
		// ExpectedFingerprint short-circuit).
		if expected != "" {
			if got != expected {
				return fingerprintChangedErr(endpoint, kh, got, expected)
			}
			return nil
		}
		// TOFU: resolve the pin per-connection so later connects see what
		// earlier connects pinned.
		if kh != nil {
			if pinned, ok := kh.Lookup(endpoint); ok {
				if got != pinned {
					return fingerprintChangedErr(endpoint, kh, got, pinned)
				}
				return nil
			}
			// First connect — pin and accept.
			if err := kh.Pin(endpoint, got); err != nil {
				return fmt.Errorf("pin %s -> %s in known_hosts: %w", endpoint, got, err)
			}
			return nil
		}
		return fmt.Errorf("pinVerifier installed with neither expected fingerprint nor known_hosts (programmer error)")
	}
}

func fingerprintChangedErr(endpoint string, kh *KnownHosts, got, want string) error {
	khPath := ""
	if kh != nil {
		khPath = kh.path
	}
	return fmt.Errorf("server cert fingerprint changed; if this is intentional, remove the entry for %q from %s and re-pin (have %s, want %s)",
		endpoint, khPath, got, want)
}

// derToPEM encodes a DER certificate block to PEM.
func derToPEM(der []byte) []byte {
	var buf bytes.Buffer
	_ = pem.Encode(&buf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	return buf.Bytes()
}

// splitHostPort wraps net.SplitHostPort, returning the bare input on parse failure.
func splitHostPort(hostport string) (host, port string) {
	h, p, err := net.SplitHostPort(hostport)
	if err != nil {
		return hostport, ""
	}
	return h, p
}
