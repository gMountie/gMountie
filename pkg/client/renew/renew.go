// Package renew implements a generic bearer-token → client-certificate
// refresher: GET {endpoint}/profile describes the identity to request
// (subject, SANs) and the CA to trust; the client builds a CSR for an
// in-memory keypair and POSTs it to {endpoint}/renew for a short-lived
// cert, swapped into a ManagedSource. The private key never leaves process
// memory. This is the ACME/step-ca "token → cert" pattern; any CSR-signing
// endpoint can implement it.
package renew

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	clienttls "go.gmountie.dev/gmountie/pkg/client/tls"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// Config configures the refresher.
type Config struct {
	Endpoint  string        // exchange base URL ({Endpoint}/profile, {Endpoint}/renew)
	Token     string        // inline bearer token; TokenFile wins when both set
	TokenFile string        // re-read on every exchange so the token can rotate
	Before    time.Duration // renewal lead time (consumed by Run, Task 5)
}

// Profile is the server's response to GET {endpoint}/profile.
type Profile struct {
	Subject string   `json:"subject"`
	SANs    []string `json:"sans"`
	CAPEM   string   `json:"ca_pem"`
}

// signResponse is the server's response to POST {endpoint}/renew.
type signResponse struct {
	CertChainPEM string    `json:"cert_chain_pem"`
	CAPEM        string    `json:"ca_pem"`
	NotAfter     time.Time `json:"not_after"`
}

// Refresher fetches a profile, generates an in-memory key, builds a CSR,
// and exchanges it for a signed certificate that it swaps into source.
type Refresher struct {
	cfg    Config
	source *clienttls.ManagedSource
	client *http.Client

	retryMin, retryMax time.Duration // Run's backoff bounds (Task 5); fields here so Task 5 needs no struct change

	mu       sync.Mutex
	notAfter time.Time
	caPEM    string
}

// New constructs a Refresher with a 30 s HTTP timeout and backoff bounds of
// [1 m, 15 m] for the Run loop (Task 5).
func New(cfg Config, source *clienttls.ManagedSource) *Refresher {
	return &Refresher{
		cfg:      cfg,
		source:   source,
		client:   &http.Client{Timeout: 30 * time.Second},
		retryMin: time.Minute,
		retryMax: 15 * time.Minute,
	}
}

// NotAfter returns the NotAfter of the most recently minted leaf certificate.
// Returns the zero time when no certificate has been minted yet.
func (r *Refresher) NotAfter() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.notAfter
}

// CAPEM returns the CA PEM delivered by the most recent successful exchange.
func (r *Refresher) CAPEM() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.caPEM
}

// token returns the bearer token to use for the current exchange. TokenFile
// wins over Token and is read and trimmed on every call so the token can
// rotate without a restart.
func (r *Refresher) token() (string, error) {
	if r.cfg.TokenFile != "" {
		b, err := os.ReadFile(r.cfg.TokenFile)
		if err != nil {
			return "", fmt.Errorf("read token file %q: %w", r.cfg.TokenFile, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	if r.cfg.Token != "" {
		return r.cfg.Token, nil
	}
	return "", fmt.Errorf("renew: no token or token_file configured")
}

// doJSON executes an HTTP request against {endpoint}{path}. When body is
// non-nil it is JSON-encoded and sent with Content-Type application/json.
// The Authorization header is set to "Bearer <tok>". Non-200 responses return
// an error that includes method, path, status code, and up to 512 bytes of
// the response body. Successful responses are JSON-decoded into out.
func (r *Refresher) doJSON(ctx context.Context, method, path string, body, out any, tok string) error {
	endpoint := strings.TrimRight(r.cfg.Endpoint, "/") + path

	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body for %s %s: %w", method, path, err)
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reqBody)
	if err != nil {
		return fmt.Errorf("build request %s %s: %w", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s %s: unexpected status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response from %s %s: %w", method, path, err)
		}
	}
	return nil
}

// classifySANs splits a list of SAN strings into URI SANs and DNS SANs.
// An entry that url.Parse succeeds on AND has a non-empty Scheme is a URI
// SAN; everything else is treated as a DNS SAN.
func classifySANs(sans []string) (uris []*url.URL, dns []string) {
	for _, s := range sans {
		if u, err := url.Parse(s); err == nil && u.Scheme != "" {
			uris = append(uris, u)
		} else {
			dns = append(dns, s)
		}
	}
	return uris, dns
}

// pairFromChain decodes all CERTIFICATE PEM blocks from chainPEM, parses the
// first as the leaf, and returns a tls.Certificate ready to be swapped into a
// ManagedSource. key is the in-memory private key generated for this exchange,
// and expectedSubject is the CN from the profile that produced the CSR.
//
// Wire contract: the chain is leaf-first (leaf at index 0, optional
// intermediates/CA after). The key-match check is what enforces this: a CA
// cert placed first can never equal the CSR key, so it will be caught here
// rather than surfacing opaquely as "private key does not match public key"
// at every subsequent TLS handshake.
func pairFromChain(chainPEM string, key *ecdsa.PrivateKey, expectedSubject string) (tls.Certificate, error) {
	var ders [][]byte
	rest := []byte(chainPEM)
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			ders = append(ders, block.Bytes)
		}
	}
	if len(ders) == 0 {
		return tls.Certificate{}, fmt.Errorf("renew: no certificates found in chain PEM")
	}
	leaf, err := x509.ParseCertificate(ders[0])
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("renew: parse leaf certificate: %w", err)
	}
	// Validate the leaf against the key and profile subject before accepting
	// the chain. A mismatch here indicates a hostile or buggy CA response.
	ecPub, ok := leaf.PublicKey.(*ecdsa.PublicKey)
	if !ok || !ecPub.Equal(&key.PublicKey) {
		return tls.Certificate{}, fmt.Errorf("renew: leaf public key mismatch: server returned a cert for a different key")
	}
	if leaf.Subject.CommonName != expectedSubject {
		return tls.Certificate{}, fmt.Errorf("renew: leaf CN mismatch: got %q, expected %q", leaf.Subject.CommonName, expectedSubject)
	}
	return tls.Certificate{
		Certificate: ders,
		PrivateKey:  key,
		Leaf:        leaf,
	}, nil
}

// RenewNow performs a single profile-fetch → CSR-generation → sign exchange
// and swaps the resulting certificate into the ManagedSource. It does not
// retry on failure; the Run loop (Task 5) owns retry policy.
//
// The flow:
//  1. GET {endpoint}/profile to learn subject and SANs.
//  2. Generate a fresh P-256 key in memory.
//  3. Build a CSR: CN=subject, DNS/URI SANs classified from profile.SANs.
//  4. POST {endpoint}/renew with the PEM-encoded CSR.
//  5. Parse the returned chain; the leaf's NotAfter is authoritative.
//  6. Swap into source; update cached caPEM + notAfter under mu.
func (r *Refresher) RenewNow(ctx context.Context) error {
	tok, err := r.token()
	if err != nil {
		return err
	}

	// Step 1 — fetch identity profile.
	var profile Profile
	if err := r.doJSON(ctx, http.MethodGet, "/profile", nil, &profile, tok); err != nil {
		return fmt.Errorf("renew: fetch profile: %w", err)
	}

	// Step 2 — generate in-memory P-256 key.
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("renew: generate key: %w", err)
	}

	// Step 3 — build CSR.
	uris, dns := classifySANs(profile.SANs)
	csrTpl := &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: profile.Subject},
		DNSNames: dns,
		URIs:     uris,
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, csrTpl, key)
	if err != nil {
		return fmt.Errorf("renew: create CSR: %w", err)
	}
	csrPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}))

	// Step 4 — submit CSR to sign endpoint.
	var signResp signResponse
	if err := r.doJSON(ctx, http.MethodPost, "/renew", map[string]string{"csr_pem": csrPEM}, &signResp, tok); err != nil {
		return fmt.Errorf("renew: sign exchange: %w", err)
	}

	// Step 5 — parse and validate the returned chain; leaf is authoritative for
	// expiry. pairFromChain enforces key-match + CN, rejecting mismatched chains
	// before any swap occurs.
	cert, err := pairFromChain(signResp.CertChainPEM, key, profile.Subject)
	if err != nil {
		return err
	}

	// Step 6 — swap into source, update cached state under mu.
	// Profile CA PEM is the primary delivery; the sign response CA is applied
	// after (with a warning if it differs from the profile value).
	r.source.Set(&cert)

	r.mu.Lock()
	if profile.CAPEM != "" {
		if r.caPEM != "" && r.caPEM != profile.CAPEM {
			log.Log.Warn("renew: CA PEM changed (profile); new CA will take effect on restart",
				zap.String("subject", profile.Subject))
		}
		r.caPEM = profile.CAPEM
	}
	if signResp.CAPEM != "" {
		if r.caPEM != "" && r.caPEM != signResp.CAPEM {
			log.Log.Warn("renew: CA PEM changed (sign response); new CA will take effect on restart",
				zap.String("subject", profile.Subject))
		}
		r.caPEM = signResp.CAPEM
	}
	r.notAfter = cert.Leaf.NotAfter
	r.mu.Unlock()

	log.Log.Info("client certificate renewed",
		zap.String("subject", profile.Subject),
		zap.Time("not_after", cert.Leaf.NotAfter))

	return nil
}
