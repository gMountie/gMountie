// Package credentials decodes the single-blob mount credential the cloud
// service hands a device: a base64-encoded JSON object in one of two forms.
// Static mode carries this device's mTLS client certificate/key, the data CA,
// the mount/resolver endpoint, and optionally the TLS verification name.
// Token mode carries just the endpoint, a renew_endpoint, and a renew_token;
// the certificate refresher mints certs on demand using those fields — the CA
// and cert identity arrive at runtime via the refresher's profile exchange.
// Decoding a credential lets the client mount with no config file and no dummy
// basic-auth — the client certificate is the identity in both modes.
package credentials

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"

	"github.com/pkg/errors"
)

// Credentials is the decoded mount credential. Two forms:
//   - static: CertPEM/KeyPEM/CAPEM carry this device's mTLS material;
//   - token: RenewEndpoint/RenewToken configure the certificate refresher —
//     certs are minted on demand and held in memory; the CA arrives via the
//     refresher's profile exchange.
//
// Endpoint is the "host:port" mount/resolver address in both forms.
type Credentials struct {
	CertPEM       string `json:"cert_pem,omitempty"`
	KeyPEM        string `json:"key_pem,omitempty"`
	CAPEM         string `json:"ca_pem,omitempty"`
	Endpoint      string `json:"endpoint"`
	ServerName    string `json:"server_name,omitempty"`
	RenewEndpoint string `json:"renew_endpoint,omitempty"`
	RenewToken    string `json:"renew_token,omitempty"`
}

// TokenMode reports whether this credential configures the refresher
// instead of carrying static cert material.
func (c *Credentials) TokenMode() bool { return c.RenewEndpoint != "" }

// Decode base64-STD-decodes blob, JSON-unmarshals it into Credentials, and
// validates that every required field is present. server_name is optional (the
// client falls back to the endpoint host when it is empty). In token mode only
// renew_endpoint + renew_token are required; in static mode cert_pem/key_pem/
// ca_pem are required instead.
func Decode(blob string) (*Credentials, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(blob))
	if err != nil {
		return nil, errors.Wrap(err, "decode credential blob: invalid base64")
	}

	var c Credentials
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, errors.Wrap(err, "decode credential blob: invalid JSON")
	}

	if strings.TrimSpace(c.Endpoint) == "" {
		return nil, errors.New("decode credential blob: missing required field \"endpoint\"")
	}
	if c.TokenMode() {
		if c.CertPEM != "" || c.KeyPEM != "" || c.CAPEM != "" {
			return nil, errors.New("decode credential blob: renew_endpoint set together with static cert material; a credential is token-mode or static, not both")
		}
		if strings.TrimSpace(c.RenewToken) == "" {
			return nil, errors.New("decode credential blob: renew_endpoint set but renew_token missing")
		}
	} else {
		for field, val := range map[string]string{
			"cert_pem": c.CertPEM, "key_pem": c.KeyPEM, "ca_pem": c.CAPEM,
		} {
			if strings.TrimSpace(val) == "" {
				return nil, errors.Errorf("decode credential blob: missing required field %q", field)
			}
		}
	}

	return &c, nil
}

// Load picks a credential source and decodes it. A non-empty file path wins: the
// file is read and its contents (whitespace-trimmed) decoded. Otherwise a
// non-empty env value is decoded. When neither is supplied it returns
// (nil, nil) — no credential was provided, which is not an error.
func Load(env, file string) (*Credentials, error) {
	var blob string
	switch {
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, errors.Wrapf(err, "read credential file %s", file)
		}
		blob = strings.TrimSpace(string(data))
	case env != "":
		blob = env
	default:
		// No credential supplied is not an error: the caller falls back to the
		// config file / flags. (nil, nil) is the deliberate "absent" signal.
		return nil, nil //nolint:nilnil // "no credential" is a valid, non-error absence
	}

	return Decode(blob)
}
