// Package credentials decodes the single-blob mount credential the cloud
// service hands a device: a base64-encoded JSON object carrying this device's
// mTLS client certificate/key, the data CA that verifies the server, the
// mount/resolver endpoint, and the TLS verification name. Decoding it lets the
// client mount with no config file and no dummy basic-auth — the client
// certificate alone is the identity.
package credentials

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"

	"github.com/pkg/errors"
)

// Credentials is the decoded mount credential. CertPEM/KeyPEM are this device's
// mTLS client cert/key; CAPEM is the data CA that verifies the server; Endpoint
// is the "host:port" mount/resolver address; ServerName is the TLS SNI /
// verification name.
type Credentials struct {
	CertPEM    string `json:"cert_pem"`
	KeyPEM     string `json:"key_pem"`
	CAPEM      string `json:"ca_pem"`
	Endpoint   string `json:"endpoint"`
	ServerName string `json:"server_name"`
}

// Decode base64-STD-decodes blob, JSON-unmarshals it into Credentials, and
// validates that every required field is present. server_name is optional (the
// client falls back to the endpoint host when it is empty).
func Decode(blob string) (*Credentials, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(blob))
	if err != nil {
		return nil, errors.Wrap(err, "decode credential blob: invalid base64")
	}

	var c Credentials
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, errors.Wrap(err, "decode credential blob: invalid JSON")
	}

	for field, val := range map[string]string{
		"cert_pem": c.CertPEM,
		"key_pem":  c.KeyPEM,
		"ca_pem":   c.CAPEM,
		"endpoint": c.Endpoint,
	} {
		if strings.TrimSpace(val) == "" {
			return nil, errors.Errorf("decode credential blob: missing required field %q", field)
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
