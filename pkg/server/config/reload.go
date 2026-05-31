package config

import (
	"os"

	"github.com/pkg/errors"
)

// ReloadFromFile re-parses the server config from path and records the path on
// the result (ConfigPath). Used at startup (so the running config knows its own
// source) and by POST /ops/acl/reload to pick up ACL + revoked_serials changes
// without a restart. Validation runs as in normal load; a bad file returns an
// error and the caller keeps the previous good config.
func ReloadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.Wrapf(err, "read config file %q", path)
	}
	cfg, err := LoadConfigFromString(string(data))
	if err != nil {
		return nil, err
	}
	cfg.ConfigPath = path
	return cfg, nil
}
