package config

import (
	"os"

	"github.com/pkg/errors"
)

// ValidateVolumePaths checks that every configured volume path exists and is a
// directory. Called at startup so misconfiguration fails fast with a clear
// message instead of surfacing as a cryptic error on the first I/O.
func (c *Config) ValidateVolumePaths() error {
	for _, v := range c.Volumes {
		info, err := os.Stat(v.Path)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.Errorf("volume %q: path does not exist: %s", v.Name, v.Path)
			}
			return errors.Wrapf(err, "volume %q: stat %s", v.Name, v.Path)
		}
		if !info.IsDir() {
			return errors.Errorf("volume %q: path is not a directory: %s", v.Name, v.Path)
		}
	}
	return nil
}
