package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/adrg/xdg"
)

const (
	DefaultConfigDirName        = "gmountie"
	DefaultServerConfigFileName = "server.yaml"
	DefaultClientConfigFileName = "client.yaml"
	DefaultMountDirName         = "gMountie"
)

// GetDefaultConfigPath returns the default config file path based on the OS
func GetDefaultConfigPath(configName string) string {
	configDir := GetDefaultConfigDir()
	return filepath.Join(configDir, configName)
}

// GetDefaultConfigDir returns the default config directory for Linux
func GetDefaultConfigDir() string {
	xdg.Reload()
	path := xdg.ConfigHome
	return filepath.Join(path, DefaultConfigDirName)
}

// EnsureConfigDir creates the config directory if it doesn't exist
func EnsureConfigDir() error {
	configDir := GetDefaultConfigDir()
	return os.MkdirAll(configDir, 0755)
}

// WriteDefaultConfig writes the default config to the default location
func WriteDefaultConfig(configName, content string) error {
	configPath := GetDefaultConfigPath(configName)
	return os.WriteFile(configPath, []byte(content), 0600)
}

// GetDefaultMountPath returns the default mount path
func GetDefaultMountPath() string {
	xdg.Reload()
	homePath := xdg.Home
	return filepath.Join(homePath, DefaultMountDirName)
}

// EnsureMountDir creates the mount directory if it doesn't exist
func EnsureMountDir() error {
	mountPath := GetDefaultMountPath()
	return os.MkdirAll(mountPath, 0755)
}

// ProfilesDirName is the subdirectory under the config dir holding named
// client-config profiles.
const ProfilesDirName = "profiles"

// profileNameRe constrains a profile name to safe characters so it can only
// resolve to a file inside the profiles directory (no separators, no "..").
var profileNameRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// ValidateProfileName rejects names that are empty, contain a path separator or
// other unsafe character, or are "." / "..".
func ValidateProfileName(name string) error {
	if name == "." || name == ".." || !profileNameRe.MatchString(name) {
		return fmt.Errorf("invalid profile name %q: use only letters, digits, '.', '_' and '-'", name)
	}
	return nil
}

// GetProfilesDir returns the directory holding profile config files.
func GetProfilesDir() string {
	return filepath.Join(GetDefaultConfigDir(), ProfilesDirName)
}

// GetProfilePath returns the config file path for a named profile. The name is
// assumed already validated by ValidateProfileName.
func GetProfilePath(name string) string {
	return filepath.Join(GetProfilesDir(), name+".yaml")
}

// ListProfileNames returns the sorted names (without the .yaml extension) of the
// profiles in GetProfilesDir(). A missing directory yields an empty list.
func ListProfileNames() ([]string, error) {
	entries, err := os.ReadDir(GetProfilesDir())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if n, ok := strings.CutSuffix(e.Name(), ".yaml"); ok {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	return names, nil
}
