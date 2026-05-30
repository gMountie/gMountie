package config

import (
	"os"
	"path/filepath"

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
