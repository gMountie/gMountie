package service

import (
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"os"

	"go.uber.org/zap"
)

// ConfigService is a service for managing the configuration
type ConfigService interface {
	// ConfigLoaded returns true if the config was loaded
	ConfigLoaded() bool

	// GetConfig returns the config
	GetConfig() *config.Config

	// SaveConfig saves the config
	SaveConfig(*config.Config) error

	// DeleteConfig deletes the config
	DeleteConfig() error
}

// ConfigServiceImpl is the implementation of the ConfigService
type ConfigServiceImpl struct {
	config     *config.Config
	configPath string
}

// NewConfigService creates a new ConfigService
func NewConfigService(configPath string) (*ConfigServiceImpl, error) {
	svc := &ConfigServiceImpl{
		configPath: configPath,
	}

	// Try to open the config file
	cfg, err := os.ReadFile(configPath)

	if os.IsNotExist(err) {
		log.Log.Info("config file not found", zap.String("configPath", configPath))
		return svc, nil
	} else if err != nil {
		// Return the error if it's not a "not found" error
		return nil, err
	}

	log.Log.Info("config file found", zap.String("configPath", configPath))
	// read the config file
	svc.config, err = config.LoadConfigFromString(string(cfg))
	if err != nil {
		return nil, err
	}
	return svc, nil
}

// ConfigLoaded returns true if the config was loaded
func (c *ConfigServiceImpl) ConfigLoaded() bool {
	return c.config != nil
}

// GetConfig returns the config
func (c *ConfigServiceImpl) GetConfig() *config.Config {
	return c.config
}

// SaveConfig saves the config
func (c *ConfigServiceImpl) SaveConfig(cfg *config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	c.config = cfg
	return c.config.Save(c.configPath)
}

// DeleteConfig deletes the config
func (c *ConfigServiceImpl) DeleteConfig() error {
	if err := os.Remove(c.configPath); err != nil {
		return err
	}
	c.config = nil
	return nil
}
