package controller

import (
	"errors"
	"fmt"
	"go.gmountie.dev/gmountie/pkg/client"
	clientConfig "go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	config2 "go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/ui/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// LogInInfo is a struct that holds the login information
type LogInInfo struct {
	Username string
	Password string
	Address  string
	Port     uint
	TLS      bool
}

// LoginController is a controller for login page
type LoginController interface {
	// IsLoggedIn checks if the user is logged in
	IsLoggedIn() bool

	// Login logs the user in
	Login(loginInfo LogInInfo) (bool, error)

	// Logout logs the user out
	Logout() error
}

// LoginControllerImpl is an implementation of LoginController
type LoginControllerImpl struct {
	configService service.ConfigService
	appService    service.AppService
}

// NewLoginControllerImpl creates a new instance of LoginControllerImpl
func NewLoginControllerImpl(configService service.ConfigService, appService service.AppService) *LoginControllerImpl {
	return &LoginControllerImpl{
		configService: configService,
		appService:    appService,
	}
}

func (l *LoginControllerImpl) IsLoggedIn() bool {
	// Check if the config is loaded
	if !l.configService.ConfigLoaded() || l.configService.GetConfig().Auth == nil {
		return false
	}
	// Check if the app context is loaded
	if l.appService.GetContext() == nil {
		if err := l.createAppContext(l.configService.GetConfig()); err != nil {
			log.Log.Error("error while creating app context", zap.Error(err))
			return false
		}
	}
	return true
}

func (l *LoginControllerImpl) Login(loginInfo LogInInfo) (bool, error) {
	log.Log.Info(
		"logging in",
		zap.String("address", loginInfo.Address),
		zap.Uint("port", loginInfo.Port),
		zap.String("username", loginInfo.Username),
	)
	// Phase 7 PR 1: the client always dials TLS; the LoginInfo.TLS bool now
	// only selects "skip verification" (the old "no TLS" semantic maps to
	// "TLS but trust anything"). Surfacing verify/tofu/insecure properly is
	// Phase 8 UI work — for now keep the boolean's user-facing meaning
	// ("Use TLS = on by default; toggle off for self-signed/dev").
	tlsCfg := clientConfig.TLSConfig{}
	if !loginInfo.TLS {
		tlsCfg.Verify = "insecure"
	}
	// Update the config
	cfg := clientConfig.Config{
		Server: &clientConfig.ServerConfig{
			Address: loginInfo.Address,
			Port:    loginInfo.Port,
			TLS:     tlsCfg,
		},
		Auth: &clientConfig.BasicAuthConfig{
			Type: config.AuthConfigTypeBasic,
			BasicAuthConfigUser: clientConfig.BasicAuthConfigUser{
				Username: loginInfo.Username,
				Password: loginInfo.Password,
			},
		},
		Mount: &clientConfig.VFSMountConfig{
			Type:     clientConfig.MountTypeVFS,
			Path:     config2.GetDefaultMountPath(),
			MountAll: false,
			Volumes:  nil,
		},
	}
	if err := cfg.Validate(); err != nil {
		log.Log.Error("error while validating config", zap.Error(err))
		return false, err
	}
	// Save the config
	if err := l.configService.SaveConfig(&cfg); err != nil {
		log.Log.Error("error while saving config", zap.Error(err))
		return false, err
	}

	// Create the app context
	if err := l.createAppContext(&cfg); err != nil {
		log.Log.Error("error while creating context", zap.Error(err))
		return false, err
	}

	return true, nil
}

// Logout logs the user out
func (l *LoginControllerImpl) Logout() error {
	log.Log.Info("logging out")

	var errs []error
	errs = append(errs, l.appService.CloseContext())
	errs = append(errs, l.configService.DeleteConfig())

	return errors.Join(errs...)
}

// createAppContext creates a new app context
func (l *LoginControllerImpl) createAppContext(cfg *clientConfig.Config) error {
	// Get server config
	if cfg.Server == nil {
		return errors.New("server config is missing")
	}
	// Get auth config
	authCfg, ok := cfg.Auth.(*clientConfig.BasicAuthConfig)
	if !ok {
		return errors.New("invalid auth config")
	}

	// Create the app context
	fullAddress := fmt.Sprintf("%s:%d", cfg.Server.Address, cfg.Server.Port)
	newClient, err := grpc.NewClient(fullAddress, grpc.WithBasicAuth(authCfg.Username, authCfg.Password))
	if err != nil {
		return err
	}

	appCtx := client.NewAppContext(newClient, config2.GetDefaultMountPath(), cfg.FUSE, cfg.Cache)
	l.appService.SetContext(appCtx)
	return nil
}
