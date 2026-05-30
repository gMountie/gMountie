package service

import "go.gmountie.dev/gmountie/pkg/client"

// AppService is a service for managing the application
type AppService interface {
	// GetContext returns the app context
	GetContext() *client.AppContext
	// CloseContext closes the app context
	CloseContext() error
	// SetContext sets the app context
	SetContext(*client.AppContext)
}

// AppServiceImpl is the implementation of the AppService
type AppServiceImpl struct {
	appCtx *client.AppContext
}

// NewAppService creates a new AppService
func NewAppService() AppService {
	return &AppServiceImpl{}
}

// GetContext returns the app context
func (a *AppServiceImpl) GetContext() *client.AppContext {
	return a.appCtx
}

// SetContext sets the app context
func (a *AppServiceImpl) SetContext(appCtx *client.AppContext) {
	a.appCtx = appCtx
}

// CloseContext closes the app context
func (a *AppServiceImpl) CloseContext() error {
	if a.appCtx == nil {
		return nil
	}
	return a.appCtx.Close()
}
