package main

import (
	"embed"
	_ "embed"
	"go.gmountie.dev/gmountie/pkg/common/config"
	"go.gmountie.dev/gmountie/pkg/ui/controller"
	"go.gmountie.dev/gmountie/pkg/ui/service"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"log/slog"

	"github.com/samber/slog-zap/v2"
	"github.com/wailsapp/wails/v3/pkg/application"
	"go.uber.org/zap"
)

// Wails uses Go's `embed` package to embed the frontend files into the binary.
// Any files in the frontend/dist folder will be embedded into the binary and
// made available to the frontend.
// See https://pkg.go.dev/embed for more information.

//go:embed all:frontend/build
var assets embed.FS

// main function serves as the application's entry point. It initializes the application, creates a window,
// and starts a goroutine that emits a time-based event every second. It subsequently runs the application and
// logs any error that might occur.
func main() {

	logger := slogzap.Option{
		Level:  slog.LevelDebug,
		Logger: log.Log.Named("ui"),
	}

	// Create a new instance of the AppService.
	appSvc := service.NewAppService()

	// Create a new instance of the ConfigService.
	err := config.EnsureConfigDir()
	if err != nil {
		log.Log.Fatal("error while ensuring config dir exists", zap.Error(err))
		return
	}
	configPath := config.GetDefaultConfigPath(config.DefaultClientConfigFileName)

	configService, err := service.NewConfigService(configPath)
	if err != nil {
		log.Log.Fatal("error while creating config service", zap.Error(err))
		return
	}

	loginController := controller.NewLoginControllerImpl(configService, appSvc)
	volumeController := controller.NewVolumeControllerImpl(appSvc)

	// Create a new Wails application by providing the necessary options.
	// Variables 'Name' and 'Description' are for application metadata.
	// 'Assets' configures the asset server with the 'FS' variable pointing to the frontend files.
	// 'Bind' is a list of Go struct instances. The frontend has access to the methods of these instances.
	// 'Mac' options tailor the application when running an macOS.
	app := application.New(application.Options{
		Name:        "gmountie-desktop",
		Description: "A demo of using raw HTML & CSS",
		Services: []application.Service{
			application.NewService(loginController),
			application.NewService(volumeController),
		},
		Assets: application.AssetOptions{
			Handler: application.AssetFileServerFS(assets),
		},
		Mac: application.MacOptions{
			ApplicationShouldTerminateAfterLastWindowClosed: true,
		},
		Logger: slog.New(logger.NewZapHandler()),
		OnShutdown: func() {
			if err := appSvc.GetContext().Close(); err != nil {
				log.Log.Error("error while closing app context", zap.Error(err))
			}
		},
	})

	// Create a new window with the necessary options.
	// 'Title' is the title of the window.
	// 'Mac' options tailor the window when running on macOS.
	// 'BackgroundColour' is the background colour of the window.
	// 'URL' is the URL that will be loaded into the webview.
	app.NewWebviewWindowWithOptions(application.WebviewWindowOptions{
		Title: "gMountie",
		Mac: application.MacWindow{
			InvisibleTitleBarHeight: 50,
			Backdrop:                application.MacBackdropTranslucent,
			TitleBar:                application.MacTitleBarHiddenInset,
		},
		Height:           768,
		Width:            1024,
		MinHeight:        584,
		MinWidth:         512,
		MaxHeight:        1080,
		MaxWidth:         1920,
		BackgroundColour: application.NewRGB(220, 244, 242),
		URL:              "/",
	})

	// Run the application. This blocks until the application has been exited.
	if err = app.Run(); err != nil {
		log.Log.Fatal("error while running application", zap.Error(err))

	}
}
