package commands

import (
	"fmt"
	"gmountie/pkg/client/config"
	"gmountie/pkg/client/grpc"
	"gmountie/pkg/client/mount"
	"gmountie/pkg/utils/log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var (
	serverAddr string
	volumeName string
	authType   string
	username   string
	password   string
	cfg        *config.Config
)

var mountCmd = &cobra.Command{
	Use:   "mount [mountpoint]",
	Short: "Mount a gMountie volume",
	Long:  `Mount a gMountie volume at the specified mountpoint`,
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if volumeName == "" {
			return fmt.Errorf("volume name is required")
		}

		// Create config from command line args
		v := viper.New()

		// Split the server address into address and port
		// and set them in the config
		endpointSlice := strings.Split(serverAddr, ":")
		if len(endpointSlice) != 2 {
			return fmt.Errorf("invalid server address: %s", serverAddr)
		}
		v.Set("server.address", endpointSlice[0])
		v.Set("server.port", endpointSlice[1])
		v.Set("auth.type", authType)

		if authType == "basic" {
			if username == "" || password == "" {
				return fmt.Errorf("username and password are required for basic auth")
			}
			v.Set("auth.username", username)
			v.Set("auth.password", password)
		}

		var err error
		cfg, err = config.ParseConfig(v)
		if err != nil {
			return fmt.Errorf("failed to parse config: %v", err)
		}

		mountpoint := args[0]
		// Verify that the mountpoint directory exists
		if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
			return fmt.Errorf("mountpoint %s does not exist", mountpoint)
		}

		// Create client
		c, err := grpc.NewClientFromConfig(cfg)
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}

		defer func(c grpc.Client) {
			err := c.Close()
			if err != nil {
				log.Log.Error("failed to close the client", zap.Error(err))
			}
		}(c)

		// Create mounter
		mounter := mount.NewSingleVolumeMounter(c, cfg.FUSE, *cfg.Cache)
		defer func(mounter mount.SingleVolumeMounter) {
			err := mounter.Close()
			if err != nil {
				log.Log.Error("failed to close the mounter", zap.Error(err))
			}
		}(mounter)

		// Mount volume
		if err := mounter.Mount(volumeName, mountpoint); err != nil {
			return fmt.Errorf("failed to mount volume: %v", err)
		}

		log.Log.Sugar().Infof("Mounted volume %s at %s", volumeName, mountpoint)
		log.Log.Sugar().Info("Press Ctrl+C to unmount")

		// Wait for interrupt signal
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		<-ch

		return nil
	},
}

func init() {
	mountCmd.PersistentFlags().StringVarP(&serverAddr, "server", "s", "127.0.0.1:9449", "server address")
	mountCmd.PersistentFlags().StringVarP(&volumeName, "volume", "n", "", "volume name")
	mountCmd.PersistentFlags().StringVarP(&authType, "auth-type", "t", "none", "authentication type (none, basic)")
	mountCmd.PersistentFlags().StringVarP(&username, "username", "u", "", "username for basic auth")
	mountCmd.PersistentFlags().StringVarP(&password, "password", "p", "", "password for basic auth")
	rootCmd.AddCommand(mountCmd)
}
