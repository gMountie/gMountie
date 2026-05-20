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

		// Build a viper instance, optionally seeded from a config file,
		// and let CLI flags layer on top.
		//
		// Precedence (highest first):
		//   1. Explicitly-passed CLI flags (cmd.Flags().Changed(name) == true)
		//   2. Values read from the file passed via --config / -c
		//   3. Flag default values (--server 127.0.0.1:9449, --auth-type none)
		//
		// (3) only applies when no config file was loaded; once a config
		// file is in play, the file's values must not be silently shadowed
		// by flag defaults that the user never explicitly set.
		v := viper.New()
		hasConfig := configFile != ""
		if hasConfig {
			v.SetConfigFile(configFile)
			if err := v.ReadInConfig(); err != nil {
				return fmt.Errorf("failed to read config file %s: %w", configFile, err)
			}
		}

		// applyServer is special: -s "host:port" splits into two viper keys.
		if !hasConfig || cmd.Flags().Changed("server") {
			endpointSlice := strings.Split(serverAddr, ":")
			if len(endpointSlice) != 2 {
				return fmt.Errorf("invalid server address: %s", serverAddr)
			}
			v.Set("server.address", endpointSlice[0])
			v.Set("server.port", endpointSlice[1])
		}
		setFromFlag := func(name, viperKey, value string) {
			if !hasConfig || cmd.Flags().Changed(name) {
				v.Set(viperKey, value)
			}
		}
		setFromFlag("auth-type", "auth.type", authType)
		setFromFlag("username", "auth.username", username)
		setFromFlag("password", "auth.password", password)

		if v.GetString("auth.type") == "basic" {
			if v.GetString("auth.username") == "" || v.GetString("auth.password") == "" {
				return fmt.Errorf("username and password are required for basic auth")
			}
		}

		var err error
		cfg, err = config.ParseConfig(v)
		if err != nil {
			return fmt.Errorf("failed to parse config: %w", err)
		}

		mountpoint := args[0]
		// Verify that the mountpoint directory exists
		if _, err := os.Stat(mountpoint); os.IsNotExist(err) {
			return fmt.Errorf("mountpoint %s does not exist", mountpoint)
		}

		// Create client
		c, err := grpc.NewClientFromConfig(cfg)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
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
			return fmt.Errorf("failed to mount volume: %w", err)
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
