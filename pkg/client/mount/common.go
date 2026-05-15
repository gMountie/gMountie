package mount

import (
	"context"
	"fmt"
	"time"

	"gmountie/pkg/client/config"
	clientgrpc "gmountie/pkg/client/grpc"
	"gmountie/pkg/proto"
	"gmountie/pkg/utils/log"

	"github.com/avast/retry-go/v4"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/fuse/nodefs"
	"github.com/hanwen/go-fuse/v2/fuse/pathfs"
	"go.uber.org/zap"
)

const (
	FuseFSName = "gMountie"
	debug      = false
	// versionNegotiationTimeout bounds the up-front Version RPC. Short
	// enough not to delay mount, long enough for a slow link. A miss
	// (timeout, RST, unimplemented) falls back to the configured value.
	versionNegotiationTimeout = 3 * time.Second
)

// Mounter is the interface for the mounter
type Mounter interface {
	// IsVolumeMounted checks if a volume is mounted
	IsVolumeMounted(volume string) bool
	// Unmount unmounts a volume
	Unmount(volumeName string) error
	// UnmountAll unmounts all volumes from the root filesystem
	UnmountAll() error
	// Close closes the root filesystem
	Close() error
}

// ---------------------- Utils ----------------------

// createMountOptions returns the mount options, with FUSE knobs sourced
// from the supplied FUSEConfig. maxWriteBytes is the (already-negotiated)
// ceiling for FUSE WRITE/READ — go-fuse sets the kernel's max_read equal
// to MaxWrite, so this single knob drives both directions.
func createMountOptions(endpoint, volume string, cfg *config.FUSEConfig, maxWriteBytes int) *fuse.MountOptions {
	opts := &fuse.MountOptions{
		AllowOther:     false,
		SingleThreaded: false,
		MaxBackground:  cfg.MaxBackground,
		MaxWrite:       maxWriteBytes,
		Debug:          debug,
		EnableLocks:    true,
		DirectMount:    true,
		Name:           FuseFSName,
		FsName:         fmt.Sprintf("%s://%s/%s", FuseFSName, endpoint, volume),
		Logger:         zap.NewStdLog(log.Log.Named("fuse")),
	}
	// go-fuse v2.10.1 has no `EnableWriteback` field; the writeback page
	// cache is controlled via the CAP_WRITEBACK_CACHE capability bit.
	// Leaving it disabled (default) matches the synchronous read/write
	// path pending Phase 4's cache layer.
	if cfg.WritebackCache {
		opts.ExtraCapabilities |= fuse.CAP_WRITEBACK_CACHE
	} else {
		opts.DisabledCapabilities |= fuse.CAP_WRITEBACK_CACHE
	}
	return opts
}

// negotiateMaxWriteBytes asks the server for its frame ceiling via the
// Version RPC and caps the client's configured MaxWriteBytes at that
// value. Falls back to the configured value if the call fails or the
// server returns 0 (old server, unimplemented, network glitch). Never
// returns an error: a failed handshake must not gate the mount.
func negotiateMaxWriteBytes(client clientgrpc.Client, cfg *config.FUSEConfig) int {
	ctx, cancel := context.WithTimeout(context.Background(), versionNegotiationTimeout)
	defer cancel()

	reply, err := client.Version().Get(ctx, &proto.VersionRequest{})
	if err != nil {
		log.Log.Warn(
			"version negotiation failed; using configured FUSE max_write_bytes",
			zap.Int("max_write_bytes", cfg.MaxWriteBytes),
			zap.Error(err),
		)
		return cfg.MaxWriteBytes
	}
	serverFrame := int(reply.GetFrameSizeBytes())
	if serverFrame <= 0 {
		log.Log.Debug(
			"server reported no frame_size_bytes; using configured FUSE max_write_bytes",
			zap.Int("max_write_bytes", cfg.MaxWriteBytes),
		)
		return cfg.MaxWriteBytes
	}
	if serverFrame >= cfg.MaxWriteBytes {
		return cfg.MaxWriteBytes
	}
	log.Log.Info(
		"capping FUSE max_write_bytes at server frame ceiling",
		zap.Int("configured", cfg.MaxWriteBytes),
		zap.Int("server_frame_size_bytes", serverFrame),
	)
	return serverFrame
}

// createConnectorOptions returns the connector options
func createConnectorOptions() *nodefs.Options {
	sec := time.Second
	return &nodefs.Options{
		EntryTimeout:        sec,
		AttrTimeout:         sec,
		NegativeTimeout:     0.0,
		Debug:               debug,
		LookupKnownChildren: true,
	}
}

// createFsOptions returns the filesystem options
func createFsOptions() *pathfs.PathNodeFsOptions {
	return &pathfs.PathNodeFsOptions{
		ClientInodes: false,
		Debug:        debug,
	}
}

// stopServer stops the server
func stopServer(server *fuse.Server) error {
	return retry.Do(
		func() error {
			err := server.Unmount()
			if err != nil {
				log.Log.Warn("unmount fail, retrying ...", zap.Error(err))
				return err
			}
			return nil
		},
		retry.Attempts(3),
		retry.Delay(5*time.Second),
	)
}
