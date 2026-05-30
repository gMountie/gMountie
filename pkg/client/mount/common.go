package mount

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"go.gmountie.dev/gmountie/pkg/client/config"
	clientgrpc "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
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
	// Disabled by default (synchronous writes); enabled opt-in for async
	// writeback on high-RTT links (see FUSEConfig.WritebackCache).
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
		log.Log.Debug(
			"version negotiation: server frame ceiling at or above configured max_write_bytes; no cap applied",
			zap.Int("configured", cfg.MaxWriteBytes),
			zap.Int("server_frame_size_bytes", serverFrame),
		)
		return cfg.MaxWriteBytes
	}
	log.Log.Info(
		"capping FUSE max_write_bytes at server frame ceiling",
		zap.Int("configured", cfg.MaxWriteBytes),
		zap.Int("server_frame_size_bytes", serverFrame),
	)
	return serverFrame
}

// stopServer requests the FUSE kernel to detach the mount, then blocks
// until the server's Serve goroutine has fully exited.
//
// The second step matters when callers spin up a fresh mount on the
// heels of an unmount (e.g. back-to-back benchmark iterations):
// Unmount only requests teardown — go-fuse's Serve loop may still be
// draining the kernel connection. A new fuse.NewServer issued before
// Serve returns can land the kernel in a state where the very first
// op on the new mount comes back with a pre-cancelled context.
// Waiting here closes that window.
//
// If the regular unmount keeps failing with EBUSY (something still
// has an open fd into the mount, e.g. an interactive `cat`/`pv`
// pipeline the user just Ctrl-C'd), we fall back to a lazy unmount
// via `fusermount3 -uz`. Lazy detaches the mount from the namespace
// immediately and lets the kernel release per-fd state as each fd
// closes — the alternative is hanging forever waiting for fds we
// can't close on the user's behalf.
func stopServer(server *fuse.Server, mountPath string) error {
	const (
		regularAttempts = 2
		regularDelay    = 500 * time.Millisecond
	)
	var lastErr error
	for i := 0; i < regularAttempts; i++ {
		lastErr = server.Unmount()
		if lastErr == nil {
			server.Wait()
			return nil
		}
		log.Log.Warn("unmount fail, retrying",
			zap.Int("attempt", i+1),
			zap.Int("max_attempts", regularAttempts),
			zap.Error(lastErr),
		)
		time.Sleep(regularDelay)
	}
	log.Log.Warn("regular unmount kept failing; falling back to lazy unmount",
		zap.String("mount_path", mountPath),
		zap.Error(lastErr),
	)
	if lazyErr := lazyUnmount(mountPath); lazyErr != nil {
		return errors.Wrapf(lazyErr, "regular unmount: %v; lazy unmount fallback", lastErr)
	}
	log.Log.Info("lazy unmount succeeded; kernel will release refs as fds close",
		zap.String("mount_path", mountPath),
	)
	server.Wait()
	return nil
}

// lazyUnmount shells out to `fusermount3 -uz <path>`. Equivalent to
// `umount(2)` with MNT_DETACH but routed through the setuid fusermount
// helper so an unprivileged user can request it on their own FUSE
// mount. The mount disappears from the namespace immediately; in-use
// fds keep working until they close.
//
// Bounded with a 5-second context so a stuck helper can't hang the
// caller; that's well above the normal fusermount3 runtime (~ms).
func lazyUnmount(mountPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "fusermount3", "-uz", mountPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return errors.Wrapf(err, "fusermount3 -uz %s: %s",
			mountPath, strings.TrimSpace(string(out)))
	}
	return nil
}
