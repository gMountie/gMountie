package grpc

import (
	"go.gmountie.dev/gmountie/pkg/server/config"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// limitsServerOptions returns the slice of grpc.ServerOption derived from
// cfg. Zero values disable the corresponding limit; callers compose this
// with their other options.
func limitsServerOptions(cfg config.LimitsConfig) []grpc.ServerOption {
	var out []grpc.ServerOption
	if cfg.MaxRecvMessageSize > 0 {
		out = append(out, grpc.MaxRecvMsgSize(cfg.MaxRecvMessageSize))
	}
	if cfg.MaxConcurrentStreams > 0 {
		out = append(out, grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams))
	}
	// KeepaliveParams covers both Idle and Age. Skip the option entirely
	// when both are zero so we don't override gRPC's defaults with no-op values.
	if cfg.MaxConnectionIdle > 0 || cfg.MaxConnectionAge > 0 {
		ka := keepalive.ServerParameters{}
		if cfg.MaxConnectionIdle > 0 {
			ka.MaxConnectionIdle = cfg.MaxConnectionIdle
		}
		if cfg.MaxConnectionAge > 0 {
			ka.MaxConnectionAge = cfg.MaxConnectionAge
		}
		out = append(out, grpc.KeepaliveParams(ka))
	}
	return out
}
