package grpc

import (
	"go.gmountie.dev/gmountie/pkg/server/config"

	"google.golang.org/grpc"
)

// limitsServerOptions returns the slice of grpc.ServerOption derived from
// cfg. Zero values disable the corresponding limit; callers compose this
// with their other options.
//
// NOTE: this deliberately does NOT emit KeepaliveParams for
// MaxConnectionIdle/MaxConnectionAge. The sole caller (getOptions) folds those
// two into the single keepalive.ServerParameters it already builds, so there is
// exactly one KeepaliveParams option and the fields can't clobber each other. A
// KeepaliveParams branch here would be dead by construction (MN-L1).
func limitsServerOptions(cfg config.LimitsConfig) []grpc.ServerOption {
	var out []grpc.ServerOption
	if cfg.MaxRecvMessageSize > 0 {
		out = append(out, grpc.MaxRecvMsgSize(cfg.MaxRecvMessageSize))
	}
	if cfg.MaxConcurrentStreams > 0 {
		out = append(out, grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams))
	}
	return out
}
