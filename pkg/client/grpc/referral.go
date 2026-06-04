package grpc

import (
	"context"
	"net"

	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/pkg/errors"
)

// listVolumes asks the server which volumes the caller may mount. Pre-session,
// like resolveLocation: the server authenticates the caller from its mTLS cert
// or basic-auth creds, no session_id required. Used to auto-resolve the volume
// when the user omits --volume — a single-volume credential then needs no name.
func listVolumes(ctx context.Context, client Client) ([]string, error) {
	reply, err := client.Volume().List(ctx, &proto.VolumeListRequest{})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(reply.GetVolumes()))
	for _, v := range reply.GetVolumes() {
		names = append(names, v.GetName())
	}
	return names, nil
}

// resolveLocation asks the server where the volume lives. An empty location
// means "served here"; a non-empty location is a referral the client should
// reconnect to. Pre-session: the server authenticates the caller from its mTLS
// cert or basic-auth creds, no session_id required (see pkg/server/grpc/auth.go).
func resolveLocation(ctx context.Context, client Client, volume string) (string, error) {
	reply, err := client.Volume().Resolve(ctx, &proto.VolumeResolveRequest{Name: volume})
	if err != nil {
		return "", err
	}
	return reply.GetLocation(), nil
}

// tlsConfigForReferral clones cfg with TLS retargeted at the referred host:
// ServerName becomes the location's host (SNI + cert verification target) and
// any pinned ExpectedFingerprint is dropped (a host-specific pin can't transfer
// to a different host; rely on CA chain validation or TOFU of the new host).
// The dial endpoint is the caller's responsibility — pass the raw location
// string to newUnconnectedClient, since ServerConfig.Address is IP-validated
// and a referral host is usually a name.
func tlsConfigForReferral(cfg *config.Config, location string) (*config.Config, error) {
	host, _, err := net.SplitHostPort(location)
	if err != nil {
		return nil, errors.Wrapf(err, "parse referral location %q", location)
	}
	clone := *cfg
	server := *cfg.Server
	server.TLS = cfg.Server.TLS
	server.TLS.ServerName = host
	server.TLS.ExpectedFingerprint = ""
	clone.Server = &server
	return &clone, nil
}
