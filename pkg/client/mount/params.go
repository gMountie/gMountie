package mount

import (
	"context"
	"os"

	"go.gmountie.dev/gmountie/pkg/client/backend/identity"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/utils/log"
	"go.uber.org/zap"
)

// MountParams is the set of capabilities resolved ONCE, before the backend
// stack is built, from the existing Version + WhoAmI RPCs. Threading it as one
// value (rather than scattered positional args) fixes the old ordering bug
// where the backend was constructed before WhoAmI ran, and gives future layers
// a single place to read negotiated capabilities. No new wire surface.
type MountParams struct {
	MaxWriteBytes      int
	DefaultPermissions bool
}

// negotiateMountParams runs version negotiation and (unless rawIDs) WhoAmI,
// returning the resolved params and an optional IDRewriter. Soft-fails to
// configured/raw defaults exactly as the prior inline code did.
func negotiateMountParams(client grpc.Client, fuseCfg *config.FUSEConfig, rawIDs bool, volume string) (MountParams, *identity.IDRewriter) {
	params := MountParams{MaxWriteBytes: negotiateMaxWriteBytes(client, fuseCfg)}
	if rawIDs {
		return params, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), client.MetaTimeout())
	defer cancel()
	idResp, err := client.WhoAmI(ctx, volume)
	if err != nil {
		log.Log.Warn("WhoAmI failed, mounting with raw IDs", zap.String("volume", volume), zap.Error(err))
		return params, nil
	}
	rewriter := identity.NewIDRewriter(identityFromProto(idResp), uint32(os.Getuid()), uint32(os.Getgid()))
	params.DefaultPermissions = idResp.GetMappingMode() == mappingModeSquash
	return params, rewriter
}
