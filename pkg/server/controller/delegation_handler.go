package controller

import (
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/server/delegation"
	"go.gmountie.dev/gmountie/pkg/utils/log"

	"go.uber.org/zap"
)

// arbitrateContention enforces recall-on-contention for any contending op at
// path — reads included (a Phase-2 holder may have deferred state a reader
// would miss), not just mutations. Returns FS_OK when arb is nil (delegation
// disabled) or when the path is free (or owned by contender). Returns
// FS_EAGAIN when a recall fails/times out — the contender must back off (use
// a fresh request_id on retry, where the op has one).
func arbitrateContention(arb *delegation.Arbiter, sessionID, path string) proto.FsError {
	if arb == nil {
		return proto.FsError_FS_OK
	}
	if err := arb.OnMutation(sessionID, path); err != nil {
		log.Log.Warn("delegation recall failed; contending op rejected",
			zap.String("path", path), zap.Error(err))
		return proto.FsError_FS_EAGAIN
	}
	return proto.FsError_FS_OK
}

// grantFor evaluates a piggybacked delegation request (nil-safe).
// Returns nil when arb is nil or the request carries no root.
//
// principal, volume and wal_epoch must match the watermark.Key that Apply
// derives for the same (identity, volume, epoch) stream — only then does a
// revoked gen fence the right replay. principal/volume come from the handler's
// BindIdentity result; wal_epoch is carried on the DelegationRequest from the
// client's wal.db.
func grantFor(arb *delegation.Arbiter, sessionID, principal, volume string, req *proto.DelegationRequest) *proto.DelegationGrant {
	if arb == nil || req.GetRoot() == "" {
		return nil
	}
	return arb.Request(sessionID, req.GetRoot(), principal, volume, req.GetWalEpoch())
}
