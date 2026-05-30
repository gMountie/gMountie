// Package io contains the client-side FUSE filesystem implementation.
// compound.go ships a CompoundBatcher helper that accumulates read-only
// metadata ops and issues them in a single Compound RPC. The helper is
// intentionally NOT wired into the FUSE handlers yet — Phase 4 (cache) picks
// up the readdir-with-stat optimisation that consumes this surface.
package io

import (
	"context"

	"go.gmountie.dev/gmountie/pkg/proto"
)

// CompoundBatcher accumulates read-only metadata ops (GetAttr/OpenDir/...)
// and dispatches them in a single Compound RPC. Not safe for concurrent use
// — one batcher per caller / per readdir traversal.
type CompoundBatcher struct {
	client proto.RpcFsClient
	ops    []*proto.CompoundOp
}

// NewCompoundBatcher constructs a batcher that will issue Compound RPCs
// against the supplied client. The returned batcher is empty; callers stage
// ops via the Add* methods and flush with Send.
func NewCompoundBatcher(c proto.RpcFsClient) *CompoundBatcher {
	return &CompoundBatcher{client: c}
}

// Len returns the number of ops currently staged.
func (b *CompoundBatcher) Len() int {
	return len(b.ops)
}

// AddGetAttr stages a GetAttr op against (volume, path).
func (b *CompoundBatcher) AddGetAttr(volume, path string) {
	b.ops = append(b.ops, &proto.CompoundOp{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: volume, Path: path}}})
}

// AddStatFs stages a StatFs op against (volume, path).
func (b *CompoundBatcher) AddStatFs(volume, path string) {
	b.ops = append(b.ops, &proto.CompoundOp{Op: &proto.CompoundOp_StatFs{StatFs: &proto.StatFsRequest{Volume: volume, Path: path}}})
}

// AddOpenDir stages an OpenDir op against (volume, path).
func (b *CompoundBatcher) AddOpenDir(volume, path string) {
	b.ops = append(b.ops, &proto.CompoundOp{Op: &proto.CompoundOp_OpenDir{OpenDir: &proto.OpenDirRequest{Volume: volume, Path: path}}})
}

// AddAccess stages an Access op against (volume, path) with the given mode.
func (b *CompoundBatcher) AddAccess(volume, path string, mode uint32) {
	b.ops = append(b.ops, &proto.CompoundOp{Op: &proto.CompoundOp_Access{Access: &proto.AccessRequest{Volume: volume, Path: path, Mode: mode}}})
}

// AddGetXAttr stages a GetXAttr op against (volume, path, attribute).
func (b *CompoundBatcher) AddGetXAttr(volume, path, attribute string) {
	b.ops = append(b.ops, &proto.CompoundOp{Op: &proto.CompoundOp_GetXattr{GetXattr: &proto.GetXAttrRequest{Volume: volume, Path: path, Attribute: attribute}}})
}

// Send issues the staged ops as a single Compound RPC and returns the per-op
// replies in the same order they were added. On success the batcher is
// reset so it can be reused for the next batch. An empty batcher returns
// (nil, nil) without issuing a round trip.
func (b *CompoundBatcher) Send(ctx context.Context) ([]*proto.CompoundReply, error) {
	if len(b.ops) == 0 {
		return nil, nil
	}
	reply, err := b.client.Compound(ctx, &proto.CompoundRequest{Ops: b.ops})
	if err != nil {
		return nil, err
	}
	b.ops = b.ops[:0]
	return reply.GetReplies(), nil
}
