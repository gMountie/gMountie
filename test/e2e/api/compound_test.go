package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gmountie/pkg/proto"
	"gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// CompoundE2ESuite exercises the RpcFs.Compound batched-metadata RPC against
// a live server (bufconn). It uses the raw gRPC client (Fs()) — no FUSE mount
// — to isolate the Compound path from kernel-side caching.
type CompoundE2ESuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func (s *CompoundE2ESuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.testAppCtx = ctx
	s.volume = ctx.GetVolumes()[0]
}

func (s *CompoundE2ESuite) TearDownSuite() {
	if s.testAppCtx != nil {
		_ = s.testAppCtx.Close()
	}
	if s.volume != nil {
		_ = s.volume.Close()
	}
}

// Test100GetAttrInOneRTT pre-creates 100 files server-side, fires a single
// Compound RPC with 100 GetAttr ops, and asserts: (1) 100 replies; (2) each
// carries a typed GetAttrReply (no status fallbacks); (3) sizes match what
// was written; (4) the batch completes well under a generous local-RTT
// budget.
func (s *CompoundE2ESuite) Test100GetAttrInOneRTT() {
	const n = 100
	const payloadSize = 16
	srcRoot := s.volume.GetSrcPath()
	for i := 0; i < n; i++ {
		path := filepath.Join(srcRoot, fmt.Sprintf("compound_%03d.bin", i))
		buf := make([]byte, payloadSize)
		// Encode index into byte 0 so a mis-routed reply would be detectable
		// if we ever want to extend the assertion — not required today.
		buf[0] = byte(i)
		s.Require().NoError(os.WriteFile(path, buf, 0o644))
	}

	ops := make([]*proto.CompoundOp, n)
	for i := 0; i < n; i++ {
		ops[i] = &proto.CompoundOp{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{
			Volume: s.volume.Name,
			Path:   fmt.Sprintf("/compound_%03d.bin", i),
		}}}
	}

	fsClient := s.testAppCtx.GetClient().Fs()
	start := time.Now()
	batch, err := fsClient.Compound(context.Background(), &proto.CompoundRequest{Ops: ops})
	elapsed := time.Since(start)

	s.Require().NoError(err)
	s.Require().Len(batch.GetReplies(), n)
	for i, r := range batch.GetReplies() {
		typed := r.GetGetAttr()
		s.Require().NotNilf(typed, "reply %d should be GetAttrReply, got status=%d", i, r.GetStatus())
		s.Require().NotNilf(typed.Attributes, "reply %d missing Attributes", i)
		s.Equal(uint64(payloadSize), typed.Attributes.Size, "reply %d size mismatch", i)
	}
	// Generous budget: 50ms covers VM overhead. Localhost typically lands in
	// the single-digit-ms range for 100 metadata ops over bufconn.
	s.Less(elapsed, 50*time.Millisecond, "100-op Compound batch took %v, expected <50ms locally", elapsed)
}

// TestPerOpErrorReturnsStatus stages two ops: one for an existing file, one
// against an unknown volume. The unknown-volume op surfaces via the
// dispatcher's grpcErrToFuseStatus mapper; the good op succeeds. Validates
// the per-op-error contract end to end (transport-class failure flattened
// into the reply slot, not aborting the batch).
func (s *CompoundE2ESuite) TestPerOpErrorReturnsStatus() {
	goodPath := filepath.Join(s.volume.GetSrcPath(), "ok.bin")
	s.Require().NoError(os.WriteFile(goodPath, []byte("x"), 0o644))

	ops := []*proto.CompoundOp{
		{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: "no-such-volume", Path: "/whatever"}}},
		{Op: &proto.CompoundOp_GetAttr{GetAttr: &proto.GetAttrRequest{Volume: s.volume.Name, Path: "/ok.bin"}}},
	}
	batch, err := s.testAppCtx.GetClient().Fs().Compound(context.Background(), &proto.CompoundRequest{Ops: ops})
	s.Require().NoError(err)
	s.Require().Len(batch.GetReplies(), 2)
	s.Nil(batch.Replies[0].GetGetAttr(), "unknown-volume op should not carry typed reply")
	s.NotZero(batch.Replies[0].GetStatus(), "unknown-volume op should carry a non-zero fuse status")
	s.Require().NotNil(batch.Replies[1].GetGetAttr(), "second op should succeed")
}

func TestCompoundE2ESuite(t *testing.T) {
	suite.Run(t, new(CompoundE2ESuite))
}
