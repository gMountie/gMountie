package fs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// ConnPoolFSSuite verifies that with a multi-connection client pool
// (rpc.connections=4, matching the production default), fds opened on the
// shared session work for reads and writes regardless of which pooled
// connection a given stream lands on (Read/Write round-robin per stream). A
// round-trip mismatch would mean a stream hit a connection where the
// fd/session was not resolvable.
type ConnPoolFSSuite struct {
	suite.Suite
	ctx    *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *ConnPoolFSSuite) SetupSuite() {
	// WithConnections(4) explicitly sets the pool to 4 connections, matching
	// the production default (rpc.connections=4). The default harness client
	// has only 1 connection, so this option is required to exercise the pool.
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
		utils.WithConnections(4),
	)
	s.Require().NoError(err)
	s.Require().NoError(ctx.Start())
	s.ctx = ctx
	// Safety net: a failed Require below skips TearDownSuite (testify never
	// marks the suite set up), which would leak the running server into later
	// suites. Close is idempotent, so this coexists with TearDownSuite.
	s.T().Cleanup(func() { _ = ctx.Close() })
	s.volume = ctx.GetVolumes()[0]
	s.Require().NoError(ctx.MountVolumeErr(s.volume))
}

func (s *ConnPoolFSSuite) TearDownSuite() {
	if s.ctx != nil {
		s.Require().NoError(s.ctx.Close())
	}
}

// TestManyFilesReadWriteAcrossPool round-trips enough files that, with
// per-stream round-robin, writes/reads land on multiple pooled connections.
func (s *ConnPoolFSSuite) TestManyFilesReadWriteAcrossPool() {
	mp := s.volume.GetMountPath()
	for i := 0; i < 24; i++ {
		name := filepath.Join(mp, fmt.Sprintf("pool-%02d.bin", i))
		want := bytes.Repeat([]byte{byte('A' + i%26)}, 4096+i)
		s.Require().NoError(os.WriteFile(name, want, 0o644))
		got, err := os.ReadFile(name)
		s.Require().NoError(err)
		s.Require().True(bytes.Equal(want, got), "file %d round-trip mismatch across pool", i)
	}
}

// TestLargeFileReadWriteAcrossPool round-trips one larger file whose reads and
// writes fan out into multiple concurrent streams (and thus connections).
func (s *ConnPoolFSSuite) TestLargeFileReadWriteAcrossPool() {
	mp := s.volume.GetMountPath()
	path := filepath.Join(mp, "large.bin")
	want := bytes.Repeat([]byte("gMountie-pool-0123456789"), 1<<16) // ~1.5 MiB
	s.Require().NoError(os.WriteFile(path, want, 0o644))
	got, err := os.ReadFile(path)
	s.Require().NoError(err)
	s.Require().Equal(len(want), len(got))
	s.Require().True(bytes.Equal(want, got), "large-file round-trip mismatch across pool")
}

func TestConnPoolFSSuite(t *testing.T) { suite.Run(t, new(ConnPoolFSSuite)) }
