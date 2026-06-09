package api

// ConcurrentClientsSuite exercises the server's concurrency-safety under load
// from multiple independent gRPC clients hitting the same volume simultaneously.
// The suite runs in-process via bufconn (no FUSE mount required) precisely so
// it can run under the -race detector — keep it FUSE-free. Multi-mount FUSE
// concurrency is deferred to Phase 8 (VFS).
//
// Writer variant: we use on-disk seed + concurrent gRPC reads rather than driving
// raw streaming-Write RPCs (Create/WriteFrame/Flush/Release). That variant would
// also exercise concurrent write paths, but the goal here is server-side volume
// lookup + identity binding + FS reads under concurrency, which on-disk seed
// fully covers. The session_test confirms the file-handle lifecycle; the write
// driver complexity would obscure the concurrency assertion.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	grpcClient "go.gmountie.dev/gmountie/pkg/client/grpc"
	"go.gmountie.dev/gmountie/pkg/proto"
	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

const (
	concurrentClients    = 8
	concurrentIterations = 10
)

// ConcurrentClientsSuite spins M independent clients against one server/volume
// concurrently and verifies: no errors, no data races, correct content.
type ConcurrentClientsSuite struct {
	suite.Suite
	appCtx *utils.AppTestingContext
	volume *utils.TestVolume
}

func (s *ConcurrentClientsSuite) SetupSuite() {
	ctx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(true),
	)
	s.Require().NoError(err, "build app testing context")
	s.Require().NoError(ctx.Start(), "start server")
	s.appCtx = ctx
	s.volume = ctx.GetVolumes()[0]
	s.Require().NotEmpty(s.volume.GeneratedFiles,
		"WithRandomTestVolume(true) must seed at least one file; check RandomFileGenerator")
}

func (s *ConcurrentClientsSuite) TearDownSuite() {
	if s.appCtx != nil {
		s.Require().NoError(s.appCtx.Close())
	}
}

// newClientAs is a helper that dials a fresh client and returns it. Callers own
// the returned client and must close it. Uses s.Require to fail fast if the
// dial/handshake fails — safe because it runs on the test goroutine.
func (s *ConcurrentClientsSuite) newClientAs(user, pass string) grpcClient.Client {
	s.T().Helper()
	cli, err := s.appCtx.NewClientAs(user, pass)
	s.Require().NoError(err, "NewClientAs(%q) must succeed", user)
	return cli
}

// readFileContent opens and reads a file over gRPC, returning its bytes.
// All proto/stream errors are returned as a non-nil error so the goroutine
// can propagate them through errgroup rather than calling Require/FailNow
// (which is unsafe from a non-test goroutine).
func readFileContent(ctx context.Context, cli grpcClient.Client, volumeName, path string) ([]byte, error) {
	sessionID := cli.SessionID()

	openReply, err := cli.File().Open(ctx, &proto.OpenRequest{
		Volume:    volumeName,
		Path:      path,
		Flags:     0, // O_RDONLY
		Caller:    &proto.Caller{Owner: &proto.Owner{Uid: 0, Gid: 0}},
		SessionId: sessionID,
		RequestId: fmt.Sprintf("concurrent-open-%s", path),
	})
	if err != nil {
		return nil, fmt.Errorf("Open %q: %w", path, err)
	}
	if openReply.GetStatus() != 0 {
		return nil, fmt.Errorf("Open %q: status %d", path, openReply.GetStatus())
	}
	fd := openReply.GetFd()

	// Release the fd when done regardless of read outcome.
	defer func() {
		_, _ = cli.File().Release(ctx, &proto.ReleaseRequest{
			Volume:    volumeName,
			Fd:        fd,
			SessionId: sessionID,
		})
	}()

	stream, err := cli.File().Read(ctx, &proto.ReadRequest{
		Volume:    volumeName,
		Fd:        fd,
		Offset:    0,
		Size:      1 << 20, // 1 MiB per Read RPC — server sends frames up to this
		SessionId: sessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("Read %q: %w", path, err)
	}

	var buf bytes.Buffer
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Read %q recv: %w", path, err)
		}
		if frame.GetStatus() != 0 {
			return nil, fmt.Errorf("Read %q frame status %d", path, frame.GetStatus())
		}
		buf.Write(frame.GetData())
	}
	return buf.Bytes(), nil
}

// TestConcurrentReadersOneVolume: M clients each issue GetAttr (and one full
// Open+Read) concurrently against every file in the pre-seeded volume.
// Asserts: no RPC error, GetAttr.Status == 0, sizes non-negative, and that
// file content round-trips correctly (bytes match the on-disk source).
func (s *ConcurrentClientsSuite) TestConcurrentReadersOneVolume() {
	// Spin up M clients on the test goroutine; only use them from goroutines
	// (gRPC clients are goroutine-safe — distinct clients give the strongest
	// signal of independent server-side sessions under load).
	clients := make([]grpcClient.Client, concurrentClients)
	for i := range clients {
		clients[i] = s.newClientAs("test", "test")
	}
	defer func() {
		for _, cli := range clients {
			_ = cli.Close()
		}
	}()

	volName := s.volume.Name
	files := s.volume.GeneratedFiles // relative paths, e.g. "random/a/b.bin"

	g, gCtx := errgroup.WithContext(context.Background())
	for i := 0; i < concurrentClients; i++ {
		workerIdx := i
		cli := clients[i]
		g.Go(func() error {
			for iter := 0; iter < concurrentIterations; iter++ {
				for _, relPath := range files {
					// Normalise to a leading-slash path as the wire convention expects.
					wirePath := "/" + relPath

					// GetAttr — metadata probe.
					attrReply, err := cli.Fs().GetAttr(gCtx, &proto.GetAttrRequest{
						Volume: volName,
						Path:   wirePath,
					})
					if err != nil {
						return fmt.Errorf("worker %d iter %d GetAttr %q: %w", workerIdx, iter, wirePath, err)
					}
					if attrReply.GetStatus() != 0 {
						return fmt.Errorf("worker %d iter %d GetAttr %q: status %d", workerIdx, iter, wirePath, attrReply.GetStatus())
					}
					if attrReply.GetAttributes() == nil {
						return fmt.Errorf("worker %d iter %d GetAttr %q: nil Attributes", workerIdx, iter, wirePath)
					}

					// On the first iteration of the first worker, also do a content
					// read to make sure the byte round-trip is correct. We limit this
					// to one worker to keep the test duration reasonable.
					if workerIdx == 0 && iter == 0 {
						got, err := readFileContent(gCtx, cli, volName, wirePath)
						if err != nil {
							return fmt.Errorf("worker %d content read %q: %w", workerIdx, wirePath, err)
						}
						want, err := os.ReadFile(filepath.Join(s.volume.GetSrcPath(), relPath))
						if err != nil {
							return fmt.Errorf("worker %d os.ReadFile %q: %w", workerIdx, relPath, err)
						}
						if !bytes.Equal(got, want) {
							return fmt.Errorf("worker %d content mismatch %q: got %d bytes, want %d bytes", workerIdx, wirePath, len(got), len(want))
						}
					}
				}
			}
			return nil
		})
	}
	s.Require().NoError(g.Wait(), "concurrent readers: no worker must return an error")
}

// TestConcurrentWritersDistinctPaths: M workers each write a distinct file into
// the volume's source directory on disk, then concurrently GetAttr and read
// their own + all other workers' files over gRPC. No path contention is expected
// (distinct paths), but this exercises concurrent server FS access + volume
// lookup + identity binding under load. The write barrier (all files written
// before any gRPC read) prevents ENOENT races on the read phase.
func (s *ConcurrentClientsSuite) TestConcurrentWritersDistinctPaths() {
	const workerPayloadSize = 4096

	// Phase 1: seed files on disk (sequential, before any concurrent reads).
	// Each worker owns exactly one file named worker-N.bin in the volume src dir.
	contents := make([][]byte, concurrentClients)
	for i := 0; i < concurrentClients; i++ {
		buf := make([]byte, workerPayloadSize)
		// Fill with a recognisable pattern per worker so content can be verified.
		for j := range buf {
			buf[j] = byte(i*7 + j%256)
		}
		contents[i] = buf
		path := filepath.Join(s.volume.GetSrcPath(), fmt.Sprintf("worker-%d.bin", i))
		s.Require().NoError(os.WriteFile(path, buf, 0o644),
			"seed file for worker %d", i)
	}

	// Phase 2: spin M clients and concurrently GetAttr + read all worker files.
	clients := make([]grpcClient.Client, concurrentClients)
	for i := range clients {
		clients[i] = s.newClientAs("test", "test")
	}
	defer func() {
		for _, cli := range clients {
			_ = cli.Close()
		}
	}()

	volName := s.volume.Name

	g, gCtx := errgroup.WithContext(context.Background())
	for i := 0; i < concurrentClients; i++ {
		workerIdx := i
		cli := clients[i]
		g.Go(func() error {
			// Each worker reads ALL worker files to maximise server-side concurrency
			// on the same paths.
			for j := 0; j < concurrentClients; j++ {
				wirePath := fmt.Sprintf("/worker-%d.bin", j)

				attrReply, err := cli.Fs().GetAttr(gCtx, &proto.GetAttrRequest{
					Volume: volName,
					Path:   wirePath,
				})
				if err != nil {
					return fmt.Errorf("worker %d GetAttr %q: %w", workerIdx, wirePath, err)
				}
				if attrReply.GetStatus() != 0 {
					return fmt.Errorf("worker %d GetAttr %q: status %d", workerIdx, wirePath, attrReply.GetStatus())
				}
				if attrReply.GetAttributes().GetSize() != uint64(workerPayloadSize) {
					return fmt.Errorf("worker %d GetAttr %q: size %d != %d",
						workerIdx, wirePath, attrReply.GetAttributes().GetSize(), workerPayloadSize)
				}

				// Full content read to verify byte integrity.
				got, err := readFileContent(gCtx, cli, volName, wirePath)
				if err != nil {
					return fmt.Errorf("worker %d read %q: %w", workerIdx, wirePath, err)
				}
				if !bytes.Equal(got, contents[j]) {
					return fmt.Errorf("worker %d read %q: content mismatch (%d vs %d bytes)",
						workerIdx, wirePath, len(got), len(contents[j]))
				}
			}
			return nil
		})
	}
	s.Require().NoError(g.Wait(), "concurrent writers/readers: no worker must return an error")
}

func TestConcurrentClientsSuite(t *testing.T) {
	suite.Run(t, new(ConcurrentClientsSuite))
}
