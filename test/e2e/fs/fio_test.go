package fs

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"gmountie/test/e2e/utils"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

// fioTimeout caps each individual fio config run. Configs target tiny
// (16 MiB) workloads and finish in seconds on real hardware; CI
// runners under FUSE-over-gRPC pressure can drag this out, but a
// runaway invocation needs a hard ceiling so go test's own -timeout
// doesn't fire and kill the suite with a useless SIGQUIT dump.
//
// 90s is well over real-world runtime. If a future config legitimately
// needs longer, raise this rather than working around it elsewhere.
//
// Note: exec.CommandContext sends SIGKILL when the context expires,
// but a process stuck in D-state on a hung FUSE op won't die until
// the kernel can deliver the signal. The embedded configs are chosen
// to avoid that class of deadlock (libaio with high iodepth was the
// known offender — see aio-read.fio in the git history).
const fioTimeout = 90 * time.Second

// Embed fio config files
//
//go:embed fio/*.fio
var configs embed.FS

type FioTestSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func (s *FioTestSuite) SetupSuite() {
	// Start the app.
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(false),
	)
	utils.Must0(s.T(), err)
	utils.Must0(s.T(), testAppCtx.Start())

	s.testAppCtx = testAppCtx
	// Copy the fio config files to the volume.
	s.volume = s.testAppCtx.GetVolumes()[0]
	scriptsPath := filepath.Join(s.volume.GetRootPath(), "scripts")
	utils.Must0(s.T(), os.Mkdir(scriptsPath, 0o700))
	utils.Must0(s.T(), CopyEmbedFiles(configs, scriptsPath))

	// Mount the volume.
	s.testAppCtx.MountVolume(s.volume)
}

func (s *FioTestSuite) TestFS() {
	entries, err := configs.ReadDir("fio")
	if err != nil {
		s.T().Fatal(err)
	}

	for _, entry := range entries {
		s.Run(entry.Name(), func() {
			path := filepath.Join(s.volume.GetRootPath(), "scripts", entry.Name())
			ctx, cancel := context.WithTimeout(context.Background(), fioTimeout)
			defer cancel()
			// Default fio output (no --output-format) is shorter and more
			// readable than json+; nothing in the test consumes the blob
			// programmatically, so JSON's only role was filling the CI
			// log. Now we capture stdout silently and surface it only
			// when the test fails.
			cmd := exec.CommandContext(ctx, "fio", path)
			cmd.Dir = s.volume.GetMountPath()
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			out, err := cmd.Output()

			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.T().Fatalf("fio %s timed out after %s\nstdout:\n%s\nstderr:\n%s",
					entry.Name(), fioTimeout, out, stderr.String())
			}
			if err != nil {
				s.T().Fatalf("fio %s failed: %v\nstdout:\n%s\nstderr:\n%s",
					entry.Name(), err, out, stderr.String())
			}
			// Success path: PASS marker from go test is enough.
		})
	}
}

func (s *FioTestSuite) TearDownSuite() {
	err := s.testAppCtx.Close()
	if err != nil {
		s.T().Fatal(err)
	}
}

// ----------- Helpers

// CopyEmbedFiles copies the embed files to the destination.
func CopyEmbedFiles(src embed.FS, dest string) error {
	entries, err := src.ReadDir("fio")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		data, err := src.ReadFile(filepath.Join("fio", entry.Name()))
		if err != nil {
			return err
		}
		err = os.WriteFile(filepath.Join(dest, entry.Name()), data, fs.ModePerm)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestFioTestSuite(t *testing.T) {
	_, err := exec.LookPath("fio")
	if err != nil {
		t.Skip("fio not found, skipping test")
	}
	suite.Run(t, new(FioTestSuite))
}
