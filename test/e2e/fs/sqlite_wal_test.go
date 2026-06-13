package fs

import (
	"errors"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"go.gmountie.dev/gmountie/test/e2e/utils"

	"github.com/stretchr/testify/suite"
)

// SQLiteWALTestSuite is the regression guard for issue #111: opening a SQLite
// database in WAL mode on a gMountie mount used to crash with SIGBUS (the
// kernel can't back SQLite's MAP_SHARED writable mmap of the -shm sidecar over
// FUSE), and the leftover -wal/-shm pinned the DB in WAL mode so every later
// open SIGBUSed too — bricking it. The client now opens -shm sidecars with
// FOPEN_DIRECT_IO, which makes the kernel refuse that mmap with a clean error
// instead of a bus fault. SQLite therefore returns a normal I/O error (or
// works), but never crashes, and the DB is never left unopenable.
//
// We drive the real `sqlite3` CLI because the bug is in the kernel↔FUSE mmap
// interaction — only a live mount through /dev/fuse exercises it (the node
// unit tests assert the FOPEN_DIRECT_IO flag; this asserts the end behaviour).
// Skipped when sqlite3 is not installed.
type SQLiteWALTestSuite struct {
	suite.Suite
	testAppCtx *utils.AppTestingContext
	volume     *utils.TestVolume
}

func (s *SQLiteWALTestSuite) SetupSuite() {
	if _, err := exec.LookPath("sqlite3"); err != nil {
		s.T().Skip("sqlite3 CLI not installed; skipping WAL-over-FUSE regression test")
	}
	testAppCtx, err := utils.NewAppTestingContext(
		utils.WithBasicAuth("test", "test"),
		utils.WithRandomTestVolume(true),
	)
	s.Require().NoError(err)
	utils.Must0(s.T(), testAppCtx.Start())
	s.testAppCtx = testAppCtx
	s.T().Cleanup(func() { _ = testAppCtx.Close() })
	s.volume = s.testAppCtx.GetVolumes()[0]
	s.Require().NotNil(s.volume)
	s.Require().NoError(s.testAppCtx.MountVolumeErr(s.volume))
}

func (s *SQLiteWALTestSuite) TearDownSuite() {
	if s.testAppCtx != nil {
		s.Require().NoError(s.testAppCtx.Close())
	}
}

// runSQLite executes the sqlite3 CLI against dbPath with the given SQL and
// reports whether the child died from SIGBUS (the #111 crash signature).
func (s *SQLiteWALTestSuite) runSQLite(dbPath, sql string) (out string, sigbus bool) {
	s.T().Helper()
	cmd := exec.Command("sqlite3", dbPath, sql)
	b, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGBUS {
			sigbus = true
		}
	}
	return string(b), sigbus
}

// TestWALModeDoesNotSIGBUS runs the exact repro from issue #111 and asserts the
// sqlite3 process is never killed by SIGBUS, and that the database remains
// openable afterwards (not bricked by leftover WAL sidecars). WAL may still
// fail cleanly over the network mount — that is acceptable; a hard crash is not.
func (s *SQLiteWALTestSuite) TestWALModeDoesNotSIGBUS() {
	db := filepath.Join(s.volume.GetMountPath(), "notes.db")

	// 1. Create a table on the default (DELETE) journal so there is real data.
	out, sigbus := s.runSQLite(db, "CREATE TABLE notes(body TEXT); INSERT INTO notes(body) VALUES('seed');")
	s.Require().False(sigbus, "table create must not SIGBUS: %s", out)

	// 2. Switch to WAL and write — this is what bus-faulted before the fix.
	out, sigbus = s.runSQLite(db, `PRAGMA journal_mode=WAL; INSERT INTO notes(body) VALUES('wal-write');`)
	s.Require().False(sigbus, "WAL-mode write must not SIGBUS (issue #111): %s", out)

	// 3. The killer part of the bug: every *subsequent* open also SIGBUSed,
	//    leaving the DB unopenable. Assert it still opens cleanly.
	out, sigbus = s.runSQLite(db, "PRAGMA integrity_check;")
	s.Require().False(sigbus, "DB must stay openable after WAL attempt, not be bricked by SIGBUS: %s", out)

	// 4. And the original rows are intact and readable.
	out, sigbus = s.runSQLite(db, "SELECT count(*) FROM notes;")
	s.Require().False(sigbus, "reading rows must not SIGBUS: %s", out)
}

func TestSQLiteWALTestSuite(t *testing.T) {
	suite.Run(t, new(SQLiteWALTestSuite))
}
