package ops

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/service"

	"github.com/stretchr/testify/suite"
)

type ReloadSuite struct{ suite.Suite }

func TestReloadSuite(t *testing.T) { suite.Run(t, new(ReloadSuite)) }

func (s *ReloadSuite) writeConfig(dir, body string) string {
	path := filepath.Join(dir, "server.yaml")
	s.Require().NoError(os.WriteFile(path, []byte(body), 0o600))
	return path
}

const reloadCfgGranted = `
auth:
  type: mtls
  default_allow: false
  revoked_serials: []
  users:
    - username: alice
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`

const reloadCfgRevoked = `
auth:
  type: mtls
  default_allow: false
  revoked_serials: ["dead"]
  users:
    - username: alice
      volumes: []
volumes:
  - name: photos
    path: /tmp
`

// Two principals both granted "photos"; nothing revoked.
const reloadCfgTwoUsers = `
auth:
  type: mtls
  default_allow: false
  revoked_serials: []
  users:
    - username: alice
      volumes: [photos]
    - username: bob
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`

// alice's volume access withdrawn (no serial blocked); bob retained.
const reloadCfgAliceRevokedByACL = `
auth:
  type: mtls
  default_allow: false
  revoked_serials: []
  users:
    - username: alice
      volumes: []
    - username: bob
      volumes: [photos]
volumes:
  - name: photos
    path: /tmp
`

func reapedCount(s *ReloadSuite, rec *httptest.ResponseRecorder) int {
	var body map[string]int
	s.Require().NoError(json.Unmarshal(rec.Body.Bytes(), &body))
	return body["reaped"]
}

func (s *ReloadSuite) deps(path string) (service.VolumeService, service.SessionManager, *service.RevocationStore, *config.Config) {
	cfg, err := config.ReloadFromFile(path)
	s.Require().NoError(err)
	vs, err := service.NewVolumeService(cfg)
	s.Require().NoError(err)
	return vs, service.NewSessionManager(service.SessionManagerOptions{}), service.NewRevocationStore(), cfg
}

func (s *ReloadSuite) TestReloadAppliesAndReaps() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgGranted)
	vs, sm, rs, cfg := s.deps(path)
	// alice has an open session on the revoked device.
	id, _ := sm.Create("alice", "dead")

	// Operator rewrites the file to revoke alice's volume + block the serial.
	s.Require().NoError(os.WriteFile(path, []byte(reloadCfgRevoked), 0o600))

	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ops/acl/reload", nil))

	s.Equal(http.StatusOK, rec.Code)
	s.True(rs.IsBlocked("dead")) // blocklist swapped
	_, err := sm.Get(id)
	s.Require().Error(err) // session reaped
}

// Revoking a principal's volume access (without blocking any serial) reaps that
// principal's session via the ACL branch of the predicate, while a principal
// who keeps access is left untouched — the additive/no-op case.
func (s *ReloadSuite) TestReloadReapsByACLRevocationKeepsRetained() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgTwoUsers)
	vs, sm, rs, cfg := s.deps(path)
	aliceID, _ := sm.Create("alice", "alicecert") // serial NOT blocked
	bobID, _ := sm.Create("bob", "bobcert")

	// alice loses her volume; bob keeps his; no serial is blocked.
	s.Require().NoError(os.WriteFile(path, []byte(reloadCfgAliceRevokedByACL), 0o600))

	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ops/acl/reload", nil))

	s.Equal(http.StatusOK, rec.Code)
	s.Equal(1, reapedCount(s, rec))    // exactly one reaped
	s.False(rs.IsBlocked("alicecert")) // reaped by ACL, not by serial block
	_, errAlice := sm.Get(aliceID)
	s.Require().Error(errAlice) // alice reaped (no accessible volume)
	_, errBob := sm.Get(bobID)
	s.Require().NoError(errBob) // bob retained access → not reaped
}

func (s *ReloadSuite) TestReloadBadConfigKeepsState() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgGranted)
	vs, sm, rs, cfg := s.deps(path)

	// Establish non-trivial prior state the fail-safe must preserve ("beef" is
	// valid hex, so it survives normalization into the blocklist).
	rs.Set([]string{"beef"})
	id, _ := sm.Create("alice", "beef")

	// Corrupt the file: invalid auth type fails validation.
	s.Require().NoError(os.WriteFile(path, []byte("auth:\n  type: bogus\n"), 0o600))

	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/ops/acl/reload", nil))

	s.Equal(http.StatusBadRequest, rec.Code)
	s.True(rs.IsBlocked("beef")) // blocklist NOT swapped — prior state stands
	_, err := sm.Get(id)
	s.Require().NoError(err) // no reap ran — session intact
}

func (s *ReloadSuite) TestReloadRejectsGET() {
	dir := s.T().TempDir()
	path := s.writeConfig(dir, reloadCfgGranted)
	vs, sm, rs, cfg := s.deps(path)
	h := ReloadHandler(cfg, vs, sm, rs)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ops/acl/reload", nil))
	s.Equal(http.StatusMethodNotAllowed, rec.Code)
}
