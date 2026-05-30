package ops

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.gmountie.dev/gmountie/pkg/common/passhash"
	"go.gmountie.dev/gmountie/pkg/server/config"

	"github.com/stretchr/testify/suite"
)

func TestBasicAuthSuite(t *testing.T) {
	suite.Run(t, new(BasicAuthSuite))
}

type BasicAuthSuite struct {
	suite.Suite
}

func mustHash(t *testing.T, s string) string {
	t.Helper()
	h, err := passhash.HashFast(s)
	if err != nil {
		t.Fatalf("passhash.Hash: %v", err)
	}
	return h
}

func okHandler(reached *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		*reached = true
		w.WriteHeader(http.StatusOK)
	})
}

func (s *BasicAuthSuite) TestNilAuthIsPassthrough() {
	reached := false
	var a *BasicAuth
	h := a.Wrap(okHandler(&reached))

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	s.True(reached, "handler should be called when auth is nil")
	s.Equal(http.StatusOK, rec.Code)
}

func (s *BasicAuthSuite) TestMissingAuthHeaderReturns401() {
	a := NewBasicAuth([]config.BasicAuthConfigUser{
		{Username: "admin", PasswordHash: mustHash(s.T(), "pw")},
	})
	h := a.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	s.Equal(http.StatusUnauthorized, rec.Code)
	s.Equal(`Basic realm="gmountie-ops"`, rec.Header().Get("WWW-Authenticate"))
}

func (s *BasicAuthSuite) TestWrongPasswordReturns401() {
	a := NewBasicAuth([]config.BasicAuthConfigUser{
		{Username: "admin", PasswordHash: mustHash(s.T(), "correct")},
	})
	h := a.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.SetBasicAuth("admin", "wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	s.Equal(http.StatusUnauthorized, rec.Code)
}

func (s *BasicAuthSuite) TestRightPasswordReachesHandler() {
	reached := false
	a := NewBasicAuth([]config.BasicAuthConfigUser{
		{Username: "admin", PasswordHash: mustHash(s.T(), "secret")},
	})
	h := a.Wrap(okHandler(&reached))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.SetBasicAuth("admin", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	s.True(reached, "handler should be called with correct credentials")
	s.Equal(http.StatusOK, rec.Code)
}

func (s *BasicAuthSuite) TestUnknownUserReturns401() {
	a := NewBasicAuth([]config.BasicAuthConfigUser{
		{Username: "admin", PasswordHash: mustHash(s.T(), "pw")},
	})
	h := a.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.SetBasicAuth("ghost", "anything")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	s.Equal(http.StatusUnauthorized, rec.Code)
}
