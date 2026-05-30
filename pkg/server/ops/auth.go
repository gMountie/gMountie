package ops

import (
	"net/http"

	"gmountie/pkg/common/passhash"
	"gmountie/pkg/server/config"
)

// BasicAuth wraps next with HTTP Basic auth backed by argon2id password
// hashes. A nil-receiver BasicAuth is a no-op (auth.type: none).
type BasicAuth struct {
	users map[string]string // username -> password_hash
}

// NewBasicAuth builds a BasicAuth from a slice of users. Returns nil when
// the slice is empty so callers can `if auth != nil { wrap }` cleanly.
func NewBasicAuth(users []config.BasicAuthConfigUser) *BasicAuth {
	if len(users) == 0 {
		return nil
	}
	m := make(map[string]string, len(users))
	for _, u := range users {
		m[u.Username] = u.PasswordHash
	}
	return &BasicAuth{users: m}
}

// Wrap returns next gated by basic-auth. If a is nil, next is returned
// unchanged — the auth.type: none path.
func (a *BasicAuth) Wrap(next http.Handler) http.Handler {
	if a == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok {
			a.deny(w)
			return
		}
		hash, exists := a.users[user]
		// Even when the user doesn't exist, run Verify against the sentinel
		// hash to keep verify latency constant per request — avoids leaking
		// user existence via timing.
		if !exists {
			_, _ = passhash.Verify(sentinelHash, pass)
			a.deny(w)
			return
		}
		match, err := passhash.Verify(hash, pass)
		if err != nil || !match {
			a.deny(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *BasicAuth) deny(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="gmountie-ops"`)
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
}

// sentinelHash is a precomputed argon2id PHC of a value no real user would
// have. Verify is run against it for unknown users so the verify latency is
// roughly the same as for a real user — uniform timing.
var sentinelHash = mustSentinel()

func mustSentinel() string {
	h, err := passhash.Hash("\x00\x00\x00unreachable\x00\x00\x00")
	if err != nil {
		// If hashing fails at init (e.g., no entropy), return an empty
		// string — Verify will reject it cheaply, which is still correct
		// since unknown users never authenticate anyway.
		return ""
	}
	return h
}
