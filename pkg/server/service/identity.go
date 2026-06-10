package service

import "github.com/pkg/errors"

// Identity is the resolved server-side identity of a principal on a volume.
// Caps is carried for Phase 3 (admin capabilities); it is unused in Phase 1a.
type Identity struct {
	Principal  string
	Uid        uint32
	Gid        uint32            // primary
	Gids       []uint32          // supplementary, MUST include Gid
	Caps       []string          // Phase 3 (dac_read/dac_override); empty in 1a
	UserName   string            // populated by resolvers in 1b-2 (empty in 1a/1b-1)
	GroupNames map[uint32]string // gid -> name, for groups the caller is in
}

// ErrPrincipalNotFound is returned by resolvers when a principal cannot be
// resolved. Callers MUST fail closed (deny), never fall back to a privileged
// identity.
var ErrPrincipalNotFound = errors.New("principal not found")

// IdentityResolver maps an authenticated principal to a server-side Identity
// for one volume. One implementation per mapping mode (squash/static/system).
// passthrough does not implement this — its identity comes from the wire
// caller and is handled in BindIdentity.
type IdentityResolver interface {
	Resolve(principal string) (Identity, error)
}

// ConstantIdentityResolver marks resolvers whose Resolve answer for any given
// principal is fixed for the process lifetime (squash: one identity for
// everyone; static: per-principal identities frozen in config). Constancy is
// what makes a volume eligible for the pre-credentialed executor fast path —
// the identity can be applied to worker threads once instead of per op.
// Resolvers backed by mutable system state (system mode's getent) MUST NOT
// report constant; wrappers (the TTL cache) forward their inner resolver's
// answer.
type ConstantIdentityResolver interface {
	IdentityResolver
	// ConstantIdentity reports whether Resolve is time-invariant.
	ConstantIdentity() bool
}

// resolverIsConstant reports whether r promises time-invariant resolution.
func resolverIsConstant(r IdentityResolver) bool {
	c, ok := r.(ConstantIdentityResolver)
	return ok && c.ConstantIdentity()
}
