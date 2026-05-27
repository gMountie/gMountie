package service

import "github.com/pkg/errors"

// Identity is the resolved server-side identity of a principal on a volume.
// Caps is carried for Phase 3 (admin capabilities); it is unused in Phase 1a.
type Identity struct {
	Principal string
	Uid       uint32
	Gid       uint32   // primary
	Gids      []uint32 // supplementary, MUST include Gid
	Caps      []string // Phase 3 (dac_read/dac_override); empty in 1a
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
