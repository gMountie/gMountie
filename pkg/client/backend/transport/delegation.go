// delegation.go defines the DelegationHook transport seam. When wired in
// (Part B / single.go), it piggybacks delegation requests on every mutating
// RPC and delivers the server's grant back to the hook. The nil default
// preserves byte-for-byte current behaviour: delegationReq() returns nil
// (the proto field stays unset) and applyGrant() is a no-op.
package transport

import "go.gmountie.dev/gmountie/pkg/proto"

// DelegationHook is implemented by the delegation Manager (Part B). The
// transport layer calls RequestedRoot() to stamp each mutating request and
// Apply() to deliver any grant the server includes in the reply.
type DelegationHook interface {
	// RequestedRoot returns the subtree root the client is requesting a
	// delegation for. Stamped on every mutating RPC as
	// DelegationRequest.Root.
	RequestedRoot() string
	// WalEpoch returns the client wal.db epoch, stamped on every
	// DelegationRequest as DelegationRequest.WalEpoch so the server keys the
	// delegation gen + dedup watermark per (identity, volume, wal-epoch).
	WalEpoch() string
	// Apply is called with the server's DelegationGrant on every successful
	// mutating reply that carries one. Never called with a nil grant.
	Apply(*proto.DelegationGrant)
}

// WithDelegationHook attaches hook to the BackendClient. When non-nil, every
// mutating RPC carries a DelegationRequest and server grants are delivered to
// hook.Apply. A nil BackendOption value (the field default) means today's
// behaviour — no Delegation field, no Apply call — unchanged.
func WithDelegationHook(h DelegationHook) BackendOption {
	return func(b *BackendClient) { b.delegation = h }
}

// delegationReq builds the wire DelegationRequest from the hook, returning nil
// when no hook is set. Returning nil leaves the proto Delegation field unset,
// so the wire is bit-identical to today's requests when no hook is wired in.
func (b *BackendClient) delegationReq() *proto.DelegationRequest {
	if b.delegation == nil {
		return nil
	}
	return &proto.DelegationRequest{Root: b.delegation.RequestedRoot(), WalEpoch: b.delegation.WalEpoch()}
}

// applyGrant delivers the server's grant to the hook, if both the hook and the
// grant are non-nil. Called after every successful mutating reply.
func (b *BackendClient) applyGrant(g *proto.DelegationGrant) {
	if b.delegation != nil && g != nil {
		b.delegation.Apply(g)
	}
}
