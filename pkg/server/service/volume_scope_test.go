package service

// Volume-scope SAN tests — white-box (package service) so they can call
// PrincipalCanAccess directly and reuse buildService / ctxFor from acl_test.go.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"net/url"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"go.gmountie.dev/gmountie/pkg/server/config"
	"go.gmountie.dev/gmountie/pkg/server/principal"
)

type VolumeScopeTestSuite struct{ suite.Suite }

func TestVolumeScopeTestSuite(t *testing.T) { suite.Run(t, new(VolumeScopeTestSuite)) }

// ctxWithScopedCert builds a context that carries both:
//   - a TLS peer whose VerifiedChains leaf has CN=cn and the given URI SANs, and
//   - a principal.WithPrincipal value equal to cn.
//
// This mirrors the real server wiring: the mTLS interceptor extracts the CN
// from the verified leaf and stores it via principal.WithPrincipal, so
// PrincipalCanAccess sees both the cert (for scope) and the principal (for ACL).
func ctxWithScopedCert(cn string, sans []string) context.Context {
	uris := make([]*url.URL, 0, len(sans))
	for _, s := range sans {
		u, err := url.Parse(s)
		if err != nil {
			panic("bad URI SAN in test: " + err.Error())
		}
		uris = append(uris, u)
	}
	leaf := &x509.Certificate{
		Subject: pkix.Name{CommonName: cn},
		URIs:    uris,
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{
			State: tls.ConnectionState{
				VerifiedChains: [][]*x509.Certificate{{leaf}},
			},
		},
	})
	return principal.WithPrincipal(ctx, cn)
}

// svcBothVolumes builds a VolumeService whose ACL (default_allow=false) grants
// alice access to both "photos" and "team". With a real ACL enabled, the
// volume-scope SAN check is exercised before the ACL grant path, so:
//   - no-SAN tests traverse the full code path (scope no-op → ACL grant);
//   - named-scope tests are denied by the SCOPE gate, not the ACL.
func (s *VolumeScopeTestSuite) svcBothVolumes() *VolumeServiceImpl {
	alice := config.BasicAuthConfigUser{
		Username:     "alice",
		PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder",
		Volumes:      []string{"photos", "team"},
	}
	return buildService(s.T(), authWithUsers(boolPtr(false), alice))
}

// --- Tests -------------------------------------------------------------------

func (s *VolumeScopeTestSuite) TestVolumeScopeSANsRestrictAccess() {
	svc := s.svcBothVolumes()
	ctx := ctxWithScopedCert("alice", []string{"gmountie://pat/pat_1", "gmountie://vol/photos"})
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "photos"))
	err := svc.PrincipalCanAccess(ctx, "team")
	s.Require().Error(err)
	s.Equal(codes.PermissionDenied, status.Code(err))
	s.Contains(err.Error(), "not scoped")
}

func (s *VolumeScopeTestSuite) TestWildcardScopeAllowsAll() {
	svc := s.svcBothVolumes()
	ctx := ctxWithScopedCert("alice", []string{"gmountie://vol/*"})
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "photos"))
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "team"))
}

func (s *VolumeScopeTestSuite) TestNoScopeSANsBehavesAsToday() {
	svc := s.svcBothVolumes()
	ctx := ctxWithScopedCert("alice", nil)
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "photos"))
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "team"))
}

func (s *VolumeScopeTestSuite) TestNonVolURISANsAreIgnored() {
	// Only a pat audit SAN, no vol SANs → unrestricted (no volume scoping).
	svc := s.svcBothVolumes()
	ctx := ctxWithScopedCert("alice", []string{"gmountie://pat/pat_1"})
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "photos"))
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "team"))
}

func (s *VolumeScopeTestSuite) TestNoPeerInfoIsUnrestricted() {
	// No peer in ctx at all (e.g. basic-auth path) → scope check is a no-op.
	svc := s.svcBothVolumes()
	ctx := principal.WithPrincipal(context.Background(), "alice")
	s.Require().NoError(svc.PrincipalCanAccess(ctx, "photos"))
}

// TestResolveNoExistenceLeak pins the no-existence-leak invariant for Resolve:
// a cert scoped to vol-a only must receive PermissionDenied for BOTH an
// existing-but-out-of-scope volume AND a nonexistent volume.  The scoped-out
// caller must never be able to distinguish the two cases via error code.
func (s *VolumeScopeTestSuite) TestResolveNoExistenceLeak() {
	svc := s.svcBothVolumes()
	// Cert is scoped to "photos" only; "team" exists but is out of scope;
	// "does-not-exist" is not configured at all.
	ctx := ctxWithScopedCert("alice", []string{"gmountie://vol/photos"})

	_, errOutOfScope := svc.Resolve(ctx, "team")
	s.Require().Error(errOutOfScope)
	s.Equal(codes.PermissionDenied, status.Code(errOutOfScope),
		"existing-but-out-of-scope volume must return PermissionDenied, not NotFound")

	_, errNonexistent := svc.Resolve(ctx, "does-not-exist")
	s.Require().Error(errNonexistent)
	s.Equal(codes.PermissionDenied, status.Code(errNonexistent),
		"nonexistent volume must return PermissionDenied (not NotFound) to a scoped-out caller")
}

func (s *VolumeScopeTestSuite) TestScopeDoesNotWidenACLDenial() {
	// Defense-in-depth: a wildcard scope SAN does NOT grant access to a volume
	// the ACL never allows. bob holds a "*" scope SAN but is not in the ACL at
	// all (default_allow=false); PrincipalCanAccess must still deny him.
	alice := config.BasicAuthConfigUser{
		Username:     "alice",
		PasswordHash: "$argon2id$v=19$m=65536,t=3,p=4$placeholder$placeholder",
		Volumes:      []string{"photos", "team"},
	}
	// Build a service where only alice is granted — bob has no entry.
	svc := buildService(s.T(), authWithUsers(boolPtr(false), alice))
	ctx := ctxWithScopedCert("bob", []string{"gmountie://vol/*"})
	err := svc.PrincipalCanAccess(ctx, "photos")
	s.Require().Error(err)
	s.Equal(codes.PermissionDenied, status.Code(err))
	// Error must come from the ACL (no grants), not the scope gate.
	s.NotContains(err.Error(), "not scoped")
}
