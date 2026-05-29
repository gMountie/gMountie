package grpc

import (
	"context"
	"testing"

	"gmountie/pkg/common"

	"github.com/stretchr/testify/suite"
)

// BasicAuthCredentialsTestSuite verifies the sessionLive gate in GetRequestMetadata.
type BasicAuthCredentialsTestSuite struct {
	suite.Suite
}

// TestNilSessionLiveAlwaysSendsCreds ensures backward compatibility: when
// sessionLive is not wired (nil), GetRequestMetadata always returns user+pass.
func (s *BasicAuthCredentialsTestSuite) TestNilSessionLiveAlwaysSendsCreds() {
	creds := NewBasicAuthCredentials("alice", "secret")
	s.Nil(creds.sessionLive, "sessionLive must be nil when not wired")

	md, err := creds.GetRequestMetadata(context.Background())
	s.Require().NoError(err)
	s.Equal("alice", md[common.MetadataAuthBasicUsername])
	s.Equal("secret", md[common.MetadataAuthBasicPassword])
}

// TestSessionLiveFalseAlwaysSendsCreds ensures basic-auth is sent when the
// session is not yet live (e.g. during Create, Resume, or recovery).
func (s *BasicAuthCredentialsTestSuite) TestSessionLiveFalseAlwaysSendsCreds() {
	creds := NewBasicAuthCredentials("alice", "secret")
	creds.sessionLive = func() bool { return false }

	md, err := creds.GetRequestMetadata(context.Background())
	s.Require().NoError(err)
	s.Equal("alice", md[common.MetadataAuthBasicUsername])
	s.Equal("secret", md[common.MetadataAuthBasicPassword])
}

// TestSessionLiveTrueOmitsBasicAuth ensures basic-auth is omitted on the
// steady-state hot path once a keepalive-backed session is healthy.
func (s *BasicAuthCredentialsTestSuite) TestSessionLiveTrueOmitsBasicAuth() {
	creds := NewBasicAuthCredentials("alice", "secret")
	creds.sessionLive = func() bool { return true }

	md, err := creds.GetRequestMetadata(context.Background())
	s.Require().NoError(err)
	s.Empty(md, "no basic-auth metadata must be carried when session is live")
}

func TestBasicAuthCredentialsTestSuite(t *testing.T) {
	suite.Run(t, new(BasicAuthCredentialsTestSuite))
}
