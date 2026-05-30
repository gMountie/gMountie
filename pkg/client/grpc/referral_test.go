package grpc

import (
	"context"
	"testing"

	protomocks "go.gmountie.dev/gmountie/internal/mocks/pkg/proto"
	"go.gmountie.dev/gmountie/pkg/client/config"
	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ReferralSuite struct{ suite.Suite }

func TestReferralSuite(t *testing.T) { suite.Run(t, new(ReferralSuite)) }

// fakeClient is a minimal in-package Client double. The mockery MockClient
// can't be used here: it imports pkg/client/grpc, and this in-package test
// (needed for the unexported helpers) importing it would form a cycle.
// Embedding the Client interface satisfies the type; only the methods the
// referral path exercises are overridden — others panic if ever called.
type fakeClient struct {
	Client
	vc         proto.VolumeServiceClient
	connectErr error
	connected  bool
	closed     bool
}

func (f *fakeClient) Volume() proto.VolumeServiceClient { return f.vc }

func (f *fakeClient) Connect() error {
	f.connected = true
	return f.connectErr
}

func (f *fakeClient) Close() error {
	f.closed = true
	return nil
}

// baseConfig returns a minimal client config with a verify-mode TLS block and
// a pinned fingerprint, so tests can assert the referral clears the pin.
func baseConfig() *config.Config {
	return &config.Config{
		Server: &config.ServerConfig{
			Address: "10.0.0.1",
			Port:    9000,
			TLS: config.TLSConfig{
				Verify:              "verify",
				CAFile:              "/etc/ca.pem",
				ExpectedFingerprint: "AA:BB",
				ServerName:          "origin.example.com",
			},
		},
	}
}

func (s *ReferralSuite) newVolumeClient(location string) proto.VolumeServiceClient {
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return(&proto.VolumeResolveReply{Location: location}, nil)
	return vc
}

func (s *ReferralSuite) TestResolveLocation_ReturnsServerLocation() {
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.MatchedBy(func(r *proto.VolumeResolveRequest) bool {
		return r.GetName() == "photos"
	})).Return(&proto.VolumeResolveReply{Location: "v-abc.data.example.com:443"}, nil)

	loc, err := resolveLocation(context.Background(), &fakeClient{vc: vc}, "photos")
	s.Require().NoError(err)
	s.Equal("v-abc.data.example.com:443", loc)
}

func (s *ReferralSuite) TestResolveLocation_PropagatesError() {
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return((*proto.VolumeResolveReply)(nil), errors.New("denied"))

	_, err := resolveLocation(context.Background(), &fakeClient{vc: vc}, "photos")
	s.Require().Error(err)
}

func (s *ReferralSuite) TestTLSConfigForReferral_RetargetsHostAndClearsPin() {
	cfg := baseConfig()
	out, err := tlsConfigForReferral(cfg, "v-abc.data.example.com:443")
	s.Require().NoError(err)

	// ServerName retargeted to the referred host; pinned fingerprint dropped.
	s.Equal("v-abc.data.example.com", out.Server.TLS.ServerName)
	s.Empty(out.Server.TLS.ExpectedFingerprint)
	// CA + verify mode preserved.
	s.Equal("verify", out.Server.TLS.Verify)
	s.Equal("/etc/ca.pem", out.Server.TLS.CAFile)
	// Original config is not mutated.
	s.Equal("origin.example.com", cfg.Server.TLS.ServerName)
	s.Equal("AA:BB", cfg.Server.TLS.ExpectedFingerprint)
}

func (s *ReferralSuite) TestTLSConfigForReferral_RejectsBadLocation() {
	_, err := tlsConfigForReferral(baseConfig(), "not-a-host-port")
	s.Require().Error(err)
}

// withStubbedBuilder swaps newUnconnectedClientFn for the duration of fn,
// routing each (cfg,endpoint) build to a caller-supplied factory so the
// referral re-dial branch is exercised without real network dials.
func (s *ReferralSuite) withStubbedBuilder(build func(cfg *config.Config, endpoint string) (Client, error), fn func()) {
	orig := newUnconnectedClientFn
	newUnconnectedClientFn = build
	defer func() { newUnconnectedClientFn = orig }()
	fn()
}

func (s *ReferralSuite) TestNewClientForVolume_LocalConnectsConfigured() {
	cfg := baseConfig()
	local := &fakeClient{vc: s.newVolumeClient("")} // empty location = served here

	var dialed []string
	s.withStubbedBuilder(func(_ *config.Config, endpoint string) (Client, error) {
		dialed = append(dialed, endpoint)
		return local, nil
	}, func() {
		got, err := NewClientForVolume(cfg, "photos")
		s.Require().NoError(err)
		s.Equal(local, got)
		s.True(local.connected)
		s.False(local.closed)
	})
	// Only the configured endpoint was dialed; no referral re-dial.
	s.Equal([]string{createEndpoint(cfg.Server)}, dialed)
}

func (s *ReferralSuite) TestNewClientForVolume_FollowsReferral() {
	cfg := baseConfig()
	const loc = "v-abc.data.example.com:443"
	resolver := &fakeClient{vc: s.newVolumeClient(loc)}
	data := &fakeClient{}

	var dialed []string
	s.withStubbedBuilder(func(_ *config.Config, endpoint string) (Client, error) {
		dialed = append(dialed, endpoint)
		if endpoint == loc {
			return data, nil
		}
		return resolver, nil
	}, func() {
		got, err := NewClientForVolume(cfg, "photos")
		s.Require().NoError(err)
		s.Equal(data, got) // the referred client is returned
		s.True(resolver.closed)
		s.True(data.connected)
	})
	s.Equal([]string{createEndpoint(cfg.Server), loc}, dialed)
}

func (s *ReferralSuite) TestNewClientForVolume_ResolveErrorIsFatal() {
	cfg := baseConfig()
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("Resolve", mock.Anything, mock.Anything).
		Return((*proto.VolumeResolveReply)(nil), errors.New("denied"))
	resolver := &fakeClient{vc: vc}

	s.withStubbedBuilder(func(_ *config.Config, _ string) (Client, error) {
		return resolver, nil
	}, func() {
		_, err := NewClientForVolume(cfg, "photos")
		s.Require().Error(err) // no fallback; resolve failure fails the mount
		s.True(resolver.closed)
		s.False(resolver.connected)
	})
}
