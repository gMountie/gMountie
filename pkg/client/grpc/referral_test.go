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
// routing each build to a caller-supplied factory so the referral re-dial
// branch is exercised without real network dials. The stub receives the same
// (cfg, endpoint, parts, runLoop) the production builder would — referral
// (non-renew) tests ignore parts/runLoop; the renew-aware tests assert on them.
func (s *ReferralSuite) withStubbedBuilder(build func(cfg *config.Config, endpoint string, parts renewParts, runLoop bool) (Client, error), fn func()) {
	orig := newUnconnectedClientFn
	newUnconnectedClientFn = build
	defer func() { newUnconnectedClientFn = orig }()
	fn()
}

func (s *ReferralSuite) TestNewClientForVolume_LocalConnectsConfigured() {
	cfg := baseConfig()
	local := &fakeClient{vc: s.newVolumeClient("")} // empty location = served here

	var dialed []string
	s.withStubbedBuilder(func(_ *config.Config, endpoint string, _ renewParts, _ bool) (Client, error) {
		dialed = append(dialed, endpoint)
		return local, nil
	}, func() {
		got, vol, err := NewClientForVolume(cfg, "photos")
		s.Require().NoError(err)
		s.Equal(local, got)
		s.Equal("photos", vol)
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
	s.withStubbedBuilder(func(buildCfg *config.Config, endpoint string, _ renewParts, _ bool) (Client, error) {
		dialed = append(dialed, endpoint)
		if endpoint == loc {
			// The data-plane build must receive the retargeted TLS config, not
			// the original — else it dials the referred host but verifies its
			// cert against the origin's ServerName and the mount fails.
			s.Equal("v-abc.data.example.com", buildCfg.Server.TLS.ServerName)
			s.Empty(buildCfg.Server.TLS.ExpectedFingerprint)
			return data, nil
		}
		// The resolver build dials the original endpoint with the original TLS.
		s.Equal("origin.example.com", buildCfg.Server.TLS.ServerName)
		return resolver, nil
	}, func() {
		got, vol, err := NewClientForVolume(cfg, "photos")
		s.Require().NoError(err)
		s.Equal(data, got) // the referred client is returned
		s.Equal("photos", vol)
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

	s.withStubbedBuilder(func(_ *config.Config, _ string, _ renewParts, _ bool) (Client, error) {
		return resolver, nil
	}, func() {
		_, _, err := NewClientForVolume(cfg, "photos")
		s.Require().Error(err) // no fallback; resolve failure fails the mount
		s.True(resolver.closed)
		s.False(resolver.connected)
	})
}

// newListResolveClient builds a VolumeServiceClient mock that returns the given
// volume names from List and, for an expected resolved name, an empty Resolve
// location ("served here"). Resolve is wired only when resolveName is non-empty
// so the zero/multi tests can assert Resolve is never reached.
func (s *ReferralSuite) newListResolveClient(names []string, resolveName string) proto.VolumeServiceClient {
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	volumes := make([]*proto.Volume, 0, len(names))
	for _, n := range names {
		volumes = append(volumes, &proto.Volume{Name: n})
	}
	vc.On("List", mock.Anything, mock.Anything).
		Return(&proto.VolumeListReply{Volumes: volumes}, nil)
	if resolveName != "" {
		vc.On("Resolve", mock.Anything, mock.MatchedBy(func(r *proto.VolumeResolveRequest) bool {
			return r.GetName() == resolveName
		})).Return(&proto.VolumeResolveReply{Location: ""}, nil)
	}
	return vc
}

func (s *ReferralSuite) TestNewClientForVolume_EmptyVolumeAutoResolvesSingle() {
	cfg := baseConfig()
	// List returns exactly one volume; Resolve must be called with that name.
	local := &fakeClient{vc: s.newListResolveClient([]string{"photos"}, "photos")}

	s.withStubbedBuilder(func(_ *config.Config, _ string, _ renewParts, _ bool) (Client, error) {
		return local, nil
	}, func() {
		got, vol, err := NewClientForVolume(cfg, "")
		s.Require().NoError(err)
		s.Equal(local, got)
		s.Equal("photos", vol) // discovered name is surfaced to the caller
		s.True(local.connected)
		s.False(local.closed)
	})
}

func (s *ReferralSuite) TestNewClientForVolume_EmptyVolumeNoVolumesIsError() {
	cfg := baseConfig()
	resolver := &fakeClient{vc: s.newListResolveClient([]string{}, "")}

	s.withStubbedBuilder(func(_ *config.Config, _ string, _ renewParts, _ bool) (Client, error) {
		return resolver, nil
	}, func() {
		_, _, err := NewClientForVolume(cfg, "")
		s.Require().Error(err)
		s.Contains(err.Error(), "no volumes available")
		s.True(resolver.closed)
		s.False(resolver.connected)
	})
}

// TestListVolumes_ReturnsNamesPreSession proves the ls path: ListVolumes dials
// an unconnected client, lists over the pre-session VolumeService.List call, and
// returns the names WITHOUT ever running the session handshake (Connect).
func (s *ReferralSuite) TestListVolumes_ReturnsNamesPreSession() {
	cfg := baseConfig()
	client := &fakeClient{vc: s.newListResolveClient([]string{"photos", "music"}, "")}

	var dialed []string
	s.withStubbedBuilder(func(_ *config.Config, endpoint string, _ renewParts, _ bool) (Client, error) {
		dialed = append(dialed, endpoint)
		return client, nil
	}, func() {
		names, err := ListVolumes(cfg)
		s.Require().NoError(err)
		s.Equal([]string{"photos", "music"}, names)
		s.False(client.connected, "ListVolumes must not run the session handshake")
		s.True(client.closed, "ListVolumes must close the client")
	})
	s.Equal([]string{createEndpoint(cfg.Server)}, dialed)
}

// TestListVolumes_SurfacesListError proves a List failure propagates to the
// caller (and the client is still closed).
func (s *ReferralSuite) TestListVolumes_SurfacesListError() {
	cfg := baseConfig()
	vc := protomocks.NewMockVolumeServiceClient(s.T())
	vc.On("List", mock.Anything, mock.Anything).
		Return((*proto.VolumeListReply)(nil), errors.New("denied"))
	client := &fakeClient{vc: vc}

	s.withStubbedBuilder(func(_ *config.Config, _ string, _ renewParts, _ bool) (Client, error) {
		return client, nil
	}, func() {
		_, err := ListVolumes(cfg)
		s.Require().Error(err)
		s.False(client.connected)
		s.True(client.closed)
	})
}

func (s *ReferralSuite) TestListVolumes_NilConfigIsError() {
	_, err := ListVolumes(nil)
	s.Require().Error(err)
	_, err = ListVolumes(&config.Config{})
	s.Require().Error(err)
}

func (s *ReferralSuite) TestNewClientForVolume_EmptyVolumeMultipleIsError() {
	cfg := baseConfig()
	resolver := &fakeClient{vc: s.newListResolveClient([]string{"photos", "music"}, "")}

	s.withStubbedBuilder(func(_ *config.Config, _ string, _ renewParts, _ bool) (Client, error) {
		return resolver, nil
	}, func() {
		_, _, err := NewClientForVolume(cfg, "")
		s.Require().Error(err)
		// Both names are listed so the user knows what to pass to --volume.
		s.Contains(err.Error(), "photos")
		s.Contains(err.Error(), "music")
		s.True(resolver.closed)
		s.False(resolver.connected)
	})
}
