package grpc

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ResolverDialTargetSuite struct{ suite.Suite }

func TestResolverDialTargetSuite(t *testing.T) { suite.Run(t, new(ResolverDialTargetSuite)) }

// A bare host:port must be forced onto the passthrough resolver so gRPC does
// not use its built-in "dns" resolver (which stalls ~20s per dial on macOS).
func (s *ResolverDialTargetSuite) TestBareHostPortGetsPassthrough() {
	s.Equal("passthrough:///mount.gmountie.cloud:443", resolverDialTarget("mount.gmountie.cloud:443"))
	s.Equal("passthrough:///127.0.0.1:9449", resolverDialTarget("127.0.0.1:9449"))
}

// An endpoint that already carries a resolver scheme must be returned
// unchanged (no double-prefix), so callers/tests passing an explicit scheme
// keep their chosen resolver.
func (s *ResolverDialTargetSuite) TestExplicitSchemeUnchanged() {
	s.Equal("passthrough:///test-unreachable", resolverDialTarget("passthrough:///test-unreachable"))
	s.Equal("dns:///mount.gmountie.cloud:443", resolverDialTarget("dns:///mount.gmountie.cloud:443"))
	s.Equal("unix:///tmp/sock", resolverDialTarget("unix:///tmp/sock"))
}
