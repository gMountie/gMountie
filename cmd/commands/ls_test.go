//go:build linux

package commands

import (
	"bytes"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/stretchr/testify/suite"
)

type LsSuite struct{ suite.Suite }

func TestLsSuite(t *testing.T) { suite.Run(t, new(LsSuite)) }

func (s *LsSuite) TestRenderVolumes() {
	var out bytes.Buffer
	renderVolumes(&out, []*proto.Volume{{Name: "shared"}, {Name: "backups"}})
	s.Contains(out.String(), "shared")
	s.Contains(out.String(), "backups")
}

func (s *LsSuite) TestRenderEmpty() {
	var out bytes.Buffer
	renderVolumes(&out, nil)
	s.Contains(out.String(), "no volumes")
}
