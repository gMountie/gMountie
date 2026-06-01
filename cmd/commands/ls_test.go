//go:build linux

package commands

import (
	"bytes"
	"testing"

	"go.gmountie.dev/gmountie/pkg/proto"

	"github.com/spf13/cobra"
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

func (s *LsSuite) TestLsCmd_ProfileAndConfigConflict() {
	profileName, configFile = "", ""
	defer func() { profileName, configFile = "", "" }()

	root := &cobra.Command{Use: "root"}
	root.PersistentFlags().StringVarP(&configFile, "config", "c", "", "config file path")
	root.AddCommand(lsCmd)
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs([]string{"ls", "--profile", "work", "--config", "/tmp/x.yaml"})

	err := root.Execute()
	s.Require().Error(err)
	s.Assert().Contains(err.Error(), "one of --profile or --config")
}
