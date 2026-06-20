package mount

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// fakeHandle implements mountHandle for the refactor test.
type fakeHandle struct {
	waited    bool
	unmounted string
}

func (h *fakeHandle) Wait()                  { h.waited = true }
func (h *fakeHandle) Unmount(p string) error { h.unmounted = p; return nil }

type HandleSuite struct{ suite.Suite }

func TestHandleSuite(t *testing.T) { suite.Run(t, new(HandleSuite)) }

func (s *HandleSuite) TestMountHandleInterfaceShape() {
	var h mountHandle = &fakeHandle{}
	h.Wait()
	s.Require().NoError(h.Unmount("/mnt/x"))
	s.Equal("/mnt/x", h.(*fakeHandle).unmounted)
}
