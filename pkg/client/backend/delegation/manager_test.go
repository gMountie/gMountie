package delegation

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"go.gmountie.dev/gmountie/pkg/proto"
)

type fakeInv struct{ subtrees []string }

func (f *fakeInv) InvalidateSubtree(p string) { f.subtrees = append(f.subtrees, p) }

type ManagerSuite struct{ suite.Suite }

func TestManagerSuite(t *testing.T) { suite.Run(t, new(ManagerSuite)) }

func (s *ManagerSuite) TestApplyThenIsDelegated() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})
	s.True(m.IsDelegated("proj/src/a.go"))
	s.False(m.IsDelegated("other/x"))
}

func (s *ManagerSuite) TestExcludedPathNotDelegated() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj", ExcludedPaths: []string{"proj/vendor"}})
	s.True(m.IsDelegated("proj/src/a.go"))
	s.False(m.IsDelegated("proj/vendor/dep/x"))
}

func (s *ManagerSuite) TestOnRecallDropsAndInvalidates() {
	inv := &fakeInv{}
	m := NewManager(inv)
	defer m.Close()
	m.Apply(&proto.DelegationGrant{GrantedRoot: "proj"})
	m.OnRecall("proj")
	s.False(m.IsDelegated("proj/src/a.go"))
	s.Equal([]string{"proj"}, inv.subtrees)
}

func (s *ManagerSuite) TestEmptyGrantNoOp() {
	m := NewManager(&fakeInv{})
	defer m.Close()
	m.Apply(&proto.DelegationGrant{}) // denied
	s.False(m.IsDelegated("anything"))
}
