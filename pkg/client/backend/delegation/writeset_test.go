package delegation

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type WriteSetSuite struct{ suite.Suite }

func TestWriteSetSuite(t *testing.T) { suite.Run(t, new(WriteSetSuite)) }

func (s *WriteSetSuite) TestSingleDirIsThatDir() {
	w := newWriteSet(16)
	w.record("proj/src/a.go")
	w.record("proj/src/b.go")
	s.Equal("proj/src", w.root())
}

func (s *WriteSetSuite) TestScatterPromotesToCommonAncestor() {
	w := newWriteSet(16)
	w.record("proj/src/a.go")
	w.record("proj/test/b.go")
	s.Equal("proj", w.root())
}

func (s *WriteSetSuite) TestFullScatterIsMountRoot() {
	w := newWriteSet(16)
	w.record("a/x")
	w.record("b/y")
	s.Empty(w.root())
}
