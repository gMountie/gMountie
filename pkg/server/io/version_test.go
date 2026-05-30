package io_test

import (
	"testing"

	"gmountie/pkg/server/io"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/suite"
)

type VersionFromAttrSuite struct{ suite.Suite }

func (s *VersionFromAttrSuite) TestSameTripleSameVersion() {
	a := &fuse.Attr{Mtime: 100, Mtimensec: 200, Size: 1024, Ctime: 50, Ctimensec: 75}
	b := &fuse.Attr{Mtime: 100, Mtimensec: 200, Size: 1024, Ctime: 50, Ctimensec: 75}
	s.Assert().Equal(io.VersionFromAttr(a), io.VersionFromAttr(b))
}

func (s *VersionFromAttrSuite) TestSizeChangeProducesDifferentVersion() {
	a := &fuse.Attr{Mtime: 100, Size: 1024, Ctime: 50}
	b := &fuse.Attr{Mtime: 100, Size: 1025, Ctime: 50}
	s.Assert().NotEqual(io.VersionFromAttr(a), io.VersionFromAttr(b))
}

func (s *VersionFromAttrSuite) TestMtimeChangeProducesDifferentVersion() {
	a := &fuse.Attr{Mtime: 100, Size: 1024, Ctime: 50}
	b := &fuse.Attr{Mtime: 101, Size: 1024, Ctime: 50}
	s.Assert().NotEqual(io.VersionFromAttr(a), io.VersionFromAttr(b))
}

func (s *VersionFromAttrSuite) TestCtimeChangeProducesDifferentVersion() {
	// chmod / chown changes ctime without touching mtime or size.
	a := &fuse.Attr{Mtime: 100, Size: 1024, Ctime: 50}
	b := &fuse.Attr{Mtime: 100, Size: 1024, Ctime: 51}
	s.Assert().NotEqual(io.VersionFromAttr(a), io.VersionFromAttr(b))
}

func (s *VersionFromAttrSuite) TestNilAttrYieldsZero() {
	s.Assert().Equal(uint64(0), io.VersionFromAttr(nil))
}

// TestMtimeEqCtimeDistinct is the regression for the XOR-cancellation bug:
// when mtime == ctime (the common post-write state), same-size overwrites at
// different timestamps must still produce different versions.
func (s *VersionFromAttrSuite) TestMtimeEqCtimeDistinct() {
	// Both attrs have the same size; mtime==ctime==T1 vs mtime==ctime==T2.
	a := &fuse.Attr{Mtime: 100, Mtimensec: 0, Size: 4096, Ctime: 100, Ctimensec: 0}
	b := &fuse.Attr{Mtime: 101, Mtimensec: 0, Size: 4096, Ctime: 101, Ctimensec: 0}
	s.Assert().NotEqual(io.VersionFromAttr(a), io.VersionFromAttr(b),
		"same-size overwrites with mtime==ctime at different timestamps must produce different versions")
}

// TestNilAttrYieldsZeroNotRealAttr ensures the nil sentinel (0) is not aliased
// by any real attr's version.
func (s *VersionFromAttrSuite) TestNilAttrNeverAliasedByZeroInput() {
	// An all-zero attr is a degenerate but valid attr; its version must not be 0.
	a := &fuse.Attr{}
	s.Assert().NotEqual(uint64(0), io.VersionFromAttr(a),
		"all-zero attr must not produce version 0 (that is the nil sentinel)")
}

func TestVersionFromAttrSuite(t *testing.T) { suite.Run(t, new(VersionFromAttrSuite)) }
