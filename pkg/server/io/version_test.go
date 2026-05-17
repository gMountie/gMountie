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

func TestVersionFromAttrSuite(t *testing.T) { suite.Run(t, new(VersionFromAttrSuite)) }
