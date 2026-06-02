package service

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

// Named distinctly from any existing revocation suite to avoid a duplicate
// TestXxx registration in the package.
type RevocationStoreSerialsSuite struct{ suite.Suite }

func TestRevocationStoreSerialsSuite(t *testing.T) {
	suite.Run(t, new(RevocationStoreSerialsSuite))
}

func (s *RevocationStoreSerialsSuite) TestSerialsReturnsSortedCanonicalKeys() {
	r := NewRevocationStore()
	// Mixed formats + out of order; Set normalizes to lowercase-hex SerialKeys.
	r.Set([]string{"0xEF01", "AB:CD", "00ab"})

	// "0xEF01" → "ef01", "AB:CD" → "abcd" (colon stripped, 4-digit hex),
	// "00ab" → "ab" (big.Int drops leading zeros), returned sorted.
	s.Require().Equal([]string{"ab", "abcd", "ef01"}, r.Serials())
}

func (s *RevocationStoreSerialsSuite) TestSerialsEmptyWhenNoneSet() {
	r := NewRevocationStore()
	s.Require().Empty(r.Serials())
}

func (s *RevocationStoreSerialsSuite) TestSerialsNilOnZeroValueStore() {
	// A RevocationStore built without the constructor has a nil snapshot; Serials
	// must not panic.
	var r RevocationStore
	s.Require().Nil(r.Serials())
}
