package common

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type FingerprintIDTestSuite struct{ suite.Suite }

func (s *FingerprintIDTestSuite) TestEmptyInput() {
	s.Empty(FingerprintID(""), "empty input must return empty string")
}

func (s *FingerprintIDTestSuite) TestDeterministic() {
	id := "some-session-uuid-1234"
	first := FingerprintID(id)
	second := FingerprintID(id)
	s.Equal(first, second, "same input must produce same fingerprint")
}

func (s *FingerprintIDTestSuite) TestLength() {
	fp := FingerprintID("any-non-empty-id")
	s.Len(fp, 16, "fingerprint must be exactly 16 hex characters")
}

func (s *FingerprintIDTestSuite) TestDifferentInputsDifferentFingerprints() {
	fp1 := FingerprintID("alice-session-id")
	fp2 := FingerprintID("bob-session-id")
	s.NotEqual(fp1, fp2, "different ids must produce different fingerprints")
}

func (s *FingerprintIDTestSuite) TestIsHex() {
	fp := FingerprintID("test-id")
	for _, c := range fp {
		s.True((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
			"fingerprint must consist only of lowercase hex characters, got %q", string(c))
	}
}

func TestFingerprintIDTestSuite(t *testing.T) {
	suite.Run(t, new(FingerprintIDTestSuite))
}
