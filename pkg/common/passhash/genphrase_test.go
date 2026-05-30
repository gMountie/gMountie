package passhash

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type GenPhraseSuite struct{ suite.Suite }

func TestGenPhraseSuite(t *testing.T) { suite.Run(t, new(GenPhraseSuite)) }

func (s *GenPhraseSuite) TestGeneratesUsableDistinctPassphrases() {
	a, err := GeneratePassphrase()
	s.Require().NoError(err)
	b, err := GeneratePassphrase()
	s.Require().NoError(err)

	s.GreaterOrEqual(len(a), 20, "passphrase should be at least 20 chars")
	s.NotEqual(a, b, "two calls must differ (crypto-random)")
	s.NotContains(a, "=", "no base32 padding")

	phc, err := Hash(a)
	s.Require().NoError(err)
	ok, err := Verify(phc, a)
	s.Require().NoError(err)
	s.True(ok)
}
