package passhash

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

type Argon2idSuite struct{ suite.Suite }

func TestArgon2idSuite(t *testing.T) { suite.Run(t, new(Argon2idSuite)) }

func (s *Argon2idSuite) TestHash_RoundTripsThroughVerify() {
	h, err := Hash("hunter2")
	s.Require().NoError(err)
	s.Require().True(strings.HasPrefix(h, "$argon2id$v=19$m=65536,t=3,p=4$"),
		"expected PHC prefix, got %q", h)

	ok, err := Verify(h, "hunter2")
	s.Require().NoError(err)
	s.True(ok, "correct password must verify")

	ok, err = Verify(h, "wrong")
	s.Require().NoError(err)
	s.False(ok, "wrong password must not verify")
}

func (s *Argon2idSuite) TestHash_RandomSaltDifferentEachCall() {
	h1, err := Hash("same")
	s.Require().NoError(err)
	h2, err := Hash("same")
	s.Require().NoError(err)
	s.NotEqual(h1, h2, "salt must be fresh per call so hashes differ")

	ok, err := Verify(h1, "same")
	s.Require().NoError(err)
	s.True(ok)
	ok, err = Verify(h2, "same")
	s.Require().NoError(err)
	s.True(ok)
}

func (s *Argon2idSuite) TestVerify_RejectsMalformedHash() {
	cases := []string{
		"",
		"not-a-hash",
		"$argon2id$",                              // missing fields
		"$argon2id$v=19$m=65536,t=3,p=4$",         // missing salt + hash
		"$argon2id$v=19$m=BAD,t=3,p=4$YWFh$YmJi",  // malformed param
		"$bcrypt$something",                        // wrong algorithm
	}
	for _, c := range cases {
		ok, err := Verify(c, "x")
		s.Require().Error(err, "case %q must error", c)
		s.False(ok)
	}
}

func (s *Argon2idSuite) TestIsHashed_Cheap() {
	s.True(IsHashed("$argon2id$v=19$m=65536,t=3,p=4$YWFh$YmJi"))
	s.False(IsHashed("plaintext"))
	s.False(IsHashed(""))
	s.False(IsHashed("$bcrypt$x"))
}
