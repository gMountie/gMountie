package service

import (
	"math/big"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type RevocationSuite struct{ suite.Suite }

func TestRevocationSuite(t *testing.T) { suite.Run(t, new(RevocationSuite)) }

func (s *RevocationSuite) TestSerialKeyCanonical() {
	n := big.NewInt(0xABCD)
	s.Equal("abcd", SerialKey(n))
}

func (s *RevocationSuite) TestSerialKeyNilIsEmpty() {
	s.Empty(SerialKey(nil)) // must not panic; absent serial = empty key
}

func (s *RevocationSuite) TestParseSerialKeyNormalizesFormats() {
	for _, in := range []string{"abcd", "ABCD", "ab:cd", "0xABCD", "AB:CD"} {
		key, ok := ParseSerialKey(in)
		s.Require().Truef(ok, "input %q", in)
		s.Equalf("abcd", key, "input %q", in)
	}
	_, ok := ParseSerialKey("nothex!")
	s.False(ok)
	_, ok = ParseSerialKey("0x") // empty after stripping the prefix → invalid
	s.False(ok)
}

func (s *RevocationSuite) TestSetAndIsBlocked() {
	store := NewRevocationStore()
	s.False(store.IsBlocked(SerialKey(big.NewInt(0xABCD))))
	s.False(store.IsBlocked(""))            // no-cert path must never be blocked
	store.Set([]string{"AB:CD", "garbage"}) // garbage silently dropped
	s.True(store.IsBlocked(SerialKey(big.NewInt(0xABCD))))
	s.False(store.IsBlocked(SerialKey(big.NewInt(0x1234))))
	store.Set(nil) // empty reload clears the list
	s.False(store.IsBlocked(SerialKey(big.NewInt(0xABCD))))
}

func (s *RevocationSuite) TestZeroValueIsBlockedSafe() {
	var store RevocationStore // built without the constructor
	s.False(store.IsBlocked("abcd"))
}

func (s *RevocationSuite) TestConcurrentSetAndRead() {
	store := NewRevocationStore()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); store.Set([]string{"abcd"}) }()
		wg.Add(1)
		go func() { defer wg.Done(); _ = store.IsBlocked("abcd") }()
	}
	wg.Wait()
}
