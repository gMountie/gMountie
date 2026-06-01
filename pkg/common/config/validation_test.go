package config

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/suite"
)

type ValidationTestSuite struct{ suite.Suite }

func TestValidationTestSuite(t *testing.T) { suite.Run(t, new(ValidationTestSuite)) }

// sampleConfig mirrors the shape of real config structs: a nested struct with
// snake_case-mapped fields and the validation tags actually used in the tree.
type sampleServer struct {
	Address        string `validate:"required,ip"`
	FrameSizeBytes int    `validate:"min=4096,max=16777216"`
	Mode           string `validate:"omitempty,oneof=squash static system"`
}

type sampleConfig struct {
	Server  *sampleServer `validate:"required"`
	Volumes []string      `validate:"required"`
}

func (s *ValidationTestSuite) friendly(c any) string {
	err := validator.New().Struct(c)
	s.Require().Error(err)
	out := FriendlyValidationError(err)
	s.Require().Error(out)
	return out.Error()
}

func (s *ValidationTestSuite) TestRequiredFieldUsesDottedKeyPath() {
	msg := s.friendly(sampleConfig{Server: &sampleServer{Address: "1.2.3.4", FrameSizeBytes: 4096}})
	// Volumes is required and absent.
	s.Contains(msg, "volumes")
	s.Contains(msg, "required")
}

func (s *ValidationTestSuite) TestNestedFieldUsesSnakeCaseDottedPath() {
	msg := s.friendly(sampleConfig{
		Server:  &sampleServer{Address: "", FrameSizeBytes: 4096},
		Volumes: []string{"v"},
	})
	s.Contains(msg, "server.address")
}

func (s *ValidationTestSuite) TestIPTagGivesActionableHint() {
	msg := s.friendly(sampleConfig{
		Server:  &sampleServer{Address: "localhost", FrameSizeBytes: 4096},
		Volumes: []string{"v"},
	})
	s.Contains(msg, "server.address")
	s.Contains(msg, "IP")
}

func (s *ValidationTestSuite) TestOneofTagListsAllowedValues() {
	msg := s.friendly(sampleConfig{
		Server:  &sampleServer{Address: "1.2.3.4", FrameSizeBytes: 4096, Mode: "bogus"},
		Volumes: []string{"v"},
	})
	s.Contains(msg, "server.mode")
	s.Contains(msg, "squash static system")
}

func (s *ValidationTestSuite) TestMinMaxTagMentionsBound() {
	msg := s.friendly(sampleConfig{
		Server:  &sampleServer{Address: "1.2.3.4", FrameSizeBytes: 100},
		Volumes: []string{"v"},
	})
	s.Contains(msg, "server.frame_size_bytes")
	s.Contains(msg, "4096")
}

// The raw validator noise must not leak through.
func (s *ValidationTestSuite) TestNoRawValidatorNoise() {
	msg := s.friendly(sampleConfig{Server: &sampleServer{Address: "1.2.3.4", FrameSizeBytes: 4096}})
	s.NotContains(msg, "Field validation for")
	s.NotContains(msg, "Error:Field")
}

// A non-validation error passes through untouched.
func (s *ValidationTestSuite) TestNonValidationErrorPassesThrough() {
	in := assertErr("boom")
	out := FriendlyValidationError(in)
	s.Equal(in, out)
}

type assertErr string

func (e assertErr) Error() string { return string(e) }
