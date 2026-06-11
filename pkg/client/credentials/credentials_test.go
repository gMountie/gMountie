package credentials

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
)

type CredentialsTestSuite struct {
	suite.Suite
}

// validBlob builds a base64-STD-encoded JSON credential with all required
// fields populated.
func validBlob() string {
	return base64.StdEncoding.EncodeToString([]byte(`{
		"cert_pem": "CERT",
		"key_pem": "KEY",
		"ca_pem": "CA",
		"endpoint": "data.example.com:443",
		"server_name": "data.example.com"
	}`))
}

func (s *CredentialsTestSuite) TestDecode_RoundTrip() {
	c, err := Decode(validBlob())
	s.Require().NoError(err)
	s.Equal("CERT", c.CertPEM)
	s.Equal("KEY", c.KeyPEM)
	s.Equal("CA", c.CAPEM)
	s.Equal("data.example.com:443", c.Endpoint)
	s.Equal("data.example.com", c.ServerName)
}

func (s *CredentialsTestSuite) TestDecode_ServerNameOptional() {
	blob := base64.StdEncoding.EncodeToString([]byte(
		`{"cert_pem":"C","key_pem":"K","ca_pem":"A","endpoint":"h:1"}`))
	c, err := Decode(blob)
	s.Require().NoError(err)
	s.Empty(c.ServerName)
}

func (s *CredentialsTestSuite) TestDecode_TrimsWhitespace() {
	c, err := Decode("  " + validBlob() + "\n")
	s.Require().NoError(err)
	s.Equal("CERT", c.CertPEM)
}

func (s *CredentialsTestSuite) TestDecode_BadBase64() {
	_, err := Decode("not!!base64!!")
	s.Require().Error(err)
	s.Contains(err.Error(), "base64")
}

func (s *CredentialsTestSuite) TestDecode_BadJSON() {
	blob := base64.StdEncoding.EncodeToString([]byte("not json"))
	_, err := Decode(blob)
	s.Require().Error(err)
	s.Contains(err.Error(), "JSON")
}

func (s *CredentialsTestSuite) TestDecode_MissingRequiredField() {
	// endpoint omitted
	blob := base64.StdEncoding.EncodeToString([]byte(
		`{"cert_pem":"C","key_pem":"K","ca_pem":"A"}`))
	_, err := Decode(blob)
	s.Require().Error(err)
	s.Contains(err.Error(), "endpoint")
}

func (s *CredentialsTestSuite) TestLoad_Neither() {
	c, err := Load("", "")
	s.Require().NoError(err)
	s.Nil(c)
}

func (s *CredentialsTestSuite) TestLoad_FromEnv() {
	c, err := Load(validBlob(), "")
	s.Require().NoError(err)
	s.Require().NotNil(c)
	s.Equal("CERT", c.CertPEM)
}

func (s *CredentialsTestSuite) TestLoad_FromFile() {
	path := filepath.Join(s.T().TempDir(), "cred")
	s.Require().NoError(os.WriteFile(path, []byte("  "+validBlob()+"\n"), 0600))
	c, err := Load("", path)
	s.Require().NoError(err)
	s.Require().NotNil(c)
	s.Equal("KEY", c.KeyPEM)
}

func (s *CredentialsTestSuite) TestLoad_FileWinsOverEnv() {
	path := filepath.Join(s.T().TempDir(), "cred")
	fileBlob := base64.StdEncoding.EncodeToString([]byte(
		`{"cert_pem":"FILECERT","key_pem":"K","ca_pem":"A","endpoint":"h:1"}`))
	s.Require().NoError(os.WriteFile(path, []byte(fileBlob), 0600))
	c, err := Load(validBlob(), path)
	s.Require().NoError(err)
	s.Equal("FILECERT", c.CertPEM)
}

func (s *CredentialsTestSuite) TestLoad_FileMissing() {
	_, err := Load("", filepath.Join(s.T().TempDir(), "nope"))
	s.Require().Error(err)
}

func (s *CredentialsTestSuite) TestDecodeTokenModeBundle() {
	blob := base64.StdEncoding.EncodeToString([]byte(
		`{"endpoint":"mount.example:443","renew_endpoint":"https://cp.example/v1/certs","renew_token":"gmpat_x"}`))
	c, err := Decode(blob)
	s.Require().NoError(err)
	s.True(c.TokenMode())
	s.Equal("https://cp.example/v1/certs", c.RenewEndpoint)
}

func (s *CredentialsTestSuite) TestTokenModeRequiresToken() {
	blob := base64.StdEncoding.EncodeToString([]byte(
		`{"endpoint":"mount.example:443","renew_endpoint":"https://cp.example/v1/certs"}`))
	_, err := Decode(blob)
	s.Require().Error(err)
	s.Contains(err.Error(), "renew_token")
}

func (s *CredentialsTestSuite) TestStaticModeStillRequiresCertKeyCA() {
	blob := base64.StdEncoding.EncodeToString([]byte(`{"endpoint":"mount.example:443"}`))
	_, err := Decode(blob)
	s.Require().Error(err)
}

func (s *CredentialsTestSuite) TestTokenModeMissingEndpointErrors() {
	blob := base64.StdEncoding.EncodeToString([]byte(
		`{"renew_endpoint":"https://cp.example/v1/certs","renew_token":"gmpat_x"}`))
	_, err := Decode(blob)
	s.Require().Error(err)
	s.Contains(err.Error(), "endpoint")
}

func (s *CredentialsTestSuite) TestMixedModeBlobErrors() {
	blob := base64.StdEncoding.EncodeToString([]byte(
		`{"endpoint":"mount.example:443","renew_endpoint":"https://cp.example/v1/certs","renew_token":"gmpat_x","cert_pem":"CERT","key_pem":"KEY","ca_pem":"CA"}`))
	_, err := Decode(blob)
	s.Require().Error(err)
	s.Contains(err.Error(), "renew_endpoint")
	s.Contains(err.Error(), "cert")
}

func TestCredentialsTestSuite(t *testing.T) {
	suite.Run(t, new(CredentialsTestSuite))
}
