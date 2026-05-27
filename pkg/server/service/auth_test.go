package service

import (
	"context"
	"gmountie/pkg/common"
	"gmountie/pkg/server/config"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

// AuthServiceBaseTestSuite is a base test suite with common utilities
type AuthServiceBaseTestSuite struct {
	suite.Suite
	ctx context.Context
}

func (s *AuthServiceBaseTestSuite) SetupTest() {
	s.ctx = context.Background()
}

// createContextWithBasicAuth is a helper to create context with basic auth metadata
func (s *AuthServiceBaseTestSuite) createContextWithBasicAuth(username, password string) context.Context {
	md := metadata.New(map[string]string{
		common.MetadataAuthBasicUsername: username,
		common.MetadataAuthBasicPassword: password,
	})
	return metadata.NewIncomingContext(s.ctx, md)
}

// DenyAllAuthServiceTestSuite tests the fail-closed default auth service
type DenyAllAuthServiceTestSuite struct {
	AuthServiceBaseTestSuite
	service AuthService
}

func (s *DenyAllAuthServiceTestSuite) SetupTest() {
	s.AuthServiceBaseTestSuite.SetupTest()
	s.service = &denyAllAuthService{}
}

func (s *DenyAllAuthServiceTestSuite) TestAuthorize_DeniesWithError() {
	authorized, details, err := s.service.Authorize(s.ctx, "test-method")
	s.Assert().False(authorized)
	s.Assert().Nil(details)
	s.Require().Error(err)
}

// BasicAuthServiceTestSuite is the test suite for BasicAuthService
type BasicAuthServiceTestSuite struct {
	AuthServiceBaseTestSuite
	service AuthService
}

func (s *BasicAuthServiceTestSuite) SetupTest() {
	s.AuthServiceBaseTestSuite.SetupTest()
	users := map[string]string{
		"testuser": "testpass",
		"admin":    "adminpass",
	}
	s.service = NewBasicAuthService(users)
}

func (s *BasicAuthServiceTestSuite) TestAuthorize_ValidCredentials() {
	// Test with valid credentials
	ctx := s.createContextWithBasicAuth("testuser", "testpass")
	authorized, details, err := s.service.Authorize(ctx, "test-method")
	s.Require().NoError(err)
	s.Assert().True(authorized)
	s.Assert().Equal("testuser", details.Username)
}

func (s *BasicAuthServiceTestSuite) TestAuthorize_InvalidPassword() {
	// Test with invalid password
	ctx := s.createContextWithBasicAuth("testuser", "wrongpass")
	authorized, details, err := s.service.Authorize(ctx, "test-method")
	s.Require().Error(err)
	s.Assert().False(authorized)
	s.Assert().Nil(details)
	s.Assert().Contains(err.Error(), "invalid user or password")
}

func (s *BasicAuthServiceTestSuite) TestAuthorize_NonexistentUser() {
	// Test with nonexistent user
	ctx := s.createContextWithBasicAuth("nonexistent", "pass")
	authorized, details, err := s.service.Authorize(ctx, "test-method")
	s.Require().Error(err)
	s.Assert().False(authorized)
	s.Assert().Nil(details)
	s.Assert().Contains(err.Error(), "invalid user or password")
}

func (s *BasicAuthServiceTestSuite) TestAuthorize_NoMetadata() {
	// Test with no metadata
	authorized, details, err := s.service.Authorize(s.ctx, "test-method")
	s.Require().Error(err)
	s.Assert().False(authorized)
	s.Assert().Nil(details)
	s.Assert().Contains(err.Error(), "metadata is not provided")
}

func (s *BasicAuthServiceTestSuite) TestAuthorize_EmptyCredentials() {
	// Create context with empty credentials
	md := metadata.New(map[string]string{})
	ctx := metadata.NewIncomingContext(s.ctx, md)

	authorized, details, err := s.service.Authorize(ctx, "test-method")
	s.Require().Error(err)
	s.Assert().False(authorized)
	s.Assert().Nil(details)
	s.Assert().Contains(err.Error(), "user or password is not provided")
}

// AuthServiceFactoryTestSuite is the test suite for the AuthService factory
type AuthServiceFactoryTestSuite struct {
	suite.Suite
}

func (s *AuthServiceFactoryTestSuite) TestNewAuthServiceFromConfig_Basic() {
	cfg := &config.BasicAuthConfig{
		AuthConfigBase: config.AuthConfigBase{
			Type: config.AuthConfigTypeBasic,
		},
		Users: []config.BasicAuthConfigUser{
			{Username: "test", Password: "pass"},
		},
	}
	service := NewAuthServiceFromConfig(cfg)
	s.Assert().IsType(&BasicAuthService{}, service)
}

// Test Runners
func TestDenyAllAuthServiceTestSuite(t *testing.T) {
	suite.Run(t, new(DenyAllAuthServiceTestSuite))
}

func TestBasicAuthServiceTestSuite(t *testing.T) {
	suite.Run(t, new(BasicAuthServiceTestSuite))
}

func TestAuthServiceFactoryTestSuite(t *testing.T) {
	suite.Run(t, new(AuthServiceFactoryTestSuite))
}
