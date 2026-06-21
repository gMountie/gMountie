package controller

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/common"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
)

type SessionOriginSuite struct{ suite.Suite }

func TestSessionOriginSuite(t *testing.T) { suite.Run(t, new(SessionOriginSuite)) }

func (s *SessionOriginSuite) TestReadsSessionFromIncomingMetadata() {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(common.MetadataSessionID, "sess-123"))
	s.Equal("sess-123", sessionIDFromContext(ctx))
}

func (s *SessionOriginSuite) TestEmptyWhenAbsent() {
	s.Empty(sessionIDFromContext(context.Background()))
	ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{})
	s.Empty(sessionIDFromContext(ctx))
}
