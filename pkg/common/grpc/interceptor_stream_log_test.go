package grpc

import (
	"context"
	"testing"

	"go.gmountie.dev/gmountie/pkg/common"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// fakeServerStream is a minimal grpc.ServerStream whose RecvMsg hands out
// pre-baked messages in order.
type fakeServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	msgs []any
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func (f *fakeServerStream) RecvMsg(msg any) error {
	if len(f.msgs) == 0 {
		return context.Canceled
	}
	// Tests pass *carrierMsg both as the baked message and the target.
	*(msg.(*carrierMsg)) = *(f.msgs[0].(*carrierMsg))
	f.msgs = f.msgs[1:]
	return nil
}

// carrierMsg implements SessionIDCarrier + VolumeCarrier.
type carrierMsg struct {
	sessionID string
	volume    string
}

func (m *carrierMsg) GetSessionId() string { return m.sessionID }
func (m *carrierMsg) GetVolume() string    { return m.volume }

type StreamLogInterceptorSuite struct{ suite.Suite }

func TestStreamLogInterceptorSuite(t *testing.T) { suite.Run(t, new(StreamLogInterceptorSuite)) }

func (s *StreamLogInterceptorSuite) TestStreamRequestIDFromMetadata() {
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(RequestIDMetadataKey, "req-42"))
	stream := &fakeServerStream{ctx: ctx}

	var gotID string
	var gotFields logging.Fields
	err := ServerStreamRequestID()(nil, stream, &grpc.StreamServerInfo{FullMethod: "/x/Y"},
		func(_ any, ss grpc.ServerStream) error {
			gotID = RequestIDFromContext(ss.Context())
			gotFields = logging.ExtractFields(ss.Context())
			return nil
		})
	s.Require().NoError(err)
	s.Equal("req-42", gotID)
	s.Contains(gotFields, "request_id")
}

func (s *StreamLogInterceptorSuite) TestStreamRequestIDGeneratedWhenAbsent() {
	stream := &fakeServerStream{ctx: context.Background()}
	var gotID string
	err := ServerStreamRequestID()(nil, stream, &grpc.StreamServerInfo{FullMethod: "/x/Y"},
		func(_ any, ss grpc.ServerStream) error {
			gotID = RequestIDFromContext(ss.Context())
			return nil
		})
	s.Require().NoError(err)
	s.NotEmpty(gotID, "a missing metadata request id must be generated")
}

func (s *StreamLogInterceptorSuite) TestStreamLogContextCapturesFirstMessage() {
	raw := "super-secret-session-token"
	stream := &fakeServerStream{
		ctx:  context.Background(),
		msgs: []any{&carrierMsg{sessionID: raw, volume: "photos"}},
	}

	var fields logging.Fields
	err := ServerStreamLogContext()(nil, stream, &grpc.StreamServerInfo{FullMethod: "/x/Y"},
		func(_ any, ss grpc.ServerStream) error {
			// Before any message: holder present but empty.
			s.Empty(StreamLogFields(ss.Context()))
			var m carrierMsg
			if err := ss.RecvMsg(&m); err != nil {
				return err
			}
			// After the first message the SAME ctx (captured at stream start,
			// as the logging interceptor does) must yield the fields.
			fields = StreamLogFields(ss.Context())
			return nil
		})
	s.Require().NoError(err)
	s.Require().Len(fields, 4) // session_fp + volume key/value pairs
	asMap := map[string]any{}
	for i := 0; i+1 < len(fields); i += 2 {
		asMap[fields[i].(string)] = fields[i+1]
	}
	s.Equal(common.FingerprintID(raw), asMap["session_fp"], "session id must be fingerprinted")
	s.NotContains(asMap["session_fp"], raw)
	s.Equal("photos", asMap["volume"])
}

func (s *StreamLogInterceptorSuite) TestStreamLogFieldsNilWithoutHolder() {
	s.Nil(StreamLogFields(context.Background()), "unary ctx (no holder) must yield no fields")
}
