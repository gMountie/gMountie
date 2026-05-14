package grpc

import (
	"context"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ServerUnaryRequestID extracts the request id from incoming metadata if
// present, otherwise generates a fresh UUID. The id is stashed on the
// handler's context via NewContextWithRequestID.
func ServerUnaryRequestID() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if vals := md.Get(RequestIDMetadataKey); len(vals) > 0 && vals[0] != "" {
				id = vals[0]
			}
		}
		if id == "" {
			id = uuid.NewString()
		}
		return handler(NewContextWithRequestID(ctx, id), req)
	}
}

// ClientUnaryRequestID injects a request id into outgoing metadata. If
// the context already carries one (RequestIDFromContext), it is reused;
// otherwise a fresh UUID is generated.
func ClientUnaryRequestID() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		id := RequestIDFromContext(ctx)
		if id == "" {
			id = uuid.NewString()
			ctx = NewContextWithRequestID(ctx, id)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, RequestIDMetadataKey, id)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}
