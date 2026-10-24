package observe

import (
	"context"
	"crypto/rand"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type requestIDKey struct{}

func RequestID(ctx context.Context) string { id, _ := ctx.Value(requestIDKey{}).(string); return id }

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		if strings.HasPrefix(info.FullMethod, "/grpc.health.v1.Health/") {
			return handler(ctx, req)
		}
		id := ""
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if values := md.Get("x-request-id"); len(values) > 0 {
				id = strings.TrimSpace(strings.Map(func(c rune) rune {
					if c >= 32 && c < 127 {
						return c
					}
					return -1
				}, values[0]))
				if len(id) > 128 {
					id = id[:128]
				}
			}
		}
		if id == "" {
			id = rand.Text()
		}
		ctx = context.WithValue(ctx, requestIDKey{}, id)
		_ = grpc.SetHeader(ctx, metadata.Pairs("x-request-id", id))
		start := time.Now()
		defer func() {
			if recover() != nil {
				resp = nil
				err = status.Error(codes.Internal, "internal error")
			}
			slog.InfoContext(ctx, "rpc request", "request_id", id, "method", info.FullMethod, "code", status.Code(err).String(), "duration_ms", time.Since(start).Milliseconds())
		}()
		return handler(ctx, req)
	}
}

func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		if id := RequestID(ctx); id != "" && len(outgoing.Get("x-request-id")) == 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
		}
		return invoke(ctx, method, req, reply, cc, opts...)
	}
}
