package grpcclient

import (
	"context"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/requestid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const requestIDMetadata = "x-request-id"

// Interceptor bounds every outbound call by timeout and propagates the request ID.
func Interceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if id := requestid.From(ctx); id != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, requestIDMetadata, id)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func Dial(target string, timeout time.Duration, extra ...grpc.DialOption) (*grpc.ClientConn, error) {
	options := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithFilter(observe.RPCFilter))),
		grpc.WithChainUnaryInterceptor(Interceptor(timeout)),
	}, extra...)
	return grpc.NewClient(target, options...)
}
