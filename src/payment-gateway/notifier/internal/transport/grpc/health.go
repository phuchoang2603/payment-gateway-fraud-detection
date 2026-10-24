package grpc

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	LivenessService  = "liveness"
	ReadinessService = "readiness"
)

type Server struct {
	grpc         *grpc.Server
	health       *health.Server
	ready        func(context.Context) error
	pollInterval time.Duration
	checkTimeout time.Duration
}

func New(ready func(context.Context) error) *Server {
	s := &Server{
		grpc:         grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(observe.RPCFilter)))),
		health:       health.NewServer(),
		ready:        ready,
		pollInterval: 2 * time.Second,
		checkTimeout: 2 * time.Second,
	}
	healthpb.RegisterHealthServer(s.grpc, s.health)
	s.health.SetServingStatus(LivenessService, healthpb.HealthCheckResponse_SERVING)
	s.health.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	return s
}

func (s *Server) Serve(ctx context.Context, listener net.Listener, shutdownTimeout time.Duration) error {
	go s.pollReadiness(ctx)
	errc := make(chan error, 1)
	go func() { errc <- s.grpc.Serve(listener) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	s.health.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	stopped := make(chan struct{})
	go func() {
		s.grpc.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(shutdownTimeout):
		s.grpc.Stop()
	}
	if err := <-errc; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

func (s *Server) pollReadiness(ctx context.Context) {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		checkCtx, cancel := context.WithTimeout(ctx, s.checkTimeout)
		status := healthpb.HealthCheckResponse_SERVING
		if err := s.ready(checkCtx); err != nil {
			status = healthpb.HealthCheckResponse_NOT_SERVING
		}
		cancel()
		if ctx.Err() != nil {
			return
		}
		s.health.SetServingStatus(ReadinessService, status)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
