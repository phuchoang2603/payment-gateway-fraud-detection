package grpctransport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/domain"
)

const (
	LivenessService  = "liveness"
	ReadinessService = "readiness"
	readinessPeriod  = 2 * time.Second
)

type Vault interface {
	Tokenize(ctx context.Context, integrationID uuid.UUID, cardNumber string) (string, application.Method, error)
	Describe(ctx context.Context, integrationID uuid.UUID, token string) (application.Method, error)
	Resolve(ctx context.Context, integrationID uuid.UUID, token string) (application.Fixture, error)
}

type Readiness interface {
	Ready(ctx context.Context) error
}

type Server struct {
	vaultv1.UnimplementedVaultServiceServer
	vault           Vault
	readiness       Readiness
	shutdownTimeout time.Duration
	health          *health.Server
}

type processingServer struct {
	vaultv1.UnimplementedProcessingServiceServer
	server *Server
}

func NewServer(cfg config.Config, vault Vault, readiness Readiness) *Server {
	server := &Server{
		vault:           vault,
		readiness:       readiness,
		shutdownTimeout: cfg.ShutdownTimeout,
		health:          health.NewServer(),
	}
	server.health.SetServingStatus(LivenessService, healthpb.HealthCheckResponse_SERVING)
	server.health.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	return server
}

func (s *Server) Run(ctx context.Context, vaultListener, processingListener net.Listener) error {
	newGRPC := func() *grpc.Server {
		return grpc.NewServer(
			grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(observe.RPCFilter))),
			grpc.ChainUnaryInterceptor(observe.UnaryServerInterceptor()),
		)
	}
	vaultServer := newGRPC()
	vaultv1.RegisterVaultServiceServer(vaultServer, s)
	healthpb.RegisterHealthServer(vaultServer, s.health)
	processingGRPC := newGRPC()
	vaultv1.RegisterProcessingServiceServer(processingGRPC, processingServer{server: s})

	errs := make(chan error, 3)
	go func() { errs <- vaultServer.Serve(vaultListener) }()
	go func() { errs <- processingGRPC.Serve(processingListener) }()
	healthCtx, stopHealth := context.WithCancel(ctx)
	defer stopHealth()
	go s.pollReadiness(healthCtx)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errs:
	}
	s.health.SetServingStatus(ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)
	stopHealth()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		var group sync.WaitGroup
		for _, server := range []*grpc.Server{vaultServer, processingGRPC} {
			group.Add(1)
			go func() {
				defer group.Done()
				server.GracefulStop()
			}()
		}
		group.Wait()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		vaultServer.Stop()
		processingGRPC.Stop()
	}
	return runErr
}

func (s *Server) pollReadiness(ctx context.Context) {
	check := func() {
		checkCtx, cancel := context.WithTimeout(ctx, readinessPeriod)
		defer cancel()
		state := healthpb.HealthCheckResponse_SERVING
		if err := s.readiness.Ready(checkCtx); err != nil {
			state = healthpb.HealthCheckResponse_NOT_SERVING
		}
		if ctx.Err() == nil {
			s.health.SetServingStatus(ReadinessService, state)
		}
	}
	check()
	ticker := time.NewTicker(readinessPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
}

func (s *Server) TokenizeCard(ctx context.Context, req *vaultv1.TokenizeCardRequest) (*vaultv1.TokenizeCardResponse, error) {
	integrationID, err := parseIntegration(req.GetIntegrationId())
	if err != nil {
		return nil, err
	}
	token, method, err := s.vault.Tokenize(ctx, integrationID, req.GetCardNumber())
	if err != nil {
		return nil, mapError(err)
	}
	return &vaultv1.TokenizeCardResponse{Token: token, Metadata: methodMetadata(method)}, nil
}

func (s *Server) DescribeMethod(ctx context.Context, req *vaultv1.DescribeMethodRequest) (*vaultv1.MethodMetadata, error) {
	integrationID, err := parseIntegration(req.GetIntegrationId())
	if err != nil {
		return nil, err
	}
	method, err := s.vault.Describe(ctx, integrationID, req.GetToken())
	if err != nil {
		return nil, mapError(err)
	}
	return methodMetadata(method), nil
}

func (p processingServer) ResolveForProcessing(ctx context.Context, req *vaultv1.ResolveRequest) (*vaultv1.ProcessingFixture, error) {
	integrationID, err := parseIntegration(req.GetIntegrationId())
	if err != nil {
		return nil, err
	}
	fixture, err := p.server.vault.Resolve(ctx, integrationID, req.GetToken())
	if err != nil {
		return nil, mapError(err)
	}
	return &vaultv1.ProcessingFixture{CardReference: fixture.CardReference, Behavior: protoBehavior(fixture.Behavior)}, nil
}

func parseIntegration(value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, status.Error(codes.InvalidArgument, "integration_id must be a UUID")
	}
	return id, nil
}

func mapError(err error) error {
	switch {
	case errors.Is(err, application.ErrNotAllowListed):
		return status.Error(codes.InvalidArgument, application.ErrNotAllowListed.Error())
	case errors.Is(err, application.ErrTokenNotFound):
		return status.Error(codes.NotFound, "token not found")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "canceled")
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

func methodMetadata(method application.Method) *vaultv1.MethodMetadata {
	return &vaultv1.MethodMetadata{Type: method.Type, Fingerprint: method.Fingerprint}
}

func protoBehavior(behavior domain.Behavior) vaultv1.Behavior {
	switch behavior {
	case domain.BehaviorOK:
		return vaultv1.Behavior_BEHAVIOR_APPROVE
	case domain.BehaviorNSF:
		return vaultv1.Behavior_BEHAVIOR_INSUFFICIENT_FUNDS
	case domain.BehaviorIssuer:
		return vaultv1.Behavior_BEHAVIOR_ISSUER_DECLINE
	case domain.BehaviorInvalid:
		return vaultv1.Behavior_BEHAVIOR_INVALID
	default:
		return vaultv1.Behavior_BEHAVIOR_UNSPECIFIED
	}
}
