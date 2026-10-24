package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/fraud"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/processor"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/redis"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application/workers"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/metrics"
	transport "github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/transport/grpc"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/security/secretbox"
)

const (
	readinessInterval = 2 * time.Second
	leaderLockID      = 0x7061796d656e7473
)

type App struct {
	config  config.Config
	pool    *pgxpool.Pool
	metrics *metrics.Metrics
	cache   *redis.Cache
	conns   []*grpc.ClientConn
	service *application.Service
	workers *workers.Workers
}

func New(ctx context.Context, cfg config.Config) (*App, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	app := &App{config: cfg, pool: pool, metrics: metrics.New()}

	sealer, err := secretbox.New(cfg.EncryptionKey)
	if err != nil {
		app.Close()
		return nil, err
	}
	var cache application.IdempotencyCache
	if cfg.RedisAddr != "" {
		app.cache = redis.New(redis.Options{
			Addr: cfg.RedisAddr, Password: cfg.RedisPassword, TTL: cfg.IdempotencyTTL, OpTimeout: cfg.RedisTimeout, Observer: app.metrics,
		})
		cache = app.cache
	}
	fraudConn, err := dial(cfg.FraudAddr)
	if err != nil {
		app.Close()
		return nil, fmt.Errorf("create fraud client: %w", err)
	}
	app.conns = append(app.conns, fraudConn)
	processorConn, err := dial(cfg.ProcessorAddr)
	if err != nil {
		app.Close()
		return nil, fmt.Errorf("create processor client: %w", err)
	}
	app.conns = append(app.conns, processorConn)

	app.service = application.NewService(application.Config{
		PaymentExpiry: cfg.PaymentExpiry, AttemptBudget: cfg.AttemptBudget,
		FraudTimeout: cfg.FraudTimeout, ProcessorTimeout: cfg.ProcessorTimeout,
	}, application.Dependencies{
		Store: postgres.NewStore(pool), Cache: cache, Fraud: fraud.NewClient(fraudConn), Processor: processor.NewClient(processorConn),
		Sealer: sealer, Tokens: domain.NewCheckoutTokens(cfg.CheckoutTokenKey), Metrics: app.metrics,
	})
	app.workers = workers.New(workers.Config{RecoveryGrace: cfg.RecoveryGrace, Interval: cfg.WorkerInterval}, app.service,
		postgres.NewLeader(pool, leaderLockID, cfg.WorkerInterval), app.metrics)
	return app, nil
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithFilter(observe.RPCFilter))),
		grpc.WithChainUnaryInterceptor(observe.UnaryClientInterceptor()),
	)
}

func (a *App) Service() *application.Service {
	return a.service
}

func (a *App) Close() {
	for _, conn := range a.conns {
		_ = conn.Close()
	}
	if a.cache != nil {
		_ = a.cache.Close()
	}
	a.pool.Close()
}

func (a *App) Serve(ctx context.Context, grpcListener net.Listener) error {
	defer a.Close()
	healthServer := health.NewServer()
	healthServer.SetServingStatus(transport.LivenessService, healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(transport.ReadinessService, healthpb.HealthCheckResponse_NOT_SERVING)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(observe.RPCFilter))),
		grpc.ChainUnaryInterceptor(transport.CorrelationInterceptor()),
	)
	paymentsv1.RegisterPaymentServiceServer(grpcServer, transport.NewServer(a.service))
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	background, stopBackground := context.WithCancel(context.WithoutCancel(ctx))
	var group sync.WaitGroup
	run := func(work func()) {
		group.Add(1)
		go func() {
			defer group.Done()
			work()
		}()
	}
	run(func() {
		transport.WatchReadiness(background, healthServer, readinessInterval, func(ctx context.Context) error {
			return postgres.Ready(ctx, a.pool)
		})
	})
	run(func() { a.workers.Run(background) })

	errCh := make(chan error, 2)
	go func() { errCh <- grpcServer.Serve(grpcListener) }()

	var serveErr error
	select {
	case serveErr = <-errCh:
	case <-ctx.Done():
	}

	stopBackground()
	healthServer.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
	defer cancel()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		slog.Warn("graceful stop timed out; forcing stop")
		grpcServer.Stop()
		<-stopped
	}
	group.Wait()
	return serveErr
}
