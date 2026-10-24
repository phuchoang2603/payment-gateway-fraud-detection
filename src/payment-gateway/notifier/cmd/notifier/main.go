package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/kafka"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/webhook"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/metrics"
	transport "github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/transport/grpc"
)

func main() {
	slog.SetDefault(slog.New(observe.NewLogHandler(os.Stdout, "notifier")))
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch command {
	case "serve":
		err = serve(ctx)
	case "migrate":
		err = migrate(ctx)
	default:
		err = fmt.Errorf("unknown command %q (want serve or migrate)", command)
	}
	if err != nil {
		slog.Error("notifier stopped with error", "command", command, "error", err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context) error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	url, err := cfg.MigrationURL()
	if err != nil {
		return err
	}
	if err := postgres.Migrate(ctx, url); err != nil {
		return err
	}
	slog.Info("migrations applied")
	return nil
}

func serve(ctx context.Context) error {
	shutdown, err := observe.Init(ctx, "notifier")
	if err != nil {
		return err
	}
	defer shutdown()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	if err := cfg.ValidateServe(); err != nil {
		return err
	}
	raw, err := os.ReadFile(cfg.IntegrationsFile)
	if err != nil {
		return fmt.Errorf("read integrations file: %w", err)
	}
	integrations, err := domain.ParseIntegrations(raw)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer pool.Close()
	readiness, err := postgres.NewReadiness(pool)
	if err != nil {
		return err
	}
	defer func() { _ = readiness.Close() }()

	store := postgres.NewStore(pool)
	m := metrics.New()
	ingestor := &application.Ingestor{Store: store, Integrations: integrations, Now: time.Now}
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, ingestor, m)
	if err != nil {
		return fmt.Errorf("create kafka consumer: %w", err)
	}
	defer consumer.Close()
	worker := &application.DeliveryWorker{
		Queue:        store,
		Sender:       webhook.NewSender(cfg.DeliveryTimeout),
		Integrations: integrations,
		Schedule:     cfg.Schedule,
		Metrics:      m,
		Now:          time.Now,
		Jitter:       rand.Float64,
		BatchSize:    cfg.BatchSize,
		Concurrency:  cfg.Concurrency,
		Lease:        cfg.LeaseDuration(),
		PollInterval: cfg.PollInterval,
	}

	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen grpc: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errc := make(chan error, 2)
	var wg sync.WaitGroup
	spawn := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	spawn(func() {
		if err := transport.New(readiness.Check).Serve(runCtx, grpcListener, cfg.ShutdownTimeout); err != nil {
			errc <- err
		}
	})
	spawn(func() { consumer.Run(runCtx) })
	spawn(func() { postgres.NewLeader(pool, postgres.LeaderLockKey).Run(runCtx, worker.Run) })

	slog.Info("notifier started", "grpc", cfg.GRPCAddr, "integrations", integrations.Len())
	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errc:
	}
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-shutdownCtx.Done():
	}
	return runErr
}
