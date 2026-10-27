package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/adapters/velocity"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"

	grpctransport "github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/transport/grpc"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fraud stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) > 1 {
		if len(os.Args) != 2 || os.Args[1] != "migrate" {
			return errors.New("usage: fraud [migrate]")
		}
		databaseURL := os.Getenv("FRAUD_MIGRATE_DATABASE_URL")
		if databaseURL == "" {
			return errors.New("FRAUD_MIGRATE_DATABASE_URL is required")
		}
		return postgres.Migrate(ctx, databaseURL)
	}
	shutdown, err := observe.Init(ctx, "fraud-service")
	if err != nil {
		return err
	}
	defer shutdown()
	timeout := 10 * time.Second
	if value := os.Getenv("SHUTDOWN_TIMEOUT"); value != "" {
		timeout, err = time.ParseDuration(value)
		if err != nil || timeout <= 0 || timeout > 60*time.Second {
			return errors.New("SHUTDOWN_TIMEOUT must be a positive duration up to 60s")
		}
	}
	config := application.Config{}
	for _, entry := range []struct {
		name     string
		fallback time.Duration
		target   *time.Duration
	}{
		{"FRAUD_ASSESSMENT_DEADLINE", time.Second, &config.Deadline},
		{"FRAUD_ASSESSMENT_LEASE", 2 * time.Second, &config.Lease},
		{"FRAUD_ASSESSMENT_RETRY_HORIZON", 5 * time.Minute, &config.RetryHorizon},
		{"FRAUD_ASSESSMENT_ERROR_BACKOFF", 250 * time.Millisecond, &config.ErrorBackoff},
		{"FRAUD_FEAST_TIMEOUT", 100 * time.Millisecond, &config.HistoryTimeout},
		{"FRAUD_GUARD_TIMEOUT", 100 * time.Millisecond, &config.GuardTimeout},
	} {
		*entry.target = entry.fallback
		if value := os.Getenv(entry.name); value != "" {
			*entry.target, err = time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("%s must be a duration", entry.name)
			}
		}
	}
	databaseURL := os.Getenv("FRAUD_DATABASE_URL")
	if databaseURL == "" {
		return errors.New("FRAUD_DATABASE_URL is required")
	}
	poolConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return errors.New("invalid FRAUD_DATABASE_URL")
	}
	poolConfig.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.New(pool)
	var guard application.VelocityReader
	if address := os.Getenv("FRAUD_AUTH_REDIS_ADDR"); address != "" {
		password := os.Getenv("FRAUD_AUTH_REDIS_PASSWORD")
		if password == "" {
			return errors.New("FRAUD_AUTH_REDIS_PASSWORD is required")
		}
		short, err := strconv.ParseInt(os.Getenv("FRAUD_VELOCITY_THRESHOLD_10S"), 10, 64)
		if err != nil {
			return errors.New("FRAUD_VELOCITY_THRESHOLD_10S must be a positive integer")
		}
		long, err := strconv.ParseInt(os.Getenv("FRAUD_VELOCITY_THRESHOLD_60S"), 10, 64)
		if err != nil {
			return errors.New("FRAUD_VELOCITY_THRESHOLD_60S must be a positive integer")
		}
		observer, err := velocity.New(velocity.Config{Address: address, Password: password, Epoch: os.Getenv("FRAUD_VELOCITY_EPOCH"), PolicyVersion: "velocity-observe-v1", RetryHorizon: config.RetryHorizon, Timeout: config.GuardTimeout, Threshold10s: short, Threshold60s: long})
		if err != nil {
			return err
		}
		defer func() { _ = observer.Close() }()
		guard = observer
	}
	service, err := application.New(store, nil, guard, config)
	if err != nil {
		return err
	}
	ready, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = service.Ready(ready); err != nil {
		return fmt.Errorf("assessment database is not ready: %w", err)
	}
	address := os.Getenv("GRPC_ADDR")
	if address == "" {
		address = ":8000"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	slog.Info("fraud started", "grpc_addr", address)
	return grpctransport.NewServer(service).Run(ctx, listener, timeout)
}
