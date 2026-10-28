package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/app"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/config"
)

func main() {
	slog.SetDefault(slog.New(observe.NewLogHandler(os.Stdout, "payments")))
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	cfg, err := config.FromEnv()
	if err != nil {
		slog.Error("invalid payments configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch command {
	case "migrate":
		if err := cfg.ValidateMigrate(); err != nil {
			slog.Error("invalid payments configuration", "error", err)
			os.Exit(1)
		}
		if err := postgres.Migrate(ctx, cfg.MigrationURL()); err != nil {
			slog.Error("migrate payments database", "error", err)
			os.Exit(1)
		}
		slog.Info("payments migrations applied")
	case "serve":
		if err := serve(ctx, cfg); err != nil {
			slog.Error("payments stopped with error", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("unknown command", "command", command)
		os.Exit(2)
	}
}

func serve(ctx context.Context, cfg config.Config) error {
	shutdown, err := observe.Init(ctx, "payments")
	if err != nil {
		return err
	}
	defer shutdown()

	application, err := app.New(ctx, cfg)
	if err != nil {
		return err
	}
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		application.Close()
		return err
	}
	slog.Info("starting payments service", "grpc", cfg.GRPCAddr)
	return application.Serve(ctx, grpcListener)
}
