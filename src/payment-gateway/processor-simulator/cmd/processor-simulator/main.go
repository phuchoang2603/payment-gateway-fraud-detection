package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/adapters/vault"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/domain"
	grpctransport "github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/transport/grpc"
)

func main() {
	slog.SetDefault(slog.New(observe.NewLogHandler(os.Stdout, "processor-simulator")))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	var err error
	switch command {
	case "serve":
		err = serve(ctx)
	case "migrate":
		err = migrate(ctx)
	default:
		slog.Error("unknown command", "command", command)
		os.Exit(2)
	}
	if err != nil {
		slog.Error("processor simulator failed", "command", command, "error", err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context) error {
	url, err := config.MigrateURL(os.Getenv)
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
	shutdown, err := observe.Init(ctx, "processor-simulator")
	if err != nil {
		return err
	}
	defer shutdown()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	store, err := postgres.NewStore(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer store.Close()
	vaultClient, err := vault.NewClient(cfg.Vault)
	if err != nil {
		return err
	}
	defer func() { _ = vaultClient.Close() }()

	service := application.NewService(store, vaultClient, domain.Faults{
		Seed:        cfg.Seed,
		LatencyMin:  cfg.LatencyMin,
		LatencyMax:  cfg.LatencyMax,
		TimeoutRate: cfg.TimeoutRate,
	}, cfg.TimeoutHold)
	server := grpctransport.NewServer(grpctransport.Options{
		ShutdownTimeout:   cfg.ShutdownTimeout,
		ReadinessInterval: cfg.ReadinessInterval,
	}, service, store)
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	slog.Info("starting processor simulator", "grpc_addr", cfg.GRPCAddr)
	return server.Run(ctx, grpcListener)
}
