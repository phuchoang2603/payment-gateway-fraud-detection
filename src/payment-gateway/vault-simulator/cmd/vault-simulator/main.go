package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/security/secretbox"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/config"
	grpctransport "github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/transport/grpc"
)

func main() {
	slog.SetDefault(slog.New(observe.NewLogHandler(os.Stdout, "vault-simulator")))
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
		slog.Error("vault simulator failed", "command", command, "error", err)
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
	shutdown, err := observe.Init(ctx, "vault-simulator")
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
	sealer, err := secretbox.New(cfg.EncryptionKey)
	if err != nil {
		return err
	}
	service := application.NewService(store, sealer, cfg.FingerprintKey)
	server := grpctransport.NewServer(cfg, service, store)
	vaultListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	processingListener, err := net.Listen("tcp", cfg.ProcessingAddr)
	if err != nil {
		_ = vaultListener.Close()
		return err
	}
	slog.Info("starting vault simulator", "grpc_addr", cfg.GRPCAddr, "processing_addr", cfg.ProcessingAddr)
	return server.Run(ctx, vaultListener, processingListener)
}
