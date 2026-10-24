package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	return grpctransport.NewServer().Run(ctx, listener, timeout)
}
