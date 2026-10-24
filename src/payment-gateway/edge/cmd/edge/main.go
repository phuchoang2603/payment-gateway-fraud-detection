package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/grpcclient"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/integrations"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/payments"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/vault"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/config"
	httptransport "github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/transport/http"
)

func main() {
	slog.SetDefault(slog.New(observe.NewLogHandler(os.Stdout, "edge")))
	if err := run(); err != nil {
		slog.Error("edge service stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	shutdown, err := observe.Init(context.Background(), "edge")
	if err != nil {
		return err
	}
	defer shutdown()

	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	registry, err := integrations.Load(cfg.IntegrationsFile)
	if err != nil {
		return err
	}

	paymentsConn, err := grpcclient.Dial(cfg.PaymentsAddr, cfg.PaymentsTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = paymentsConn.Close() }()

	vaultConn, err := grpcclient.Dial(cfg.VaultAddr, cfg.VaultTimeout)
	if err != nil {
		return err
	}
	defer func() { _ = vaultConn.Close() }()

	service := application.New(application.Config{
		Payments:                payments.New(paymentsConn),
		Vault:                   vault.New(vaultConn),
		Integrations:            registry,
		FingerprintKey:          cfg.FingerprintKey,
		SyntheticFingerprintKey: cfg.SyntheticFingerprintKey,
		PublicBaseURL:           cfg.PublicOrigin(),
	})
	edge, err := httptransport.New(httptransport.Options{
		Address:            cfg.Address,
		ShutdownTimeout:    cfg.ShutdownTimeout,
		Service:            service,
		PublicBaseURL:      cfg.PublicOrigin(),
		CookieKey:          []byte(cfg.CheckoutCookieKey),
		CookieSecure:       cfg.CookieSecure,
		TrustedProxyHeader: cfg.TrustedProxyHeader,
		TrustedProxyHops:   cfg.TrustedProxyHops,
	})
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("starting edge service", "addr", cfg.Address)
	return edge.Run(ctx)
}
