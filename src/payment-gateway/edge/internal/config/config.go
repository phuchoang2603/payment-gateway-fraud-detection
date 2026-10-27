package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddress                 = ":8080"
	defaultShutdownTimeout         = 10 * time.Second
	defaultPaymentsTimeout         = 6 * time.Second
	defaultVaultTimeout            = 2 * time.Second
	defaultTrustedProxyHeader      = "X-Forwarded-For"
	defaultSyntheticFingerprintKey = "synthetic-fingerprint-v1"
)

// Environment:
//
//	EDGE_HTTP_ADDR                    listen address, default :8080
//	EDGE_SHUTDOWN_TIMEOUT             bounded graceful shutdown, default 10s
//	EDGE_INTEGRATIONS_JSON            JSON array of integrations (required)
//	EDGE_PAYMENTS_ADDR                Payments gRPC target (required)
//	EDGE_PAYMENTS_TIMEOUT             deadline for every Payments call, default 6s
//	EDGE_VAULT_ADDRESS / VAULT_ADDRESS vault VaultService gRPC target (required)
//	EDGE_VAULT_TIMEOUT                deadline for every vault call, default 2s
//	EDGE_PUBLIC_BASE_URL              public origin of the hosted page, e.g. https://pay.example.com (required)
//	EDGE_TRUSTED_PROXY_HEADER         header carrying the client address, default X-Forwarded-For
//	EDGE_TRUSTED_PROXY_HOPS           trusted proxies appending to that header, default 1
//	EDGE_FINGERPRINT_KEY              secret key for real integrations (required)
//	EDGE_SYNTHETIC_FINGERPRINT_KEY    non-secret key for synthetic integrations, default synthetic-fingerprint-v1
//	EDGE_CHECKOUT_COOKIE_KEY          secret binding hosted-form CSRF tokens to a session (required)
//	EDGE_COOKIE_SECURE                set false only for plain-HTTP local runs, default true
type Config struct {
	Address                 string
	ShutdownTimeout         time.Duration
	IntegrationsJSON        string
	PaymentsAddr            string
	PaymentsTimeout         time.Duration
	VaultAddr               string
	VaultTimeout            time.Duration
	PublicBaseURL           string
	TrustedProxyHeader      string
	TrustedProxyHops        int
	FingerprintKey          string
	SyntheticFingerprintKey string
	CheckoutCookieKey       string
	CookieSecure            bool
}

func FromEnv() (Config, error) {
	return FromLookup(os.Getenv)
}

func FromLookup(lookup func(string) string) (Config, error) {
	cfg := Config{
		Address:                 defaultAddress,
		ShutdownTimeout:         defaultShutdownTimeout,
		PaymentsTimeout:         defaultPaymentsTimeout,
		VaultTimeout:            defaultVaultTimeout,
		TrustedProxyHeader:      defaultTrustedProxyHeader,
		TrustedProxyHops:        1,
		SyntheticFingerprintKey: defaultSyntheticFingerprintKey,
		CookieSecure:            true,
	}
	setString := func(target *string, name string) {
		if value := lookup(name); value != "" {
			*target = value
		}
	}
	setString(&cfg.Address, "EDGE_HTTP_ADDR")
	setString(&cfg.IntegrationsJSON, "EDGE_INTEGRATIONS_JSON")
	setString(&cfg.PaymentsAddr, "EDGE_PAYMENTS_ADDR")
	setString(&cfg.VaultAddr, "VAULT_ADDRESS")
	setString(&cfg.VaultAddr, "EDGE_VAULT_ADDRESS")
	setString(&cfg.PublicBaseURL, "EDGE_PUBLIC_BASE_URL")
	setString(&cfg.TrustedProxyHeader, "EDGE_TRUSTED_PROXY_HEADER")
	setString(&cfg.SyntheticFingerprintKey, "EDGE_SYNTHETIC_FINGERPRINT_KEY")

	for name, target := range map[string]*time.Duration{
		"EDGE_SHUTDOWN_TIMEOUT": &cfg.ShutdownTimeout,
		"EDGE_PAYMENTS_TIMEOUT": &cfg.PaymentsTimeout,
		"EDGE_VAULT_TIMEOUT":    &cfg.VaultTimeout,
	} {
		if value := lookup(name); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return Config{}, fmt.Errorf("parse %s: %w", name, err)
			}
			*target = duration
		}
	}
	if value := lookup("EDGE_TRUSTED_PROXY_HOPS"); value != "" {
		hops, err := strconv.Atoi(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse EDGE_TRUSTED_PROXY_HOPS: %w", err)
		}
		cfg.TrustedProxyHops = hops
	}
	if value := lookup("EDGE_COOKIE_SECURE"); value != "" {
		secure, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse EDGE_COOKIE_SECURE: %w", err)
		}
		cfg.CookieSecure = secure
	}
	setString(&cfg.FingerprintKey, "EDGE_FINGERPRINT_KEY")
	setString(&cfg.CheckoutCookieKey, "EDGE_CHECKOUT_COOKIE_KEY")
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("EDGE_HTTP_ADDR must not be empty")
	}
	_, port, err := net.SplitHostPort(c.Address)
	if err != nil {
		return fmt.Errorf("EDGE_HTTP_ADDR must be host:port: %w", err)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 0 || portNumber > 65535 {
		return fmt.Errorf("EDGE_HTTP_ADDR port must be between 0 and 65535")
	}
	required := []struct{ name, value string }{
		{"EDGE_INTEGRATIONS_JSON", c.IntegrationsJSON},
		{"EDGE_PAYMENTS_ADDR", c.PaymentsAddr},
		{"EDGE_VAULT_ADDRESS", c.VaultAddr},
		{"EDGE_PUBLIC_BASE_URL", c.PublicBaseURL},
		{"EDGE_TRUSTED_PROXY_HEADER", c.TrustedProxyHeader},
		{"EDGE_FINGERPRINT_KEY", c.FingerprintKey},
		{"EDGE_SYNTHETIC_FINGERPRINT_KEY", c.SyntheticFingerprintKey},
		{"EDGE_CHECKOUT_COOKIE_KEY", c.CheckoutCookieKey},
	}
	for _, item := range required {
		if item.value == "" {
			return fmt.Errorf("%s is required", item.name)
		}
	}
	base, err := url.Parse(c.PublicBaseURL)
	if err != nil || (base.Scheme != "https" && base.Scheme != "http") || base.Host == "" ||
		(base.Path != "" && base.Path != "/") || base.RawQuery != "" || base.User != nil {
		return fmt.Errorf("EDGE_PUBLIC_BASE_URL must be an http(s) origin without a path")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown timeout must be greater than zero")
	}
	if c.PaymentsTimeout <= 0 || c.VaultTimeout <= 0 {
		return fmt.Errorf("outbound call timeouts must be greater than zero")
	}
	if c.TrustedProxyHops < 1 {
		return fmt.Errorf("EDGE_TRUSTED_PROXY_HOPS must be at least 1")
	}
	return nil
}

func (c Config) PublicOrigin() string {
	return strings.TrimSuffix(c.PublicBaseURL, "/")
}
