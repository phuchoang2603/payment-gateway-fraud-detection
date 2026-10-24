package config

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultGRPCAddr          = ":8000"
	defaultShutdownTimeout   = 10 * time.Second
	defaultReadinessInterval = 2 * time.Second
	defaultVaultTimeout      = 2 * time.Second
	defaultTimeoutHold       = 30 * time.Second
)

// Environment variables (each secret also accepts <NAME>_FILE with a mounted file path):
//
//	PROCESSOR_GRPC_ADDR, PROCESSOR_SHUTDOWN_TIMEOUT, PROCESSOR_READINESS_INTERVAL
//	PROCESSOR_DATABASE_URL, PROCESSOR_MIGRATE_DATABASE_URL (owner role, falls back to PROCESSOR_DATABASE_URL)
//	PROCESSOR_SEED (uint64), PROCESSOR_LATENCY_MIN, PROCESSOR_LATENCY_MAX (durations),
//	PROCESSOR_TIMEOUT_RATE (0..1), PROCESSOR_TIMEOUT_HOLD (longest an injected timeout withholds the response)
//	VAULT_ADDRESS (host:port of the vault ProcessingService listener), VAULT_TIMEOUT (per resolve call, default 2s)
type Config struct {
	GRPCAddr           string
	ShutdownTimeout    time.Duration
	ReadinessInterval  time.Duration
	DatabaseURL        string
	MigrateDatabaseURL string
	Seed               uint64
	LatencyMin         time.Duration
	LatencyMax         time.Duration
	TimeoutRate        float64
	TimeoutHold        time.Duration
	Vault              Vault
}

type Vault struct {
	Address string
	Timeout time.Duration
}

func FromEnv() (Config, error) {
	return FromLookup(os.Getenv)
}

func FromLookup(lookup func(string) string) (Config, error) {
	cfg := Config{
		GRPCAddr:          defaultGRPCAddr,
		ShutdownTimeout:   defaultShutdownTimeout,
		ReadinessInterval: defaultReadinessInterval,
		TimeoutHold:       defaultTimeoutHold,
		Vault: Vault{
			Address: lookup("VAULT_ADDRESS"),
			Timeout: defaultVaultTimeout,
		},
	}
	if value := lookup("PROCESSOR_GRPC_ADDR"); value != "" {
		cfg.GRPCAddr = value
	}
	durations := []struct {
		name   string
		target *time.Duration
	}{
		{"PROCESSOR_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
		{"PROCESSOR_READINESS_INTERVAL", &cfg.ReadinessInterval},
		{"PROCESSOR_LATENCY_MIN", &cfg.LatencyMin},
		{"PROCESSOR_LATENCY_MAX", &cfg.LatencyMax},
		{"PROCESSOR_TIMEOUT_HOLD", &cfg.TimeoutHold},
		{"VAULT_TIMEOUT", &cfg.Vault.Timeout},
	}
	for _, item := range durations {
		if value := lookup(item.name); value != "" {
			duration, err := time.ParseDuration(value)
			if err != nil {
				return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
			}
			*item.target = duration
		}
	}
	if value := lookup("PROCESSOR_SEED"); value != "" {
		seed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("parse PROCESSOR_SEED: %w", err)
		}
		cfg.Seed = seed
	}
	if value := lookup("PROCESSOR_TIMEOUT_RATE"); value != "" {
		rate, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return Config{}, fmt.Errorf("parse PROCESSOR_TIMEOUT_RATE: %w", err)
		}
		cfg.TimeoutRate = rate
	}
	var err error
	if cfg.DatabaseURL, err = secret(lookup, "PROCESSOR_DATABASE_URL"); err != nil {
		return Config{}, err
	}
	if cfg.MigrateDatabaseURL, err = secret(lookup, "PROCESSOR_MIGRATE_DATABASE_URL"); err != nil {
		return Config{}, err
	}
	if cfg.MigrateDatabaseURL == "" {
		cfg.MigrateDatabaseURL = cfg.DatabaseURL
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if err := validateAddr("PROCESSOR_GRPC_ADDR", c.GRPCAddr); err != nil {
		return err
	}
	for name, value := range map[string]time.Duration{
		"PROCESSOR_SHUTDOWN_TIMEOUT":   c.ShutdownTimeout,
		"PROCESSOR_READINESS_INTERVAL": c.ReadinessInterval,
		"PROCESSOR_TIMEOUT_HOLD":       c.TimeoutHold,
		"VAULT_TIMEOUT":                c.Vault.Timeout,
	} {
		if value <= 0 {
			return fmt.Errorf("%s must be greater than zero", name)
		}
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("PROCESSOR_DATABASE_URL is required")
	}
	if c.LatencyMin < 0 || c.LatencyMax < c.LatencyMin {
		return fmt.Errorf("PROCESSOR_LATENCY_MIN must not be negative and not exceed PROCESSOR_LATENCY_MAX")
	}
	if math.IsNaN(c.TimeoutRate) || c.TimeoutRate < 0 || c.TimeoutRate > 1 {
		return fmt.Errorf("PROCESSOR_TIMEOUT_RATE must be between 0 and 1")
	}
	if err := validateAddr("VAULT_ADDRESS", c.Vault.Address); err != nil {
		return err
	}
	return nil
}

func MigrateURL(lookup func(string) string) (string, error) {
	url, err := secret(lookup, "PROCESSOR_MIGRATE_DATABASE_URL")
	if err != nil {
		return "", err
	}
	if url == "" {
		if url, err = secret(lookup, "PROCESSOR_DATABASE_URL"); err != nil {
			return "", err
		}
	}
	if url == "" {
		return "", fmt.Errorf("PROCESSOR_MIGRATE_DATABASE_URL or PROCESSOR_DATABASE_URL is required")
	}
	return url, nil
}

func secret(lookup func(string) string, name string) (string, error) {
	if path := lookup(name + "_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read %s_FILE: %w", name, err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	return lookup(name), nil
}

func validateAddr(name, addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be host:port: %w", name, err)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return fmt.Errorf("%s port must be between 0 and 65535", name)
	}
	return nil
}
