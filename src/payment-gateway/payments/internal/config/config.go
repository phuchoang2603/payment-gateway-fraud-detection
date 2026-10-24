package config

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	GRPCAddr        string
	ShutdownTimeout time.Duration

	DatabaseURL        string
	MigrateDatabaseURL string

	RedisAddr     string
	RedisPassword string
	RedisTimeout  time.Duration

	FraudAddr     string
	ProcessorAddr string

	EncryptionKey    []byte
	CheckoutTokenKey []byte

	IdempotencyTTL   time.Duration
	PaymentExpiry    time.Duration
	AttemptBudget    time.Duration
	FraudTimeout     time.Duration
	ProcessorTimeout time.Duration
	RecoveryGrace    time.Duration
	WorkerInterval   time.Duration
}

const minCheckoutTokenKeyLength = 32

func FromEnv() (Config, error) {
	return FromLookup(os.Getenv, os.ReadFile)
}

type reader struct {
	lookup   func(string) string
	readFile func(string) ([]byte, error)
	err      error
}

func (r *reader) text(name, fallback string) string {
	if value := r.lookup(name); value != "" {
		return value
	}
	return fallback
}

func (r *reader) duration(name string, fallback time.Duration) time.Duration {
	value := r.lookup(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		r.err = errors.Join(r.err, fmt.Errorf("parse %s: %w", name, err))
	}
	return duration
}

func (r *reader) secret(name string) string {
	if value := r.lookup(name); value != "" {
		return value
	}
	path := r.lookup(name + "_FILE")
	if path == "" {
		return ""
	}
	raw, err := r.readFile(path)
	if err != nil {
		r.err = errors.Join(r.err, fmt.Errorf("read %s_FILE: %w", name, err))
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func FromLookup(lookup func(string) string, readFile func(string) ([]byte, error)) (Config, error) {
	r := &reader{lookup: lookup, readFile: readFile}
	cfg := Config{
		GRPCAddr:           r.text("PAYMENTS_GRPC_ADDR", ":8000"),
		ShutdownTimeout:    r.duration("PAYMENTS_SHUTDOWN_TIMEOUT", 10*time.Second),
		DatabaseURL:        r.text("PAYMENTS_DATABASE_URL", ""),
		MigrateDatabaseURL: r.text("PAYMENTS_MIGRATE_DATABASE_URL", ""),
		RedisAddr:          r.text("PAYMENTS_REDIS_ADDR", ""),
		RedisPassword:      r.secret("PAYMENTS_REDIS_PASSWORD"),
		RedisTimeout:       r.duration("PAYMENTS_REDIS_TIMEOUT", 50*time.Millisecond),
		FraudAddr:          r.text("PAYMENTS_FRAUD_ADDR", ""),
		ProcessorAddr:      r.text("PAYMENTS_PROCESSOR_ADDR", ""),
		IdempotencyTTL:     r.duration("PAYMENTS_IDEMPOTENCY_TTL", 24*time.Hour),
		PaymentExpiry:      r.duration("PAYMENTS_PAYMENT_EXPIRY", 15*time.Minute),
		AttemptBudget:      r.duration("PAYMENTS_ATTEMPT_BUDGET", 5*time.Second),
		FraudTimeout:       r.duration("PAYMENTS_FRAUD_TIMEOUT", time.Second),
		ProcessorTimeout:   r.duration("PAYMENTS_PROCESSOR_TIMEOUT", 3*time.Second),
		RecoveryGrace:      r.duration("PAYMENTS_RECOVERY_GRACE", 10*time.Second),
		WorkerInterval:     r.duration("PAYMENTS_WORKER_INTERVAL", 5*time.Second),
	}
	cfg.CheckoutTokenKey = []byte(r.secret("PAYMENTS_CHECKOUT_TOKEN_KEY"))
	if encoded := r.secret("PAYMENTS_ENCRYPTION_KEY"); encoded != "" {
		key, err := decodeKey(encoded)
		if err != nil {
			r.err = errors.Join(r.err, fmt.Errorf("PAYMENTS_ENCRYPTION_KEY: %w", err))
		}
		cfg.EncryptionKey = key
	}
	if r.err != nil {
		return Config{}, r.err
	}
	return cfg, nil
}

func decodeKey(encoded string) ([]byte, error) {
	if key, err := hex.DecodeString(encoded); err == nil && len(key) == 32 {
		return key, nil
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) != 32 {
		return nil, errors.New("must be 32 bytes encoded as base64 or 64 hex characters")
	}
	return key, nil
}

func (c Config) MigrationURL() string {
	if c.MigrateDatabaseURL != "" {
		return c.MigrateDatabaseURL
	}
	return c.DatabaseURL
}

func (c Config) ValidateMigrate() error {
	if c.MigrationURL() == "" {
		return errors.New("PAYMENTS_MIGRATE_DATABASE_URL or PAYMENTS_DATABASE_URL is required")
	}
	return nil
}

func (c Config) Validate() error {
	var problems []error
	require := func(ok bool, message string) {
		if !ok {
			problems = append(problems, errors.New(message))
		}
	}
	for name, addr := range map[string]string{"PAYMENTS_GRPC_ADDR": c.GRPCAddr} {
		_, port, err := net.SplitHostPort(addr)
		portNumber, convErr := strconv.Atoi(port)
		require(err == nil && convErr == nil && portNumber >= 0 && portNumber <= 65535, name+" must be host:port")
	}
	require(c.DatabaseURL != "", "PAYMENTS_DATABASE_URL is required")
	require(c.FraudAddr != "", "PAYMENTS_FRAUD_ADDR is required")
	require(c.ProcessorAddr != "", "PAYMENTS_PROCESSOR_ADDR is required")
	require(len(c.EncryptionKey) == 32, "PAYMENTS_ENCRYPTION_KEY (or _FILE) is required")
	require(len(c.CheckoutTokenKey) >= minCheckoutTokenKeyLength, "PAYMENTS_CHECKOUT_TOKEN_KEY (or _FILE) must be at least 32 bytes")
	for name, value := range map[string]time.Duration{
		"PAYMENTS_SHUTDOWN_TIMEOUT": c.ShutdownTimeout, "PAYMENTS_REDIS_TIMEOUT": c.RedisTimeout, "PAYMENTS_IDEMPOTENCY_TTL": c.IdempotencyTTL,
		"PAYMENTS_PAYMENT_EXPIRY": c.PaymentExpiry, "PAYMENTS_ATTEMPT_BUDGET": c.AttemptBudget, "PAYMENTS_FRAUD_TIMEOUT": c.FraudTimeout,
		"PAYMENTS_PROCESSOR_TIMEOUT": c.ProcessorTimeout, "PAYMENTS_RECOVERY_GRACE": c.RecoveryGrace, "PAYMENTS_WORKER_INTERVAL": c.WorkerInterval,
	} {
		require(value > 0, name+" must be greater than zero")
	}
	return errors.Join(problems...)
}
