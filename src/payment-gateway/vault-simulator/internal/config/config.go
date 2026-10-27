package config

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	defaultGRPCAddr        = ":8000"
	defaultProcessingAddr  = ":8001"
	defaultShutdownTimeout = 10 * time.Second
)

// Environment variables:
//
//	VAULT_GRPC_ADDR (VaultService), VAULT_PROCESSING_ADDR (ProcessingService), VAULT_SHUTDOWN_TIMEOUT
//	VAULT_DATABASE_URL, VAULT_MIGRATE_DATABASE_URL (owner role, falls back to VAULT_DATABASE_URL)
//	VAULT_ENCRYPTION_KEY: base64 of 32 bytes (AES-256-GCM)
//	VAULT_FINGERPRINT_KEY: synthetic HMAC key shared with contracts/synthetic/identity-vectors.json
//
// Both gRPC listeners are plaintext h2c; the mesh authorizes callers per port.
type Config struct {
	GRPCAddr           string
	ProcessingAddr     string
	ShutdownTimeout    time.Duration
	DatabaseURL        string
	MigrateDatabaseURL string
	EncryptionKey      []byte
	FingerprintKey     []byte
}

func FromEnv() (Config, error) {
	return FromLookup(os.Getenv)
}

func FromLookup(lookup func(string) string) (Config, error) {
	cfg := Config{
		GRPCAddr:        defaultGRPCAddr,
		ProcessingAddr:  defaultProcessingAddr,
		ShutdownTimeout: defaultShutdownTimeout,
	}
	if value := lookup("VAULT_PROCESSING_ADDR"); value != "" {
		cfg.ProcessingAddr = value
	}
	if value := lookup("VAULT_GRPC_ADDR"); value != "" {
		cfg.GRPCAddr = value
	}
	if value := lookup("VAULT_SHUTDOWN_TIMEOUT"); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse VAULT_SHUTDOWN_TIMEOUT: %w", err)
		}
		cfg.ShutdownTimeout = duration
	}
	cfg.DatabaseURL = lookup("VAULT_DATABASE_URL")
	cfg.MigrateDatabaseURL = lookup("VAULT_MIGRATE_DATABASE_URL")
	if cfg.MigrateDatabaseURL == "" {
		cfg.MigrateDatabaseURL = cfg.DatabaseURL
	}
	encoded := lookup("VAULT_ENCRYPTION_KEY")
	if encoded != "" {
		key, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return Config{}, fmt.Errorf("VAULT_ENCRYPTION_KEY must be base64: %w", err)
		}
		cfg.EncryptionKey = key
	}
	cfg.FingerprintKey = []byte(lookup("VAULT_FINGERPRINT_KEY"))
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) Validate() error {
	if err := validateAddr("VAULT_GRPC_ADDR", c.GRPCAddr); err != nil {
		return err
	}
	if err := validateAddr("VAULT_PROCESSING_ADDR", c.ProcessingAddr); err != nil {
		return err
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("VAULT_SHUTDOWN_TIMEOUT must be greater than zero")
	}
	if c.DatabaseURL == "" {
		return fmt.Errorf("VAULT_DATABASE_URL is required")
	}
	if len(c.EncryptionKey) != 32 {
		return fmt.Errorf("VAULT_ENCRYPTION_KEY must be base64 of exactly 32 bytes")
	}
	if len(c.FingerprintKey) == 0 {
		return fmt.Errorf("VAULT_FINGERPRINT_KEY is required")
	}
	return nil
}

func MigrateURL(lookup func(string) string) (string, error) {
	url := lookup("VAULT_MIGRATE_DATABASE_URL")
	if url == "" {
		url = lookup("VAULT_DATABASE_URL")
	}
	if url == "" {
		return "", fmt.Errorf("VAULT_MIGRATE_DATABASE_URL or VAULT_DATABASE_URL is required")
	}
	return url, nil
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
