package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
)

// Environment variables (all prefixed NOTIFIER_):
//
//	DATABASE_URL / DATABASE_URL_FILE                runtime database URL (serve)
//	MIGRATE_DATABASE_URL / _FILE                    owner database URL (migrate); falls back to DATABASE_URL
//	GRPC_ADDR (:8000), SHUTDOWN_TIMEOUT (10s)
//	INTEGRATIONS_FILE                               path of the mounted integrations JSON
//	KAFKA_BROKERS                                   comma-separated bootstrap servers
//	KAFKA_TOPIC (payments.events.v1), KAFKA_GROUP (gateway-notifier)
//	DELIVERY_TIMEOUT (10s), DELIVERY_INITIAL_DELAY (2m), DELIVERY_BACKOFF_FACTOR (2),
//	DELIVERY_MAX_DELAY (6h), DELIVERY_MAX_ATTEMPTS (12), DELIVERY_POLL_INTERVAL (1s),
//	DELIVERY_BATCH_SIZE (20), DELIVERY_CONCURRENCY (4)
const (
	defaultGRPCAddr        = ":8000"
	defaultShutdownTimeout = 10 * time.Second
	defaultTopic           = "payments.events.v1"
	defaultGroup           = "gateway-notifier"
)

type Config struct {
	DatabaseURL        string
	MigrateDatabaseURL string
	GRPCAddr           string
	ShutdownTimeout    time.Duration
	IntegrationsFile   string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroup         string
	DeliveryTimeout    time.Duration
	Schedule           domain.Schedule
	PollInterval       time.Duration
	BatchSize          int
	Concurrency        int
}

type Lookup func(string) string

func FromEnv() (Config, error) {
	return FromLookup(os.Getenv, os.ReadFile)
}

func FromLookup(lookup Lookup, readFile func(string) ([]byte, error)) (Config, error) {
	cfg := Config{
		GRPCAddr:        orDefault(lookup("NOTIFIER_GRPC_ADDR"), defaultGRPCAddr),
		ShutdownTimeout: defaultShutdownTimeout,
		KafkaTopic:      orDefault(lookup("NOTIFIER_KAFKA_TOPIC"), defaultTopic),
		KafkaGroup:      orDefault(lookup("NOTIFIER_KAFKA_GROUP"), defaultGroup),
		DeliveryTimeout: 10 * time.Second,
		Schedule: domain.Schedule{
			Initial:     2 * time.Minute,
			Factor:      2,
			Cap:         6 * time.Hour,
			MaxAttempts: 12,
		},
		PollInterval:     time.Second,
		BatchSize:        20,
		Concurrency:      4,
		IntegrationsFile: lookup("NOTIFIER_INTEGRATIONS_FILE"),
	}
	for _, broker := range strings.Split(lookup("NOTIFIER_KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}

	var err error
	if cfg.DatabaseURL, err = secret(lookup, readFile, "NOTIFIER_DATABASE_URL"); err != nil {
		return Config{}, err
	}
	if cfg.MigrateDatabaseURL, err = secret(lookup, readFile, "NOTIFIER_MIGRATE_DATABASE_URL"); err != nil {
		return Config{}, err
	}

	durations := []struct {
		name string
		dst  *time.Duration
	}{
		{"NOTIFIER_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
		{"NOTIFIER_DELIVERY_TIMEOUT", &cfg.DeliveryTimeout},
		{"NOTIFIER_DELIVERY_INITIAL_DELAY", &cfg.Schedule.Initial},
		{"NOTIFIER_DELIVERY_MAX_DELAY", &cfg.Schedule.Cap},
		{"NOTIFIER_DELIVERY_POLL_INTERVAL", &cfg.PollInterval},
	}
	for _, item := range durations {
		if value := lookup(item.name); value != "" {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
			}
			*item.dst = parsed
		}
	}
	ints := []struct {
		name string
		dst  *int
	}{
		{"NOTIFIER_DELIVERY_MAX_ATTEMPTS", &cfg.Schedule.MaxAttempts},
		{"NOTIFIER_DELIVERY_BATCH_SIZE", &cfg.BatchSize},
		{"NOTIFIER_DELIVERY_CONCURRENCY", &cfg.Concurrency},
	}
	for _, item := range ints {
		if value := lookup(item.name); value != "" {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
			}
			*item.dst = parsed
		}
	}
	if value := lookup("NOTIFIER_DELIVERY_BACKOFF_FACTOR"); value != "" {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return Config{}, fmt.Errorf("parse NOTIFIER_DELIVERY_BACKOFF_FACTOR: %w", err)
		}
		cfg.Schedule.Factor = parsed
	}
	return cfg, nil
}

func (c Config) ValidateServe() error {
	if c.DatabaseURL == "" {
		return errors.New("NOTIFIER_DATABASE_URL must be set")
	}
	if c.IntegrationsFile == "" {
		return errors.New("NOTIFIER_INTEGRATIONS_FILE must be set")
	}
	if len(c.KafkaBrokers) == 0 {
		return errors.New("NOTIFIER_KAFKA_BROKERS must be set")
	}
	if c.KafkaTopic == "" || c.KafkaGroup == "" {
		return errors.New("NOTIFIER_KAFKA_TOPIC and NOTIFIER_KAFKA_GROUP must not be empty")
	}
	for name, addr := range map[string]string{"NOTIFIER_GRPC_ADDR": c.GRPCAddr} {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("%s must be host:port: %w", name, err)
		}
	}
	if c.ShutdownTimeout <= 0 || c.DeliveryTimeout <= 0 || c.PollInterval <= 0 {
		return errors.New("shutdown, delivery timeout and poll interval must be greater than zero")
	}
	if c.BatchSize < 1 || c.Concurrency < 1 {
		return errors.New("delivery batch size and concurrency must be at least 1")
	}
	if err := c.Schedule.Validate(); err != nil {
		return fmt.Errorf("delivery schedule: %w", err)
	}
	return nil
}

func (c Config) MigrationURL() (string, error) {
	if c.MigrateDatabaseURL != "" {
		return c.MigrateDatabaseURL, nil
	}
	if c.DatabaseURL != "" {
		return c.DatabaseURL, nil
	}
	return "", errors.New("NOTIFIER_MIGRATE_DATABASE_URL or NOTIFIER_DATABASE_URL must be set")
}

func (c Config) LeaseDuration() time.Duration {
	return c.DeliveryTimeout + 30*time.Second
}

func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func secret(lookup Lookup, readFile func(string) ([]byte, error), name string) (string, error) {
	if path := lookup(name + "_FILE"); path != "" {
		data, err := readFile(path)
		if err != nil {
			return "", fmt.Errorf("read %s_FILE: %w", name, err)
		}
		return strings.TrimSpace(string(data)), nil
	}
	return lookup(name), nil
}
