package config_test

import (
	"os"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/config"
)

func lookupFrom(env map[string]string) config.Lookup {
	return func(key string) string { return env[key] }
}

func TestDefaults(t *testing.T) {
	cfg, err := config.FromLookup(lookupFrom(map[string]string{
		"NOTIFIER_DATABASE_URL":      "postgres://x",
		"NOTIFIER_INTEGRATIONS_FILE": "/etc/integrations.json",
		"NOTIFIER_KAFKA_BROKERS":     "a:9092, b:9092",
	}), os.ReadFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateServe(); err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddr != ":8000" || cfg.KafkaTopic != "payments.events.v1" || cfg.KafkaGroup != "gateway-notifier" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.Schedule.MaxAttempts != 12 || cfg.DeliveryTimeout != 10*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestSecretFromFileAndMigrateFallback(t *testing.T) {
	path := t.TempDir() + "/url"
	if err := os.WriteFile(path, []byte("postgres://from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.FromLookup(lookupFrom(map[string]string{"NOTIFIER_DATABASE_URL_FILE": path}), os.ReadFile)
	if err != nil {
		t.Fatal(err)
	}
	url, err := cfg.MigrationURL()
	if err != nil || url != "postgres://from-file" {
		t.Fatalf("migration url %q err %v", url, err)
	}
}

func TestInvalidValues(t *testing.T) {
	tests := map[string]map[string]string{
		"bad duration":   {"NOTIFIER_DELIVERY_TIMEOUT": "soon"},
		"bad attempts":   {"NOTIFIER_DELIVERY_MAX_ATTEMPTS": "x"},
		"missing broker": {"NOTIFIER_DATABASE_URL": "p", "NOTIFIER_INTEGRATIONS_FILE": "f"},
	}
	for name, env := range tests {
		cfg, err := config.FromLookup(lookupFrom(env), os.ReadFile)
		if err == nil {
			err = cfg.ValidateServe()
		}
		if err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
