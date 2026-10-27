package config_test

import (
	"encoding/base64"
	"testing"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/config"
)

func load(t *testing.T, values map[string]string) (config.Config, error) {
	t.Helper()
	return config.FromLookup(func(name string) string { return values[name] })
}

func validEnv() map[string]string {
	return map[string]string{
		"PAYMENTS_DATABASE_URL": "postgres://app@db/payments", "PAYMENTS_FRAUD_ADDR": "fraud:8000", "PAYMENTS_PROCESSOR_ADDR": "processor:8000",
		"PAYMENTS_ENCRYPTION_KEY":     base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"PAYMENTS_CHECKOUT_TOKEN_KEY": "checkout-token-key-for-tests-0123456789",
	}
}

func TestUnusableSecretsAreRejected(t *testing.T) {
	tests := map[string]func(map[string]string){
		"short encryption key": func(e map[string]string) { e["PAYMENTS_ENCRYPTION_KEY"] = "c2hvcnQ=" },
		"short checkout key":   func(e map[string]string) { e["PAYMENTS_CHECKOUT_TOKEN_KEY"] = "short" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			env := validEnv()
			mutate(env)
			cfg, err := load(t, env)
			if err == nil {
				err = cfg.Validate()
			}
			if err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
