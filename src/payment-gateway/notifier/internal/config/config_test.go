package config_test

import (
	"testing"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/config"
)

func lookupFrom(env map[string]string) config.Lookup {
	return func(key string) string { return env[key] }
}

func TestInvalidValues(t *testing.T) {
	tests := map[string]map[string]string{
		"bad duration":   {"NOTIFIER_DELIVERY_TIMEOUT": "soon"},
		"bad attempts":   {"NOTIFIER_DELIVERY_MAX_ATTEMPTS": "x"},
		"missing broker": {"NOTIFIER_DATABASE_URL": "p", "NOTIFIER_INTEGRATIONS_JSON": "f"},
	}
	for name, env := range tests {
		cfg, err := config.FromLookup(lookupFrom(env))
		if err == nil {
			err = cfg.ValidateServe()
		}
		if err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
