package config_test

import (
	"testing"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/config"
)

func baseEnv(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"EDGE_INTEGRATIONS_JSON":         "[]",
		"EDGE_PAYMENTS_ADDR":             "payments:8000",
		"EDGE_VAULT_ADDRESS":             "vault:8000",
		"EDGE_PUBLIC_BASE_URL":           "https://pay.example.com/",
		"EDGE_FINGERPRINT_KEY":           "real-key",
		"EDGE_CHECKOUT_COOKIE_KEY":       "cookie-key",
		"EDGE_SYNTHETIC_FINGERPRINT_KEY": "",
	}
}

func lookup(env map[string]string) func(string) string {
	return func(name string) string { return env[name] }
}

func TestInvalidConfigurationIsRejected(t *testing.T) {
	tests := map[string]func(map[string]string){
		"missing integrations":  func(e map[string]string) { delete(e, "EDGE_INTEGRATIONS_JSON") },
		"missing payments":      func(e map[string]string) { delete(e, "EDGE_PAYMENTS_ADDR") },
		"missing fingerprint":   func(e map[string]string) { delete(e, "EDGE_FINGERPRINT_KEY") },
		"missing cookie key":    func(e map[string]string) { delete(e, "EDGE_CHECKOUT_COOKIE_KEY") },
		"base url with path":    func(e map[string]string) { e["EDGE_PUBLIC_BASE_URL"] = "https://pay.example.com/x" },
		"base url wrong scheme": func(e map[string]string) { e["EDGE_PUBLIC_BASE_URL"] = "ftp://pay.example.com" },
		"bad address":           func(e map[string]string) { e["EDGE_HTTP_ADDR"] = "8080" },
		"bad timeout":           func(e map[string]string) { e["EDGE_VAULT_TIMEOUT"] = "soon" },
		"zero timeout":          func(e map[string]string) { e["EDGE_PAYMENTS_TIMEOUT"] = "0s" },
		"bad hops":              func(e map[string]string) { e["EDGE_TRUSTED_PROXY_HOPS"] = "0" },
		"bad cookie flag":       func(e map[string]string) { e["EDGE_COOKIE_SECURE"] = "maybe" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			env := baseEnv(t)
			mutate(env)
			if _, err := config.FromLookup(lookup(env)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
