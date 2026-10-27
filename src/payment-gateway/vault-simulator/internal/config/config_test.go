package config

import (
	"testing"
)

func lookupFrom(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func validValues(t *testing.T) map[string]string {
	return map[string]string{
		"VAULT_DATABASE_URL":    "postgres://x",
		"VAULT_ENCRYPTION_KEY":  "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
		"VAULT_FINGERPRINT_KEY": "synthetic-fingerprint-v1",
	}
}

func TestFromLookupRejectsInvalid(t *testing.T) {
	tests := map[string]func(map[string]string){
		"short encryption key": func(v map[string]string) { v["VAULT_ENCRYPTION_KEY"] = "c2hvcnQ=" },
		"missing database":     func(v map[string]string) { delete(v, "VAULT_DATABASE_URL") },
		"missing fingerprint":  func(v map[string]string) { delete(v, "VAULT_FINGERPRINT_KEY") },
		"bad address":          func(v map[string]string) { v["VAULT_GRPC_ADDR"] = "nope" },
		"bad timeout":          func(v map[string]string) { v["VAULT_SHUTDOWN_TIMEOUT"] = "x" },
		"bad processing addr":  func(v map[string]string) { v["VAULT_PROCESSING_ADDR"] = "nope" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			values := validValues(t)
			mutate(values)
			if _, err := FromLookup(lookupFrom(values)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
