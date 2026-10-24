package config

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

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

func TestFromLookup(t *testing.T) {
	values := validValues(t)
	cfg, err := FromLookup(lookupFrom(values))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddr != ":8000" || cfg.MigrateDatabaseURL != "postgres://x" || cfg.ProcessingAddr != ":8001" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	values["VAULT_ENCRYPTION_KEY_FILE"] = writeFile(t, "c2hvcnQ=\n")
	if _, err := FromLookup(lookupFrom(values)); err == nil {
		t.Fatal("short encryption key accepted")
	}
}

func TestFromLookupRejectsInvalid(t *testing.T) {
	tests := map[string]func(map[string]string){
		"missing database":    func(v map[string]string) { delete(v, "VAULT_DATABASE_URL") },
		"missing fingerprint": func(v map[string]string) { delete(v, "VAULT_FINGERPRINT_KEY") },
		"bad address":         func(v map[string]string) { v["VAULT_GRPC_ADDR"] = "nope" },
		"bad timeout":         func(v map[string]string) { v["VAULT_SHUTDOWN_TIMEOUT"] = "x" },
		"bad processing addr": func(v map[string]string) { v["VAULT_PROCESSING_ADDR"] = "nope" },
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
