package config_test

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/config"
)

func load(t *testing.T, values map[string]string) (config.Config, error) {
	t.Helper()
	return config.FromLookup(func(name string) string { return values[name] }, os.ReadFile)
}

func validEnv() map[string]string {
	return map[string]string{
		"PAYMENTS_DATABASE_URL": "postgres://app@db/payments", "PAYMENTS_FRAUD_ADDR": "fraud:8000", "PAYMENTS_PROCESSOR_ADDR": "processor:8000",
		"PAYMENTS_ENCRYPTION_KEY":     base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")),
		"PAYMENTS_CHECKOUT_TOKEN_KEY": "checkout-token-key-for-tests-0123456789",
	}
}

func TestSecretsAreReadFromFiles(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		path := dir + "/" + name
		if err := os.WriteFile(path, []byte(content+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	env := validEnv()
	delete(env, "PAYMENTS_ENCRYPTION_KEY")
	delete(env, "PAYMENTS_CHECKOUT_TOKEN_KEY")
	env["PAYMENTS_ENCRYPTION_KEY_FILE"] = write("enc", strings.Repeat("ab", 32))
	env["PAYMENTS_CHECKOUT_TOKEN_KEY_FILE"] = write("tok", "checkout-token-key-for-tests-0123456789")
	env["PAYMENTS_REDIS_PASSWORD_FILE"] = write("redis", "s3cret")
	cfg, err := load(t, env)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.EncryptionKey) != 32 || string(cfg.CheckoutTokenKey) != "checkout-token-key-for-tests-0123456789" || cfg.RedisPassword != "s3cret" {
		t.Fatalf("secrets from files: %d key bytes, token %q", len(cfg.EncryptionKey), cfg.CheckoutTokenKey)
	}
}

func TestUnusableSecretsAreRejected(t *testing.T) {
	tests := map[string]func(map[string]string){
		"short encryption key": func(e map[string]string) { e["PAYMENTS_ENCRYPTION_KEY"] = "c2hvcnQ=" },
		"short checkout key":   func(e map[string]string) { e["PAYMENTS_CHECKOUT_TOKEN_KEY"] = "short" },
		"unreadable secret file": func(e map[string]string) {
			delete(e, "PAYMENTS_ENCRYPTION_KEY")
			e["PAYMENTS_ENCRYPTION_KEY_FILE"] = "/nonexistent"
		},
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
