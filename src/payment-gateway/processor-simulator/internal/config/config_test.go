package config

import (
	"testing"
	"time"
)

func lookupFrom(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func validValues(t *testing.T) map[string]string {
	return map[string]string{
		"PROCESSOR_DATABASE_URL": "postgres://x",
		"VAULT_ADDRESS":          "vault:8000",
	}
}

func TestFromLookupDefaultsAndFaults(t *testing.T) {
	values := validValues(t)
	values["PROCESSOR_SEED"] = "42"
	values["PROCESSOR_LATENCY_MIN"] = "20ms"
	values["PROCESSOR_LATENCY_MAX"] = "80ms"
	values["PROCESSOR_TIMEOUT_RATE"] = "0.05"
	cfg, err := FromLookup(lookupFrom(values))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GRPCAddr != ":8000" || cfg.MigrateDatabaseURL != "postgres://x" {
		t.Fatalf("unexpected config %+v", cfg)
	}
	if cfg.Seed != 42 || cfg.LatencyMin != 20*time.Millisecond || cfg.LatencyMax != 80*time.Millisecond || cfg.TimeoutRate != 0.05 {
		t.Fatalf("unexpected faults %+v", cfg)
	}
}

func TestFromLookupRejectsInvalid(t *testing.T) {
	tests := map[string]func(map[string]string){
		"missing database":  func(v map[string]string) { delete(v, "PROCESSOR_DATABASE_URL") },
		"missing vault":     func(v map[string]string) { delete(v, "VAULT_ADDRESS") },
		"rate above one":    func(v map[string]string) { v["PROCESSOR_TIMEOUT_RATE"] = "1.5" },
		"rate not a number": func(v map[string]string) { v["PROCESSOR_TIMEOUT_RATE"] = "NaN" },
		"latency inverted": func(v map[string]string) {
			v["PROCESSOR_LATENCY_MIN"] = "2s"
			v["PROCESSOR_LATENCY_MAX"] = "1s"
		},
		"bad seed": func(v map[string]string) { v["PROCESSOR_SEED"] = "-1" },
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
