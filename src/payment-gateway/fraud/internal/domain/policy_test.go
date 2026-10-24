package domain

import (
	"reflect"
	"testing"
	"time"
)

func TestPolicyBoundaries(t *testing.T) {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	country := "CA"
	for _, tc := range []struct {
		name    string
		amount  int64
		age     time.Duration
		ip      *string
		reasons []string
	}{
		{"high amount boundary", 200000, 30 * 24 * time.Hour, nil, nil},
		{"high amount", 200001, 30 * 24 * time.Hour, nil, []string{"HIGH_AMOUNT"}},
		{"geo amount boundary", 20000, 24 * time.Hour, &country, nil},
		{"geo match", 20001, 24 * time.Hour, &country, []string{"NEW_ACCOUNT_GEO_MISMATCH"}},
		{"geo age boundary", 20001, 7 * 24 * time.Hour, &country, nil},
		{"unknown IP", 20001, 0, nil, nil},
		{"all reasons", 200001, 0, &country, []string{"HIGH_AMOUNT", "NEW_ACCOUNT_GEO_MISMATCH"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := Input{AmountMinor: tc.amount, CustomerCreatedAt: now.Add(-tc.age), AttemptedAt: now, ShippingCountry: "US", IPCountry: tc.ip}
			got := Evaluate(input)
			if !reflect.DeepEqual(got.Reasons, tc.reasons) || got.Declined != (len(tc.reasons) > 0) || got.PolicyVersion != "rules-v1" {
				t.Fatalf("decision = %+v", got)
			}
			if !reflect.DeepEqual(got, Evaluate(input)) {
				t.Fatal("policy is not deterministic")
			}
		})
	}
}
