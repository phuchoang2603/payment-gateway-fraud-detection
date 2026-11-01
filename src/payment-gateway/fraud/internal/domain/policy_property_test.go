package domain

import (
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func policyInput(t *rapid.T) Input {
	attemptedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	input := Input{
		AmountMinor:       rapid.Int64Range(0, 1_000_000).Draw(t, "amount"),
		CustomerCreatedAt: attemptedAt.Add(-time.Duration(rapid.Int64Range(0, 60*24).Draw(t, "age hours")) * time.Hour),
		AttemptedAt:       attemptedAt,
		ShippingCountry:   rapid.SampledFrom([]string{"CA", "US", "VN"}).Draw(t, "shipping"),
	}
	if rapid.Bool().Draw(t, "has ip country") {
		country := rapid.SampledFrom([]string{"CA", "US", "VN"}).Draw(t, "ip country")
		input.IPCountry = &country
	}
	return input
}

func TestPolicyDeclinesExactlyWhenARuleFires(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		input := policyInput(t)
		decision := Evaluate(input)

		if decision.PolicyVersion != PolicyVersion || decision.Declined != (len(decision.Reasons) > 0) {
			t.Fatalf("decision %+v", decision)
		}
		if input.AmountMinor <= 20000 && decision.Declined {
			t.Fatalf("declined %d minor units, below every rule's threshold", input.AmountMinor)
		}
		geo := slices.Contains(decision.Reasons, "NEW_ACCOUNT_GEO_MISMATCH")
		if geo && (input.IPCountry == nil || *input.IPCountry == input.ShippingCountry || input.AttemptedAt.Sub(input.CustomerCreatedAt) >= 7*24*time.Hour) {
			t.Fatalf("geo mismatch fired for %+v", input)
		}
	})
}

func TestRaisingTheAmountNeverTurnsADeclineIntoAnApproval(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		input := policyInput(t)
		raised := input
		raised.AmountMinor += rapid.Int64Range(0, 1_000_000).Draw(t, "increase")

		before, after := Evaluate(input), Evaluate(raised)
		for _, reason := range before.Reasons {
			if !slices.Contains(after.Reasons, reason) {
				t.Fatalf("%s fired at %d but not at %d", reason, input.AmountMinor, raised.AmountMinor)
			}
		}
	})
}
