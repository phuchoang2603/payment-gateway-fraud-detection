package domain

import "time"

const PolicyVersion = "rules-v1"

type Input struct {
	AmountMinor       int64
	CustomerCreatedAt time.Time
	AttemptedAt       time.Time
	ShippingCountry   string
	IPCountry         *string
}

type Decision struct {
	Declined      bool
	Reasons       []string
	PolicyVersion string
}

func Evaluate(input Input) Decision {
	result := Decision{PolicyVersion: PolicyVersion}
	if input.AmountMinor > 200000 {
		result.Reasons = append(result.Reasons, "HIGH_AMOUNT")
	}
	if input.IPCountry != nil && *input.IPCountry != input.ShippingCountry && input.AttemptedAt.Sub(input.CustomerCreatedAt) < 7*24*time.Hour && input.AmountMinor > 20000 {
		result.Reasons = append(result.Reasons, "NEW_ACCOUNT_GEO_MISMATCH")
	}
	result.Declined = len(result.Reasons) != 0
	return result
}
