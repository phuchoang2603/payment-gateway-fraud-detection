package domain

import "slices"

const (
	ModelPolicyVersion = "rules-v1+model"
	ModelReason        = "MODEL_RISK_HIGH"
)

type ModelInput struct {
	Enforce, Available, Complete bool
	Score, Threshold             float64
}

// ApplyModel lets an enforced score add a decline. It never turns a rules decline into an approval,
// and it leaves the rules decision untouched unless the missing-feature policy holds.
func ApplyModel(rules Decision, input ModelInput) (Decision, bool) {
	if !input.Enforce || !input.Available || !input.Complete {
		return rules, false
	}
	result := Decision{Declined: rules.Declined, Reasons: slices.Clone(rules.Reasons), PolicyVersion: ModelPolicyVersion}
	if !rules.Declined && input.Score >= input.Threshold {
		result.Declined = true
		result.Reasons = append(result.Reasons, ModelReason)
	}
	return result, true
}
