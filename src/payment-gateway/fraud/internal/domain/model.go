package domain

import "slices"

const (
	// ModelPolicyVersion is the policy version of a decision the model score was applied to.
	ModelPolicyVersion = "rules-v1+model"
	// ModelReason is the reason code of a decline the model added.
	ModelReason = "MODEL_RISK_HIGH"
)

// ModelInput is one model score and the conditions under which it may be used: Available says the
// call succeeded, Complete says every feature view was available.
type ModelInput struct {
	Available, Complete bool
	Score, Threshold    float64
}

// ApplyModel lets an enforced score add a decline, and reports whether the policy let it apply. It never turns a rules decline into an approval,
// and it leaves the rules decision untouched unless the missing-feature policy holds.
func ApplyModel(rules Decision, input ModelInput) (Decision, bool) {
	if !input.Available || !input.Complete {
		return rules, false
	}
	result := Decision{Declined: rules.Declined, Reasons: slices.Clone(rules.Reasons), PolicyVersion: ModelPolicyVersion}
	if !rules.Declined && input.Score >= input.Threshold {
		result.Declined = true
		result.Reasons = append(result.Reasons, ModelReason)
	}
	return result, true
}
