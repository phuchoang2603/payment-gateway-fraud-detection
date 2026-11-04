package domain

import (
	"slices"
	"testing"
)

func TestApplyModelBoundaries(t *testing.T) {
	approved := Decision{PolicyVersion: PolicyVersion}
	declined := Decision{Declined: true, Reasons: []string{"HIGH_AMOUNT"}, PolicyVersion: PolicyVersion}
	eligible := ModelInput{Available: true, Complete: true, Threshold: 0.8}
	tests := []struct {
		name         string
		rules        Decision
		change       func(*ModelInput)
		wantApplied  bool
		wantDeclined bool
		wantReasons  []string
		wantPolicy   string
	}{
		{"score at the threshold declines", approved, func(i *ModelInput) { i.Score = 0.8 }, true, true, []string{ModelReason}, ModelPolicyVersion},
		{"score just below approves", approved, func(i *ModelInput) { i.Score = 0.7999999 }, true, false, nil, ModelPolicyVersion},
		{"score above declines", approved, func(i *ModelInput) { i.Score = 1 }, true, true, []string{ModelReason}, ModelPolicyVersion},
		{"rules decline stays with a low score", declined, func(i *ModelInput) { i.Score = 0 }, true, true, []string{"HIGH_AMOUNT"}, ModelPolicyVersion},
		{"rules decline keeps its reasons with a high score", declined, func(i *ModelInput) { i.Score = 1 }, true, true, []string{"HIGH_AMOUNT"}, ModelPolicyVersion},
		{"an unavailable model changes nothing", approved, func(i *ModelInput) { i.Score, i.Available = 1, false }, false, false, nil, PolicyVersion},
		{"incomplete features change nothing", approved, func(i *ModelInput) { i.Score, i.Complete = 1, false }, false, false, nil, PolicyVersion},
		{"incomplete features keep a rules decline", declined, func(i *ModelInput) { i.Score, i.Complete = 1, false }, false, true, []string{"HIGH_AMOUNT"}, PolicyVersion},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := eligible
			test.change(&input)
			decision, applied := ApplyModel(test.rules, input)
			if applied != test.wantApplied || decision.Declined != test.wantDeclined || decision.PolicyVersion != test.wantPolicy || !slices.Equal(decision.Reasons, test.wantReasons) {
				t.Fatalf("got %+v applied=%v", decision, applied)
			}
		})
	}
}
