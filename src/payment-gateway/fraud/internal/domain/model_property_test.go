package domain

import (
	"slices"
	"testing"

	"pgregory.net/rapid"
)

func modelInput(t *rapid.T) ModelInput {
	return ModelInput{
		Available: rapid.Bool().Draw(t, "available"),
		Complete:  rapid.Bool().Draw(t, "complete"),
		Score:     rapid.Float64Range(0, 1).Draw(t, "score"),
		Threshold: rapid.Float64Range(0.01, 1).Draw(t, "threshold"),
	}
}

func rulesDecision(t *rapid.T) Decision {
	return Evaluate(policyInput(t))
}

func TestTheModelNeverTurnsARulesDeclineIntoAnApproval(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		rules := rulesDecision(t)
		decision, _ := ApplyModel(rules, modelInput(t))
		if rules.Declined && !decision.Declined {
			t.Fatalf("rules declined %+v but the model approved %+v", rules, decision)
		}
		for _, reason := range rules.Reasons {
			if !slices.Contains(decision.Reasons, reason) {
				t.Fatalf("rules reason %s was dropped from %+v", reason, decision)
			}
		}
	})
}

func TestTheRulesDecisionStandsUnlessThePolicyHolds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		rules, input := rulesDecision(t), modelInput(t)
		decision, applied := ApplyModel(rules, input)
		eligible := input.Available && input.Complete
		if applied != eligible {
			t.Fatalf("applied %v for %+v", applied, input)
		}
		if !eligible && (decision.Declined != rules.Declined || decision.PolicyVersion != PolicyVersion || !slices.Equal(decision.Reasons, rules.Reasons)) {
			t.Fatalf("an ineligible score changed %+v into %+v", rules, decision)
		}
	})
}

func TestAnEligibleScoreDeclinesExactlyAtOrAboveTheThreshold(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		input := modelInput(t)
		input.Available, input.Complete = true, true
		approved := Decision{PolicyVersion: PolicyVersion}
		decision, applied := ApplyModel(approved, input)
		if !applied || decision.PolicyVersion != ModelPolicyVersion {
			t.Fatalf("decision %+v applied %v", decision, applied)
		}
		if decision.Declined != (input.Score >= input.Threshold) {
			t.Fatalf("score %v threshold %v gave %+v", input.Score, input.Threshold, decision)
		}
		if decision.Declined != slices.Equal(decision.Reasons, []string{ModelReason}) {
			t.Fatalf("reasons %v do not match declined=%v", decision.Reasons, decision.Declined)
		}
	})
}

func TestApplyModelDoesNotAliasTheRulesReasons(t *testing.T) {
	rules := Decision{Declined: false, Reasons: make([]string, 0, 4), PolicyVersion: PolicyVersion}
	decision, _ := ApplyModel(rules, ModelInput{Available: true, Complete: true, Score: 1, Threshold: 0.5})
	if len(rules.Reasons) != 0 || len(decision.Reasons) != 1 {
		t.Fatalf("rules %v decision %v", rules.Reasons, decision.Reasons)
	}
}
