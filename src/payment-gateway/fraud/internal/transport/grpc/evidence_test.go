package grpctransport

import (
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
	fraudv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/fraud/v1"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

func scored(score float64, enforced bool) application.Result {
	readAt := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	return application.Result{EvidenceHash: "hash", Evidence: application.Evidence{
		AssessmentID: "assessment-1",
		Decision:     domain.Decision{Declined: enforced, Reasons: []string{domain.ModelReason}, PolicyVersion: domain.ModelPolicyVersion},
		Features:     application.FeatureObservation{Service: "payment_risk_v1"},
		Model: &application.ModelObservation{
			Name: "payment-risk", Version: "v3", Status: application.ModelAvailable, Score: &score,
			ReadAt: readAt, Threshold: 0.8, Enforced: enforced, Declined: enforced,
		},
	}}
}

func TestAnEnforcedScoreCarriesTheModelVersionFeatureSetAndScoreInTheResponse(t *testing.T) {
	assessment := riskAssessment(scored(0.9, true), paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE)
	if assessment.GetModelVersion() != "v3" || assessment.GetFeatureSetVersion() != "payment_risk_v1" || assessment.GetRiskScore() != 0.9 ||
		assessment.PolicyVersion != domain.ModelPolicyVersion || assessment.AssessmentId != "assessment-1" || assessment.EvidenceSha256 != "hash" || assessment.Outcome != paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE {
		t.Fatalf("assessment %+v", assessment)
	}
}

func TestAScoreThatWasNotEnforcedStaysOutOfTheResponse(t *testing.T) {
	result := scored(0.9, false)
	assessment := riskAssessment(result, paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE)
	if assessment.ModelVersion != nil || assessment.FeatureSetVersion != nil || assessment.RiskScore != nil {
		t.Fatalf("a score the policy did not apply leaked into the response: %+v", assessment)
	}
	result.Evidence.Model = nil
	if assessment := riskAssessment(result, paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE); assessment.ModelVersion != nil || assessment.RiskScore != nil {
		t.Fatalf("a missing model produced model fields: %+v", assessment)
	}
}

func TestTheEvidenceRecordsTheModelCallEvenWhenItIsNotEnforced(t *testing.T) {
	evidence := evidenceProto(scored(0.42, false).Evidence)
	model := evidence.GetModel()
	if model.GetName() != "payment-risk" || model.GetVersion() != "v3" || model.GetStatus() != fraudv1.ModelStatus_MODEL_STATUS_AVAILABLE || model.GetScore() != 0.42 || model.Score == nil ||
		model.GetThreshold() != 0.8 || model.GetEnforced() || model.GetDeclined() || model.GetReadAt().AsTime().Year() != 2026 {
		t.Fatalf("model evidence %+v", model)
	}
	if evidenceProto(application.Evidence{}).Model != nil {
		t.Fatal("evidence without a model gained one")
	}
}

func TestEveryModelStatusMapsToItsEnumAndAnUnknownOneToError(t *testing.T) {
	for status, want := range map[string]fraudv1.ModelStatus{
		application.ModelAvailable: fraudv1.ModelStatus_MODEL_STATUS_AVAILABLE,
		application.ModelTimeout:   fraudv1.ModelStatus_MODEL_STATUS_TIMEOUT,
		application.ModelError:     fraudv1.ModelStatus_MODEL_STATUS_ERROR,
		"surprise":                 fraudv1.ModelStatus_MODEL_STATUS_ERROR,
		"":                         fraudv1.ModelStatus_MODEL_STATUS_ERROR,
	} {
		if got := modelProto(application.ModelObservation{Status: status}).Status; got != want {
			t.Errorf("%q mapped to %v, want %v", status, got, want)
		}
	}
	if modelProto(application.ModelObservation{}).ReadAt != nil {
		t.Fatal("a zero read time was reported")
	}
}
