package application

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
)

type memoryStore struct{ completed *Result }

func (m *memoryStore) Claim(context.Context, Request, time.Duration, time.Duration) (Claim, error) {
	return Claim{ID: "assessment-1", Fence: 1}, nil
}

func (m *memoryStore) Complete(_ context.Context, _ Claim, result Result) error {
	m.completed = &result
	return nil
}
func (m *memoryStore) Fail(context.Context, Claim, time.Duration) error { return nil }
func (m *memoryStore) Ready(context.Context) error                      { return nil }

type fixedHistory struct{ features FeatureObservation }

func (f fixedHistory) Read(context.Context, Request) FeatureObservation { return f.features }

type fakeScorer struct {
	observation ModelObservation
	seen        FeatureObservation
	deadline    time.Duration
	calls       int
}

func (f *fakeScorer) Score(ctx context.Context, features FeatureObservation) ModelObservation {
	f.calls++
	f.seen = features
	if at, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(at)
	}
	return f.observation
}

func available() FeatureObservation {
	views := map[string]ViewObservation{}
	for _, name := range RequiredViews {
		views[name] = ViewObservation{Status: "available"}
	}
	return FeatureObservation{Service: "payment_risk_v1", Views: views}
}

func answer(score float64) ModelObservation {
	return ModelObservation{Name: "payment-risk", Version: "v3", Status: ModelAvailable, Score: &score, ReadAt: time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)}
}

func request(amountMinor int64) Request {
	attemptedAt := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	return Request{
		Identity: Identity{IntegrationID: "i", PaymentID: "p", AttemptID: "a"}, Digest: "digest",
		CustomerID: "c", MethodFingerprint: "m",
		Input: domain.Input{AmountMinor: amountMinor, CustomerCreatedAt: attemptedAt.Add(-90 * 24 * time.Hour), AttemptedAt: attemptedAt, ShippingCountry: "US"},
	}
}

func decide(t *testing.T, features FeatureObservation, scorer *fakeScorer, threshold float64, amountMinor int64) (Evidence, *memoryStore) {
	t.Helper()
	store := &memoryStore{}
	service, err := New(store, fixedHistory{features}, Config{Deadline: time.Second, Lease: 2 * time.Second, RetryHorizon: time.Hour, ErrorBackoff: time.Millisecond, HistoryTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if scorer != nil {
		if err := service.WithModel(ModelConfig{Scorer: scorer, Timeout: 150 * time.Millisecond, Threshold: threshold}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := service.Decide(context.Background(), request(amountMinor))
	if err != nil {
		t.Fatal(err)
	}
	if store.completed == nil || store.completed.Evidence.AssessmentID != result.Evidence.AssessmentID {
		t.Fatal("the assessment was not persisted")
	}
	return result.Evidence, store
}

func TestAnEnforcedScoreAtTheThresholdDeclinesAnApprovedAttempt(t *testing.T) {
	scorer := &fakeScorer{observation: answer(0.8)}
	evidence, _ := decide(t, available(), scorer, 0.8, 1000)
	if !evidence.Decision.Declined || len(evidence.Decision.Reasons) != 1 || evidence.Decision.Reasons[0] != domain.ModelReason || evidence.Decision.PolicyVersion != domain.ModelPolicyVersion {
		t.Fatalf("decision %+v", evidence.Decision)
	}
	model := evidence.Model
	if model == nil || !model.Enforced || !model.Declined || model.Threshold != 0.8 || model.Version != "v3" || model.Name != "payment-risk" || *model.Score != 0.8 {
		t.Fatalf("model evidence %+v", model)
	}
	if evidence.HistoricalShadowOnly {
		t.Fatal("an enforced score must not be recorded as shadow-only")
	}
}

func TestAScoreJustBelowTheThresholdApprovesButIsStillReported(t *testing.T) {
	evidence, _ := decide(t, available(), &fakeScorer{observation: answer(0.79)}, 0.8, 1000)
	if evidence.Decision.Declined || evidence.Decision.PolicyVersion != domain.ModelPolicyVersion || evidence.Model == nil || !evidence.Model.Enforced || evidence.Model.Declined {
		t.Fatalf("decision %+v model %+v", evidence.Decision, evidence.Model)
	}
}

func TestAnIncompleteViewKeepsTheRulesDecisionForEveryRequiredView(t *testing.T) {
	for _, name := range RequiredViews {
		t.Run(name, func(t *testing.T) {
			features := available()
			features.Views[name] = ViewObservation{Status: "expired"}
			evidence, _ := decide(t, features, &fakeScorer{observation: answer(1)}, 0.5, 1000)
			if evidence.Decision.Declined || evidence.Decision.PolicyVersion != domain.PolicyVersion || evidence.Model == nil || evidence.Model.Enforced || !evidence.HistoricalShadowOnly {
				t.Fatalf("decision %+v model %+v", evidence.Decision, evidence.Model)
			}
		})
	}
}

func TestAMissingViewKeepsTheRulesDecision(t *testing.T) {
	features := available()
	delete(features.Views, "request_features")
	evidence, _ := decide(t, features, &fakeScorer{observation: answer(1)}, 0.5, 1000)
	if evidence.Decision.Declined || evidence.Model.Enforced {
		t.Fatalf("decision %+v model %+v", evidence.Decision, evidence.Model)
	}
}

func TestARulesDeclineStaysDeclinedWhateverTheScore(t *testing.T) {
	for _, score := range []float64{0, 0.99} {
		evidence, _ := decide(t, available(), &fakeScorer{observation: answer(score)}, 0.5, 200001)
		if !evidence.Decision.Declined || len(evidence.Decision.Reasons) != 1 || evidence.Decision.Reasons[0] != "HIGH_AMOUNT" || evidence.Model.Declined {
			t.Fatalf("score %v: decision %+v model %+v", score, evidence.Decision, evidence.Model)
		}
	}
}

func TestAFailedCallKeepsTheRulesDecisionAndRecordsNoScore(t *testing.T) {
	for _, status := range []string{ModelTimeout, ModelError} {
		scorer := &fakeScorer{observation: ModelObservation{Name: "payment-risk", Status: status}}
		evidence, _ := decide(t, available(), scorer, 0.5, 1000)
		if evidence.Decision.Declined || evidence.Decision.PolicyVersion != domain.PolicyVersion || evidence.Model.Status != status || evidence.Model.Score != nil || evidence.Model.Enforced {
			t.Fatalf("%s: decision %+v model %+v", status, evidence.Decision, evidence.Model)
		}
	}
}

func TestAnAvailableAnswerWithoutAScoreIsNotEnforced(t *testing.T) {
	scorer := &fakeScorer{observation: ModelObservation{Name: "payment-risk", Status: ModelAvailable}}
	evidence, _ := decide(t, available(), scorer, 0.5, 1000)
	if evidence.Decision.Declined || evidence.Model.Enforced {
		t.Fatalf("decision %+v model %+v", evidence.Decision, evidence.Model)
	}
}

func TestTheScorerSeesTheFeatureReadUnderItsOwnDeadline(t *testing.T) {
	features := available()
	scorer := &fakeScorer{observation: answer(0.1)}
	decide(t, features, scorer, 0.5, 1000)
	if scorer.calls != 1 || scorer.seen.Service != "payment_risk_v1" || len(scorer.seen.Views) != len(RequiredViews) {
		t.Fatalf("scorer saw %+v after %d calls", scorer.seen, scorer.calls)
	}
	if scorer.deadline <= 0 || scorer.deadline > 150*time.Millisecond {
		t.Fatalf("model deadline %v is not within its 150ms budget", scorer.deadline)
	}
}

func TestWithoutAModelTheEvidenceKeepsItsOldShapeAndHash(t *testing.T) {
	evidence, store := decide(t, available(), nil, 0, 1000)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Model != nil || strings.Contains(string(encoded), `"Model"`) || !evidence.HistoricalShadowOnly || store.completed.Evidence.Model != nil {
		t.Fatalf("evidence without a model changed shape: %s", encoded)
	}
}

func TestEvidenceWithAModelSurvivesAJSONRoundTripUnchanged(t *testing.T) {
	evidence, _ := decide(t, available(), &fakeScorer{observation: answer(0.8123456789012345)}, 0.8, 1000)
	first, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Evidence
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	second, err := json.Marshal(decoded)
	if err != nil || string(first) != string(second) {
		t.Fatalf("replay would change the evidence hash:\n%s\n%s", first, second)
	}
}

func TestWithModelValidatesTheModeBudgetAndThreshold(t *testing.T) {
	service, err := New(&memoryStore{}, nil, Config{Deadline: time.Second, Lease: 2 * time.Second, RetryHorizon: time.Hour, ErrorBackoff: time.Millisecond, HistoryTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	scorer := &fakeScorer{}
	for _, test := range []struct {
		name   string
		config ModelConfig
		ok     bool
	}{
		{"valid", ModelConfig{Scorer: scorer, Timeout: 150 * time.Millisecond, Threshold: 0.8}, true},
		{"the whole budget", ModelConfig{Scorer: scorer, Timeout: 900 * time.Millisecond, Threshold: 0.8}, true},
		{"over the budget by a nanosecond", ModelConfig{Scorer: scorer, Timeout: 900*time.Millisecond + 1, Threshold: 0.8}, false},
		{"no scorer", ModelConfig{Timeout: time.Millisecond, Threshold: 0.8}, false},
		{"no timeout", ModelConfig{Scorer: scorer, Threshold: 0.8}, false},
		{"negative timeout", ModelConfig{Scorer: scorer, Timeout: -time.Millisecond, Threshold: 0.8}, false},
		{"no threshold", ModelConfig{Scorer: scorer, Timeout: time.Millisecond}, false},
		{"negative threshold", ModelConfig{Scorer: scorer, Timeout: time.Millisecond, Threshold: -0.1}, false},
		{"threshold above one", ModelConfig{Scorer: scorer, Timeout: time.Millisecond, Threshold: 1.0001}, false},
		{"threshold of one", ModelConfig{Scorer: scorer, Timeout: time.Millisecond, Threshold: 1}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := service.WithModel(test.config); (err == nil) != test.ok {
				t.Fatalf("error %v, want ok=%v", err, test.ok)
			}
		})
	}
}

func TestTheThresholdOfTheVersionThatAnsweredDecidesAndIsRecorded(t *testing.T) {
	observation := answer(0.7)
	observation.Version, observation.Threshold = "v4", 0.6
	evidence, _ := decide(t, available(), &fakeScorer{observation: observation}, 0.8, 1000)
	model := evidence.Model
	if !evidence.Decision.Declined || model == nil || model.Threshold != 0.6 || model.Version != "v4" || !model.Declined {
		t.Fatalf("decision %+v model %+v", evidence.Decision, model)
	}
	for _, threshold := range []float64{0, -1, 1.5} {
		observation.Threshold = threshold
		evidence, _ = decide(t, available(), &fakeScorer{observation: observation}, 0.8, 1000)
		if evidence.Decision.Declined || evidence.Model.Threshold != 0.8 {
			t.Fatalf("threshold %v: decision %+v model %+v", threshold, evidence.Decision, evidence.Model)
		}
	}
}
