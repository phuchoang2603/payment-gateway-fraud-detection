package domain

import (
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"
)

var allowedPhases = map[Phase][]Phase{
	PhaseStarted:      {PhaseRiskApproved, PhaseRiskDeclined},
	PhaseRiskApproved: {PhaseRequested},
	PhaseRiskDeclined: {PhaseDeclined},
	PhaseRequested:    {PhaseUnknown, PhaseDeclined, PhaseSucceeded},
	PhaseUnknown:      {PhaseDeclined, PhaseSucceeded},
}

// command draws any lifecycle command against the payment, valid or not, so the property covers rejections too.
func command(t *rapid.T, state State, step int) Command {
	version := state.Version
	if rapid.IntRange(0, 9).Draw(t, "stale") == 0 {
		version = rapid.Uint64Range(0, state.Version+2).Draw(t, "version")
	}
	envelope := Envelope{
		EventID:         fmt.Sprintf("event-%d", step),
		ExpectedVersion: version,
		Now:             time.Unix(1_800_000_000, 0).Add(time.Duration(rapid.IntRange(-60, 600).Draw(t, "seconds")) * time.Second),
	}
	attempt := rapid.SampledFrom([]string{"attempt-1", "attempt-2", "attempt-3"}).Draw(t, "attempt")
	if active, ok := state.ActiveAttempt(); ok && rapid.IntRange(0, 3).Draw(t, "use active") > 0 {
		attempt = active.ID
	}
	switch rapid.IntRange(0, 8).Draw(t, "command") {
	case 0:
		return CreatePayment{
			Envelope: envelope, PaymentID: "payment", IntegrationID: "integration", CorrelationID: "correlation",
			SessionID: "session", Merchant: &MerchantRef{}, Customer: &CustomerRef{}, Snapshot: &CheckoutSnapshot{},
		}
	case 1:
		return StartAttempt{Envelope: envelope, AttemptID: attempt}
	case 2:
		outcome := rapid.SampledFrom([]RiskOutcome{RiskOutcomeApprove, RiskOutcomeDecline}).Draw(t, "outcome")
		return RecordRiskDecision{Envelope: envelope, AttemptID: attempt, Assessment: &RiskAssessment{Outcome: outcome, PolicyVersion: "rules-v1"}}
	case 3:
		return RequestProcessorOperation{Envelope: envelope, AttemptID: attempt, OperationKey: "operation-" + attempt}
	case 4:
		return RecordOutcomeUnknown{Envelope: envelope, AttemptID: attempt}
	case 5:
		source := rapid.SampledFrom([]DeclineSource{DeclineSourceRisk, DeclineSourceProcessor}).Draw(t, "source")
		return DeclineAttempt{Envelope: envelope, AttemptID: attempt, Source: source, Reason: "declined"}
	case 6:
		return SucceedPayment{Envelope: envelope, AttemptID: attempt, ProcessorReference: "reference"}
	default:
		return ExpirePayment{Envelope: envelope}
	}
}

func checkInvariants(t *rapid.T, state State) {
	active := 0
	ids := map[string]bool{}
	succeeded := 0
	for _, attempt := range state.Attempts {
		if attempt.Phase.Active() {
			active++
		}
		if ids[attempt.ID] {
			t.Fatalf("attempt %s appears twice", attempt.ID)
		}
		ids[attempt.ID] = true
		if attempt.Phase == PhaseSucceeded {
			succeeded++
		}
	}
	if active > 1 {
		t.Fatalf("%d active attempts", active)
	}
	if (state.Status == StatusSucceeded) != (succeeded == 1) || succeeded > 1 {
		t.Fatalf("status %s with %d succeeded attempts", state.Status, succeeded)
	}
	if state.Status == StatusExpired && active == 1 {
		if attempt, _ := state.ActiveAttempt(); attempt.Phase.AwaitingProcessor() {
			t.Fatalf("expired while attempt %s awaits the processor", attempt.ID)
		}
	}
}

func TestTransitionKeepsLifecycleInvariantsForAnyCommandSequence(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var state State
		steps := rapid.IntRange(1, 40).Draw(t, "steps")
		for step := range steps {
			cmd := command(t, state, step)
			before := state.clone()
			next, event, err := Transition(state, cmd)

			if !reflect.DeepEqual(state, before) {
				t.Fatalf("%T mutated its input state", cmd)
			}
			if err != nil {
				if event != nil || RuleOf(err) == "" {
					t.Fatalf("%T rejected without a rule or with an event: %v", cmd, err)
				}
				if before.Status == StatusSucceeded || before.Status == StatusExpired {
					if _, create := cmd.(CreatePayment); !create && RuleOf(err) != RuleEventAfterTerminal {
						t.Fatalf("%T after %s rejected with %s, want %s", cmd, before.Status, RuleOf(err), RuleEventAfterTerminal)
					}
				}
				continue
			}

			if before.Status == StatusSucceeded || before.Status == StatusExpired {
				t.Fatalf("%T accepted after %s", cmd, before.Status)
			}
			if next.Version != before.Version+1 || event.Version != next.Version || event.ID != cmd.envelope().EventID {
				t.Fatalf("%T: version %d -> %d, event version %d", cmd, before.Version, next.Version, event.Version)
			}
			if next.UpdatedAt.Before(before.UpdatedAt) {
				t.Fatalf("%T moved updated_at back from %v to %v", cmd, before.UpdatedAt, next.UpdatedAt)
			}
			for _, attempt := range before.Attempts {
				after, _ := next.Attempt(attempt.ID)
				if after.Phase != attempt.Phase && !slices.Contains(allowedPhases[attempt.Phase], after.Phase) {
					t.Fatalf("%T moved attempt %s from %s to %s", cmd, attempt.ID, attempt.Phase, after.Phase)
				}
			}
			checkInvariants(t, next)
			state = next
		}
	})
}
