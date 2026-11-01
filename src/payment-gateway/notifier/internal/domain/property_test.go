package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"
)

func schedule(t *rapid.T) Schedule {
	initial := time.Duration(rapid.Int64Range(1, int64(time.Minute)).Draw(t, "initial"))
	return Schedule{
		Initial:     initial,
		Factor:      rapid.Float64Range(1, 10).Draw(t, "factor"),
		Cap:         initial + time.Duration(rapid.Int64Range(0, int64(24*time.Hour)).Draw(t, "cap above initial")),
		MaxAttempts: rapid.IntRange(1, 50).Draw(t, "max attempts"),
	}
}

func TestBackoffStaysWithinZeroAndTheCapAndItsCeilingNeverShrinks(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := schedule(t)
		if err := s.Validate(); err != nil {
			t.Fatalf("generated schedule invalid: %v", err)
		}
		failed := rapid.IntRange(-2, 200).Draw(t, "failed attempts")
		jitter := rapid.Float64Range(0, 0.999999).Draw(t, "jitter")

		delay := s.Delay(failed, jitter)
		if delay < 0 || delay > s.Cap {
			t.Fatalf("delay %v outside [0, %v]", delay, s.Cap)
		}
		if s.Delay(failed, 0) != 0 {
			t.Fatalf("zero jitter waited %v", s.Delay(failed, 0))
		}
		const top = 0.999999
		if s.Delay(failed+1, top) < s.Delay(failed, top) {
			t.Fatalf("ceiling shrank after attempt %d", failed)
		}
		if s.Exhausted(failed) != (failed >= s.MaxAttempts) {
			t.Fatalf("exhausted(%d) with max %d", failed, s.MaxAttempts)
		}
	})
}

func TestSignatureCommitsToSecretTimestampAndBody(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		secret := rapid.StringN(1, 32, -1).Draw(t, "secret")
		at := time.Unix(rapid.Int64Range(0, 4_000_000_000).Draw(t, "unix"), 0)
		body := rapid.SliceOfN(rapid.Byte(), 0, 256).Draw(t, "body")
		signature := Sign(secret, at, body)

		if signature != Sign(secret, at, body) || !strings.HasPrefix(signature, "t=") || !strings.Contains(signature, ",v1=") {
			t.Fatalf("signature %q", signature)
		}
		changed := append(append([]byte{}, body...), rapid.Byte().Draw(t, "extra"))
		if Sign(secret, at, changed) == signature || Sign(secret+"x", at, body) == signature || Sign(secret, at.Add(time.Second), body) == signature {
			t.Fatalf("signature did not change with its input")
		}
	})
}

func TestWebhookBodyRoundTripsItsFields(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		event := Event{
			ID:         rapid.String().Draw(t, "event id"),
			PaymentID:  rapid.String().Draw(t, "payment id"),
			OccurredAt: time.Unix(0, rapid.Int64Range(0, 4_000_000_000_000_000_000).Draw(t, "nanos")),
		}
		outcome := Outcome{
			Type:      rapid.String().Draw(t, "type"),
			Status:    rapid.SampledFrom([]string{StatusDeclined, StatusSucceeded, StatusExpired}).Draw(t, "status"),
			AttemptID: rapid.String().Draw(t, "attempt id"),
			Reason:    rapid.String().Draw(t, "reason"),
		}
		reference := rapid.String().Draw(t, "order reference")

		raw, err := BuildBody(event, outcome, reference)
		if err != nil {
			t.Fatal(err)
		}
		var body webhookBody
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatal(err)
		}
		occurred, err := time.Parse(time.RFC3339Nano, body.OccurredAt)
		if err != nil || !occurred.Equal(event.OccurredAt) {
			t.Fatalf("occurred_at %q, want %v", body.OccurredAt, event.OccurredAt)
		}
		if body.EventID != event.ID || body.PaymentID != event.PaymentID || body.Type != outcome.Type ||
			body.Status != outcome.Status || body.AttemptID != outcome.AttemptID || body.Reason != outcome.Reason ||
			body.OrderReference != reference {
			t.Fatalf("body %+v from %+v %+v", body, event, outcome)
		}
	})
}
