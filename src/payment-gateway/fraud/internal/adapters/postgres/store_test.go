package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/testsupport"
)

func request() application.Request {
	digest := sha256.Sum256([]byte("request"))
	now := time.Now().UTC()
	return application.Request{Identity: application.Identity{IntegrationID: "integration", PaymentID: "payment", AttemptID: "attempt"}, Digest: hex.EncodeToString(digest[:]), CustomerID: "customer", MethodFingerprint: "method", Input: domain.Input{AmountMinor: 100, AttemptedAt: now, CustomerCreatedAt: now.Add(-30 * 24 * time.Hour), ShippingCountry: "US"}}
}

func TestConcurrentClaimsFencedRecoveryAndImmutableCompletion(t *testing.T) {
	store, admin := testsupport.Database(t)
	ctx := context.Background()
	input := request()
	const replicas = 8
	results := make(chan application.Claim, replicas)
	failures := make(chan error, replicas)
	var workers sync.WaitGroup
	for range replicas {
		workers.Go(func() {
			claim, err := store.Claim(ctx, input, time.Second, time.Hour)
			if err != nil {
				failures <- err
			} else {
				results <- claim
			}
		})
	}
	workers.Wait()
	close(results)
	close(failures)
	if len(results) != 1 {
		t.Fatalf("owners=%d", len(results))
	}
	for err := range failures {
		if !errors.Is(err, application.ErrInProgress) {
			t.Fatal(err)
		}
	}
	first := <-results
	if _, err := admin.Exec(ctx, `UPDATE assessments SET lease_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	resumed, err := store.Claim(ctx, input, time.Second, time.Hour)
	if err != nil || resumed.Fence <= first.Fence {
		t.Fatalf("recovery: %+v %v", resumed, err)
	}
	if err = store.Complete(ctx, first, application.Result{}); !errors.Is(err, application.ErrFenced) {
		t.Fatalf("stale completion: %v", err)
	}
	if err = store.Fail(ctx, first, time.Millisecond); !errors.Is(err, application.ErrFenced) {
		t.Fatalf("stale failure: %v", err)
	}
	completed := application.Result{Evidence: application.Evidence{AssessmentID: resumed.ID, Decision: domain.Evaluate(input.Input)}}
	if err = store.Complete(ctx, resumed, completed); err != nil {
		t.Fatal(err)
	}
	replay, err := store.Claim(ctx, input, time.Second, time.Hour)
	if err != nil || replay.Result == nil || !reflect.DeepEqual(*replay.Result, completed) {
		t.Fatalf("completion replay: %+v %v", replay, err)
	}
	for _, statement := range []string{`UPDATE assessments SET result='changed'::bytea`, `DELETE FROM assessments`, `TRUNCATE assessments`} {
		if _, err = admin.Exec(ctx, statement); err == nil {
			t.Fatalf("completed evidence was mutable: %s", statement)
		}
	}
	for _, statement := range []string{`UPDATE assessments SET request_digest=repeat('0',64)`, `DELETE FROM assessments`, `CREATE TABLE unauthorized (id int)`} {
		tx, err := admin.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, `SET LOCAL ROLE fraud_app`); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, statement)
		var denied *pgconn.PgError
		if !errors.As(err, &denied) || denied.Code != "42501" {
			t.Errorf("runtime privilege leak: %s: %v", statement, err)
		}
		if err = tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	changed := input
	changed.Digest = hex.EncodeToString(make([]byte, 32))
	if _, err = store.Claim(ctx, changed, time.Second, time.Hour); !errors.Is(err, application.ErrConflict) {
		t.Fatalf("content binding: %v", err)
	}
}

type history struct {
	calls int
	value int64
}

func (h *history) Read(context.Context, application.Request) (application.ViewObservation, application.ViewObservation) {
	h.calls++
	value := h.value
	return application.ViewObservation{Status: "available", Values: application.SnapshotValues{Count1h: &value}}, application.ViewObservation{Status: "timeout"}
}

func TestAssessmentResponseLossReplaysOriginalEvidenceAndRejectsExpiredRecovery(t *testing.T) {
	store, admin := testsupport.Database(t)
	observed := &history{value: 1}
	config := application.Config{Deadline: time.Second, Lease: 2 * time.Second, RetryHorizon: time.Hour, ErrorBackoff: time.Millisecond, HistoryTimeout: time.Second, GuardTimeout: time.Second}
	first, err := application.New(store, observed, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	input := request()
	original, err := first.Decide(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	observed.value = 99
	replica, err := application.New(store, observed, nil, config)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := replica.Decide(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.EvidenceHash != original.EvidenceHash || !reflect.DeepEqual(replay.Evidence, original.Evidence) || observed.calls != 1 {
		t.Fatalf("resampled completed result: %+v", replay)
	}
	pending := input
	pending.AttemptID = "failed-attempt"
	claim, err := store.Claim(context.Background(), pending, time.Second, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Fail(context.Background(), claim, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Claim(context.Background(), pending, time.Second, time.Hour); !errors.Is(err, application.ErrInProgress) {
		t.Fatalf("error backoff: %v", err)
	}
	if _, err = admin.Exec(context.Background(), `UPDATE assessments SET retry_until=clock_timestamp()-interval '1 second' WHERE assessment_id=$1`, claim.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = replica.Decide(context.Background(), pending); !errors.Is(err, application.ErrExpired) {
		t.Fatalf("unsafe expired recovery: %v", err)
	}
}

type timedOutHistory struct{}

func (timedOutHistory) Read(ctx context.Context, _ application.Request) (application.ViewObservation, application.ViewObservation) {
	<-ctx.Done()
	return application.ViewObservation{Status: "timeout"}, application.ViewObservation{Status: "timeout"}
}

type partialGuard struct{}

func (partialGuard) Register(context.Context, application.Request, time.Time) (application.GuardObservation, application.GuardObservation) {
	count := int64(100)
	return application.GuardObservation{Status: "registered", Count10s: &count, ThresholdExceeded: true}, application.GuardObservation{Status: "error"}
}

func TestObservationFailuresAndThresholdsPreserveRulesAndDurableEvidence(t *testing.T) {
	store, _ := testsupport.Database(t)
	service, err := application.New(store, timedOutHistory{}, partialGuard{}, application.Config{Deadline: time.Second, Lease: 2 * time.Second, RetryHorizon: time.Hour, ErrorBackoff: time.Millisecond, HistoryTimeout: 20 * time.Millisecond, GuardTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Decide(context.Background(), request())
	if err != nil {
		t.Fatal(err)
	}
	evidence := result.Evidence
	if evidence.Decision.Declined || evidence.Customer.Status != "timeout" || evidence.Method.Status != "timeout" || evidence.CustomerGuard.Status != "registered" || !evidence.CustomerGuard.ThresholdExceeded || evidence.MethodGuard.Status != "error" {
		t.Fatalf("observation changed rules or lost partial evidence: %+v", evidence)
	}
}
