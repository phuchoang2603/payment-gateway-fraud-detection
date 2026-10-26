package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

func shortConfig() application.Config {
	return application.Config{PaymentExpiry: 15 * time.Minute, AttemptBudget: 2 * time.Second, FraudTimeout: 300 * time.Millisecond, ProcessorTimeout: 400 * time.Millisecond}
}

func newStack(t *testing.T, redis bool) *testsupport.Stack {
	t.Helper()
	return testsupport.NewStack(t, testsupport.StackOptions{Config: shortConfig(), WithRedis: redis})
}

func mustCreate(t *testing.T, stack *testsupport.Stack, key, order string) *paymentsv1.Payment {
	t.Helper()
	payment, err := stack.Service.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, key, order, 12500))
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}
	return payment
}

func start(stack *testsupport.Stack, payment *paymentsv1.Payment, key, token string) (*paymentsv1.AttemptResult, error) {
	return stack.Service.StartAttempt(context.Background(), testsupport.AttemptRequest(payment, key, token))
}

func mustStart(t *testing.T, stack *testsupport.Stack, payment *paymentsv1.Payment, key, token string) *paymentsv1.AttemptResult {
	t.Helper()
	result, err := start(stack, payment, key, token)
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}
	return result
}

func kinds(t *testing.T, stack *testsupport.Stack, payment *paymentsv1.Payment) []string {
	t.Helper()
	return testsupport.EventKinds(stack.DB.Events(t, payment.GetPaymentId()))
}

func wantKinds(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

func TestIdempotencyProperties(t *testing.T) {
	for _, withRedis := range []bool{false, true} {
		t.Run(map[bool]string{false: "postgres only", true: "with redis"}[withRedis], func(t *testing.T) {
			stack := newStack(t, withRedis)
			sequence := 0
			property := func(amount uint16, repeats uint8) bool {
				sequence++
				key := fmt.Sprintf("property-%d", sequence)
				request := testsupport.CreateRequest(testsupport.IntegrationID, key, key, int64(amount))
				payment, err := stack.Service.CreatePayment(context.Background(), request)
				if err != nil {
					t.Logf("create: %v", err)
					return false
				}
				if payment.GetCheckoutToken() == "" || payment.GetSessionId() == payment.GetPaymentId() ||
					!payment.GetExpiresAt().AsTime().Equal(stack.Clock.Now().Add(15*time.Minute).Truncate(time.Second)) {
					return false
				}
				for range 1 + int(repeats%4) {
					stack.Clock.Advance(time.Second)
					again, err := stack.Service.CreatePayment(context.Background(), request)
					if err != nil || !proto.Equal(payment, again) {
						t.Logf("create replay: %v", err)
						return false
					}
				}
				request.Snapshot.Total.MinorUnits++
				request.Snapshot.Items[0].UnitPrice.MinorUnits++
				_, err = stack.Service.CreatePayment(context.Background(), request)
				if !errors.Is(err, application.ErrIdempotencyConflict) {
					return false
				}
				if withRedis {
					stack.FlushRedis(t)
					if _, err := stack.Service.CreatePayment(context.Background(), request); !errors.Is(err, application.ErrIdempotencyConflict) {
						return false
					}
				}
				loaded, err := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{IntegrationId: payment.GetIntegrationId(), PaymentId: payment.GetPaymentId()})
				if err != nil || loaded.GetSnapshot().GetTotal().GetMinorUnits() != int64(amount) || loaded.GetVersion() != 1 {
					return false
				}
				sales, decisions := stack.Processor.TotalSaleCalls(), len(stack.Fraud.Requests())
				first := mustStart(t, stack, payment, key, "vault-token")
				for range 1 + int(repeats%4) {
					again := mustStart(t, stack, payment, key, "vault-token")
					if !proto.Equal(first, again) {
						return false
					}
				}
				_, err = start(stack, payment, key, "different-token")
				if !errors.Is(err, application.ErrIdempotencyConflict) || stack.Processor.TotalSaleCalls() != sales+1 || len(stack.Fraud.Requests()) != decisions+1 {
					return false
				}
				wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "succeeded")
				return true
			}
			if err := quick.Check(property, &quick.Config{MaxCount: 12, Rand: rand.New(rand.NewSource(82))}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCreatePaymentReusingAnOrderReferenceUnderANewKeyIsAConflict(t *testing.T) {
	stack := newStack(t, false)
	mustCreate(t, stack, "key-1", "order-1")
	_, err := stack.Service.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-2", "order-1", 12500))
	if err == nil || !strings.Contains(err.Error(), application.ErrOrderConflict.Error()) {
		t.Fatalf("err = %v, want order conflict", err)
	}
	other := testsupport.CreateRequest(uuid.MustParse("22222222-89ab-4cde-8fab-0123456789ab"), "key-2", "order-1", 12500)
	if _, err := stack.Service.CreatePayment(context.Background(), other); err != nil {
		t.Fatalf("another integration may use the same order reference: %v", err)
	}
}

func TestCreatePaymentRejectsInvalidSnapshots(t *testing.T) {
	stack := newStack(t, false)
	tests := map[string]func(*paymentsv1.CreatePaymentRequest){
		"items disagree with total": func(r *paymentsv1.CreatePaymentRequest) { r.Snapshot.Total.MinorUnits++ },
		"missing key":               func(r *paymentsv1.CreatePaymentRequest) { r.IdempotencyKey = "" },
		"missing seller":            func(r *paymentsv1.CreatePaymentRequest) { r.ExternalSellerId = "" },
		"bad integration":           func(r *paymentsv1.CreatePaymentRequest) { r.IntegrationId = "nope" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request := testsupport.CreateRequest(testsupport.IntegrationID, "key", "order", 12500)
			mutate(request)
			if _, err := stack.Service.CreatePayment(context.Background(), request); err == nil || !strings.Contains(err.Error(), application.ErrInvalidRequest.Error()) {
				t.Fatalf("err = %v, want invalid request", err)
			}
		})
	}
}

func TestCreatePaymentCarriesTheMerchantAndCustomerIdsOfTheIdentityVectors(t *testing.T) {
	raw, err := os.ReadFile("../../../../../contracts/synthetic/identity-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors struct {
		IntegrationID string `json:"integration_id"`
		IDs           []struct {
			Kind       string `json:"kind"`
			ExternalID string `json:"external_id"`
			Expected   string `json:"expected"`
		} `json:"ids"`
	}
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	stack := newStack(t, false)
	integrationID := uuid.MustParse(vectors.IntegrationID)
	for i, vector := range vectors.IDs {
		request := testsupport.CreateRequest(integrationID, "key", "order", 12500)
		request.IdempotencyKey = vector.Kind + "-" + vector.ExternalID
		request.Snapshot.ExternalOrderId = "order-" + request.IdempotencyKey
		request.ExternalSellerId, request.ExternalBuyerId = "other-seller", "other-buyer"
		switch vector.Kind {
		case "merchant":
			request.ExternalSellerId = vector.ExternalID
		case "customer":
			request.ExternalBuyerId = vector.ExternalID
		}
		payment, err := stack.Service.CreatePayment(context.Background(), request)
		if err != nil {
			t.Fatalf("vector %d: %v", i, err)
		}
		got := payment.GetMerchant().GetMerchantId()
		if vector.Kind == "customer" {
			got = payment.GetCustomer().GetCustomerId()
		}
		if got != vector.Expected {
			t.Errorf("%s %q id = %s, want %s", vector.Kind, vector.ExternalID, got, vector.Expected)
		}
	}
}

func TestStartAttemptRejectsATokenIssuedForAnotherSession(t *testing.T) {
	stack := newStack(t, false)
	first := mustCreate(t, stack, "key-1", "order-1")
	second := mustCreate(t, stack, "key-2", "order-2")

	request := testsupport.AttemptRequest(second, "attempt-1", "vault-token")
	request.CheckoutToken = first.GetCheckoutToken()
	if _, err := stack.Service.StartAttempt(context.Background(), request); err == nil || !strings.Contains(err.Error(), application.ErrAccessDenied.Error()) {
		t.Fatalf("err = %v, want access denied", err)
	}
	request.CheckoutToken = "garbage"
	if _, err := stack.Service.StartAttempt(context.Background(), request); err == nil || !strings.Contains(err.Error(), application.ErrAccessDenied.Error()) {
		t.Fatalf("err = %v, want access denied", err)
	}
	if stack.Processor.TotalSaleCalls() != 0 || len(stack.Fraud.Requests()) != 0 {
		t.Fatal("a rejected token reached fraud or the processor")
	}
	wantKinds(t, kinds(t, stack, second), "created")
}

func TestStartAttemptSuccessRecordsTheFullLifecycle(t *testing.T) {
	stack := newStack(t, true)
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token-1")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED || result.GetAttemptId() == "" {
		t.Fatalf("result = %v", result)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "succeeded")
	events := stack.DB.Events(t, payment.GetPaymentId())
	created := events[0].GetPaymentCreated()
	started := events[1].GetAttemptStarted()
	context := started.GetContext()
	requests := stack.Fraud.Requests()
	if events[1].GetSchemaVersion() != 3 || context == nil || len(requests) != 1 ||
		!proto.Equal(context.GetMerchant(), created.GetMerchant()) ||
		!proto.Equal(context.GetCustomer(), created.GetCustomer()) ||
		!proto.Equal(context.GetSnapshot(), created.GetSnapshot()) ||
		!proto.Equal(context.GetSnapshot(), requests[0].GetSnapshot()) ||
		!proto.Equal(started.GetSignals(), requests[0].GetSignals()) {
		t.Fatalf("attempt scoring context differs from checkout/Fraud input: %v", started)
	}
	for i, event := range events {
		if event.GetAggregateVersion() != uint64(i+1) || event.GetCorrelationId() == "" || event.GetCorrelationId() != events[0].GetCorrelationId() ||
			event.GetIntegrationId() != testsupport.IntegrationID.String() {
			t.Fatalf("event %d envelope = %v", i, event)
		}
	}
	loaded, err := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{IntegrationId: payment.GetIntegrationId(), PaymentId: payment.GetPaymentId()})
	if err != nil || loaded.GetStatus() != paymentsv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED || loaded.GetVersion() != 5 || len(loaded.GetAttempts()) != 1 {
		t.Fatalf("payment = %v, %v", loaded, err)
	}
	if got := stack.Fraud.Requests(); len(got) != 1 || got[0].GetSignals().GetAttemptedAt() == nil || got[0].GetAttemptId() != result.GetAttemptId() {
		t.Fatalf("fraud requests = %v", got)
	}
	var dump strings.Builder
	for _, event := range events {
		dump.WriteString(event.String())
	}
	if strings.Contains(dump.String(), "vault-token-1") {
		t.Fatal("payment method token leaked into events")
	}
}

func TestAttemptRetryAfterProcessRestartReturnsStoredResultWithoutAnotherProcessorEffect(t *testing.T) {
	stack := newStack(t, true)
	payment := mustCreate(t, stack, "key-1", "order-1")
	first := mustStart(t, stack, payment, "attempt-1", "vault-token")

	restarted := stack.Restart(t, testsupport.StackOptions{Config: shortConfig(), WithRedis: true})
	restarted.FlushRedis(t)
	second, err := start(restarted, payment, "attempt-1", "vault-token")
	if err != nil || second.String() != first.String() {
		t.Fatalf("result after restart = %v, %v; want %v", second, err, first)
	}
	if restarted.Processor.TotalSaleCalls() != 0 || len(restarted.Fraud.Requests()) != 0 {
		t.Fatal("restarted instance re-ran an effect")
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "succeeded")
}

func TestAttemptIsRefusedWhileAnotherIsActiveOrThePaymentIsNotOpen(t *testing.T) {
	stack := newStack(t, false)
	pending := mustCreate(t, stack, "key-1", "order-1")
	stack.Processor.SetBehavior(testsupport.SaleTimeout)
	if result := mustStart(t, stack, pending, "attempt-1", "vault-token"); result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING {
		t.Fatalf("result = %v, want pending", result)
	}
	if _, err := start(stack, pending, "attempt-2", "vault-token"); err == nil || !strings.Contains(err.Error(), application.ErrAttemptActive.Error()) {
		t.Fatalf("err = %v, want attempt active", err)
	}

	stack.Processor.SetBehavior(testsupport.SaleApprove)
	done := mustCreate(t, stack, "key-2", "order-2")
	mustStart(t, stack, done, "attempt-1", "vault-token")
	if _, err := start(stack, done, "attempt-2", "vault-token"); err == nil || !strings.Contains(err.Error(), application.ErrPaymentNotOpen.Error()) {
		t.Fatalf("err = %v, want not open", err)
	}

	expired := mustCreate(t, stack, "key-3", "order-3")
	stack.Clock.Advance(16 * time.Minute)
	if _, err := start(stack, expired, "attempt-1", "vault-token"); err == nil {
		t.Fatal("attempt on an expired payment must be refused")
	}
}

func TestRiskDeclineEndsTheAttemptWithoutCallingTheProcessor(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetAssessment(testsupport.Decline("VELOCITY"))
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED || result.GetDeclineReason() != "velocity" {
		t.Fatalf("result = %v", result)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "declined")
	declined := stack.DB.Events(t, payment.GetPaymentId())[3].GetAttemptDeclined()
	if declined.GetSource() != paymentsv1.DeclineSource_DECLINE_SOURCE_RISK {
		t.Fatalf("declined = %v", declined)
	}
	if stack.Processor.TotalSaleCalls() != 0 {
		t.Fatal("processor called after a risk decline")
	}

	stack.Fraud.SetAssessment(testsupport.Approve())
	if result := mustStart(t, stack, payment, "attempt-2", "vault-token"); result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED {
		t.Fatalf("second attempt after a decline = %v", result)
	}
}

func TestApproveWithoutReasonCodesIsAcceptedAndReachesTheProcessor(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetAssessment(&paymentsv1.RiskAssessment{Outcome: paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE, PolicyVersion: "rules-v1"})
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED {
		t.Fatalf("result = %v", result)
	}
	assessment := stack.DB.Events(t, payment.GetPaymentId())[2].GetRiskDecisionRecorded().GetAssessment()
	if assessment.GetOutcome() != paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE || assessment.GetPolicyVersion() != "rules-v1" || len(assessment.GetReasonCodes()) != 0 {
		t.Fatalf("assessment = %v", assessment)
	}
	if stack.Processor.TotalSaleCalls() != 1 {
		t.Fatalf("sale calls = %d, want 1", stack.Processor.TotalSaleCalls())
	}
}

func TestDeclineWithoutReasonCodesUsesAGenericReasonWithoutCallingTheProcessor(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetAssessment(&paymentsv1.RiskAssessment{Outcome: paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE, PolicyVersion: "rules-v1"})
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED || result.GetDeclineReason() != "risk_declined" {
		t.Fatalf("result = %v", result)
	}
	if stack.Processor.TotalSaleCalls() != 0 {
		t.Fatal("risk decline invoked the processor")
	}
}

func TestFraudTimeoutFailsClosedWithARecordedFallbackDecision(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetHandler(func(ctx context.Context, _ *fraudRequest) (*fraudResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED || result.GetDeclineReason() != "fraud_unavailable" {
		t.Fatalf("result = %v", result)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "declined")
	assessment := stack.DB.Events(t, payment.GetPaymentId())[2].GetRiskDecisionRecorded().GetAssessment()
	if assessment.GetOutcome() != paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE || assessment.GetPolicyVersion() != "payments-fallback-v1" ||
		len(assessment.GetReasonCodes()) != 1 || assessment.GetReasonCodes()[0] != "FRAUD_UNAVAILABLE" {
		t.Fatalf("assessment = %v", assessment)
	}
	if stack.Processor.TotalSaleCalls() != 0 {
		t.Fatal("processor called although fraud was unavailable")
	}
}

func TestFraudErrorFailsClosedToo(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetHandler(func(context.Context, *fraudRequest) (*fraudResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	})
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED || stack.Processor.TotalSaleCalls() != 0 {
		t.Fatalf("result = %v, sales = %d", result, stack.Processor.TotalSaleCalls())
	}
}

func TestProcessorDeclineLeavesThePaymentOpenForAnotherAttempt(t *testing.T) {
	stack := newStack(t, false)
	stack.Processor.SetBehavior(testsupport.SaleDecline)
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED || result.GetDeclineReason() != "insufficient_funds" {
		t.Fatalf("result = %v", result)
	}
	loaded, _ := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{IntegrationId: payment.GetIntegrationId(), PaymentId: payment.GetPaymentId()})
	if loaded.GetStatus() != paymentsv1.PaymentStatus_PAYMENT_STATUS_OPEN {
		t.Fatalf("status = %v, want open", loaded.GetStatus())
	}
	stack.Processor.SetBehavior(testsupport.SaleApprove)
	if result := mustStart(t, stack, payment, "attempt-2", "vault-token"); result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED {
		t.Fatalf("second attempt = %v", result)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "declined", "started", "risk", "requested", "succeeded")
}

func TestProcessorTimeoutRecordsAnUnknownOutcomeAndReturnsPending(t *testing.T) {
	stack := newStack(t, false)
	stack.Processor.SetBehavior(testsupport.SaleTimeout)
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING {
		t.Fatalf("result = %v, want pending", result)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "unknown")
	requested := stack.DB.Events(t, payment.GetPaymentId())[3].GetProcessorOperationRequested()
	if requested.GetOperationKey() != "op-"+result.GetAttemptId() {
		t.Fatalf("operation key = %q", requested.GetOperationKey())
	}
	retry := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if retry.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING || stack.Processor.TotalSaleCalls() != 1 {
		t.Fatalf("retry = %v, sales = %d", retry, stack.Processor.TotalSaleCalls())
	}
}

func TestProcessorUnavailableIsAnUnknownOutcomeNotADecline(t *testing.T) {
	stack := newStack(t, false)
	stack.Processor.SetBehavior(testsupport.SaleUnavailable)
	payment := mustCreate(t, stack, "key-1", "order-1")
	if result := mustStart(t, stack, payment, "attempt-1", "vault-token"); result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING {
		t.Fatalf("result = %v", result)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "unknown")
}

func TestBudgetExhaustedBeforeTheIntentFailsClosed(t *testing.T) {
	stack := testsupport.NewStack(t, testsupport.StackOptions{Config: application.Config{
		PaymentExpiry: 15 * time.Minute, AttemptBudget: 150 * time.Millisecond, FraudTimeout: time.Second, ProcessorTimeout: 3 * time.Second,
	}})
	stack.Fraud.SetHandler(func(ctx context.Context, _ *fraudRequest) (*fraudResponse, error) {
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
		}
		return nil, status.Error(codes.DeadlineExceeded, "slow")
	})
	payment := mustCreate(t, stack, "key-1", "order-1")
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED {
		t.Fatalf("result = %v; a budget exhausted before the intent fails closed", result)
	}
	if stack.Processor.TotalSaleCalls() != 0 {
		t.Fatal("processor called after the budget was exhausted before the intent")
	}
}

func TestConcurrentIdenticalAttemptRequestsProduceOneEffect(t *testing.T) {
	for _, withRedis := range []bool{false, true} {
		t.Run(map[bool]string{false: "postgres only", true: "with redis"}[withRedis], func(t *testing.T) {
			stack := newStack(t, withRedis)
			stack.Processor.SetDelay(150 * time.Millisecond)
			payment := mustCreate(t, stack, "key-1", "order-1")
			results := make([]*paymentsv1.AttemptResult, 6)
			errs := make([]error, len(results))
			var group sync.WaitGroup
			for i := range results {
				group.Add(1)
				go func() {
					defer group.Done()
					results[i], errs[i] = start(stack, payment, "attempt-1", "vault-token")
				}()
			}
			group.Wait()
			for i, err := range errs {
				if err != nil {
					t.Fatalf("request %d: %v", i, err)
				}
				if results[i].GetAttemptId() != results[0].GetAttemptId() {
					t.Fatalf("attempt ids differ: %v", results)
				}
			}
			if stack.Processor.TotalSaleCalls() != 1 || len(stack.Fraud.Requests()) != 1 {
				t.Fatalf("sale calls = %d, fraud calls = %d, want 1 each", stack.Processor.TotalSaleCalls(), len(stack.Fraud.Requests()))
			}
			wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "succeeded")
		})
	}
}

func TestConcurrentIdenticalCreateRequestsYieldOnePayment(t *testing.T) {
	stack := newStack(t, true)
	payments := make([]*paymentsv1.Payment, 6)
	var group sync.WaitGroup
	for i := range payments {
		group.Add(1)
		go func() {
			defer group.Done()
			payment, err := stack.Service.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
			if err != nil {
				t.Errorf("request %d: %v", i, err)
				return
			}
			payments[i] = payment
		}()
	}
	group.Wait()
	for _, payment := range payments {
		if payment == nil || payment.GetPaymentId() != payments[0].GetPaymentId() || payment.GetCheckoutToken() != payments[0].GetCheckoutToken() {
			t.Fatalf("payments differ: %v", payments)
		}
	}
	if got := stack.DB.CountEvents(t); got != 1 {
		t.Fatalf("ledger rows = %d, want 1", got)
	}
}

func TestPaymentQueriesAreScopedToTheOwningIntegration(t *testing.T) {
	stack := newStack(t, false)
	payment := mustCreate(t, stack, "key-1", "order-1")
	other := uuid.MustParse("22222222-89ab-4cde-8fab-0123456789ab")

	if _, err := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{IntegrationId: other.String(), PaymentId: payment.GetPaymentId()}); err == nil || !strings.Contains(err.Error(), application.ErrNotFound.Error()) {
		t.Fatalf("foreign integration err = %v, want not found", err)
	}
	if _, err := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{IntegrationId: testsupport.IntegrationID.String(), PaymentId: uuid.NewString()}); err == nil || !strings.Contains(err.Error(), application.ErrNotFound.Error()) {
		t.Fatalf("unknown payment err = %v, want not found", err)
	}
	stack.Processor.SetBehavior(testsupport.SaleDecline)
	mustStart(t, stack, payment, "attempt-1", "vault-token")

	owned, err := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{IntegrationId: payment.GetIntegrationId(), PaymentId: payment.GetPaymentId()})
	if err != nil {
		t.Fatal(err)
	}
	if owned.GetCheckoutToken() != "" || owned.GetStatus() != paymentsv1.PaymentStatus_PAYMENT_STATUS_OPEN || owned.GetVersion() != 5 ||
		len(owned.GetAttempts()) != 1 || owned.GetMerchant().GetExternalSellerId() != "seller-01" || owned.GetReturnUrl() == "" || owned.GetIntegrationId() == "" {
		t.Fatalf("payment = %v", owned)
	}
}

func TestCheckoutSessionLookupRequiresItsTokenAndNeverEchoesIt(t *testing.T) {
	stack := newStack(t, false)
	payment := mustCreate(t, stack, "key-1", "order-1")
	other := mustCreate(t, stack, "key-2", "order-2")

	session, err := stack.Service.GetCheckoutSession(context.Background(), &paymentsv1.GetCheckoutSessionRequest{SessionId: payment.GetSessionId(), CheckoutToken: payment.GetCheckoutToken()})
	if err != nil || session.GetPaymentId() != payment.GetPaymentId() || session.GetCheckoutToken() != "" || session.GetIntegrationId() != payment.GetIntegrationId() {
		t.Fatalf("session = %v, %v", session, err)
	}
	if _, err := stack.Service.GetCheckoutSession(context.Background(), &paymentsv1.GetCheckoutSessionRequest{SessionId: payment.GetSessionId(), CheckoutToken: other.GetCheckoutToken()}); err == nil {
		t.Fatal("token of another session accepted")
	}
}

func TestBudgetExhaustedDuringTheProcessorCallAfterTheIntentReturnsPending(t *testing.T) {
	stack := testsupport.NewStack(t, testsupport.StackOptions{Config: application.Config{
		PaymentExpiry: 15 * time.Minute, AttemptBudget: 400 * time.Millisecond, FraudTimeout: time.Second, ProcessorTimeout: 3 * time.Second,
	}})
	stack.Processor.SetDelay(2 * time.Second)
	payment := mustCreate(t, stack, "key-1", "order-1")
	started := time.Now()
	result := mustStart(t, stack, payment, "attempt-1", "vault-token")
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING {
		t.Fatalf("result = %v, want pending", result)
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Fatalf("attempt took %v with a 400ms budget", elapsed)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "unknown")
}
