package application_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLateAssessmentCannotAuthorizeAnExpiredPayment(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetHandler(func(context.Context, *fraudRequest) (*fraudResponse, error) {
		stack.Clock.Advance(16 * time.Minute)
		return &fraudResponse{Assessment: testsupport.Approve(), EvaluatedAt: timestamppb.Now()}, nil
	})
	payment := mustCreate(t, stack, "key", "order")
	_, err := stack.Service.StartAttempt(context.Background(), testsupport.AttemptRequest(payment, "attempt", "vault-token"))
	if !errors.Is(err, application.ErrPaymentNotOpen) {
		t.Fatalf("late result: %v", err)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "expired")
	if stack.Processor.TotalSaleCalls() != 0 {
		t.Fatal("late assessment authorized processor effects")
	}
	view, err := stack.Service.GetPayment(context.Background(), &paymentsv1.GetPaymentRequest{PaymentId: payment.GetPaymentId(), IntegrationId: payment.GetIntegrationId()})
	if err != nil || view.GetAttempts()[0].GetAssessmentStatus() != paymentsv1.AssessmentStatus_ASSESSMENT_STATUS_EXPIRED {
		t.Fatalf("expired workflow: %v %v", view, err)
	}
}

type lostDecisionResponse struct {
	application.Store
	lost bool
}

func (s *lostDecisionResponse) Transition(ctx context.Context, request application.TransitionRequest) (domain.State, error) {
	state, err := s.Store.Transition(ctx, request)
	if _, decision := request.Command.(domain.RecordRiskDecision); err == nil && decision && !s.lost {
		s.lost = true
		return domain.State{}, errors.New("commit response lost")
	}
	return state, err
}

type recoveryDependencies struct{ decisions, sales int }

func (d *recoveryDependencies) Decide(context.Context, application.RiskInput) (*paymentsv1.RiskAssessment, error) {
	d.decisions++
	return testsupport.Approve(), nil
}

func (d *recoveryDependencies) Sale(_ context.Context, request application.SaleRequest) (application.Operation, error) {
	d.sales++
	return application.Operation{OperationKey: request.OperationKey, Outcome: application.OutcomeApproved, ProcessorReference: "approved"}, nil
}

func (*recoveryDependencies) GetOperation(context.Context, string) (application.Operation, error) {
	return application.Operation{}, application.ErrOperationNotFound
}

func TestLostDecisionCommitResponseRecoversWithoutRescoringOrDuplicateEffects(t *testing.T) {
	stack := newStack(t, false)
	store := &lostDecisionResponse{Store: stack.Store}
	dependencies := &recoveryDependencies{}
	stack.Service = application.NewService(shortConfig(), application.Dependencies{
		Store: store, Fraud: dependencies, Processor: dependencies, Sealer: stack.Sealer,
		Tokens: stack.Tokens, Metrics: stack.Metrics, Clock: stack.Clock.Now,
	})
	payment := mustCreate(t, stack, "create", "order")
	request := testsupport.AttemptRequest(payment, "attempt", "vault-token")
	if _, err := stack.Service.StartAttempt(context.Background(), request); err == nil || !store.lost {
		t.Fatalf("expected lost commit response: %v", err)
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk")
	if dependencies.sales != 0 {
		t.Fatal("processor called after ambiguous commit response")
	}
	stack.Clock.Advance(5 * time.Second)
	for range 2 {
		result, err := stack.Service.StartAttempt(context.Background(), request)
		if err != nil || result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED {
			t.Fatalf("retry: %v %v", result, err)
		}
	}
	wantKinds(t, kinds(t, stack, payment), "created", "started", "risk", "requested", "succeeded")
	if dependencies.decisions != 1 || dependencies.sales != 1 {
		t.Fatalf("decisions=%d sales=%d", dependencies.decisions, dependencies.sales)
	}
	assessment := stack.DB.Events(t, payment.GetPaymentId())[2].GetRiskDecisionRecorded().GetAssessment()
	if assessment.GetAssessmentId() != "test-assessment" || assessment.GetEvidenceSha256() != strings.Repeat("a", 64) {
		t.Fatalf("lost evidence linkage: %v", assessment)
	}
}
