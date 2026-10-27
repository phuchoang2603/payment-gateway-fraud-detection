package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
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
