package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
)

func TestAttemptRiskInputsRebuildARiskDeclinedAttempt(t *testing.T) {
	stack := newStack(t, false)
	stack.Fraud.SetAssessment(testsupport.Decline("HIGH_AMOUNT"))
	payment := mustCreate(t, stack, "key-1", "order-1")
	request := testsupport.AttemptRequest(payment, "attempt-1", "vault-token")
	request.Signals.IpCountry = proto.String("GB")
	request.Signals.DeviceFingerprint = proto.String("device-1")
	declined, err := stack.Service.StartAttempt(context.Background(), request)
	if err != nil {
		t.Fatalf("start attempt: %v", err)
	}

	for _, attemptID := range []string{"", declined.GetAttemptId()} {
		inputs, err := stack.Service.AttemptRiskInputs(context.Background(), application.AttemptRiskInputsQuery{
			IntegrationID: testsupport.IntegrationID.String(), PaymentID: payment.GetPaymentId(), AttemptID: attemptID,
		})
		if err != nil {
			t.Fatalf("risk inputs for %q: %v", attemptID, err)
		}
		snapshot := payment.GetSnapshot()
		if inputs.AttemptID != declined.GetAttemptId() || inputs.CustomerID != payment.GetCustomer().GetCustomerId() ||
			inputs.PaymentMethodFingerprint != "fingerprint-1" || inputs.AmountMinor != snapshot.GetTotal().GetMinorUnits() ||
			inputs.ItemCount != len(snapshot.GetItems()) || inputs.ShippingCountry != snapshot.GetShipping().GetCountry() ||
			inputs.IPCountry != "GB" || !inputs.HasDevice ||
			!inputs.CustomerAccountCreatedAt.Equal(snapshot.GetCustomerAccountCreatedAt().AsTime()) ||
			!inputs.MerchantAccountCreatedAt.Equal(snapshot.GetMerchantAccountCreatedAt().AsTime()) ||
			inputs.DeclineSource != "risk" || inputs.DeclineReason != "high_amount" {
			t.Fatalf("inputs = %+v", inputs)
		}
	}
}

func TestAttemptRiskInputsAnswerOnlyForTheOwningIntegration(t *testing.T) {
	stack := newStack(t, false)
	payment := mustCreate(t, stack, "key-1", "order-1")
	mustStart(t, stack, payment, "attempt-1", "vault-token")

	for name, query := range map[string]application.AttemptRiskInputsQuery{
		"another integration": {IntegrationID: uuid.NewString(), PaymentID: payment.GetPaymentId()},
		"unknown attempt":     {IntegrationID: testsupport.IntegrationID.String(), PaymentID: payment.GetPaymentId(), AttemptID: uuid.NewString()},
	} {
		if _, err := stack.Service.AttemptRiskInputs(context.Background(), query); !errors.Is(err, application.ErrNotFound) {
			t.Fatalf("%s: error = %v, want not found", name, err)
		}
	}
}
