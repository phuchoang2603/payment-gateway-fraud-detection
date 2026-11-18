package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/proto"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

type withHeaders http.Header

func (h withHeaders) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	for name, values := range h {
		request.Header[name] = values
	}
	return http.DefaultTransport.RoundTrip(request)
}

func declinedPayment(t *testing.T) (*sdk.ClientSession, *paymentsv1.Payment, *paymentsv1.AttemptResult) {
	t.Helper()
	stack := testsupport.NewStack(t, testsupport.StackOptions{Config: application.Config{
		PaymentExpiry: 15 * time.Minute, AttemptBudget: 2 * time.Second, FraudTimeout: 300 * time.Millisecond, ProcessorTimeout: 400 * time.Millisecond,
	}})
	stack.Fraud.SetAssessment(testsupport.Decline("HIGH_AMOUNT"))
	payment, err := stack.Service.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatal(err)
	}
	request := testsupport.AttemptRequest(payment, "attempt-1", "vault-token")
	request.Signals.IpCountry = proto.String("GB")
	request.Signals.DeviceFingerprint = proto.String("device-1")
	attempt, err := stack.Service.StartAttempt(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return connect(t, stack, http.Header{scopeHeader: {scopeAll}}), payment, attempt
}

func connect(t *testing.T, stack *testsupport.Stack, header http.Header) *sdk.ClientSession {
	t.Helper()
	server := httptest.NewServer(NewHandler(stack.Service))
	t.Cleanup(server.Close)
	transport := &sdk.StreamableClientTransport{
		Endpoint:             server.URL,
		HTTPClient:           &http.Client{Transport: withHeaders(header)},
		DisableStandaloneSSE: true,
	}
	session, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, nil).Connect(context.Background(), transport, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func call[T any](t *testing.T, session *sdk.ClientSession, tool string, arguments map[string]any) T {
	t.Helper()
	result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: tool, Arguments: arguments})
	if err != nil || result.IsError {
		t.Fatalf("%s: %v %+v", tool, err, result)
	}
	raw, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var output T
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatalf("%s output %s: %v", tool, raw, err)
	}
	return output
}

func TestGetPaymentStatusMapsThePaymentView(t *testing.T) {
	session, payment, attempt := declinedPayment(t)
	output := call[paymentStatusOutput](t, session, "get_payment_status", map[string]any{
		"integration_id": testsupport.IntegrationID.String(), "payment_id": payment.GetPaymentId(),
	})
	if output.PaymentID != payment.GetPaymentId() || output.Status != "open" || output.Version == 0 || len(output.Attempts) != 1 {
		t.Fatalf("output = %+v", output)
	}
	if got := output.Attempts[0]; got.AttemptID != attempt.GetAttemptId() || got.Status != "declined" || got.DeclineReason != "high_amount" {
		t.Fatalf("attempt = %+v", got)
	}
}

func TestGetAttemptRiskInputsMapsTheRecordedInputs(t *testing.T) {
	session, payment, attempt := declinedPayment(t)
	output := call[attemptRiskInputsOutput](t, session, "get_attempt_risk_inputs", map[string]any{
		"integration_id": testsupport.IntegrationID.String(), "payment_id": payment.GetPaymentId(),
	})
	snapshot := payment.GetSnapshot()
	want := riskInputs{
		CustomerID: payment.GetCustomer().GetCustomerId(), PaymentMethodFingerprint: "fingerprint-1",
		AmountMinor: snapshot.GetTotal().GetMinorUnits(), ShippingCountry: snapshot.GetShipping().GetCountry(),
		IPCountry: "GB", ItemCount: len(snapshot.GetItems()), HasDevice: true,
		CustomerAccountCreatedAt: snapshot.GetCustomerAccountCreatedAt().AsTime(), MerchantAccountCreatedAt: snapshot.GetMerchantAccountCreatedAt().AsTime(),
	}
	got := output.RiskInputs
	if !got.CustomerAccountCreatedAt.Equal(want.CustomerAccountCreatedAt) || !got.MerchantAccountCreatedAt.Equal(want.MerchantAccountCreatedAt) {
		t.Fatalf("account creation times = %v and %v, want %v and %v", got.CustomerAccountCreatedAt, got.MerchantAccountCreatedAt, want.CustomerAccountCreatedAt, want.MerchantAccountCreatedAt)
	}
	got.CustomerAccountCreatedAt, got.MerchantAccountCreatedAt = want.CustomerAccountCreatedAt, want.MerchantAccountCreatedAt
	if output.AttemptID != attempt.GetAttemptId() || output.DeclineSource != "risk" || output.DeclineReason != "high_amount" || got != want {
		t.Fatalf("output = %+v, want inputs %+v", output, want)
	}
}

func TestToolsRefuseAnIntegrationOtherThanTheOneBoundByTheGateway(t *testing.T) {
	stack := testsupport.NewStack(t, testsupport.StackOptions{Config: testsupport.DefaultConfig()})
	payment, err := stack.Service.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatal(err)
	}
	session := connect(t, stack, http.Header{scopeHeader: {scopeIntegration}, integrationHeader: {"22222222-89ab-4cde-8fab-0123456789ab"}})
	for _, tool := range []string{"get_payment_status", "get_attempt_risk_inputs"} {
		result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: tool, Arguments: map[string]any{
			"integration_id": testsupport.IntegrationID.String(), "payment_id": payment.GetPaymentId(),
		}})
		if err != nil || !result.IsError {
			t.Fatalf("%s: err = %v, result = %+v; want a tool error", tool, err, result)
		}
	}
}

func TestToolsRefuseARequestWithoutAGatewayScope(t *testing.T) {
	stack := testsupport.NewStack(t, testsupport.StackOptions{Config: testsupport.DefaultConfig()})
	payment, err := stack.Service.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatal(err)
	}
	session := connect(t, stack, nil)
	result, err := session.CallTool(context.Background(), &sdk.CallToolParams{Name: "get_payment_status", Arguments: map[string]any{
		"integration_id": testsupport.IntegrationID.String(), "payment_id": payment.GetPaymentId(),
	}})
	if err != nil || !result.IsError {
		t.Fatalf("err = %v, result = %+v; want a tool error", err, result)
	}
}
