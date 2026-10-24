package grpctransport

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc/metadata"

	fraudv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/fraud/v1"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func validRequest() *fraudv1.DecideRequest {
	now := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	return &fraudv1.DecideRequest{
		IntegrationId: "synthetic-v1", PaymentId: "payment-1", AttemptId: "attempt-1",
		Merchant: &paymentsv1.MerchantRef{MerchantId: "merchant-1", ExternalSellerId: "seller-1"},
		Customer: &paymentsv1.CustomerRef{CustomerId: "customer-1", ExternalBuyerId: "buyer-1"},
		Snapshot: &paymentsv1.CheckoutSnapshot{
			ExternalOrderId: "order-1", Total: &paymentsv1.Money{MinorUnits: 15000, Currency: "USD"},
			Items:                    []*paymentsv1.LineItem{{ProductId: "product-1", Category: "electronics", Condition: "used", Quantity: 1, UnitPrice: &paymentsv1.Money{MinorUnits: 15000, Currency: "USD"}}},
			MerchantAccountCreatedAt: timestamppb.New(now.Add(-30 * 24 * time.Hour)), CustomerAccountCreatedAt: timestamppb.New(now.Add(-30 * 24 * time.Hour)),
			Shipping: &paymentsv1.ShippingLocation{Country: "US", Region: "NY", PostalCode: "10001", AddressFingerprint: "address-hash"},
		},
		Signals: &paymentsv1.AttemptSignals{AttemptedAt: timestamppb.New(now), PaymentMethod: &paymentsv1.PaymentMethod{Type: "card", Fingerprint: "method-hash"}},
	}
}

func TestRejectsInvalidSnapshotAndSignals(t *testing.T) {
	for name, mutate := range map[string]func(*fraudv1.DecideRequest){
		"integration": func(r *fraudv1.DecideRequest) { r.IntegrationId = "" },
		"payment":     func(r *fraudv1.DecideRequest) { r.PaymentId = "" },
		"attempt":     func(r *fraudv1.DecideRequest) { r.AttemptId = "" },
		"total":       func(r *fraudv1.DecideRequest) { r.Snapshot.Total = nil },
		"currency":    func(r *fraudv1.DecideRequest) { r.Snapshot.Total.Currency = "EUR" },
		"sum":         func(r *fraudv1.DecideRequest) { r.Snapshot.Total.MinorUnits++ },
		"timestamp":   func(r *fraudv1.DecideRequest) { r.Signals.AttemptedAt = &timestamppb.Timestamp{Seconds: math.MaxInt64} },
		"future account": func(r *fraudv1.DecideRequest) {
			r.Snapshot.CustomerAccountCreatedAt = timestamppb.New(r.Signals.AttemptedAt.AsTime().Add(time.Second))
		},
		"empty IP":        func(r *fraudv1.DecideRequest) { v := ""; r.Signals.IpCountry = &v },
		"negative amount": func(r *fraudv1.DecideRequest) { r.Snapshot.Total.MinorUnits = -1 },
		"missing method":  func(r *fraudv1.DecideRequest) { r.Signals.PaymentMethod = nil },
		"overflow": func(r *fraudv1.DecideRequest) {
			r.Snapshot.Items[0].UnitPrice.MinorUnits = math.MaxInt64
			r.Snapshot.Items[0].Quantity = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := validRequest()
			mutate(r)
			_, err := validate(r)
			if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != "invalid fraud decision request" {
				t.Fatalf("unsafe validation error: %v", err)
			}
		})
	}
}

func TestDeclineSucceedsAndCountsDecisionOnce(t *testing.T) {
	reader := telemetry.Metrics(t)
	provider, spans := telemetry.Traces(t)
	server := NewServer()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx, listener, 100*time.Millisecond) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("fraud shutdown was not bounded")
		}
	})
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	call, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	for _, name := range []string{"liveness", "readiness"} {
		res, err := healthpb.NewHealthClient(conn).Check(call, &healthpb.HealthCheckRequest{Service: name})
		if err != nil || res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("health %s: %v %v", name, res, err)
		}
	}
	req := validRequest()
	req.Snapshot.Total.MinorUnits = 200001
	req.Snapshot.Items[0].UnitPrice.MinorUnits = 200001
	req.Snapshot.CustomerAccountCreatedAt = req.Signals.AttemptedAt
	country := "CA"
	req.Signals.IpCountry = &country
	parentCtx, parent := provider.Tracer("caller").Start(call, "payment attempt")
	defer parent.End()
	parentCtx = metadata.AppendToOutgoingContext(parentCtx, "x-request-id", "request-123")
	var headers metadata.MD
	res, err := fraudv1.NewFraudServiceClient(conn).Decide(parentCtx, req, grpc.Header(&headers))
	if err != nil {
		t.Fatal(err)
	}
	if got := headers.Get("x-request-id"); len(got) != 1 || got[0] != "request-123" {
		t.Fatalf("response request ID: %v", got)
	}
	connected := false
	for _, span := range spans.Ended() {
		if span.Name() == "fraud.policy.evaluate" && span.SpanContext().TraceID() == parent.SpanContext().TraceID() {
			connected = true
		}
	}
	if !connected {
		t.Fatal("Payments-to-fraud trace context was lost")
	}
	a := res.Assessment
	if a.Outcome != paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE || len(a.ReasonCodes) != 2 || a.PolicyVersion != "rules-v1" || a.ModelVersion != nil || a.FeatureSetVersion != nil || a.RiskScore != nil {
		t.Fatalf("assessment: %v", a)
	}
	sums := telemetry.Sums(t, reader)
	if sums["fraud.decisions"] != 1 || sums["fraud.reason.matches"] != 2 {
		t.Fatalf("decision/reason measurements: %v", sums)
	}
}
