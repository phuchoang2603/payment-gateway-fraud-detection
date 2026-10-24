package grpctransport

import (
	"context"
	"log/slog"
	"math"
	"net"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	fraudv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/fraud/v1"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	fraudv1.UnimplementedFraudServiceServer
	decisions metric.Int64Counter
	reasons   metric.Int64Counter
	latency   metric.Float64Histogram
}

func NewServer() *Server {
	return &Server{decisions: observe.Counter("fraud.decisions", "{decision}"), reasons: observe.Counter("fraud.reason.matches", "{match}"), latency: observe.Histogram("fraud.policy.duration", "s")}
}

func (s *Server) Decide(ctx context.Context, req *fraudv1.DecideRequest) (*fraudv1.DecideResponse, error) {
	input, err := validate(req)
	if err != nil {
		return nil, err
	}
	ctx, span := otel.Tracer("fraud").Start(ctx, "fraud.policy.evaluate")
	defer span.End()
	start := time.Now()
	decision := domain.Evaluate(input)
	outcome := paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE
	label := "approve"
	if decision.Declined {
		outcome = paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE
		label = "decline"
	}
	attrs := metric.WithAttributes(attribute.String("outcome", label), attribute.String("policy.version", decision.PolicyVersion))
	s.latency.Record(ctx, time.Since(start).Seconds(), attrs)
	s.decisions.Add(ctx, 1, attrs)
	for _, reason := range decision.Reasons {
		s.reasons.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason), attribute.String("policy.version", decision.PolicyVersion)))
	}
	span.SetAttributes(attribute.String("fraud.outcome", label), attribute.String("fraud.policy.version", decision.PolicyVersion), attribute.StringSlice("fraud.reasons", decision.Reasons))
	slog.InfoContext(ctx, "fraud decision", "request_id", observe.RequestID(ctx), "outcome", label, "policy_version", decision.PolicyVersion, "reasons", decision.Reasons)
	return &fraudv1.DecideResponse{Assessment: &paymentsv1.RiskAssessment{Outcome: outcome, ReasonCodes: decision.Reasons, PolicyVersion: decision.PolicyVersion}, EvaluatedAt: timestamppb.Now()}, nil
}

func validate(req *fraudv1.DecideRequest) (domain.Input, error) {
	invalid := func() (domain.Input, error) {
		return domain.Input{}, status.Error(codes.InvalidArgument, "invalid fraud decision request")
	}
	if req == nil || req.IntegrationId == "" || req.PaymentId == "" || req.AttemptId == "" {
		return invalid()
	}
	merchant, customer, snapshot, signals := req.Merchant, req.Customer, req.Snapshot, req.Signals
	if merchant == nil || merchant.MerchantId == "" || merchant.ExternalSellerId == "" || customer == nil || customer.CustomerId == "" || customer.ExternalBuyerId == "" || snapshot == nil || signals == nil {
		return invalid()
	}
	if snapshot.Total == nil || snapshot.Total.Currency != "USD" || snapshot.Total.MinorUnits < 0 || snapshot.ExternalOrderId == "" || len(snapshot.Items) == 0 {
		return invalid()
	}
	for _, timestamp := range []*timestamppb.Timestamp{snapshot.MerchantAccountCreatedAt, snapshot.CustomerAccountCreatedAt, signals.AttemptedAt} {
		if timestamp == nil || timestamp.CheckValid() != nil {
			return invalid()
		}
	}
	attempted := signals.AttemptedAt.AsTime()
	if snapshot.MerchantAccountCreatedAt.AsTime().After(attempted) || snapshot.CustomerAccountCreatedAt.AsTime().After(attempted) {
		return invalid()
	}
	shipping, method := snapshot.Shipping, signals.PaymentMethod
	if shipping == nil || shipping.Country == "" || shipping.Region == "" || shipping.PostalCode == "" || shipping.AddressFingerprint == "" || method == nil || method.Type == "" || method.Fingerprint == "" {
		return invalid()
	}
	if signals.IpCountry != nil && *signals.IpCountry == "" {
		return invalid()
	}
	var sum int64
	for _, item := range snapshot.Items {
		if item == nil || item.ProductId == "" || item.Category == "" || item.Condition == "" || item.Quantity <= 0 || item.UnitPrice == nil || item.UnitPrice.MinorUnits < 0 || item.UnitPrice.Currency != "USD" {
			return invalid()
		}
		price, quantity := item.UnitPrice.MinorUnits, int64(item.Quantity)
		if price > math.MaxInt64/quantity {
			return invalid()
		}
		amount := price * quantity
		if sum > math.MaxInt64-amount {
			return invalid()
		}
		sum += amount
	}
	if sum != snapshot.Total.MinorUnits {
		return invalid()
	}
	return domain.Input{AmountMinor: sum, CustomerCreatedAt: snapshot.CustomerAccountCreatedAt.AsTime(), AttemptedAt: attempted, ShippingCountry: shipping.Country, IPCountry: signals.IpCountry}, nil
}

func (s *Server) Run(ctx context.Context, listener net.Listener, timeout time.Duration) error {
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(observe.UnaryServerInterceptor()), grpc.MaxRecvMsgSize(64<<10), grpc.MaxConcurrentStreams(64), grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(observe.RPCFilter))))
	fraudv1.RegisterFraudServiceServer(server, s)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(server, healthServer)
	healthServer.SetServingStatus("liveness", healthpb.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("readiness", healthpb.HealthCheckResponse_SERVING)
	errors := make(chan error, 1)
	go func() { errors <- server.Serve(listener) }()
	var err error
	select {
	case <-ctx.Done():
	case err = <-errors:
	}
	healthServer.SetServingStatus("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	stopped := make(chan struct{})
	go func() { server.GracefulStop(); close(stopped) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-stopped:
	case <-timer.C:
		server.Stop()
	}
	return err
}
