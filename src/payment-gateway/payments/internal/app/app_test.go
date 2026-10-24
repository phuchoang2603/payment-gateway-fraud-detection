package app_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/telemetry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/app"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/testsupport"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

const closedAddr = "127.0.0.1:1"

type running struct {
	conn     *grpc.ClientConn
	cancel   context.CancelFunc
	done     chan struct{}
	payments paymentsv1.PaymentServiceClient
	health   healthpb.HealthClient
}

func testConfig(databaseURL, fraudAddr, processorAddr, redisAddr string) config.Config {
	return config.Config{
		GRPCAddr: "127.0.0.1:0", ShutdownTimeout: 500 * time.Millisecond,
		DatabaseURL: databaseURL, RedisAddr: redisAddr, RedisTimeout: 50 * time.Millisecond,
		FraudAddr: fraudAddr, ProcessorAddr: processorAddr,
		EncryptionKey: []byte("0123456789abcdef0123456789abcdef"), CheckoutTokenKey: []byte("checkout-token-key-for-tests-0123456789"),
		IdempotencyTTL: time.Hour, PaymentExpiry: 15 * time.Minute, AttemptBudget: 5 * time.Second,
		FraudTimeout: time.Second, ProcessorTimeout: 3 * time.Second, RecoveryGrace: time.Hour, WorkerInterval: time.Hour,
	}
}

func startApp(t *testing.T, cfg config.Config) *running {
	t.Helper()
	application, err := app.New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = application.Serve(ctx, grpcListener)
	}()
	conn, err := grpc.NewClient(grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	instance := &running{
		conn: conn, cancel: cancel, done: done,
		payments: paymentsv1.NewPaymentServiceClient(conn), health: healthpb.NewHealthClient(conn),
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("service did not stop")
		}
		_ = conn.Close()
	})
	return instance
}

func (r *running) status(t *testing.T, service string) healthpb.HealthCheckResponse_ServingStatus {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := r.health.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		t.Fatalf("health check %s: %v", service, err)
	}
	return response.GetStatus()
}

func (r *running) waitStatus(t *testing.T, service string, want healthpb.HealthCheckResponse_ServingStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if r.status(t, service) == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("%s did not become %v (is %v)", service, want, r.status(t, service))
}

func TestReadinessIsNotServingWhileTheDatabaseIsDownButLivenessStaysServing(t *testing.T) {
	instance := startApp(t, testConfig("postgres://payments_app:x@127.0.0.1:1/payments?sslmode=disable&connect_timeout=1", closedAddr, closedAddr, closedAddr))
	time.Sleep(300 * time.Millisecond)
	if got := instance.status(t, "liveness"); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness = %v", got)
	}
	if got := instance.status(t, "readiness"); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness = %v, want NOT_SERVING", got)
	}
}

func TestReadinessIsServingWithRedisAndFraudAndProcessorDown(t *testing.T) {
	db := testsupport.NewDatabase(t)
	instance := startApp(t, testConfig(db.AppURL, closedAddr, closedAddr, closedAddr))
	instance.waitStatus(t, "readiness", healthpb.HealthCheckResponse_SERVING)
	if got := instance.status(t, "liveness"); got != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness = %v", got)
	}

	response, err := instance.payments.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatalf("create with redis down: %v", err)
	}
	attempt, err := instance.payments.StartAttempt(context.Background(), testsupport.AttemptRequest(response, "attempt-1", "vault-token"))
	if err != nil || attempt.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED || attempt.GetDeclineReason() != "fraud_unavailable" {
		t.Fatalf("attempt with fraud down = %v, %v; want a fail-closed decline", attempt, err)
	}
}

func TestReadinessTurnsNotServingWhenMigrationsAreMissing(t *testing.T) {
	db := testsupport.NewDatabase(t)
	if _, err := db.Owner.Exec(context.Background(), `UPDATE goose_db_version SET is_applied = false WHERE version_id = (SELECT max(version_id) FROM goose_db_version)`); err != nil {
		t.Fatal(err)
	}
	instance := startApp(t, testConfig(db.AppURL, closedAddr, closedAddr, closedAddr))
	time.Sleep(300 * time.Millisecond)
	if got := instance.status(t, "readiness"); got != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness = %v, want NOT_SERVING", got)
	}
}

func TestGRPCErrorMappingAndCorrelationPropagation(t *testing.T) {
	db := testsupport.NewDatabase(t)
	fraud, _ := testsupport.NewFakeFraud(t)
	processor, _ := testsupport.NewFakeProcessor(t)
	instance := startApp(t, testConfig(db.AppURL, fraud.Addr, processor.Addr, closedAddr))
	instance.waitStatus(t, "readiness", healthpb.HealthCheckResponse_SERVING)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-request-id", "request-abc-123")

	payment, err := instance.payments.CreatePayment(ctx, testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatal(err)
	}
	if events := db.Events(t, payment.GetPaymentId()); events[0].GetCorrelationId() != "request-abc-123" {
		t.Fatalf("correlation id = %q", events[0].GetCorrelationId())
	}
	generated, err := instance.payments.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-2", "order-2", 12500))
	if err != nil {
		t.Fatal(err)
	}
	if id := db.Events(t, generated.GetPaymentId())[0].GetCorrelationId(); len(id) < 16 {
		t.Fatalf("generated correlation id = %q", id)
	}
	if got := fraud.Requests(); len(got) != 0 {
		t.Fatalf("fraud called by creation: %v", got)
	}

	changed := testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 99999)
	invalid := testsupport.CreateRequest(testsupport.IntegrationID, "key-3", "order-3", 12500)
	invalid.Snapshot.Items = nil
	foreign := &paymentsv1.GetPaymentRequest{IntegrationId: "22222222-89ab-4cde-8fab-0123456789ab", PaymentId: payment.GetPaymentId()}
	badToken := testsupport.AttemptRequest(payment, "attempt-1", "vault-token")
	badToken.CheckoutToken = "nope"
	if _, err := instance.payments.StartAttempt(ctx, testsupport.AttemptRequest(payment, "attempt-1", "vault-token")); err != nil {
		t.Fatal(err)
	}
	alreadyDone := testsupport.AttemptRequest(payment, "attempt-2", "vault-token")

	tests := []struct {
		name string
		call func() error
		want codes.Code
	}{
		{"idempotency conflict", func() error { _, err := instance.payments.CreatePayment(ctx, changed); return err }, codes.AlreadyExists},
		{"order reference conflict", func() error {
			_, err := instance.payments.CreatePayment(ctx, testsupport.CreateRequest(testsupport.IntegrationID, "key-9", "order-1", 12500))
			return err
		}, codes.AlreadyExists},
		{"invalid snapshot", func() error { _, err := instance.payments.CreatePayment(ctx, invalid); return err }, codes.InvalidArgument},
		{"foreign integration", func() error { _, err := instance.payments.GetPayment(ctx, foreign); return err }, codes.NotFound},
		{"bad checkout token", func() error { _, err := instance.payments.StartAttempt(ctx, badToken); return err }, codes.PermissionDenied},
		{"payment not open", func() error { _, err := instance.payments.StartAttempt(ctx, alreadyDone); return err }, codes.FailedPrecondition},
		{"bad session lookup", func() error {
			_, err := instance.payments.GetCheckoutSession(ctx, &paymentsv1.GetCheckoutSessionRequest{SessionId: payment.GetSessionId(), CheckoutToken: "nope"})
			return err
		}, codes.PermissionDenied},
	}
	for _, test := range tests {
		if got := status.Code(test.call()); got != test.want {
			t.Errorf("%s: code = %v, want %v", test.name, got, test.want)
		}
	}
	if got := fraud.Requests(); len(got) != 1 || processor.TotalSaleCalls() != 1 {
		t.Fatalf("fraud calls = %d, sale calls = %d", len(got), processor.TotalSaleCalls())
	}
	if md := fraud.Requests()[0]; md.GetPaymentId() != payment.GetPaymentId() {
		t.Fatalf("fraud request = %v", md)
	}
}

func TestOTLPMetricsRecordFraudDecline(t *testing.T) {
	reader := telemetry.Metrics(t)
	db := testsupport.NewDatabase(t)
	fraud, _ := testsupport.NewFakeFraud(t)
	processor, _ := testsupport.NewFakeProcessor(t)
	instance := startApp(t, testConfig(db.AppURL, fraud.Addr, processor.Addr, closedAddr))
	instance.waitStatus(t, "readiness", healthpb.HealthCheckResponse_SERVING)
	fraud.SetAssessment(testsupport.Decline("VELOCITY"))
	payment, err := instance.payments.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.payments.StartAttempt(context.Background(), testsupport.AttemptRequest(payment, "attempt-1", "vault-token")); err != nil {
		t.Fatal(err)
	}

	values := telemetry.Sums(t, reader)
	if values["payments.attempt.failures"] != 1 {
		t.Fatalf("failure metrics = %v", values)
	}
}

func TestShutdownIsBoundedWhileARequestIsStillRunning(t *testing.T) {
	db := testsupport.NewDatabase(t)
	fraud, _ := testsupport.NewFakeFraud(t)
	processor, _ := testsupport.NewFakeProcessor(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	fraud.SetHandler(func(ctx context.Context, _ *testsupportRequest) (*testsupportResponse, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, status.Error(codes.Unavailable, "stopped")
	})
	cfg := testConfig(db.AppURL, fraud.Addr, processor.Addr, closedAddr)
	cfg.FraudTimeout, cfg.AttemptBudget = time.Minute, time.Minute
	instance := startApp(t, cfg)
	instance.waitStatus(t, "readiness", healthpb.HealthCheckResponse_SERVING)
	payment, err := instance.payments.CreatePayment(context.Background(), testsupport.CreateRequest(testsupport.IntegrationID, "key-1", "order-1", 12500))
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_, _ = instance.payments.StartAttempt(context.Background(), testsupport.AttemptRequest(payment, "attempt-1", "vault-token"))
	}()
	testsupport.WaitFor(t, 5*time.Second, func() bool { return len(fraud.Requests()) == 1 })

	started := time.Now()
	instance.cancel()
	select {
	case <-instance.done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown was not bounded")
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("shutdown took %v with a 500ms bound", elapsed)
	}
}
