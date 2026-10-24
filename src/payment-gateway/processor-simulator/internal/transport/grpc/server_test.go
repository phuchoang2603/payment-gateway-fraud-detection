package grpctransport_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/adapters/vault"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/domain"
	grpctransport "github.com/phuchoang2603/payment-gateway-fraud-detection/processor-simulator/internal/transport/grpc"
	processorv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/processor/v1"
	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
	testpostgres "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/postgres"
)

const (
	integration      = "01234567-89ab-4cde-8fab-0123456789ab"
	otherIntegration = "11111111-89ab-4cde-8fab-0123456789ab"
	tokenApprove     = "vt_approve_token_000000001"
	tokenNSF         = "vt_nsf_token_00000000000002"
	tokenIssuer      = "vt_issuer_token_000000000003"
	tokenInvalid     = "vt_invalid_token_00000000004"
)

type fakeVault struct {
	vaultv1.UnimplementedProcessingServiceServer
	calls    atomic.Int64
	fixtures map[string]vaultv1.Behavior
	server   *grpc.Server
}

func (f *fakeVault) ResolveForProcessing(ctx context.Context, req *vaultv1.ResolveRequest) (*vaultv1.ProcessingFixture, error) {
	f.calls.Add(1)
	behavior, ok := f.fixtures[req.GetIntegrationId()+"/"+req.GetToken()]
	if !ok {
		return nil, status.Error(codes.NotFound, "token not found")
	}
	return &vaultv1.ProcessingFixture{CardReference: "card-0001", Behavior: behavior}, nil
}

func startFakeVault(t *testing.T) (*fakeVault, config.Vault) {
	t.Helper()
	fake := &fakeVault{fixtures: map[string]vaultv1.Behavior{
		integration + "/" + tokenApprove: vaultv1.Behavior_BEHAVIOR_APPROVE,
		integration + "/" + tokenNSF:     vaultv1.Behavior_BEHAVIOR_INSUFFICIENT_FUNDS,
		integration + "/" + tokenIssuer:  vaultv1.Behavior_BEHAVIOR_ISSUER_DECLINE,
		integration + "/" + tokenInvalid: vaultv1.Behavior_BEHAVIOR_INVALID,
	}}
	fake.server = grpc.NewServer()
	vaultv1.RegisterProcessingServiceServer(fake.server, fake)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = fake.server.Serve(listener) }()
	t.Cleanup(fake.server.Stop)
	return fake, config.Vault{
		Address: listener.Addr().String(),
		Timeout: time.Second,
	}
}

type harness struct {
	client      processorv1.ProcessorServiceClient
	conn        *grpc.ClientConn
	vault       *fakeVault
	databaseURL string
	pool        *pgxpool.Pool
}

type harnessOptions struct {
	faults domain.Faults
	hold   time.Duration
}

func newHarness(t *testing.T, options harnessOptions) *harness {
	t.Helper()
	databaseURL := testpostgres.SetupPostgres(t)
	ctx := context.Background()
	if err := postgres.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	store, err := postgres.NewStore(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	fake, vaultConfig := startFakeVault(t)
	vaultClient, err := vault.NewClient(vaultConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = vaultClient.Close() })

	hold := options.hold
	if hold == 0 {
		hold = 5 * time.Second
	}
	service := application.NewService(store, vaultClient, options.faults, hold)
	server := grpctransport.NewServer(grpctransport.Options{ShutdownTimeout: 2 * time.Second, ReadinessInterval: 100 * time.Millisecond}, service, store)
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(runCtx, grpcListener) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	conn, err := grpc.NewClient(grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return &harness{
		client:      processorv1.NewProcessorServiceClient(conn),
		conn:        conn,
		vault:       fake,
		databaseURL: databaseURL,
		pool:        pool,
	}
}

func (h *harness) sale(t *testing.T, key, token string, amount int64) (*processorv1.Operation, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.client.Sale(ctx, &processorv1.SaleRequest{
		OperationKey:       key,
		IntegrationId:      integration,
		PaymentMethodToken: token,
		AmountMinorUnits:   amount,
		Currency:           "USD",
	})
}

func (h *harness) operationCount(t *testing.T) int {
	t.Helper()
	var count int
	if err := h.pool.QueryRow(context.Background(), "SELECT count(*) FROM operations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestSaleBehaviorsMapToOutcomesEveryTime(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	tests := []struct {
		token   string
		outcome processorv1.OperationOutcome
		reason  string
	}{
		{tokenApprove, processorv1.OperationOutcome_OPERATION_OUTCOME_APPROVED, ""},
		{tokenNSF, processorv1.OperationOutcome_OPERATION_OUTCOME_DECLINED, "INSUFFICIENT_FUNDS"},
		{tokenIssuer, processorv1.OperationOutcome_OPERATION_OUTCOME_DECLINED, "ISSUER_DECLINED"},
		{tokenInvalid, processorv1.OperationOutcome_OPERATION_OUTCOME_DECLINED, "INVALID_CARD"},
	}
	for index, test := range tests {
		for attempt := range 2 {
			key := fmt.Sprintf("op-%d-%d", index, attempt)
			operation, err := h.sale(t, key, test.token, 1000)
			if err != nil {
				t.Fatal(err)
			}
			if operation.GetOutcome() != test.outcome || operation.GetDeclineReason() != test.reason || operation.GetOperationKey() != key {
				t.Fatalf("%s: got %v", test.token, operation)
			}
			if len(operation.GetProcessorReference()) < 10 || operation.GetProcessorReference()[:5] != "proc_" || operation.GetRecordedAt().AsTime().IsZero() {
				t.Fatalf("missing generated reference or timestamp: %v", operation)
			}
		}
	}
}

func TestSameKeyReplayReturnsOriginalOutcomeWithoutVaultAccess(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	first, err := h.sale(t, "op-replay", tokenNSF, 2500)
	if err != nil {
		t.Fatal(err)
	}
	callsAfterFirst := h.vault.calls.Load()
	for range 3 {
		again, err := h.sale(t, "op-replay", tokenNSF, 2500)
		if err != nil {
			t.Fatal(err)
		}
		if again.GetProcessorReference() != first.GetProcessorReference() || again.GetDeclineReason() != "INSUFFICIENT_FUNDS" || !again.GetRecordedAt().AsTime().Equal(first.GetRecordedAt().AsTime()) {
			t.Fatalf("replay differs: %v vs %v", again, first)
		}
	}
	if h.vault.calls.Load() != callsAfterFirst || callsAfterFirst != 1 {
		t.Fatalf("vault calls: first=%d total=%d", callsAfterFirst, h.vault.calls.Load())
	}
	if h.operationCount(t) != 1 {
		t.Fatalf("expected one recorded sale, got %d", h.operationCount(t))
	}
}

func TestSameKeyDifferentRequestIsRejectedWithAlreadyExists(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	first, err := h.sale(t, "op-conflict", tokenApprove, 1000)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	variants := map[string]*processorv1.SaleRequest{
		"amount":      {OperationKey: "op-conflict", IntegrationId: integration, PaymentMethodToken: tokenApprove, AmountMinorUnits: 1001, Currency: "USD"},
		"token":       {OperationKey: "op-conflict", IntegrationId: integration, PaymentMethodToken: tokenNSF, AmountMinorUnits: 1000, Currency: "USD"},
		"currency":    {OperationKey: "op-conflict", IntegrationId: integration, PaymentMethodToken: tokenApprove, AmountMinorUnits: 1000, Currency: "EUR"},
		"integration": {OperationKey: "op-conflict", IntegrationId: otherIntegration, PaymentMethodToken: tokenApprove, AmountMinorUnits: 1000, Currency: "USD"},
	}
	for name, request := range variants {
		_, err := h.client.Sale(ctx, request)
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("%s: got %v, want AlreadyExists", name, err)
		}
	}
	original, err := h.client.GetOperation(ctx, &processorv1.GetOperationRequest{OperationKey: "op-conflict"})
	if err != nil || original.GetProcessorReference() != first.GetProcessorReference() {
		t.Fatalf("original outcome changed: %v %v", original, err)
	}
	if h.operationCount(t) != 1 {
		t.Fatal("conflicting request recorded a sale")
	}
}

func TestConcurrentSameKeyRecordsOneSale(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	references := make(chan string, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			operation, err := h.sale(t, "op-concurrent", tokenApprove, 700)
			if err != nil {
				t.Error(err)
				return
			}
			references <- operation.GetProcessorReference()
		}()
	}
	wg.Wait()
	close(references)
	seen := map[string]bool{}
	for reference := range references {
		seen[reference] = true
	}
	if len(seen) != 1 || h.operationCount(t) != 1 {
		t.Fatalf("references=%v rows=%d", seen, h.operationCount(t))
	}
}

func TestRawCardCredentialsAreRejectedWithoutRecordingASale(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	for _, token := range []string{"4242424242424242", "4242 4242 4242 4242", "tok_card-0001_ok", "vt_", "vt_4242 4242 4242 4242", "", "pm_1234567890123456789012"} {
		_, err := h.sale(t, "op-raw", token, 1000)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%q: got %v, want InvalidArgument", token, err)
		}
		if message := status.Convert(err).Message(); token != "" && len(token) > 4 && strings.Contains(message, token) {
			t.Fatalf("error echoes the credential: %q", message)
		}
	}
	if h.operationCount(t) != 0 || h.vault.calls.Load() != 0 {
		t.Fatalf("rows=%d vault calls=%d", h.operationCount(t), h.vault.calls.Load())
	}
}

func TestInvalidRequestFieldsAreRejected(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base := func() *processorv1.SaleRequest {
		return &processorv1.SaleRequest{OperationKey: "op-valid", IntegrationId: integration, PaymentMethodToken: tokenApprove, AmountMinorUnits: 100, Currency: "USD"}
	}
	mutations := map[string]func(*processorv1.SaleRequest){
		"empty key":       func(r *processorv1.SaleRequest) { r.OperationKey = "" },
		"key with space":  func(r *processorv1.SaleRequest) { r.OperationKey = "a b" },
		"bad integration": func(r *processorv1.SaleRequest) { r.IntegrationId = "nope" },
		"zero amount":     func(r *processorv1.SaleRequest) { r.AmountMinorUnits = 0 },
		"negative amount": func(r *processorv1.SaleRequest) { r.AmountMinorUnits = -5 },
		"bad currency":    func(r *processorv1.SaleRequest) { r.Currency = "usd" },
	}
	for name, mutate := range mutations {
		request := base()
		mutate(request)
		if _, err := h.client.Sale(ctx, request); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%s: got %v", name, err)
		}
	}
	if h.operationCount(t) != 0 {
		t.Fatal("invalid request recorded a sale")
	}
}

func TestUnknownOrForeignTokenIsRejectedWithoutRecordingASale(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	_, err := h.sale(t, "op-unknown", "vt_unknown_token_0000000009", 100)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown token: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = h.client.Sale(ctx, &processorv1.SaleRequest{OperationKey: "op-foreign", IntegrationId: otherIntegration, PaymentMethodToken: tokenApprove, AmountMinorUnits: 100, Currency: "USD"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("foreign token: %v", err)
	}
	if h.operationCount(t) != 0 {
		t.Fatal("rejected token recorded a sale")
	}
	if _, err := h.client.GetOperation(ctx, &processorv1.GetOperationRequest{OperationKey: "op-unknown"}); status.Code(err) != codes.NotFound {
		t.Fatalf("rejected sale visible to status query: %v", err)
	}
}

func TestVaultOutageOnNewOperationIsUnavailableAndRecoveryUsesRecordedOutcome(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	first, err := h.sale(t, "op-before-outage", tokenIssuer, 4200)
	if err != nil {
		t.Fatal(err)
	}
	h.vault.server.Stop()
	callsBefore := h.vault.calls.Load()

	_, err = h.sale(t, "op-during-outage", tokenApprove, 100)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("new operation during outage: got %v, want Unavailable", err)
	}
	if h.operationCount(t) != 1 {
		t.Fatalf("outage recorded a fabricated sale: %d rows", h.operationCount(t))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status1, err := h.client.GetOperation(ctx, &processorv1.GetOperationRequest{OperationKey: "op-before-outage"})
	if err != nil || status1.GetProcessorReference() != first.GetProcessorReference() || status1.GetDeclineReason() != "ISSUER_DECLINED" {
		t.Fatalf("status during outage: %v %v", status1, err)
	}
	replay, err := h.sale(t, "op-before-outage", tokenIssuer, 4200)
	if err != nil || replay.GetProcessorReference() != first.GetProcessorReference() {
		t.Fatalf("replay during outage: %v %v", replay, err)
	}
	if _, err := h.client.GetOperation(ctx, &processorv1.GetOperationRequest{OperationKey: "op-during-outage"}); status.Code(err) != codes.NotFound {
		t.Fatalf("operation attempted during outage must be unknown: %v", err)
	}
	if h.vault.calls.Load() != callsBefore {
		t.Fatal("recorded outcomes consulted the vault")
	}
}

func TestInjectedTimeoutRecordsOutcomeThenStatusQueryReturnsIt(t *testing.T) {
	h := newHarness(t, harnessOptions{faults: domain.Faults{Seed: 7, TimeoutRate: 1}, hold: 10 * time.Second})
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := h.client.Sale(ctx, &processorv1.SaleRequest{OperationKey: "op-timeout", IntegrationId: integration, PaymentMethodToken: tokenNSF, AmountMinorUnits: 900, Currency: "USD"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	if time.Since(started) > 3*time.Second {
		t.Fatalf("timeout was not bounded by the caller deadline: %s", time.Since(started))
	}

	statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer statusCancel()
	recorded, err := h.client.GetOperation(statusCtx, &processorv1.GetOperationRequest{OperationKey: "op-timeout"})
	if err != nil {
		t.Fatal(err)
	}
	if recorded.GetDeclineReason() != "INSUFFICIENT_FUNDS" || recorded.GetOutcome() != processorv1.OperationOutcome_OPERATION_OUTCOME_DECLINED {
		t.Fatalf("recorded outcome: %v", recorded)
	}
	replay, err := h.client.Sale(statusCtx, &processorv1.SaleRequest{OperationKey: "op-timeout", IntegrationId: integration, PaymentMethodToken: tokenNSF, AmountMinorUnits: 900, Currency: "USD"})
	if err != nil || replay.GetProcessorReference() != recorded.GetProcessorReference() {
		t.Fatalf("replay after timeout: %v %v", replay, err)
	}
	if h.operationCount(t) != 1 {
		t.Fatal("retry after timeout recorded a second sale")
	}
}

func TestInjectedLatencyDelaysNewSalesOnly(t *testing.T) {
	h := newHarness(t, harnessOptions{faults: domain.Faults{LatencyMin: 200 * time.Millisecond, LatencyMax: 200 * time.Millisecond}})
	started := time.Now()
	if _, err := h.sale(t, "op-latency", tokenApprove, 100); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 200*time.Millisecond {
		t.Fatalf("new sale returned after %s", elapsed)
	}
	started = time.Now()
	if _, err := h.sale(t, "op-latency", tokenApprove, 100); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("replay was delayed by %s", elapsed)
	}
}

func TestGetOperationNotFoundNeverTouchesVault(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := h.client.GetOperation(ctx, &processorv1.GetOperationRequest{OperationKey: "never-received"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("got %v, want NotFound", err)
	}
	if _, err := h.client.GetOperation(ctx, &processorv1.GetOperationRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty key: %v", err)
	}
	if h.vault.calls.Load() != 0 {
		t.Fatal("status query reached the vault")
	}
}

func TestOperationsSurviveRestart(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	first, err := h.sale(t, "op-durable", tokenApprove, 100)
	if err != nil {
		t.Fatal(err)
	}
	store, err := postgres.NewStore(context.Background(), h.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := application.NewService(store, nil, domain.Faults{}, time.Second)
	operation, err := service.GetOperation(context.Background(), "op-durable")
	if err != nil || operation.ProcessorReference != first.GetProcessorReference() {
		t.Fatalf("restarted store: %v %v", operation, err)
	}
}

func TestReadinessFollowsDatabaseNotVault(t *testing.T) {
	h := newHarness(t, harnessOptions{})
	health := healthpb.NewHealthClient(h.conn)
	await := func(service string, want healthpb.HealthCheckResponse_ServingStatus) {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			response, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
			cancel()
			if err == nil && response.GetStatus() == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never became %s (last %v, %v)", service, want, response.GetStatus(), err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	await("liveness", healthpb.HealthCheckResponse_SERVING)
	await("readiness", healthpb.HealthCheckResponse_SERVING)

	h.vault.server.Stop()
	time.Sleep(400 * time.Millisecond)
	await("readiness", healthpb.HealthCheckResponse_SERVING)

	admin := adminConn(t)
	database := databaseName(t, h.databaseURL)
	execAdmin(t, admin, fmt.Sprintf("ALTER DATABASE %s ALLOW_CONNECTIONS false", database))
	execAdmin(t, admin, fmt.Sprintf("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s'", database))
	await("readiness", healthpb.HealthCheckResponse_NOT_SERVING)
	await("liveness", healthpb.HealthCheckResponse_SERVING)

	execAdmin(t, admin, fmt.Sprintf("ALTER DATABASE %s ALLOW_CONNECTIONS true", database))
	await("readiness", healthpb.HealthCheckResponse_SERVING)
}

func TestReadinessRequiresMigratedDatabase(t *testing.T) {
	databaseURL := testpostgres.SetupPostgres(t)
	ctx := context.Background()
	store, err := postgres.NewStore(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(ctx); err == nil {
		t.Fatal("ready before migrations")
	}
	if err := postgres.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(ctx); err != nil {
		t.Fatalf("not ready after migrations: %v", err)
	}
}

func adminConn(t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), testpostgres.AdminURL(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func execAdmin(t *testing.T, conn *pgx.Conn, statement string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func databaseName(t *testing.T, databaseURL string) string {
	t.Helper()
	config, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	return config.Database
}
