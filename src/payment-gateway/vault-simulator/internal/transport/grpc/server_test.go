package grpctransport_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/security/secretbox"
	testpostgres "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/config"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/domain"
	grpctransport "github.com/phuchoang2603/payment-gateway-fraud-detection/vault-simulator/internal/transport/grpc"
)

const fingerprintKey = "synthetic-fingerprint-v1"

var (
	integrationA = uuid.MustParse("01234567-89ab-4cde-8fab-0123456789ab")
	integrationB = uuid.MustParse("11111111-89ab-4cde-8fab-0123456789ab")
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type harness struct {
	databaseURL string
	logs        *syncBuffer
	vault       vaultv1.VaultServiceClient
	processing  vaultv1.ProcessingServiceClient
	vaultConn   *grpc.ClientConn
}

func newHarness(t *testing.T) *harness {
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

	logs := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	sealer, err := secretbox.New([]byte(strings.Repeat("k", 32)))
	if err != nil {
		t.Fatal(err)
	}
	server := grpctransport.NewServer(config.Config{ShutdownTimeout: 2 * time.Second}, application.NewService(store, sealer, []byte(fingerprintKey)), store)
	listen := func() net.Listener {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return listener
	}
	vaultListener, processingListener := listen(), listen()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(runCtx, vaultListener, processingListener) }()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	connect := func(addr string) *grpc.ClientConn {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	vaultConn := connect(vaultListener.Addr().String())
	return &harness{
		databaseURL: databaseURL,
		logs:        logs,
		vault:       vaultv1.NewVaultServiceClient(vaultConn),
		processing:  vaultv1.NewProcessingServiceClient(connect(processingListener.Addr().String())),
		vaultConn:   vaultConn,
	}
}

func rpcContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func tokenize(t *testing.T, client vaultv1.VaultServiceClient, integration uuid.UUID, number string) (*vaultv1.TokenizeCardResponse, error) {
	t.Helper()
	ctx, cancel := rpcContext()
	defer cancel()
	return client.TokenizeCard(ctx, &vaultv1.TokenizeCardRequest{IntegrationId: integration.String(), CardNumber: number})
}

func expectedFingerprint(cardRef string) string {
	return domain.Fingerprint([]byte(fingerprintKey), cardRef)
}

func TestTokenizeIssuesOpaqueTokensWithStableFingerprint(t *testing.T) {
	h := newHarness(t)
	edge := h.vault
	cards := []struct {
		number, ref string
		behavior    vaultv1.Behavior
	}{
		{"4242424242424242", "card-0001", vaultv1.Behavior_BEHAVIOR_APPROVE},
		{"4000000000009995", "card-0002", vaultv1.Behavior_BEHAVIOR_INSUFFICIENT_FUNDS},
		{"4000000000000002", "card-0003", vaultv1.Behavior_BEHAVIOR_ISSUER_DECLINE},
		{"4000000000000069", "card-0004", vaultv1.Behavior_BEHAVIOR_INVALID},
	}
	processor := h.processing
	for _, card := range cards {
		first, err := tokenize(t, edge, integrationA, card.number)
		if err != nil {
			t.Fatal(err)
		}
		second, err := tokenize(t, edge, integrationA, card.number)
		if err != nil {
			t.Fatal(err)
		}
		if first.GetToken() == second.GetToken() {
			t.Fatalf("%s: reissued token is identical", card.ref)
		}
		want := expectedFingerprint(card.ref)
		for _, response := range []*vaultv1.TokenizeCardResponse{first, second} {
			token := response.GetToken()
			if !strings.HasPrefix(token, "vt_") || len(token) < 40 {
				t.Fatalf("unexpected token shape %q", token)
			}
			for _, leaked := range []string{card.ref, card.number, "ok", "nsf", "issuer", "invalid"} {
				if strings.Contains(strings.TrimPrefix(token, "vt_"), leaked) && len(leaked) > 3 {
					t.Fatalf("token %q embeds %q", token, leaked)
				}
			}
			if response.GetMetadata().GetType() != "card" || response.GetMetadata().GetFingerprint() != want {
				t.Fatalf("%s: metadata %v, want fingerprint %s", card.ref, response.GetMetadata(), want)
			}
			ctx, cancel := rpcContext()
			described, err := edge.DescribeMethod(ctx, &vaultv1.DescribeMethodRequest{IntegrationId: integrationA.String(), Token: token})
			cancel()
			if err != nil || described.GetFingerprint() != want || described.GetType() != "card" {
				t.Fatalf("describe: %v %v", described, err)
			}
			ctx, cancel = rpcContext()
			fixture, err := processor.ResolveForProcessing(ctx, &vaultv1.ResolveRequest{IntegrationId: integrationA.String(), Token: token})
			cancel()
			if err != nil || fixture.GetCardReference() != card.ref || fixture.GetBehavior() != card.behavior {
				t.Fatalf("resolve: %v %v", fixture, err)
			}
		}
	}
}

func TestTokenizeRejectsNumbersOutsideAllowList(t *testing.T) {
	h := newHarness(t)
	edge := h.vault
	for _, number := range []string{"4111111111111111", "5555555555554444", "4242424242424243", "", "4242 4242 4242 4242", "4111000000000000000"} {
		_, err := tokenize(t, edge, integrationA, number)
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("%q: got %v, want InvalidArgument", number, err)
		}
	}
	_, err := tokenize(t, edge, integrationA, "4242424242424242")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := rpcContext()
	defer cancel()
	_, err = edge.TokenizeCard(ctx, &vaultv1.TokenizeCardRequest{IntegrationId: "not-a-uuid", CardNumber: "4242424242424242"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad integration: %v", err)
	}

	pool, err := pgxpool.New(ctx, h.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM tokens").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rejected numbers stored rows: count=%d err=%v", count, err)
	}
}

func TestForeignIntegrationTokenIsNotFound(t *testing.T) {
	h := newHarness(t)
	edge := h.vault
	processor := h.processing
	issued, err := tokenize(t, edge, integrationA, "4242424242424242")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := rpcContext()
	defer cancel()
	_, err = edge.DescribeMethod(ctx, &vaultv1.DescribeMethodRequest{IntegrationId: integrationB.String(), Token: issued.GetToken()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("describe foreign token: %v", err)
	}
	_, err = processor.ResolveForProcessing(ctx, &vaultv1.ResolveRequest{IntegrationId: integrationB.String(), Token: issued.GetToken()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("resolve foreign token: %v", err)
	}
	_, err = edge.DescribeMethod(ctx, &vaultv1.DescribeMethodRequest{IntegrationId: integrationA.String(), Token: "vt_unknown"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("describe unknown token: %v", err)
	}
}

func TestInactiveTokenIsNotFound(t *testing.T) {
	h := newHarness(t)
	edge := h.vault
	issued, err := tokenize(t, edge, integrationA, "4242424242424242")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := rpcContext()
	defer cancel()
	pool, err := pgxpool.New(ctx, h.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "UPDATE tokens SET active = false"); err != nil {
		t.Fatal(err)
	}
	_, err = edge.DescribeMethod(ctx, &vaultv1.DescribeMethodRequest{IntegrationId: integrationA.String(), Token: issued.GetToken()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("inactive token: %v", err)
	}
}

func TestNoPlaintextFixtureOrCardNumberInStorageOrLogs(t *testing.T) {
	h := newHarness(t)
	edge := h.vault
	processor := h.processing
	number := "4000000000009995"
	issued, err := tokenize(t, edge, integrationA, number)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := rpcContext()
	defer cancel()
	if _, err := processor.ResolveForProcessing(ctx, &vaultv1.ResolveRequest{IntegrationId: integrationA.String(), Token: issued.GetToken()}); err != nil {
		t.Fatal(err)
	}
	_, _ = tokenize(t, edge, integrationA, "4111111111111111")
	_, _ = edge.DescribeMethod(ctx, &vaultv1.DescribeMethodRequest{IntegrationId: integrationB.String(), Token: issued.GetToken()})

	pool, err := pgxpool.New(ctx, h.databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, "SELECT t::text, encode(t.fixture_ciphertext, 'escape'), encode(t.token_digest, 'hex') FROM tokens t")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var stored strings.Builder
	rowCount := 0
	for rows.Next() {
		var rowText, ciphertext, digest string
		if err := rows.Scan(&rowText, &ciphertext, &digest); err != nil {
			t.Fatal(err)
		}
		stored.WriteString(rowText + ciphertext)
		rowCount++
		sum := sha256.Sum256([]byte(issued.GetToken()))
		if digest != hex.EncodeToString(sum[:]) {
			t.Fatal("stored digest is not sha256 of token")
		}
	}
	if rowCount != 1 {
		t.Fatalf("got %d rows", rowCount)
	}
	for _, forbidden := range []string{number, "card-0002", "card_reference", "behavior", "nsf", issued.GetToken(), strings.TrimPrefix(issued.GetToken(), "vt_"), "4111111111111111"} {
		if strings.Contains(stored.String(), forbidden) {
			t.Fatalf("storage contains %q", forbidden)
		}
	}
	for _, forbidden := range []string{number, "4111111111111111", issued.GetToken(), strings.TrimPrefix(issued.GetToken(), "vt_"), "card-0002"} {
		if strings.Contains(h.logs.String(), forbidden) {
			t.Fatalf("logs contain %q", forbidden)
		}
	}
	if h.logs.String() == "" {
		t.Fatal("expected request logs to be captured")
	}
}

func TestHealthServicesAreServed(t *testing.T) {
	h := newHarness(t)
	health := healthpb.NewHealthClient(h.vaultConn)
	for _, name := range []string{grpctransport.LivenessService, grpctransport.ReadinessService} {
		deadline := time.Now().Add(10 * time.Second)
		for {
			ctx, cancel := rpcContext()
			response, err := health.Check(ctx, &healthpb.HealthCheckRequest{Service: name})
			cancel()
			if err == nil && response.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never SERVING: %v %v", name, response, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestReadinessRequiresMigratedDatabase(t *testing.T) {
	databaseURL := testpostgres.SetupPostgres(t)
	ctx := context.Background()
	store, err := postgres.NewStore(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(ctx); err == nil {
		t.Fatal("ready before migrations")
	}
	if err := postgres.Migrate(ctx, databaseURL); err != nil {
		t.Fatal(err)
	}
	if err := store.Ready(ctx); err != nil {
		t.Fatalf("not ready after migrations: %v", err)
	}
	store.Close()
	if err := store.Ready(ctx); err == nil {
		t.Fatal("ready with closed pool")
	}
}

func TestResolveIsNotServedOnTheVaultPort(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := rpcContext()
	defer cancel()
	err := h.vaultConn.Invoke(ctx, vaultv1.ProcessingService_ResolveForProcessing_FullMethodName,
		&vaultv1.ResolveRequest{IntegrationId: integrationA.String(), Token: "vt_x"}, &vaultv1.ProcessingFixture{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("resolve on vault port: %v", err)
	}
}
