package httptransport_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/domain"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/grpcclient"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/integrations"
	paymentsadapter "github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/payments"
	vaultadapter "github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/adapters/vault"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/testvectors"
	httptransport "github.com/phuchoang2603/payment-gateway-fraud-detection/edge/internal/transport/http"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	vaultv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/vault/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	syntheticID  = "01234567-89ab-4cde-8fab-0123456789ab"
	realID       = "76543210-89ab-4cde-8fab-0123456789ab"
	syntheticKey = "api-key-synthetic"
	realKey      = "api-key-real"
	publicOrigin = "https://pay.example.test"
	realFPKey    = "real-fingerprint-key"
	cookieKey    = "cookie-key"
)

type storedPayment struct {
	payment *paymentsv1.Payment
	token   string
}

type attemptRecord struct {
	token  string
	result *paymentsv1.AttemptResult
}

type vaultToken struct {
	integration string
	behavior    string
	method      *vaultv1.MethodMetadata
	inactive    bool
}

type fakeCard struct {
	behavior    string
	fingerprint string
}

type world struct {
	mu sync.Mutex

	clock        time.Time
	payments     map[string]*storedPayment
	sessions     map[string]*storedPayment
	createKeys   map[string]*paymentsv1.Payment
	createDigest map[string][]byte
	attemptKeys  map[string]attemptRecord

	createCalls []*paymentsv1.CreatePaymentRequest
	getCalls    []*paymentsv1.GetPaymentRequest
	startCalls  []*paymentsv1.StartAttemptRequest
	requestIDs  []string

	attemptsCreated int
	forcePending    bool
	failures        map[string]error
	block           map[string]chan struct{}

	cards         map[string]fakeCard
	tokens        map[string]*vaultToken
	tokenizeCalls int
	describeCalls int
	counter       int
}

func newWorld(t *testing.T) *world {
	t.Helper()
	vectors := testvectors.Load(t)
	w := &world{
		clock:        time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		payments:     map[string]*storedPayment{},
		sessions:     map[string]*storedPayment{},
		createKeys:   map[string]*paymentsv1.Payment{},
		createDigest: map[string][]byte{},
		attemptKeys:  map[string]attemptRecord{},
		failures:     map[string]error{},
		block:        map[string]chan struct{}{},
		cards:        map[string]fakeCard{},
		tokens:       map[string]*vaultToken{},
	}
	named := map[string][2]string{
		"4111000000000008": {"card-000000", "ok"},
		"4112000000000007": {"card-000000", "nsf"},
		"4113000000000006": {"card-000000", "issuer"},
		"4114000000000005": {"card-000000", "invalid"},
	}
	for number, ref := range named {
		mac := hmac.New(sha256.New, []byte(vectors.SyntheticKey))
		mac.Write([]byte(ref[0]))
		w.cards[number] = fakeCard{behavior: ref[1], fingerprint: hex.EncodeToString(mac.Sum(nil))}
	}
	for _, card := range vectors.TestCards {
		w.cards[card.Number] = fakeCard{behavior: card.Behavior, fingerprint: card.Expected}
	}
	return w
}

func (w *world) now() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.clock
}

func (w *world) advance(d time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.clock = w.clock.Add(d)
}

func (w *world) fail(method string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failures[method] = err
}

func (w *world) recordRequestID(ctx context.Context) {
	if values := metadata.ValueFromIncomingContext(ctx, "x-request-id"); len(values) > 0 {
		w.requestIDs = append(w.requestIDs, values[0])
	}
}

func (w *world) gate(ctx context.Context, method string) error {
	w.recordRequestID(ctx)
	if channel, found := w.block[method]; found {
		w.mu.Unlock()
		select {
		case <-channel:
		case <-ctx.Done():
		}
		w.mu.Lock()
		if ctx.Err() != nil {
			return status.FromContextError(ctx.Err()).Err()
		}
	}
	return w.failures[method]
}

type fakePayments struct {
	paymentsv1.UnimplementedPaymentServiceServer
	w *world
}

func (f fakePayments) CreatePayment(ctx context.Context, req *paymentsv1.CreatePaymentRequest) (*paymentsv1.Payment, error) {
	w := f.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.createCalls = append(w.createCalls, req)
	if err := w.gate(ctx, "CreatePayment"); err != nil {
		return nil, err
	}
	digest, _ := proto.MarshalOptions{Deterministic: true}.Marshal(&paymentsv1.CreatePaymentRequest{
		ExternalSellerId: req.GetExternalSellerId(),
		ExternalBuyerId:  req.GetExternalBuyerId(),
		Snapshot:         req.GetSnapshot(),
		ReturnUrl:        req.GetReturnUrl(),
	})
	key := req.GetIntegrationId() + "|" + req.GetIdempotencyKey()
	if existing, found := w.createKeys[key]; found {
		if !bytes.Equal(w.createDigest[key], digest) {
			return nil, status.Error(codes.AlreadyExists, "idempotency key reused with different content")
		}
		return existing, nil
	}
	w.counter++
	id := fmt.Sprintf("pay-%d", w.counter)
	session := fmt.Sprintf("sess-%d", w.counter)
	token := fmt.Sprintf("ct-%d-%s", w.counter, rand.Text())
	payment := &paymentsv1.Payment{
		PaymentId:     id,
		SessionId:     session,
		CheckoutToken: token,
		ExpiresAt:     timestamppb.New(w.clock.Add(15 * time.Minute)),
		Status:        paymentsv1.PaymentStatus_PAYMENT_STATUS_OPEN,
		Version:       1,
		Merchant:      &paymentsv1.MerchantRef{MerchantId: "merchant-" + req.GetExternalSellerId(), ExternalSellerId: req.GetExternalSellerId()},
		Customer:      &paymentsv1.CustomerRef{CustomerId: "customer-" + req.GetExternalBuyerId(), ExternalBuyerId: req.GetExternalBuyerId()},
		Snapshot:      req.GetSnapshot(),
		ReturnUrl:     req.GetReturnUrl(),
		IntegrationId: req.GetIntegrationId(),
	}
	stored := &storedPayment{payment: payment, token: token}
	w.payments[id] = stored
	w.sessions[session] = stored
	w.createKeys[key] = payment
	w.createDigest[key] = digest
	return payment, nil
}

func (f fakePayments) GetPayment(ctx context.Context, req *paymentsv1.GetPaymentRequest) (*paymentsv1.Payment, error) {
	w := f.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.getCalls = append(w.getCalls, req)
	if err := w.gate(ctx, "GetPayment"); err != nil {
		return nil, err
	}
	stored, found := w.payments[req.GetPaymentId()]
	if !found || stored.payment.GetIntegrationId() != req.GetIntegrationId() {
		return nil, status.Error(codes.NotFound, "payment not found")
	}
	return stored.payment, nil
}

func (f fakePayments) GetCheckoutSession(ctx context.Context, req *paymentsv1.GetCheckoutSessionRequest) (*paymentsv1.Payment, error) {
	w := f.w
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.gate(ctx, "GetCheckoutSession"); err != nil {
		return nil, err
	}
	stored, found := w.sessions[req.GetSessionId()]
	if !found {
		return nil, status.Error(codes.NotFound, "session not found")
	}
	if stored.token != req.GetCheckoutToken() {
		return nil, status.Error(codes.Unauthenticated, "bad checkout token")
	}
	return stored.payment, nil
}

func (f fakePayments) StartAttempt(ctx context.Context, req *paymentsv1.StartAttemptRequest) (*paymentsv1.AttemptResult, error) {
	w := f.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.startCalls = append(w.startCalls, req)
	if err := w.gate(ctx, "StartAttempt"); err != nil {
		return nil, err
	}
	stored, found := w.sessions[req.GetSessionId()]
	if !found || stored.token != req.GetCheckoutToken() {
		return nil, status.Error(codes.Unauthenticated, "bad checkout token")
	}
	key := req.GetIntegrationId() + "|" + req.GetIdempotencyKey()
	if existing, found := w.attemptKeys[key]; found {
		if existing.token != req.GetPaymentMethodToken() {
			return nil, status.Error(codes.AlreadyExists, "idempotency key reused with a different method")
		}
		return existing.result, nil
	}
	if stored.payment.GetStatus() != paymentsv1.PaymentStatus_PAYMENT_STATUS_OPEN {
		return nil, status.Error(codes.FailedPrecondition, "payment is closed")
	}
	w.attemptsCreated++
	result := &paymentsv1.AttemptResult{AttemptId: fmt.Sprintf("att-%d", w.attemptsCreated)}
	behavior := "ok"
	if token, found := w.tokens[req.GetPaymentMethodToken()]; found {
		behavior = token.behavior
	}
	switch {
	case w.forcePending:
		result.Status = paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING
	case behavior == "ok":
		result.Status = paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED
		stored.payment.Status = paymentsv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	default:
		result.Status = paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED
		result.DeclineReason = map[string]string{"nsf": "INSUFFICIENT_FUNDS", "issuer": "ISSUER_DECLINED", "invalid": "INVALID_CARD"}[behavior]
	}
	stored.payment.Attempts = append(stored.payment.Attempts, result)
	w.attemptKeys[key] = attemptRecord{token: req.GetPaymentMethodToken(), result: result}
	return result, nil
}

type fakeVault struct {
	vaultv1.UnimplementedVaultServiceServer
	w *world
}

func (f fakeVault) TokenizeCard(ctx context.Context, req *vaultv1.TokenizeCardRequest) (*vaultv1.TokenizeCardResponse, error) {
	w := f.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tokenizeCalls++
	if err := w.gate(ctx, "TokenizeCard"); err != nil {
		return nil, err
	}
	card, found := w.cards[req.GetCardNumber()]
	if !found {
		return nil, status.Error(codes.InvalidArgument, "card not accepted: "+req.GetCardNumber())
	}
	w.counter++
	value := fmt.Sprintf("vt_%d_%s", w.counter, rand.Text())
	method := &vaultv1.MethodMetadata{Type: "card", Fingerprint: card.fingerprint}
	w.tokens[value] = &vaultToken{integration: req.GetIntegrationId(), behavior: card.behavior, method: method}
	return &vaultv1.TokenizeCardResponse{Token: value, Metadata: method}, nil
}

func (f fakeVault) DescribeMethod(ctx context.Context, req *vaultv1.DescribeMethodRequest) (*vaultv1.MethodMetadata, error) {
	w := f.w
	w.mu.Lock()
	defer w.mu.Unlock()
	w.describeCalls++
	if err := w.gate(ctx, "DescribeMethod"); err != nil {
		return nil, err
	}
	token, found := w.tokens[req.GetToken()]
	if !found || token.inactive || token.integration != req.GetIntegrationId() {
		return nil, status.Error(codes.NotFound, "unknown token")
	}
	return token.method, nil
}

type harness struct {
	t       *testing.T
	world   *world
	server  *httptest.Server
	service *application.Service
	logs    *syncBuffer
	options harnessOptions
}

type harnessOptions struct {
	paymentsTimeout time.Duration
}

type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func sha(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func registry(t *testing.T) *integrations.Registry {
	t.Helper()
	raw := fmt.Sprintf(`[
	{"integration_id":%q,"name":"live-simulator","api_key_sha256":%q,"synthetic":true,"webhook":{"url":"https://x.example","secret":"s","events":["payment.succeeded"]}},
	{"integration_id":%q,"name":"marketplace","api_key_sha256":%q,"synthetic":false}]`,
		syntheticID, sha(syntheticKey), realID, sha(realKey))
	reg, err := integrations.Parse([]byte(raw))
	if err != nil {
		t.Fatalf("parse integrations: %v", err)
	}
	return reg
}

func serve(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return dialBuf(t, listener, 0)
}

func dialBuf(t *testing.T, listener *bufconn.Listener, timeout time.Duration) *grpc.ClientConn {
	t.Helper()
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	conn, err := grpcclient.Dial("passthrough:///bufnet", timeout,
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newHarness(t *testing.T, opts ...func(*harnessOptions)) *harness {
	t.Helper()
	options := harnessOptions{paymentsTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(&options)
	}
	logs := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	w := newWorld(t)
	paymentsListener := bufconn.Listen(1 << 20)
	paymentsServer := grpc.NewServer()
	paymentsv1.RegisterPaymentServiceServer(paymentsServer, fakePayments{w: w})
	go func() { _ = paymentsServer.Serve(paymentsListener) }()
	t.Cleanup(paymentsServer.Stop)
	vaultConn := serve(t, func(s *grpc.Server) { vaultv1.RegisterVaultServiceServer(s, fakeVault{w: w}) })

	h := &harness{t: t, world: w, logs: logs, options: options}
	h.service = h.newService(dialBuf(t, paymentsListener, options.paymentsTimeout), vaultConn)
	h.server = h.mount(h.service)
	return h
}

func (h *harness) newService(paymentsConn, vaultConn *grpc.ClientConn) *application.Service {
	return application.New(application.Config{
		Payments:                paymentsadapter.New(paymentsConn),
		Vault:                   vaultadapter.New(vaultConn),
		Integrations:            registry(h.t),
		FingerprintKey:          realFPKey,
		SyntheticFingerprintKey: testvectors.Load(h.t).SyntheticKey,
		PublicBaseURL:           publicOrigin,
		Now:                     h.world.now,
	})
}

func (h *harness) mount(service *application.Service) *httptest.Server {
	h.t.Helper()
	edge, err := httptransport.New(httptransport.Options{
		Address:            "127.0.0.1:0",
		ShutdownTimeout:    time.Second,
		Service:            service,
		PublicBaseURL:      publicOrigin,
		CookieKey:          []byte(cookieKey),
		CookieSecure:       true,
		TrustedProxyHeader: "X-Forwarded-For",
		TrustedProxyHops:   1,
	})
	if err != nil {
		h.t.Fatalf("new server: %v", err)
	}
	server := httptest.NewServer(edge.Handler())
	h.t.Cleanup(server.Close)
	return server
}

type response struct {
	*http.Response
	body string
}

func (h *harness) do(method, path string, header map[string]string, body string) response {
	return h.doOn(h.server, method, path, header, body)
}

func (h *harness) doOn(server *httptest.Server, method, path string, header map[string]string, body string) response {
	h.t.Helper()
	request, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("new request: %v", err)
	}
	for name, value := range header {
		request.Header.Set(name, value)
	}
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(request)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(res.Body)
	return response{Response: res, body: string(raw)}
}

func apiHeaders(key, idempotency string) map[string]string {
	headers := map[string]string{"Authorization": "Bearer " + key, "Content-Type": "application/json"}
	if idempotency != "" {
		headers["Idempotency-Key"] = idempotency
	}
	return headers
}

func (h *harness) assertLogsExclude(forbidden ...string) {
	h.t.Helper()
	logs := h.logs.String()
	for _, value := range forbidden {
		if value != "" && strings.Contains(logs, value) {
			h.t.Fatalf("logs contain forbidden value %q:\n%s", value, logs)
		}
	}
}

func (h *harness) replicaService() *application.Service {
	h.t.Helper()
	paymentsListener := bufconn.Listen(1 << 20)
	paymentsServer := grpc.NewServer()
	paymentsv1.RegisterPaymentServiceServer(paymentsServer, fakePayments{w: h.world})
	go func() { _ = paymentsServer.Serve(paymentsListener) }()
	h.t.Cleanup(paymentsServer.Stop)
	vaultConn := serve(h.t, func(s *grpc.Server) { vaultv1.RegisterVaultServiceServer(s, fakeVault{w: h.world}) })
	return h.newService(dialBuf(h.t, paymentsListener, h.options.paymentsTimeout), vaultConn)
}

func grpcUnavailable() error { return status.Error(codes.Unavailable, "vault down") }

func domainFingerprinter(key string) domain.Fingerprinter { return domain.NewFingerprinter(key) }

func mustAddr(value string) netip.Addr { return netip.MustParseAddr(value) }
