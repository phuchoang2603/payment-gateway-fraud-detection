package application_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/telemetry"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/webhook"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/testutil"
)

const (
	subscribed   = "int-subscribed"
	noEndpoint   = "int-no-endpoint"
	narrow       = "int-narrow"
	unconfigured = "int-unknown"
	secret       = "whsec_unit"
)

func integrations(t *testing.T, url string) domain.Integrations {
	t.Helper()
	doc := `[
	{"integration_id":"` + subscribed + `","webhook":{"url":"` + url + `","secret":"` + secret + `","events":["attempt.declined","payment.succeeded","payment.expired"]}},
	{"integration_id":"` + noEndpoint + `"},
	{"integration_id":"` + narrow + `","webhook":{"url":"` + url + `","secret":"` + secret + `","events":["payment.succeeded"]}}]`
	set, err := domain.ParseIntegrations([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func ingestor(pool *pgxpool.Pool, set domain.Integrations) *application.Ingestor {
	return &application.Ingestor{Store: postgres.NewStore(pool), Integrations: set, Now: time.Now}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestReconsumptionInsertsNoNewDelivery(t *testing.T) {
	pool, _ := testutil.NewPool(t)
	in := ingestor(pool, integrations(t, "https://hooks.example.com/x"))
	ctx := context.Background()
	messages := [][]byte{
		testutil.Created(t, "e1", "p1", subscribed, "order-1"),
		testutil.Declined(t, "e2", "p1", subscribed, "a1", "INSUFFICIENT_FUNDS"),
		testutil.Succeeded(t, "e3", "p1", subscribed, "a2"),
	}
	for round := range 3 {
		for _, message := range messages {
			result, err := in.Handle(ctx, message)
			if err != nil {
				t.Fatal(err)
			}
			if round > 0 && result == application.ResultRecorded {
				t.Fatalf("round %d recorded a new delivery", round)
			}
		}
	}
	if n := count(t, pool, "SELECT count(*) FROM webhook_deliveries"); n != 2 {
		t.Fatalf("deliveries = %d, want 2", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM payment_refs"); n != 1 {
		t.Fatalf("payment refs = %d, want 1", n)
	}
	var body string
	if err := pool.QueryRow(ctx, "SELECT convert_from(body, 'UTF8') FROM webhook_deliveries WHERE event_id = 'e2'").Scan(&body); err != nil {
		t.Fatal(err)
	}
	want := `{"event_id":"e2","type":"attempt.declined","payment_id":"p1","order_reference":"order-1","attempt_id":"a1","status":"DECLINED","reason":"INSUFFICIENT_FUNDS","occurred_at":"2026-10-01T12:00:00Z"}`
	if body != want {
		t.Fatalf("body = %s", body)
	}
}

func TestUnsubscribedAndEndpointlessEventsAreSkipped(t *testing.T) {
	pool, _ := testutil.NewPool(t)
	in := ingestor(pool, integrations(t, "https://hooks.example.com/x"))
	ctx := context.Background()
	tests := []struct {
		name    string
		message []byte
		want    application.Result
	}{
		{"endpoint-less integration", testutil.Succeeded(t, "n1", "pn", noEndpoint, "a"), application.ResultSkipped},
		{"unknown integration", testutil.Succeeded(t, "n2", "pn", unconfigured, "a"), application.ResultSkipped},
		{"unsubscribed type", testutil.Declined(t, "n3", "pn", narrow, "a", "ISSUER_DECLINED"), application.ResultSkipped},
		{"non-outcome event", testutil.Created(t, "n4", "pn", noEndpoint, "order-n"), application.ResultSkipped},
		{"subscribed", testutil.Succeeded(t, "n5", "pn", narrow, "a"), application.ResultRecorded},
	}
	for _, tt := range tests {
		got, err := in.Handle(ctx, tt.message)
		if err != nil || got != tt.want {
			t.Errorf("%s: result %v err %v, want %v", tt.name, got, err, tt.want)
		}
	}
	if n := count(t, pool, "SELECT count(*) FROM webhook_deliveries"); n != 1 {
		t.Fatalf("deliveries = %d, want 1", n)
	}
	if n := count(t, pool, "SELECT count(*) FROM payment_refs WHERE integration_id = $1", noEndpoint); n != 1 {
		t.Fatal("payment ref of endpoint-less integration must still be recorded")
	}
}

func TestMalformedEventIsRejected(t *testing.T) {
	pool, _ := testutil.NewPool(t)
	in := ingestor(pool, integrations(t, "https://hooks.example.com/x"))
	if _, err := in.Handle(context.Background(), []byte{0xff, 0xff, 0xff}); err == nil {
		t.Fatal("expected error")
	}
}

type nopMetrics struct {
	mu       sync.Mutex
	outcomes map[string]int
}

func (m *nopMetrics) Delivery(outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.outcomes == nil {
		m.outcomes = map[string]int{}
	}
	m.outcomes[outcome]++
}
func (*nopMetrics) Backlog(domain.Stats) {}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type received struct {
	idempotencyKey string
	signature      string
	body           string
	contentType    string
}

func TestDeliveryRecoversAfterReceiverOutage(t *testing.T) {
	provider, recorder := telemetry.Traces(t)
	origin, originSpan := provider.Tracer("test").Start(context.Background(), "committed event")
	originSpan.End()
	pool, _ := testutil.NewPool(t)
	var mu sync.Mutex
	var requests []received
	const failures = 3
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		requests = append(requests, received{r.Header.Get("Idempotency-Key"), r.Header.Get("Gateway-Signature"), string(body), r.Header.Get("Content-Type")})
		n := len(requests)
		mu.Unlock()
		if n <= failures {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	set, err := domain.ParseIntegrations([]byte(`[{"integration_id":"` + subscribed + `","webhook":{"url":"https://placeholder.example/h","secret":"` + secret + `","events":["payment.succeeded"]}}]`))
	if err != nil {
		t.Fatal(err)
	}
	in := ingestor(pool, set)
	if _, err := in.Handle(context.Background(), testutil.Created(t, "d0", "pd", subscribed, "order-d")); err != nil {
		t.Fatal(err)
	}
	if _, err := in.Handle(origin, testutil.Succeeded(t, "d1", "pd", subscribed, "a1")); err != nil {
		t.Fatal(err)
	}

	clk := &clock{now: time.Now()}
	metrics := &nopMetrics{}
	worker := &application.DeliveryWorker{
		Queue:        postgres.NewStore(pool),
		Sender:       redirectingSender{target: receiver.URL, inner: webhook.NewSender(2 * time.Second)},
		Integrations: set,
		Schedule:     domain.Schedule{Initial: time.Minute, Factor: 2, Cap: time.Hour, MaxAttempts: 12},
		Metrics:      metrics,
		Now:          clk.Now,
		Jitter:       func() float64 { return 1 },
		BatchSize:    10,
		Concurrency:  2,
		Lease:        time.Minute,
	}
	ctx := context.Background()

	for range failures + 1 {
		restarted := *worker
		restarted.Queue = postgres.NewStore(pool)
		worker = &restarted
		if _, err := worker.Tick(ctx); err != nil {
			t.Fatal(err)
		}
		if n, _ := worker.Tick(ctx); n != 0 {
			t.Fatal("a pending delivery must not be claimable before its backoff elapses")
		}
		clk.Advance(2 * time.Hour)
	}

	attemptsSeen := map[string]bool{}
	for _, span := range recorder.Ended() {
		if span.Name() != "webhook.delivery.attempt" {
			continue
		}
		if span.Parent().SpanID() != originSpan.SpanContext().SpanID() || span.SpanContext().TraceID() != originSpan.SpanContext().TraceID() {
			t.Fatal("restarted retry lost persisted causality")
		}
		attemptsSeen[span.SpanContext().SpanID().String()] = true
	}
	if len(attemptsSeen) != failures+1 {
		t.Fatalf("distinct attempt spans = %d", len(attemptsSeen))
	}
	var persisted *string
	if err := pool.QueryRow(ctx, "SELECT tracingspancontext FROM webhook_deliveries WHERE event_id = 'd1'").Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted == nil || *persisted != *observe.SerializeContext(origin) {
		t.Fatal("delivery context changed across retries")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requests) != failures+1 {
		t.Fatalf("requests = %d, want %d", len(requests), failures+1)
	}
	signatures := map[string]bool{}
	for _, request := range requests {
		if request.idempotencyKey != "d1" || request.body != requests[0].body || request.contentType != "application/json" {
			t.Fatalf("retry changed identity or body: %+v", request)
		}
		signatures[request.signature] = true
		if !strings.HasPrefix(request.signature, "t=") {
			t.Fatalf("bad signature header %q", request.signature)
		}
	}
	if len(signatures) < 2 {
		t.Fatal("expected fresh signatures across attempts")
	}
	var status string
	var attempts int
	if err := pool.QueryRow(ctx, "SELECT status, attempts FROM webhook_deliveries WHERE event_id = 'd1'").Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || attempts != failures+1 {
		t.Fatalf("status %s attempts %d", status, attempts)
	}
	if metrics.outcomes["retry"] != failures || metrics.outcomes["delivered"] != 1 {
		t.Fatalf("metrics %v", metrics.outcomes)
	}
	if n := count(t, pool, "SELECT count(*) FROM webhook_deliveries WHERE delivered_at IS NOT NULL"); n != 1 {
		t.Fatal("delivered_at not set")
	}
}

type redirectingSender struct {
	target string
	inner  application.Sender
}

func (s redirectingSender) Send(ctx context.Context, request application.Request) (int, error) {
	request.URL = s.target
	return s.inner.Send(ctx, request)
}

func TestExhaustedDeliveryIsKeptFailed(t *testing.T) {
	pool, _ := testutil.NewPool(t)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer receiver.Close()
	set, err := domain.ParseIntegrations([]byte(`[{"integration_id":"` + subscribed + `","webhook":{"url":"https://placeholder.example/h","secret":"s","events":["payment.expired"]}}]`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ingestor(pool, set).Handle(context.Background(), testutil.Expired(t, "x1", "px", subscribed)); err != nil {
		t.Fatal(err)
	}
	clk := &clock{now: time.Now()}
	worker := &application.DeliveryWorker{
		Queue: postgres.NewStore(pool), Sender: redirectingSender{target: receiver.URL, inner: webhook.NewSender(time.Second)},
		Integrations: set, Schedule: domain.Schedule{Initial: time.Second, Factor: 2, Cap: time.Minute, MaxAttempts: 3},
		Metrics: &nopMetrics{}, Now: clk.Now, Jitter: func() float64 { return 1 }, BatchSize: 5, Concurrency: 1, Lease: time.Minute,
	}
	for range 5 {
		if _, err := worker.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Hour)
	}
	var status string
	var attempts int
	var lastStatus int
	if err := pool.QueryRow(context.Background(), "SELECT status, attempts, last_status FROM webhook_deliveries WHERE event_id = 'x1'").Scan(&status, &attempts, &lastStatus); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 3 || lastStatus != 500 {
		t.Fatalf("status %s attempts %d last %d", status, attempts, lastStatus)
	}
}

func TestRedirectsAreNotFollowed(t *testing.T) {
	var followed bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { followed = true }))
	defer target.Close()
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer receiver.Close()
	status, err := webhook.NewSender(time.Second).Send(context.Background(), application.Request{URL: receiver.URL, EventID: "e", Signature: "s", Body: []byte("{}")})
	if err != nil || status != http.StatusFound || followed {
		t.Fatalf("status %d err %v followed %v", status, err, followed)
	}
}
