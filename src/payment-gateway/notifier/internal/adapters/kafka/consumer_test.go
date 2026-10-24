package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/telemetry"
	"go.opentelemetry.io/otel/trace"

	testkafka "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/testutil/kafka"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/kafka"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/postgres"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/metrics"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/testutil"
)

func TestConsumerRecordsOnceAcrossRestartsAndGroups(t *testing.T) {
	provider, recorder := telemetry.Traces(t)
	origin, parent := provider.Tracer("producer").Start(context.Background(), "append event")
	parent.End()
	brokers := testkafka.SetupKafka(t)
	pool, _ := testutil.NewPool(t)
	set, err := domain.ParseIntegrations([]byte(`[
	{"integration_id":"sub","webhook":{"url":"https://hooks.example.com/x","secret":"s","events":["attempt.declined","payment.succeeded"]}},
	{"integration_id":"bare"}]`))
	if err != nil {
		t.Fatal(err)
	}
	suffix := time.Now().UnixNano()
	topic := fmt.Sprintf("notifier-test-%d", suffix)
	group := fmt.Sprintf("notifier-test-group-%d", suffix)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	messages := []struct {
		key   string
		value []byte
	}{
		{"p1", testutil.Created(t, "k1", "p1", "sub", "order-1")},
		{"p1", testutil.Declined(t, "k2", "p1", "sub", "a1", "ISSUER_DECLINED")},
		{"p1", testutil.Succeeded(t, "k3", "p1", "sub", "a2")},
		{"p1", testutil.Expired(t, "k4", "p1", "sub")},
		{"p2", testutil.Created(t, "k5", "p2", "bare", "order-2")},
		{"p2", testutil.Succeeded(t, "k6", "p2", "bare", "a1")},
		{"p2", []byte{0xff, 0xff}},
	}
	produceCtx, cancelProduce := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelProduce()
	for _, message := range messages {
		record := &kgo.Record{Topic: topic, Key: []byte(message.key), Value: message.value}
		if message.key == "p1" {
			record.Headers = []kgo.RecordHeader{{Key: "tracingspancontext", Value: []byte(*observe.SerializeContext(origin))}}
		} else {
			record.Headers = []kgo.RecordHeader{{Key: "tracingspancontext", Value: []byte("traceparent=invalid\n")}}
		}
		if err := producer.ProduceSync(produceCtx, record).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	runConsumer := func(group string) {
		t.Helper()
		ingestor := &application.Ingestor{Store: postgres.NewStore(pool), Integrations: set, Now: time.Now}
		markerID := fmt.Sprintf("marker-%d", time.Now().UnixNano())
		marker := testutil.Created(t, markerID, markerID, "bare", markerID)
		handler := &observedHandler{Handler: ingestor, marker: marker, observed: make(chan struct{})}
		consumer, err := kafka.NewConsumer(brokers, topic, group, handler, metrics.New())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			consumer.Run(ctx)
		}()
		defer func() {
			cancel()
			<-done
			consumer.Close()
		}()
		produceCtx, stopProduce := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopProduce()
		if err := producer.ProduceSync(produceCtx, &kgo.Record{Topic: topic, Key: []byte("p1"), Value: marker}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-handler.observed:
		case <-time.After(40 * time.Second):
			t.Fatal("consumer did not handle the marker after the outcome events")
		}
	}

	runConsumer(group)
	assertDeliveries := func() {
		t.Helper()
		rows, err := pool.Query(context.Background(), "SELECT event_id FROM webhook_deliveries ORDER BY event_id")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, id)
		}
		if got := strings.Join(ids, ","); got != "k2,k3" {
			t.Fatalf("deliveries = %s, want k2,k3 (k4 unsubscribed, bare integration endpoint-less)", got)
		}
	}
	assertDeliveries()

	runConsumer(group)
	assertDeliveries()

	runConsumer(group + "-replay")
	assertDeliveries()
	for _, eventID := range []string{"k2", "k3"} {
		var saved *string
		if err := pool.QueryRow(context.Background(), "SELECT tracingspancontext FROM webhook_deliveries WHERE event_id=$1", eventID).Scan(&saved); err != nil {
			t.Fatal(err)
		}
		sc := trace.SpanContextFromContext(observe.RestoreContext(context.Background(), saved))
		if !sc.IsValid() || sc.TraceID() != parent.SpanContext().TraceID() {
			t.Fatal("Kafka/replay lost the event parent")
		}
	}
	matched := false
	for _, span := range recorder.Ended() {
		if span.Name() == "payments.events.v1 process" && span.Parent().SpanID() == parent.SpanContext().SpanID() {
			matched = true
		}
	}
	if !matched {
		t.Fatal("consumer spans do not descend from committed context")
	}
}

type observedHandler struct {
	kafka.Handler
	marker   []byte
	observed chan struct{}
	once     sync.Once
}

func (h *observedHandler) Handle(ctx context.Context, value []byte) (application.Result, error) {
	result, err := h.Handler.Handle(ctx, value)
	if err == nil && bytes.Equal(value, h.marker) {
		h.once.Do(func() { close(h.observed) })
	}
	return result, err
}
