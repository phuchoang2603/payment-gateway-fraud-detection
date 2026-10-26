package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestArchiverKafkaPublicationRecovery(t *testing.T) {
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		t.Skip("broker recovery runs in Python CI")
	}
	topic := fmt.Sprintf("archive-%d", time.Now().UnixNano())
	group := topic + "-group"
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	stamp := timestamppb.New(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	event := &paymentsv1.PaymentEvent{EventId: "same", PaymentId: "payment", IntegrationId: "integration", AggregateVersion: 1, SchemaVersion: 2, OccurredAt: stamp, RecordedAt: stamp, Payload: &paymentsv1.PaymentEvent_PaymentExpired{PaymentExpired: &paymentsv1.PaymentExpired{Present: true}}}
	body, err := proto.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for range 2 {
		if err = client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte("p"), Value: body}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	type object struct {
		body     []byte
		metadata http.Header
	}
	objects := map[string]object{}
	var mu sync.Mutex
	fail := true
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		old, exists := objects[r.URL.Path]
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("unconditional publication")
			}
			if fail && strings.Contains(r.URL.Path, "/_batches/") {
				w.WriteHeader(403)
				_, _ = fmt.Fprint(w, "<Error><Code>AccessDenied</Code></Error>")
				return
			}
			if exists {
				w.WriteHeader(412)
				_, _ = fmt.Fprint(w, "<Error><Code>PreconditionFailed</Code></Error>")
				return
			}
			value, readErr := io.ReadAll(r.Body)
			if readErr != nil {
				t.Error(readErr)
				w.WriteHeader(500)
				return
			}
			objects[r.URL.Path] = object{value, r.Header.Clone()}
			writes++
			w.WriteHeader(200)
		case http.MethodHead:
			if !exists {
				w.WriteHeader(404)
				return
			}
			for _, key := range []string{"X-Amz-Meta-Sha256", "X-Amz-Meta-Landing-Format"} {
				w.Header().Set(key, old.metadata.Get(key))
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(old.body)))
			w.WriteHeader(200)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(405)
		}
	}))
	defer server.Close()
	for key, value := range map[string]string{"KAFKA_BROKERS": brokers, "LANDING_TOPICS": topic, "LANDING_GROUP": group, "LANDING_MAX_BYTES": fmt.Sprint(len(body) + 10), "LANDING_FLUSH_INTERVAL": "100ms", "S3_ENDPOINT": server.URL, "AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"} {
		t.Setenv(key, value)
	}
	if err = run(ctx); err == nil {
		t.Fatal("publication failure did not fail ingestion")
	}
	mu.Lock()
	if len(objects) != 1 {
		t.Errorf("incomplete batch published manifest: %d objects", len(objects))
	}
	fail = false
	mu.Unlock()
	restartCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(restartCtx) }()
	defer func() {
		stop()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("archiver did not stop")
		}
	}()
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{0}}}
	request.Groups = []kmsg.OffsetFetchRequestGroup{g}
	committed := false
	for ctx.Err() == nil && !committed {
		response, fetchErr := request.RequestWith(ctx, client)
		if fetchErr == nil {
			for _, group := range response.Groups {
				for _, topic := range group.Topics {
					for _, partition := range topic.Partitions {
						committed = committed || partition.Offset == 2
					}
				}
			}
			for _, topic := range response.Topics {
				for _, partition := range topic.Partitions {
					committed = committed || partition.Offset == 2
				}
			}
		}
		if !committed {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !committed {
		t.Fatal("completed files did not advance committed offsets")
	}
	mu.Lock()
	defer mu.Unlock()
	if writes != 4 || len(objects) != 4 {
		t.Fatalf("expected two immutable files and two completion manifests after replay, got %d writes, %d objects", writes, len(objects))
	}
}
