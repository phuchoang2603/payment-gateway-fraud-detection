package landing

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type memoryStore struct {
	objects map[string][]byte
	order   []string
	fail    bool
}

func (s *memoryStore) Create(_ context.Context, key string, body []byte) error {
	if s.fail {
		return errors.New("publication interrupted")
	}
	if old, ok := s.objects[key]; ok && !bytes.Equal(old, body) {
		return errors.New("collision")
	}
	s.objects[key] = append([]byte{}, body...)
	s.order = append(s.order, key)
	return nil
}

func eventRecord(t *testing.T, offset int64) *kgo.Record {
	t.Helper()
	stamp := timestamppb.New(time.Date(2026, 7, 1, 0, 0, 0, 123456000, time.UTC))
	event := &paymentsv1.PaymentEvent{EventId: "same-delivery", PaymentId: "payment", IntegrationId: "integration", AggregateVersion: 1, SchemaVersion: 2, OccurredAt: stamp, RecordedAt: stamp, Payload: &paymentsv1.PaymentEvent_PaymentExpired{PaymentExpired: &paymentsv1.PaymentExpired{Present: true}}}
	body, err := proto.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "payments.events.v1", Partition: 0, Offset: offset, Value: body, Key: []byte{}, Headers: []kgo.RecordHeader{{Key: "h", Value: nil}, {Key: "h", Value: []byte{}}}}
}

func TestPublishConservesDuplicatesAndCompletesManifestLast(t *testing.T) {
	store := &memoryStore{objects: map[string][]byte{}}
	records := []*kgo.Record{eventRecord(t, 1), eventRecord(t, 2)}
	manifests, err := Publish(context.Background(), store, records, "fraud.labels.v1")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 || store.order[len(store.order)-1] != manifests[0] {
		t.Fatal("manifest not last")
	}
	var m Manifest
	if err = json.Unmarshal(store.objects[manifests[0]], &m); err != nil {
		t.Fatal(err)
	}
	if m.Rows != 2 || m.Files[0].Rows != 2 || m.FirstOffset != 1 || m.LastOffset != 2 {
		t.Fatalf("lost deliveries: %+v", m)
	}
	again, err := Publish(context.Background(), store, records, "fraud.labels.v1")
	if err != nil || again[0] != manifests[0] {
		t.Fatal("replay changed immutable identity", err)
	}
	store.fail = true
	if _, err = Publish(context.Background(), store, records, "fraud.labels.v1"); err == nil {
		t.Fatal("accepted incomplete publication")
	}
}

func TestRejectMalformedAndUnsupportedWithoutPublication(t *testing.T) {
	for _, body := range [][]byte{nil, {255}, {}} {
		r := eventRecord(t, 1)
		r.Value = body
		store := &memoryStore{objects: map[string][]byte{}}
		if _, err := Publish(context.Background(), store, []*kgo.Record{r}, "fraud.labels.v1"); err == nil || len(store.objects) != 0 {
			t.Fatal("published malformed input")
		}
	}
	r := eventRecord(t, 1)
	var event paymentsv1.PaymentEvent
	if err := proto.Unmarshal(r.Value, &event); err != nil {
		t.Fatal(err)
	}
	event.SchemaVersion = 3
	r.Value, _ = proto.Marshal(&event)
	if _, err := Decode(r, "fraud.labels.v1"); err == nil {
		t.Fatal("accepted unknown schema")
	}
}
