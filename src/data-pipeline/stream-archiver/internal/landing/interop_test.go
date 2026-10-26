package landing

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func TestCrossLanguageFixtures(t *testing.T) {
	directory := os.Getenv("LANDING_FIXTURES_DIR")
	if directory == "" {
		t.Skip("Python wire/Parquet interoperability runs in Python CI")
	}
	for _, name := range []string{"event-v1", "event-v2", "created-v2", "risk-v2", "expired-v2", "label"} {
		body, err := os.ReadFile(filepath.Join(directory, name+".pb"))
		if err != nil {
			t.Fatal(err)
		}
		topic := "payments.events.v1"
		if name == "label" {
			topic = "fraud.labels.v1"
		}
		r := &kgo.Record{Topic: topic, Partition: 2, Offset: 42, Timestamp: time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC), Key: []byte{}, Value: body, Headers: []kgo.RecordHeader{{Key: "x", Value: nil}, {Key: "x", Value: []byte{}}, {Key: "x", Value: []byte{0, 255}}}}
		var message proto.Message = &paymentsv1.PaymentEvent{}
		if name == "label" {
			message = &paymentsv1.FraudLabel{}
		}
		if err = proto.Unmarshal(body, message); err != nil {
			t.Fatal(err)
		}
		encoded, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(directory, name+".go.pb"), encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		row, err := Decode(r, "fraud.labels.v1")
		if err != nil {
			t.Fatal(err)
		}
		parquet, err := Encode([]Decoded{row})
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(directory, name+".parquet"), parquet, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
