package landing

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Header struct {
	Key   string  `parquet:"key"`
	Value *[]byte `parquet:"value,optional"`
}

type Transport struct {
	Topic     string     `parquet:"topic"`
	Partition int32      `parquet:"partition"`
	Offset    int64      `parquet:"offset"`
	Timestamp *time.Time `parquet:"timestamp,optional,timestamp(microsecond:utc)"`
	Key       *[]byte    `parquet:"key,optional"`
	Value     []byte     `parquet:"value"`
	Headers   []Header   `parquet:"headers,list"`
}

func binary(value []byte) *[]byte {
	if value == nil {
		return nil
	}
	return &value
}

func transport(record *kgo.Record) Transport {
	t := Transport{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: binary(record.Key), Value: record.Value, Headers: []Header{}}
	if !record.Timestamp.IsZero() {
		t.Timestamp = &record.Timestamp
	}
	for _, h := range record.Headers {
		t.Headers = append(t.Headers, Header{h.Key, binary(h.Value)})
	}
	return t
}

type Money struct {
	MinorUnits *int64  `json:"minor_units,string" parquet:"minor_units,optional"`
	Currency   *string `json:"currency" parquet:"currency,optional"`
}
type Merchant struct {
	ID       *string `json:"merchant_id" parquet:"merchant_id,optional"`
	External *string `json:"external_seller_id" parquet:"external_seller_id,optional"`
}
type Customer struct {
	ID       *string `json:"customer_id" parquet:"customer_id,optional"`
	External *string `json:"external_buyer_id" parquet:"external_buyer_id,optional"`
}
type Shipping struct {
	Country            *string `json:"country" parquet:"country,optional"`
	Region             *string `json:"region" parquet:"region,optional"`
	PostalCode         *string `json:"postal_code" parquet:"postal_code,optional"`
	AddressFingerprint *string `json:"address_fingerprint" parquet:"address_fingerprint,optional"`
}
type Item struct {
	ProductID *string `json:"product_id" parquet:"product_id,optional"`
	Category  *string `json:"category" parquet:"category,optional"`
	Condition *string `json:"condition" parquet:"condition,optional"`
	Quantity  *int32  `json:"quantity" parquet:"quantity,optional"`
	UnitPrice *Money  `json:"unit_price" parquet:"unit_price,optional"`
}
type Checkout struct {
	OrderID           *string    `json:"external_order_id" parquet:"external_order_id,optional"`
	Total             *Money     `json:"total" parquet:"total,optional"`
	Items             []Item     `json:"items" parquet:"items,list,optional" parquet-element:",optional"`
	MerchantCreatedAt *time.Time `json:"merchant_account_created_at" parquet:"merchant_account_created_at,optional,timestamp(microsecond:utc)"`
	CustomerCreatedAt *time.Time `json:"customer_account_created_at" parquet:"customer_account_created_at,optional,timestamp(microsecond:utc)"`
	Shipping          *Shipping  `json:"shipping" parquet:"shipping,optional"`
}
type Created struct {
	Merchant *Merchant `json:"merchant" parquet:"merchant,optional"`
	Customer *Customer `json:"customer" parquet:"customer,optional"`
	Snapshot *Checkout `json:"snapshot" parquet:"snapshot,optional"`
}
type Method struct {
	Type        *string `json:"type" parquet:"type,optional"`
	Fingerprint *string `json:"fingerprint" parquet:"fingerprint,optional"`
}
type Signals1 struct {
	AttemptedAt *time.Time `json:"attempted_at" parquet:"attempted_at,optional,timestamp(microsecond:utc)"`
	Method      *Method    `json:"payment_method" parquet:"payment_method,optional"`
	Device      *string    `json:"device_fingerprint" parquet:"device_fingerprint,optional"`
	Country     *string    `json:"ip_country" parquet:"ip_country,optional"`
	Network     *string    `json:"ip_network_fingerprint" parquet:"ip_network_fingerprint,optional"`
}
type Signals2 struct {
	Signals1
	UserAgent *string `json:"user_agent_family" parquet:"user_agent_family,optional"`
}
type Started1 struct {
	AttemptID *string   `json:"attempt_id" parquet:"attempt_id,optional"`
	Signals   *Signals1 `json:"signals" parquet:"signals,optional"`
}
type Started2 struct {
	AttemptID *string   `json:"attempt_id" parquet:"attempt_id,optional"`
	Signals   *Signals2 `json:"signals" parquet:"signals,optional"`
}
type Assessment struct {
	Outcome    *string  `json:"outcome" parquet:"outcome,optional"`
	Reasons    []string `json:"reason_codes" parquet:"reason_codes,list,optional" parquet-element:",optional"`
	Policy     *string  `json:"policy_version" parquet:"policy_version,optional"`
	Model      *string  `json:"model_version" parquet:"model_version,optional"`
	FeatureSet *string  `json:"feature_set_version" parquet:"feature_set_version,optional"`
	Score      *float64 `json:"risk_score" parquet:"risk_score,optional"`
}
type Risk struct {
	AttemptID  *string     `json:"attempt_id" parquet:"attempt_id,optional"`
	Assessment *Assessment `json:"assessment" parquet:"assessment,optional"`
}
type Requested struct {
	AttemptID    *string `json:"attempt_id" parquet:"attempt_id,optional"`
	OperationKey *string `json:"operation_key" parquet:"operation_key,optional"`
}
type Unknown struct {
	AttemptID *string `json:"attempt_id" parquet:"attempt_id,optional"`
}
type Declined struct {
	AttemptID *string `json:"attempt_id" parquet:"attempt_id,optional"`
	Source    *string `json:"source" parquet:"source,optional"`
	Reason    *string `json:"reason" parquet:"reason,optional"`
}
type Succeeded struct {
	AttemptID *string `json:"attempt_id" parquet:"attempt_id,optional"`
	Reference *string `json:"processor_reference" parquet:"processor_reference,optional"`
}
type Expired struct {
	Present *bool `json:"present" parquet:"_present,optional"`
}
type EventBase struct {
	EventID       string    `json:"event_id" parquet:"event_id"`
	PaymentID     string    `json:"payment_id" parquet:"payment_id"`
	Version       int64     `json:"aggregate_version,string" parquet:"aggregate_version"`
	IntegrationID string    `json:"integration_id" parquet:"integration_id"`
	OccurredAt    time.Time `json:"occurred_at" parquet:"occurred_at,timestamp(microsecond:utc)"`
	RecordedAt    time.Time `json:"recorded_at" parquet:"recorded_at,timestamp(microsecond:utc)"`
	SchemaVersion uint32    `json:"schema_version" parquet:"schema_version"`
	CorrelationID string    `json:"correlation_id" parquet:"correlation_id"`
}
type Event1 struct {
	EventBase
	Created   *Created   `json:"payment_created" parquet:"payment_created,optional"`
	Started   *Started1  `json:"attempt_started" parquet:"attempt_started,optional"`
	Risk      *Risk      `json:"risk_decision_recorded" parquet:"risk_decision_recorded,optional"`
	Requested *Requested `json:"processor_operation_requested" parquet:"processor_operation_requested,optional"`
	Unknown   *Unknown   `json:"processor_outcome_unknown" parquet:"processor_outcome_unknown,optional"`
	Declined  *Declined  `json:"attempt_declined" parquet:"attempt_declined,optional"`
	Succeeded *Succeeded `json:"payment_succeeded" parquet:"payment_succeeded,optional"`
	Expired   *Expired   `json:"payment_expired" parquet:"payment_expired,optional"`
	Transport Transport  `json:"-" parquet:"_transport"`
}
type Event2 struct {
	EventBase
	Created   *Created   `json:"payment_created" parquet:"payment_created,optional"`
	Started   *Started2  `json:"attempt_started" parquet:"attempt_started,optional"`
	Risk      *Risk      `json:"risk_decision_recorded" parquet:"risk_decision_recorded,optional"`
	Requested *Requested `json:"processor_operation_requested" parquet:"processor_operation_requested,optional"`
	Unknown   *Unknown   `json:"processor_outcome_unknown" parquet:"processor_outcome_unknown,optional"`
	Declined  *Declined  `json:"attempt_declined" parquet:"attempt_declined,optional"`
	Succeeded *Succeeded `json:"payment_succeeded" parquet:"payment_succeeded,optional"`
	Expired   *Expired   `json:"payment_expired" parquet:"payment_expired,optional"`
	Transport Transport  `json:"-" parquet:"_transport"`
}
type Label struct {
	IntegrationID string    `json:"integration_id" parquet:"integration_id"`
	PaymentID     string    `json:"payment_id" parquet:"payment_id"`
	AttemptID     string    `json:"attempt_id" parquet:"attempt_id"`
	IsFraud       bool      `json:"is_fraud" parquet:"is_fraud"`
	OccurredAt    time.Time `json:"occurred_at" parquet:"occurred_at,timestamp(microsecond:utc)"`
	AvailableAt   time.Time `json:"label_available_at" parquet:"label_available_at,timestamp(microsecond:utc)"`
	Source        string    `json:"source" parquet:"source"`
	Scenario      *string   `json:"scenario" parquet:"scenario,optional"`
	Transport     Transport `json:"-" parquet:"_transport"`
}

type Decoded struct {
	Table   string
	Version uint32
	Row     any
}

func Decode(record *kgo.Record, labelTopic string) (Decoded, error) {
	if record.Value == nil {
		return Decoded{}, fmt.Errorf("tombstone is not a payment or label")
	}
	var message proto.Message
	if record.Topic == labelTopic {
		message = &paymentsv1.FraudLabel{}
	} else {
		message = &paymentsv1.PaymentEvent{}
	}
	if err := proto.Unmarshal(record.Value, message); err != nil {
		return Decoded{}, fmt.Errorf("invalid protobuf")
	}
	table, version := "payment_events", uint32(0)
	var row any
	switch m := message.(type) {
	case *paymentsv1.PaymentEvent:
		if m.EventId == "" || m.PaymentId == "" || m.IntegrationId == "" || m.AggregateVersion == 0 || m.AggregateVersion > math.MaxInt64 || m.Payload == nil || m.OccurredAt == nil || m.RecordedAt == nil {
			return Decoded{}, fmt.Errorf("incomplete payment event")
		}
		if err := m.OccurredAt.CheckValid(); err != nil {
			return Decoded{}, err
		}
		if err := m.RecordedAt.CheckValid(); err != nil {
			return Decoded{}, err
		}
		version = m.SchemaVersion
		switch version {
		case 1:
			if a := m.GetAttemptStarted(); a != nil && a.Signals != nil && a.Signals.UserAgentFamily != nil {
				return Decoded{}, fmt.Errorf("v1 contains v2 signal")
			}
			row = &Event1{Transport: transport(record)}
		case 2:
			row = &Event2{Transport: transport(record)}
		default:
			return Decoded{}, fmt.Errorf("unsupported event schema %d", version)
		}
	case *paymentsv1.FraudLabel:
		if m.PaymentId == "" || m.AttemptId == "" || m.IntegrationId == "" || m.OccurredAt == nil || m.LabelAvailableAt == nil || (m.Source != paymentsv1.LabelSource_LABEL_SOURCE_CHARGEBACK && m.Source != paymentsv1.LabelSource_LABEL_SOURCE_SIMULATION_TRUTH) {
			return Decoded{}, fmt.Errorf("incomplete fraud label")
		}
		if err := m.OccurredAt.CheckValid(); err != nil {
			return Decoded{}, err
		}
		if err := m.LabelAvailableAt.CheckValid(); err != nil {
			return Decoded{}, err
		}
		table, version = "fraud_labels", 1
		row = &Label{Transport: transport(record)}
	}
	body, err := (protojson.MarshalOptions{UseProtoNames: true, EmitDefaultValues: true}).Marshal(message)
	if err != nil {
		return Decoded{}, fmt.Errorf("protobuf projection failed")
	}
	if err = json.Unmarshal(body, row); err != nil {
		return Decoded{}, fmt.Errorf("parquet projection failed: %w", err)
	}
	return Decoded{table, version, row}, nil
}

func Encode(rows []Decoded) ([]byte, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("empty Parquet batch")
	}
	var output bytes.Buffer
	switch rows[0].Row.(type) {
	case *Event1:
		return encodeRows[Event1](&output, rows)
	case *Event2:
		return encodeRows[Event2](&output, rows)
	case *Label:
		return encodeRows[Label](&output, rows)
	default:
		return nil, fmt.Errorf("unsupported Parquet row")
	}
}

func encodeRows[T any](output *bytes.Buffer, rows []Decoded) ([]byte, error) {
	writer := parquet.NewGenericWriter[T](output, parquet.Compression(&zstd.Codec{}))
	for _, r := range rows {
		value, ok := r.Row.(*T)
		if !ok {
			return nil, fmt.Errorf("mixed physical schemas")
		}
		if _, err := writer.Write([]T{*value}); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
