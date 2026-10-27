package feast

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
	featuresv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/features/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testHash = "8ca9a423f452f8ed7a8d1e8aa082dadb72590498cfd1e8da48f94f6c06b0e50d"

var testNow = time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)

func update(scope string) *featuresv1.SnapshotUpdate {
	zero, four, amount, devices := int64(0), int64(4), int64(1<<53+1), int64(2)
	value := &featuresv1.SnapshotUpdate{
		SchemaVersion: 1, Epoch: "live-v1", Definition: &featuresv1.DefinitionIdentity{Version: definitionVersion, Sha256: testHash},
		IntegrationId: "integration-π", EntityId: scope, View: featuresv1.EntityView_ENTITY_VIEW_METHOD, Revision: 2,
		Anchor: timestamppb.New(testNow.Add(-time.Second)), ComputedAt: timestamppb.New(testNow.Add(-time.Second)),
		PublishedAt: timestamppb.New(testNow.Add(-time.Second)), SourceAvailableAt: timestamppb.New(testNow.Add(-time.Second)), OnlineWriteAt: timestamppb.New(testNow.Add(-time.Second)),
		Coverage:       &featuresv1.WindowCoverage{OneHour: featuresv1.Coverage_COVERAGE_COMPLETE, TwentyFourHours: featuresv1.Coverage_COVERAGE_COMPLETE, SevenDays: featuresv1.Coverage_COVERAGE_COMPLETE},
		SourceProgress: []*featuresv1.SourcePartitionProgress{{Topic: "payments.events.v1", Partition: 0, ConsumedOffset: 3, ObservedHighWatermark: 4, ObservedAt: timestamppb.New(testNow.Add(-time.Second))}},
		Values:         &featuresv1.SnapshotValues{AssessedCount_1H: &zero, AssessedCount_24H: &four},
	}
	if scope == "customer" {
		value.View = featuresv1.EntityView_ENTITY_VIEW_CUSTOMER
		value.Values.AssessedAmountMinor_24H = &amount
		value.Values.DistinctDevices_7D = &devices
		value.Values.LastAssessedStartAt = timestamppb.New(testNow.Add(-time.Hour))
	}
	return value
}

func seal(t *testing.T, value *featuresv1.SnapshotUpdate) {
	t.Helper()
	value.SnapshotId = ""
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(raw)
	value.SnapshotId = hex.EncodeToString(digest[:])
}

func servingResponse(t *testing.T, value *featuresv1.SnapshotUpdate, scope string) map[string]any {
	t.Helper()
	seal(t, value)
	metadata := proto.Clone(value).(*featuresv1.SnapshotUpdate)
	metadata.Values = nil
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	fields := methodFields
	if scope == "customer" {
		fields = customerFields
	}
	names := []string{"snapshot_key"}
	results := []map[string]any{{"values": []any{"entity"}, "statuses": []string{"PRESENT"}, "event_timestamps": []time.Time{value.OnlineWriteAt.AsTime()}}}
	for _, field := range fields {
		var content any
		switch field {
		case "snapshot_metadata":
			content = base64.StdEncoding.EncodeToString(encoded)
		case "assessed_count_1h":
			content = value.Values.AssessedCount_1H
		case "assessed_count_24h":
			content = value.Values.AssessedCount_24H
		case "assessed_amount_minor_24h":
			content = value.Values.AssessedAmountMinor_24H
		case "distinct_devices_7d":
			content = value.Values.DistinctDevices_7D
		case "last_assessed_start_us":
			if value.Values.LastAssessedStartAt != nil {
				content = value.Values.LastAssessedStartAt.AsTime().UnixMicro()
			}
		}
		names = append(names, scope+"_snapshot__"+field)
		results = append(results, map[string]any{"values": []any{content}, "statuses": []string{"PRESENT"}, "event_timestamps": []time.Time{value.OnlineWriteAt.AsTime()}})
	}
	return map[string]any{"metadata": map[string]any{"feature_names": names}, "results": results}
}

func request() application.Request {
	return application.Request{Identity: application.Identity{IntegrationID: "integration-π"}, CustomerID: "customer", MethodFingerprint: "method", Input: domain.Input{AttemptedAt: testNow}}
}

func TestIndependentViewsAndCoverage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Features []string            `json:"features"`
			Entities map[string][]string `json:"entities"`
			Full     bool                `json:"full_feature_names"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		scope := "method"
		if strings.HasPrefix(body.Features[0], "customer_snapshot:") {
			scope = "customer"
		}
		if r.URL.Path != "/get-online-features" || !body.Full || len(body.Entities["snapshot_key"]) != 1 || body.Entities["snapshot_key"][0] != entityKey("live-v1", scope, "integration-π", scope) {
			t.Error("unexpected Feast request")
		}
		value := update(scope)
		if scope == "method" {
			value.Coverage.OneHour = featuresv1.Coverage_COVERAGE_UNKNOWN
			value.Values.AssessedCount_1H = nil
		}
		if err := json.NewEncoder(w).Encode(servingResponse(t, value, scope)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := New(Config{URL: server.URL, Epoch: "live-v1", DefinitionHash: testHash, Timeout: time.Second, MaxAnchorAge: time.Minute, MaxProgressAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.now = func() time.Time { return testNow }
	customer, method := client.Read(context.Background(), request())
	if customer.Status != "available" || customer.Values.Count1h == nil || *customer.Values.Count1h != 0 || customer.Values.AmountMinor24h == nil || *customer.Values.AmountMinor24h != 1<<53+1 || customer.ElapsedSeconds == nil || *customer.ElapsedSeconds != 3600 {
		t.Fatalf("customer=%+v", customer)
	}
	if method.Status != "partial" || method.Values.Count1h != nil || method.Values.Count24h == nil || *method.Values.Count24h != 4 {
		t.Fatalf("method=%+v", method)
	}
	if customer.Observed == nil || len(customer.Observed.SourceProgress) != 1 || customer.AvailableAt == nil || !customer.ReadAt.Equal(testNow) {
		t.Fatalf("missing serving evidence: %+v", customer)
	}
}

func TestUnavailableViewsNeverSupplyQuantities(t *testing.T) {
	cases := map[string]struct {
		change func(*featuresv1.SnapshotUpdate)
		status string
	}{
		"old anchor":     {func(v *featuresv1.SnapshotUpdate) { v.Anchor = timestamppb.New(testNow.Add(-2 * time.Minute)) }, "stale"},
		"lagging source": {func(v *featuresv1.SnapshotUpdate) { v.SourceProgress[0].ObservedHighWatermark = 8 }, "stale"},
		"future anchor": {func(v *featuresv1.SnapshotUpdate) {
			v.Anchor = timestamppb.New(testNow.Add(time.Second))
			v.ComputedAt = v.Anchor
			v.PublishedAt = v.Anchor
		}, "future"},
		"other epoch":      {func(v *featuresv1.SnapshotUpdate) { v.Epoch = "replay" }, "incompatible"},
		"other definition": {func(v *featuresv1.SnapshotUpdate) { v.Definition.Sha256 = strings.Repeat("0", 64) }, "incompatible"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				v := update("customer")
				tc.change(v)
				_ = json.NewEncoder(w).Encode(servingResponse(t, v, "customer"))
			}))
			defer server.Close()
			client, err := New(Config{URL: server.URL, Epoch: "live-v1", DefinitionHash: testHash, Timeout: time.Second, MaxAnchorAge: time.Minute, MaxProgressAge: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			client.now = func() time.Time { return testNow }
			result := client.read(context.Background(), request(), "customer", "customer")
			if result.Status != tc.status || result.Values.Count1h != nil || result.Values.AmountMinor24h != nil || result.ElapsedSeconds != nil {
				t.Fatalf("observation=%+v", result)
			}
		})
	}
}

func TestTimeoutAndMalformedResponseRemainIndependent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Features []string `json:"features"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if strings.HasPrefix(body.Features[0], "customer_snapshot:") {
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte(`{"metadata":{"feature_names":["method_snapshot__snapshot_metadata"]},"results":[]}`))
	}))
	defer server.Close()
	client, err := New(Config{URL: server.URL, Epoch: "live-v1", DefinitionHash: testHash, Timeout: 30 * time.Millisecond, MaxAnchorAge: time.Minute, MaxProgressAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	customer, method := client.Read(context.Background(), request())
	if customer.Status != "timeout" || method.Status != "error" {
		t.Fatalf("customer=%s method=%s", customer.Status, method.Status)
	}
}

func TestElapsedUsesRequestHorizonWithoutErasingSnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		value := update("customer")
		value.Values.LastAssessedStartAt = timestamppb.New(testNow.Add(-7*24*time.Hour - time.Microsecond))
		_ = json.NewEncoder(w).Encode(servingResponse(t, value, "customer"))
	}))
	defer server.Close()
	client, err := New(Config{URL: server.URL, Epoch: "live-v1", DefinitionHash: testHash, Timeout: time.Second, MaxAnchorAge: time.Minute, MaxProgressAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.now = func() time.Time { return testNow }
	result := client.read(context.Background(), request(), "customer", "customer")
	if result.Status != "available" || result.ElapsedSeconds != nil || result.Values.Count24h == nil || result.Observed.Values.LastAssessedAt == nil {
		t.Fatalf("elapsed/observed history=%+v", result)
	}
}
