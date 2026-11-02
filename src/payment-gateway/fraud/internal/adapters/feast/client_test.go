package feast

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
)

func TestViewAvailabilityBoundaries(t *testing.T) {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		timestamp time.Time
		value     json.RawMessage
		status    string
	}{
		{"TTL edge", now.Add(-time.Hour), json.RawMessage("0"), "available"},
		{"expired", now.Add(-time.Hour - time.Microsecond), json.RawMessage("4"), "expired"},
		{"future", now.Add(time.Microsecond), json.RawMessage("4"), "future"},
		{"missing", time.Unix(0, 0), json.RawMessage("null"), "missing"},
		{"malformed", now, json.RawMessage(`"4"`), "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]featureResult{"customer_stream__customer_snapshot_attempt_count_5m": {Values: []json.RawMessage{tc.value}, Statuses: []string{"PRESENT"}, EventTimestamps: []time.Time{tc.timestamp}}}
			result := evaluate("customer_stream", []string{"customer_snapshot_attempt_count_5m"}, fields, now, time.Hour)
			if result.Status != tc.status {
				t.Fatalf("status = %s, want %s", result.Status, tc.status)
			}
			if tc.status != "available" && result.Values != nil {
				t.Fatal("unavailable view exposed values")
			}
		})
	}
}

func TestStoreAndForwardUsesServerScoringTimeForRequestAndViewAvailability(t *testing.T) {
	calls := 0
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request struct {
			FeatureService   string           `json:"feature_service"`
			Entities         map[string][]any `json:"entities"`
			FullFeatureNames bool             `json:"full_feature_names"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.FeatureService != "payment_risk_v1" || !request.FullFeatureNames || request.Entities["integration_id"][0] != "integration" || len(request.Entities) != 11 || request.Entities["has_device"][0] != false || request.Entities["ip_country"][0] != nil || request.Entities["amount_minor"][0] != float64(500) || request.Entities["item_count"][0] != float64(2) || request.Entities["scored_at"][0] != now.Format(time.RFC3339Nano) || request.Entities["merchant_account_created_at"][0] != now.Add(-24*time.Hour).Format(time.RFC3339Nano) {
			t.Error("invalid feature service request")
		}
		reply := response{}
		for view, fields := range viewFields {
			for _, field := range fields {
				reply.Metadata.FeatureNames = append(reply.Metadata.FeatureNames, view+"__"+field)
				value := json.RawMessage("null")
				if view == "request_features" {
					value = json.RawMessage("1")
					if field == "has_device" {
						value = json.RawMessage("false")
					}
					if field == "ip_shipping_country_mismatch" {
						value = json.RawMessage("null")
					}
				}
				timestamp := now.Add(-time.Minute)
				if view == "payment_method_stream" {
					timestamp = now.Add(time.Microsecond)
				}
				if view == "customer_daily" {
					timestamp = now.Add(-time.Hour - time.Microsecond)
				}
				if view == "request_features" {
					timestamp = time.Unix(0, 0).UTC()
				}
				reply.Results = append(reply.Results, featureResult{Values: []json.RawMessage{value}, Statuses: []string{"PRESENT"}, EventTimestamps: []time.Time{timestamp}})
			}
		}
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := New(Config{URL: server.URL, FeatureService: "payment_risk_v1", Timeout: time.Second, DailyTTL: time.Hour, StreamTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.now = func() time.Time { return now }
	observation := client.Read(context.Background(), application.Request{ScoredAt: now, Identity: application.Identity{IntegrationID: "integration"}, CustomerID: "customer", MethodFingerprint: "method", MerchantAccountCreatedAt: now.Add(-24 * time.Hour), ItemCount: 2, Input: domain.Input{AttemptedAt: now.Add(-10 * time.Minute), AmountMinor: 500}})
	if calls != 1 || len(observation.Views) != 5 {
		t.Fatalf("calls = %d, views = %d", calls, len(observation.Views))
	}
	for name, view := range observation.Views {
		want := "available"
		if name == "customer_daily" {
			want = "expired"
		}
		if name == "payment_method_stream" {
			want = "future"
		}
		if view.Status != want {
			t.Fatalf("%s status = %s, want %s", name, view.Status, want)
		}
	}
	features := observation.Views["request_features"].Values
	if features["customer_amount_to_30d_avg"].Number != nil || features["payment_method_amount_to_30d_avg"].Number == nil {
		t.Fatalf("expired daily view must null its ratio only: %+v", features)
	}
}

func TestRequestFeatureBooleansAndNullRetainScoringTime(t *testing.T) {
	now := time.Now().UTC()
	fields := map[string]featureResult{}
	for _, name := range viewFields["request_features"] {
		value := json.RawMessage("1")
		if nullableRequestFeature(name) {
			value = json.RawMessage("null")
		}
		if name == "customer_amount_to_30d_avg" {
			value = json.RawMessage("2.5")
		}
		if name == "has_device" {
			value = json.RawMessage("false")
		}
		fields["request_features__"+name] = featureResult{Values: []json.RawMessage{value}, Statuses: []string{"PRESENT"}, EventTimestamps: []time.Time{time.Unix(0, 0)}}
	}
	result := evaluate("request_features", viewFields["request_features"], fields, now, time.Hour)
	if result.Status != "available" || !result.EventTimestamp.Equal(now) || result.Values["has_device"].Boolean == nil || *result.Values["has_device"].Boolean || result.Values["ip_shipping_country_mismatch"].Boolean != nil || result.Values["customer_amount_to_30d_avg"].Number == nil || *result.Values["customer_amount_to_30d_avg"].Number != 2.5 || result.Values["payment_method_amount_to_30d_avg"].Number != nil {
		t.Fatalf("request features lost null, false or scoring time: %+v", result)
	}
}

func TestTheViewsReadAreExactlyTheViewsAnEnforcedModelScoreNeeds(t *testing.T) {
	read := map[string]bool{}
	for view := range viewFields {
		read[view] = true
	}
	if len(read) != len(application.RequiredViews) {
		t.Fatalf("the adapter reads %d views, the policy requires %d", len(read), len(application.RequiredViews))
	}
	for _, view := range application.RequiredViews {
		if !read[view] {
			t.Fatalf("the policy requires %s, which the adapter does not read", view)
		}
	}
}
