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

func TestOneNaturalKeyServiceRequest(t *testing.T) {
	calls := 0
	now := time.Now().UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request struct {
			FeatureService   string              `json:"feature_service"`
			Entities         map[string][]string `json:"entities"`
			FullFeatureNames bool                `json:"full_feature_names"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.FeatureService != "fraud_model_v1" || !request.FullFeatureNames || request.Entities["integration_id"][0] != "integration" || len(request.Entities) != 3 {
			t.Error("invalid feature service request")
		}
		reply := response{}
		for view, fields := range viewFields {
			for _, field := range fields {
				reply.Metadata.FeatureNames = append(reply.Metadata.FeatureNames, view+"__"+field)
				reply.Results = append(reply.Results, featureResult{Values: []json.RawMessage{json.RawMessage("null")}, Statuses: []string{"PRESENT"}, EventTimestamps: []time.Time{now}})
			}
		}
		if err := json.NewEncoder(w).Encode(reply); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := New(Config{URL: server.URL, FeatureService: "fraud_model_v1", Timeout: time.Second, DailyTTL: time.Hour, StreamTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	observation := client.Read(context.Background(), application.Request{Identity: application.Identity{IntegrationID: "integration"}, CustomerID: "customer", MethodFingerprint: "method", Input: domain.Input{AttemptedAt: now}})
	if calls != 1 || len(observation.Views) != 4 {
		t.Fatalf("calls = %d, views = %d", calls, len(observation.Views))
	}
	for _, view := range observation.Views {
		if view.Status != "available" {
			t.Fatalf("null coverage status = %s", view.Status)
		}
	}
}
