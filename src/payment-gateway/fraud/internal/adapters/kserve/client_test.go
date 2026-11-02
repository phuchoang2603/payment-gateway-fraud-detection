package kserve

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
)

var fixedNow = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

func newClient(t *testing.T, url string, timeout time.Duration) *Client {
	t.Helper()
	client, err := New(Config{URL: url, Model: "payment-risk", DialTimeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	client.now = func() time.Time { return fixedNow }
	t.Cleanup(client.Close)
	return client
}

func integer(value int64) application.FeatureValue { return application.FeatureValue{Integer: &value} }

func number(value float64) application.FeatureValue { return application.FeatureValue{Number: &value} }

func boolean(value bool) application.FeatureValue { return application.FeatureValue{Boolean: &value} }

func views(v map[string]application.ViewObservation) application.FeatureObservation {
	return application.FeatureObservation{Service: "payment_risk_v1", Views: v}
}

func TestTheInstanceNamesOnlyAvailableViewValuesAndKeepsUnknownsNull(t *testing.T) {
	row := instance(views(map[string]application.ViewObservation{
		"customer_daily":        {Status: "available", Values: map[string]application.FeatureValue{"attempt_count_7d": integer(3), "decline_rate_7d": number(0.25), "unknown": {}}},
		"request_features":      {Status: "available", Values: map[string]application.FeatureValue{"has_device": boolean(true), "ip_shipping_country_mismatch": {}}},
		"payment_method_daily":  {Status: "expired", Values: map[string]application.FeatureValue{"attempt_count_7d": integer(9)}},
		"customer_stream":       {Status: "missing"},
		"payment_method_stream": {Status: "timeout", Values: map[string]application.FeatureValue{"x": integer(1)}},
	}))
	want := map[string]any{
		"customer_daily__attempt_count_7d":               int64(3),
		"customer_daily__decline_rate_7d":                0.25,
		"customer_daily__unknown":                        nil,
		"request_features__has_device":                   true,
		"request_features__ip_shipping_country_mismatch": nil,
	}
	if len(row) != len(want) {
		t.Fatalf("instance %v", row)
	}
	for key, value := range want {
		if got, exists := row[key]; !exists || got != value {
			t.Fatalf("%s = %v (present %v), want %v", key, got, exists, value)
		}
	}
}

func TestAScoreIsRequestedFromTheNamedModelAndRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Instances []map[string]any `json:"instances"`
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/models/payment-risk:predict" || r.Header.Get("Content-Type") != "application/json" || json.NewDecoder(r.Body).Decode(&body) != nil || len(body.Instances) != 1 || body.Instances[0]["customer_daily__attempt_count_7d"] != float64(3) {
			t.Errorf("unexpected request %s %s %+v", r.Method, r.URL.Path, body)
		}
		_, _ = w.Write([]byte(`{"predictions":[0.25],"model_version":"v3"}`))
	}))
	defer server.Close()
	features := views(map[string]application.ViewObservation{"customer_daily": {Status: "available", Values: map[string]application.FeatureValue{"attempt_count_7d": integer(3)}}})
	observation := newClient(t, server.URL+"/", time.Second).Score(t.Context(), features)
	if observation.Status != application.ModelAvailable || observation.Version != "v3" || observation.Name != "payment-risk" || observation.Score == nil || *observation.Score != 0.25 || !observation.ReadAt.Equal(fixedNow) {
		t.Fatalf("observation %+v", observation)
	}
}

func TestTheScoreBoundariesAreAccepted(t *testing.T) {
	for _, score := range []string{"0", "1", "0.0"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"predictions":[` + score + `],"model_version":"v1"}`))
		}))
		observation := newClient(t, server.URL, time.Second).Score(t.Context(), views(nil))
		server.Close()
		if observation.Status != application.ModelAvailable || observation.Score == nil {
			t.Fatalf("score %s: %+v", score, observation)
		}
	}
}

func TestAnythingElseIsAnErrorWithoutAScoreOrVersion(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"server error", http.StatusInternalServerError, `{"predictions":[0.5],"model_version":"v1"}`},
		{"not found", http.StatusNotFound, `{"predictions":[0.5],"model_version":"v1"}`},
		{"redirect", http.StatusTemporaryRedirect, `{"predictions":[0.5],"model_version":"v1"}`},
		{"no predictions", http.StatusOK, `{"predictions":[],"model_version":"v1"}`},
		{"null predictions", http.StatusOK, `{"predictions":null,"model_version":"v1"}`},
		{"two predictions", http.StatusOK, `{"predictions":[0.1,0.2],"model_version":"v1"}`},
		{"below zero", http.StatusOK, `{"predictions":[-0.0001],"model_version":"v1"}`},
		{"above one", http.StatusOK, `{"predictions":[1.0001],"model_version":"v1"}`},
		{"no version", http.StatusOK, `{"predictions":[0.5]}`},
		{"empty version", http.StatusOK, `{"predictions":[0.5],"model_version":""}`},
		{"text score", http.StatusOK, `{"predictions":["0.5"],"model_version":"v1"}`},
		{"not json", http.StatusOK, `score=0.5`},
		{"empty body", http.StatusOK, ``},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			observation := newClient(t, server.URL, time.Second).Score(t.Context(), views(nil))
			if observation.Status != application.ModelError || observation.Score != nil || observation.Version != "" || observation.Name != "payment-risk" || !observation.ReadAt.Equal(fixedNow) {
				t.Fatalf("observation %+v", observation)
			}
		})
	}
}

func TestTheAnswerSizeLimitIsExact(t *testing.T) {
	body := `{"predictions":[0.5],"model_version":"v1"}`
	for _, test := range []struct {
		size   int
		status string
	}{{maxAnswer, application.ModelAvailable}, {maxAnswer + 1, application.ModelError}} {
		padded := body + strings.Repeat(" ", test.size-len(body))
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(padded)) }))
		observation := newClient(t, server.URL, time.Second).Score(t.Context(), views(nil))
		server.Close()
		if observation.Status != test.status {
			t.Fatalf("a %d byte answer gave %+v", test.size, observation)
		}
	}
}

func TestASlowPredictorTimesOutAndADeadOneIsAnError(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	deadline, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	slow := newClient(t, server.URL, time.Second).Score(deadline, views(nil))
	if slow.Status != application.ModelTimeout || slow.Score != nil {
		t.Fatalf("slow predictor: %+v", slow)
	}
	dead := httptest.NewServer(http.NotFoundHandler())
	address := dead.URL
	dead.Close()
	if refused := newClient(t, address, time.Second).Score(t.Context(), views(nil)); refused.Status != application.ModelError || refused.Score != nil {
		t.Fatalf("closed predictor: %+v", refused)
	}
}

func TestAnEndpointOrNameThatCouldMisdirectTheCallIsRejected(t *testing.T) {
	valid := Config{URL: "http://payment-risk-predictor.ml.svc.cluster.local", Model: "payment-risk", DialTimeout: time.Second}
	if _, err := New(valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Config){
		"https":        func(c *Config) { c.URL = "https://predictor" },
		"no host":      func(c *Config) { c.URL = "http://" },
		"credentials":  func(c *Config) { c.URL = "http://user:pw@predictor" },
		"query":        func(c *Config) { c.URL = "http://predictor?x=1" },
		"fragment":     func(c *Config) { c.URL = "http://predictor#x" },
		"not a url":    func(c *Config) { c.URL = "::" },
		"empty name":   func(c *Config) { c.Model = "" },
		"upper case":   func(c *Config) { c.Model = "Payment-Risk" },
		"path in name": func(c *Config) { c.Model = "a/b" },
		"colon":        func(c *Config) { c.Model = "a:predict" },
		"leading dash": func(c *Config) { c.Model = "-a" },
		"no timeout":   func(c *Config) { c.DialTimeout = 0 },
	} {
		config := valid
		mutate(&config)
		if _, err := New(config); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}
