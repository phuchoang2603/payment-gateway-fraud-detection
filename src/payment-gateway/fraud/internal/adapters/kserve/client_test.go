package kserve

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
)

var fixedNow = time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)

var order = []string{"customer_daily__attempt_count_7d", "customer_daily__decline_rate_7d", "request_features__has_device", "request_features__ip_shipping_country_mismatch", "customer_stream__customer_snapshot_attempt_count_5m"}

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

// triton is a fake Triton server for one model: its config parameters, its answer to an infer call and a count of both.
type triton struct {
	registryVersion string
	featureOrder    string
	modelVersion    string
	probabilities   [2]float32
	status          int
	configBody      string
	inferHeader     func(map[string]any)
	inferData       func([]byte) []byte
	headerLength    func(int) string
	configCalls     atomic.Int32
	inferCalls      atomic.Int32
	lastRow         atomic.Value
}

func newTriton() *triton {
	encoded, _ := json.Marshal(order)
	return &triton{registryVersion: "v3", featureOrder: string(encoded), modelVersion: "3", probabilities: [2]float32{0.75, 0.25}, status: http.StatusOK}
}

func (f *triton) serve(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v2/models/payment-risk/config":
			f.configCalls.Add(1)
			w.WriteHeader(f.status)
			body := f.configBody
			if body == "" {
				body = `{"name":"payment-risk","parameters":{"feature_order":{"string_value":` + strconv.Quote(f.featureOrder) + `},"registry_version":{"string_value":"` + f.registryVersion + `"}}}`
			}
			_, _ = w.Write([]byte(body))
		case r.Method == http.MethodPost && r.URL.Path == "/v2/models/payment-risk/infer":
			f.inferCalls.Add(1)
			f.answer(t, w, r)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func (f *triton) answer(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	size, err := strconv.Atoi(r.Header.Get(headerLength))
	buffer := make([]byte, r.ContentLength)
	_, _ = io.ReadFull(r.Body, buffer)
	if r.Header.Get("Content-Type") != "application/octet-stream" || err != nil || size <= 0 || size > len(buffer) {
		t.Errorf("bad request framing: %v %d", r.Header, len(buffer))
		return
	}
	var request struct {
		Inputs []struct {
			Name       string `json:"name"`
			Shape      []int  `json:"shape"`
			Datatype   string `json:"datatype"`
			Parameters struct {
				BinaryDataSize int `json:"binary_data_size"`
			} `json:"parameters"`
		} `json:"inputs"`
		Outputs []struct {
			Name       string `json:"name"`
			Parameters struct {
				Binary bool `json:"binary_data"`
			} `json:"parameters"`
		} `json:"outputs"`
	}
	if json.Unmarshal(buffer[:size], &request) != nil || len(request.Inputs) != 1 || len(request.Outputs) != 1 {
		t.Errorf("request header %s", buffer[:size])
		return
	}
	input, data := request.Inputs[0], buffer[size:]
	if input.Name != "input__0" || input.Datatype != "FP32" || len(input.Shape) != 2 || input.Shape[0] != 1 || input.Shape[1] != len(order) || input.Parameters.BinaryDataSize != 4*len(order) || len(data) != 4*len(order) || request.Outputs[0].Name != "output__0" || !request.Outputs[0].Parameters.Binary {
		t.Errorf("request %+v with %d bytes", request, len(data))
		return
	}
	values := make([]float32, len(order))
	for index := range values {
		values[index] = math.Float32frombits(binary.LittleEndian.Uint32(data[4*index:]))
	}
	f.lastRow.Store(values)
	header := map[string]any{"model_name": "payment-risk", "model_version": f.modelVersion, "outputs": []any{map[string]any{"name": "output__0", "datatype": "FP32", "shape": []int{1, 2}, "parameters": map[string]any{"binary_data_size": 8}}}}
	if f.inferHeader != nil {
		f.inferHeader(header)
	}
	encoded, _ := json.Marshal(header)
	tensor := binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, math.Float32bits(f.probabilities[0])), math.Float32bits(f.probabilities[1]))
	if f.inferData != nil {
		tensor = f.inferData(tensor)
	}
	length := strconv.Itoa(len(encoded))
	if f.headerLength != nil {
		length = f.headerLength(len(encoded))
	}
	w.Header().Set(headerLength, length)
	w.WriteHeader(f.status)
	_, _ = w.Write(append(encoded, tensor...))
}

func TestTheRowFollowsTheModelOrderAndMarksEverythingUnknownMissing(t *testing.T) {
	got := row(views(map[string]application.ViewObservation{
		"customer_daily":   {Status: "available", Values: map[string]application.FeatureValue{"attempt_count_7d": integer(3), "decline_rate_7d": number(0.25), "unrelated": integer(9)}},
		"request_features": {Status: "available", Values: map[string]application.FeatureValue{"has_device": boolean(true), "ip_shipping_country_mismatch": {}}},
		"customer_stream":  {Status: "expired", Values: map[string]application.FeatureValue{"customer_snapshot_attempt_count_5m": integer(4)}},
	}), order)
	want := []float32{3, 0.25, 1, float32(math.NaN()), float32(math.NaN())}
	for index := range want {
		if math.IsNaN(float64(want[index])) != math.IsNaN(float64(got[index])) || (!math.IsNaN(float64(want[index])) && got[index] != want[index]) {
			t.Fatalf("row %v, want %v", got, want)
		}
	}
	if falsy := row(views(map[string]application.ViewObservation{"request_features": {Status: "available", Values: map[string]application.FeatureValue{"has_device": boolean(false)}}}), []string{"request_features__has_device"}); falsy[0] != 0 {
		t.Fatalf("false is %v", falsy)
	}
	if odd := row(views(nil), []string{"no_separator", "missing_view__x"}); !math.IsNaN(float64(odd[0])) || !math.IsNaN(float64(odd[1])) {
		t.Fatalf("unknown names %v", odd)
	}
}

func TestAScoreIsRequestedInTheModelOrderAndTheFraudProbabilityRead(t *testing.T) {
	fake := newTriton()
	server := fake.serve(t)
	features := views(map[string]application.ViewObservation{
		"customer_daily":   {Status: "available", Values: map[string]application.FeatureValue{"attempt_count_7d": integer(3)}},
		"request_features": {Status: "available", Values: map[string]application.FeatureValue{"has_device": boolean(true)}},
	})
	observation := newClient(t, server.URL+"/", time.Second).Score(t.Context(), features)
	if observation.Status != application.ModelAvailable || observation.Version != "v3" || observation.Name != "payment-risk" || observation.Score == nil || *observation.Score != 0.25 || !observation.ReadAt.Equal(fixedNow) {
		t.Fatalf("observation %+v", observation)
	}
	sent := fake.lastRow.Load().([]float32)
	if sent[0] != 3 || sent[2] != 1 || !math.IsNaN(float64(sent[1])) || !math.IsNaN(float64(sent[3])) || !math.IsNaN(float64(sent[4])) {
		t.Fatalf("sent row %v", sent)
	}
}

func TestTheOrderIsReadOnceAndReusedAcrossScores(t *testing.T) {
	fake := newTriton()
	server := fake.serve(t)
	client := newClient(t, server.URL, time.Second)
	for range 3 {
		if observation := client.Score(t.Context(), views(nil)); observation.Status != application.ModelAvailable {
			t.Fatalf("observation %+v", observation)
		}
	}
	if fake.configCalls.Load() != 1 || fake.inferCalls.Load() != 3 {
		t.Fatalf("config reads %d, infer calls %d", fake.configCalls.Load(), fake.inferCalls.Load())
	}
}

func TestTheScoreBoundariesAreAccepted(t *testing.T) {
	for _, probability := range []float32{0, 1} {
		fake := newTriton()
		fake.probabilities = [2]float32{1 - probability, probability}
		observation := newClient(t, fake.serve(t).URL, time.Second).Score(t.Context(), views(nil))
		if observation.Status != application.ModelAvailable || observation.Score == nil || float32(*observation.Score) != probability {
			t.Fatalf("score %v: %+v", probability, observation)
		}
	}
}

func TestAScoreFromAnotherVersionThanTheOrderIsDiscardedAndTheOrderReadAgain(t *testing.T) {
	fake := newTriton()
	server := fake.serve(t)
	client := newClient(t, server.URL, time.Second)
	if first := client.Score(t.Context(), views(nil)); first.Status != application.ModelAvailable || first.Version != "v3" {
		t.Fatalf("first %+v", first)
	}
	fake.modelVersion = "4"
	if stale := client.Score(t.Context(), views(nil)); stale.Status != application.ModelError || stale.Score != nil || stale.Version != "" {
		t.Fatalf("a v4 score against a v3 order was used: %+v", stale)
	}
	fake.registryVersion = "v4"
	if next := client.Score(t.Context(), views(nil)); next.Status != application.ModelAvailable || next.Version != "v4" {
		t.Fatalf("after the order was read again: %+v", next)
	}
	if fake.configCalls.Load() != 2 {
		t.Fatalf("config reads %d", fake.configCalls.Load())
	}
}

func TestAnythingElseIsAnErrorWithoutAScoreOrVersion(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*triton)
	}{
		{"server error", func(f *triton) { f.status = http.StatusInternalServerError }},
		{"config not json", func(f *triton) { f.configBody = `parameters` }},
		{"config without parameters", func(f *triton) { f.configBody = `{}` }},
		{"order not json", func(f *triton) { f.featureOrder = `customer_daily__a` }},
		{"empty order", func(f *triton) { f.featureOrder = `[]` }},
		{"no registry version", func(f *triton) { f.registryVersion = "" }},
		{"registry version without v", func(f *triton) { f.registryVersion = "3" }},
		{"registry version with text", func(f *triton) { f.registryVersion = "v3x" }},
		{"no header length", func(f *triton) { f.headerLength = func(int) string { return "" } }},
		{"zero header length", func(f *triton) { f.headerLength = func(int) string { return "0" } }},
		{"header longer than the body", func(f *triton) { f.headerLength = func(n int) string { return strconv.Itoa(n + 1000) } }},
		{"header length not a number", func(f *triton) { f.headerLength = func(int) string { return "x" } }},
		{"version missing", func(f *triton) { f.modelVersion = "" }},
		{"version with text", func(f *triton) { f.modelVersion = "3a" }},
		{"no outputs", func(f *triton) { f.inferHeader = func(h map[string]any) { h["outputs"] = []any{} } }},
		{"two outputs", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"] = append(h["outputs"].([]any), h["outputs"].([]any)[0]) }
		}},
		{"wrong output name", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"].([]any)[0].(map[string]any)["name"] = "other" }
		}},
		{"wrong datatype", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"].([]any)[0].(map[string]any)["datatype"] = "FP64" }
		}},
		{"one class", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"].([]any)[0].(map[string]any)["shape"] = []int{1, 1} }
		}},
		{"three classes", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"].([]any)[0].(map[string]any)["shape"] = []int{1, 3} }
		}},
		{"two rows", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"].([]any)[0].(map[string]any)["shape"] = []int{2, 2} }
		}},
		{"flat shape", func(f *triton) {
			f.inferHeader = func(h map[string]any) { h["outputs"].([]any)[0].(map[string]any)["shape"] = []int{2} }
		}},
		{"wrong binary size", func(f *triton) {
			f.inferHeader = func(h map[string]any) {
				h["outputs"].([]any)[0].(map[string]any)["parameters"] = map[string]any{"binary_data_size": 4}
			}
		}},
		{"short tensor", func(f *triton) { f.inferData = func(b []byte) []byte { return b[:4] } }},
		{"long tensor", func(f *triton) { f.inferData = func(b []byte) []byte { return append(b, 0) } }},
		{"below zero", func(f *triton) { f.probabilities = [2]float32{1, -0.001} }},
		{"above one", func(f *triton) { f.probabilities = [2]float32{0, 1.001} }},
		{"not a number", func(f *triton) { f.probabilities = [2]float32{0, float32(math.NaN())} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := newTriton()
			test.setup(fake)
			observation := newClient(t, fake.serve(t).URL, time.Second).Score(t.Context(), views(nil))
			if observation.Status != application.ModelError || observation.Score != nil || observation.Version != "" || observation.Name != "payment-risk" || !observation.ReadAt.Equal(fixedNow) {
				t.Fatalf("observation %+v", observation)
			}
		})
	}
}

func TestOnlyTheProbabilityOfTheFraudClassIsRead(t *testing.T) {
	fake := newTriton()
	fake.probabilities = [2]float32{0.9, 0.125}
	observation := newClient(t, fake.serve(t).URL, time.Second).Score(t.Context(), views(nil))
	if observation.Score == nil || *observation.Score != 0.125 {
		t.Fatalf("observation %+v", observation)
	}
}

func TestAnAnswerFromTheWrongPathOrWithARedirectIsAnError(t *testing.T) {
	server := httptest.NewServer(http.RedirectHandler("http://elsewhere.invalid/", http.StatusTemporaryRedirect))
	defer server.Close()
	if observation := newClient(t, server.URL, time.Second).Score(t.Context(), views(nil)); observation.Status != application.ModelError || observation.Score != nil {
		t.Fatalf("observation %+v", observation)
	}
}

func TestTheAnswerSizeLimitIsExact(t *testing.T) {
	for _, test := range []struct {
		size   int
		status string
	}{{maxAnswer, application.ModelAvailable}, {maxAnswer + 1, application.ModelError}} {
		fake := newTriton()
		fake.inferHeader = func(h map[string]any) {
			h["padding"] = ""
			encoded, _ := json.Marshal(h)
			h["padding"] = strings.Repeat("x", test.size-8-len(encoded))
		}
		observation := newClient(t, fake.serve(t).URL, time.Second).Score(t.Context(), views(nil))
		if observation.Status != test.status {
			t.Fatalf("a %d byte answer gave %+v", test.size, observation)
		}
	}
	fake := newTriton()
	fake.configBody = `{"parameters":{"feature_order":{"string_value":"[\"a__b\"]"},"registry_version":{"string_value":"v3"},"padding":{"string_value":"` + strings.Repeat("x", maxAnswer) + `"}}}`
	if observation := newClient(t, fake.serve(t).URL, time.Second).Score(t.Context(), views(nil)); observation.Status != application.ModelError {
		t.Fatalf("an oversized config gave %+v", observation)
	}
}

func TestASlowModelServerTimesOutAndADeadOneIsAnError(t *testing.T) {
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
		t.Fatalf("slow config: %+v", slow)
	}
	fake := newTriton()
	stall := make(chan struct{})
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			fake.serveConfig(w)
			return
		}
		select {
		case <-stall:
		case <-r.Context().Done():
		}
	}))
	defer healthy.Close()
	defer close(stall)
	late, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer stop()
	if slowInfer := newClient(t, healthy.URL, time.Second).Score(late, views(nil)); slowInfer.Status != application.ModelTimeout || slowInfer.Score != nil {
		t.Fatalf("slow infer: %+v", slowInfer)
	}
	dead := httptest.NewServer(http.NotFoundHandler())
	address := dead.URL
	dead.Close()
	if refused := newClient(t, address, time.Second).Score(t.Context(), views(nil)); refused.Status != application.ModelError || refused.Score != nil {
		t.Fatalf("closed model server: %+v", refused)
	}
}

func (f *triton) serveConfig(w http.ResponseWriter) {
	_, _ = w.Write([]byte(`{"parameters":{"feature_order":{"string_value":` + strconv.Quote(f.featureOrder) + `},"registry_version":{"string_value":"` + f.registryVersion + `"}}}`))
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

// bins records the bin each value is counted into, as "feature/bin/version".
type bins struct {
	noop.Int64Counter
	seen map[string]int64
}

func (b *bins) Add(_ context.Context, amount int64, options ...metric.AddOption) {
	set := metric.NewAddConfig(options).Attributes()
	feature, _ := set.Value("feature")
	bin, _ := set.Value("bin")
	version, _ := set.Value("model_version")
	b.seen[feature.AsString()+"/"+bin.AsString()+"/"+version.AsString()] += amount
}

func profiled(t *testing.T, edges string) (*Client, *triton, *bins) {
	t.Helper()
	fake := newTriton()
	encoded, _ := json.Marshal(order)
	fake.configBody = `{"parameters":{"feature_order":{"string_value":` + strconv.Quote(string(encoded)) + `},"registry_version":{"string_value":"v3"},"bin_edges":{"string_value":` + strconv.Quote(edges) + `}}}`
	counter := &bins{seen: map[string]int64{}}
	client, err := New(Config{URL: fake.serve(t).URL, Model: "payment-risk", DialTimeout: time.Second, Bins: counter})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, fake, counter
}

func TestScoredFeaturesAreCountedIntoTheBinsOfTheServedProfile(t *testing.T) {
	edges := `{"customer_daily__attempt_count_7d":[2,5],"customer_daily__decline_rate_7d":[0.5],"request_features__has_device":[1],"request_features__ip_shipping_country_mismatch":[1],"customer_stream__customer_snapshot_attempt_count_5m":[3]}`
	client, _, counter := profiled(t, edges)
	observation := client.Score(context.Background(), views(map[string]application.ViewObservation{
		"customer_daily":   {Status: "available", Values: map[string]application.FeatureValue{"attempt_count_7d": integer(5), "decline_rate_7d": number(0.1)}},
		"request_features": {Status: "available", Values: map[string]application.FeatureValue{"has_device": boolean(true), "ip_shipping_country_mismatch": boolean(false)}},
	}))
	if observation.Status != application.ModelAvailable {
		t.Fatalf("status %s", observation.Status)
	}
	want := map[string]int64{
		"customer_daily__attempt_count_7d/2/v3":                    1,
		"customer_daily__decline_rate_7d/0/v3":                     1,
		"request_features__has_device/1/v3":                        1,
		"request_features__ip_shipping_country_mismatch/0/v3":      1,
		"customer_stream__customer_snapshot_attempt_count_5m/m/v3": 1,
	}
	if len(counter.seen) != len(want) {
		t.Fatalf("counted %v", counter.seen)
	}
	for key, count := range want {
		if counter.seen[key] != count {
			t.Errorf("%s counted %d, want %d (%v)", key, counter.seen[key], count, counter.seen)
		}
	}
}

func TestNothingIsCountedWithoutAProfileOrWithAMalformedOne(t *testing.T) {
	for name, edges := range map[string]string{"none": "", "text": "nope", "unsorted": `{"customer_daily__attempt_count_7d":[5,2]}`, "missing feature": `{"customer_daily__attempt_count_7d":[2]}`} {
		client, _, counter := profiled(t, edges)
		if status := client.Score(context.Background(), views(nil)).Status; status != application.ModelAvailable {
			t.Fatalf("%s: status %s", name, status)
		}
		if len(counter.seen) != 0 {
			t.Errorf("%s: counted %v", name, counter.seen)
		}
	}
}

func TestAnAnswerFromAnotherVersionCountsNothing(t *testing.T) {
	edges := `{"customer_daily__attempt_count_7d":[2],"customer_daily__decline_rate_7d":[1],"request_features__has_device":[1],"request_features__ip_shipping_country_mismatch":[1],"customer_stream__customer_snapshot_attempt_count_5m":[1]}`
	client, fake, counter := profiled(t, edges)
	fake.modelVersion = "4"
	if status := client.Score(context.Background(), views(nil)).Status; status != application.ModelError {
		t.Fatalf("status %s", status)
	}
	if len(counter.seen) != 0 {
		t.Errorf("counted %v", counter.seen)
	}
}

func configOf(version, threshold string) string {
	encoded, _ := json.Marshal(order)
	extra := ""
	if threshold != "" {
		extra = `,"decline_threshold":{"string_value":"` + threshold + `"}`
	}
	return `{"parameters":{"feature_order":{"string_value":` + strconv.Quote(string(encoded)) + `},"registry_version":{"string_value":"` + version + `"}` + extra + `}}`
}

func TestTwoVersionsBehindOneAddressEachScoreUnderTheirOwnThreshold(t *testing.T) {
	fake := newTriton()
	fake.configBody = configOf("v3", "0.5")
	client := newClient(t, fake.serve(t).URL, time.Second)
	if first := client.Score(t.Context(), views(nil)); first.Status != application.ModelAvailable || first.Version != "v3" || first.Threshold != 0.5 {
		t.Fatalf("first %+v", first)
	}
	fake.modelVersion = "4"
	if unseen := client.Score(t.Context(), views(nil)); unseen.Status != application.ModelError || unseen.Score != nil {
		t.Fatalf("a score from a version that was never read was used: %+v", unseen)
	}
	fake.configBody = configOf("v4", "0.7")
	if second := client.Score(t.Context(), views(nil)); second.Status != application.ModelAvailable || second.Version != "v4" || second.Threshold != 0.7 {
		t.Fatalf("second %+v", second)
	}
	fake.modelVersion = "3"
	if back := client.Score(t.Context(), views(nil)); back.Status != application.ModelAvailable || back.Version != "v3" || back.Threshold != 0.5 {
		t.Fatalf("back %+v", back)
	}
	if fake.configCalls.Load() != 2 {
		t.Fatalf("config reads %d", fake.configCalls.Load())
	}
}

func TestAThresholdThatIsNotAProbabilityIsIgnored(t *testing.T) {
	for _, text := range []string{"", "x", "0", "-0.2", "1.5"} {
		fake := newTriton()
		fake.configBody = configOf("v3", text)
		if observation := newClient(t, fake.serve(t).URL, time.Second).Score(t.Context(), views(nil)); observation.Status != application.ModelAvailable || observation.Threshold != 0 {
			t.Fatalf("threshold %q: %+v", text, observation)
		}
	}
}

func TestAVersionThatNeverShowsUpInTheConfigIsGivenUpOn(t *testing.T) {
	fake := newTriton()
	client := newClient(t, fake.serve(t).URL, time.Second)
	client.Score(t.Context(), views(nil))
	fake.modelVersion = "9"
	for range maxReads + 5 {
		client.Score(t.Context(), views(nil))
	}
	reads := fake.configCalls.Load()
	for range 5 {
		client.Score(t.Context(), views(nil))
	}
	if fake.configCalls.Load() != reads || reads > int32(maxReads)+2 {
		t.Fatalf("config reads %d then %d", reads, fake.configCalls.Load())
	}
}
