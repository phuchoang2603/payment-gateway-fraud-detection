package kserve

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
)

const (
	maxAnswer    = 64 * 1024
	inputName    = "input__0"
	outputName   = "output__0"
	headerLength = "Inference-Header-Content-Length"
	maxReads     = 20
	retryAfter   = time.Minute
)

var (
	modelName   = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	versionName = regexp.MustCompile(`^v[0-9]+$`)
	digits      = regexp.MustCompile(`^[0-9]+$`)
	errInvalid  = errors.New("the model server answered something unusable")
)

// Config is the model server endpoint, the model name and the connection timeout. The request deadline
// belongs to the caller's context, not to the client. Bins, when set, counts every scored feature into the bin of the
// served model's reference profile, so drift can be measured from what the model saw.
type Config struct {
	URL, Model  string
	DialTimeout time.Duration
	Bins        metric.Int64Counter
}

// served is what the client knows of one model version Triton serves: the registry version it was packaged as, the
// feature order and decline threshold that packaging recorded (a zero threshold means it recorded none) and, when it
// recorded them, each feature's bin edges with the attributes that count a value into each bin (the last bin is the
// missing one).
type served struct {
	version   string
	order     []string
	threshold float64
	edges     [][]float64
	options   [][]metric.AddOption
}

// Client calls a Triton model over the Open Inference Protocol v2. Features travel as one binary FP32 row in the
// order the served model was packaged with, because the binary extension is the only way to send a missing value as NaN.
// While two versions are served behind one address, answers come from either, so the client keeps what it learned of
// each version and scores an answer under the version that made it.
type Client struct {
	config  Config
	http    *http.Client
	now     func() time.Time
	mu      sync.RWMutex
	known   map[string]served
	order   []string
	pending map[string]int
	gaveUp  map[string]time.Time
}

// New returns a client for an `http` model server endpoint and a Kubernetes-style model name.
func New(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || !modelName.MatchString(config.Model) || config.DialTimeout <= 0 {
		return nil, errors.New("invalid model endpoint, name or timeout")
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: config.DialTimeout}).DialContext, MaxConnsPerHost: 64, MaxIdleConns: 64, MaxIdleConnsPerHost: 64, IdleConnTimeout: time.Minute}
	return &Client{config: config, known: map[string]served{}, pending: map[string]int{}, gaveUp: map[string]time.Time{}, http: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, now: time.Now}, nil
}

// Close releases the client's idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// row lays the features out in the model's order. A value is NaN, which the model reads as missing, when its view is
// not available or the view holds no number for it; booleans are 0 or 1, as in training.
func row(features application.FeatureObservation, order []string) []float32 {
	values := make([]float32, len(order))
	for index, name := range order {
		values[index] = float32(math.NaN())
		view, field, _ := strings.Cut(name, "__")
		observed := features.Views[view]
		if observed.Status != "available" {
			continue
		}
		switch value := observed.Values[field]; {
		case value.Integer != nil:
			values[index] = float32(*value.Integer)
		case value.Number != nil:
			values[index] = float32(*value.Number)
		case value.Boolean != nil && *value.Boolean:
			values[index] = 1
		case value.Boolean != nil:
			values[index] = 0
		}
	}
	return values
}

// Score sends one row and returns the observation: the model's version, score and decline threshold, or a timeout or
// error status. The caller's context is the only deadline. A score from a version whose configuration the client has
// not read, or whose feature order differs from the one the row was laid out in, is discarded, and the configuration is
// read again on the next call, so a rollout never scores a mismatched row.
func (c *Client) Score(ctx context.Context, features application.FeatureObservation) (observation application.ModelObservation) {
	observation = application.ModelObservation{Name: c.config.Model, Status: application.ModelError}
	defer func() { observation.ReadAt = c.now().UTC() }()
	order, err := c.layout(ctx)
	if err != nil {
		observation.Status = failureStatus(ctx, err)
		return observation
	}
	values := row(features, order)
	score, version, err := c.infer(ctx, values)
	if err != nil {
		observation.Status = failureStatus(ctx, err)
		return observation
	}
	layout, found := c.lookup(version)
	if !found {
		c.want(version)
		return observation
	}
	if !slices.Equal(layout.order, order) {
		return observation
	}
	c.count(ctx, layout, values)
	observation.Status, observation.Version, observation.Score, observation.Threshold = application.ModelAvailable, version, &score, layout.threshold
	return observation
}

// count adds each scored value to its feature's bin: the number of edges at or below it, or the missing bin for NaN.
func (c *Client) count(ctx context.Context, layout served, values []float32) {
	if c.config.Bins == nil || layout.edges == nil {
		return
	}
	for index, value := range values {
		edges, slot := layout.edges[index], len(layout.edges[index])+1
		if number := float64(value); !math.IsNaN(number) {
			slot = sort.Search(len(edges), func(at int) bool { return edges[at] > number })
		}
		c.config.Bins.Add(ctx, 1, layout.options[index][slot])
	}
}

// layout returns the feature order rows are laid out in. It reads the model's configuration when the client knows no
// version yet, and again while an answer from a version it has not read is waiting to be learned.
func (c *Client) layout(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	order, waiting := c.order, len(c.pending) > 0
	c.mu.RUnlock()
	if len(order) > 0 && !waiting {
		return order, nil
	}
	fetched, err := c.fetch(ctx)
	if err != nil {
		if len(order) > 0 {
			return order, nil
		}
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.known[fetched.version] = fetched
	delete(c.pending, fetched.version)
	for version := range c.pending {
		if c.pending[version]++; c.pending[version] > maxReads {
			delete(c.pending, version)
			c.gaveUp[version] = c.now()
		}
	}
	c.order = fetched.order
	return c.order, nil
}

func (c *Client) lookup(version string) (served, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	layout, found := c.known[version]
	return layout, found
}

func (c *Client) want(version string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, waiting := c.pending[version]; waiting || c.now().Sub(c.gaveUp[version]) < retryAfter {
		return
	}
	c.pending[version] = 0
}

func (c *Client) address(path string) string {
	return strings.TrimRight(c.config.URL, "/") + "/v2/models/" + c.config.Model + path
}

// fetch reads the feature order and registry version the model was packaged with from its Triton config.
func (c *Client) fetch(ctx context.Context) (served, error) {
	call, err := http.NewRequestWithContext(ctx, http.MethodGet, c.address("/config"), nil)
	if err != nil {
		return served{}, err
	}
	content, _, err := c.do(call)
	if err != nil {
		return served{}, err
	}
	var config struct {
		Parameters map[string]struct {
			StringValue string `json:"string_value"`
		} `json:"parameters"`
	}
	var order []string
	if json.Unmarshal(content, &config) != nil || json.Unmarshal([]byte(config.Parameters["feature_order"].StringValue), &order) != nil || len(order) == 0 {
		return served{}, errInvalid
	}
	version := config.Parameters["registry_version"].StringValue
	if !versionName.MatchString(version) {
		return served{}, errInvalid
	}
	known := served{version: version, order: order}
	if threshold, err := strconv.ParseFloat(config.Parameters["decline_threshold"].StringValue, 64); err == nil && threshold > 0 && threshold <= 1 {
		known.threshold = threshold
	}
	c.profile(&known, config.Parameters["bin_edges"].StringValue)
	return known, nil
}

// profile reads the bin edges of the reference profile into a served model, and ignores a missing or malformed set,
// which only stops the counting.
func (c *Client) profile(known *served, text string) {
	var edges map[string][]float64
	if c.config.Bins == nil || json.Unmarshal([]byte(text), &edges) != nil || len(edges) == 0 {
		return
	}
	known.edges, known.options = make([][]float64, len(known.order)), make([][]metric.AddOption, len(known.order))
	for index, name := range known.order {
		list, found := edges[name]
		if !found || !sort.Float64sAreSorted(list) {
			known.edges, known.options = nil, nil
			return
		}
		known.edges[index] = list
		for slot := 0; slot <= len(list)+1; slot++ {
			label := strconv.Itoa(slot)
			if slot == len(list)+1 {
				label = "m"
			}
			known.options[index] = append(known.options[index], metric.WithAttributes(attribute.String("feature", name), attribute.String("bin", label), attribute.String("model_version", known.version)))
		}
	}
}

// infer scores one row and returns the fraud probability and the registry version of the model that made it.
func (c *Client) infer(ctx context.Context, values []float32) (float64, string, error) {
	tensor := make([]byte, 0, 4*len(values))
	for _, value := range values {
		tensor = binary.LittleEndian.AppendUint32(tensor, math.Float32bits(value))
	}
	header, err := json.Marshal(map[string]any{
		"inputs":  []any{map[string]any{"name": inputName, "shape": []int{1, len(values)}, "datatype": "FP32", "parameters": map[string]any{"binary_data_size": len(tensor)}}},
		"outputs": []any{map[string]any{"name": outputName, "parameters": map[string]any{"binary_data": true}}},
	})
	if err != nil {
		return 0, "", err
	}
	call, err := http.NewRequestWithContext(ctx, http.MethodPost, c.address("/infer"), bytes.NewReader(append(header, tensor...)))
	if err != nil {
		return 0, "", err
	}
	call.Header.Set("Content-Type", "application/octet-stream")
	call.Header.Set(headerLength, strconv.Itoa(len(header)))
	content, headers, err := c.do(call)
	if err != nil {
		return 0, "", err
	}
	size, err := strconv.Atoi(headers.Get(headerLength))
	if err != nil || size <= 0 || size > len(content) {
		return 0, "", errInvalid
	}
	var answer struct {
		ModelVersion string `json:"model_version"`
		Outputs      []struct {
			Name       string `json:"name"`
			Datatype   string `json:"datatype"`
			Shape      []int  `json:"shape"`
			Parameters struct {
				BinaryDataSize int `json:"binary_data_size"`
			} `json:"parameters"`
		} `json:"outputs"`
	}
	data := content[size:]
	if json.Unmarshal(content[:size], &answer) != nil || !digits.MatchString(answer.ModelVersion) || len(answer.Outputs) != 1 || len(data) != 8 {
		return 0, "", errInvalid
	}
	if output := answer.Outputs[0]; output.Name != outputName || output.Datatype != "FP32" || len(output.Shape) != 2 || output.Shape[0] != 1 || output.Shape[1] != 2 || output.Parameters.BinaryDataSize != len(data) {
		return 0, "", errInvalid
	}
	score := float64(math.Float32frombits(binary.LittleEndian.Uint32(data[4:])))
	if math.IsNaN(score) || score < 0 || score > 1 {
		return 0, "", errInvalid
	}
	return score, "v" + answer.ModelVersion, nil
}

// do sends a request and returns the bounded body and headers of a 200 answer.
func (c *Client) do(call *http.Request) ([]byte, http.Header, error) {
	result, err := c.http.Do(call)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = result.Body.Close() }()
	if result.StatusCode != http.StatusOK {
		return nil, nil, errInvalid
	}
	content, err := io.ReadAll(io.LimitReader(result.Body, maxAnswer+1))
	if err != nil {
		return nil, nil, err
	}
	if len(content) > maxAnswer {
		return nil, nil, errInvalid
	}
	return content, result.Header, nil
}

func failureStatus(ctx context.Context, err error) string {
	if application.TimedOut(ctx, err) {
		return application.ModelTimeout
	}
	return application.ModelError
}
