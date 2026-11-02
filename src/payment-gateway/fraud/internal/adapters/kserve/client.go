package kserve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
)

const maxAnswer = 64 * 1024

var modelName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type Config struct {
	URL, Model string
	Timeout    time.Duration
}
type Client struct {
	config Config
	http   *http.Client
	now    func() time.Time
}

func New(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || !modelName.MatchString(config.Model) || config.Timeout <= 0 {
		return nil, errors.New("invalid model endpoint, name or timeout")
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: config.Timeout}).DialContext, MaxConnsPerHost: 64, MaxIdleConns: 64, MaxIdleConnsPerHost: 64, IdleConnTimeout: time.Minute, ResponseHeaderTimeout: config.Timeout}
	return &Client{config: config, http: &http.Client{Transport: transport, Timeout: config.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, now: time.Now}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }

// instance names every value of an available view `<view>__<feature>`, the names the model was trained on. Unknown values are null, and the other views are absent, which the predictor reads as missing.
func instance(features application.FeatureObservation) map[string]any {
	row := map[string]any{}
	for view, observed := range features.Views {
		if observed.Status != "available" {
			continue
		}
		for field, value := range observed.Values {
			switch {
			case value.Integer != nil:
				row[view+"__"+field] = *value.Integer
			case value.Number != nil:
				row[view+"__"+field] = *value.Number
			case value.Boolean != nil:
				row[view+"__"+field] = *value.Boolean
			default:
				row[view+"__"+field] = nil
			}
		}
	}
	return row
}

func (c *Client) Score(ctx context.Context, features application.FeatureObservation) (observation application.ModelObservation) {
	observation = application.ModelObservation{Name: c.config.Model, Status: application.ModelError}
	defer func() { observation.ReadAt = c.now().UTC() }()
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	body, err := json.Marshal(struct {
		Instances []map[string]any `json:"instances"`
	}{[]map[string]any{instance(features)}})
	if err != nil {
		return observation
	}
	call, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.config.URL, "/")+"/v1/models/"+c.config.Model+":predict", bytes.NewReader(body))
	if err != nil {
		return observation
	}
	call.Header.Set("Content-Type", "application/json")
	result, err := c.http.Do(call)
	if err != nil {
		observation.Status = failureStatus(ctx, err)
		return observation
	}
	defer func() { _ = result.Body.Close() }()
	if result.StatusCode != http.StatusOK {
		return observation
	}
	content, err := io.ReadAll(io.LimitReader(result.Body, maxAnswer+1))
	if err != nil {
		observation.Status = failureStatus(ctx, err)
		return observation
	}
	if len(content) > maxAnswer {
		return observation
	}
	var answer struct {
		Predictions  []float64 `json:"predictions"`
		ModelVersion string    `json:"model_version"`
	}
	if json.Unmarshal(content, &answer) != nil || len(answer.Predictions) != 1 || answer.ModelVersion == "" {
		return observation
	}
	score := answer.Predictions[0]
	if math.IsNaN(score) || score < 0 || score > 1 {
		return observation
	}
	observation.Status, observation.Version, observation.Score = application.ModelAvailable, answer.ModelVersion, &score
	return observation
}

func failureStatus(ctx context.Context, err error) string {
	var timeout net.Error
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		return application.ModelTimeout
	}
	return application.ModelError
}
