package feast

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
)

type Config struct {
	URL, FeatureService          string
	Timeout, DailyTTL, StreamTTL time.Duration
}
type Client struct {
	config Config
	http   *http.Client
	now    func() time.Time
}

func New(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.URL)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || config.FeatureService == "" || config.Timeout <= 0 || config.DailyTTL <= 0 || config.StreamTTL <= 0 {
		return nil, errors.New("invalid Feast endpoint, service or TTL")
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: config.Timeout}).DialContext, MaxConnsPerHost: 128, MaxIdleConns: 128, MaxIdleConnsPerHost: 128, IdleConnTimeout: time.Minute, ResponseHeaderTimeout: config.Timeout}
	return &Client{config: config, http: &http.Client{Transport: transport, Timeout: config.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, now: time.Now}, nil
}
func (c *Client) Close() { c.http.CloseIdleConnections() }

var viewFields = map[string][]string{
	"customer_stream":       {"customer_snapshot_attempt_count_5m", "customer_snapshot_attempt_count_1h", "customer_snapshot_attempt_count_24h", "customer_snapshot_distinct_devices_24h"},
	"payment_method_stream": {"method_snapshot_attempt_count_5m", "method_snapshot_attempt_count_1h", "method_snapshot_attempt_count_24h"},
	"customer_daily":        dailyFields("customer"),
	"payment_method_daily":  dailyFields("payment_method"),
	"request_features":      {"amount_minor", "customer_account_age_days", "merchant_account_age_days", "ip_shipping_country_mismatch", "item_count", "has_device", "customer_amount_to_30d_avg", "payment_method_amount_to_30d_avg", "scored_hour_utc"},
}

func nullableRequestFeature(name string) bool {
	return name == "ip_shipping_country_mismatch" || strings.HasSuffix(name, "_amount_to_30d_avg")
}

func dailyFields(scope string) []string {
	measures := []string{"attempt_count", "amount_sum_minor", "decided_count", "declined_count", "risk_declined_count", "decline_rate", "distinct_merchant_count"}
	if scope == "customer" {
		measures = append(measures, "distinct_device_count", "distinct_payment_method_count")
	} else {
		measures = append(measures, "distinct_customer_count")
	}
	fields := make([]string, 0, len(measures)*2)
	for _, window := range []string{"7d", "30d"} {
		for _, measure := range measures {
			fields = append(fields, measure+"_"+window)
		}
	}
	return fields
}

type featureResult struct {
	Values          []json.RawMessage `json:"values"`
	Statuses        []string          `json:"statuses"`
	EventTimestamps []time.Time       `json:"event_timestamps"`
}
type response struct {
	Metadata struct {
		FeatureNames []string `json:"feature_names"`
	} `json:"metadata"`
	Results []featureResult `json:"results"`
}

func (c *Client) Read(ctx context.Context, request application.Request) (observation application.FeatureObservation) {
	observation = application.FeatureObservation{Service: c.config.FeatureService, Views: map[string]application.ViewObservation{}}
	status := "error"
	defer func() {
		for name := range viewFields {
			view, exists := observation.Views[name]
			if !exists {
				view.Status = status
			}
			view.ReadAt = c.now().UTC()
			observation.Views[name] = view
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	body, err := json.Marshal(struct {
		FeatureService   string           `json:"feature_service"`
		Entities         map[string][]any `json:"entities"`
		FullFeatureNames bool             `json:"full_feature_names"`
	}{c.config.FeatureService, map[string][]any{
		"integration_id": {request.IntegrationID}, "customer_id": {request.CustomerID}, "payment_method_fingerprint": {request.MethodFingerprint},
		"scored_at": {request.ScoredAt.Format(time.RFC3339Nano)}, "amount_minor": {request.Input.AmountMinor},
		"customer_account_created_at": {request.Input.CustomerCreatedAt.Format(time.RFC3339Nano)}, "merchant_account_created_at": {request.MerchantAccountCreatedAt.Format(time.RFC3339Nano)},
		"shipping_country": {request.Input.ShippingCountry}, "ip_country": {request.Input.IPCountry}, "item_count": {request.ItemCount}, "has_device": {request.HasDevice},
	}, true})
	if err != nil {
		return observation
	}
	call, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.config.URL, "/")+"/get-online-features", bytes.NewReader(body))
	if err != nil {
		return observation
	}
	call.Header.Set("Content-Type", "application/json")
	result, err := c.http.Do(call)
	if err != nil {
		status = failureStatus(ctx, err)
		return observation
	}
	defer func() { _ = result.Body.Close() }()
	if result.StatusCode != http.StatusOK {
		return observation
	}
	content, err := io.ReadAll(io.LimitReader(result.Body, 256*1024+1))
	if err != nil {
		status = failureStatus(ctx, err)
		return observation
	}
	if len(content) > 256*1024 {
		return observation
	}
	var decoded response
	if json.Unmarshal(content, &decoded) != nil || len(decoded.Metadata.FeatureNames) != len(decoded.Results) {
		return observation
	}
	fields := map[string]featureResult{}
	for index, name := range decoded.Metadata.FeatureNames {
		if _, exists := fields[name]; exists {
			return observation
		}
		fields[name] = decoded.Results[index]
	}
	for name, names := range viewFields {
		ttl := c.config.DailyTTL
		if strings.HasSuffix(name, "_stream") {
			ttl = c.config.StreamTTL
		}
		observation.Views[name] = evaluate(name, names, fields, request.ScoredAt, ttl)
	}
	// Feast's online read ignores TTL, so a ratio built from an expired or missing daily row is unknown, as in training.
	if features, exists := observation.Views["request_features"]; exists && features.Status == "available" {
		for _, scope := range []string{"customer", "payment_method"} {
			if observation.Views[scope+"_daily"].Status != "available" {
				features.Values[scope+"_amount_to_30d_avg"] = application.FeatureValue{}
			}
		}
	}
	return observation
}

func evaluate(view string, names []string, fields map[string]featureResult, scoredAt time.Time, ttl time.Duration) application.ViewObservation {
	result := application.ViewObservation{Status: "error"}
	values := map[string]application.FeatureValue{}
	var timestamp time.Time
	allNull := true
	for index, name := range names {
		field, exists := fields[view+"__"+name]
		if !exists || len(field.Values) != 1 || len(field.Statuses) != 1 || len(field.EventTimestamps) != 1 {
			return result
		}
		if index == 0 {
			timestamp = field.EventTimestamps[0]
		} else if !timestamp.Equal(field.EventTimestamps[0]) {
			return result
		}
		isNull := bytes.Equal(field.Values[0], []byte("null"))
		switch field.Statuses[0] {
		case "PRESENT":
		case "NULL_VALUE", "NOT_FOUND":
			if !isNull {
				return result
			}
		default:
			return result
		}
		value := application.FeatureValue{}
		if !isNull {
			allNull = false
			if name == "ip_shipping_country_mismatch" || name == "has_device" {
				var boolean bool
				if json.Unmarshal(field.Values[0], &boolean) != nil {
					return result
				}
				value.Boolean = &boolean
			} else if strings.HasPrefix(name, "decline_rate_") {
				var number float64
				if json.Unmarshal(field.Values[0], &number) != nil || number < 0 || number > 1 {
					return result
				}
				value.Number = &number
			} else if strings.HasSuffix(name, "_amount_to_30d_avg") {
				var number float64
				if json.Unmarshal(field.Values[0], &number) != nil || number < 0 {
					return result
				}
				value.Number = &number
			} else {
				var number int64
				if json.Unmarshal(field.Values[0], &number) != nil || number < 0 {
					return result
				}
				value.Integer = &number
			}
		}
		values[name] = value
	}
	result.EventTimestamp = &timestamp
	if view == "request_features" {
		for name, value := range values {
			if !nullableRequestFeature(name) && value.Integer == nil && value.Boolean == nil {
				return result
			}
		}
		result.Status = "available"
		result.EventTimestamp = &scoredAt
		result.Values = values
		return result
	}
	switch {
	case allNull && timestamp.Equal(time.Unix(0, 0)):
		result.Status = "missing"
	case timestamp.After(scoredAt):
		result.Status = "future"
	case scoredAt.Sub(timestamp) > ttl:
		result.Status = "expired"
	default:
		result.Status = "available"
		result.Values = values
	}
	return result
}

func failureStatus(ctx context.Context, err error) string {
	if application.TimedOut(ctx, err) {
		return "timeout"
	}
	return "error"
}
