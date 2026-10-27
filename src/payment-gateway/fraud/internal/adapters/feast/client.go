package feast

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
	featuresv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/features/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const definitionVersion = "fraud-snapshot-v1"

var (
	customerFields = []string{"assessed_count_1h", "assessed_count_24h", "assessed_amount_minor_24h", "distinct_devices_7d", "last_assessed_start_us", "snapshot_metadata"}
	methodFields   = []string{"assessed_count_1h", "assessed_count_24h", "snapshot_metadata"}
)

type Config struct {
	URL, Epoch, DefinitionHash            string
	Timeout, MaxAnchorAge, MaxProgressAge time.Duration
}

type Client struct {
	config Config
	http   *http.Client
	now    func() time.Time
}

func New(config Config) (*Client, error) {
	endpoint, err := url.Parse(config.URL)
	hash, hashErr := hex.DecodeString(config.DefinitionHash)
	if err != nil || endpoint.Scheme != "http" || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || config.Epoch == "" || hashErr != nil || len(hash) != sha256.Size || config.Timeout <= 0 || config.MaxAnchorAge <= 0 || config.MaxProgressAge <= 0 {
		return nil, errors.New("invalid Feast endpoint, identity or freshness budgets")
	}
	transport := &http.Transport{
		DialContext:     (&net.Dialer{Timeout: config.Timeout}).DialContext,
		MaxConnsPerHost: 128, MaxIdleConns: 128, MaxIdleConnsPerHost: 128,
		IdleConnTimeout: time.Minute, ResponseHeaderTimeout: config.Timeout,
	}
	return &Client{config: config, http: &http.Client{
		Transport: transport, Timeout: config.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, now: time.Now}, nil
}

func (c *Client) Close() { c.http.CloseIdleConnections() }

func entityKey(epoch, view, integration, entity string) string {
	digest := sha256.New()
	for _, value := range []string{epoch, view, integration, entity} {
		_, _ = digest.Write([]byte(strconv.Itoa(len(value)) + ":" + value))
	}
	return hex.EncodeToString(digest.Sum(nil))
}

func (c *Client) Read(ctx context.Context, request application.Request) (application.ViewObservation, application.ViewObservation) {
	type result struct {
		scope       string
		observation application.ViewObservation
	}
	results := make(chan result, 2)
	for scope, entity := range map[string]string{"customer": request.CustomerID, "method": request.MethodFingerprint} {
		go func() { results <- result{scope, c.read(ctx, request, scope, entity)} }()
	}
	var customer, method application.ViewObservation
	for range 2 {
		value := <-results
		if value.scope == "customer" {
			customer = value.observation
		} else {
			method = value.observation
		}
	}
	return customer, method
}

type response struct {
	Metadata struct {
		FeatureNames []string `json:"feature_names"`
	} `json:"metadata"`
	Results []struct {
		Values          []json.RawMessage `json:"values"`
		Statuses        []string          `json:"statuses"`
		EventTimestamps []time.Time       `json:"event_timestamps"`
	} `json:"results"`
}

func (c *Client) read(ctx context.Context, request application.Request, scope, entity string) (observation application.ViewObservation) {
	observation.Status = "error"
	defer func() { observation.ReadAt = c.now().UTC() }()
	ctx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	fields := methodFields
	if scope == "customer" {
		fields = customerFields
	}
	view := scope + "_snapshot"
	features := make([]string, len(fields))
	for index, field := range fields {
		features[index] = view + ":" + field
	}
	body, err := json.Marshal(struct {
		Entities         map[string][]string `json:"entities"`
		Features         []string            `json:"features"`
		FullFeatureNames bool                `json:"full_feature_names"`
	}{map[string][]string{"snapshot_key": {entityKey(c.config.Epoch, scope, request.IntegrationID, entity)}}, features, true})
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
		observation.Status = failureStatus(ctx, err)
		return observation
	}
	defer func() { _ = result.Body.Close() }()
	if result.StatusCode != http.StatusOK {
		return observation
	}
	content, err := io.ReadAll(io.LimitReader(result.Body, 256*1024+1))
	if err != nil {
		observation.Status = failureStatus(ctx, err)
		return observation
	}
	if len(content) > 256*1024 {
		return observation
	}
	var decoded response
	if json.Unmarshal(content, &decoded) != nil || len(decoded.Metadata.FeatureNames) != len(decoded.Results) {
		return observation
	}
	indices := make(map[string]int, len(decoded.Metadata.FeatureNames))
	for index, name := range decoded.Metadata.FeatureNames {
		if _, duplicate := indices[name]; duplicate {
			return observation
		}
		indices[name] = index
	}
	values := &featuresv1.SnapshotValues{}
	var metadata []byte
	var metadataMissing bool
	var writeAt time.Time
	for _, field := range fields {
		index, exists := indices[view+"__"+field]
		if !exists {
			return observation
		}
		row := decoded.Results[index]
		if len(row.Values) != 1 || len(row.Statuses) != 1 || len(row.EventTimestamps) != 1 {
			return observation
		}
		switch row.Statuses[0] {
		case "NOT_FOUND":
			observation.Status = "missing"
			return observation
		case "OUTSIDE_MAX_AGE":
			observation.Status = "stale"
			return observation
		case "PRESENT", "NULL_VALUE":
		default:
			return observation
		}
		if writeAt.IsZero() {
			writeAt = row.EventTimestamps[0]
		} else if !writeAt.Equal(row.EventTimestamps[0]) {
			return observation
		}
		isNull := bytes.Equal(row.Values[0], []byte("null"))
		if row.Statuses[0] == "NULL_VALUE" && !isNull {
			return observation
		}
		if field == "snapshot_metadata" {
			if isNull {
				metadataMissing = true
				continue
			}
			if row.Statuses[0] != "PRESENT" {
				return observation
			}
			var encoded string
			if json.Unmarshal(row.Values[0], &encoded) != nil {
				return observation
			}
			metadata, err = base64.StdEncoding.DecodeString(encoded)
			if err != nil {
				return observation
			}
			continue
		}
		if isNull {
			continue
		}
		var number int64
		if json.Unmarshal(row.Values[0], &number) != nil {
			return observation
		}
		switch field {
		case "assessed_count_1h":
			values.AssessedCount_1H = &number
		case "assessed_count_24h":
			values.AssessedCount_24H = &number
		case "assessed_amount_minor_24h":
			values.AssessedAmountMinor_24H = &number
		case "distinct_devices_7d":
			values.DistinctDevices_7D = &number
		case "last_assessed_start_us":
			values.LastAssessedStartAt = timestamppb.New(time.UnixMicro(number).UTC())
		}
	}
	// Feast 0.67 represents absent entities as PRESENT/null at the Unix epoch.
	if metadataMissing {
		if proto.Size(values) == 0 && writeAt.Equal(time.Unix(0, 0)) {
			observation.Status = "missing"
		}
		return observation
	}
	var snapshot featuresv1.SnapshotUpdate
	if proto.Unmarshal(metadata, &snapshot) != nil || snapshot.Values != nil {
		return observation
	}
	snapshot.Values = values
	return c.evaluate(&snapshot, writeAt, request, scope, entity)
}

func (c *Client) evaluate(update *featuresv1.SnapshotUpdate, writeAt time.Time, request application.Request, scope, entity string) application.ViewObservation {
	observation := application.ViewObservation{Status: "incompatible"}
	view := featuresv1.EntityView_ENTITY_VIEW_METHOD
	if scope == "customer" {
		view = featuresv1.EntityView_ENTITY_VIEW_CUSTOMER
	}
	if update.SchemaVersion != 1 || update.GetDefinition().GetVersion() != definitionVersion || update.GetDefinition().GetSha256() != c.config.DefinitionHash || update.Epoch != c.config.Epoch || update.View != view || update.IntegrationId != request.IntegrationID || update.EntityId != entity || update.Revision == 0 || update.Coverage == nil {
		return observation
	}
	for _, timestamp := range []*timestamppb.Timestamp{update.Anchor, update.ComputedAt, update.PublishedAt, update.SourceAvailableAt, update.OnlineWriteAt} {
		if timestamp == nil || timestamp.CheckValid() != nil || timestamp.Nanos%1000 != 0 {
			return observation
		}
	}
	if !update.OnlineWriteAt.AsTime().Equal(writeAt) {
		return observation
	}
	identifier := update.SnapshotId
	update.SnapshotId = ""
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(update)
	digest := sha256.Sum256(encoded)
	if err != nil || identifier != hex.EncodeToString(digest[:]) {
		return observation
	}
	update.SnapshotId = identifier
	values := update.Values
	for _, number := range []*int64{values.AssessedCount_1H, values.AssessedCount_24H, values.AssessedAmountMinor_24H, values.DistinctDevices_7D} {
		if number != nil && *number < 0 {
			return observation
		}
	}
	if values.LastAssessedStartAt != nil && (values.LastAssessedStartAt.CheckValid() != nil || !values.LastAssessedStartAt.AsTime().Before(update.Anchor.AsTime())) {
		return observation
	}
	if update.ComputedAt.AsTime().Before(update.Anchor.AsTime()) || update.PublishedAt.AsTime().Before(update.ComputedAt.AsTime()) || update.ComputedAt.AsTime().Before(update.SourceAvailableAt.AsTime()) {
		return observation
	}
	observed := &application.Snapshot{
		ID: identifier, Epoch: update.Epoch, Definition: update.Definition.Version, DefinitionHash: update.Definition.Sha256, Revision: update.Revision,
		Anchor: update.Anchor.AsTime(), ComputedAt: update.ComputedAt.AsTime(), PublishedAt: update.PublishedAt.AsTime(), SourceAvailableAt: update.SourceAvailableAt.AsTime(), WriteAt: writeAt,
		Coverage: map[string]string{}, Values: application.SnapshotValues{Count1h: values.AssessedCount_1H, Count24h: values.AssessedCount_24H, AmountMinor24h: values.AssessedAmountMinor_24H, Devices7d: values.DistinctDevices_7D},
	}
	if values.LastAssessedStartAt != nil {
		last := values.LastAssessedStartAt.AsTime()
		observed.Values.LastAssessedAt = &last
	}
	windows := map[string]featuresv1.Coverage{"1h": update.Coverage.OneHour, "24h": update.Coverage.TwentyFourHours}
	if scope == "customer" {
		windows["7d"] = update.Coverage.SevenDays
	}
	for window, coverage := range windows {
		if coverage != featuresv1.Coverage_COVERAGE_COMPLETE && coverage != featuresv1.Coverage_COVERAGE_UNKNOWN {
			return observation
		}
		observed.Coverage[window] = "unknown"
		if coverage == featuresv1.Coverage_COVERAGE_COMPLETE {
			observed.Coverage[window] = "complete"
		}
	}
	now := c.now().UTC()
	progressHealthy := len(update.SourceProgress) > 0
	for _, progress := range update.SourceProgress {
		if progress.Topic == "" || progress.Partition < 0 || progress.ConsumedOffset < -1 || progress.ObservedHighWatermark <= progress.ConsumedOffset || progress.ObservedAt == nil || progress.ObservedAt.CheckValid() != nil || progress.ObservedAt.AsTime().After(observed.ComputedAt) {
			return observation
		}
		record := application.SourceProgress{Topic: progress.Topic, Partition: progress.Partition, ConsumedOffset: progress.ConsumedOffset, ObservedHighWatermark: progress.ObservedHighWatermark, ObservedAt: progress.ObservedAt.AsTime()}
		if progress.HeartbeatAt != nil {
			if progress.HeartbeatAt.CheckValid() != nil || progress.HeartbeatAt.AsTime().After(progress.ObservedAt.AsTime()) {
				return observation
			}
			heartbeat := progress.HeartbeatAt.AsTime()
			record.HeartbeatAt = &heartbeat
		}
		observed.SourceProgress = append(observed.SourceProgress, record)
		if now.Sub(record.ObservedAt) > c.config.MaxProgressAge || progress.ConsumedOffset+1 != progress.ObservedHighWatermark {
			progressHealthy = false
		}
	}
	observation.Observed = observed
	observation.AvailableAt = &now
	if observed.Anchor.After(request.Input.AttemptedAt) || observed.ComputedAt.After(now) || observed.PublishedAt.After(now) || writeAt.After(now) {
		observation.Status = "future"
		return observation
	}
	if request.Input.AttemptedAt.Sub(observed.Anchor) > c.config.MaxAnchorAge || now.Sub(observed.SourceAvailableAt) > c.config.MaxProgressAge || !progressHealthy {
		observation.Status = "stale"
		return observation
	}
	observation.Status = "available"
	for window, coverage := range windows {
		if coverage == featuresv1.Coverage_COVERAGE_COMPLETE {
			switch window {
			case "1h":
				if values.AssessedCount_1H == nil {
					observation.Status = "partial"
				} else {
					observation.Values.Count1h = values.AssessedCount_1H
				}
			case "24h":
				observation.Values.Count24h = values.AssessedCount_24H
				if values.AssessedCount_24H == nil {
					observation.Status = "partial"
				}
				if scope == "customer" {
					observation.Values.AmountMinor24h = values.AssessedAmountMinor_24H
					if values.AssessedAmountMinor_24H == nil {
						observation.Status = "partial"
					}
				}
			case "7d":
				observation.Values.Devices7d = values.DistinctDevices_7D
				if values.DistinctDevices_7D == nil {
					observation.Status = "partial"
				}
				observation.Values.LastAssessedAt = observed.Values.LastAssessedAt
				if last := observed.Values.LastAssessedAt; last != nil && request.Input.AttemptedAt.Sub(*last) <= 7*24*time.Hour && last.Before(request.Input.AttemptedAt) {
					seconds := int64(request.Input.AttemptedAt.Sub(*last) / time.Second)
					observation.ElapsedSeconds = &seconds
				}
			}
		} else {
			observation.Status = "partial"
		}
	}
	return observation
}

func failureStatus(ctx context.Context, err error) string {
	var timeout net.Error
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout"
	}
	return "error"
}
