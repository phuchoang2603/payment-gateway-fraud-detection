package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/domain"
)

var (
	ErrConflict   = errors.New("assessment request conflicts with recorded content")
	ErrInProgress = errors.New("assessment is already owned")
	ErrExpired    = errors.New("assessment recovery horizon expired")
	ErrFenced     = errors.New("assessment ownership expired")
)

type (
	Identity struct{ IntegrationID, PaymentID, AttemptID string }
	Request  struct {
		Identity
		Digest                        string
		ScoredAt                      time.Time
		CustomerID, MethodFingerprint string
		Input                         domain.Input
		MerchantAccountCreatedAt      time.Time
		ItemCount                     int64
		HasDevice                     bool
	}
)

type Claim struct {
	ID             string
	Fence          int64
	FirstClaimedAt time.Time
	Result         *Result
}
type Store interface {
	Claim(context.Context, Request, time.Duration, time.Duration) (Claim, error)
	Complete(context.Context, Claim, Result) error
	Fail(context.Context, Claim, time.Duration) error
	Ready(context.Context) error
}
type FeatureValue struct {
	Integer *int64
	Number  *float64
	Boolean *bool
}
type ViewObservation struct {
	Status         string
	EventTimestamp *time.Time
	ReadAt         time.Time
	Values         map[string]FeatureValue
}
type FeatureObservation struct {
	Service string
	Views   map[string]ViewObservation
}
type Evidence struct {
	SchemaVersion               int
	AssessmentID, RequestDigest string
	Identity
	Decision                      domain.Decision
	CustomerID, MethodFingerprint string
	Input                         domain.Input
	Features                      FeatureObservation
	HistoricalShadowOnly          bool
	EvaluatedAt                   time.Time
	Model                         *ModelObservation `json:",omitempty"`
}
type Result struct {
	Replayed     bool `json:"-"`
	Evidence     Evidence
	EvidenceHash string
}
type (
	HistoryReader interface {
		Read(context.Context, Request) FeatureObservation
	}
	Config  struct{ Deadline, Lease, RetryHorizon, ErrorBackoff, HistoryTimeout time.Duration }
	Service struct {
		store   Store
		history HistoryReader
		config  Config
		model   ModelConfig
		now     func() time.Time
	}
)

func New(store Store, history HistoryReader, config Config) (*Service, error) {
	if store == nil || config.Deadline <= 0 || config.Lease <= config.Deadline || config.RetryHorizon <= config.Lease || config.ErrorBackoff <= 0 || config.HistoryTimeout <= 0 || config.HistoryTimeout > config.Deadline {
		return nil, errors.New("invalid assessment persistence/deadline configuration")
	}
	return &Service{store: store, history: history, config: config, now: time.Now}, nil
}

func (s *Service) Decide(ctx context.Context, request Request) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, s.config.Deadline)
	defer cancel()
	claim, err := s.store.Claim(ctx, request, s.config.Lease, s.config.RetryHorizon)
	if err != nil {
		return Result{}, err
	}
	if claim.Result != nil {
		result := *claim.Result
		encoded, err := json.Marshal(result.Evidence)
		if err != nil {
			return Result{}, err
		}
		digest := sha256.Sum256(encoded)
		if result.Evidence.SchemaVersion != 2 || result.Evidence.AssessmentID != claim.ID || result.Evidence.RequestDigest != request.Digest || result.Evidence.Identity != request.Identity || result.EvidenceHash != hex.EncodeToString(digest[:]) {
			return Result{}, errors.New("incompatible or corrupted persisted assessment")
		}
		result.Replayed = true
		return result, nil
	}
	request.ScoredAt = s.now().UTC()
	result := Result{Evidence: Evidence{
		SchemaVersion: 2, AssessmentID: claim.ID, RequestDigest: request.Digest, Identity: request.Identity,
		CustomerID: request.CustomerID, MethodFingerprint: request.MethodFingerprint, Input: request.Input, HistoricalShadowOnly: true,
		Features: FeatureObservation{Views: map[string]ViewObservation{
			"customer_daily": {Status: "disabled"}, "payment_method_daily": {Status: "disabled"},
			"customer_stream": {Status: "disabled"}, "payment_method_stream": {Status: "disabled"},
			"request_features": {Status: "disabled"},
		}},
	}}
	type historyResult struct{ features FeatureObservation }
	history := make(chan historyResult, 1)
	if s.history != nil {
		go func() {
			call, stop := context.WithTimeout(ctx, s.config.HistoryTimeout)
			defer stop()
			features := s.history.Read(call, request)
			history <- historyResult{features}
		}()
	} else {
		history <- historyResult{result.Evidence.Features}
	}
	select {
	case observation := <-history:
		result.Evidence.Features = observation.features
	case <-ctx.Done():
		s.fail(ctx, claim)
		return Result{}, ctx.Err()
	}
	result.Evidence.Decision = domain.Evaluate(request.Input)
	if s.model.Scorer != nil {
		s.scoreModel(ctx, &result.Evidence)
	}
	result.Evidence.EvaluatedAt = request.ScoredAt
	encoded, err := json.Marshal(result.Evidence)
	if err != nil {
		s.fail(ctx, claim)
		return Result{}, fmt.Errorf("encode assessment evidence: %w", err)
	}
	digest := sha256.Sum256(encoded)
	result.EvidenceHash = hex.EncodeToString(digest[:])
	if err := s.store.Complete(ctx, claim, result); err != nil {
		s.fail(ctx, claim)
		return Result{}, fmt.Errorf("persist assessment completion: %w", err)
	}
	return result, nil
}

func (s *Service) fail(ctx context.Context, claim Claim) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.config.Deadline)
	defer cancel()
	_ = s.store.Fail(cleanup, claim, s.config.ErrorBackoff)
}

func (s *Service) Ready(ctx context.Context) error { return s.store.Ready(ctx) }
