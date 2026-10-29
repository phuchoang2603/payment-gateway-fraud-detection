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
		CustomerID, MethodFingerprint string
		Input                         domain.Input
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
type SnapshotValues struct {
	Count5m, Count1h, Count24h, Devices24h *int64
}
type SourceProgress struct {
	Topic                                 string
	Partition                             int32
	ConsumedOffset, ObservedHighWatermark int64
	ObservedAt                            time.Time
	HeartbeatAt                           *time.Time
}

type Snapshot struct {
	ID, Epoch, Definition, DefinitionHash string
	Revision                              uint64
	Anchor, SourceAvailableAt, WriteAt    time.Time
	Coverage                              map[string]string
	SourceProgress                        []SourceProgress
	Values                                SnapshotValues
}
type ViewObservation struct {
	Status      string
	ReadAt      time.Time
	AvailableAt *time.Time
	Observed    *Snapshot
	Values      SnapshotValues
}
type GuardObservation struct {
	Status, Epoch, PolicyVersion string
	RegisteredAt, ReadAt         *time.Time
	Count10s, Count60s           *int64
	ThresholdExceeded            bool
	Threshold10s, Threshold60s   int64
}
type Evidence struct {
	SchemaVersion               int
	AssessmentID, RequestDigest string
	Identity
	Decision                                  domain.Decision
	CustomerID, MethodFingerprint             string
	Input                                     domain.Input
	Customer, Method                          ViewObservation
	CustomerGuard, MethodGuard                GuardObservation
	HistoricalShadowOnly, VelocityObserveOnly bool
	EvaluatedAt                               time.Time
}
type Result struct {
	Replayed     bool `json:"-"`
	Evidence     Evidence
	EvidenceHash string
}
type (
	HistoryReader interface {
		Read(context.Context, Request) (ViewObservation, ViewObservation)
	}
	VelocityReader interface {
		Register(context.Context, Request, time.Time) (GuardObservation, GuardObservation)
	}
	Config  struct{ Deadline, Lease, RetryHorizon, ErrorBackoff, HistoryTimeout, GuardTimeout time.Duration }
	Service struct {
		store    Store
		history  HistoryReader
		velocity VelocityReader
		config   Config
		now      func() time.Time
	}
)

func New(store Store, history HistoryReader, velocity VelocityReader, config Config) (*Service, error) {
	if store == nil || config.Deadline <= 0 || config.Lease <= config.Deadline || config.RetryHorizon <= config.Lease || config.ErrorBackoff <= 0 || config.HistoryTimeout <= 0 || config.GuardTimeout <= 0 || config.HistoryTimeout > config.Deadline || config.GuardTimeout > config.Deadline {
		return nil, errors.New("invalid assessment persistence/deadline configuration")
	}
	return &Service{store: store, history: history, velocity: velocity, config: config, now: time.Now}, nil
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
		if result.Evidence.SchemaVersion != 1 || result.Evidence.AssessmentID != claim.ID || result.Evidence.RequestDigest != request.Digest || result.Evidence.Identity != request.Identity || result.EvidenceHash != hex.EncodeToString(digest[:]) {
			return Result{}, errors.New("incompatible or corrupted persisted assessment")
		}
		result.Replayed = true
		return result, nil
	}
	result := Result{Evidence: Evidence{
		SchemaVersion: 1, AssessmentID: claim.ID, RequestDigest: request.Digest, Identity: request.Identity,
		CustomerID: request.CustomerID, MethodFingerprint: request.MethodFingerprint, Input: request.Input, HistoricalShadowOnly: true, VelocityObserveOnly: true,
		Customer: ViewObservation{Status: "disabled"}, Method: ViewObservation{Status: "disabled"},
		CustomerGuard: GuardObservation{Status: "disabled"}, MethodGuard: GuardObservation{Status: "disabled"},
	}}
	type historyResult struct{ customer, method ViewObservation }
	type guardResult struct{ customer, method GuardObservation }
	history := make(chan historyResult, 1)
	guard := make(chan guardResult, 1)
	if s.history != nil {
		go func() {
			call, stop := context.WithTimeout(ctx, s.config.HistoryTimeout)
			defer stop()
			customer, method := s.history.Read(call, request)
			history <- historyResult{customer, method}
		}()
	} else {
		history <- historyResult{result.Evidence.Customer, result.Evidence.Method}
	}
	if s.velocity != nil {
		go func() {
			call, stop := context.WithTimeout(ctx, s.config.GuardTimeout)
			defer stop()
			customer, method := s.velocity.Register(call, request, claim.FirstClaimedAt)
			guard <- guardResult{customer, method}
		}()
	} else {
		guard <- guardResult{result.Evidence.CustomerGuard, result.Evidence.MethodGuard}
	}
	for history != nil || guard != nil {
		select {
		case observation := <-history:
			result.Evidence.Customer, result.Evidence.Method = observation.customer, observation.method
			history = nil
		case observation := <-guard:
			result.Evidence.CustomerGuard, result.Evidence.MethodGuard = observation.customer, observation.method
			guard = nil
		case <-ctx.Done():
			s.fail(ctx, claim)
			return Result{}, ctx.Err()
		}
	}
	result.Evidence.Decision = domain.Evaluate(request.Input)
	result.Evidence.EvaluatedAt = s.now().UTC()
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
