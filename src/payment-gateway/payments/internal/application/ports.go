package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrOrderReferenceTaken = errors.New("order reference already used by another payment")
	ErrIdempotencyExists   = errors.New("idempotency key already recorded")
	ErrOperationNotFound   = errors.New("processor has no record of the operation")
)

const (
	ScopeCreatePayment = "create_payment"
	ScopeAttemptPrefix = "attempt:"
)

type IdempotencyClaim struct {
	IntegrationID uuid.UUID
	Scope         string
	Key           string
	Digest        []byte
	PaymentID     uuid.UUID
	AttemptID     *uuid.UUID
}

type IdempotencyRecord struct {
	IntegrationID uuid.UUID
	Scope         string
	Key           string
	Digest        []byte
	Completed     bool
	PaymentID     uuid.UUID
	AttemptID     *uuid.UUID
}

type CreateRequest struct {
	Command     domain.CreatePayment
	Idempotency IdempotencyClaim
}

type ProcessorRequestRecord struct {
	OperationKey   string
	PaymentID      uuid.UUID
	AttemptID      uuid.UUID
	IntegrationID  uuid.UUID
	RequestHash    []byte
	EncryptedToken []byte
}

type TransitionRequest struct {
	PaymentID         uuid.UUID
	Command           domain.Command
	Idempotency       *IdempotencyClaim
	ProcessorRequest  *ProcessorRequestRecord
	AssessmentRequest *AssessmentRequestRecord
	AssessmentFence   *int64
}

type AssessmentRequestRecord struct {
	AttemptID      uuid.UUID
	Signals        *paymentsv1.AttemptSignals
	EncryptedToken []byte
	Status         string
	Fence          int64
	RetryUntil     time.Time
}

type RecoveryCandidate struct {
	PaymentID uuid.UUID
	AttemptID uuid.UUID
}

type Store interface {
	CreatePayment(ctx context.Context, request CreateRequest) (domain.State, error)
	Transition(ctx context.Context, request TransitionRequest) (domain.State, error)
	LoadPayment(ctx context.Context, paymentID uuid.UUID) (domain.State, error)
	LoadPaymentBySession(ctx context.Context, sessionID uuid.UUID) (domain.State, error)
	Idempotency(ctx context.Context, integrationID uuid.UUID, scope, key string) (IdempotencyRecord, error)
	ProcessorRequest(ctx context.Context, attemptID uuid.UUID) (ProcessorRequestRecord, error)
	AssessmentRequest(context.Context, uuid.UUID) (AssessmentRequestRecord, error)
	ClaimAssessment(context.Context, uuid.UUID, time.Duration) (AssessmentRequestRecord, error)
	FailAssessment(context.Context, uuid.UUID, int64, time.Duration) error
	RecoveryCandidates(ctx context.Context, updatedBefore time.Time, limit int) ([]RecoveryCandidate, error)
	ExpiryCandidates(ctx context.Context, now, staleBefore time.Time, limit int) ([]uuid.UUID, error)
	OldestAwaitingProcessor(ctx context.Context) (time.Duration, error)
}

type CacheKey struct {
	IntegrationID uuid.UUID
	Scope         string
	Key           string
}

type CacheState string

const (
	CacheInFlight  CacheState = "in_flight"
	CacheCompleted CacheState = "completed"
)

type CacheRecord struct {
	Digest           string     `json:"digest"`
	State            CacheState `json:"state"`
	ClaimedAtMs      int64      `json:"claimed_at_ms"`
	PaymentID        string     `json:"payment_id,omitempty"`
	AttemptID        string     `json:"attempt_id,omitempty"`
	AttemptStatus    string     `json:"attempt_status,omitempty"`
	AssessmentStatus string     `json:"assessment_status,omitempty"`
	DeclineReason    string     `json:"decline_reason,omitempty"`
}

type IdempotencyCache interface {
	Claim(ctx context.Context, key CacheKey, record CacheRecord) (existing *CacheRecord, claimed bool, err error)
	Complete(ctx context.Context, key CacheKey, record CacheRecord) error
	Release(ctx context.Context, key CacheKey, digest string) error
}

type RiskInput struct {
	IntegrationID string
	PaymentID     string
	AttemptID     string
	Merchant      *paymentsv1.MerchantRef
	Customer      *paymentsv1.CustomerRef
	Snapshot      *paymentsv1.CheckoutSnapshot
	Signals       *paymentsv1.AttemptSignals
	CorrelationID string
}

type RiskDecider interface {
	Decide(ctx context.Context, input RiskInput) (*paymentsv1.RiskAssessment, error)
}

type SaleRequest struct {
	OperationKey       string
	IntegrationID      string
	PaymentMethodToken string
	AmountMinorUnits   int64
	Currency           string
	CorrelationID      string
}

type OperationOutcome int

const (
	OutcomeApproved OperationOutcome = iota + 1
	OutcomeDeclined
)

type Operation struct {
	OperationKey       string
	Outcome            OperationOutcome
	DeclineReason      string
	ProcessorReference string
}

type ProcessorGateway interface {
	Sale(ctx context.Context, request SaleRequest) (Operation, error)
	GetOperation(ctx context.Context, operationKey string) (Operation, error)
}

type Sealer interface {
	Seal(plaintext, associatedData []byte) ([]byte, error)
	Open(sealed, associatedData []byte) ([]byte, error)
}

type Metrics interface {
	ObserveAttempt(duration time.Duration)
	AttemptFailure(reason string)
}

type Clock func() time.Time
