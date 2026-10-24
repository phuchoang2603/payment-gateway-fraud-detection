package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/wire"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

const (
	FallbackPolicyVersion = "payments-fallback-v1"
	FallbackReason        = "FRAUD_UNAVAILABLE"
	maxIdempotencyKeyLen  = 255
	storeTimeout          = 5 * time.Second
	maxDriveSteps         = 12
	stalledAttemptFactor  = 2
)

type Config struct {
	PaymentExpiry    time.Duration
	AttemptBudget    time.Duration
	FraudTimeout     time.Duration
	ProcessorTimeout time.Duration
}

type Dependencies struct {
	Store     Store
	Cache     IdempotencyCache
	Fraud     RiskDecider
	Processor ProcessorGateway
	Sealer    Sealer
	Tokens    domain.CheckoutTokens
	Metrics   Metrics
	Clock     Clock
}

type Service struct {
	config Config
	deps   Dependencies
}

func NewService(config Config, deps Dependencies) *Service {
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	return &Service{config: config, deps: deps}
}

func (s *Service) now() time.Time {
	return s.deps.Clock().UTC()
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}

func parseUUID(field, value string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil {
		return uuid.Nil, invalid("%s must be a UUID", field)
	}
	return id, nil
}

func digestOf(message proto.Message) (string, error) {
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return "", fmt.Errorf("marshal request for digest: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Service) CreatePayment(ctx context.Context, request *paymentsv1.CreatePaymentRequest) (*paymentsv1.Payment, error) {
	integrationID, err := parseUUID("integration_id", request.GetIntegrationId())
	if err != nil {
		return nil, err
	}
	key := request.GetIdempotencyKey()
	if key == "" || len(key) > maxIdempotencyKeyLen {
		return nil, invalid("idempotency_key must be 1 to %d characters", maxIdempotencyKeyLen)
	}
	if request.GetExternalSellerId() == "" || request.GetExternalBuyerId() == "" {
		return nil, invalid("external_seller_id and external_buyer_id are required")
	}
	if err := domain.ValidateSnapshot(wire.Snapshot(request.GetSnapshot())); err != nil {
		return nil, invalid("%v", err)
	}
	digest, err := digestOf(&paymentsv1.CreatePaymentRequest{
		IntegrationId: request.GetIntegrationId(), ExternalSellerId: request.GetExternalSellerId(),
		ExternalBuyerId: request.GetExternalBuyerId(), Snapshot: request.GetSnapshot(), ReturnUrl: request.GetReturnUrl(),
	})
	if err != nil {
		return nil, err
	}
	cacheKey := CacheKey{IntegrationID: integrationID, Scope: ScopeCreatePayment, Key: key}

	claimed := false
	if s.deps.Cache != nil {
		existing, ok, cacheErr := s.deps.Cache.Claim(ctx, cacheKey, CacheRecord{Digest: digest, State: CacheInFlight, ClaimedAtMs: s.now().UnixMilli()})
		switch {
		case cacheErr != nil:
			slog.WarnContext(ctx, "idempotency cache unavailable; using database", "error", cacheErr)
		case ok:
			claimed = true
		case existing.Digest != digest:
			return nil, ErrIdempotencyConflict
		case existing.State == CacheCompleted && existing.PaymentID != "":
			if payment, err := s.createReplay(ctx, integrationID, existing.PaymentID); err == nil {
				return payment, nil
			}
		}
	}

	payment, err := s.createInDatabase(ctx, integrationID, key, digest, request)
	if err != nil {
		if claimed && !errors.Is(err, ErrIdempotencyConflict) {
			s.release(ctx, cacheKey, digest)
		}
		return nil, err
	}
	if s.deps.Cache != nil {
		completion := CacheRecord{Digest: digest, State: CacheCompleted, ClaimedAtMs: s.now().UnixMilli(), PaymentID: payment.GetPaymentId()}
		if err := s.deps.Cache.Complete(ctx, cacheKey, completion); err != nil {
			slog.WarnContext(ctx, "complete idempotency cache record", "error", err)
		}
	}
	return payment, nil
}

func (s *Service) release(ctx context.Context, key CacheKey, digest string) {
	if err := s.deps.Cache.Release(context.WithoutCancel(ctx), key, digest); err != nil {
		slog.WarnContext(ctx, "release idempotency claim", "error", err)
	}
}

func (s *Service) createReplay(ctx context.Context, integrationID uuid.UUID, paymentID string) (*paymentsv1.Payment, error) {
	id, err := uuid.Parse(paymentID)
	if err != nil {
		return nil, err
	}
	state, err := s.deps.Store.LoadPayment(ctx, id)
	if err != nil {
		return nil, err
	}
	if state.IntegrationID != integrationID.String() {
		return nil, ErrNotFound
	}
	return s.paymentView(state, true)
}

func (s *Service) createInDatabase(ctx context.Context, integrationID uuid.UUID, key, digest string, request *paymentsv1.CreatePaymentRequest) (*paymentsv1.Payment, error) {
	digestBytes, _ := hex.DecodeString(digest)
	now := s.now()
	paymentID, sessionID := uuid.New(), uuid.New()
	integration := integrationID.String()
	command := domain.CreatePayment{
		Envelope:      domain.Envelope{EventID: uuid.NewString(), Now: now},
		PaymentID:     paymentID.String(),
		IntegrationID: integration,
		CorrelationID: CorrelationID(ctx),
		SessionID:     sessionID.String(),
		ReturnURL:     request.GetReturnUrl(),
		ExpiresAt:     now.Add(s.config.PaymentExpiry).Truncate(time.Second),
		Merchant:      &domain.MerchantRef{MerchantID: domain.MerchantID(integration, request.GetExternalSellerId()).String(), ExternalSellerID: request.GetExternalSellerId()},
		Customer:      &domain.CustomerRef{CustomerID: domain.CustomerID(integration, request.GetExternalBuyerId()).String(), ExternalBuyerID: request.GetExternalBuyerId()},
		Snapshot:      wire.Snapshot(request.GetSnapshot()),
	}
	storeCtx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	state, err := s.deps.Store.CreatePayment(storeCtx, CreateRequest{
		Command:     command,
		Idempotency: IdempotencyClaim{IntegrationID: integrationID, Scope: ScopeCreatePayment, Key: key, Digest: digestBytes, PaymentID: paymentID},
	})
	switch {
	case errors.Is(err, ErrIdempotencyExists):
		record, loadErr := s.deps.Store.Idempotency(storeCtx, integrationID, ScopeCreatePayment, key)
		if loadErr != nil {
			return nil, loadErr
		}
		if hex.EncodeToString(record.Digest) != digest {
			return nil, ErrIdempotencyConflict
		}
		return s.createReplay(storeCtx, integrationID, record.PaymentID.String())
	case errors.Is(err, ErrOrderReferenceTaken):
		return nil, ErrOrderConflict
	case err != nil:
		return nil, err
	}
	return s.paymentView(state, true)
}

func (s *Service) GetPayment(ctx context.Context, request *paymentsv1.GetPaymentRequest) (*paymentsv1.Payment, error) {
	integrationID, err := parseUUID("integration_id", request.GetIntegrationId())
	if err != nil {
		return nil, err
	}
	paymentID, err := parseUUID("payment_id", request.GetPaymentId())
	if err != nil {
		return nil, err
	}
	state, err := s.deps.Store.LoadPayment(ctx, paymentID)
	if err != nil {
		return nil, err
	}
	if state.IntegrationID != integrationID.String() {
		return nil, ErrNotFound
	}
	return s.paymentView(state, false)
}

func (s *Service) GetCheckoutSession(ctx context.Context, request *paymentsv1.GetCheckoutSessionRequest) (*paymentsv1.Payment, error) {
	sessionID, err := parseUUID("session_id", request.GetSessionId())
	if err != nil {
		return nil, err
	}
	tokenSession, _, err := s.deps.Tokens.Verify(request.GetCheckoutToken())
	if err != nil || tokenSession != sessionID {
		return nil, ErrAccessDenied
	}
	state, err := s.deps.Store.LoadPaymentBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	return s.paymentView(state, false)
}

func (s *Service) paymentView(state domain.State, withToken bool) (*paymentsv1.Payment, error) {
	sessionID, err := uuid.Parse(state.SessionID)
	if err != nil {
		return nil, fmt.Errorf("session id: %w", err)
	}
	payment := &paymentsv1.Payment{
		PaymentId:     state.PaymentID,
		SessionId:     state.SessionID,
		ExpiresAt:     timestamppb.New(state.ExpiresAt),
		Status:        paymentStatus(state.Status),
		Version:       state.Version,
		Merchant:      wire.MerchantProto(state.Merchant),
		Customer:      wire.CustomerProto(state.Customer),
		Snapshot:      wire.SnapshotProto(state.Snapshot),
		ReturnUrl:     state.ReturnURL,
		IntegrationId: state.IntegrationID,
	}
	if withToken {
		payment.CheckoutToken = s.deps.Tokens.Issue(sessionID, state.ExpiresAt)
	}
	for _, attempt := range state.Attempts {
		payment.Attempts = append(payment.Attempts, attemptResult(attempt))
	}
	return payment, nil
}

func paymentStatus(status domain.Status) paymentsv1.PaymentStatus {
	switch status {
	case domain.StatusOpen:
		return paymentsv1.PaymentStatus_PAYMENT_STATUS_OPEN
	case domain.StatusSucceeded:
		return paymentsv1.PaymentStatus_PAYMENT_STATUS_SUCCEEDED
	case domain.StatusExpired:
		return paymentsv1.PaymentStatus_PAYMENT_STATUS_EXPIRED
	}
	return paymentsv1.PaymentStatus_PAYMENT_STATUS_UNSPECIFIED
}

func attemptResult(attempt domain.Attempt) *paymentsv1.AttemptResult {
	result := &paymentsv1.AttemptResult{AttemptId: attempt.ID, Status: paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING}
	switch attempt.Phase {
	case domain.PhaseSucceeded:
		result.Status = paymentsv1.AttemptStatus_ATTEMPT_STATUS_SUCCEEDED
	case domain.PhaseDeclined:
		result.Status = paymentsv1.AttemptStatus_ATTEMPT_STATUS_DECLINED
		result.DeclineReason = attempt.DeclineReason
	}
	return result
}

func (s *Service) envelope(version uint64) domain.Envelope {
	return domain.Envelope{EventID: uuid.NewString(), Now: s.now(), ExpectedVersion: version}
}
