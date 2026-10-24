package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/wire"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

type attemptInput struct {
	integrationID uuid.UUID
	paymentID     uuid.UUID
	attemptID     uuid.UUID
	token         string
	signals       *paymentsv1.AttemptSignals
	correlationID string
	claim         IdempotencyClaim
	cacheKey      CacheKey
	digest        string
}

func (s *Service) StartAttempt(ctx context.Context, request *paymentsv1.StartAttemptRequest) (*paymentsv1.AttemptResult, error) {
	started := s.now()
	integrationID, err := parseUUID("integration_id", request.GetIntegrationId())
	if err != nil {
		return nil, err
	}
	sessionID, err := parseUUID("session_id", request.GetSessionId())
	if err != nil {
		return nil, err
	}
	key := request.GetIdempotencyKey()
	if key == "" || len(key) > maxIdempotencyKeyLen {
		return nil, invalid("idempotency_key must be 1 to %d characters", maxIdempotencyKeyLen)
	}
	if request.GetPaymentMethodToken() == "" {
		return nil, invalid("payment_method_token is required")
	}
	tokenSession, tokenExpiry, err := s.deps.Tokens.Verify(request.GetCheckoutToken())
	if err != nil || tokenSession != sessionID || !tokenExpiry.After(started) {
		return nil, ErrAccessDenied
	}
	state, err := s.deps.Store.LoadPaymentBySession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if state.IntegrationID != integrationID.String() {
		return nil, ErrNotFound
	}
	paymentID := uuid.MustParse(state.PaymentID)
	attemptID := uuid.NewSHA1(paymentID, []byte("attempt:"+key))
	digest, err := digestOf(&paymentsv1.StartAttemptRequest{
		IntegrationId: request.GetIntegrationId(), SessionId: request.GetSessionId(), PaymentMethodToken: request.GetPaymentMethodToken(),
		Signals: signalsWithoutTime(request.GetSignals()),
	})
	if err != nil {
		return nil, err
	}
	digestBytes, _ := hex.DecodeString(digest)
	signals := proto.Clone(request.GetSignals()).(*paymentsv1.AttemptSignals)
	if request.GetSignals() == nil {
		signals = &paymentsv1.AttemptSignals{}
	}
	if signals.GetAttemptedAt() == nil {
		signals.AttemptedAt = timestamppb.New(started)
	}
	scope := ScopeAttemptPrefix + sessionID.String()
	input := attemptInput{
		integrationID: integrationID, paymentID: paymentID, attemptID: attemptID,
		token: request.GetPaymentMethodToken(), signals: signals, correlationID: CorrelationID(ctx),
		claim:    IdempotencyClaim{IntegrationID: integrationID, Scope: scope, Key: key, Digest: digestBytes, PaymentID: paymentID, AttemptID: &attemptID},
		cacheKey: CacheKey{IntegrationID: integrationID, Scope: scope, Key: key},
		digest:   digest,
	}

	claimed := false
	if s.deps.Cache != nil {
		existing, ok, cacheErr := s.deps.Cache.Claim(ctx, input.cacheKey, CacheRecord{Digest: digest, State: CacheInFlight, ClaimedAtMs: started.UnixMilli(), AttemptID: attemptID.String()})
		switch {
		case cacheErr != nil:
			slog.WarnContext(ctx, "idempotency cache unavailable; using database", "error", cacheErr)
		case ok:
			claimed = true
		case existing.Digest != digest:
			return nil, ErrIdempotencyConflict
		case existing.State == CacheCompleted:
			return cachedResult(existing), nil
		case started.UnixMilli()-existing.ClaimedAtMs < s.stalledAfter().Milliseconds():
			return &paymentsv1.AttemptResult{AttemptId: attemptID.String(), Status: paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING}, nil
		}
	}

	result, err := s.runAttempt(ctx, input)
	if err != nil {
		if claimed && !errors.Is(err, ErrIdempotencyConflict) && !errors.Is(err, errAttemptInProgress) {
			s.release(ctx, input.cacheKey, digest)
		}
		return nil, err
	}
	if s.deps.Metrics != nil {
		s.deps.Metrics.ObserveAttempt(s.now().Sub(started))
	}
	if result.GetStatus() != paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING && s.deps.Cache != nil {
		completion := CacheRecord{
			Digest: digest, State: CacheCompleted, ClaimedAtMs: started.UnixMilli(), AttemptID: result.GetAttemptId(),
			AttemptStatus: result.GetStatus().String(), DeclineReason: result.GetDeclineReason(),
		}
		if err := s.deps.Cache.Complete(ctx, input.cacheKey, completion); err != nil {
			slog.WarnContext(ctx, "complete idempotency cache record", "error", err)
		}
	}
	return result, nil
}

var errAttemptInProgress = errors.New("attempt is still being processed")

func (s *Service) stalledAfter() time.Duration {
	return s.config.AttemptBudget * stalledAttemptFactor
}

func cachedResult(record *CacheRecord) *paymentsv1.AttemptResult {
	status := paymentsv1.AttemptStatus_ATTEMPT_STATUS_PENDING
	if value, ok := paymentsv1.AttemptStatus_value[record.AttemptStatus]; ok {
		status = paymentsv1.AttemptStatus(value)
	}
	return &paymentsv1.AttemptResult{AttemptId: record.AttemptID, Status: status, DeclineReason: record.DeclineReason}
}

func signalsWithoutTime(signals *paymentsv1.AttemptSignals) *paymentsv1.AttemptSignals {
	if signals == nil {
		return nil
	}
	stripped := proto.Clone(signals).(*paymentsv1.AttemptSignals)
	stripped.AttemptedAt = nil
	return stripped
}

func (s *Service) runAttempt(ctx context.Context, input attemptInput) (*paymentsv1.AttemptResult, error) {
	budgetCtx, cancel := context.WithTimeout(ctx, s.config.AttemptBudget)
	defer cancel()

	record, err := s.deps.Store.Idempotency(ctx, input.integrationID, input.claim.Scope, input.claim.Key)
	switch {
	case err == nil:
		return s.resumeAttempt(ctx, budgetCtx, input, record)
	case !errors.Is(err, ErrNotFound):
		return nil, err
	}

	var state domain.State
	for range maxDriveSteps {
		state, err = s.deps.Store.LoadPayment(ctx, input.paymentID)
		if err != nil {
			return nil, err
		}
		if state.Status != domain.StatusOpen || !state.ExpiresAt.After(s.now()) {
			return s.replayOr(ctx, budgetCtx, input, ErrPaymentNotOpen)
		}
		claim := input.claim
		state, err = s.commit(ctx, state, &claim, nil, domain.StartAttempt{
			Envelope: s.envelope(state.Version), AttemptID: input.attemptID.String(), Signals: wire.Signals(input.signals),
		})
		switch {
		case err == nil:
			return s.drive(ctx, budgetCtx, state, input)
		case errors.Is(err, ErrIdempotencyExists):
			record, loadErr := s.deps.Store.Idempotency(ctx, input.integrationID, input.claim.Scope, input.claim.Key)
			if loadErr != nil {
				return nil, loadErr
			}
			return s.resumeAttempt(ctx, budgetCtx, input, record)
		case errors.Is(err, domain.ErrStaleVersion):
			continue
		case errors.Is(err, ErrAttemptActive), errors.Is(err, ErrPaymentNotOpen):
			return s.replayOr(ctx, budgetCtx, input, err)
		default:
			return nil, err
		}
	}
	return nil, fmt.Errorf("payment %s changed too often to start an attempt", input.paymentID)
}

func (s *Service) replayOr(ctx, budgetCtx context.Context, input attemptInput, refusal error) (*paymentsv1.AttemptResult, error) {
	record, err := s.deps.Store.Idempotency(ctx, input.integrationID, input.claim.Scope, input.claim.Key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, refusal
		}
		return nil, err
	}
	return s.resumeAttempt(ctx, budgetCtx, input, record)
}

func (s *Service) resumeAttempt(ctx, budgetCtx context.Context, input attemptInput, record IdempotencyRecord) (*paymentsv1.AttemptResult, error) {
	if hex.EncodeToString(record.Digest) != input.digest {
		return nil, ErrIdempotencyConflict
	}
	state, err := s.deps.Store.LoadPayment(ctx, input.paymentID)
	if err != nil {
		return nil, err
	}
	attempt, ok := state.Attempt(input.attemptID.String())
	if !ok {
		return nil, ErrNotFound
	}
	switch attempt.Phase {
	case domain.PhaseStarted, domain.PhaseRiskApproved, domain.PhaseRiskDeclined:
		if s.now().Sub(attempt.UpdatedAt) < s.stalledAfter() {
			return attemptResult(attempt), nil
		}
		return s.drive(ctx, budgetCtx, state, input)
	default:
		return attemptResult(attempt), nil
	}
}

func (s *Service) commit(ctx context.Context, state domain.State, claim *IdempotencyClaim, processorRequest *ProcessorRequestRecord, command domain.Command) (domain.State, error) {
	storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer cancel()
	next, err := s.deps.Store.Transition(storeCtx, TransitionRequest{
		PaymentID: uuid.MustParse(state.PaymentID), Command: command, Idempotency: claim, ProcessorRequest: processorRequest,
	})
	if err != nil {
		var ruleErr *domain.RuleError
		if errors.As(err, &ruleErr) && !errors.Is(err, domain.ErrStaleVersion) {
			switch ruleErr.Rule {
			case domain.RuleEventAfterTerminal:
				return domain.State{}, ErrPaymentNotOpen
			case domain.RuleAttemptWhileActive:
				return domain.State{}, ErrAttemptActive
			}
		}
		return domain.State{}, err
	}
	return next, nil
}

func (s *Service) drive(ctx, budgetCtx context.Context, state domain.State, input attemptInput) (*paymentsv1.AttemptResult, error) {
	attemptID := input.attemptID.String()
	for range maxDriveSteps {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		attempt, ok := state.Attempt(attemptID)
		if !ok {
			return nil, ErrNotFound
		}
		var (
			next domain.State
			err  error
		)
		switch attempt.Phase {
		case domain.PhaseStarted:
			assessment := s.assess(budgetCtx, state, input)
			next, err = s.commit(ctx, state, nil, nil, domain.RecordRiskDecision{Envelope: s.envelope(state.Version), AttemptID: attemptID, Assessment: wire.Assessment(assessment)})
		case domain.PhaseRiskDeclined:
			if strings.EqualFold(attempt.DeclineReason, FallbackReason) {
				s.failure("fraud_unavailable")
			} else {
				s.failure("risk_declined")
			}
			next, err = s.commit(ctx, state, nil, nil, domain.DeclineAttempt{
				Envelope: s.envelope(state.Version), AttemptID: attemptID, Source: domain.DeclineSourceRisk, Reason: attempt.DeclineReason,
			})
		case domain.PhaseRiskApproved:
			next, err = s.requestProcessor(ctx, state, input)
		case domain.PhaseRequested:
			if budgetCtx.Err() != nil {
				return attemptResult(attempt), nil
			}
			next, err = s.callProcessor(ctx, budgetCtx, state, attempt, input)
		default:
			return attemptResult(attempt), nil
		}
		switch {
		case err == nil:
			state = next
		case errors.Is(err, domain.ErrStaleVersion):
			if state, err = s.deps.Store.LoadPayment(ctx, input.paymentID); err != nil {
				return nil, err
			}
		default:
			return nil, err
		}
	}
	return nil, fmt.Errorf("attempt %s did not settle", attemptID)
}

func (s *Service) failure(reason string) {
	if s.deps.Metrics != nil {
		s.deps.Metrics.AttemptFailure(reason)
	}
}

func (s *Service) assess(budgetCtx context.Context, state domain.State, input attemptInput) *paymentsv1.RiskAssessment {
	fallback := &paymentsv1.RiskAssessment{
		Outcome:       paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE,
		ReasonCodes:   []string{FallbackReason},
		PolicyVersion: FallbackPolicyVersion,
	}
	timeout := min(s.config.FraudTimeout, remaining(budgetCtx))
	if timeout <= 0 {
		return fallback
	}
	fraudCtx, cancel := context.WithTimeout(budgetCtx, timeout)
	defer cancel()
	assessment, err := s.deps.Fraud.Decide(fraudCtx, RiskInput{
		IntegrationID: state.IntegrationID, PaymentID: state.PaymentID, AttemptID: input.attemptID.String(),
		Merchant: wire.MerchantProto(state.Merchant), Customer: wire.CustomerProto(state.Customer), Snapshot: wire.SnapshotProto(state.Snapshot), Signals: input.signals,
		CorrelationID: input.correlationID,
	})
	if err != nil || assessment == nil {
		slog.WarnContext(budgetCtx, "fraud decision unavailable; failing closed", "payment_id", state.PaymentID, "error", err)
		return fallback
	}
	if assessment.GetPolicyVersion() == "" || slices.Contains(assessment.GetReasonCodes(), "") ||
		(assessment.GetOutcome() != paymentsv1.RiskOutcome_RISK_OUTCOME_APPROVE && assessment.GetOutcome() != paymentsv1.RiskOutcome_RISK_OUTCOME_DECLINE) {
		slog.WarnContext(budgetCtx, "fraud returned an unusable assessment; failing closed", "payment_id", state.PaymentID)
		return fallback
	}
	return assessment
}

func remaining(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return time.Hour
	}
	return time.Until(deadline)
}

func (s *Service) requestProcessor(ctx context.Context, state domain.State, input attemptInput) (domain.State, error) {
	sealed, err := s.deps.Sealer.Seal([]byte(input.token), input.attemptID[:])
	if err != nil {
		return domain.State{}, fmt.Errorf("seal payment method token: %w", err)
	}
	total := state.Snapshot.Total
	tokenDigest := sha256.Sum256([]byte(input.token))
	binding := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s\n%d\n%s\n%x", state.IntegrationID, operationKey(input.attemptID), total.MinorUnits, total.Currency, tokenDigest)))
	return s.commit(ctx, state, nil, &ProcessorRequestRecord{
		OperationKey: operationKey(input.attemptID), PaymentID: input.paymentID, AttemptID: input.attemptID,
		IntegrationID: input.integrationID, RequestHash: binding[:], EncryptedToken: sealed,
	}, domain.RequestProcessorOperation{Envelope: s.envelope(state.Version), AttemptID: input.attemptID.String(), OperationKey: operationKey(input.attemptID)})
}

func operationKey(attemptID uuid.UUID) string {
	return "op-" + attemptID.String()
}

func (s *Service) callProcessor(ctx, budgetCtx context.Context, state domain.State, attempt domain.Attempt, input attemptInput) (domain.State, error) {
	timeout := min(s.config.ProcessorTimeout, remaining(budgetCtx))
	saleCtx, cancel := context.WithTimeout(budgetCtx, timeout)
	defer cancel()
	operation, err := s.deps.Processor.Sale(saleCtx, SaleRequest{
		OperationKey: attempt.OperationKey, IntegrationID: state.IntegrationID, PaymentMethodToken: input.token,
		AmountMinorUnits: state.Snapshot.Total.MinorUnits, Currency: state.Snapshot.Total.Currency,
		CorrelationID: input.correlationID,
	})
	if err != nil {
		slog.WarnContext(ctx, "processor outcome unknown", "payment_id", state.PaymentID, "operation_key", attempt.OperationKey, "error", err)
		s.failure("processor_unknown")
		return s.commit(ctx, state, nil, nil, domain.RecordOutcomeUnknown{Envelope: s.envelope(state.Version), AttemptID: attempt.ID})
	}
	return s.recordOperation(ctx, state, attempt, operation)
}

func (s *Service) recordOperation(ctx context.Context, state domain.State, attempt domain.Attempt, operation Operation) (domain.State, error) {
	switch operation.Outcome {
	case OutcomeApproved:
		reference := operation.ProcessorReference
		if reference == "" {
			reference = operation.OperationKey
		}
		return s.commit(ctx, state, nil, nil, domain.SucceedPayment{Envelope: s.envelope(state.Version), AttemptID: attempt.ID, ProcessorReference: reference})
	case OutcomeDeclined:
		reason := operation.DeclineReason
		if reason == "" {
			reason = "declined"
		}
		s.failure("processor_declined")
		return s.commit(ctx, state, nil, nil, domain.DeclineAttempt{
			Envelope: s.envelope(state.Version), AttemptID: attempt.ID, Source: domain.DeclineSourceProcessor, Reason: reason,
		})
	}
	return domain.State{}, fmt.Errorf("unsupported processor outcome %d", operation.Outcome)
}
