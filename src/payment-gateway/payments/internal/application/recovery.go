package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
)

func (s *Service) RecoveryCandidates(ctx context.Context, grace time.Duration, limit int) ([]RecoveryCandidate, error) {
	return s.deps.Store.RecoveryCandidates(ctx, s.now().Add(-grace), limit)
}

func (s *Service) RecoverAttempt(ctx context.Context, candidate RecoveryCandidate) error {
	for range maxDriveSteps {
		state, err := s.deps.Store.LoadPayment(ctx, candidate.PaymentID)
		if err != nil {
			return err
		}
		attempt, ok := state.Attempt(candidate.AttemptID.String())
		if !ok || !attempt.Phase.AwaitingProcessor() {
			return nil
		}
		operation, err := s.lookupOperation(ctx, state, attempt, candidate)
		if err != nil {
			return err
		}
		if operation == nil {
			return nil
		}
		_, err = s.recordOperation(ctx, state, attempt, *operation)
		if errors.Is(err, domain.ErrStaleVersion) {
			continue
		}
		return err
	}
	return fmt.Errorf("attempt %s changed too often to recover", candidate.AttemptID)
}

func (s *Service) lookupOperation(ctx context.Context, state domain.State, attempt domain.Attempt, candidate RecoveryCandidate) (*Operation, error) {
	queryCtx, cancel := context.WithTimeout(ctx, s.config.ProcessorTimeout)
	defer cancel()
	operation, err := s.deps.Processor.GetOperation(queryCtx, attempt.OperationKey)
	if err == nil {
		return &operation, nil
	}
	if !errors.Is(err, ErrOperationNotFound) {
		return nil, fmt.Errorf("query processor operation: %w", err)
	}

	record, err := s.deps.Store.ProcessorRequest(ctx, candidate.AttemptID)
	if err != nil {
		return nil, fmt.Errorf("load processor request: %w", err)
	}
	if len(record.EncryptedToken) == 0 {
		slog.ErrorContext(ctx, "processor request has no token to resubmit", "payment_id", state.PaymentID, "operation_key", attempt.OperationKey)
		return nil, nil
	}
	token, err := s.deps.Sealer.Open(record.EncryptedToken, candidate.AttemptID[:])
	if err != nil {
		return nil, fmt.Errorf("decrypt processor token: %w", err)
	}
	saleCtx, saleCancel := context.WithTimeout(ctx, s.config.ProcessorTimeout)
	defer saleCancel()
	operation, err = s.deps.Processor.Sale(saleCtx, SaleRequest{
		OperationKey: attempt.OperationKey, IntegrationID: state.IntegrationID, PaymentMethodToken: string(token),
		AmountMinorUnits: state.Snapshot.Total.MinorUnits, Currency: state.Snapshot.Total.Currency,
		CorrelationID: state.CorrelationID,
	})
	if err != nil {
		if attempt.Phase == domain.PhaseRequested {
			if _, unknownErr := s.commit(ctx, state, nil, nil, domain.RecordOutcomeUnknown{Envelope: s.envelope(state.Version), AttemptID: attempt.ID}); unknownErr != nil && !errors.Is(unknownErr, domain.ErrStaleVersion) {
				return nil, unknownErr
			}
		}
		return nil, fmt.Errorf("resubmit processor operation: %w", err)
	}
	return &operation, nil
}

func (s *Service) ExpiryCandidates(ctx context.Context, limit int) ([]uuid.UUID, error) {
	return s.deps.Store.ExpiryCandidates(ctx, s.now(), s.now().Add(-s.stalledAfter()), limit)
}

func (s *Service) ExpirePayment(ctx context.Context, paymentID uuid.UUID) error {
	state, err := s.deps.Store.LoadPayment(ctx, paymentID)
	if err != nil {
		return err
	}
	if state.Status != domain.StatusOpen || state.ExpiresAt.After(s.now()) {
		return nil
	}
	_, err = s.commit(ctx, state, nil, nil, domain.ExpirePayment{Envelope: s.envelope(state.Version)})
	var ruleErr *domain.RuleError
	if errors.Is(err, domain.ErrStaleVersion) || (errors.As(err, &ruleErr) && ruleErr.Rule == domain.RuleExpiryWhileUnresolved) || errors.Is(err, ErrPaymentNotOpen) {
		return nil
	}
	return err
}

func (s *Service) OldestAwaitingProcessor(ctx context.Context) (time.Duration, error) {
	return s.deps.Store.OldestAwaitingProcessor(ctx)
}
