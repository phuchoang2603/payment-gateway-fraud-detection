package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/shared/observe"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/wire"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/postgres/sqlcgen"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
)

const (
	uniqueViolation   = "23505"
	orderRefConstrain = "payments_integration_id_order_reference_key"
)

type Store struct {
	pool *pgxpool.Pool
}

var _ application.Store = (*Store)(nil)

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) inTx(ctx context.Context, fn func(q *sqlcgen.Queries) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(sqlcgen.New(tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (s *Store) CreatePayment(ctx context.Context, request application.CreateRequest) (domain.State, error) {
	command := request.Command
	var result domain.State
	err := s.inTx(ctx, func(q *sqlcgen.Queries) error {
		if err := claimIdempotency(ctx, q, request.Idempotency, command.Now); err != nil {
			return err
		}
		next, domainEvent, err := domain.Transition(domain.State{}, command)
		if err != nil {
			return err
		}
		paymentID, err := uuid.Parse(next.PaymentID)
		if err != nil {
			return fmt.Errorf("payment id: %w", err)
		}
		row, err := paymentRow(next)
		if err != nil {
			return err
		}
		if err := q.InsertPayment(ctx, row); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation && pgErr.ConstraintName == orderRefConstrain {
				return application.ErrOrderReferenceTaken
			}
			return fmt.Errorf("insert payment: %w", err)
		}
		if err := insertEvent(ctx, q, wire.Event(domainEvent)); err != nil {
			return err
		}
		if err := q.CompleteIdempotencyByPayment(ctx, sqlcgen.CompleteIdempotencyByPaymentParams{PaymentID: paymentID, CompletedAt: ptr(command.Now.UTC())}); err != nil {
			return fmt.Errorf("complete idempotency: %w", err)
		}
		result = next
		return nil
	})
	return result, err
}

func (s *Store) Transition(ctx context.Context, request application.TransitionRequest) (domain.State, error) {
	var result domain.State
	err := s.inTx(ctx, func(q *sqlcgen.Queries) error {
		if request.Idempotency != nil {
			if err := claimIdempotency(ctx, q, *request.Idempotency, time.Now()); err != nil {
				return err
			}
		}
		if _, err := q.GetPaymentForUpdate(ctx, request.PaymentID); err != nil {
			return mapNotFound(err)
		}
		current, err := loadState(ctx, q, request.PaymentID)
		if err != nil {
			return err
		}
		next, domainEvent, err := domain.Transition(current, request.Command)
		if err != nil {
			return err
		}
		rows, err := q.UpdatePaymentState(ctx, sqlcgen.UpdatePaymentStateParams{
			PaymentID: request.PaymentID, Status: string(next.Status), Version: int64(next.Version),
			UpdatedAt: next.UpdatedAt, Version_2: int64(current.Version),
		})
		if err != nil {
			return fmt.Errorf("update payment: %w", err)
		}
		if rows != 1 {
			return domain.ErrStaleVersion
		}
		changed, err := persistAttempts(ctx, q, request.PaymentID, current, next)
		if err != nil {
			return err
		}
		if err := insertEvent(ctx, q, wire.Event(domainEvent)); err != nil {
			return err
		}
		if request.ProcessorRequest != nil {
			if wire.Event(domainEvent).GetProcessorOperationRequested().GetOperationKey() != request.ProcessorRequest.OperationKey {
				return errors.New("processor request does not belong to a ProcessorOperationRequested event")
			}
			record := request.ProcessorRequest
			if err := q.InsertProcessorRequest(ctx, sqlcgen.InsertProcessorRequestParams{
				OperationKey: record.OperationKey, PaymentID: record.PaymentID, AttemptID: record.AttemptID,
				IntegrationID: record.IntegrationID, RequestHash: record.RequestHash, EncryptedToken: record.EncryptedToken,
				CreatedAt: next.UpdatedAt,
			}); err != nil {
				return fmt.Errorf("insert processor request: %w", err)
			}
		}
		if changed != nil && !changed.Phase.Active() {
			attemptID := uuid.MustParse(changed.ID)
			if err := q.ClearProcessorRequestToken(ctx, sqlcgen.ClearProcessorRequestTokenParams{AttemptID: attemptID, TokenClearedAt: ptr(next.UpdatedAt)}); err != nil {
				return fmt.Errorf("clear processor token: %w", err)
			}
			if err := q.CompleteIdempotencyByAttempt(ctx, sqlcgen.CompleteIdempotencyByAttemptParams{AttemptID: &attemptID, CompletedAt: ptr(next.UpdatedAt)}); err != nil {
				return fmt.Errorf("complete idempotency: %w", err)
			}
		}
		result = next
		return nil
	})
	return result, err
}

func claimIdempotency(ctx context.Context, q *sqlcgen.Queries, claim application.IdempotencyClaim, now time.Time) error {
	rows, err := q.InsertIdempotency(ctx, sqlcgen.InsertIdempotencyParams{
		IntegrationID: claim.IntegrationID, Scope: claim.Scope, IdempotencyKey: claim.Key,
		RequestDigest: claim.Digest, PaymentID: claim.PaymentID, AttemptID: claim.AttemptID, CreatedAt: now.UTC(),
	})
	if err != nil {
		return fmt.Errorf("insert idempotency: %w", err)
	}
	if rows == 0 {
		return application.ErrIdempotencyExists
	}
	return nil
}

func persistAttempts(ctx context.Context, q *sqlcgen.Queries, paymentID uuid.UUID, current, next domain.State) (*domain.Attempt, error) {
	for i := range next.Attempts {
		attempt := next.Attempts[i]
		attemptID, err := uuid.Parse(attempt.ID)
		if err != nil {
			return nil, fmt.Errorf("attempt id: %w", err)
		}
		if i >= len(current.Attempts) {
			err := q.InsertAttempt(ctx, sqlcgen.InsertAttemptParams{
				AttemptID: attemptID, PaymentID: paymentID, Ordinal: int32(i), Status: string(attempt.Phase),
				Version: int64(attempt.Version), DeclineSource: int32(attempt.DeclineSource), DeclineReason: attempt.DeclineReason,
				OperationKey: attempt.OperationKey, ProcessorReference: attempt.ProcessorReference,
				StartedAt: attempt.StartedAt, UpdatedAt: attempt.UpdatedAt,
			})
			if err != nil {
				return nil, fmt.Errorf("insert attempt: %w", err)
			}
			return &attempt, nil
		}
		previous := current.Attempts[i]
		if previous.Version == attempt.Version {
			continue
		}
		rows, err := q.UpdateAttempt(ctx, sqlcgen.UpdateAttemptParams{
			AttemptID: attemptID, Status: string(attempt.Phase), Version: int64(attempt.Version),
			DeclineSource: int32(attempt.DeclineSource), DeclineReason: attempt.DeclineReason,
			OperationKey: attempt.OperationKey, ProcessorReference: attempt.ProcessorReference,
			UpdatedAt: attempt.UpdatedAt, Version_2: int64(previous.Version),
		})
		if err != nil {
			return nil, fmt.Errorf("update attempt: %w", err)
		}
		if rows != 1 {
			return nil, domain.ErrStaleVersion
		}
		return &attempt, nil
	}
	return nil, nil
}

func insertEvent(ctx context.Context, q *sqlcgen.Queries, event *paymentsv1.PaymentEvent) error {
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	eventID, err := uuid.Parse(event.GetEventId())
	if err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	paymentID, err := uuid.Parse(event.GetPaymentId())
	if err != nil {
		return fmt.Errorf("payment id: %w", err)
	}
	_, err = q.InsertEvent(ctx, sqlcgen.InsertEventParams{
		EventID: eventID, PaymentID: paymentID, AggregateVersion: int64(event.GetAggregateVersion()),
		EventType: eventType(event), Payload: payload, Tracingspancontext: observe.SerializeContext(ctx),
		OccurredAt: event.GetOccurredAt().AsTime(), RecordedAt: event.GetRecordedAt().AsTime(),
	})
	if err != nil {
		return fmt.Errorf("insert ledger event: %w", err)
	}
	return nil
}

func eventType(event *paymentsv1.PaymentEvent) string {
	field := event.ProtoReflect().WhichOneof(event.ProtoReflect().Descriptor().Oneofs().ByName("payload"))
	if field == nil {
		return ""
	}
	return string(field.Name())
}

func paymentRow(state domain.State) (sqlcgen.InsertPaymentParams, error) {
	var ids [4]uuid.UUID
	for i, raw := range []string{state.PaymentID, state.IntegrationID, state.SessionID, state.Merchant.MerchantID} {
		id, err := uuid.Parse(raw)
		if err != nil {
			return sqlcgen.InsertPaymentParams{}, fmt.Errorf("parse identity %q: %w", raw, err)
		}
		ids[i] = id
	}
	customerID, err := uuid.Parse(state.Customer.CustomerID)
	if err != nil {
		return sqlcgen.InsertPaymentParams{}, fmt.Errorf("parse customer id: %w", err)
	}
	snapshot, err := proto.Marshal(wire.SnapshotProto(state.Snapshot))
	if err != nil {
		return sqlcgen.InsertPaymentParams{}, fmt.Errorf("marshal snapshot: %w", err)
	}
	return sqlcgen.InsertPaymentParams{
		PaymentID: ids[0], IntegrationID: ids[1], SessionID: ids[2], OrderReference: state.Snapshot.ExternalOrderID,
		Status: string(state.Status), Version: int64(state.Version), MerchantID: ids[3],
		ExternalSellerID: state.Merchant.ExternalSellerID, CustomerID: customerID,
		ExternalBuyerID: state.Customer.ExternalBuyerID, Snapshot: snapshot, ReturnUrl: state.ReturnURL,
		CorrelationID: state.CorrelationID, ExpiresAt: state.ExpiresAt, CreatedAt: state.CreatedAt, UpdatedAt: state.UpdatedAt,
	}, nil
}

func (s *Store) readSnapshot(ctx context.Context, fn func(q *sqlcgen.Queries) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	return fn(sqlcgen.New(tx))
}

func (s *Store) LoadPayment(ctx context.Context, paymentID uuid.UUID) (domain.State, error) {
	var state domain.State
	err := s.readSnapshot(ctx, func(q *sqlcgen.Queries) error {
		var err error
		state, err = loadState(ctx, q, paymentID)
		return err
	})
	return state, err
}

func (s *Store) LoadPaymentBySession(ctx context.Context, sessionID uuid.UUID) (domain.State, error) {
	var state domain.State
	err := s.readSnapshot(ctx, func(q *sqlcgen.Queries) error {
		row, err := q.GetPaymentBySession(ctx, sessionID)
		if err != nil {
			return mapNotFound(err)
		}
		state, err = loadState(ctx, q, row.PaymentID)
		return err
	})
	return state, err
}

func loadState(ctx context.Context, q *sqlcgen.Queries, paymentID uuid.UUID) (domain.State, error) {
	row, err := q.GetPayment(ctx, paymentID)
	if err != nil {
		return domain.State{}, mapNotFound(err)
	}
	attempts, err := q.ListAttempts(ctx, paymentID)
	if err != nil {
		return domain.State{}, fmt.Errorf("list attempts: %w", err)
	}
	snapshot := &paymentsv1.CheckoutSnapshot{}
	if err := proto.Unmarshal(row.Snapshot, snapshot); err != nil {
		return domain.State{}, fmt.Errorf("unmarshal snapshot: %w", err)
	}
	state := domain.State{
		PaymentID: row.PaymentID.String(), IntegrationID: row.IntegrationID.String(), CorrelationID: row.CorrelationID,
		SessionID: row.SessionID.String(), Status: domain.Status(row.Status), Version: uint64(row.Version),
		Merchant: &domain.MerchantRef{MerchantID: row.MerchantID.String(), ExternalSellerID: row.ExternalSellerID},
		Customer: &domain.CustomerRef{CustomerID: row.CustomerID.String(), ExternalBuyerID: row.ExternalBuyerID},
		Snapshot: wire.Snapshot(snapshot), ReturnURL: row.ReturnUrl,
		ExpiresAt: row.ExpiresAt.UTC(), CreatedAt: row.CreatedAt.UTC(), UpdatedAt: row.UpdatedAt.UTC(),
	}
	for _, attempt := range attempts {
		state.Attempts = append(state.Attempts, domain.Attempt{
			ID: attempt.AttemptID.String(), Version: uint64(attempt.Version), Phase: domain.Phase(attempt.Status),
			DeclineSource: domain.DeclineSource(attempt.DeclineSource), DeclineReason: attempt.DeclineReason,
			OperationKey: attempt.OperationKey, ProcessorReference: attempt.ProcessorReference,
			StartedAt: attempt.StartedAt.UTC(), UpdatedAt: attempt.UpdatedAt.UTC(),
		})
	}
	return state, nil
}

func (s *Store) Idempotency(ctx context.Context, integrationID uuid.UUID, scope, key string) (application.IdempotencyRecord, error) {
	row, err := sqlcgen.New(s.pool).GetIdempotency(ctx, sqlcgen.GetIdempotencyParams{IntegrationID: integrationID, Scope: scope, IdempotencyKey: key})
	if err != nil {
		return application.IdempotencyRecord{}, mapNotFound(err)
	}
	return application.IdempotencyRecord{
		IntegrationID: row.IntegrationID, Scope: row.Scope, Key: row.IdempotencyKey, Digest: row.RequestDigest,
		Completed: row.Status == "completed", PaymentID: row.PaymentID, AttemptID: row.AttemptID,
	}, nil
}

func (s *Store) ProcessorRequest(ctx context.Context, attemptID uuid.UUID) (application.ProcessorRequestRecord, error) {
	row, err := sqlcgen.New(s.pool).GetProcessorRequest(ctx, attemptID)
	if err != nil {
		return application.ProcessorRequestRecord{}, mapNotFound(err)
	}
	return application.ProcessorRequestRecord{
		OperationKey: row.OperationKey, PaymentID: row.PaymentID, AttemptID: row.AttemptID,
		IntegrationID: row.IntegrationID, RequestHash: row.RequestHash, EncryptedToken: row.EncryptedToken,
	}, nil
}

func (s *Store) RecoveryCandidates(ctx context.Context, updatedBefore time.Time, limit int) ([]application.RecoveryCandidate, error) {
	rows, err := sqlcgen.New(s.pool).ListRecoveryAttempts(ctx, sqlcgen.ListRecoveryAttemptsParams{UpdatedAt: updatedBefore, Limit: int32(limit)})
	if err != nil {
		return nil, fmt.Errorf("list recovery attempts: %w", err)
	}
	candidates := make([]application.RecoveryCandidate, len(rows))
	for i, row := range rows {
		candidates[i] = application.RecoveryCandidate{PaymentID: row.PaymentID, AttemptID: row.AttemptID}
	}
	return candidates, nil
}

func (s *Store) ExpiryCandidates(ctx context.Context, now, staleBefore time.Time, limit int) ([]uuid.UUID, error) {
	ids, err := sqlcgen.New(s.pool).ListExpirablePayments(ctx, sqlcgen.ListExpirablePaymentsParams{ExpiresAt: now, UpdatedAt: staleBefore, Limit: int32(limit)})
	if err != nil {
		return nil, fmt.Errorf("list expirable payments: %w", err)
	}
	return ids, nil
}

func (s *Store) OldestAwaitingProcessor(ctx context.Context) (time.Duration, error) {
	seconds, err := sqlcgen.New(s.pool).OldestAwaitingProcessor(ctx)
	if err != nil {
		return 0, fmt.Errorf("oldest awaiting processor: %w", err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func mapNotFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return application.ErrNotFound
	}
	return err
}

func ptr[T any](value T) *T {
	return &value
}
