package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/adapters/postgres/sqlcgen"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/notifier/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
	q    *sqlcgen.Queries
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool, q: sqlcgen.New(pool)}
}

func (s *Store) InTx(ctx context.Context, fn func(application.Repository) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(repository{q: s.q.WithTx(tx)}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

type repository struct {
	q *sqlcgen.Queries
}

func (r repository) SavePaymentRef(ctx context.Context, paymentID, integrationID, orderReference string) error {
	return r.q.UpsertPaymentRef(ctx, sqlcgen.UpsertPaymentRefParams{
		PaymentID:      paymentID,
		IntegrationID:  integrationID,
		OrderReference: orderReference,
	})
}

func (r repository) OrderReference(ctx context.Context, paymentID string) (string, error) {
	ref, err := r.q.GetOrderReference(ctx, paymentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return ref, err
}

func (r repository) InsertDelivery(ctx context.Context, delivery domain.Delivery) (bool, error) {
	rows, err := r.q.InsertDelivery(ctx, sqlcgen.InsertDeliveryParams{
		EventID:            delivery.EventID,
		IntegrationID:      delivery.IntegrationID,
		PaymentID:          delivery.PaymentID,
		EventType:          delivery.EventType,
		Body:               delivery.Body,
		Tracingspancontext: delivery.TraceContext,
		NextAttemptAt:      delivery.NextAttemptAt,
	})
	return rows == 1, err
}

func (s *Store) Claim(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]domain.Delivery, error) {
	rows, err := s.q.ClaimDueDeliveries(ctx, sqlcgen.ClaimDueDeliveriesParams{
		LeaseUntil: now.Add(lease),
		Now:        now,
		BatchSize:  int32(limit),
	})
	if err != nil {
		return nil, err
	}
	claimed := make([]domain.Delivery, 0, len(rows))
	for _, row := range rows {
		claimed = append(claimed, domain.Delivery{
			EventID:       row.EventID,
			IntegrationID: row.IntegrationID,
			PaymentID:     row.PaymentID,
			EventType:     row.EventType,
			Body:          row.Body,
			TraceContext:  row.Tracingspancontext,
			Attempts:      int(row.Attempts),
			CreatedAt:     row.CreatedAt,
		})
	}
	return claimed, nil
}

func (s *Store) Record(ctx context.Context, eventID string, attempt domain.Attempt) error {
	params := sqlcgen.RecordAttemptParams{
		EventID:       eventID,
		Status:        attempt.Status,
		NextAttemptAt: attempt.NextAttemptAt,
		DeliveredAt:   attempt.DeliveredAt,
	}
	if attempt.LastStatus != nil {
		params.LastStatus = pgtype.Int4{Int32: int32(*attempt.LastStatus), Valid: true}
	}
	if attempt.LastError != "" {
		params.LastError = pgtype.Text{String: attempt.LastError, Valid: true}
	}
	return s.q.RecordAttempt(ctx, params)
}

func (s *Store) Stats(ctx context.Context, now time.Time) (domain.Stats, error) {
	row, err := s.q.DeliveryStats(ctx)
	if err != nil {
		return domain.Stats{}, err
	}
	stats := domain.Stats{Pending: row.Pending}
	if row.Pending > 0 {
		stats.OldestPending = now.Sub(row.OldestCreatedAt)
	}
	return stats, nil
}
