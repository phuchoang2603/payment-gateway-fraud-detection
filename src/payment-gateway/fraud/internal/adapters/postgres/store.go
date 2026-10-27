package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/fraud/internal/application"
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Claim(ctx context.Context, request application.Request, lease, horizon time.Duration) (application.Claim, error) {
	var result application.Claim
	identity, err := json.Marshal(request.Identity)
	if err != nil {
		return result, err
	}
	digest := sha256.Sum256(identity)
	id := hex.EncodeToString(digest[:])
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return result, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	_, err = tx.Exec(ctx, `INSERT INTO assessments
 (assessment_id,integration_id,payment_id,attempt_id,request_digest,state,retry_until)
 VALUES ($1,$2,$3,$4,$5,'NEW',clock_timestamp()+$6*interval '1 millisecond')
 ON CONFLICT DO NOTHING`, id, request.IntegrationID, request.PaymentID, request.AttemptID, request.Digest, horizon.Milliseconds())
	if err != nil {
		return result, err
	}
	var binding, state string
	var payload []byte
	var leaseActive, expired, backoffActive bool
	err = tx.QueryRow(ctx, `SELECT assessment_id,request_digest,state,fence,first_claimed_at,
 lease_until>clock_timestamp(),retry_until<=clock_timestamp(),retry_after>clock_timestamp(),result FROM assessments
 WHERE integration_id=$1 AND payment_id=$2 AND attempt_id=$3 FOR UPDATE`, request.IntegrationID, request.PaymentID, request.AttemptID).Scan(
		&result.ID, &binding, &state, &result.Fence, &result.FirstClaimedAt, &leaseActive, &expired, &backoffActive, &payload,
	)
	if err != nil {
		return result, err
	}
	switch {
	case binding != request.Digest:
		return result, application.ErrConflict
	case state == "COMPLETED":
		result.Result = new(application.Result)
		if err = json.Unmarshal(payload, result.Result); err != nil {
			return result, fmt.Errorf("decode persisted assessment: %w", err)
		}
	case expired:
		return result, application.ErrExpired
	case state == "ASSESSING" && leaseActive, backoffActive:
		return result, application.ErrInProgress
	default:
		err = tx.QueryRow(ctx, `UPDATE assessments SET state='ASSESSING',fence=fence+1,
 lease_until=clock_timestamp()+$2*interval '1 millisecond' WHERE assessment_id=$1 RETURNING fence`, id, lease.Milliseconds()).Scan(&result.Fence)
		if err != nil {
			return result, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return application.Claim{}, err
	}
	return result, nil
}

func (s *Store) Complete(ctx context.Context, claim application.Claim, result application.Result) error {
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE assessments SET state='COMPLETED',result=$3,completed_at=clock_timestamp()
 WHERE assessment_id=$1 AND fence=$2 AND state='ASSESSING' AND lease_until>clock_timestamp()
 AND retry_until>clock_timestamp()`, claim.ID, claim.Fence, payload)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrFenced
	}
	return nil
}

func (s *Store) Fail(ctx context.Context, claim application.Claim, backoff time.Duration) error {
	tag, err := s.pool.Exec(ctx, `UPDATE assessments SET state='ASSESSMENT_ERROR',lease_until='-infinity',
 retry_after=clock_timestamp()+$3*interval '1 millisecond'
 WHERE assessment_id=$1 AND fence=$2 AND state='ASSESSING' AND lease_until>clock_timestamp()`, claim.ID, claim.Fence, backoff.Milliseconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrFenced
	}
	return nil
}

func (s *Store) Ready(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `SELECT assessment_id FROM assessments LIMIT 0`)
	return err
}
