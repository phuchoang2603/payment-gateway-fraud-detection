package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/adapters/postgres/sqlcgen"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/application"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/payments/internal/domain"
	paymentsv1 "github.com/phuchoang2603/payment-gateway-fraud-detection/shared/proto/payments/v1"
	"google.golang.org/protobuf/proto"
)

func assessmentRecord(row sqlcgen.AssessmentRequest, err error) (application.AssessmentRequestRecord, error) {
	if err != nil {
		return application.AssessmentRequestRecord{}, mapNotFound(err)
	}
	signals := &paymentsv1.AttemptSignals{}
	if err := proto.Unmarshal(row.Signals, signals); err != nil {
		return application.AssessmentRequestRecord{}, err
	}
	return application.AssessmentRequestRecord{AttemptID: row.AttemptID, Signals: signals, EncryptedToken: row.EncryptedToken, Status: row.Status, Fence: row.Fence, RetryUntil: row.RetryUntil}, nil
}

func (s *Store) AssessmentRequest(ctx context.Context, id uuid.UUID) (application.AssessmentRequestRecord, error) {
	return assessmentRecord(sqlcgen.New(s.pool).GetAssessmentRequest(ctx, id))
}

func (s *Store) ClaimAssessment(ctx context.Context, id uuid.UUID, lease time.Duration) (application.AssessmentRequestRecord, error) {
	row, err := sqlcgen.New(s.pool).ClaimAssessmentRequest(ctx, sqlcgen.ClaimAssessmentRequestParams{AttemptID: id, LeaseMs: lease.Milliseconds()})
	if err == pgx.ErrNoRows {
		return application.AssessmentRequestRecord{}, application.ErrAssessmentBusy
	}
	return assessmentRecord(row, err)
}

func (s *Store) FailAssessment(ctx context.Context, id uuid.UUID, fence int64, backoff time.Duration) error {
	rows, err := sqlcgen.New(s.pool).FailAssessmentRequest(ctx, sqlcgen.FailAssessmentRequestParams{AttemptID: id, Fence: fence, BackoffMs: backoff.Milliseconds()})
	if err != nil {
		return err
	}
	if rows != 1 {
		return domain.ErrStaleVersion
	}
	return nil
}
