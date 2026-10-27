-- name: InsertAssessmentRequest :exec
INSERT INTO assessment_requests (attempt_id, signals, encrypted_token, retry_until)
VALUES ($1, $2, $3, $4);

-- name: GetAssessmentRequest :one
SELECT * FROM assessment_requests WHERE attempt_id = $1;

-- name: ClaimAssessmentRequest :one
UPDATE assessment_requests
SET status = 'ASSESSING', fence = fence + 1,
    lease_until = clock_timestamp() + sqlc.arg(lease_ms)::bigint * interval '1 millisecond'
WHERE attempt_id = $1 AND status IN ('PENDING', 'ASSESSING', 'ASSESSMENT_ERROR')
    AND retry_until > clock_timestamp() AND lease_until <= clock_timestamp()
    AND retry_after <= clock_timestamp()
RETURNING *;

-- name: FailAssessmentRequest :execrows
UPDATE assessment_requests SET status = 'ASSESSMENT_ERROR', lease_until = 'epoch',
    retry_after = clock_timestamp() + sqlc.arg(backoff_ms)::bigint * interval '1 millisecond'
WHERE attempt_id = $1 AND fence = $2 AND status = 'ASSESSING' AND lease_until > clock_timestamp();

-- name: CompleteAssessmentRequest :execrows
UPDATE assessment_requests SET status = 'COMPLETED', lease_until = 'epoch'
WHERE attempt_id = $1 AND fence = $2 AND status = 'ASSESSING'
    AND lease_until > clock_timestamp() AND retry_until > clock_timestamp();

-- name: ClearAssessmentToken :exec
UPDATE assessment_requests SET encrypted_token = NULL,
    status = CASE WHEN status = 'COMPLETED' THEN status ELSE 'EXPIRED' END,
    lease_until = 'epoch'
WHERE attempt_id IN (SELECT attempt_id FROM attempts WHERE payment_id = $1);
