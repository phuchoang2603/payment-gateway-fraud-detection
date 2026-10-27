-- name: InsertAttempt :exec
INSERT INTO attempts (
    attempt_id, payment_id, ordinal, status, version, decline_source, decline_reason,
    operation_key, processor_reference, started_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: UpdateAttempt :execrows
UPDATE attempts
SET status = $2, version = $3, decline_source = $4, decline_reason = $5,
    operation_key = $6, processor_reference = $7, updated_at = $8
WHERE attempt_id = $1 AND version = $9;

-- name: ListAttempts :many
SELECT a.*, CASE WHEN r.status <> 'COMPLETED' AND r.retry_until <= clock_timestamp()
THEN 'EXPIRED' ELSE coalesce(r.status, '') END::text AS assessment_status
FROM attempts a LEFT JOIN assessment_requests r USING (attempt_id)
WHERE payment_id = $1 ORDER BY ordinal;

-- name: GetAttempt :one
SELECT * FROM attempts WHERE attempt_id = $1;

-- name: ListRecoveryAttempts :many
SELECT a.attempt_id, a.payment_id, a.status, a.updated_at FROM attempts a
JOIN payments p USING (payment_id)
LEFT JOIN assessment_requests r USING (attempt_id)
WHERE p.status = 'open' AND a.updated_at <= $1
  AND (a.status IN ('requested', 'unknown', 'risk_approved', 'risk_declined')
       OR (a.status = 'started' AND r.retry_until > clock_timestamp()
           AND r.lease_until <= clock_timestamp() AND r.retry_after <= clock_timestamp()))
ORDER BY a.updated_at
LIMIT $2;

-- name: OldestAwaitingProcessor :one
SELECT coalesce(extract(epoch FROM now() - min(updated_at)), 0)::float8 AS age_seconds FROM attempts WHERE status IN ('requested', 'unknown');
