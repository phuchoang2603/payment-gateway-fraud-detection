-- name: UpsertPaymentRef :exec
INSERT INTO payment_refs (payment_id, integration_id, order_reference)
VALUES ($1, $2, $3)
ON CONFLICT (payment_id) DO NOTHING;

-- name: GetOrderReference :one
SELECT order_reference FROM payment_refs WHERE payment_id = $1;

-- name: InsertDelivery :execrows
INSERT INTO webhook_deliveries (event_id, integration_id, payment_id, event_type, body, next_attempt_at, tracingspancontext)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (event_id) DO NOTHING;

-- name: ClaimDueDeliveries :many
UPDATE webhook_deliveries
SET next_attempt_at = sqlc.arg(lease_until)
WHERE event_id IN (
    SELECT d.event_id
    FROM webhook_deliveries d
    WHERE d.status = 'pending' AND d.next_attempt_at <= sqlc.arg(now)
    ORDER BY d.next_attempt_at
    LIMIT sqlc.arg(batch_size)
    FOR UPDATE SKIP LOCKED
)
RETURNING event_id, integration_id, payment_id, event_type, body, attempts, created_at, tracingspancontext;

-- name: RecordAttempt :exec
UPDATE webhook_deliveries
SET status = $2,
    attempts = attempts + 1,
    next_attempt_at = $3,
    last_status = $4,
    last_error = $5,
    delivered_at = $6
WHERE event_id = $1;

-- name: DeliveryStats :one
SELECT count(*)::bigint AS pending, COALESCE(min(created_at), 'epoch'::timestamptz)::timestamptz AS oldest_created_at
FROM webhook_deliveries
WHERE status = 'pending';
