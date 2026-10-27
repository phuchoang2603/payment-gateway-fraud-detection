-- name: InsertPayment :exec
INSERT INTO payments (
    payment_id, integration_id, session_id, order_reference, status, version,
    merchant_id, external_seller_id, customer_id, external_buyer_id,
    snapshot, return_url, correlation_id, expires_at, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16);

-- name: GetPayment :one
SELECT * FROM payments WHERE payment_id = $1;

-- name: GetPaymentForUpdate :one
SELECT *, clock_timestamp()::timestamptz AS checked_at FROM payments WHERE payment_id = $1 FOR UPDATE;

-- name: GetPaymentBySession :one
SELECT * FROM payments WHERE session_id = $1;

-- name: UpdatePaymentState :execrows
UPDATE payments
SET status = $2, version = $3, updated_at = $4
WHERE payment_id = $1 AND version = $5;

-- name: ListExpirablePayments :many
SELECT p.payment_id FROM payments p
WHERE p.status = 'open' AND p.expires_at <= $1
  AND NOT EXISTS (
      SELECT 1 FROM attempts a
      WHERE a.payment_id = p.payment_id
        AND (a.status IN ('requested', 'unknown')
             OR (a.status IN ('started', 'risk_approved', 'risk_declined') AND a.updated_at > $2))
  )
ORDER BY p.expires_at
LIMIT $3;
