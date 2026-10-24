-- name: InsertEvent :one
INSERT INTO payment_events (event_id, payment_id, aggregate_version, event_type, payload, occurred_at, recorded_at, tracingspancontext)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING global_position;

-- name: LatestEventVersion :one
SELECT coalesce(max(aggregate_version), 0)::bigint AS version FROM payment_events WHERE payment_id = $1;
