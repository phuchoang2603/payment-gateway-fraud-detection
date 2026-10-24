-- +goose Up
ALTER TABLE payment_events ADD COLUMN tracingspancontext text;

-- +goose Down
ALTER TABLE payment_events DROP COLUMN tracingspancontext;
