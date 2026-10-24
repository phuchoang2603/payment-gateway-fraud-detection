-- +goose Up
ALTER TABLE webhook_deliveries ADD COLUMN tracingspancontext text;

-- +goose Down
ALTER TABLE webhook_deliveries DROP COLUMN tracingspancontext;
