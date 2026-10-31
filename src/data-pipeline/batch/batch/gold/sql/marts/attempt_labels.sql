CREATE TABLE {if_not_exists}{table} (
  integration_id String,
  payment_id String,
  attempt_id String,
  truth_is_fraud Nullable(Bool),
  truth_scenario Nullable(String),
  chargeback_available_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = MergeTree
ORDER BY (integration_id, payment_id, attempt_id)
