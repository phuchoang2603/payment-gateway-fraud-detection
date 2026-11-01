CREATE TABLE {if_not_exists}{table} (
  bucket UInt8,
  integration_id String,
  payment_id String CODEC(ZSTD(3)),
  attempt_id String CODEC(ZSTD(3)),
  truth_is_fraud Nullable(Bool),
  truth_scenario Nullable(String),
  chargeback_available_at Nullable(DateTime64(6, 'UTC'))
) ENGINE = MergeTree
ORDER BY (bucket, integration_id, payment_id, attempt_id)
