CREATE TABLE {if_not_exists}{table} (
  bucket UInt8,
  integration_id LowCardinality(String),
  payment_id String CODEC(ZSTD(3)),
  attempt_id String CODEC(ZSTD(3)),
  merchant_id String,
  customer_id String CODEC(ZSTD(3)),
  payment_method_fingerprint String CODEC(ZSTD(3)),
  payment_method_type LowCardinality(String),
  device_fingerprint Nullable(String) CODEC(ZSTD(3)),
  amount_minor Int64,
  currency LowCardinality(String),
  started_at DateTime64(6, 'UTC'),
  attempted_at Nullable(DateTime64(6, 'UTC')),
  risk_outcome LowCardinality(Nullable(String)),
  risk_score Nullable(Float64),
  policy_version LowCardinality(Nullable(String)),
  model_version LowCardinality(Nullable(String)),
  outcome LowCardinality(String),
  decline_source LowCardinality(Nullable(String)),
  outcome_at Nullable(DateTime64(6, 'UTC')),
  payment_status LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (bucket, integration_id, payment_id, attempt_id)
