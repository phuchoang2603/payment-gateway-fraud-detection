CREATE TABLE {if_not_exists}{table} (
  integration_id LowCardinality(String),
  payment_id String,
  attempt_id String,
  merchant_id String,
  customer_id String,
  payment_method_fingerprint String,
  payment_method_type LowCardinality(String),
  device_fingerprint Nullable(String),
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
  payment_status LowCardinality(String),
  truth_is_fraud Nullable(Bool),
  truth_scenario LowCardinality(Nullable(String)),
  chargeback_available_at Nullable(DateTime64(6, 'UTC')),
  INDEX ix_customer customer_id TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX ix_method payment_method_fingerprint TYPE bloom_filter(0.01) GRANULARITY 4,
  INDEX ix_score risk_score TYPE minmax GRANULARITY 4,
  INDEX ix_outcome outcome TYPE set(8) GRANULARITY 4
) ENGINE = MergeTree
PARTITION BY toDate(started_at)
ORDER BY (integration_id, toDate(started_at), merchant_id, started_at)
TTL toDateTime(started_at) + INTERVAL {retention_days} DAY DELETE
SETTINGS ttl_only_drop_parts = 1
