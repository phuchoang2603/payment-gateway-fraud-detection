CREATE TABLE {if_not_exists}{table} (
  integration_id LowCardinality(String),
  merchant_id String,
  day Date,
  attempts UInt64,
  decided UInt64,
  succeeded UInt64,
  risk_declined UInt64,
  processor_declined UInt64,
  amount_attempted_minor Int64,
  amount_succeeded_minor Int64,
  distinct_customers UInt64,
  truth_labelled UInt64,
  truth_fraud UInt64,
  chargebacks UInt64,
  approval_rate Nullable(Float64),
  risk_decline_rate Nullable(Float64),
  truth_fraud_rate Nullable(Float64),
  chargeback_rate Nullable(Float64),
  chargeback_window_complete Bool
) ENGINE = MergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (integration_id, day, merchant_id)
TTL day + INTERVAL {retention_days} DAY DELETE
