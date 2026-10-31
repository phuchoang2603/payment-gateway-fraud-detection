CREATE TABLE {if_not_exists}{table} (
  run_id String,
  window_start Date,
  window_end Date,
  rebuilt_days Array(Date),
  chargebacks_complete_before Date,
  silver_attempts_snapshot_id Int64,
  silver_labels_snapshot_id Int64,
  silver_payment_events_snapshot_id Int64,
  built_at DateTime64(6, 'UTC')
) ENGINE = MergeTree
ORDER BY tuple()
