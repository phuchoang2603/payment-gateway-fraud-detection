# urn:datacontract:fraud:gold-feat-attempt-rt
## Info
*The batch layer of the fraud-snapshot-v2 windows, the customer and payment-method snapshot values each attempt would have been scored with at its start.*
- **name:** Gold attempt snapshot features
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Training history for the streaming feature views. Join on the attempt key, or as a Feast batch source on the customer or payment-method key with event_timestamp. Live values come from the Flink speed layer, not from this table.

### Purpose
The batch layer of the fraud-snapshot-v2 windows, the customer and payment-method snapshot values each attempt would have been scored with at its start.

### Limitations
Synthetic data only. An attempt counts when it started before the anchor; Flink's observed availability can lag that by its processing delay.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table feat_attempt_rt in schema gold, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### feat_attempt_rt
*One row per (integration_id, payment_id, attempt_id), valid or not.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration namespace of the payment.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Attempt identity within the payment.*<br />• `required` |
|  customer_id | string | *Customer of the attempt; null leaves the customer columns null.* |
|  payment_method_fingerprint | string | *Payment method of the attempt; null leaves the method columns null.* |
|  event_timestamp | timestamp | *The attempt's started_at, when it was scored; the anchor of its values.*<br />• `required` |
|  created | timestamp | *Commit time of the silver.attempts snapshot of the run that last recomputed the row.*<br />• `required` |
|  customer_snapshot_attempt_count_5m | integer | *The customer's other attempts with attempt time in the 5 minutes before the anchor that started before it. Null until the window is covered.* |
|  customer_snapshot_attempt_count_1h | integer | *As above over 1 hour.* |
|  customer_snapshot_attempt_count_24h | integer | *As above over 24 hours.* |
|  customer_snapshot_distinct_devices_24h | integer | *Distinct non-null device fingerprints over those 24-hour attempts.* |
|  method_snapshot_attempt_count_5m | integer | *The payment method's other attempts with attempt time in the 5 minutes before the anchor that started before it. Null until the window is covered.* |
|  method_snapshot_attempt_count_1h | integer | *As above over 1 hour.* |
|  method_snapshot_attempt_count_24h | integer | *As above over 24 hours.* |

## SLA Properties
