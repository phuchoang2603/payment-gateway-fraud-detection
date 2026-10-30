# urn:datacontract:fraud:gold-feat-payment-method-daily
## Info
*7- and 30-day payment method features of fraud-history-batch-v1 at daily anchors, written only where a value changes.*
- **name:** Gold payment-method daily features
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
For a time t, take the payment method's latest row with event_timestamp at or before t; no row means the payment method is unknown. Never require an exact event_timestamp match.

### Purpose
7- and 30-day payment method features of fraud-history-batch-v1 at daily anchors, written only where a value changes.

### Limitations
Synthetic data only. Values are as of the last DP3 anchor, so they lag the scored attempt by up to one day plus the DP3 delay. Windows shorter than seven days are served by the streaming features.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table feat_payment_method_daily in schema gold, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### feat_payment_method_daily
*One row per (integration_id, payment_method_fingerprint, event_timestamp) where a value changes.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration namespace of the payment method.*<br />• `required` |
|  payment_method_fingerprint | string | *Synthetic payment-method fingerprint.*<br />• `required` |
|  event_timestamp | timestamp | *Anchor s, a UTC midnight; the values hold from s until the entity's next row.*<br />• `required` |
|  created | timestamp | *Commit time of the silver.attempts snapshot of the run that last recomputed the row.*<br />• `required` |
|  attempt_count_7d | integer | *Eligible attempts in the window. Window 7d, null until the window is covered.* |
|  amount_sum_minor_7d | integer | *Sum of amount_minor over eligible attempts. Window 7d, null until the window is covered.* |
|  decided_count_7d | integer | *Eligible attempts succeeded or declined before the anchor. Window 7d, null until the window is covered.* |
|  declined_count_7d | integer | *Eligible attempts declined before the anchor. Window 7d, null until the window is covered.* |
|  risk_declined_count_7d | integer | *Eligible attempts declined by the risk policy before the anchor. Window 7d, null until the window is covered.* |
|  decline_rate_7d | number | *declined_count / decided_count; null when decided_count is 0. Window 7d, null until the window is covered.* |
|  distinct_merchant_count_7d | integer | *Distinct merchants over eligible attempts. Window 7d, null until the window is covered.* |
|  distinct_customer_count_7d | integer | *Distinct customers over eligible attempts. Window 7d, null until the window is covered.* |
|  attempt_count_30d | integer | *Eligible attempts in the window. Window 30d, null until the window is covered.* |
|  amount_sum_minor_30d | integer | *Sum of amount_minor over eligible attempts. Window 30d, null until the window is covered.* |
|  decided_count_30d | integer | *Eligible attempts succeeded or declined before the anchor. Window 30d, null until the window is covered.* |
|  declined_count_30d | integer | *Eligible attempts declined before the anchor. Window 30d, null until the window is covered.* |
|  risk_declined_count_30d | integer | *Eligible attempts declined by the risk policy before the anchor. Window 30d, null until the window is covered.* |
|  decline_rate_30d | number | *declined_count / decided_count; null when decided_count is 0. Window 30d, null until the window is covered.* |
|  distinct_merchant_count_30d | integer | *Distinct merchants over eligible attempts. Window 30d, null until the window is covered.* |
|  distinct_customer_count_30d | integer | *Distinct customers over eligible attempts. Window 30d, null until the window is covered.* |

## SLA Properties
