# urn:datacontract:fraud:marts-fact-payment-attempt
## Info
*One row per lifecycle-valid Silver attempt inside the retention window, with its risk decision, outcome, earliest simulation-truth label and earliest chargeback.*
- **name:** Payment attempt fact
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Filter on started_at for time ranges, which prunes by partition and primary key; filter on customer_id or payment_method_fingerprint for investigations, which uses the bloom-filter indexes. Join dim_customer point-in-time on started_at.

### Purpose
One row per lifecycle-valid Silver attempt inside the retention window, with its risk decision, outcome, earliest simulation-truth label and earliest chargeback.

### Limitations
Synthetic data only. truth_is_fraud and truth_scenario are a synthetic oracle. A missing chargeback is not a negative, and recent attempts may still receive one.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Native MergeTree table in the ClickHouse marts database. The rule placeholder {object} stands for the table and {table:name} for another table of the same build.*<br />• **environment:** dev<br />• **database:** marts<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### fact_payment_attempt
*One row per (integration_id, payment_id, attempt_id); ordered by (integration_id, toDate(started_at), merchant_id, started_at).*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration namespace of the payment.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Attempt identity within the payment.*<br />• `required` |
|  merchant_id | string | *Gateway merchant identity.*<br />• `required` |
|  customer_id | string | *Gateway customer identity; bloom-filter indexed.*<br />• `required` |
|  payment_method_fingerprint | string | *Synthetic payment-method fingerprint; bloom-filter indexed.*<br />• `required` |
|  payment_method_type | string | *Payment method category.*<br />• `required` |
|  device_fingerprint | string | *Observed synthetic device fingerprint.* |
|  amount_minor | integer | *Checkout total in minor units.*<br />• `required` |
|  currency | string | *Checkout currency.*<br />• `required` |
|  started_at | timestamp | *AttemptStarted occurrence time.*<br />• `required` |
|  attempted_at | timestamp | *Client attempt time from the signals.* |
|  risk_outcome | string | *RISK_OUTCOME_APPROVE or RISK_OUTCOME_DECLINE when a decision exists.* |
|  risk_score | number | *Optional model risk score; minmax indexed.* |
|  policy_version | string | *Evaluated policy version.* |
|  model_version | string | *Evaluated model version when a model was used.* |
|  outcome | string | *succeeded, declined, unknown or pending; set indexed.*<br />• `required` |
|  decline_source | string | *DECLINE_SOURCE_RISK or DECLINE_SOURCE_PROCESSOR when declined.* |
|  outcome_at | timestamp | *Time of the terminal or unknown outcome.* |
|  payment_status | string | *open, succeeded or expired for the whole payment.*<br />• `required` |
|  truth_is_fraud | boolean | *is_fraud of the earliest linked simulation-truth label.* |
|  truth_scenario | string | *Scenario of that label; synthetic oracle, never a feature.* |
|  chargeback_available_at | timestamp | *Earliest availability of a linked chargeback.* |

## SLA Properties
