# urn:datacontract:fraud:silver-attempts
## Info
*One row per payment attempt with its risk decision, outcome, payment status and lifecycle validity.*
- **name:** Silver payment attempts
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Join labels and features on integration_id, payment_id and attempt_id. Rows with lifecycle_valid false are violations, not usable history.

### Purpose
One row per payment attempt with its risk decision, outcome, payment status and lifecycle validity.

### Limitations
Synthetic data only. Outcome pending means the processor result has not arrived in the source; it is not a negative label.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table attempts in schema silver, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### attempts
*One row per (integration_id, payment_id, attempt_id).*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration namespace of the payment.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Attempt identity unique within the payment.*<br />• `required` |
|  merchant_id | string | *Gateway merchant identity from the AttemptStarted context.* |
|  customer_id | string | *Gateway customer identity from the AttemptStarted context.* |
|  payment_method_type | string | *Payment method category.* |
|  payment_method_fingerprint | string | *Synthetic payment-method fingerprint from the AttemptStarted signals.* |
|  device_fingerprint | string | *Observed synthetic device fingerprint.* |
|  amount_minor | integer | *Checkout total in minor units* |
|  currency | string | *Checkout currency.* |
|  started_at | timestamp | *AttemptStarted occurrence time.*<br />• `required` |
|  attempted_at | timestamp | *Client attempt time from the signals.* |
|  risk_outcome | string | *RISK_OUTCOME_APPROVE or RISK_OUTCOME_DECLINE when a decision exists.* |
|  risk_reason_codes | array | *Policy reason codes of the decision.* |
|  policy_version | string | *Evaluated policy version.* |
|  model_version | string | *Evaluated model version when a model was used.* |
|  feature_set_version | string | *Evaluated feature-set version when features were used.* |
|  risk_score | number | *Optional model risk score.* |
|  assessment_id | string | *Assessment identity; null in schema version two.* |
|  outcome | string | *succeeded*<br />• `required` |
|  decline_source | string | *DECLINE_SOURCE_RISK or DECLINE_SOURCE_PROCESSOR when declined.* |
|  outcome_at | timestamp | *Occurrence time of the terminal or unknown outcome event.* |
|  payment_status | string | *open*<br />• `required` |
|  lifecycle_valid | boolean | *False when the attempt has more than one start or terminal event*<br />• `required` |

## SLA Properties
