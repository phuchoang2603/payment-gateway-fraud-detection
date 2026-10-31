# urn:datacontract:fraud:gold-label-attempt
## Info
*The earliest linked label of each attempt per label source, keyed for point-in-time joins with the feature tables.*
- **name:** Gold attempt labels
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Filter label_available_at <= the training cutoff, then use the rows as the Feast entity dataframe on the customer or payment-method key and event_timestamp, or join feat_attempt_rt on the attempt key. Simulation truth and chargebacks are separate rows; a missing chargeback is not a negative.

### Purpose
The earliest linked label of each attempt per label source, keyed for point-in-time joins with the feature tables.

### Limitations
Synthetic data only. Simulation truth is an oracle that production would not have; chargebacks arrive 7 to 21 days after success.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table label_attempt in schema gold, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### label_attempt
*One row per (integration_id, payment_id, attempt_id, label_source).*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration namespace of the payment.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Attempt identity within the payment.*<br />• `required` |
|  customer_id | string | *Customer of the attempt, the customer feature join key.*<br />• `required` |
|  payment_method_fingerprint | string | *Payment method of the attempt, the payment-method feature join key.*<br />• `required` |
|  event_timestamp | timestamp | *The attempt's started_at, when it was scored: the point-in-time join time for every feature table.*<br />• `required` |
|  label | boolean | *is_fraud of the earliest observation from this source.*<br />• `required` |
|  label_source | string | *LABEL_SOURCE_SIMULATION_TRUTH or LABEL_SOURCE_CHARGEBACK.*<br />• `required` |
|  label_available_at | timestamp | *Earliest time the label may be used.*<br />• `required` |
|  occurred_at | timestamp | *Time of the labeled business occurrence.*<br />• `required` |

## SLA Properties
