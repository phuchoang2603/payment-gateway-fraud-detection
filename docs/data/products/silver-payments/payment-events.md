# urn:datacontract:fraud:silver-payment-events
## Info
*One authoritative copy of each payment event for payments without conflicting deliveries.*
- **name:** Silver payment events
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Read for lifecycle detail. Use silver.attempts for attempt-level analysis. Republished exports are collapsed here; the copy kept is the one with the earliest recorded_at.

### Purpose
One authoritative copy of each payment event for payments without conflicting deliveries.

### Limitations
Synthetic data only. Payments whose deliveries conflict are excluded and listed in silver.rejected_events.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table payment_events in schema silver, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### payment_events
*One row per distinct event_id with the payments.v1.PaymentEvent business fields of the kept copy.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  event_id | string | *Opaque event identity; unique within this table.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity within integration_id.*<br />• `required` |
|  integration_id | string | *Dedicated synthetic integration namespace.*<br />• `required` |
|  aggregate_version | integer | *Authoritative transition number.*<br />• `required` |
|  occurred_at | timestamp | *Business occurrence time in UTC.*<br />• `required` |
|  recorded_at | timestamp | *Recording time of the kept copy*<br />• `required` |
|  schema_version | integer | *Event schema version*<br />• `required` |
|  correlation_id | string | *Correlation identity shared across one payment history.*<br />• `required` |
|  event_hash | string | *SHA-256 over every business field except recorded_at and provenance.*<br />• `required` |
|  payment_created | object | *Checkout snapshot at creation.* |
|  attempt_started | object | *Attempt start with signals and context.* |
|  risk_decision_recorded | object | *Risk assessment for the active attempt.* |
|  processor_operation_requested | object | *Processor request after approval.* |
|  processor_outcome_unknown | object | *Pending processor result.* |
|  attempt_declined | object | *Risk or processor decline.* |
|  payment_succeeded | object | *Terminal success.* |
|  payment_expired | object | *Terminal expiry.* |
|  payload_case | string | *Terminal expiry.* |
|  _source | string | *Source of the kept copy*<br />• `required` |

## SLA Properties
