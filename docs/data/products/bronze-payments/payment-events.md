# urn:datacontract:fraud:bronze-payment-events
## Info
*Every delivered payment event from the historical v18 files and the payments Kafka topic, with provenance.*
- **name:** Bronze payment events
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Keep every delivery. Silver collapses identical deliveries and quarantines conflicting payments. Business columns are the payments.v1.PaymentEvent fields; nested payloads keep the protobuf names.

### Purpose
Every delivered payment event from the historical v18 files and the payments Kafka topic, with provenance.

### Limitations
Synthetic data only. Rows from both sources share one shape; _source tells them apart.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table payment_events in schema bronze, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### payment_events
*One row per delivered PaymentEvent. Repeated deliveries of one event_id are expected and kept.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  event_id | string | *Opaque event identity shared by every delivery of the event.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity within integration_id.*<br />• `required` |
|  aggregate_version | integer | *Consecutive authoritative transition number starting at one.*<br />• `required` |
|  integration_id | string | *Dedicated synthetic integration namespace.*<br />• `required` |
|  occurred_at | timestamp | *Business occurrence time in UTC.*<br />• `required` |
|  recorded_at | timestamp | *Source recording time; a republished export advances it.*<br />• `required` |
|  schema_version | integer | *Event schema version*<br />• `required` |
|  correlation_id | string | *Correlation identity shared across one payment history.*<br />• `required` |
|  payment_created | object | *payments.v1.PaymentCreated. Present only for the PaymentCreated event.* |
|  attempt_started | object | *payments.v1.AttemptStarted with signals and context.* |
|  risk_decision_recorded | object | *payments.v1.RiskDecisionRecorded with the assessment. Assessment fields assessment_id and evidence_sha256 are declared with sinceSchemaVersion three.* |
|  processor_operation_requested | object | *payments.v1.ProcessorOperationRequested.* |
|  processor_outcome_unknown | object | *payments.v1.ProcessorOutcomeUnknown.* |
|  attempt_declined | object | *payments.v1.AttemptDeclined with source and reason.* |
|  payment_succeeded | object | *payments.v1.PaymentSucceeded.* |
|  payment_expired | object | *payments.v1.PaymentExpired with the _present marker.* |
|  payload_case | string | *The protobuf oneof member the event carries or null when it carries none. Derived during ingestion from the decoded payload fields* |
|  _source | string | *historical or kafka.*<br />• `required` |
|  _dataset_version | string | *Historical dataset version such as v18; null for Kafka rows.* |
|  _source_file | string | *Historical Parquet object key; null for Kafka rows.* |
|  _kafka_topic | string | *Kafka topic; null for historical rows.* |
|  _kafka_partition | integer | *Kafka partition; null for historical rows.* |
|  _kafka_offset | integer | *Kafka offset; null for historical rows.* |
|  _kafka_timestamp | timestamp | *Kafka record timestamp; null for historical rows.* |
|  _ingested_at | timestamp | *Time the Bronze run wrote the row.*<br />• `required` |
|  _run_id | string | *Bronze run that wrote the row.*<br />• `required` |

## SLA Properties
