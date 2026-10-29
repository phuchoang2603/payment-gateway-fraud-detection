# urn:datacontract:fraud:bronze-fraud-labels
## Info
*Every delivered fraud label from the historical v18 files and the labels Kafka topic, with provenance.*
- **name:** Bronze fraud labels
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Keep every delivery. Silver collapses identical observations and checks each label against its attempt. Business columns are the payments.v1.FraudLabel fields.

### Purpose
Every delivered fraud label from the historical v18 files and the labels Kafka topic, with provenance.

### Limitations
Synthetic data only. Scenario is simulation truth and must never become a feature.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| starrocks | mysql | *Iceberg table fraud_labels in schema bronze, read through the StarRocks fraud_lakehouse catalog. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** bronze<br />• **host:** kube-starrocks-fe-service.starrocks.svc.cluster.local<br />• **port:** 9030 |

## Schema
### fraud_labels
*One row per delivered label observation.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Same dedicated synthetic namespace as the referenced payment.*<br />• `required` |
|  payment_id | string | *Referenced payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Referenced attempt identity.*<br />• `required` |
|  is_fraud | boolean | *Fraud truth according to this source.*<br />• `required` |
|  occurred_at | timestamp | *Time of the labeled business occurrence.*<br />• `required` |
|  label_available_at | timestamp | *Earliest time this label may be used.*<br />• `required` |
|  source | string | *LABEL_SOURCE_SIMULATION_TRUTH or LABEL_SOURCE_CHARGEBACK.*<br />• `required` |
|  scenario | string | *Synthetic generating scenario on simulation truth only.* |
|  _source | string | *historical or kafka.*<br />• `required` |
|  _dataset_version | string | *Historical dataset version; null for Kafka rows.* |
|  _source_file | string | *Historical Parquet object key; null for Kafka rows.* |
|  _kafka_topic | string | *Kafka topic; null for historical rows.* |
|  _kafka_partition | integer | *Kafka partition; null for historical rows.* |
|  _kafka_offset | integer | *Kafka offset; null for historical rows.* |
|  _kafka_timestamp | timestamp | *Kafka record timestamp; null for historical rows.* |
|  _ingested_at | timestamp | *Time the Bronze run wrote the row.*<br />• `required` |
|  _run_id | string | *Bronze run that wrote the row.*<br />• `required` |

## SLA Properties
