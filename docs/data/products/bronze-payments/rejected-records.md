# urn:datacontract:fraud:bronze-rejected-records
## Info
*Kafka records that could not be decoded as their protobuf message, kept with their raw bytes so they can be replayed or investigated.*
- **name:** Bronze rejected Kafka records
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Count rejected records per run and per topic. A record is rejected once; its offset is committed with the accepted rows of its run.

### Purpose
Kafka records that could not be decoded as their protobuf message, kept with their raw bytes so they can be replayed or investigated.

### Limitations
Historical files are never rejected here; an unreadable historical file fails its Bronze run.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| starrocks | mysql | *Iceberg table rejected_records in schema bronze, read through the StarRocks fraud_lakehouse catalog. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** bronze<br />• **host:** kube-starrocks-fe-service.starrocks.svc.cluster.local<br />• **port:** 9030 |

## Schema
### rejected_records
*One row per Kafka record that failed protobuf decoding.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  _kafka_topic | string | *Topic the record was read from.*<br />• `required` |
|  _kafka_partition | integer | *Kafka partition.*<br />• `required` |
|  _kafka_offset | integer | *Kafka offset.*<br />• `required` |
|  _kafka_timestamp | timestamp | *Kafka record timestamp.* |
|  record_key | string | *Raw Kafka key bytes.* |
|  record_value | string | *Raw Kafka value bytes.* |
|  error | string | *Decoding failure reason.*<br />• `required` |
|  _ingested_at | timestamp | *Time the Bronze run wrote the row.*<br />• `required` |
|  _run_id | string | *Bronze run that wrote the row.*<br />• `required` |

## SLA Properties
