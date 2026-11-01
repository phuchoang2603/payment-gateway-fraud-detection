# urn:datacontract:fraud:gold-feat-stream-payment-method
## Info
*Flink snapshot journal persisted by Feast feature writer.*
- **name:** Gold payment method stream features
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Natural-key point-in-time retrieval over configured epochs; unknown coverage remains null.

### Purpose
Flink snapshot journal persisted by Feast feature writer.

### Limitations
Synthetic data only. Event time is one microsecond after the online write time; created breaks ties.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg stream table read through the ClickHouse lakehouse database.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### feat_stream_payment_method
*One row per epoch and journal coordinate.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *No description.* |
|  payment_method_fingerprint | string | *No description.* |
|  event_timestamp | timestamp | *No description.* |
|  created | timestamp | *No description.* |
|  epoch | string | *No description.* |
|  revision | integer | *No description.* |
|  _kafka_partition | integer | *No description.* |
|  _kafka_offset | integer | *No description.* |
|  method_snapshot_attempt_count_5m | integer | *No description.* |
|  method_snapshot_attempt_count_1h | integer | *No description.* |
|  method_snapshot_attempt_count_24h | integer | *No description.* |

## SLA Properties
