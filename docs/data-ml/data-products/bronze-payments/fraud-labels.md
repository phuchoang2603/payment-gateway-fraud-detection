# urn:datacontract:fraud:bronze-fraud-labels
## Info
*Separately delivered simulation truth and delayed chargebacks for the historical payment events.*
- **name:** Bronze fraud-labels
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Join labels to payment attempts only after label_available_at. Validate together with the payment events they reference.

### Purpose
Separately delivered simulation truth and delayed chargebacks for the historical payment events.

### Limitations
Synthetic data only. Scenario truth and future outcomes must never become decision inputs. Labels may only be used after label_available_at.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| bronze | custom | *REST catalog with independently configured signing and S3FileIO credentials. Read accepted snapshot pairs, never unvalidated latest state.*<br />• **environment:** prod<br />• **database:** fraud-lakehouse-prod<br />• **host:** https://minio.home.phuchoang.sbs/iceberg/v1<br />• **schema_:** bronze |

## Schema
### fraud_labels
*Separate synthetic attempt truth and delayed chargebacks. Never join future labels into features.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Same dedicated synthetic namespace as the referenced payment.*<br />• `required` |
|  payment_id | string | *Referenced payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Referenced started attempt identity.*<br />• `required` |
|  is_fraud | boolean | *Fraud truth according to this source; friendly-fraud chargebacks may differ from simulation truth.*<br />• `required` |
|  occurred_at | timestamp | *Time of the labeled business occurrence.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
|  label_available_at | timestamp | *Earliest time this label may be used by training or evaluation.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
|  source | string | *LABEL_SOURCE_SIMULATION_TRUTH or LABEL_SOURCE_CHARGEBACK.*<br />• `required` |
|  scenario | string | *Synthetic generating scenario on simulation truth only; excluded from features.* |
|  _source_dataset_version | string | *Immutable source dataset version, independent of contract and product versions.*<br />• `required` |
|  _source_manifest_sha256 | string | *SHA-256 of exact completed source manifest bytes.*<br />• `required` |
|  _source_file | string | *Exact manifest-selected input object URI.*<br />• `required` |
|  _ingestion_run_id | string | *Stable logical ingestion identity, reused after driver retries.*<br />• `required` |
|  _ingested_at | timestamp | *Stable UTC microsecond ingestion time, distinct from source recorded_at.*<br />• `required` |

## SLA Properties
