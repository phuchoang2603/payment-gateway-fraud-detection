# urn:datacontract:fraud:silver-labels
## Info
*One row per distinct label observation with its linkage to a payment attempt and its availability.*
- **name:** Silver fraud labels
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Join labels to silver.attempts and use a label only after label_available_at and only when linkage is linked or pending is resolved by a later run. Never use scenario as a feature.

### Purpose
One row per distinct label observation with its linkage to a payment attempt and its availability.

### Limitations
Synthetic data only. Labels with linkage violation are contract violations; the Silver checks fail while any exist.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| starrocks | mysql | *Iceberg table labels in schema silver, read through the StarRocks fraud_lakehouse catalog. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** silver<br />• **host:** kube-starrocks-fe-service.starrocks.svc.cluster.local<br />• **port:** 9030 |

## Schema
### labels
*One row per distinct label observation; identical copies collapse to one.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  observation_id | string | *SHA-256 over every business field of the observation.*<br />• `required` |
|  integration_id | string | *Integration namespace of the payment.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Referenced attempt identity.*<br />• `required` |
|  is_fraud | boolean | *Fraud truth according to this source.*<br />• `required` |
|  occurred_at | timestamp | *Time of the labeled business occurrence.*<br />• `required` |
|  label_available_at | timestamp | *Earliest time this label may be used.*<br />• `required` |
|  source | string | *LABEL_SOURCE_SIMULATION_TRUTH or LABEL_SOURCE_CHARGEBACK.*<br />• `required` |
|  scenario | string | *Generating scenario on simulation truth only.* |
|  linkage | string | *linked*<br />• `required` |
|  _source | string | *Source of the kept copy*<br />• `required` |

## SLA Properties
