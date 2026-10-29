# urn:datacontract:fraud:historical-fraud-labels
## Info
*Separately delivered simulation truth and delayed chargebacks for the historical payment events.*
- **name:** Historical synthetic fraud labels
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
| validation | duckdb | *Local tables or views named fraud_labels and the referenced payment_events, prepared by the producing or ingesting job. Override the database path with DATACONTRACT_DUCKDB_DATABASE.*<br />• **environment:** local<br />• **database:** validation.duckdb |

## Schema
### fraud_labels
*Separate synthetic attempt truth and delayed chargebacks. Never join future labels into features.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Same dedicated synthetic namespace as the referenced payment.*<br />• `required` |
|  payment_id | string | *Referenced payment aggregate identity.*<br />• `required` |
|  attempt_id | string | *Referenced started attempt identity in the same integration and payment. Chargebacks reference an attempt that succeeded.*<br />• `required` |
|  is_fraud | boolean | *Fraud truth according to this source; friendly-fraud chargebacks may differ from simulation truth.*<br />• `required` |
|  occurred_at | timestamp | *Time of the labeled business occurrence.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
|  label_available_at | timestamp | *Earliest time this label may be used by training or evaluation.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
|  source | string | *LABEL_SOURCE_SIMULATION_TRUTH or LABEL_SOURCE_CHARGEBACK.*<br />• `required` |
|  scenario | string | *Synthetic generating scenario on simulation truth only; excluded from features.* |

## SLA Properties
