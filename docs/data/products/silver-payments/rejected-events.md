# urn:datacontract:fraud:silver-rejected-events
## Info
*Every row of each payment whose deliveries conflict, with the reason, so conflicts stay visible without entering accepted tables.*
- **name:** Silver rejected payment rows
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Investigate by payment_id and reason. Conflicts from the historical source mean the published dataset is wrong and the run must not be accepted.

### Purpose
Every row of each payment whose deliveries conflict, with the reason, so conflicts stay visible without entering accepted tables.

### Limitations
Synthetic data only. A payment appears here completely or not at all.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| starrocks | mysql | *Iceberg table rejected_events in schema silver, read through the StarRocks lakehouse catalog. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** silver<br />• **host:** kube-starrocks-fe-service.starrocks.svc.cluster.local<br />• **port:** 9030 |

## Schema
### rejected_events
*One row per rejected Bronze row, carrying its content as JSON.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  rejected_table | string | *payment_events*<br />• `required` |
|  reason | string | *The conflicting event_id*<br />• `required` |
|  source | string | *historical or kafka*<br />• `required` |
|  integration_id | string | *Integration namespace of the payment.*<br />• `required` |
|  payment_id | string | *Rejected payment identity.*<br />• `required` |
|  row_json | string | *Row content as JSON.*<br />• `required` |
|  _run_id | string | *Silver run that rejected the row.*<br />• `required` |

## SLA Properties
