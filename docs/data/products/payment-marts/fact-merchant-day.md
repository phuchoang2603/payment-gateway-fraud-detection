# urn:datacontract:fraud:marts-fact-merchant-day
## Info
*One row per integration, merchant and UTC start day, with attempt, outcome, amount and label counts and their rates.*
- **name:** Merchant day fact
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Use for dashboards and network-wide daily KPIs. Each rate states its denominator and is null when the denominator is zero. Treat chargeback_rate as provisional while chargeback_window_complete is false.

### Purpose
One row per integration, merchant and UTC start day, with attempt, outcome, amount and label counts and their rates.

### Limitations
Synthetic data only. truth_* columns aggregate a synthetic oracle.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Native MergeTree table in the ClickHouse marts database. The rule placeholder {object} stands for the table and {table:name} for another table of the same build.*<br />• **environment:** dev<br />• **database:** marts<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### fact_merchant_day
*One row per (integration_id, merchant_id, day); ordered by (integration_id, day, merchant_id).*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration namespace.*<br />• `required` |
|  merchant_id | string | *Gateway merchant identity.*<br />• `required` |
|  day | date | *UTC day of started_at.*<br />• `required` |
|  attempts | integer | *Attempts started on the day.*<br />• `required` |
|  decided | integer | *Attempts that succeeded or were declined.*<br />• `required` |
|  succeeded | integer | *Attempts that succeeded.*<br />• `required` |
|  risk_declined | integer | *Attempts declined by the risk policy.*<br />• `required` |
|  processor_declined | integer | *Attempts declined for any other source.*<br />• `required` |
|  amount_attempted_minor | integer | *Sum of amount_minor over attempts.*<br />• `required` |
|  amount_succeeded_minor | integer | *Sum of amount_minor over succeeded attempts.*<br />• `required` |
|  distinct_customers | integer | *Distinct customers with an attempt on the day.*<br />• `required` |
|  truth_labelled | integer | *Attempts with a simulation-truth label.*<br />• `required` |
|  truth_fraud | integer | *Attempts whose simulation-truth label is fraud.*<br />• `required` |
|  chargebacks | integer | *Attempts with a linked chargeback.*<br />• `required` |
|  approval_rate | number | *succeeded / decided; null when decided is 0.* |
|  risk_decline_rate | number | *risk_declined / decided; null when decided is 0.* |
|  truth_fraud_rate | number | *truth_fraud / truth_labelled; null when truth_labelled is 0.* |
|  chargeback_rate | number | *chargebacks / succeeded; null when succeeded is 0.* |
|  chargeback_window_complete | boolean | *True when day plus 21 days, the longest chargeback delay, is at or before the Silver commit day.*<br />• `required` |

## SLA Properties
