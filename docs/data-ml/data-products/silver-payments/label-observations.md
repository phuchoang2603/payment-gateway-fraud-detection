# urn:datacontract:fraud:silver-label-observations
## Info
*Independent simulation-truth and chargeback observations with availability times.*
- **name:** Silver label observations
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Read accepted snapshots and use labels only at or after label_available_at.

### Purpose
Independent simulation-truth and chargeback observations with availability times.

### Limitations
Missing chargebacks are unknown; scenario and future outcomes are not predecision features.

## Servers


## Schema
### label_observations
*One normalized distinct source observation per scoped attempt and canonical observation hash.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration identity.*<br />• `required` |
|  payment_id | string | *Scoped payment identity.*<br />• `required` |
|  attempt_id | string | *Scoped attempt identity.*<br />• `required` |
|  observation_id | string | *SHA-256 of UTF-8 Spark JSON with null fields included and UTC timestamps formatted yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX; struct fields are ordered integration_id, payment_id, attempt_id, is_fraud, occurred_at, label_available_at, source, scenario.*<br />• `required` |
|  is_fraud | boolean | *Truth according to the declared source.*<br />• `required` |
|  occurred_at | timestamp | *Labeled business occurrence time.*<br />• `required` |
|  label_available_at | timestamp | *Earliest time the observation may be used.*<br />• `required` |
|  source | string | *Simulation truth or chargeback source.*<br />• `required` |
|  scenario | string | *Synthetic scenario on simulation truth only and never a predecision feature.* |
|  attempt_started_at | timestamp | *Start time when linked attempt context is complete.* |
|  payment_succeeded_at | timestamp | *Success time when the linked attempt succeeded.* |
|  pending_context | boolean | *True while required attempt or success context is missing.*<br />• `required` |
|  _candidate_id | string | *Candidate identity.*<br />• `required` |

## SLA Properties
