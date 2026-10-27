# urn:datacontract:fraud:silver-label-delivery-provenance
## Info
*Preserve exact source-delivery provenance for accepted Silver data.*
- **name:** Silver label delivery provenance
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Read only snapshots referenced by an accepted Silver receipt.

### Purpose
Preserve exact source-delivery provenance for accepted Silver data.

### Limitations
Historical duplicate deliveries in one file are represented with multiplicity.

## Servers


## Schema
### label_delivery_provenance
*Delivery metadata and exact multiplicity for each label observation.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  observation_id | string | *SHA-256 identity of the normalized label observation.*<br />• `required` |
|  _source_dataset_version | string | *Immutable source dataset version, independent of contract and product versions.*<br />• `required` |
|  _source_manifest_sha256 | string | *SHA-256 of exact completed source manifest bytes.*<br />• `required` |
|  _source_file | string | *Exact manifest-selected input object URI.*<br />• `required` |
|  _source_schema_version | integer | *Physical schema version of the selected label source file.*<br />• `required` |
|  _ingestion_run_id | string | *Stable logical ingestion identity, reused after driver retries.*<br />• `required` |
|  _ingested_at | timestamp | *Stable UTC microsecond ingestion time, distinct from source recorded_at.*<br />• `required` |
|  _source_epoch | string | *Immutable landing epoch; null for historical exports.* |
|  _source_batch | string | *Exact completion-manifest key for this source file; null for historical exports.* |
|  _transport | object | *Original Kafka envelope, including ordered headers; null for historical sources.* |
| &#x2007;&#x21b3; topic | string | *No description.*<br />• `required` |
| &#x2007;&#x21b3; partition | integer | *No description.*<br />• `required` |
| &#x2007;&#x21b3; offset | integer | *No description.*<br />• `required` |
| &#x2007;&#x21b3; timestamp | timestamp | *No description.* |
| &#x2007;&#x21b3; key | string | *No description.* |
| &#x2007;&#x21b3; value | string | *No description.*<br />• `required` |
| &#x2007;&#x21b3; headers | array | *No description.* |
| &#x2007;&#x2007;&#x21b3; items | object | *No description.* |
| &#x2007;&#x2007;&#x2007;&#x21b3; key | string | *No description.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; value | string | *No description.* |
|  deliveries | integer | *Number of identical observation deliveries with this provenance.*<br />• `required` |
|  _candidate_id | string | *Candidate identity.*<br />• `required` |

## SLA Properties
