# urn:datacontract:fraud:silver-event-delivery-provenance
## Info
*Preserve exact source-delivery provenance for accepted Silver data.*
- **name:** Silver event delivery provenance
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
### event_delivery_provenance
*Delivery metadata and exact multiplicity for each scoped event.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  integration_id | string | *Integration identity.*<br />• `required` |
|  event_id | string | *Scoped event identity.*<br />• `required` |
|  _source_dataset_version | string | *Immutable source dataset version, independent of contract and product versions.*<br />• `required` |
|  _source_manifest_sha256 | string | *SHA-256 of exact completed source manifest bytes.*<br />• `required` |
|  _source_file | string | *Exact manifest-selected input object URI.*<br />• `required` |
|  _source_schema_version | integer | *Physical schema version of the selected source file, independent of the original event envelope.*<br />• `required` |
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
|  deliveries | integer | *Number of deliveries with this provenance.*<br />• `required` |
|  representative_deliveries | integer | *Deliveries assigned as canonical representatives.*<br />• `required` |
|  conflicting_deliveries | integer | *Deliveries with a conflicting identity.*<br />• `required` |
|  pending_context_deliveries | integer | *Representative deliveries awaiting complete context.*<br />• `required` |
|  identical_duplicate_deliveries | integer | *Extra identical deliveries.*<br />• `required` |
|  payment_id | string | *Scoped payment identity.*<br />• `required` |
|  pending_context | boolean | *Whether the payment context is incomplete.*<br />• `required` |
|  _candidate_id | string | *Candidate that last changed this row; the accepted receipt identifies the complete snapshot.*<br />• `required` |

## SLA Properties
