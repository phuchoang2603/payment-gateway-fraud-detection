# urn:datacontract:fraud:bronze-payment-events
## Info
*Complete synthetic payment lifecycle histories for reproducible pipeline development.*
- **name:** Bronze payment-events
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Validate source datasets before publication and ingestion. Deduplicate identical event deliveries before interpreting lifecycle transitions.

### Purpose
Complete synthetic payment lifecycle histories for reproducible pipeline development.

### Limitations
Synthetic data only. Histories are complete across all date partitions. Empty configured schedules are valid.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| bronze | custom | *REST catalog with independently configured signing and S3FileIO credentials. Read accepted snapshot pairs, never unvalidated latest state.*<br />• **environment:** prod<br />• **database:** fraud-lakehouse-prod<br />• **host:** https://minio.home.phuchoang.sbs/iceberg/v1<br />• **schema_:** bronze |

## Schema
### payment_events
*One authoritative transition per event_id; repeated identical deliveries are permitted. Histories are complete across all date partitions.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  event_id | string | *Opaque event identity. Repeated deliveries retain this identity and identical content.*<br />• `required` |
|  payment_id | string | *Payment aggregate identity within integration_id.*<br />• `required` |
|  aggregate_version | integer | *Consecutive authoritative transition number starting at one; delivery duplicates do not increment it.*<br />• `required` |
|  integration_id | string | *Dedicated synthetic integration namespace; never share it with a real integration.*<br />• `required` |
|  occurred_at | timestamp | *Business occurrence time in UTC at microsecond precision.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
|  recorded_at | timestamp | *Source recording time, at or after occurred_at; not ingestion time.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
|  schema_version | integer | *Physical event schema. Version one omits user_agent_family; version two permits it.*<br />• `required` |
|  correlation_id | string | *Correlation identity shared across one payment history.*<br />• `required` |
|  payment_created | object | *Initial immutable checkout snapshot. Exactly one lifecycle payload is present per event.* |
| &#x2007;&#x21b3; merchant | object | *Integration-scoped merchant references.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; merchant_id | string | *Gateway merchant identity.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; external_seller_id | string | *Integration seller identity.*<br />• `required` |
| &#x2007;&#x21b3; customer | object | *Integration-scoped customer references.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; customer_id | string | *Gateway customer identity.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; external_buyer_id | string | *Integration buyer identity.*<br />• `required` |
| &#x2007;&#x21b3; snapshot | object | *Checkout facts captured at creation; raw personal and payment credentials are excluded.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; external_order_id | string | *Integration order reference.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; total | object | *Total in integer minor units; must equal the item sum.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; minor_units | integer | *Nonnegative integer amount in currency minor units.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; currency | string | *Three-letter uppercase currency code shared by all items.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; items | array | *Nonempty checkout line items.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; items | object | *No description.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; product_id | string | *Product reference.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; category | string | *Refurbished electronics product type, such as phone, laptop or accessory.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; condition | string | *Refurbished condition grade: excellent, good or fair.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; quantity | integer | *Positive item quantity.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; unit_price | object | *Price for one item in checkout currency.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; minor_units | integer | *Nonnegative integer amount in currency minor units.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x2007;&#x2007;&#x21b3; currency | string | *Three-letter uppercase currency code shared by all items.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; merchant_account_created_at | timestamp | *Merchant account creation time.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
| &#x2007;&#x2007;&#x21b3; customer_account_created_at | timestamp | *Customer account creation time.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
| &#x2007;&#x2007;&#x21b3; shipping | object | *Coarse shipping geography and synthetic address fingerprint.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; country | string | *Shipping country code.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; region | string | *Shipping region.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; postal_code | string | *Shipping postal code.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; address_fingerprint | string | *Normalized keyed synthetic address fingerprint; no street lines.*<br />• `required` |
|  attempt_started | object | *Starts a new nonconcurrent attempt on an open payment.* |
| &#x2007;&#x21b3; attempt_id | string | *Attempt identity unique within this payment.*<br />• `required` |
| &#x2007;&#x21b3; signals | object | *Runtime observations; unavailable optional signals remain null.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; attempted_at | timestamp | *Attempt occurrence time.*<br />• **logicalTypeOptions:** {'timezone': True, 'defaultTimezone': 'UTC'}<br />• `required` |
| &#x2007;&#x2007;&#x21b3; payment_method | object | *Method category and keyed synthetic token fingerprint.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; type | string | *Payment method category.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; fingerprint | string | *Synthetic method fingerprint; never raw credentials.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; device_fingerprint | string | *Observed synthetic device fingerprint.* |
| &#x2007;&#x2007;&#x21b3; ip_country | string | *Resolved IP country if available.* |
| &#x2007;&#x2007;&#x21b3; ip_network_fingerprint | string | *Keyed network fingerprint; never raw IP.* |
| &#x2007;&#x2007;&#x21b3; user_agent_family | string | *Optional browser family introduced in physical schema two.*<br />• **customProperties:** [{'property': 'sinceSchemaVersion', 'value': 2}] |
|  risk_decision_recorded | object | *Exactly one assessment for the active attempt before any processor request.* |
| &#x2007;&#x21b3; attempt_id | string | *Active attempt identity.*<br />• `required` |
| &#x2007;&#x21b3; assessment | object | *Decision evidence. Optional model and feature versions are absent for rules-only decisions.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; outcome | string | *RISK_OUTCOME_APPROVE or RISK_OUTCOME_DECLINE.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; reason_codes | array | *Nonempty policy reason codes.*<br />• `required` |
| &#x2007;&#x2007;&#x2007;&#x21b3; items | string | *No description.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; policy_version | string | *Evaluated policy version.*<br />• `required` |
| &#x2007;&#x2007;&#x21b3; model_version | string | *Evaluated model version if a model was used.* |
| &#x2007;&#x2007;&#x21b3; feature_set_version | string | *Evaluated feature-set version if features were used.* |
| &#x2007;&#x2007;&#x21b3; risk_score | number | *Optional model risk score.* |
|  processor_operation_requested | object | *Processor authorization request after risk approval.* |
| &#x2007;&#x21b3; attempt_id | string | *Active approved attempt identity.*<br />• `required` |
| &#x2007;&#x21b3; operation_key | string | *Idempotent processor operation identity.*<br />• `required` |
|  processor_outcome_unknown | object | *Pending processor result; only processor decline or success can resolve it.* |
| &#x2007;&#x21b3; attempt_id | string | *Active requested attempt identity.*<br />• `required` |
|  attempt_declined | object | *Risk or processor decline that permits a subsequent attempt.* |
| &#x2007;&#x21b3; attempt_id | string | *Active declined attempt identity.*<br />• `required` |
| &#x2007;&#x21b3; source | string | *DECLINE_SOURCE_RISK or DECLINE_SOURCE_PROCESSOR.*<br />• `required` |
| &#x2007;&#x21b3; reason | string | *Decline reason.*<br />• `required` |
|  payment_succeeded | object | *Terminal successful processor result.* |
| &#x2007;&#x21b3; attempt_id | string | *Successful requested attempt identity.*<br />• `required` |
| &#x2007;&#x21b3; processor_reference | string | *Synthetic processor result reference.*<br />• `required` |
|  payment_expired | object | *Terminal expiry with no pending processor result.* |
| &#x2007;&#x21b3; _present | boolean | *True marker for the otherwise empty expiry payload.*<br />• `required` |
|  _source_dataset_version | string | *Immutable source dataset version, independent of contract and product versions.*<br />• `required` |
|  _source_manifest_sha256 | string | *SHA-256 of exact completed source manifest bytes.*<br />• `required` |
|  _source_file | string | *Exact manifest-selected input object URI.*<br />• `required` |
|  _ingestion_run_id | string | *Stable logical ingestion identity, reused after driver retries.*<br />• `required` |
|  _ingested_at | timestamp | *Stable UTC microsecond ingestion time, distinct from source recorded_at.*<br />• `required` |

## SLA Properties
