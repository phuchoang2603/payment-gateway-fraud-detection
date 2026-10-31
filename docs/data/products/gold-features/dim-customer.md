# urn:datacontract:fraud:gold-dim-customer
## Info
*SCD2 versions of each customer's observed profile, opened at the attempt where a tracked attribute changes.*
- **name:** Gold customer dimension
- **version:** 1.0.0
- **status:** active
- **team:** Fraud data platform

## Terms of Use
### Usage
Join on integration_id and customer_id with valid_from_ts <= t and (valid_to_ts is null or t < valid_to_ts). The current version has valid_to_ts null and is_current true.

### Purpose
SCD2 versions of each customer's observed profile, opened at the attempt where a tracked attribute changes.

### Limitations
Synthetic data only. The profile is observed from attempt_started contexts, so a change is visible from the first attempt that carries it. Versions are rebuilt from Silver on every run.

## Servers
| Name | Type | Attributes |
| ---- | ---- | ---------- |
| clickhouse | clickhouse | *Iceberg table dim_customer in schema gold, read through the ClickHouse lakehouse database. The rule placeholder {object} stands for the fully qualified table.*<br />• **environment:** dev<br />• **database:** lakehouse<br />• **host:** clickhouse.data.svc.cluster.local<br />• **port:** 8123 |

## Schema
### dim_customer
*One row per customer profile version; valid_to_ts is exclusive.*

| Field | Type | Attributes |
| ----- | ---- | ---------- |
|  customer_sk | string | *SHA-256 of integration_id, customer_id and valid_from_ts.*<br />• `required` |
|  integration_id | string | *Integration namespace of the customer.*<br />• `required` |
|  customer_id | string | *Gateway customer identity.*<br />• `required` |
|  version_number | integer | *Version position for the customer, from 1.*<br />• `required` |
|  external_buyer_id | string | *Merchant-side buyer identity from the attempt context.* |
|  customer_account_created_at | timestamp | *Account creation time from the checkout snapshot.* |
|  shipping_country | string | *Shipping country of the checkout.* |
|  shipping_region | string | *Shipping region of the checkout.* |
|  shipping_postal_code | string | *Shipping postal code of the checkout.* |
|  shipping_address_fingerprint | string | *Synthetic fingerprint of the shipping address.* |
|  familiar_device_fingerprints | array | *Sorted devices the customer used on at least two attempts in the 90 days up to and including the version's attempt.* |
|  profile_hash | string | *SHA-256 over the JSON of every tracked attribute.*<br />• `required` |
|  valid_from_ts | timestamp | *started_at of the attempt that opened the version.*<br />• `required` |
|  valid_to_ts | timestamp | *valid_from_ts of the next version; null for the current version.* |
|  is_current | boolean | *True only for the version with valid_to_ts null.*<br />• `required` |
|  source_payment_id | string | *Payment of the attempt that opened the version.*<br />• `required` |
|  source_attempt_id | string | *Attempt that opened the version.*<br />• `required` |

## SLA Properties
