## Purpose

Expose the gateway's HTTP/JSON checkout API and hosted checkout page at the Go edge, authenticate configured integrations, and translate requests into internal gRPC commands while keeping personal data out of internal services.

## Requirements

### Requirement: Public routes

The edge SHALL expose `POST /v1/payments`, `GET /v1/payments/{payment_id}` and, for synthetic integrations only, `POST /v1/payment-methods`, authenticated by an integration API key, and `POST /v1/checkout/sessions/{session_id}/attempts` authenticated by the session's checkout token.

#### Scenario: Complete public routes contract

- **WHEN** the `Public routes` capability is configured or exercised
- **THEN** The edge SHALL expose `POST /v1/payments`, `GET /v1/payments/{payment_id}` and, for synthetic integrations only, `POST /v1/payment-methods`, authenticated by an integration API key, and `POST /v1/checkout/sessions/{session_id}/attempts` authenticated by the session's checkout token. Every mutating route SHALL require an `Idempotency-Key` header. Request and response bodies SHALL be JSON with documented field names. These routes SHALL be reachable only in-cluster; only the hosted checkout paths SHALL be publicly routed.

#### Scenario: Missing idempotency key

- **WHEN** a payment creation or attempt is submitted without `Idempotency-Key`
- **THEN** the edge returns 400 without calling Payments

### Requirement: Payment creation input

Payment creation SHALL accept the full checkout snapshot in one request: order reference, external seller with its account creation time, external buyer with its account creation time, amount and currency, line items with product, category, condition, quantity and unit price, shipping address and return URL. No separate merchant or customer provisioning call SHALL be required. The edge SHALL validate that the return URL is absolute HTTPS and that items sum to the total.

#### Scenario: Marketplace-shaped request

- **WHEN** a request matching the documented shape is submitted for a seller never seen before
- **THEN** the edge returns 201 with payment ID, session ID, checkout token, hosted checkout URL and expiry, and the payment's merchant and customer IDs are the UUIDv5 values from the identity vectors

### Requirement: Configured integrations

Edge SHALL load integrations from configuration: integration ID, API key hash, synthetic flag, fingerprint key selection and, optionally, a webhook URL with its signing secret and subscribed event types.

#### Scenario: Complete configured integrations contract

- **WHEN** the `Configured integrations` capability is configured or exercised
- **THEN** Edge SHALL load integrations from configuration: integration ID, API key hash, synthetic flag, fingerprint key selection and, optionally, a webhook URL with its signing secret and subscribed event types. Keys SHALL be stored only as hashes. Unknown keys SHALL be rejected. Integration identity SHALL come only from the credential, never from caller-supplied identifiers. Marketplace callers reach Edge over the mesh because their namespace is listed in Edge's `callerNamespaces`; the API key SHALL remain required because mesh identity identifies only the caller's namespace, not the integration. Only synthetic integrations SHALL supply simulated client context or use the payment-method route. A synthetic integration's ID SHALL be operator-chosen so live traffic continues a historical dataset.

#### Scenario: Caller claims another integration

- **WHEN** a request authenticated as integration A names integration B's identifiers
- **THEN** it acts only within integration A

#### Scenario: Real integration sends simulated context

- **WHEN** a real integration submits simulated client context or calls the payment-method route
- **THEN** the request is rejected

### Requirement: Personal data minimization

The edge SHALL keep shipping country, region and postal code, replace the recipient name, street lines and city with a keyed address fingerprint over the normalized full address, drop buyer email, and replace the client IP with a keyed network fingerprint, before calling internal services.

#### Scenario: Complete personal data minimization contract

- **WHEN** the `Personal data minimization` capability is configured or exercised
- **THEN** The edge SHALL keep shipping country, region and postal code, replace the recipient name, street lines and city with a keyed address fingerprint over the normalized full address, drop buyer email, and replace the client IP with a keyed network fingerprint, before calling internal services. Real integrations SHALL use a secret fingerprint key. Synthetic integrations SHALL use a separate, non-secret synthetic key shared with the historical generator. The edge SHALL NOT log names, emails, street lines, raw IPs or tokens.

#### Scenario: Same address, different formatting

- **WHEN** two payments differ only in address letter case and surrounding whitespace
- **THEN** their address fingerprints are equal

#### Scenario: Synthetic fingerprint vectors

- **WHEN** Edge fingerprints the address/network cases and obtains the vault's method fingerprints for the shared synthetic identity vectors
- **THEN** they match exactly the fingerprints the vectors list

#### Scenario: Same address under real and synthetic integrations

- **WHEN** a real and a synthetic integration submit the same address
- **THEN** their address fingerprints differ

### Requirement: Hosted checkout page

Edge SHALL serve a hosted checkout page at `/checkout/{session_id}` authorized by the session's checkout token, showing the payment's amount and merchant and a card form.

#### Scenario: Complete hosted checkout page contract

- **WHEN** the `Hosted checkout page` capability is configured or exercised
- **THEN** Edge SHALL serve a hosted checkout page at `/checkout/{session_id}` authorized by the session's checkout token, showing the payment's amount and merchant and a card form. It SHALL move the token from the URL into a secure, HTTP-only, session-scoped cookie, bind form submissions to the session against cross-site requests, and give each submission a stable idempotency key. The page's content security policy SHALL let the form post only to itself while allowing the browser to follow the success redirect to the payment's return origin. On submit it SHALL tokenize the card number through the vault, discard it without storing or logging it, and start an attempt with the returned token. A success SHALL redirect to the payment's return URL, a decline SHALL show the reason and allow another attempt while the payment is open, and a pending outcome SHALL show a status page. Card numbers outside the vault's test-card allow-list SHALL be rejected without starting an attempt.

#### Scenario: Decline then retry

- **WHEN** a buyer submits an insufficient-funds test card and then an approving test card on the same page
- **THEN** the first attempt is declined, the payment stays open, and the second attempt succeeds and redirects to the return URL

#### Scenario: Card outside the allow-list

- **WHEN** a buyer submits a card number that is not an allow-listed test card
- **THEN** the page reports an invalid card, no attempt starts, and the number appears in no log, event or store

#### Scenario: Double submit

- **WHEN** the same form submission is posted twice
- **THEN** one attempt exists and both responses show its result

#### Scenario: Foreign origin

- **WHEN** a submission presents an origin other than the serving host or the configured public origin
- **THEN** Edge rejects it without tokenizing the card or starting an attempt

#### Scenario: Opaque origin

- **WHEN** a submission presents an opaque `Origin: null` with the session cookie and the page's CSRF token
- **THEN** Edge treats the header as absent, accepts the submission through the session and CSRF binding, and still rejects the same request without that token

### Requirement: Client context

Attempt requests SHALL accept device fingerprint and user-agent family. For synthetic integrations only, they SHALL also accept simulated IP country, IP network and client attempt time. A client attempt time SHALL be RFC 3339 and SHALL NOT be after server time; no lower bound applies. Without it, the attempt time SHALL be server time. Hosted page attempts SHALL take the IP network from the trusted ingress gateway's forwarded client address, user-agent family from the request and the device fingerprint from page script. For real integrations, IP country SHALL be absent until a geolocation source exists.

#### Scenario: Synthetic attempt with simulated geography

- **WHEN** a synthetic integration submits an attempt with a simulated IP country
- **THEN** the recorded attempt signals carry that country

#### Scenario: Store-and-forward attempt time

- **WHEN** a synthetic integration submits an attempt with a client attempt time ten minutes in the past
- **THEN** the recorded attempt signals carry that time while event recording time remains server time

#### Scenario: Future or non-synthetic attempt time

- **WHEN** the client attempt time is after server time, or the integration is not synthetic
- **THEN** the request is rejected without recording an attempt

### Requirement: Synthetic payment methods

Attempt requests SHALL accept only opaque, integration-scoped tokens issued by the isolated vault simulator in this slice.

#### Scenario: Complete synthetic payment methods contract

- **WHEN** the `Synthetic payment methods` capability is configured or exercised
- **THEN** Attempt requests SHALL accept only opaque, integration-scoped tokens issued by the isolated vault simulator in this slice. Tokens SHALL come from either the hosted page or the synthetic-only payment-method route, which SHALL accept only an allow-listed test card number, tokenize it at the vault and return the opaque token; the number SHALL NOT be stored, logged, traced or passed beyond the vault. After authorizing checkout access, Edge SHALL obtain method type and stable fingerprint from the vault metadata API without learning processing behavior. Unknown, inactive or foreign tokens SHALL be rejected before starting an attempt. Every other JSON route SHALL reject card fields. Real card collection SHALL require a separate production boundary.

#### Scenario: Raw card submission

- **WHEN** an attempt includes a card-number or card-security-code field
- **THEN** the edge rejects it without calling Payments or logging those values

#### Scenario: Simulator tokenizes a test card

- **WHEN** a synthetic integration posts an allow-listed test card number to the payment-method route
- **THEN** it receives an opaque token whose method fingerprint matches the identity vectors for that card, and the number appears in no log, trace or gateway store outside the vault

### Requirement: Isolated synthetic vault

A separately deployed vault simulator SHALL own its database, migrations and credentials.

#### Scenario: Complete isolated synthetic vault contract

- **WHEN** the `Isolated synthetic vault` capability is configured or exercised
- **THEN** A separately deployed vault simulator SHALL own its database, migrations and credentials. Its Edge-only tokenization SHALL accept only allow-listed test card numbers, each mapped to a fictional card identity and a processing behavior (`ok`, `nsf`, `issuer` or `invalid`) within the caller's integration scope; issue opaque random tokens and persist token digests with authenticated-encrypted fixture payloads using a vault-only key. The allow-list SHALL include enough cards per behavior for the live simulator's fictional card population. It SHALL NOT accept other card numbers, store card numbers, or log card numbers or token values. Its metadata API SHALL return only safe method type/fingerprint on a listener reachable only by the Edge workload identity, while processing fixtures SHALL be resolved only on a separate listener reachable only by the processor workload identity, enforced by mesh authorization. Other services SHALL NOT access its tables. The vault SHALL derive fingerprints from card identity using the shared synthetic key and identity vectors, independently of token value and behavior.

#### Scenario: Stable fingerprint after token reissue

- **WHEN** the same allow-listed card is tokenized twice
- **THEN** both tokens expose the same method fingerprint and neither exposes behavior to Edge or Fraud

#### Scenario: Unauthorized resolution

- **WHEN** Edge, Payments or any other workload invokes processing-fixture resolution
- **THEN** access is denied without exposing token mappings or fixture contents

#### Scenario: Foreign integration token

- **WHEN** checkout access for integration B is used with a token issued for integration A
- **THEN** metadata lookup is rejected as an unknown resource and no attempt starts

### Requirement: Safe error mapping

The edge SHALL map invalid input to 400, authentication failure to 401, unknown or foreign resources to 404, idempotency conflicts to 409, unavailable dependencies to 503 and deadline expiry to 504, without exposing internal details. Attempt responses SHALL report `SUCCEEDED`, `DECLINED` with reason, or `PENDING`.

#### Scenario: Idempotency conflict

- **WHEN** a repeated idempotency key carries different content
- **THEN** the edge returns 409
