## Context

After `define-payment-and-decision-contracts`, the repository has `payments.v1` types and events, a rules-only `fraud.v2` service, and a health-only edge. The shared platform (talos-proxmox) owns CloudNativePG, External Secrets (Doppler), Strimzi and ClickStack/OTel. The marketplace uses goose migrations, sqlc and pgx, and this repository adopts the same tooling. This change makes committed events durable, publishes them to an internal Kafka topic in commit order and delivers signed outcome webhooks from it; the next change (live simulator, transport faults, Flink) consumes the same topic. The marketplace currently uses its own `services/payment` and a pay/fail/expire hosted-payment simulator; its `integrate-payment-gateway-checkout` change replaces both with this gateway. The platform runs the OT-CONTAINER-KIT Redis operator (Valkey images, `redis.redis.opstreelabs.in/v1beta2`). This change reworks the protobuf contracts from scratch to match the historical-payments data contract: no `causation_id` or merchant category, labels and `RiskAssessment` in `payments.v1`, the decision RPC as `fraud.v1`, and new `payments.v1.PaymentService`, `vault.v1` and `processor.v1`.

## Goals / Non-Goals

**Goals:**

- An API-driven checkout path that produces authoritative, lifecycle-valid events for synthetic traffic, and a hosted checkout page plus signed webhooks so the marketplace can integrate as a real client.
- One idempotency rule on every hop: a caller-supplied key, checked first in Redis and arbitrated by a Postgres unique constraint committed with the effect.
- Correct idempotency, fail-closed risk handling and unknown-outcome recovery from the first version.
- As few deployables as the boundaries need: Edge (public and integration edge), Payments (payment core), notifier (outbound delivery), vault and processor simulators.

**Non-Goals:**

- Live simulator, transport faults and the faulty and labels topics (`simulate-live-traffic-with-transport-faults`).
- Integration management APIs, key rotation flows, Kafka authentication (#63) and per-integration topics.
- Marketplace-side changes (`refurbished-marketplace` `integrate-payment-gateway-checkout`), public exposure of the JSON API, rate limits, and a payment cancel command (`payments.v1` has no cancel event; abandoned payments end by expiry).
- Financial journal and postings, capture/refund, database sharding and Redis-backed distributed rate limits. Automated reconciliation and bank settlement-file ingestion are excluded from project scope, rather than assigned a later worker.
- Dashboards.
- Per-path (L7) mesh policy for marketplace-to-Edge and notifier-to-web calls. In ambient it needs a waypoint, so it is a possible later increment.
- Real bank connectivity, ISO 8583 wire exchange, raw-card vault/CDE, issuer-authentication challenges. These are adopted follow-up directions, not implemented by the synthetic sale interface in this slice.

## Decisions

### Request flow

```
marketplace checkout / live simulator            browser (hosted page, public HTTPRoute)
      │ in-cluster JSON (integration key, Idempotency-Key)   │ /checkout/{session} (checkout token)
      ▼                                                      ▼
   Edge  (integrations from config; fingerprints; hosted page)
      │ ──gRPC──▶ vault.v1 TokenizeCard (hosted page, synthetic payment-method route) · DescribeMethod
      └──gRPC──▶ Payments ──gRPC──▶ fraud.v1 Decide      (deadline 1 s, fail closed)
                    │     ──gRPC──▶ processor.v1 Sale    (deadline 3 s → unknown)
                    │ ──▶ Redis (idempotency claim + result, TTL)
                    ▼
                 Postgres: payment/attempt state, append-only ledger, idempotency
                    ▲
          recovery + expiry workers (same process, leader via advisory lock)

payment_events ──WAL──▶ Debezium ──▶ payments.events.v1 (internal)
                                        ├──▶ notifier ──signed POST──▶ integration webhook URL
                                        └──▶ fault injector / streaming jobs (later changes)
processor ──restricted gRPC──▶ vault.v1 ResolveForProcessing
```

Edge authenticates the integration from configuration, validates an opaque method token through the vault metadata API and passes gateway inputs and a stable method fingerprint to Payments. Payments derives merchant and customer IDs, calls fraud and the processor, and records every transition. The processor resolves fictional fixtures through its restricted vault API. The notifier is the only component that talks to integrations asynchronously.

### Versioned state with an append-only ledger

Authoritative state is the `payments` and `attempts` rows, each with a `version`. A transition runs in one transaction: `UPDATE … SET …, version = version + 1 WHERE id = $id AND version = $expected`, insert exactly one ledger event, and write the idempotency result. Zero updated rows means a stale writer, which reloads state or fails. Commands read current rows, never the ledger.

The `payment_events` ledger stores `global_position bigserial`, `event_id uuid unique`, `payment_id`, `aggregate_version` (the payment's new row version, unique with `payment_id`), `event_type`, `payload bytea` (serialized `PaymentEvent`), `occurred_at` and `recorded_at`. Ledger rows are immutable: the runtime role `payments_app` has only `SELECT` and `INSERT` on it, and a trigger rejects `UPDATE`, `DELETE` and `TRUNCATE` for every role, so a mistaken migration fails too. The `payments` owner role runs migrations only. `aggregate_version` keeps its consecutive per-payment meaning.

The ledger is the audit history and commit-log boundary. Debezium reads committed inserts from the WAL and maps the stored payload to Kafka (see Ledger publication to Kafka), so no separate outbox table or publisher code exists in Payments. `bigserial` positions can commit out of order across concurrent transactions; consumers rely on WAL commit order and per-payment versions, not on positions.

Alternatives: event sourcing, rehydrating state from the stream with rebuildable projections. Rejected because the slice needs no temporal queries or projection rebuilds, and state rows keep commands, recovery queries and expiry scans as plain indexed reads. EventStoreDB or Kafka as the store were rejected because they add infrastructure.

Idempotency scope, request-content binding and in-flight/final results survive process restarts in Postgres. Database uniqueness and transaction semantics arbitrate concurrent requests. A Payments-owned private `processor_requests` record stores the request hash and encrypted opaque method token needed after a crash; persist it atomically with `ProcessorOperationRequested`. Events contain the stable operation key, never the token. Remove the encrypted token once the terminal outcome commits, retaining the non-sensitive request binding. Replay never reactivates this record.

### Hosted checkout page and test-card tokenization

Edge serves `GET /checkout/{session_id}` with the checkout token in the redirect URL, then moves it into a `Secure`, `HttpOnly`, session-path cookie and redirects to the clean URL. The server-rendered page shows the immutable amount and items from `GetPayment` and a card form. On submit, Edge calls vault `TokenizeCard` with the card number and the session's integration, discards the number, and starts the attempt with the returned token and a per-submit idempotency key embedded in the form. `SUCCEEDED` redirects the buyer to the validated return URL; `DECLINED` re-renders the form with the reason; `PENDING` shows a waiting page that polls status. The buyer can retry on the page while the payment is open. Submissions are bound to the session by comparing `Origin` with the configured public origin or the serving host, treating an opaque `Origin: null` as absent, and requiring the page's CSRF token, derived from the session and idempotency key, alongside the `SameSite` cookie. The page's content security policy lists the merchant return origin in `form-action`: browsers check the redirect target of a form submission against that directive, so leaving it out charges the card and then leaves the buyer on the payment page.

The live simulator uses the same tokenization through `POST /v1/payment-methods`, allowed only for synthetic integrations. It accepts one allow-listed test card number and returns the opaque token. Request bodies of this route and the page form are excluded from access logs and traces. This replaces a separate vault CLI, so the vault has one tokenizing caller (Edge).

Edge therefore handles fictional card numbers in memory. That is acceptable only because the allow-list rejects every other number; it is a synthetic boundary, not a cardholder-data environment, and real card entry would need the separate production scope already excluded. Alternative considered: page script posting cards to a public vault route, as hosted fields do. Rejected because it publicly exposes the vault for no gain with synthetic cards.

For page traffic, Edge takes the client IP from the trusted ingress gateway's forwarded header, user-agent family from the request, and a device fingerprint computed by page script; these feed the same fingerprints as the JSON API. Only `/checkout/*` is publicly routed. The JSON API stays ClusterIP-only and integrations call it in-cluster.

### Ledger publication to Kafka

`infra/charts/kafka` copies refurbished-marketplace's chart and Strimzi settings, already tuned for these clusters: one dual-role KRaft node pool (`payment-gateway-kafka`), internal `plain` (9092) and `tls` (9093) listeners, `auto.create.topics.enable: false`, replication factor 1, `KafkaTopic` resources from an `entities.<name>.topics` list in values, and `KafkaConnect` (`payment-gateway-connect`) with `use-connector-resources` and the `KubernetesSecretConfigProvider`. A `Role` lets the Connect service account `get` only the dedicated Payments CDC Secret; that database principal has replication and `SELECT` on `payment_events` only. It deploys in `payment-gateway`, which has no Gateway policies intercepting Kafka TLS. The Connect image (`infra/docker/connect-debezium.Dockerfile`) uses the marketplace's Strimzi base and Debezium Postgres 3.7.0.Final.

The connector uses the marketplace's Postgres outbox template on the ledger: `pgoutput`, slot `debezium_payments_slot`, `publication.autocreate.mode: filtered`, `table.include.list: public.payment_events`, EventRouter with `table.field.event.id: event_id`, `table.field.event.key: payment_id`, `table.field.event.payload: payload` and the constant topic `payments.events.v1` (3 partitions, 7-day retention), `StringConverter` keys and `ByteArrayConverter` values so stored protobuf bytes pass unchanged. The WAL gives commit order and never contains rolled-back appends; per-payment order follows from the expected-version check; Connect's source offsets give at-least-once resumption.

The topic is internal: the notifier and streaming jobs consume it, integrations do not. `AttemptDeclined` is informational: the payment stays open and the buyer can retry. `PaymentSucceeded` and `PaymentExpired` are final, and because expiry waits for unresolved processor outcomes, no success follows an expiry. The default payment expiry is 15 minutes.

Alternatives considered: a polling publisher inside Payments — rejected because it adds Kafka code, a checkpoint table and leader election while the marketplace already runs the Debezium path on these clusters; letting integrations consume the topic directly — rejected because it couples them to the gateway's broker, schema and unauthenticated listener and shows them other integrations' events.

### Signed webhook notifier

`notifier` is a small Go service with its own database. It consumes `payments.events.v1` in group `gateway-notifier`. For each event whose integration has a webhook URL and subscribes to its type (initially `attempt.declined`, `payment.succeeded`, `payment.expired`), it inserts a `webhook_deliveries` row keyed by `event_id` with `ON CONFLICT DO NOTHING`, then commits the offset; re-consumption after a crash or rebalance therefore creates nothing new. A worker under an advisory lock claims due rows with `FOR UPDATE SKIP LOCKED` and POSTs JSON `{event_id, type, payment_id, order_reference, attempt_id?, status, reason?, occurred_at}` with `Idempotency-Key: <event_id>` and `Gateway-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, t "." body)>`. A 2xx marks it delivered; other results retry with exponential backoff and full jitter (initially 12 attempts over about 24 hours), then mark the row failed for inspection. Receivers deduplicate `event_id` and reject timestamps outside 5 minutes.

The order reference comes from `PaymentCreated.snapshot.external_order_id`; the notifier keeps a small `payment_refs` table from the created event so later events can carry it. Webhook URLs follow one rule: in-cluster service addresses (`*.svc.cluster.local`, `*.svc`) are plaintext HTTP carried over the mesh, and every external target must be HTTPS. The marketplace's receiver, `http://web.ecommerce.svc.cluster.local:8080/webhooks/payment-gateway`, is in-cluster. The signature authenticates the event payload, not only the connection.

Alternatives considered: delivery inside Payments — rejected because the payment core would make outbound HTTP calls and own delivery backlog; inside Edge — rejected because it adds asynchronous state to the public request path; a webhook outbox written in the Payments outcome transaction — not needed once the ledger is on Kafka, and it would add a mutable table to the payment database.

### Idempotency: Redis first, Postgres final

Every mutating call carries a key: payment creation uses the integration's `Idempotency-Key` (the marketplace sends its order ID), attempts use the per-submit key, and webhook delivery uses `event_id`. Payments handles creation and attempt keys the same way:

1. `SET key {digest, status: in_flight} NX PX <ttl>` in Redis. If the key exists, return its in-flight or final result, or 409 on a digest mismatch.
2. Run the command; the first transition's transaction inserts the key into the Postgres `idempotency` table (unique per integration and scope) with the digest.
3. After the final commit, update both records with the result.

A unique violation in step 2 means another process holds the key (for example after Redis lost it); Payments then returns the Postgres record. A Redis error, timeout over 50 ms or missing key goes straight to the Postgres path, so Redis loss costs speed, never correctness. Records hold the digest, status and response, never tokens, checkout credentials or request bodies.

Redis is one standalone `Redis` resource from the platform operator (Valkey, one instance, `maxmemory` sized for the retention window with headroom, `maxmemory-policy noeviction`, no persistence, password from an ExternalSecret, network policy admitting only Payments). With a 24-hour TTL at the 50 attempts/s design load, about 4.3 M keys of a few hundred bytes need roughly 1–1.5 GiB; the retention window is configurable. `noeviction` makes memory exhaustion fail writes, which Payments treats as a miss and falls back to Postgres. Alternative considered: Redis as a best-effort completed-result cache only — rejected at the user's direction to make Redis the first idempotency check; the Postgres constraint keeps it safe.

### Payment state machine and contract fixtures

The domain is a pure Go state machine: `Transition(state, command) → (state', event)` validates a command against current state and returns the new state and the one ledger event to insert. Compact table-driven domain tests use direct commands to cover lifecycle transitions and rejection rules. A small representative set of protobuf-JSON histories in `contracts/payments/v1/testdata/` verifies Go/Python serialization parity. The historical generator does not use them; its data is checked by the historical-payments ODCS lifecycle rules instead. The ledger is never read back to rebuild state.

### Attempt orchestration as sequential commits

`StartAttempt` commits `AttemptStarted`, then calls fraud and commits `RiskDecisionRecorded`. On approve it commits `ProcessorOperationRequested` (the effect intent) before the processor call. It then calls `Sale` and commits the outcome. Each step is its own versioned transition, so a crash leaves an attempt row whose status the recovery worker can query. Fraud (1 s) and processor (3 s) deadlines sit inside a 5 s overall attempt budget; when the budget runs out after the intent commits, the response is `PENDING`. The response returns the attempt's state after the last step. The idempotency record is written with the first append and completed with the final result, so a retry during processing gets the in-flight result, or `PENDING` after the unknown step.

Fail closed on fraud errors records a real `RiskDecisionRecorded` (`DECLINE`, `FRAUD_UNAVAILABLE`, `payments-fallback-v1`), so histories stay lifecycle-valid and the data shows fraud outages honestly.

### Checkout token without storage

`checkout_token = base64url(session_id ‖ expiry ‖ HMAC-SHA256(key, session_id ‖ expiry))`. Payments verifies by recomputing, and repeated creation returns the same token. Creation also returns the hosted checkout URL that embeds it. The key comes from an ExternalSecret. Alternative considered: random tokens stored hashed. Rejected because idempotent creation would then have to return a token it cannot recover.

### Fingerprints at the edge

The edge normalizes (trim, case-fold, collapse whitespace) the full shipping address and computes `HMAC-SHA256(fingerprint_key, normalized)` for the address fingerprint. It does the same for the client IP's /24 (IPv4) or /48 (IPv6) network. The vault computes the payment-method fingerprint from the fictional card identity using the same synthetic key and vectors as the historical generator; Edge reads only that safe metadata. Only fingerprints and country/region/postal code enter fraud inputs and payment events. For synthetic integrations, a `simulated_client` object supplies IP country and network directly. For real integrations, the network comes from the trusted proxy's forwarded client address (hosted page) or is absent (server-to-server API calls), and the country is absent until a geolocation source is chosen.

Real integrations use the secret key from the ExternalSecret. Synthetic integrations use a separate, non-secret synthetic key in chart values, the same key the historical generator uses. Historical and live fingerprints of one synthetic entity are therefore equal, the generator needs no secret, and synthetic and real fingerprints can never collide. Alternative considered: accepting precomputed fingerprints from synthetic integrations. Rejected because the edge's normalization would then go unexercised for all simulated traffic. Normalization and HMAC cases live in `contracts/synthetic/identity-vectors.json`, which Edge, vault and the Python generator test for their respective responsibilities.

### Integrations and identities without Accounts

Edge loads integrations from a mounted config file and ExternalSecret: `integration_id`, SHA-256 hash of the API key, `synthetic`, fingerprint key reference, and optionally `webhook.url`, `webhook.secret` and `webhook.events`. Edge passes the authenticated integration to Payments and to the notifier's config. Only synthetic integrations may send simulated client context or call the payment-method route.

Marketplace traffic uses the mesh as its transport baseline. The `ecommerce` namespace is ambient-enrolled and listed in Edge's `callerNamespaces`, so marketplace-to-Edge calls and notifier-to-web webhooks get STRICT mTLS without application TLS or certificates. Application-level secrets stay because the mesh cannot replace them: the per-integration API key identifies the integration (mesh identity identifies only the namespace), the webhook HMAC signature authenticates the event payload, and the checkout token keys protect hosted-page sessions. The synthetic integration ID is chosen to match the historical dataset's configuration.

Payments derives `merchant_id` as UUIDv5 of `"{integration_id}:merchant:{external_seller_id}"` and `customer_id` as UUIDv5 of `"{integration_id}:customer:{external_buyer_id}"`, under the namespace in `contracts/synthetic/identity-vectors.json`. Account creation times arrive in each payment's snapshot. A live simulator continuing a dataset produces the same IDs without importing mappings.

Alternative considered: an Accounts service with hashed keys in a database, merchant provisioning and customer mapping tables. Rejected for this slice: with two integrations it adds a deployable, database, contract and request hop for data that is either static configuration or derivable. Adding an integration is a secret change and redeploy.

### Isolated vault simulator

`vault-simulator` issues opaque cryptographically random tokens. `TokenizeCard`, callable only by Edge, accepts a test card number and maps it through an allow-list to a fictional card identity and behavior `ok`, `nsf`, `issuer` or `invalid` (for example `4242…` approves, `4000…9995` is insufficient funds); any other number is rejected. The allow-list is generated from a seeded range of Luhn-valid numbers per behavior, large enough for the live simulator's card population, and its method fingerprints come from the fictional card identity, so the identity vectors list them. Store a token digest, integration, active flag, safe method metadata and an authenticated-encrypted fixture payload in a vault-owned database. Encryption keys come from External Secrets; tokens are returned once and excluded from logs. No endpoint accepts real PAN, PIN or card-security-code fields.

`vault.v1` separates Edge card tokenization, metadata and processing resolution. `VaultService` (tokenize, describe) and `ProcessingService` (resolve) are served on separate plaintext h2c ports (8000 and 8001); the mesh `AuthorizationPolicy`, rendered from the chart's `mesh.rules`, admits only Edge's workload identity to the first and only the processor's to the second. Edge may describe an active token only for the integration resolved from checkout access, obtaining method type and stable fingerprint without card identity/behavior. Only the processor may resolve the fictional fixture. Rely on the Istio ambient mesh (STRICT mTLS and principal-based policy) for transport encryption and caller identity, with no application-level TLS or bearer credentials, and keep default-deny network policy as a layer-4 backstop, including denial of direct vault-database access by Edge/Payments/processor roles. Shared Postgres infrastructure may host the separate vault database; this is a synthetic isolation demonstration, not a production CDE or PCI certification.

Opaque token changes must update Edge, processor, live-simulator inputs and fingerprint-vector ownership together without changing the existing event/fraud schemas. Different tokens for one card retain the same fingerprint, so historical/live entity identity remains stable. The fault-driving behavior never reaches Fraud.

### Processor simulator store and faults

The simulator has its own database with a table of operations keyed by `operation_key`, request hash and outcome. For a new sale it resolves the integration-scoped token through the vault's processor-only listener; repeat operations and `GetOperation` return the recorded outcome without vault access. A vault outage on a new operation returns unavailable without fabricating a decline or charging; Payments retains uncertainty and recovery retries safely. Faults (latency distribution, timeout rate) are seeded configuration. A timeout records the outcome and then sleeps past the caller's deadline, which reproduces a lost response realistically. Transport is `processor.v1` gRPC for this synthetic slice. Payments depends on application-owned provider operations, not generated transport types; a transport adapter maps them to the simulator. `Sale` and `GetOperation` describe this simulator's capabilities, not a guarantee that every bank exposes an idempotency key or status query.

### Adopted bank integration and policy direction

The [gateway design and bank boundary](../../../../docs/payment-gateway/architecture.md#gateway-design) adopt synchronous processing, asynchronous delivery, the isolated synthetic vault and Redis. This change replaces their event-sourced authority with versioned state and an append-only ledger; task 7.3 updates those docs. Automated reconciliation and settlement files are excluded. Follow-up increments must meet these gates:

- First prove a synthetic ISO 8583 request/response exchange through a bank adapter and a bank simulator, with a named, versioned fictional profile, byte-level fixtures and no live bank or real card data. The current gRPC simulator remains the minimum slice; it is not an ISO wire test. Keep ISO codecs and connection/session state outside the payment aggregate and fraud API.
- Before real connectivity, obtain the chosen bank/acquirer's integration specification and certification requirements. Pin its ISO edition/profile, MTIs, required/conditional fields, encoding/bitmap rules, framing, transport/session management, security/MAC/key responsibilities, response codes and amount/currency rules. ISO 8583 standardizes messages; it does not prescribe transport or settlement. HTTP-based acquiring interfaces remain a provider-specific alternative.
- Durably correlate the gateway operation key with provider context and transaction identifiers. STAN/RRN alone are not global gateway identities. Specify handling for duplicate, late, malformed and unmatched responses. Recovery must use the synthetic profile's supported query or reversal/advice; unresolved outcomes remain pending for inspection. Never infer that a timed-out financial request is safe to resend because the gRPC `GetOperation` API exists.
- A separate lifecycle increment must distinguish issuer authentication, authorization holds, capture, reversal, refund and settlement before adapting such operations to ISO messages. The current `Sale`/`PaymentSucceeded` contract does not prove settlement, and `fraud.v1` APPROVE does not authenticate a cardholder. Add the required events, API states and caller/generator scenarios together rather than reinterpreting existing payloads.

Real card collection/live bank connectivity would require a separate production scope, including a reviewed cardholder-data environment and verification of financial outcomes. Keep credentials and wire dumps out of Payments events, fraud inputs, Kafka and analytical stores. The simulated isolated vault does not establish PCI DSS compliance. Initial functional and non-functional acceptance goals, including latency and availability, are in the [architecture requirements](../../../../docs/overview/requirements.md).

### Workers inside Payments

Recovery and expiry run as goroutines in Payments, and a Postgres advisory lock elects one active worker across replicas. They select candidates from indexed attempt and payment status columns. Recovery and expiry are idempotent because every transition checks the expected row version. They query individual operations and do not ingest settlement files or reconcile bank balances. A separate worker deployment is unnecessary at this scale.

### Shared Go module

One `src/payment-gateway/shared/go.mod` groups generated bindings in `src/payment-gateway/shared/proto`, reusable container fixtures in `src/payment-gateway/shared/testutil`, and common runtime/security helpers. Root contracts remain authoritative and declare `go_package`; Python generation stays in `src/fraud-service/gen`. Service modules require the shared module with a local `replace ../shared`. Docker retains that directory layout and builds with `GOWORK=off`. The five Go services and shared module live under `src/payment-gateway`; Python fraud serving remains at `src/fraud-service` and the historical simulator at `src/simulator`. The optional Go workspace is scoped to `src/payment-gateway/go.work`; CI and Docker keep `GOWORK=off`. Service-specific migrations, fixtures, orchestration and lifecycle remain with their owners.

### Application chart and database layout

One `infra/charts/payment-gateway` application chart owns all five Go services in a `services` map, CNPG, Valkey, secrets, ingress and access policies. One gateway Argo child replaces the individual service/infrastructure children; Kafka and fraud serving retain their separate charts. Resource names and access boundaries stay stable. At rollout, orphan the old gateway children's resources before removing those Applications and let the consolidated child adopt them; their deletion finalizers must not prune retained resources.

One CNPG `Cluster` (`payment-gateway-pg`, three instances in the configured prod target) with declarative `Database` resources and managed roles `payments`, `payments_app`, `payments_cdc`, `notifier`, `vault`, `processor`. Each owner role owns only its database; `payments` runs Payments migrations and `payments_app` is the Payments runtime role, so the ledger's insert-only grant holds for running code, with `CONNECT` revoked from `PUBLIC`. The cluster enables `wal_level: logical` for CDC; a separate `payments_cdc` principal holds replication and `SELECT` on `payment_events` only, so vault and private operation-request data are never published. Each service's migrations (goose, embedded SQL) run as an init container invoking `<service> migrate`, so a service can only migrate its own schema. Shared testcontainers-go helpers provide Postgres, Redis and Kafka for integration tests locally and in CI, skipping only with `go test -short` and failing when Docker is missing. No TEST_* overrides or CI service containers are used. End-to-end smoke and access evidence comes only from the prod Kubernetes cluster after GitOps rollout; do not manually run services or databases in Docker for end-to-end checks. Alternative: one cluster per service. Rejected for homelab resource cost; database roles isolate ordinary application access, while cluster administrators and replication credentials remain privileged.

### Later database sharding

The first slice uses one Payments database. Adopt merchant-based sharding as a later scaling increment: a durable versioned directory maps `(integration_id, merchant_id)` to a home shard, and payment/session lookups use a stable locator. Colocate state rows, ledger, idempotency, encrypted operation requests and the future journal so commands retain a local transaction. Prove writer/recovery fencing, complete-state migration, directory cutover, rollback and hot-merchant handling before rollout. Publication/checkpoints become per shard; stable event IDs and per-payment versions remain the consumer contract. Redis partitioning is independent. See [sharding flow](../../../../docs/payment-gateway/payment-flows.md#later-database-sharding).

### Libraries

pgx v5, sqlc for queries, goose v3, grpc-go with the health service, client_golang metrics (including NFR-07's attempt latency/failures, pending-outcome age, Redis hit/error rates), optional OTel SDK instrumentation without a configured exporter, and slog JSON logging. Application metrics, traces, dashboards and alerts are not integrated with ClickStack in this gateway skeleton; a later change will handle collection and export. This keeps service scaffolding portable without introducing a direct backend dependency.

## Risks / Trade-offs

- [Large change] → Tasks are grouped so contracts plus vault, then Payments plus processor/Redis, then edge, Kafka and notifier, then deployment, can each land as a PR.
- [Synchronous attempt latency is the sum of fraud and processor] → Deadlines cap it. Unknown outcomes return `PENDING` rather than blocking.
- [Advisory-lock leader election] → If the leader dies its lock is released with the connection, and another replica takes over within one poll interval.
- [State and ledger drift] → Both change in one transaction and the ledger version must equal the new row version; a store test checks that the latest ledger version matches every row. Without event sourcing, a corrupted state row is repaired from the ledger by hand, not by automatic rebuild.
- [Global position gaps and out-of-order commits] → Later publication uses WAL commit order and durable connector offsets, not a `global_position > last_seen` scan.
- [Public hosted page] → Only `/checkout/*` is routed, published through the platform Cloudflare tunnel at `pay-gateway.phuchoang.sbs`; the checkout token is session-scoped and expiring, the form is CSRF-bound to the session, and card numbers outside the allow-list are rejected. Rate limits remain with the routing work.
- [Kafka or Connect down] → Payments keep committing; the replication slot retains WAL until Connect catches up, and consumers can read authoritative status with `GetPayment`. Slot lag and CNPG volume size are checked in the outage evidence; a long outage is resolved by recreating the connector, which republishes (duplicates, not gaps).
- [Single broker, replication factor 1] → Matches the marketplace's prod choice; Kafka is a delivery copy and Postgres keeps the authoritative events.
- [Unauthenticated internal Kafka listener] → ClusterIP plus network policy limited to `payment-gateway`; events carry no tokens or personal data; authentication belongs to #63.
- [Webhook receiver down for long] → Bounded retries then a failed delivery row; the integration can still read authoritative status with `GetPayment`.
- [Redis memory sized for the retention window] → `noeviction` plus Postgres fallback; shorten the TTL if memory is tight, since Postgres keeps idempotency for longer.
- [Test-card route on the JSON API] → Synthetic integrations only, allow-listed numbers only, bodies excluded from logs and traces.
- [HMAC keys rotate] → Rotation changes fingerprints. Keys are versioned in secret names, and rotation handling belongs to the routing, service access and secrets work.

## Migration Plan

1. Land contracts and the vault simulator; validate migrations, encrypted fixture storage, role isolation and per-port vault policy rendering against local/integration Postgres.
2. Land processor simulator, Payments and Redis; validate idempotency with Redis loss, ledger immutability and recovery with fraud in the integration environment.
3. Land edge routes, the hosted page, Kafka, the outbox connector and the notifier; configure a synthetic integration, tokenize test cards through `POST /v1/payment-methods` and, after GitOps rollout to the prod Kubernetes cluster, run a scripted smoke flow (create payment, approve, decline, timeout-then-recover) and record evidence. Verify every committed event appears on `payments.events.v1` in per-payment order with unchanged payloads, including across a Connect restart and a Kafka outage. Then configure a real integration with a webhook URL, pay through the hosted page with test cards and verify signed, deduplicable webhook receipt.
4. Render the configured prod root and confirm the shared `fraud-doppler` ClusterSecretStore allow-list includes `payment-gateway` and its bootstrap token Secret `fraud-doppler-token` exists in `operators`. Roll out through the normal GitOps process after validation; PR builds publish disposable PR-specific tags and main builds publish main-specific tags. Only after every main image publishes successfully, one CI job pins all three production chart tags to the corresponding main tag and commits with `[skip ci]`. PRs and feature branches never change production pins. Image-only production-values commits do not rebuild images; no image-updater controller is required. Provision integration keys, webhook secrets and the synthetic key through Doppler, and run smoke/access checks only against the rollout. The marketplace switches to the gateway in its own change once this rollout is verified. Rollback: revert applications; retain the database and use schema-compatible images. No dev root is introduced.

## Open Questions

- Payment expiry default (15 minutes) and recovery grace period (for example 10 seconds) can be tuned with the live simulator without changing behavior.
