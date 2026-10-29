# Domain proposal

An ecommerce payment gateway with real-time fraud decisioning. A refurbished-electronics marketplace sends checkout snapshots to the gateway; the gateway owns hosted checkout, payment attempts, risk decisions, simulated processing and signed outcome webhooks. The marketplace keeps ownership of buyers, sellers, orders and inventory. All card data is synthetic: an isolated vault simulator issues opaque tokens for allow-listed fictional cards.

The same system produces the data for the platform: payment lifecycle events from the gateway (online) and a reproducible synthetic history with the same shape (offline), both feeding a lakehouse, a feature store and fraud models.

## What data exists

| Entity or event | Grain | Key columns | Source |
|---|---|---|---|
| `payment_events` | one lifecycle event of one payment | `event_id`, `payment_id`, `aggregate_version`, `integration_id`, `occurred_at`, `recorded_at`, `schema_version`, one payload (`payment_created`, `attempt_started`, `risk_decision_recorded`, `processor_operation_requested`, `processor_outcome_unknown`, `attempt_declined`, `payment_succeeded`, `payment_expired`) | Payments ledger via outbox CDC (online); historical generator (offline) |
| `fraud_labels` | one label observation for one attempt | `payment_id`, `attempt_id`, `is_fraud`, `source` (simulation truth or chargeback), `occurred_at`, `label_available_at`, `scenario` | Live simulator (online); historical generator (offline) |
| attempt context (inside `attempt_started`) | one payment attempt | customer and merchant IDs, order total and items, shipping address fingerprint, payment-method fingerprint, device fingerprint, IP network and country, user agent, client `attempted_at` | Edge checkout and Payments |
| `silver.attempts` | one payment attempt | `payment_id`, `attempt_id`, `customer_id`, `merchant_id`, `payment_method_fingerprint`, `amount_minor`, `started_at`, `attempted_at`, decision, outcome, payment status | DP2 Silver (in progress) |

Event and label contracts: [payment events](../data/products/historical-payments/payment-events.md), [fraud labels](../data/products/historical-payments/fraud-labels.md). Protobuf wire contracts live under [`contracts/payments/v1`](../../contracts/payments/v1).

## How the required data problems show up

| Problem | In this domain | Where |
|---|---|---|
| Skew | A hot marketplace merchant receives half of all purchases (`hot_merchant_share: 0.5`), on top of Zipf merchant and category popularity | [Offline generator](../data/offline-generator.md) |
| Schema evolution | The fraud service starts recording `assessment_id` and `evidence_sha256` on risk decisions: events move from `schema_version` 2 to 3 on a configured date, and version-2 Parquet files physically lack those columns | [Offline generator](../data/offline-generator.md) |
| Offline duplicates | About 2% of events are republished by the export with the same `event_id` and content but a later `recorded_at` | [Offline generator](../data/offline-generator.md) |
| High cardinality | About 3 million customers, 3.9 million payment methods and 4.2 million devices in the full-scale source | [Offline pipeline](../data/offline-pipeline.md) |
| Late arrivals | Store-and-forward clients submit attempts minutes after the client `attempted_at` | [Online generator](../data/online-generator.md) |
| Streaming duplicates | Kafka redelivery and at-least-once CDC deliver the same event more than once | [Online generator](../data/online-generator.md) |
| Delayed labels | Chargebacks arrive 7–21 days after a successful fraudulent attempt; absent chargebacks are unknown, not negative | [Offline generator](../data/offline-generator.md) |

## What AI problem is solved

**ML:** predict, at decision time, whether a payment attempt is fraudulent. One training row is one attempt with features computed strictly before its attempt time: request features (amount, account age, IP and shipping country mismatch) and history features per customer and per payment method (attempt counts over 5 minutes, 1 hour and 24 hours, distinct devices over 24 hours; planned 7- and 30-day windows from the offline store). The target is simulation truth, evaluated against chargebacks available before the training cutoff. See [feature store](../ml/feature-store.md) and [training](../ml/training.md).

**LLM/agent:** a read-only fraud investigation agent that answers analyst questions about a payment by retrieving recorded events, labels, features and model scores, and calling the fraud model as a tool. It never sits in the payment decision path. See [agents](../llm/agents.md).

## Functional requirements

| ID | Requirement | Delivery |
|---|---|---|
| FR-01 | Authenticate configured integrations and derive integration-scoped merchant and customer UUIDv5 identities in Payments. | Implemented |
| FR-02 | Create immutable order snapshots, scoped checkout sessions and idempotent attempts; expose authenticated payment status. | Implemented |
| FR-03 | Tokenize allow-listed fictional cards in an isolated vault simulator; only the processor identity resolves processing fixtures. | Implemented |
| FR-04 | Record a versioned fraud decision before any processor effect; fraud errors fail closed. | Implemented |
| FR-05 | Execute simulated sales through a provider adapter with a durable effect intent and operation key. | Implemented |
| FR-06 | Recover uncertain operations by status query; expire only payments without unresolved effects. | Implemented |
| FR-07 | Redis-first idempotency claims with Postgres fallback. | Implemented |
| FR-08 | Publish committed events through WAL CDC to Kafka and deliver signed, deduplicated webhooks with bounded retries. | Implemented |
| FR-09 | Synthetic ISO 8583 bank profile behind the provider adapter. | Later |
| FR-11 | Separate authorization, capture and refund with a balanced journal. | Later |
| FR-12 | Feed data, feature and model pipelines and read-only investigation from committed evidence, with temporal cutoffs and delayed labels. | In progress |

Reconciliation files, real card custody, live money movement, payouts and FX are out of scope.

## Non-functional requirements

Targets are acceptance goals for the synthetic gateway, not observed results or a PCI certification claim.

| ID | Requirement | Target |
|---|---|---|
| NFR-01 | Durable, strongly consistent payment commands | Each transition commits state, one ledger event and idempotency atomically; retries and recovery cause no extra processor effects |
| NFR-02 | Bounded checkout latency | p95 attempt response ≤ 1 s at 50 attempts/s for 10 minutes; fraud 1 s, processor 3 s, overall 5 s budgets |
| NFR-03 | Availability with bounded dependency failures | 99.9% monthly valid-request availability measured at Edge; Redis outage keeps correctness through database fallback |
| NFR-04 | Credential isolation | mTLS service transport, least-privilege roles, vault-only fixture encryption; tokens and card fields never in logs, events or the lake |
| NFR-05 | Safe recovery and delivery | Known simulator outcomes resolve within 30 s after a restart; at-least-once CDC and webhooks with stable identities |
| NFR-06 | Horizontal growth | Stateless replicas on one Payments database; merchant-keyed sharding later |
| NFR-07 | Traceable operations | Correlation IDs and policy, model and feature versions on decisions; metrics for rates, latency, failures and pending age |
| NFR-08 | Reproducible, independently deployable services | Per-service images, versioned contracts, real persistence tests, rendered GitOps targets |
