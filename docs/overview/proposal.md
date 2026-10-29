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
| Skew | A hot marketplace merchant receives half of all purchases (`hot_merchant_share: 0.5`), on top of Zipf merchant and category popularity | [Offline data](../data/offline.md) |
| Schema evolution | The fraud service starts recording `assessment_id` and `evidence_sha256` on risk decisions: events move from `schema_version` 2 to 3 on a configured date, and version-2 Parquet files physically lack those columns | [Offline data](../data/offline.md) |
| Offline duplicates | About 2% of events are republished by the export with the same `event_id` and content but a later `recorded_at` | [Offline data](../data/offline.md) |
| High cardinality | About 3 million customers, 3.9 million payment methods and 4.2 million devices in the full-scale source | [Offline data](../data/offline.md) |
| Late arrivals | Store-and-forward clients submit attempts minutes after the client `attempted_at` | [Online data](../data/online.md) |
| Streaming duplicates | Kafka redelivery and at-least-once CDC deliver the same event more than once | [Online data](../data/online.md) |
| Delayed labels | Chargebacks arrive 7–21 days after a successful fraudulent attempt; absent chargebacks are unknown, not negative | [Offline data](../data/offline.md) |

## What AI problem is solved

**ML:** predict, at decision time, whether a payment attempt is fraudulent. One training row is one attempt with features computed strictly before its attempt time: request features (amount, account age, IP and shipping country mismatch) and history features per customer and per payment method (attempt counts over 5 minutes, 1 hour and 24 hours, distinct devices over 24 hours; planned 7- and 30-day windows from the offline store). The target is simulation truth, evaluated against chargebacks available before the training cutoff. See [feature store](../ml/feature-store.md) and [training](../ml/training.md).

**LLM/agent:** a read-only fraud investigation agent that answers analyst questions about a payment by retrieving recorded events, labels, features and model scores, and calling the fraud model as a tool. It never sits in the payment decision path. See [agents](../llm/agents.md).

Out of scope: settlement and reconciliation files, real card custody, live money movement, payouts and FX.
