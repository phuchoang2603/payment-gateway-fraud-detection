# Payment Gateway and Real-Time Fraud Detection

A payment gateway for a refurbished-electronics marketplace, connecting checkout, fraud decisions and the data used to train and investigate them.

## Domain

The marketplace owns buyers, sellers, orders and inventory. The gateway owns checkout sessions, payment attempts, fraud assessments, simulated processing and signed outcome webhooks. Each payment can have multiple attempts; their lifecycle events feed the lakehouse, streaming features and model training. Cards and fraud labels are synthetic.

[Gateway architecture](payment-gateway/architecture.md) · [Data pipelines](data/orchestration-governance.md) · [Feature store](ml/feature-store.md)

## Data problems

The source combines a hot merchant receiving roughly half of purchases with millions of customer, card and device identities. It includes about 2% duplicate exports and a v2→v3 schema change that adds fraud-assessment fields. Live traffic adds late arrivals and Kafka redelivery. Chargebacks arrive 7–21 days after successful fraudulent attempts, so features and labels must be joined using their availability times. [Offline processing](data/offline.md) and [streaming/replay](data/online.md) handle these cases.

## Schema

| Entity | Key / relationship | Contents |
|---|---|---|
| Payment | `(integration_id, payment_id)` | Checkout snapshot and lifecycle |
| Attempt | `(integration_id, payment_id, attempt_id)` | Customer, merchant, amount, device, network and payment-method context |
| Event | `event_id`; linked to a payment and, where applicable, an attempt | Versioned lifecycle observations |
| Fraud label | Linked attempt and label source | Fraud outcome with separate availability time |
| Feature snapshot | Integration, customer or payment-method key, timestamp | Daily and streaming history used for scoring |

Wire schemas: [payments](../contracts/payments/v1) · [fraud](../contracts/fraud/v1). Data contracts: [events](data/products/historical-payments/payment-events.md) · [labels](data/products/historical-payments/fraud-labels.md). Physical tables and relationships: [storage schemas](data/storage.md#schemas-and-naming).
