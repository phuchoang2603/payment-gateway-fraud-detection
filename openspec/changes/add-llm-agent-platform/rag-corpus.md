# RAG corpus research (2026-10-29)

Research for the RAG knowledge pipeline (design.md decision 15). Only repository files were read. The references in `../fsds` were read, never run or imported.

## Audience and questions

The agents support payment-gateway fraud operations, in two roles:
- an analyst explaining a risk decision or a decline;
- a merchant-support assistant explaining checkout behaviour.

The corpus answers "what does this code, state or rule mean, and what should happen next". The evidence tool answers "what happened to this payment". RAG never holds payment data. It holds only documents about how the system behaves.

## Proposed corpus

Approximate chunk counts assume about 250 words per chunk.

### Generated at image build from repository sources (stay in sync automatically)

| ID | Document | Source of truth | Update trigger | Chunks | Questions it answers |
|---|---|---|---|---|---|
| `payment-lifecycle` | Payment and attempt lifecycle, recovery and expiry | `openspec/specs/payment-processing/spec.md`, `docs/payment-gateway/flows.md` | Spec or flow change | ~20 | Why is a payment still open? What happens after a processor timeout? When does a session expire? Can a declined payment be retried? |
| `risk-decision` | How Fraud decides and records an assessment; observe-only velocity | `openspec/specs/fraud-decision/spec.md`, `openspec/specs/authorization-velocity/spec.md` | Spec change | ~8 | What does Fraud record per attempt? Is velocity enforced? What happens when Fraud is unavailable? |
| `checkout-api` | Merchant checkout API: requests, statuses, errors, idempotency | `openspec/specs/checkout-api/spec.md` | Spec change | ~12 | Why did my request get 409? What does `PENDING` mean? How do idempotency keys work? |
| `processor-behaviour` | Processor outcomes, unknown outcomes and their resolution | `openspec/specs/processor-simulator/spec.md` | Spec change | ~4 | What does an unknown processor outcome mean? How is it resolved? |
| `payment-events` | Event catalogue: each event type, when it occurs, what follows it | Comments and messages in `contracts/payments/v1/events.proto` and `types.proto` (rendered) | Proto change | ~6 | What follows `RiskDecisionRecorded`? What is a decline source? |
| `fraud-features` | Feature glossary and freshness rules | `contracts/features/fraud-snapshot-v1.yaml`, `openspec/specs/fraud-feature-contracts/spec.md`, `openspec/specs/streaming-fraud-features/spec.md` | Feature contract change | ~12 | What does a feature measure, over which window? How stale may it be? |
| `data-products` | Dataset and field glossary (payment events, fraud labels) | `docs/data/products/historical-payments/*.md`, descriptions in `contracts/data-products/historical-payments/*.odcs.yaml` (rendered) | Contract change | ~20 | What does `label_available_at` mean? Which label sources exist? |

### Written by hand, short and labelled synthetic

Each of these documents opens with "Synthetic document for the payment-gateway-fraud-detection demo; not a real policy." A CI test keeps every code-bearing document consistent with the code.

| ID | Document | Consistent with | Sync guard | Chunks | Questions it answers |
|---|---|---|---|---|---|
| `decline-codes` | Decline and reason-code glossary: risk reasons (`HIGH_AMOUNT`, `NEW_ACCOUNT_GEO_MISMATCH`), decline sources (`RISK`, `PROCESSOR`), processor reasons (`INSUFFICIENT_FUNDS`, `ISSUER_DECLINED`, `INVALID_CARD`), attempt statuses and edge HTTP errors. Each entry gives the meaning, the merchant-facing wording and the retry advice. | `fraud/internal/domain/policy.go`, `processor-simulator/internal/domain/sale.go`, `contracts/payments/v1/*.proto`, checkout-api spec | Test: the set of codes in the document equals the set in code and protos | ~12 | What does `NEW_ACCOUNT_GEO_MISMATCH` mean? Should the buyer retry after `ISSUER_DECLINED`? |
| `risk-policy` | Risk policy `rules-v1`: each rule, its threshold and rationale, what is observe-only or shadow, and how policy versions are recorded. Thresholds are minor units, applied regardless of currency, as the code does today. | `policy.go` (`PolicyVersion`, thresholds) | Test: version and thresholds match the constants | ~5 | Why was a 2,500.00 order declined? Which policy version applied? |
| `analyst-runbook` | Investigation steps per decline type; reading the event timeline; decision-time evidence versus later labels (no hindsight); escalation; agents are read-only | Events, labels and fraud-decision contracts | Review on contract change | ~12 | How do I investigate a geo-mismatch decline? Can a later chargeback label say the decision was wrong? |
| `chargebacks` | Chargeback and dispute policy (synthetic). The gateway is sale-only and has no dispute or refund API; chargebacks arrive later as fraud labels; friendly fraud is defined. | `labels.proto` (`LABEL_SOURCE_CHARGEBACK`) | Test: the label sources named match the enum | ~6 | Can I dispute this payment through the gateway? How do chargebacks reach the system? |
| `merchant-terms` | Merchant integration terms excerpt (synthetic): idempotency keys, card data only through hosted checkout, signed webhooks and their verification, retention | checkout-api spec, `docs/deployment/security.md` (card data) | Review on spec change | ~6 | Can I send card numbers to the API? Do I have to verify webhook signatures? |
| `fraud-patterns` | Fraud pattern primer: card testing, account takeover, stolen card on a new device, high-value fraud, friendly fraud. Generic signals only, with no simulator rates or mixes. It extends the card-present/card-not-present background in `docs/ml/background.md`, which does not cover these patterns. | Scenario names in the label contract | Review | ~5 | What does card testing look like? What is friendly fraud? |
| `merchant-faq` | Merchant support FAQ (synthetic), about 15 questions | All of the above | Review | ~15 | Short answers to common support questions |

**Total:** about 14 documents and 140–160 chunks.

### Excluded

- Simulator configs and calibration (`src/data-pipeline/simulator/configs/*`, `us-calibration.json`). They encode the generator's ground truth (scenario mix, rates, label delays), and an agent that could read them would leak simulation truth into its answers.
- Source code, tests, deployment and security operations docs, open OpenSpec changes (unapproved), and any payment or label data.

## How the references compare

- `card-fraud-platform` uses 5 hand-written bank documents (about 2,200 words, 57 chunks), one chunk per numbered clause, Qwen3-Embedding-0.6B at 1024 dimensions, a Feast project with Parquet history and online pgvector, and 7 checks. We keep the clause-level chunking, the embedding model, pgvector and the checks. We also generate most of the corpus from contracts, so it cannot drift from the code.
- Feast 0.67 (our pin) supports `vector_enabled` on the Postgres online store, with `retrieve_online_documents_v2`, cosine distance and an optional full-text query. It creates the `vector` column without a fixed dimension and builds no ANN index. Search is therefore an exact scan, which is fine at this size. It does not create the `vector` extension.
- The reference recorded a Feast 0.66 bug: `retrieve_online_documents_v2(...).to_df()` can mismatch ids and feature values. Consumers should use the search only for candidate ids, then read features by id. This has to be checked again on 0.67.
- CNPG operator 1.30.1 (platform chart 0.29.1) supports `Database.spec.extensions`. pgvector ships in the `standard` CNPG images; the `system` images are deprecated.
