# Platform architecture

This is the **planned target** for an ecommerce payment gateway with fraud decisioning. The repository implements Edge, Payments, notifier, vault and processor simulators alongside Go fraud at `src/payment-gateway/fraud`. Common six-service telemetry awaits CI and deployed acceptance; Feast/KServe integration follows later. Production acceptance remains subject to the [gateway verification evidence](../deployment/gitops.md#gateway-verification). The gateway integrates with the refurbished marketplace, takes synthetic payment-method tokens through hosted checkout, evaluates risk, executes simulated processor operations, and delivers durable payment outcomes. It also produces reproducible data for the mini-coursework and its final ML extension.

The [marketplace gateway note](https://github.com/phuchoang2603/refurbished-marketplace/blob/main/docs/ecommerce-fraud-gateway.md) supplies commerce context. Marketplace owns buyers, sellers, catalog/prices, carts, orders, shipping choices, and inventory reservation/commit/release. Gateway stores scoped references and immutable checkout snapshots; it does not become their system of record. Shared cluster provisioning, networking, storage operators, secret operators, Argo CD and ClickStack/OTel infrastructure belong to [talos-proxmox](https://github.com/phuchoang2603/talos-proxmox/tree/main/docs/architecture); this repository owns its application deployments and service policies, with application telemetry integration deferred.

## How the platform is organized

- [Payment gateway architecture](../payment-gateway/architecture.md): checkout, fraud decisioning, simulated processing and durable outcomes.
- [Payment flows](../payment-gateway/payment-flows.md): identity, checkout and payment-authority contracts, with the checkout and recovery sequence.
- [Service conventions](../payment-gateway/service-conventions.md): code layout, dependency direction and per-service ownership rules.
- [Data pipeline architecture](../data-ml/data-pipeline.md): generators, DP1–DP3 pipelines and feature stores.
- [ML pipeline architecture](../data-ml/ml-pipeline.md): training, serving and drift monitoring.
- [Fraud investigation architecture](../rag/fraud-investigation.md): planned read-only retrieval agent over recorded payment and fraud evidence.

## Deployable units and owned state

| Planned deployable unit                        | Language / role                                                      | Owned state                                                                                 |
| ---------------------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Edge / hosted checkout                         | Go; integration configuration, merchant API and browser checkout boundary                       | Bounded checkout access; authoritative sessions remain with Payments                        |
| Vault simulator                                | Go; synthetic tokenization and restricted fixture resolution         | Opaque token mappings, encrypted fictional fixtures and stable method fingerprints         |
| Payments                                       | Go; sessions, attempts, versioned payment commands               | Versioned payment/attempt rows, append-only ledger, idempotency and encrypted effect intents |
| Redis                                          | Payments-owned first idempotency check                                     | Atomic in-flight claims and completed results with TTL; Postgres arbitrates conflicts   |
| Notifier                                       | Go; signed versioned delivery with retries                           | Consumer inbox, subscriptions/delivery attempts, retry state                                |
| Fraud decision service                         | Go; validate inputs, call Feast and KServe, apply risk policy         | Versioned policy and decision metadata; no payment authority                                |
| Feast feature server                           | Python Feast runtime; feature retrieval and on-demand transformations | Feature definitions/registry and online-store access; no decision or payment authority        |
| Model inference                                | KServe predictor; Python or another selected model runtime            | Approved immutable model artifact and feature contract                                      |
| Drift API and periodic drift job               | Python; online ingestion and scheduled comparisons                   | Versioned reference windows, drift results, retraining trigger state                        |
| Fraud investigation agent                      | Internal, read-only analyst assistant                                 | Versioned investigation sources and audited retrievals; no payment authority               |
| Historical generator and live simulator        | Data/simulation jobs                                                 | Seeded profiles/scenarios; separate delayed-label truth                                     |
| DP1, DP2, DP3 and streaming windows            | Data jobs with separate observable stages                            | Bronze, curated tables, historical features, checkpoints and validation results             |
| Offline writer, online writer, materialization | Independently deployable jobs                                        | Store-specific writes, checkpoints, freshness/version handling                              |
| Training pipeline                              | Python ML jobs, including distributed workers                        | Versioned training data, metrics and candidate model artifacts                              |

The processor transport adapter lives within Payments; the financial journal remains later work. Merchant/customer IDs are UUIDv5 values derived in Payments from Edge-configured integrations and external seller/buyer IDs; there is no Accounts service or provisioning call. An adapter, SDK, aggregate or class is not a separate microservice. Initial gateway storage is specified in the [minimum gateway design](../../openspec/changes/archive/2026-10-23-build-minimum-payment-gateway/design.md); data-platform choices are scoped in the [roadmap](roadmap.md#planned-data-platform-direction). The planned serving choices are Feast and KServe: Go fraud calls each directly, without a custom Python coordinator. Their deployments and Knative scaling require later acceptance; the rules-service replacement does not depend on them.

## Planned deployment view

Solid arrows carry synchronous requests/responses or storage operations. Dotted arrows carry asynchronous events or telemetry. Arrow numbers are local to this diagram; data/training internals are expanded in the [data pipeline architecture](../data-ml/data-pipeline.md).

```mermaid
flowchart TB
    M["External marketplace"]
    B["Buyer browser"]
    X["Processor / synthetic bank simulator"]
    subgraph gateway["Gateway application workloads - planned"]
        E["Go edge / hosted checkout"]
        V["Go vault simulator"]
        VS[("Vault-owned token and encrypted fixture store")]
        P["Go Payments with processor adapter"]
        PS[("Payments-owned versioned rows and append-only ledger")]
        RC[("Redis idempotency claims and results")]
        C["WAL CDC / Debezium"]
        Q[("Kafka internal payment events")]
        W["Go notifier"]
        WS[("Delivery-owned inbox and retry store")]
        F["Go fraud decision service"]
        FS["Feast feature server - Python"]
        I["KServe model inference"]
        OS[("Feature-platform-owned online store")]
    end
    subgraph data["Data and ML application workloads - planned"]
        J["Generators, DP1-DP3, windows, writers and training jobs"]
        DS[("Data-platform-owned analytical stores and registry")]
        D["Python drift API and periodic drift job"]
    end
    subgraph platform["Shared platform - externally owned"]
        O["ClickStack/OTel platform (app integration deferred)"]
        K["Talos workload clusters and management Argo CD"]
    end
    M -->|"1. credentials and checkout snapshot"| E
    B -->|"2. checkout access and synthetic method token"| E
    E -->|"5. session and attempt commands"| P
    E -->|"Edge-only tokenization and safe metadata"| V
    V -->|"token mappings and encrypted fixtures"| VS
    P -->|"first idempotency claim; DB fallback"| RC
    P -->|"6. update state and append event atomically"| PS
    P -->|"7. attempt risk inputs"| F
    F -->|"versioned feature request"| FS
    FS -->|"feature lookup"| OS
    FS -->|"8. current feature values"| F
    F -->|"9. validated feature vector"| I
    I -->|"10. score and model version"| F
    F -->|"11. risk decision and versions"| P
    P <-->|"12. adapter: simulator gRPC / bank-specific ISO 8583 or HTTP"| X
    X -->|"authenticated synthetic fixture resolution"| V
    PS -.->|"13a. committed event inserts"| C
    C -.->|"13b. stored payloads; keyed by payment"| Q
    Q -.->|"14. payment outcomes"| W
    W -->|"15. deduplication and delivery attempts"| WS
    W -.->|"16. signed webhook"| M
    Q -.->|"19. decision and payment events"| J
    J -->|"20. datasets, features and models"| DS
    J -->|"21. versioned online features"| OS
    DS -->|"22. reference and current datasets"| D
    D -.->|"23. controlled retraining request"| J
    DS -->|"24. approved model artifact"| I
    P -.->|"25. future application telemetry (not yet integrated)"| O
    K -.->|"26. application deployment reconciliation"| E
```

Shared platform hosting applies to all gateway/data workloads; the last two edges are representative to keep the view readable and do not give application services ownership of the platform. Only Edge tokenizes allow-listed test cards, through the hosted form or synthetic-only payment-method route. Integrations consume signed HTTP webhooks and status queries; Kafka remains internal. The current processor queries that vault; a future bank connector must define its own credential boundary rather than giving a bank direct access to this service.

## Reference material

The [marketplace architecture](https://github.com/phuchoang2603/refurbished-marketplace/blob/main/docs/architecture.md) is the reference for composition roots, Go modules, transport/domain/persistence separation, service-owned data, inbox/outbox patterns and shared telemetry conventions; this repository's adoption is recorded in [service conventions](../payment-gateway/service-conventions.md).
