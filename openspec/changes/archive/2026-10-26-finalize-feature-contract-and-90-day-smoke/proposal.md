# Proposal

## Why

Attempt inputs are split across lifecycle events, and online/offline feature timing has no shared executable contract. Establish that boundary and a reusable 90-day development dataset before building feature pipelines or spending time generating scale data.

## What Changes

- **BREAKING**: make newly produced `AttemptStarted` records self-contained for scoring inputs, while retaining distinct lifecycle events, durable attempt/idempotency ownership before Fraud, and committed-event publication. Coordinate Go/Python bindings, gateway persistence/publication, historical generation, Parquet landing and data contracts; remove superseded current-schema branches rather than maintaining legacy adapters.
- Define a small versioned feature set with scoped entity keys, types, units, formulas, rolling-window boundaries, causal availability, null/freshness behavior, label exclusion and shared conformance examples for later Flink/Spark/Feast consumers. This change defines the contract; it does not deploy those systems.
- Replace the maintained six-day smoke profile with a 90-day US/USD electronics development profile under an unused immutable dataset version, provisionally v16. Keep entity volume small and resource-bounded, preserve lifecycle/label tails, and publish inventory and maturity/split guidance for later Spark ingestion and model development.
- Exercise the updated generator and current-time live gateway/archiver behavior through CI and deployed acceptance, including recovery and transport faults. Report source integrity, contract acceptance and downstream DP1/training readiness separately.
- Update all affected maintained documentation and the roadmap: contract and smoke first; continuous Flink/Feast features; Spark offline ingestion/curation/features; Kubeflow training and KServe serving; full-scale generation only after the end-to-end pipeline is built and exercised. Historical scale/coursework evidence remains required later.
- Preserve published v8/v11/v12/v13, stopped incomplete v14 and prior evidence. No dataset purge, full generation restart, Kafka reset, Spark simulator, registry/Avro migration or new serving deployment is included.

## Capabilities

### New Capabilities

- `fraud-feature-contracts`: shared attempt-grain feature definitions, causal availability, offline/online conformance, and leakage-safe training handoff.

### Modified Capabilities

- `payment-contracts`: self-contained immutable attempt scoring context and explicit new payload-version adoption.
- `dataset-contracts`: matching typed business fields, source provenance and versioned feature references.
- `historical-data-generation`: new 90-day small development source, tails and maturity evidence; scale generation deferred.
- `live-traffic-simulation`: current-contract reference selection, isolated run/epoch evidence and new-contract live/landing acceptance without legacy identity assumptions.

## Impact

Implementation touches root payment contracts and fixtures, generated Go/Python bindings, Payments domain/application/wire/storage mappings, simulator generation/configuration/Arrow schemas, live reference selection, the Python archiver and ODPS/ODCS descriptors/generated references. Existing Fraud policy and gateway API behavior stay rules-only; no feature-store call is introduced yet.

Documentation tasks cover the root README where relevant, `docs/overview/{roadmap,architecture,requirements}.md`, `docs/payment-gateway/{architecture,payment-flows}.md`, `docs/data-ml/{data-products,data-pipeline,ml-pipeline}.md`, generated historical product references, and affected run/config/deployment instructions. Update current guidance and links without rewriting research or archived acceptance evidence. Inspect concurrent DP1 plans before changing storage/acceptance claims; this change does not select a different catalog or warehouse.

Only planning artifacts are created now. Applying the change will produce a source usable for downstream development, not a claim of completed Spark/GX ingestion, Feast parity, model quality, or production latency acceptance.
