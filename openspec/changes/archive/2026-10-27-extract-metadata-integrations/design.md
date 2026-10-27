## Context

DP1 uses native OpenMetadata 2.0.6 discovery/tests/profiling plus a reusable Python SDK publisher. Operational databases and Kubeflow integration are outside this change. Kafka exposes internal plaintext transport and repository protobuf contracts. Main now deploys Karapace, but producers and consumers still use raw protobuf without registry framing.

## Decisions

A new metadata-integrations chart owns shared service configuration, ExternalSecrets, a registration synchronization Job and per-connector native discovery CronWorkflows. Shared resources stay in fraud-data-prod; data-pipelines retains the namespace and workload lifecycle guard. Registration is an ordinary content-addressed Job: configuration/image/secret-reference changes produce a new Job, while scheduled discovery reapplies current native configuration. Secrets remain external, with each ExternalSecret declared beside its owning connector and the catalog token beside the OpenMetadata endpoint. Runtime Secret references are derived from those declarations. ODCS/ODPS definitions remain product-owned under contracts/; shared SDK publication code can be reused by pipeline stages without moving those definitions into the connector chart. No hook reruns are required on every unchanged sync.

The shared configuration is mounted into both metadata discovery pods and pipeline native stages. Pipeline discovery explicitly runs before validation, even if scheduled discovery has not run. Existing starrocks/rustfs/argo identities remain stable. Only the shared registration path creates service connections; the product publisher resolves existing services and owns its source containers, products and executions. Native workflows own discovered tables/topics, never custom discovery.

Kafka discovery uses the current broker protocol and allowlisted topics, without consuming payloads. The native Kafka connector points to Karapace at `http://schema-registry.payment-gateway.svc.cluster.local:8081` for registered topic schemas, using its default `<topic>-value` subject convention. Deploying Karapace alone does not register schemas or change message encoding. RustFS registration remains storage-service configuration; per-run logical source containers remain with pipelines to avoid file-level asset explosion.

Data products are consumer contracts, not a required wrapper around each database. Kafka topics are catalog assets first. Future Feast/Flink/Kubeflow semantic integrations require supported connectors and separately verified lineage; this change does not invent them.

## Verification

Reuse existing Python and Helm CI. Add only behavior coverage for missing/shared connection selection and service registration ownership. Locally run lint, formatting, Helm rendering and OpenSpec validation; no tests/builds or production pipeline reruns. A verified image digest and shared service registration are rollout prerequisites; do not claim live connector acceptance from chart rendering.
