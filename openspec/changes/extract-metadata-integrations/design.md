## Context

DP1 uses native OpenMetadata 2.0.6 discovery/tests/profiling plus a reusable Python SDK publisher. Operational databases and Kubeflow integration are outside this change. Kafka exposes internal plaintext transport and repository protobuf contracts without Schema Registry.

## Decisions

A new metadata-integrations chart owns shared service configuration, ExternalSecrets, a registration synchronization Job and per-connector native discovery CronWorkflows. Shared resources stay in fraud-data-prod; data-pipelines retains the namespace and workload lifecycle guard. Registration is an ordinary content-addressed Job: configuration/image/secret-reference changes produce a new Job, while scheduled discovery reapplies current native configuration. Secrets remain external, with each ExternalSecret declared beside its owning connector and the catalog token beside the OpenMetadata endpoint. Runtime Secret references are derived from those declarations. ODCS/ODPS definitions remain product-owned under contracts/; shared SDK publication code can be reused by pipeline stages without moving those definitions into the connector chart. No hook reruns are required on every unchanged sync.

The shared configuration is mounted into both metadata discovery pods and pipeline native stages. Pipeline discovery explicitly runs before validation, even if scheduled discovery has not run. Existing starrocks/rustfs/argo identities remain stable. Only the shared registration path creates service connections; the product publisher resolves existing services and owns its source containers, products and executions. Native workflows own discovered tables/topics, never custom discovery.

Kafka discovery uses the current broker protocol and allowlisted topics, without consuming payloads or supplying a nonexistent Schema Registry. RustFS registration remains storage-service configuration; per-run logical source containers remain with pipelines to avoid file-level asset explosion.

Data products are consumer contracts, not a required wrapper around each database. Kafka topics are catalog assets first. Future Feast/Flink/Kubeflow semantic integrations require supported connectors and separately verified lineage; this change does not invent them.

## OpenLineage direction

OpenMetadata 2.0.6 exposes `/api/v1/openlineage/lineage` for runtime lineage events. A future adoption should send Spark/Flink events directly there and use the Argo workflow job as the parent, avoiding a separate collector unless buffering or fan-out requires it. Canonical namespace/service and dataset mappings must resolve to discovered assets rather than create duplicates. Replace the SDK's manual input/output lineage edges only after equivalent mappings and readback are verified; do not run competing publishers for the same edge. Keep contract/product publication, quality acceptance and Argo task graph/status publication, which this endpoint does not replace. Argo events need explicit emission; chart extraction alone does not instrument workflows. The matched direct mapper resolves table and storage-container datasets, not Kafka topic entities; Kafka edges need a supported mapping path before they can replace SDK publication. This change does not enable OpenLineage or claim Feast/Kubeflow lineage support.

## Verification

Reuse existing Python and Helm CI. Add only behavior coverage for missing/shared connection selection and service registration ownership. Locally run lint, formatting, Helm rendering and OpenSpec validation; no tests/builds or production pipeline reruns. A verified image digest and shared service registration are rollout prerequisites; do not claim live connector acceptance from chart rendering.
