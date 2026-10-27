## Context

DP1 uses native OpenMetadata 2.0.6 discovery/tests/profiling plus a reusable Python SDK publisher. Four gateway databases and the Kubeflow model registry use Postgres. Kafka exposes internal plaintext transport and repository protobuf contracts without Schema Registry.

## Decisions

A new metadata-integrations chart owns shared service configuration, ExternalSecrets, a registration synchronization Job and per-connector native discovery CronWorkflows. Shared resources stay in fraud-data-prod; data-pipelines retains the namespace and workload lifecycle guard. Registration is an ordinary content-addressed Job: configuration/image/secret-reference changes produce a new Job, while scheduled discovery reapplies current native configuration. Secrets remain external. No hook reruns are required on every unchanged sync.

The shared configuration is mounted into both metadata discovery pods and pipeline native stages. Pipeline discovery explicitly runs before validation, even if scheduled discovery has not run. Existing starrocks/rustfs/argo identities remain stable. Only the shared registration path creates service connections; the product publisher resolves existing services and owns its source containers, products and executions. Native workflows own discovered tables/topics, never custom discovery.

Gateway Postgres readers are managed by CNPG, with database-owner jobs granting only connection/schema/read access and default table privileges. The model-registry owner provisions its dedicated reader with a database-owner job. New passwords use ExternalSecrets, not owner credentials in catalog jobs. Database metadata extraction excludes samples, profiling, query usage and stored procedure bodies. Kafka discovery uses the current broker protocol and allowlisted topics, without consuming payloads or supplying a nonexistent Schema Registry. RustFS registration remains storage-service configuration; per-run logical source containers remain with pipelines to avoid file-level asset explosion.

Data products are consumer contracts, not a required wrapper around each database. Postgres databases and Kafka topics are catalog assets first. Future Feast/Flink/Kubeflow semantic integrations require supported connectors and separately verified lineage; this change does not invent them.

## Verification

Reuse existing Python and Helm CI. Add only behavior coverage for missing/shared connection selection and service registration ownership. Locally run lint, formatting, Helm rendering and OpenSpec validation; no tests/builds or production pipeline reruns. New image digest and reader secret provisioning are rollout prerequisites; do not claim live connector acceptance from chart rendering.
