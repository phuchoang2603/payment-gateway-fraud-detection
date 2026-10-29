# Orchestration and governance

Argo Workflows orchestrates the finite data pipelines; OpenMetadata is the catalog for lineage, contracts and data validation. They stand in for the course's Airflow and DataHub. Connections and credentials are defined once in the charts and Doppler and reused by every pipeline.

## Workflows

All templates live in the [`data-pipelines` chart](../../infra/charts/data-pipelines) in the `data` namespace.

| Workflow | Stages | Status |
|---|---|---|
| `generate-historical` | Shards in parallel → publish | Done; prod renders only this template |
| `dp1-bronze` (historical or Kafka mode) | Spark ingest → StarRocks checks → OpenMetadata publish | In progress (PR #102) |
| `dp2-silver` | Spark curate → StarRocks checks → OpenMetadata publish | In progress (PR #102) |
| `dp3-features` | Spark features → checks → publish | Planned |
| `maintain-lakehouse`, diagnostics, benchmark, profiling | Single on-demand stage | In progress (PR #102) |

Each pipeline stage retries only on execution errors, never on failed data checks, and each workflow holds a mutex so one writer per pipeline runs at a time. Scheduled Kafka-mode ingestion exists as a suspended CronWorkflow. Screenshots of each pipeline graph and run are added here as runs complete.

## Data products and contracts

Each data product lives in one folder under [`contracts/data-products/`](../../contracts/data-products): an [ODPS](https://github.com/bitol-io/open-data-product-standard) descriptor `product.odps.yaml` and one [ODCS](https://github.com/bitol-io/open-data-contract-standard) contract per output port. Contracts are the authoritative schema, field meanings and quality rules; product, contract and dataset versions are independent.

| Product | Ports | Reference |
|---|---|---|
| `historical-payments` | `payment-events`, `fraud-labels` | [payment events](products/historical-payments/payment-events.md), [fraud labels](products/historical-payments/fraud-labels.md) |
| `bronze-payments`, `silver-payments` | Bronze and Silver tables | Added with PR #102 |

Each contract has a generated reference under [`products/<product>/`](products). CI lints every descriptor and contract with the pinned native tools and diffs each reference against a fresh export (`bash scripts/ci-checks.sh contracts`). Regenerate a reference with the export command in [`scripts/ci-checks.sh`](../../scripts/ci-checks.sh) after changing a contract.

Historical source contracts describe schema and meaning only. Bronze and Silver contracts carry executable StarRocks SQL quality rules, each returning a violation count that must be zero; the batch package turns them into native OpenMetadata test cases.

## Catalog

The `metadata-integrations` application owns the shared catalog connections and a daily 03:00 UTC discovery of StarRocks tables and Kafka topics. Each connector declares its ExternalSecret alongside its connection. Discovery samples no rows and runs no profiling.

Pipelines publish their own lineage (historical source and Kafka topics → Bronze → Silver), pipeline execution status and native test results. A run succeeds only when every expected test reports Success. Lineage and validation screenshots for DP1–DP3 are added here.

## Schema registry

Karapace serves an internal Protobuf schema registry at `http://schema-registry.payment-gateway.svc.cluster.local:8081` with `BACKWARD_TRANSITIVE` compatibility. A registration Job registers the payment and label `<topic>-value` subjects from the root contracts on each release; an incompatible change fails the Job. Topic payloads stay raw protobuf without registry framing, so the registry gates compatibility rather than serving consumers.
