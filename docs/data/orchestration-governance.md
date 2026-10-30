# Orchestration and governance

Argo Workflows orchestrates the finite data pipelines; OpenMetadata is the catalog for lineage, contracts and data validation. They stand in for the course's Airflow and DataHub. Connections and credentials are defined once in the charts and Doppler and reused by every pipeline.

## Workflows

All templates live in the [`data-pipelines` chart](../../infra/charts/data-pipelines) in the `data` namespace.

| Workflow | Stages | Status |
|---|---|---|
| `generate-historical` | Shards in parallel → publish | Done; v18 and v19 published |
| `dp1-bronze` (historical or Kafka mode) | Spark ingest to an audit branch → ClickHouse checks on the branch → publish branch to main → OpenMetadata publish | Historical v19 succeeded; Kafka schedule suspended |
| `dp2-silver` | Spark curate to an audit branch → ClickHouse checks on the branch → publish branch to main → OpenMetadata publish | Implemented; prod v19 validation rerun in progress |
| `dp3-features` | Spark features → checks → publish | Planned |
| `maintain-lakehouse`, `dp2-benchmark` | Single on-demand stage | Implemented; prod v19 evidence pending |

Bronze and Silver use write-audit-publish: a run's rows stay on its `audit-<workflow>` Iceberg branch until every check passes, so DP2 and other readers of `main` never see unaudited data, and a failed run leaves its branch for diagnosis. Check and publication stages retry only on execution errors, never on failed data checks; Spark uses the bounded retry policy below. Each workflow holds a mutex so one writer per pipeline runs at a time. Scheduled Kafka-mode ingestion exists as a suspended CronWorkflow that runs `dp1-bronze` with `source=kafka`, then `dp2-silver` with `mode=incremental`. Pending: the complete prod graph and run screenshot set described in [offline evidence](offline.md#remaining-optimization-evidence).

### Spark sizing and placement

Prod Spark executors run on the `aws-cpu-burst` spot pool that the generator uses, through `spark.nodeSelector`. Drivers use `spark.driverNodeSelector` and workflow step pods use `workflowNodeSelector` to stay on the existing `aws-workers` on-demand pool: losing a driver restarts the application, and losing an Argo resource-step pod can submit another SparkApplication. The spot pool has 12 nodes of 4 vCPU and 16 GiB (about 3.5 vCPU and 14 GiB allocatable) and a 40 GiB root volume. Spark steps retry like the generator (limit 10, `Always`), and executors decommission gracefully; writes are idempotent, but a spot loss can still repeat computation.

- **Large jobs (prod Bronze historical, Silver, analytics, maintenance):** 11 executors of 3 cores, 9 GiB and 3 GiB overhead (DP2's Python workers hold the broadcast duplicate keys in overhead memory), one per spot node. The on-demand driver has 1 core (500m requested), 4 GiB heap and 1.5 GiB overhead; DP2 now collects about 20M duplicate-ID keys as a sorted int64 array (about 154 MB in the recorded v19 report), avoiding the earlier large broadcast hash relation. The pool allows only `large` and `xlarge` instances, so 12 xlarge nodes are its whole 48 vCPU limit; after Talos reservations and DaemonSets an xlarge has 3,270m CPU for pods, which fits one 3-core executor but not a 4-core one. Shuffle disk is the binding limit: a hash-distributed v19 Bronze write shuffled about 217 GB into 24 GiB of ephemeral storage per executor, which fits only when all 11 executors run, so the historical Bronze and Silver event writes no longer shuffle (see [storage](storage.md)). Executors need a 16 GiB node: 5 GiB executors that fit an 8 GiB c-family xlarge were OOM-killed in the Iceberg write. When Karpenter picks 32 GiB r-family spot nodes, the pool's 192 GiB memory limit stops it before 12 nodes. Shuffle partitions are 1,024 with AQE coalescing (advisory 128 MB), not 3–4× the cores, because v19 shuffles reach hundreds of GB. Historical read parallelism is set by the file splits (about 1,000 at 128 MB for v19), not by the executor count. Executors request 24 GiB of ephemeral storage, not 40 GiB, because the root volume must also hold the Spark image and shuffle spill.
- **Kafka mode (dev and prod):** one executor with 3 cores and 3 shuffle partitions. The Spark Kafka source makes one task per topic partition, and `payments.events.v1` and `fraud.labels.v1` each have 3 partitions, so `bronze.kafka.partitions` must follow the Kafka chart's topic partitions.
- **Dev:** one executor per job, 2 cores and 16 shuffle partitions, sized to fit one dev node; v18 is 112 MB.
- **Shared pool:** the generator and the Spark jobs need the same 12 nodes, so they should not run at the same time. The first v19 run records each phase's tasks and time in its report, which confirms or lowers the large executor count.

## Data products and contracts

Each data product lives in one folder under [`contracts/data-products/`](../../contracts/data-products): an [ODPS](https://github.com/bitol-io/open-data-product-standard) descriptor `product.odps.yaml` and one [ODCS](https://github.com/bitol-io/open-data-contract-standard) contract per output port. Contracts are the authoritative schema, field meanings and quality rules; product, contract and dataset versions are independent.

| Product | Ports | Reference |
|---|---|---|
| `historical-payments` | `payment-events`, `fraud-labels` | [payment events](products/historical-payments/payment-events.md), [fraud labels](products/historical-payments/fraud-labels.md) |
| `bronze-payments` | `payment-events`, `fraud-labels`, `rejected-records` | [payment events](products/bronze-payments/payment-events.md), [fraud labels](products/bronze-payments/fraud-labels.md), [rejected records](products/bronze-payments/rejected-records.md) |
| `silver-payments` | `payment-events`, `attempts`, `labels`, `rejected-events` | [payment events](products/silver-payments/payment-events.md), [attempts](products/silver-payments/attempts.md), [labels](products/silver-payments/labels.md), [rejected events](products/silver-payments/rejected-events.md) |

Each contract has a generated reference under [`products/<product>/`](products). CI lints every descriptor and contract with the pinned native tools and diffs each reference against a fresh export (`bash scripts/ci-checks.sh contracts`). Regenerate a reference with the export command in [`scripts/ci-checks.sh`](../../scripts/ci-checks.sh) after changing a contract.

Historical source contracts describe schema and meaning only. Bronze and Silver contracts carry executable ClickHouse SQL quality rules, each returning a violation count that must be zero; the batch package turns them into native OpenMetadata test cases.

### Full-data validation at v19 scale

The checks stage pins each rule to its table's audit snapshot and evaluates the full relevant table contents, including after incremental curation. Incremental SQL validation is not implemented. The checks pod submits SQL to the shared ClickHouse server; moving that pod onto Spark's spot nodes would not distribute SQL execution or increase server memory.

At v19 scale, the attempt-uniqueness rule initially aborted at ClickHouse's memory limit, so `dp2-silver-cxjgf` failed and did not publish its Silver audit branches. External aggregation and a disk-backed sort-merge join now bound check execution; the full-history amount/context join independently passed with zero mismatches at 1.77 GiB peak query memory. The [storage optimization evidence](storage.md#full-load-check-optimization-on-v19) records the SQL plan, settings, failures and successful diagnostic. That diagnostic is separate from the workflow's native OpenMetadata results. Historical rerun `dp2-silver-cnr27`, submitted on 2026-10-30 with release `91a35cc`, is still awaiting its full Spark → checks → publish outcome in this evidence record.

For coursework, capture the successful Argo graph and each table's native OpenMetadata test results from that completed workflow, plus the historical source → DP1 → Bronze → DP2 → Silver lineage and pipeline execution status. Retain the failed run as evidence that a resource-aborted check blocks publication; a Spark report alone does not establish a successful pipeline.

## Catalog

The `data-pipelines` application owns the shared catalog connections. A PostSync register job creates the OpenMetadata services on each deploy. Every checks run ingests the ClickHouse tables, and Kafka topics are ingested by the on-demand `metadata-discovery` template. Each connector declares its ExternalSecret alongside its connection. Discovery samples no rows and runs no profiling.

Pipelines publish lineage (historical source and Kafka topics → Bronze → Silver), their execution status on success and failure through the workflow exit handler, and native test results. The Kafka CronWorkflow's exit handler records only the Bronze pipeline: its Silver step runs through `templateRef`, which does not run the Silver template's exit handler. A run succeeds only when every expected test reports Success. Pending: lineage and validation screenshots for DP1–DP3, taken after the first dev run.

## Schema registry

Karapace serves an internal Protobuf schema registry at `http://schema-registry.payment-gateway.svc.cluster.local:8081` with `BACKWARD_TRANSITIVE` compatibility. A registration Job registers the payment and label `<topic>-value` subjects from the root contracts on each release; an incompatible change fails the Job. Topic payloads stay raw protobuf without registry framing, so the registry gates compatibility rather than serving consumers.
