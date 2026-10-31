# Orchestration and governance

Argo Workflows orchestrates the finite data pipelines; OpenMetadata is the catalog for lineage, contracts and data validation. They stand in for the course's Airflow and DataHub. Connections and credentials are defined once in the charts and Doppler and reused by every pipeline.

## Workflows

All templates live in the [`data-pipelines` chart](../../infra/charts/data-pipelines) in the `data` namespace.

| Workflow | Stages | Status |
|---|---|---|
| `generate-historical` | Shards in parallel → publish | Done; v18 and v19 published |
| `dp1-bronze` (historical or Kafka source) | Spark ingest to an audit branch → ClickHouse checks on the branch → publish branch to main → OpenMetadata publish | Implemented; Kafka runs inside `dp-daily` |
| `dp-pipeline` (`start`/`end` days) | Spark Silver, then Spark Gold, to one audit branch → ClickHouse marts into staging → checks on the branch and staging → publish tables → publish marts → profile marts → OpenMetadata publish | Implemented |
| `dp-daily`, `dp-backfill` | Kafka into Bronze, then `dp-pipeline` for yesterday and two lookback days (suspended cron); `dp-pipeline` month by month in order | Implemented |
| `warehouse-benchmark` | Single on-demand stage | Implemented; prod v19 evidence pending |
| `maintain-lakehouse`, `dp2-benchmark` (skewed join) | Single on-demand stage | Implemented; prod v19 evidence pending |

Every writer publishes its audit branch to `main` only after all native tests pass. Failed runs retain their branches; the exit handler records pipeline status in OpenMetadata. Bronze holds `dp1-bronze-writer`; the `dp-pipeline` template holds `dp-writer`, so the daily run and each backfill month serialize. Checks retry execution errors, not failed data rules.

### Spark sizing and placement

| Workload | Prod placement and sizing |
|---|---|
| Large Spark jobs | Spot `aws-cpu-burst`: 11 executors × 3 cores = 33 executor cores; each 9 GiB heap + 3 GiB overhead, 24 GiB ephemeral storage |
| Driver | Spot `aws-cpu-burst` with the executors (the on-demand pool is fully requested by platform workloads); 1 core (500m request), 4 GiB heap + 1.5 GiB overhead |
| Shuffle | 1,024 partitions, AQE coalescing with 128 MB advisory size; large v19 shuffles require more than 3–4× core count |
| Kafka mode | One 3-core executor, three shuffle partitions matching the topics |
| Argo steps / SQL checks | Steps on on-demand workers; SQL executes on shared ClickHouse |

The spot pool is capped at 12 nodes / 48 vCPU / 192 GiB. Allowed sizes are `large` and `xlarge`; an xlarge has about 3,270m pod CPU after reservations, so one 3-core executor fits. A 16 GiB node fits the executor memory; 32 GiB instances can hit the pool memory cap early. Keep generator and Spark jobs sequential. Spark retains graceful decommission and its bounded 10-retry policy.

## Data products and contracts

Each data product lives in one folder under [`contracts/data-products/`](../../contracts/data-products): an [ODPS](https://github.com/bitol-io/open-data-product-standard) descriptor `product.odps.yaml` and one [ODCS](https://github.com/bitol-io/open-data-contract-standard) contract per output port. Contracts are the authoritative schema, field meanings and quality rules; product, contract and dataset versions are independent.

| Product | Ports | Reference |
|---|---|---|
| `historical-payments` | `payment-events`, `fraud-labels` | [payment events](products/historical-payments/payment-events.md), [fraud labels](products/historical-payments/fraud-labels.md) |
| `bronze-payments` | `payment-events`, `fraud-labels`, `rejected-records` | [payment events](products/bronze-payments/payment-events.md), [fraud labels](products/bronze-payments/fraud-labels.md), [rejected records](products/bronze-payments/rejected-records.md) |
| `silver-payments` | `payment-events`, `attempts`, `labels`, `rejected-events` | [payment events](products/silver-payments/payment-events.md), [attempts](products/silver-payments/attempts.md), [labels](products/silver-payments/labels.md), [rejected events](products/silver-payments/rejected-events.md) |
| `gold-features` | `dim-customer`, `feat-customer-daily`, `feat-payment-method-daily`, `label-attempt` | [customer dimension](products/gold-features/dim-customer.md), [customer features](products/gold-features/feat-customer-daily.md), [payment-method features](products/gold-features/feat-payment-method-daily.md), [attempt labels](products/gold-features/label-attempt.md) |
| `payment-marts` | `fact-payment-attempt`, `fact-merchant-day`, `build-info` | [attempt fact](products/payment-marts/fact-payment-attempt.md), [merchant day](products/payment-marts/fact-merchant-day.md), [build info](products/payment-marts/build-info.md) |

Each contract has a generated reference under [`products/<product>/`](products). CI lints every descriptor and contract with the pinned native tools and diffs each reference against a fresh export (`bash scripts/ci-checks.sh contracts`). Regenerate a reference with the export command in [`scripts/ci-checks.sh`](../../scripts/ci-checks.sh) after changing a contract.

Historical source contracts describe schema and meaning only. Bronze, Silver, gold and marts contracts carry executable ClickHouse SQL quality rules, each returning a violation count that must be zero, which the checks stage runs as native OpenMetadata test cases. Each rule's table comes from its contract's server database: lakehouse rules run against the audited Iceberg snapshot, and marts rules against the build's `__staging` tables while their results attach to the live tables.

### Full-data validation at v19 scale

The checks stage pins each rule to its table's audit snapshot and evaluates the full relevant table contents, including after incremental curation. Incremental SQL validation is not implemented. The checks pod submits SQL to the shared ClickHouse server; moving that pod onto Spark's spot nodes would not distribute SQL execution or increase server memory.

Saved test definitions are excluded from execution; the processor upserts the current contract rules. This fixes stale snapshot SQL, but full-data checks still need to fit ClickHouse's limits. The amount/context join passed independently; the incremental rerun's event-uniqueness query aborted at the server memory limit. See [measured settings and limits](storage.md#full-load-check-optimization-on-v19).

Successful Argo graphs, per-table native results, pipeline statuses and historical source → DP1 → Bronze → DP2 → Silver lineage screenshots remain in the [verification checklist](../../openspec/changes/rebuild-generation-to-silver/tasks.md).

## Catalog

The `data-pipelines` application owns the shared catalog connections. A PostSync register job creates the OpenMetadata services on each deploy. Every checks run ingests the ClickHouse `lakehouse` and `marts` schemas, without staging tables, and Kafka topics are ingested by the on-demand `metadata-discovery` template. Each connector declares its ExternalSecret alongside its connection. Discovery samples no rows; only DP3's `profile` stage profiles, and only the published marts.

Pipelines publish lineage (historical source and Kafka topics → Bronze → Silver → gold tables and marts), their execution status, with the Silver snapshots a DP3 run read as its execution parameters, on success and failure through the workflow exit handler, and native test results. The Kafka CronWorkflow's exit handler records only the Bronze pipeline: its Silver step runs through `templateRef`, which does not run the Silver template's exit handler. A run succeeds only when every expected test reports Success. Pending: prod lineage and validation screenshots for DP1–DP3.

### Grafana dashboards

Three dashboards (payments overview, merchant drill-down, fraud outcomes) live in [`infra/charts/data-pipelines/dashboards`](../../infra/charts/data-pipelines/dashboards) and ship as ConfigMaps labelled `grafana_dashboard: "1"` for the platform Grafana sidecar. They query only `marts.*` through the datasource UID `clickhouse-marts` as the read-only `grafana` user. They are not catalogued in OpenMetadata. The platform repository must provide:

1. the `grafana-clickhouse-datasource@4.23.3` plugin in the VictoriaMetrics stack's Grafana;
2. a datasource named `ClickHouse marts` with UID `clickhouse-marts`: host `clickhouse.data.svc.cluster.local`, port 9000, native protocol, no TLS, user `grafana`, default database `marts`;
3. Doppler keys `CLICKHOUSE_GRAFANA_PASSWORD` (this repository's ClickHouse chart and the platform Grafana) and `CLICKHOUSE_MARTS_PASSWORD` (this repository only), in dev and prod;
4. an ExternalSecret in `observability` mapping `CLICKHOUSE_GRAFANA_PASSWORD` into Grafana;
5. dashboard sidecar coverage of namespace `data`, with `folderAnnotation: grafana_folder`;
6. if a NetworkPolicy restricts `data`, a rule allowing `observability/grafana` to reach `data/clickhouse` on TCP 9000.

Until then the dashboards load without a datasource; DP3 is unaffected.

## Schema registry

Karapace serves an internal Protobuf schema registry at `http://schema-registry.payment-gateway.svc.cluster.local:8081` with `BACKWARD_TRANSITIVE` compatibility. A registration Job registers the payment and label `<topic>-value` subjects from the root contracts on each release; an incompatible change fails the Job. Topic payloads stay raw protobuf without registry framing, so the registry gates compatibility rather than serving consumers.
