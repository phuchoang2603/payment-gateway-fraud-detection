# Storage

Three storage roles: immutable source files in object storage, an Iceberg lakehouse for Bronze and Silver (and later Gold and feature tables), and ClickHouse as the SQL warehouse over the lakehouse.

## Object storage

| Environment | Bucket | Engine |
|---|---|---|
| Dev | `platform-dev` | MinIO |
| Prod | `platform-prod-142737101504-us-east-1` | AWS S3, versioned and private |

| Prefix | Contents |
|---|---|
| `datasets/payment-gateway/historical/<version>/` | Generator output; immutable once `manifest.json` exists |
| `runs/payment-gateway/` | Pipeline run reports and Flink checkpoints |
| `lakehouse/` | Iceberg table data, through the Polaris catalog |
| `artifacts/` | Kubeflow artifacts, owned by the platform |

Bucket names have no meaning to application code: workloads receive `S3_ENDPOINT`, `S3_PATH_STYLE`, `AWS_REGION` and an `output` or root URL. Never tag active Flink state with `expire=true`, which enables the platform's finished-run lifecycle rule.

## Lakehouse

Tables are Iceberg, registered in the Polaris REST catalog that the `lakehouse` chart deploys. Spark writes them; ClickHouse reads them through a DataLakeCatalog database.

The implementation merged in [PR #102](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/102). Prod v19 Bronze and Silver are published; complete Silver validation and maintenance measurements remain open.

| Technique | Applied to | Why |
|---|---|---|
| Partitioning | Bronze by `_source` and `days(occurred_at)` (labels by `days(label_available_at)`); Silver by day | One-day and one-month reads touch only their partitions |
| Write distribution | `write.distribution-mode=hash` and a target file size on every table, except the historical Bronze and Silver event writes, which use `distribution-mode=none` with the fanout writer | Without it, each Spark task writes one small file per partition it touches. Historical files hold one purchase day each and Silver reads Bronze files back, so those tasks already touch only a few day partitions; at v19 the hash shuffle cost about 217 GB of executor disk for no extra clustering |
| Compaction | Bronze Kafka-mode appends | Frequent small appends create many small files; `rewrite_data_files` with `binpack` merges them |
| Z-order | `silver.attempts` by `customer_id` and `payment_method_fingerprint`, as a sort rewrite in `maintain-lakehouse` | Feature queries filter one customer or card over a window; clustering lets file statistics skip files. The maintenance report compares file planning before and after the rewrite |
| Maintenance | All tables | `rewrite_data_files` with `binpack` (a Z-order sort for `silver.attempts`), `rewrite_manifests`, and `expire_snapshots` (older than 7 days, last 10 kept) so pinned Silver runs keep their snapshots |

Kafka offsets are derived from the Bronze rows themselves, not from snapshot metadata, so compaction and snapshot expiry never lose ingestion progress.

`maintain-lakehouse` is on demand; its daily 03:43 UTC CronWorkflow is suspended. It reports before/after files and bytes plus plan-only partition and Z-order probes. Historical Silver rewrites its files; incremental mode replaces affected payments, so later writes can change clustering. Maintenance and measured layout benefits are deferred until after Gold work.

## Warehouse

ClickHouse reads the Iceberg tables directly through the `lakehouse` DataLakeCatalog database configured by `lakehouse`; queries need no copy of the lake. It runs the contract-derived SQL checks for Bronze and Silver.

### Full-load check optimization on v19

The full-history amount/context rule joins `attempt_started` to `payment_created`. Hash-join attempts exceeded ClickHouse's limits. The deployed checks use external aggregation (800 MB threshold), external sort (400 MB), `join_algorithm='full_sorting_merge'`, `max_threads=2` and `max_block_size=16384`, with each run's audit snapshot pinned.

`EXPLAIN indexes = 1` showed a sort-merge join on `(integration_id, payment_id)`, explicit column reads and `payload_case` filtering through `PREWHERE`.

| Independent diagnostic | Result |
|---|---|
| Query / snapshot | `silver-full-join-merge-v19` / `8514390407709950043` |
| Amount/currency mismatches | 0 |
| Duration | 1,128.528 s (18m48.5s) |
| Rows / bytes read | 377,909,828 / 11,314,568,660 |
| Peak query memory | 1.77 GiB |
| Compressed external-sort bytes written | 17.77 GiB |

[SQL, plan and metrics](evidence/clickhouse-v19-full-load-check.json) record this diagnostic on the existing 4 GiB container and 30 GiB volume. It establishes one rule completed, not a speedup or a successful Silver workflow. `silver_unique_event` still exceeded the 3.4 GiB server memory limit in `dp2-silver-q6xgs`; aggregation needs further tuning or a bounded exact strategy.

| Technique | Applicability |
|---|---|
| Iceberg partition pruning, metadata cache and Parquet pushdown | Already enabled; date pruning helps bounded queries, not all-history checks |
| Explicit columns and `PREWHERE` | Present in the measured join plan |
| Local filesystem cache | No configured cache; no benefit claimed |
| Approximate uniqueness | Unsuitable for an exact duplicate gate |
| MergeTree keys and materialized aggregates | Relevant to native Gold tables; not indexes on Iceberg files |
| Smaller Parquet row groups | Unmeasured; keep existing layout until a controlled comparison |

**Planned:** warehouse indexing, not part of PR #102: Gold marts as ClickHouse MergeTree tables with `ORDER BY` keys and skip indexes chosen for the fixed analytical queries, measured before and after on the same data.
