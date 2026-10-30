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

The implementation merged in [PR #102](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/102). Prod v19 Bronze is published; Silver validation and maintenance evidence are still in progress.

| Technique | Applied to | Why |
|---|---|---|
| Partitioning | Bronze by `_source` and `days(occurred_at)` (labels by `days(label_available_at)`); Silver by day | One-day and one-month reads touch only their partitions |
| Write distribution | `write.distribution-mode=hash` and a target file size on every table, except the historical Bronze and Silver event writes, which use `distribution-mode=none` with the fanout writer | Without it, each Spark task writes one small file per partition it touches. Historical files hold one purchase day each and Silver reads Bronze files back, so those tasks already touch only a few day partitions; at v19 the hash shuffle cost about 217 GB of executor disk for no extra clustering |
| Compaction | Bronze Kafka-mode appends | Frequent small appends create many small files; `rewrite_data_files` with `binpack` merges them |
| Z-order | `silver.attempts` by `customer_id` and `payment_method_fingerprint`, as a sort rewrite in `maintain-lakehouse` | Feature queries filter one customer or card over a window; clustering lets file statistics skip files. The maintenance report compares file planning before and after the rewrite |
| Maintenance | All tables | `rewrite_data_files` with `binpack` (a Z-order sort for `silver.attempts`), `rewrite_manifests`, and `expire_snapshots` (older than 7 days, last 10 kept) so pinned Silver runs keep their snapshots |

Kafka offsets are derived from the Bronze rows themselves, not from snapshot metadata, so compaction and snapshot expiry never lose ingestion progress.

A `maintain-lakehouse` workflow runs compaction, the Z-order sort and manifest and snapshot maintenance as one Spark job. It is on demand, with a suspended daily CronWorkflow at 03:43 UTC. It is a separate schedule, not a step after `dp2-silver`, because every Silver run overwrites all of its files: maintaining after each run would rewrite the same files again. Once a day, after the Silver run, is enough for the layout. Kafka mode runs Silver every six hours, which overwrites these files, so the Z-order holds only until the next Silver run. The evidence run keeps the order Silver, then `maintain-lakehouse`, then the benchmark. Its report also gives, for one customer's 30-day window on `silver.attempts` before and after the Z-order rewrite, and for partitioned Bronze a one-day filter against no filter, the files, bytes and planning time that Iceberg's planner selects. These are plan-only measurements. Pending: the v19 Silver run has not captured them yet.

## Warehouse

ClickHouse reads the Iceberg tables directly through the `lakehouse` DataLakeCatalog database configured by `lakehouse`; queries need no copy of the lake. It runs the contract-derived SQL checks for Bronze and Silver.

### Full-load check optimization on v19

The `silver_attempt_amount_context` rule joins every `attempt_started` event with its payment's `payment_created` event and counts amount or currency mismatches. On the unpublished audit snapshot from `dp2-silver-cxjgf`, `silver.payment_events` contains 964,433,589 rows. This is a full-history correctness check: adding a date limit would change what it validates.

The initial `auto` join hit its 800,000,000-byte join limit. In ClickHouse 26.10.2, `max_bytes_in_join` is a hard cap, while `max_bytes_before_external_join` controls spilling. Adding a 400 MB spill threshold got past that cap but still exhausted the server's 3.4 GiB memory limit after 391.852 seconds. Reducing concurrency and block size alone did not keep the hash join's late memory peak low enough; that diagnostic was cancelled. The final implementation uses external sort-merge instead.

The settings in [`openmetadata/checks.py`](../../src/data-pipeline/openmetadata/openmetadata/checks.py) apply to every contract check:

```sql
SETTINGS
    iceberg_snapshot_id = 8514390407709950043,
    max_bytes_before_external_group_by = 800000000,
    max_bytes_before_external_sort = 400000000,
    join_algorithm = 'full_sorting_merge',
    max_threads = 2,
    max_block_size = 16384
```

The snapshot above is this diagnostic's audit head; pipeline runs resolve their own audit heads. External aggregation bounds uniqueness-check state, external sorting lets the join use disk, and two threads with smaller blocks limit concurrent read buffers. The contract SQL and its full-data scope are unchanged.

`EXPLAIN indexes = 1` confirmed `FullSortingMergeJoin`, with sorting on `(integration_id, payment_id)` on both sides. Each `ReadFromObjectStorage` reads only `payload_case`, the two keys and its amount/currency fields, and applies `payload_case = 'attempt_started'` or `'payment_created'` as a `PREWHERE` filter. Explicit projection and early filtering therefore already occur in the physical plan; the query does not load every event field.

| Successful diagnostic, 2026-10-30 | Measured value |
|---|---|
| Query ID | `silver-full-join-merge-v19` |
| Audit snapshot | `8514390407709950043` |
| Amount/currency mismatches | 0 |
| Duration from `system.query_log` | 1,128.528 s (18m48.5s) |
| Reported rows / bytes read | 377,909,828 / 11,314,568,660 bytes |
| Peak query memory | 1,898,138,700 bytes (1.77 GiB) |
| Compressed external-sort bytes written | 19,085,161,916 bytes (17.77 GiB) |

The [recorded SQL, plan and query-log metrics](evidence/clickhouse-v19-full-load-check.json) retain the measurement. Reported read bytes are ClickHouse's query metric, not a measurement of S3 network traffic; sort bytes are cumulative writes, not peak disk occupancy. The test ran on the existing 4 GiB ClickHouse container and 30 GiB data volume. This establishes that the rule completes within those resources, not a speedup over the failed query or success of the whole Silver workflow. These diagnostics ran through `clickhouse-client`; the separate pipeline checks stage must still pass through native OpenMetadata tests.

Iceberg partition pruning (`use_iceberg_partition_pruning`), metadata caching (`use_iceberg_metadata_files_cache`), and Parquet predicate pushdown were already enabled. Day partition pruning helps date-bounded queries, but this join needs all days. MergeTree primary-key marks do not apply to this object-storage plan. `enable_filesystem_cache` was enabled, but `system.filesystem_cache_settings` had no configured caches, so no local filesystem-cache benefit is claimed. File targets already use 256 MiB. A local data cache or 2 MiB Parquet row groups would need their own measured experiment; neither was introduced for this fix.

**Planned:** warehouse indexing, not part of PR #102: Gold marts as ClickHouse MergeTree tables with `ORDER BY` keys and skip indexes chosen for the fixed analytical queries, measured before and after on the same data.
