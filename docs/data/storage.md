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
| `datasets/payment-gateway/knowledge/<version>/` | RAG corpus documents published by the `rag-knowledge` workflow; immutable once `manifest.json` exists |
| `runs/payment-gateway/` | Pipeline run reports and Flink checkpoints |
| `lakehouse/` | Iceberg table data, through the Polaris catalog |
| `artifacts/` | Kubeflow artifacts, owned by the platform |

Bucket names have no meaning to application code: workloads receive `S3_ENDPOINT`, `S3_PATH_STYLE`, `AWS_REGION` and an `output` or root URL. Never tag active Flink state with `expire=true`, which enables the platform's finished-run lifecycle rule.

## Lakehouse

Tables are Iceberg, registered in the Polaris REST catalog that the `lakehouse` chart deploys. Spark writes them; ClickHouse reads them through a DataLakeCatalog database.

**Status: in progress** in [PR #102](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/102). No dev run has written these tables yet.

| Technique | Applied to | Why |
|---|---|---|
| Partitioning | Bronze by `_source` and `days(occurred_at)` (labels by `days(label_available_at)`); Silver by day | One-day and one-month reads touch only their partitions |
| Write distribution | `write.distribution-mode=hash` and a target file size on every table, except the historical Bronze and Silver event writes, which use `distribution-mode=none` with the fanout writer | Without it, each Spark task writes one small file per partition it touches. Historical files hold one purchase day each and Silver reads Bronze files back, so those tasks already touch only a few day partitions; at v19 the hash shuffle cost about 217 GB of executor disk for no extra clustering |
| Compaction | Bronze Kafka-mode appends | Frequent small appends create many small files; `rewrite_data_files` with `binpack` merges them |
| Z-order | `silver.attempts` by `customer_id` and `payment_method_fingerprint`, as a sort rewrite in `maintain-lakehouse` | Feature queries filter one customer or card over a window; clustering lets file statistics skip files. The storage benchmark measures the gain on a copy before and after |
| Maintenance | All tables | `rewrite_data_files` with `binpack` (a Z-order sort for `silver.attempts`), `rewrite_manifests`, and `expire_snapshots` (older than 7 days, last 10 kept) so pinned Silver runs keep their snapshots |

Kafka offsets are derived from the Bronze rows themselves, not from snapshot metadata, so compaction and snapshot expiry never lose ingestion progress.

A `maintain-lakehouse` workflow runs compaction, the Z-order sort and manifest and snapshot maintenance as one Spark job. It is on demand, with a suspended daily CronWorkflow at 03:43 UTC. It is a separate schedule, not a step after `dp2-silver`, because every Silver run overwrites all of its files: maintaining after each run would rewrite the same files again. Once a day, after the Silver run, is enough for the layout. Kafka mode runs Silver every six hours, which overwrites these files, so the Z-order holds only until the next Silver run. The evidence run keeps the order Silver, then `maintain-lakehouse`, then the benchmark. Its report also gives, for one customer's 30-day window on `silver.attempts` before and after the Z-order rewrite, and for partitioned Bronze a one-day filter against no filter, the files, bytes and planning time that Iceberg's planner selects. These are plan-only measurements. Pending: the v19 Silver run has not captured them yet.

## Warehouse

ClickHouse reads the Iceberg tables directly through the `lakehouse` DataLakeCatalog database configured by `lakehouse`; queries need no copy of the lake. It runs the contract-derived SQL checks for Bronze and Silver.

**Planned:** warehouse indexing, not part of PR #102: Gold marts as ClickHouse MergeTree tables with `ORDER BY` keys and skip indexes chosen for the fixed analytical queries, measured before and after on the same data.
