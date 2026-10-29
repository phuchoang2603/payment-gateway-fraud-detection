# Storage

Three storage roles: immutable source files in object storage, an Iceberg lakehouse for Bronze and Silver (and later Gold and feature tables), and StarRocks as the SQL warehouse over the lakehouse.

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

Tables are Iceberg, registered in the platform's Polaris REST catalog through the `lakehouse-integration` chart. Spark writes them; StarRocks reads them through an external catalog.

**Status: planned**, with the Bronze and Silver tables in [PR #102](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/102).

| Technique | Applied to | Why |
|---|---|---|
| Partitioning | Bronze by `_source` and `days(occurred_at)` (labels by `days(label_available_at)`); Silver by day | One-day and one-month reads touch only their partitions |
| Write distribution | `write.distribution-mode=hash` and a target file size on every table | Without it, each Spark task writes one small file per partition it touches |
| Compaction | Bronze Kafka-mode appends | Frequent small appends create many small files; `rewrite_data_files` with `binpack` merges them |
| Z-order | `silver.attempts` by `customer_id` and `payment_method_fingerprint` | Feature queries filter one customer or card over a window; clustering lets file statistics skip most files |
| Maintenance | All tables | `rewrite_manifests` and `expire_snapshots`, keeping enough history for pinned Silver runs |

Kafka offsets are derived from the Bronze rows themselves, not from snapshot metadata, so compaction and snapshot expiry never lose ingestion progress.

A `maintain-lakehouse` workflow runs compaction, manifest rewrite and snapshot expiry. A `storage-layout` benchmark measures compaction (file count, average size, scan time), partition pruning (files and bytes scanned for a one-day query) and Z-order (files scanned for one customer's 30-day query), each as a median of 3 runs after a warm-up with equal results asserted. Results are added here.

## Warehouse

StarRocks reads the Iceberg tables directly through the external catalog configured by `lakehouse-integration`; queries need no copy of the lake. It runs the contract-derived SQL checks for Bronze and Silver.

**Planned:** Gold marts as StarRocks tables with sort keys and indexes chosen for the fixed analytical queries, measured before and after on the same data.
