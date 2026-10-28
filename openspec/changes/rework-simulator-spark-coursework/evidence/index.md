# Data coursework evidence

The generator job `payment-gateway/historical-v17` completed. The published manifest is `s3://synthetic-source/historical/v17/manifest.json`, SHA256 `c57ed26a5a16fd7acd359cfe0eee8927d7f276ebe6d1349de1fff207f51aab7b`. It reports 811,960 event deliveries, 161,967 labels, 1,959 Parquet files and 146,690,621 bytes (Zstandard level 3), with physical event schemas 3 and 4. Label availability extends through 2025-10-19. This is publication evidence only; downstream validation is pending. No new DP1/DP2 acceptance, Spark benchmark, Argo or OpenMetadata capture has been collected for this change. The [archived v16 DP1 evidence](../../archive/2026-10-27-historical-dp1-bronze/evidence/) is a historical baseline, not acceptance of the new source or Silver.

| Requirement | Status | Required correlation |
|---|---|---|
| Generator configuration and source diagnostics | Published; downstream diagnostics pending | New manifest hash, code revision, rows, files, bytes, format and event/label time ranges |
| Skew and cardinality | Pending | Executable queries, denominators, approximation error and observed output |
| Physical schema evolution | Pending | Definitions, schedule, normalized results and eligible-field values/nulls by version and date |
| Duplicate deliveries | Pending | Injection and deduplication code, exact accounting and conflict results |
| Spark baseline and batch snapshots | Pending | Same inputs/resources, application and SQL IDs, plans, runtime, task/shuffle/spill metrics and equal-content comparison |
| Spark code and UI captures | Pending | Real code and Spark UI screenshots correlated to benchmark application and stage IDs |
| Argo stages | Pending | Real captures of the separate DP1 and DP2 workflows, run IDs, stage ordering and outcomes |
| OpenMetadata lineage, validation and contracts | Pending | Real lineage, native rule results and contract captures with accepted input/output snapshots |

Use the [DP2 evidence instructions](../../../../docs/data-ml/data-pipeline.md#silver-dp2) to correlate retained machine-readable results and real captures. Missing captures remain pending; a passing CI check or historical receipt is not a substitute.
