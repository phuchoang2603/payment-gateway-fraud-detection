# Data coursework evidence

Fresh dev evidence is pending after the shared-platform rebase. The selected source is `s3://platform-dev/datasets/payment-gateway/historical/v17/manifest.json`; it must be generated afresh and its actual SHA256 recorded. Old publication, CI and pipeline receipts are not acceptance of this environment.

| Requirement | Status | Retain after execution |
|---|---|---|
| Generator configuration and diagnostics | Pending | Fresh manifest/hash, code revision, volume, dates, skew, cardinality, schema null/value results and duplicate rates in the [notebook](../../../../src/data-pipeline/simulator/notebooks/historical-sanity.ipynb) |
| Bronze acceptance | Pending | Independent event/label snapshots, integrity, duplicate-aware conservation, native validation and governance |
| Silver acceptance | Pending | Accepted Bronze inputs, canonical outputs, delivery accounting, lifecycle/linkage and label-availability results |
| Spark optimization | Pending | Fixed inputs/resources, plans, application IDs, measured baseline/optimized metrics and equivalent output |
| Code and UI captures | Pending | Actual code, Spark, separate Argo DP1/DP2 stages and OpenMetadata lineage/validation/contracts |

Tests and image builds run in CI only. Record fresh results after the integration updates; prod and full-scale acceptance remain deferred. Archived history is retained separately.
