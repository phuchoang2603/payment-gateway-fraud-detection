## 1. Feature definition and contracts

- [ ] 1.1 Add `contracts/features/fraud-history-batch-v1.yaml` in the v2 format. It covers identity and version, entity keys, attempt time, daily anchor, eligibility (attempt time window, `started_at < s`, and `outcome_at < s` for outcome-based counts), every 7- and 30-day quantity with its numerator and denominator, coverage from the first Silver day, change-point emission, availability (DP3 delay after midnight), and the never-input fields. Add `fraud-history-batch-v1.examples.json` with hand-checkable cases: lower and upper boundary, expiry with no input (rows at D+1, D+8 and D+31), client and server time skew, outcome after the anchor, empty denominator, coverage before 7 and 30 days, integration separation, and an unknown device. Verify with the CI contract and feature-definition lint.
- [ ] 1.2 Add `contracts/data-products/gold-features/`: an ODPS descriptor with Silver input ports, plus ODCS v1 for `dim-customer`, `feat-customer-daily`, `feat-payment-method-daily` and `label-attempt`. Use server database `lakehouse`, `sqlDialect: clickhouse`, and the quality rules in design decision 10. Verify with the CI contract lint and regenerated references under `docs/data/products/gold-features/`.
- [ ] 1.3 Add `contracts/data-products/payment-marts/`: an ODPS descriptor plus ODCS v1 for `fact-payment-attempt` and `fact-merchant-day`. Use server database `marts`, the rate-denominator descriptions, and the decision 10 rules, including reconciliation with ``lakehouse.`silver.attempts` ``. Verify with the CI contract lint and regenerated references.

## 2. DP3 Spark job

- [ ] 2.1 Add `batch/gold.py`:
  - pin the snapshots of `silver.attempts`, `silver.payment_events` and `silver.labels`;
  - build `gold.dim_customer` (decision 4), `gold.feat_customer_daily` and `gold.feat_payment_method_daily` (decision 3, using interval deltas, merged distinct intervals, change-point rows, coverage nulls, and anchors capped at the Silver commit day), and `gold.label_attempt` (decision 5);
  - write each table with full replacement, hash distribution, the 256 MB target file size, day partitioning and `snapshot-property.silver.*` IDs;
  - set `created` from the pinned `silver.attempts` commit time;
  - write the run report through `batch.metrics.phase`.
- [ ] 2.2 Add focused CI tests on a local Iceberg warehouse:
  - the conformance examples from 1.1, reproduced exactly for both entity types;
  - SCD2 versions for the A→B→A address case and the unchanged case, with no overlaps or gaps;
  - label point-in-time rows: earliest observation, sources kept separate, linked only;
  - rerun equality across all columns, `created` included.

  Reuse the Silver test fixtures and do not add tests that mirror configuration.
- [ ] 2.3 Add `gold` tables to `batch/maintenance.py`'s table list (binpack, manifests and snapshot expiry) and its rendered template. Verify that the existing compaction test still covers the new list, in CI.

## 3. ClickHouse marts and evidence

- [ ] 3.1 Add `clickhouse-connect` (pinned) to `src/data-pipeline/batch` and lock it. Add `batch/sql/marts/{fact_payment_attempt,fact_merchant_day}.sql` (decision 6 DDL) and `batch/marts.py`:
  - drop leftover staging tables;
  - run monthly `INSERT … SELECT` from `lakehouse` with the Spark report's snapshot IDs (decision 7);
  - aggregate the merchant-day table;
  - reconcile the row count before the swap;
  - exchange each table, then drop staging;
  - write a report with rows, months, durations and peak memory from `system.query_log`.

  Verify that `uv lock`, `ruff` and `ruff format` pass locally.
- [ ] 3.2 Add CI tests against a pinned ClickHouse 26.10 test container, reusing the existing ClickHouse container fixture:
  - the build's rates are null on zero denominators;
  - a failed insert leaves the live tables unchanged;
  - two builds give equal content;
  - leftover staging is dropped.

  The lakehouse source is replaced by local MergeTree fixture tables named like the lakehouse tables, because CI has no Polaris.
- [ ] 3.3 Add `batch/warehouse_benchmark.py` (decision 8):
  - variants A, B, C and C′ for Q1 (hot and median merchant), Q2, Q3 and Q4;
  - `EXPLAIN indexes = 1`, the cold run after cache drops, and the median of 5 warm runs;
  - `system.query_log` metrics by `query_id`;
  - the equality guard by count and `sum(cityHash64(*))`;
  - the report to object storage, and the scratch table dropped.

  Add CI tests on the ClickHouse container: a variant with a changed value fails the guard, and the report contains every query and variant.

## 4. Checks, catalog and lineage

- [ ] 4.1 Change `openmetadata_jobs.checks` to resolve `{object}` and the entity FQN from each contract's server database, as decision 10 shows for `marts` and ``lakehouse.`gold.x` ``. Accept several products in `CHECK_PRODUCT`. Remove the environment-based schema lookup from `warehouse.py` and its callers. Extend the existing preparation test over the real contracts to cover the gold products and the mart FQNs.
- [ ] 4.2 Add a scoped `marts` profiler run to the checks stage when the product list includes `payment-marts` (decision 10 configuration). A failed profile fails the stage. Verify with a focused test of the generated profiler workflow's scope.
- [ ] 4.3 Add pipelines `dp3-features` and `dp3-marts` with their Silver → gold and mart edges to `openmetadata_jobs.lineage`. Record the Silver snapshot IDs from the DP3 report in the pipeline status. Add the fixed mart → dashboard map: edges are published only when the dashboard entity exists, and missing ones are reported as pending. Extend the existing lineage and publish tests.
- [ ] 4.4 In `infra/charts/metadata-integrations`:
  - replace `databaseSchema: lakehouse` with `schemaFilterPattern` (`^lakehouse$`, `^marts$`) and add `^fact_` to the table filter;
  - add a `grafana` dashboard connector gated by `connectors.grafana.enabled` (default false), with its ExternalSecret for `GRAFANA_OPENMETADATA_TOKEN`, `dashboardFilterPattern` and `dbServicePrefixes`;
  - extend `integrations.register` with the dashboard service request.

  Verify with CI helm lint and template, and the existing integrations test.

## 5. ClickHouse users and databases

- [ ] 5.1 In `infra/charts/clickhouse`:
  - add the `grafana` and `marts` profiles, users and grants;
  - add `GRANT SELECT ON marts.*` for `openmetadata` (decision 12);
  - add `grafana: CLICKHOUSE_GRAFANA_PASSWORD` and `marts: CLICKHOUSE_MARTS_PASSWORD` to `passwordKeys`;
  - extend the setup script with `CREATE DATABASE IF NOT EXISTS marts` and `marts_benchmark`.

  Verify with CI helm lint and template for every values file. Add one CI test on the ClickHouse container with the same users XML: `grafana` cannot read `lakehouse`, cannot insert, and cannot raise `max_execution_time`.

## 6. Workflows, values and dashboards

- [ ] 6.1 Add the `dp3-gold` WorkflowTemplate (decision 9):
  - `features` SparkApplication, then `marts` container, then `checks` (`gold-features,payment-marts`), with the exit-handler publish of `dp3-features,dp3-marts`;
  - mutexes `dp3-gold-writer` and `dp2-silver-writer`;
  - the `dev` and `large` profiles through `dp3.profile`.

  Add the `clickhouse-marts` ExternalSecret and the values `dp3.*` and `marts.*`. Add the `gold` step and publish names to the suspended Kafka CronWorkflow. Verify with CI helm lint and template for all values files.
- [ ] 6.2 Add the on-demand `warehouse-benchmark` WorkflowTemplate (one container step, `dp3-gold-writer` mutex, `warehouseBenchmark.reportRoot`). Verify with CI helm template.
- [ ] 6.3 Add `infra/charts/data-pipelines/dashboards/{payments-overview,merchant-drilldown,fraud-outcomes}.json` (decision 13 panels):
  - datasource `{"type": "grafana-clickhouse-datasource", "uid": "clickhouse-marts"}`;
  - `editorType: sql`, `rawSql` with `marts.<table>` and `$__timeFilter`;
  - a `$merchant` variable.

  Add `templates/dashboards.yaml` rendering them as ConfigMaps labelled `grafana_dashboard: "1"` with `grafana_folder`, gated by `dashboards.enabled`. Verify that the JSON parses and that CI helm template renders three ConfigMaps.
- [ ] 6.4 Make sure the Spark image's rebuild paths cover `src/data-pipeline/batch/batch/sql` and the metadata image's cover the new contracts. Today both are already under their copied directories, so confirm and change nothing if so. Verify that the CI image builds succeed.

## 7. Docs

- [ ] 7.1 Update the existing pages only:
  - `docs/data/offline.md`: DP3 section, gold schemas, naming table, Mermaid ER diagram, SCD2 and point-in-time join rules;
  - `docs/data/storage.md` § Warehouse: marts DDL rationale and evidence method, with results pending;
  - `docs/data/orchestration-governance.md`: the `dp3-gold` row, products table, Grafana prerequisites;
  - `docs/ml/feature-store.md`: long windows at daily anchors, correcting "as of each attempt time";
  - `docs/overview/roadmap.md`: the Storage, Orchestration, Governance and Schemas rows set to in progress.

  Verify that links resolve and that the generated references match.

## 8. Local checks and handoff

- [ ] 8.1 Run local non-test checks only: `ruff`, `ruff format --check`, treefmt, helm lint and template for the changed charts, the contract lints, `openspec validate dp3-features-and-marts --strict` and `git diff --check`. Commit locally and do not push while PR #102 is open.
- [ ] 8.2 After #102 merges, rebase onto `main`, push, open the PR and inspect every CI check. Report pending or failed checks honestly.

## 9. Dev runs and evidence (cluster, after the PR deploys to dev)

- [ ] 9.1 Spike: as `marts`, confirm `SETTINGS iceberg_snapshot_id` pins a ``lakehouse.`silver.attempts` `` read, that `SELECT ON lakehouse.*` grants and pruning on monthly filters work, and that `grafana`'s profile accepts the plugin's settings. Record the results in design.md decision 7 and choose the snapshot fallback if pinning fails.
- [ ] 9.2 Run `dp1-bronze` (v18), `dp2-silver`, `dp3-gold`, then `warehouse-benchmark` on dev. Record the run IDs, gold and mart row counts, DP3 phase tasks and times, check and profile outcomes, lineage screenshots, the benchmark table and a DBeaver ER export in the docs. Results that do not meet expectations are recorded as observed.
- [ ] 9.3 Send the platform requests (design decision 13, items 1–7). Once they are met, capture the three dashboards and the dashboard lineage in OpenMetadata.
