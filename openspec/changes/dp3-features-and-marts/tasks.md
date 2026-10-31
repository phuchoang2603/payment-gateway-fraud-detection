## 1. Feature definition and contracts

- [x] 1.1 Add `contracts/features/fraud-history-batch-v1.yaml` in the v2 format. It covers identity and version, entity keys, attempt time, daily anchor, eligibility (attempt time window, `started_at < s`, and `outcome_at < s` for outcome-based counts), every 7- and 30-day quantity with its numerator and denominator, coverage from the first Silver day, change-point emission, availability (DP3 delay after midnight), and the never-input fields. Add `fraud-history-batch-v1.examples.json` with hand-checkable cases: lower and upper boundary, expiry with no input (rows at D+1, D+8 and D+31), client and server time skew, outcome after the anchor, empty denominator, coverage before 7 and 30 days, integration separation, and an unknown device. Verify with the CI contract and feature-definition lint.
- [x] 1.2 Add `contracts/data-products/gold-features/`: an ODPS descriptor with Silver input ports, plus ODCS v1 for `dim-customer`, `feat-customer-daily`, `feat-payment-method-daily` and `label-attempt`. Use server database `lakehouse`, `sqlDialect: clickhouse`, and the quality rules in design decision 10. Verify with the CI contract lint and regenerated references under `docs/data/products/gold-features/`.
- [x] 1.3 Add `contracts/data-products/payment-marts/`: an ODPS descriptor plus ODCS v1 for `fact-payment-attempt` and `fact-merchant-day`. Use server database `marts`, the rate-denominator descriptions, and the decision 10 rules, including reconciliation with ``lakehouse.`silver.attempts` ``. Verify with the CI contract lint and regenerated references.

## 2. DP3 Spark job

- [x] 2.1 Add the shared interval-delta and change-point engine to `batch/`, as a function parameterized by anchor rule (daily grid, or exact change instants for the Feast `feature-backfill`). Then add `batch/gold/features.py` on top of it:
  - pin the snapshots of `silver.attempts`, `silver.payment_events` and `silver.labels`;
  - build `gold.dim_customer` (decision 4), `gold.feat_customer_daily` and `gold.feat_payment_method_daily` (decision 3, using interval deltas, merged distinct intervals, change-point rows, coverage nulls, and anchors capped at the Silver commit day), and `gold.label_attempt` (decision 5);
  - write each table with full replacement, hash distribution, the 256 MB target file size, day partitioning and `snapshot-property.silver.*` IDs;
  - set `created` from the pinned `silver.attempts` commit time;
  - write the run report through `batch.metrics.phase`.
- [x] 2.2 Add focused CI tests on a local Iceberg warehouse:
  - the conformance examples from 1.1, reproduced exactly for both entity types;
  - SCD2 versions for the A→B→A address case, device rotation (Z joins at its second use and X leaves after 90 days), a one-off device that opens no version, and the unchanged case, with no overlaps or gaps;
  - label point-in-time rows: earliest observation, sources kept separate, linked only;
  - rerun equality across all columns, `created` included.

  Reuse the Silver test fixtures and do not add tests that mirror configuration.
- [x] 2.3 Add `gold` tables to `batch/maintenance.py`'s table list (binpack, manifests and snapshot expiry) and its rendered template. Verify that the existing compaction test still covers the new list, in CI.

## 3. ClickHouse marts and evidence

- [x] 3.1 Add a standard-library ClickHouse HTTP client (no new dependency) and `batch/gold/sql/marts/{fact_payment_attempt,fact_merchant_day,build_info,attempt_labels}.sql` (decision 6 DDL) and `batch/gold/marts.py`:
  - split into `build` and `publish` entry points: `build` stops before the exchange, leaving the staging tables for the checks, and `publish` runs the exchanges;
  - drop leftover staging tables;
  - compute the rolling window from `MARTS_RETENTION_DAYS` (180), render it into the DDL `TTL`, insert only rows inside the window, and write `build_info`;
  - run monthly `INSERT … SELECT` from `lakehouse` with the Spark report's snapshot IDs and the sort-merge spill settings (decision 7);
  - aggregate the merchant-day table;
  - reconcile the row count within the window before the swap;
  - exchange each table, then drop staging;
  - write a report with rows, months, durations and peak memory from `system.query_log`.

  Verify that `ruff` and `ruff format` pass locally.
- [x] 3.2 Add CI tests against a pinned ClickHouse 26.10 test container, reusing the existing ClickHouse container fixture:
  - the build's rates are null on zero denominators;
  - a failed insert leaves the live tables unchanged;
  - two builds give equal content;
  - the build keeps only start days inside the rolling window, and reconciles against Silver within that window; a day older than the window is dropped by the `TTL` after `OPTIMIZE … FINAL`;
  - leftover staging is dropped.

  The lakehouse source is replaced by local MergeTree fixture tables named like the lakehouse tables, because CI has no Polaris.
- [x] 3.3 Add `batch/gold/warehouse_benchmark.py` (decision 8):
  - variants A, B, C and C′ for Q0, Q1 (hot and median merchant), Q2, Q3 and Q4;
  - `EXPLAIN indexes = 1`, the cold run after cache drops, and the median of 5 warm runs;
  - `system.query_log` metrics by `query_id`;
  - the equality guard by count and `sum(cityHash64(*))`;
  - the report to object storage, and the scratch table dropped.

  Add CI tests on the ClickHouse container: a variant with a changed value fails the guard, and the report contains every query and variant.

## 4. Checks, catalog and lineage

- [x] 4.1 Change `openmetadata.checks` to resolve `{object}` and the entity FQN from each contract's server database, as decision 10 shows for `marts` and ``lakehouse.`gold.x` ``. Under `WAP_BRANCH`, Iceberg rules are already pinned to branch heads; `marts` rules must resolve to `marts.<table>__staging` while the test still attaches to the live table's entity. Accept several products in `CHECK_PRODUCT`. Remove the environment-based schema lookup from `warehouse.py` and its callers. Extend the existing preparation test over the real contracts to cover the gold products and the mart FQNs.
- [x] 4.2 Add a scoped `marts` profiler step that runs after `publish-marts` (decision 10 configuration). A failed profile fails the stage. Verify with a focused test of the generated profiler workflow's scope.
- [x] 4.3 Add pipelines `dp3-features` and `dp3-marts` with their Silver → gold and mart edges to `openmetadata.lineage`. Record the Silver snapshot IDs from the DP3 report in the pipeline status. Extend the existing lineage and publish tests.
- [x] 4.4 In `infra/charts/data-pipelines` `catalog.connectors`, replace `databaseSchema: lakehouse` with `schemaFilterPattern` (`^lakehouse$`, `^marts$`) and add `^fact_` to the table filter;

  Verify with CI helm lint and template, and the existing integrations test.

## 5. ClickHouse users and databases

- [x] 5.1 In `infra/charts/clickhouse`:
  - add the `grafana` and `marts` profiles, users and grants;
  - add `GRANT SELECT ON marts.*` for `openmetadata` (decision 12);
  - add `grafana: CLICKHOUSE_GRAFANA_PASSWORD` and `marts: CLICKHOUSE_MARTS_PASSWORD` to `passwordKeys`;
  - extend the setup script with `CREATE DATABASE IF NOT EXISTS marts` and `marts_benchmark`.

  Verify with CI helm lint and template for every values file. Add one CI test on the ClickHouse container with the same users XML: `grafana` cannot read `lakehouse`, cannot insert, and cannot raise `max_execution_time`.

## 6. Workflows, values and dashboards

- [x] 6.1 Add the `dp3-gold` WorkflowTemplate (decision 9):
  - `features` SparkApplication with `WAP_BRANCH`, `marts` build container, `checks` (`gold-features,payment-marts`, with `WAP_BRANCH`), the shared `publish-tables` step for `gold-features`, the `publish-marts` container, then `profile`, with the exit-handler publish of `dp3-features,dp3-marts`;
  - mutexes `dp3-gold-writer` and `dp2-silver-writer`;
  - `data-pipelines.workflowPlacement` on every step pod and the shared driver placement on the SparkApplication;
  - the `dev` and `large` profiles through `dp3.profile`.

  Read the clickhouse chart's existing `clickhouse-marts` secret (a second ExternalSecret of that name would collide), and add the values `dp3.*` and `marts.*`, including `marts.retentionDays: 180` in `values.yaml`. Add a suspended daily `dp3-gold-daily` CronWorkflow (`17 4 * * *`) that references the template with `workflowTemplateRef`. Leave the six-hourly Kafka schedule unchanged. Verify with CI helm lint and template for all values files.
- [x] 6.2 Add the on-demand `warehouse-benchmark` WorkflowTemplate (one container step, `dp3-gold-writer` mutex, `warehouseBenchmark.reportRoot`). Verify with CI helm template.
- [x] 6.3 Add `infra/charts/data-pipelines/dashboards/{payments-overview,merchant-drilldown,fraud-outcomes}.json` (decision 13 panels):
  - datasource `{"type": "grafana-clickhouse-datasource", "uid": "clickhouse-marts"}`;
  - `editorType: sql`, `rawSql` with `marts.<table>` and `$__timeFilter`;
  - a `$merchant` variable.

  Add `templates/dashboards.yaml` rendering them as ConfigMaps labelled `grafana_dashboard: "1"` with `grafana_folder`, gated by `dashboards.enabled`. Verify that the JSON parses and that CI helm template renders three ConfigMaps.
- [x] 6.4 Make sure the Spark image's rebuild paths cover `src/data-pipeline/batch/batch/gold/sql` and the metadata image's cover the new contracts. Today both are already under their copied directories, so confirm and change nothing if so. Verify that the CI image builds succeed.

## 6b. Day ranges (decision 9b, replacing the modes on 2026-10-31)

- [x] 6b.1 Replace `mode` with `start`/`end` day parameters (defaulting to yesterday plus `dp3.lookbackDays`) in `batch/gold/features.py`, `batch/gold/marts.py` and `dp3-gold`. Remove the changelog views, consumed-snapshot state, refusal rules and `EXCHANGE` publish.
- [x] 6b.2 Make `windows.window_states` exact from a baseline anchor at the range's first midnight, with `windows.changes` for the daily change rows and `windows.as_of` for per-attempt lookups.
- [x] 6b.3 Add `gold.feat_attempt_rt`, the `fraud-snapshot-v2` batch layer at each attempt's `started_at`. Add its contract, generated reference and lineage edge, and set `label_attempt.event_timestamp` to `started_at`.
- [x] 6b.4 Add CI tests:
  - daily runs build the same gold as one backfill;
  - a re-run of a range changes nothing;
  - a late label rebuilds its attempt day;
  - range runs of both engines equal the same days of a full run;
  - hand-checked snapshot values;
  - a range mart rebuild equals a full one.
- [x] 6b.5 Give prod DP3 the `gold` Spark profile with a 2 GiB driver. The `large` driver was unschedulable on the on-demand pool on 2026-10-31.

## 7. Docs

- [x] 7.1 Update the existing pages only:
  - `docs/data/offline.md`: DP3 section, gold schemas, naming table, Mermaid ER diagram, SCD2 and point-in-time join rules;
  - `docs/data/storage.md` § Warehouse: marts DDL rationale and evidence method, with results pending;
  - `docs/data/orchestration-governance.md`: the `dp3-gold` row, products table, Grafana prerequisites;
  - `docs/ml/feature-store.md`: long windows at daily anchors, correcting "as of each attempt time";
  - `docs/overview/roadmap.md`: the Storage, Orchestration, Governance and Schemas rows set to in progress.

  Verify that links resolve and that the generated references match.

## 8. Local checks and handoff

- [ ] 8.0 Implementation started on the user's go-ahead on 2026-10-30, before the v19 Silver evidence finished; the push and deploys still wait for it. The `CLICKHOUSE_MARTS_PASSWORD` and `CLICKHOUSE_GRAFANA_PASSWORD` Doppler keys must exist in dev and prod before the first deploy.
- [x] 8.1 Run local non-test checks only: `ruff`, `ruff format --check`, treefmt, helm lint and template for the changed charts, the contract lints, `openspec validate dp3-features-and-marts --strict` and `git diff --check`. Commit locally.
- [x] 8.2 Push, open the PR and inspect every CI check. Report pending or failed checks honestly. (#106 merged as `b500b61`; the day-range rewrite follows in its own PR.)

## 9. Runs and evidence (cluster)

- [ ] 9.1 Dev smoke, after the PR deploys to dev. As `marts`, confirm that `SETTINGS iceberg_snapshot_id` pins a ``lakehouse.`silver.attempts` `` read, that the `SELECT ON lakehouse.*` grant and pruning on monthly filters work, and that `grafana`'s profile accepts the plugin's settings. Record the results in design.md decision 7, and choose the snapshot fallback if pinning fails. Then run `dp3-gold` and `warehouse-benchmark` on the existing v18 Silver.
- [ ] 9.2 Prod evidence, after the PR merges and `main` releases to prod. Run `dp3-gold` as a backfill from the first Silver day to yesterday, then one daily run without parameters, and record rows read and wall time for both. Capture it promptly, because the rolling `TTL` shrinks the marts until the live simulator runs. Run `dp3-gold`, then `warehouse-benchmark`, on the v19 Silver. Record in the docs:
  - run IDs, gold and mart row counts, DP3 phase tasks and times;
  - marts peak memory and disk;
  - check and profile outcomes, and lineage screenshots;
  - Spark UI from the event logs, the benchmark tables and a DBeaver ER export.

  Results that do not meet expectations are recorded as observed.
- [ ] 9.3 Send the platform requests (design decision 13). Once they are met, capture the three dashboards.

## 10. Archive and handback

- [ ] 10.1 After the evidence is recorded, archive the change, which syncs the delta specs into `openspec/specs/`. Verify with `openspec validate --strict --all`.
- [ ] 10.2 Send the final handback to the coordinator thread with `t3_thread_send` (threadId `c34bb87e-6fb4-4456-81b1-669837d72ff3`). It covers:
  - the commits on `main` and the CI state;
  - the prod runs and their outcomes;
  - the evidence and screenshots, with paths;
  - blockers and open questions;
  - for the Feast change, the shared engine's import path and signature, and the final names, keys and locations of `gold.feat_customer_daily`, `gold.feat_payment_method_daily`, `gold.feat_attempt_rt`, `gold.dim_customer` and `gold.label_attempt`: Iceberg catalog and namespace, entity key columns, `event_timestamp` and `created` semantics, partitioning, and the SCD2 validity columns.
- [ ] 10.3 Send the Feast planning thread (threadId `mcp:launch:7ca1eca307dd1025361c2668599d5e1e0680de28f6edec3ffd254c9b9283182b`, branch `plan/feast-store`) the same final table names, keys, locations and semantics with `t3_thread_send`, and tell it to rebase onto `main`. Its implementation starts on that message or the user's go-ahead.
