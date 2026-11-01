## 1. Generator

- [x] 1.1 Rewrite the generator around `simulator/world.py`, shared with `live/`; per-day deterministic parts, day markers, `config.json` and a manifest written last.
- [x] 1.2 Declare schema versions 2 and 3 in the payment-events contract (`sinceSchemaVersion`) and write `.v2` and `.v3` part files.
- [x] 1.3 Add the `generate-historical` workflow; publish v18 in dev and v19 in prod (`generate-historical-v19-nvp7l`, 12 spot shards, 49 minutes, 983,727,083 events, 192,204,996 labels, 133.5 GB).

## 2. Bronze

- [x] 2.1 Implement historical ingestion (manifest listing, `mergeSchema`, lossless cast, per-version overwrite, per-file row check) and Kafka ingestion (offsets from Bronze rows, `from_protobuf`, `bronze.rejected_records`).
- [x] 2.2 Contract checks, write-audit-publish and the `dp1-bronze` workflow; prod v19 Bronze published.

## 3. Silver

- [x] 3.1 Day-range curation by `payment_day`: Bronze days `start − 1` to `end + 1`, deduplication, conflict quarantine, attempts, labels linked over 45 days.
- [x] 3.2 Silver contracts, run report and CI equality of daily runs and one backfill.

## 4. Gold

- [x] 4.1 `window_states` and `changes`; `feat_customer_daily` and `feat_payment_method_daily` at daily anchors with change-point rows.
- [x] 4.2 `dim_customer` continued from stored versions, with the familiar-device XOR sweep (`LOGIC_VERSION` 3), and `label_attempt`.
- [x] 4.3 Remove the short-window path (`feat_attempt_rt`); keep the existing prod table and its October–January rows until the user confirms the Flink history exists.
- [x] 4.4 Reset the `dim_customer` audit branch to `main` before a rebuild reads it (`lakehouse.restage`).

## 5. Marts

- [x] 5.1 Mart DDL with time-first keys, bloom, minmax and set indexes, and a 180-day `TTL`.
- [x] 5.2 Staged builds of the run's days, `REPLACE PARTITION` publish, `build_info`.
- [x] 5.3 Insert the fact one day at a time with the day's attempts as the hash join's build side, and bound the label summary to the labels the rebuilt days can have.

## 6. Orchestration and validation

- [x] 6.1 `dp-pipeline` with write-audit-publish per layer, `dp-daily` (suspended) and `dp-backfill` (one month at a time, `failFast`).
- [x] 6.2 Scope every Silver and Gold rule to the audited commit's range with `additional_table_filters`, and pin referenced audited tables.
- [x] 6.3 Publish lineage on every run, not only on success.
- [x] 6.4 Lean maintenance: nightly expiry, stale-branch drop and recent Bronze compaction; on-demand Z-order.
- [x] 6.5 Gold executors at 7g + 5g and `MALLOC_ARENA_MAX=2` on every executor.

## 7. Prod evidence

- [x] 7.1 Backfill 2025-10-29 to 2026-04-30 on prod v19 and record per-month stage times in `docs/data/offline.md`.
- [x] 7.2 Capture the Argo backfill timelines, the OpenMetadata data quality page and the source → Bronze → Silver → Gold → marts lineage.
- [ ] 7.3 Finish the backfill (May in `dp-backfill-drbzt`, June to 2026-10-28 in `dp-backfill-8nb8m`) and add its months to the evidence table.
- [ ] 7.4 After the backfill, count Bronze rows per `schema_version` with and without `assessment_id` as the schema-evolution evidence.
- [x] 7.5 Capture the three Grafana dashboards (May 2026; refresh over the full window after 7.3).
- [ ] 7.6 Record `EXPLAIN indexes = 1` for the fixed mart queries once the marts hold the backfilled window.

## 8. Close

- [ ] 8.1 Archive this change after 7.3–7.6, syncing the delta specs into `openspec/specs/`; verify with `openspec validate --strict --all`.
