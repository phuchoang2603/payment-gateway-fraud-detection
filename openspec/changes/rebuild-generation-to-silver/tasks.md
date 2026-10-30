## Status

Implementation is merged in #100/#102 and subsequent fixes. Checked implementation items do not imply prod acceptance. Bronze and all four Silver tables are published; full Silver validation remains incomplete. Benchmark execution is deferred while Gold/features proceeds separately.

## 1. Shared entity world

- [x] 1.1 Implement deterministic `simulator/world.py`; identity vectors covered in CI.
- [x] 1.2 Move the live simulator to the shared world and remove superseded modules (#100).

## 2. Contracts reset to v1

- [x] 2.1 Historical ODPS/ODCS contracts declare schema versions 2/3 and manifest publication.
- [x] 2.2 Bronze contracts cover historical/Kafka provenance and SQL rules.
- [x] 2.3 Silver contracts cover all four tables; remove old quality/landing contracts.

## 3. Historical generator

- [x] 3.1 Add validated v18/v19 configurations, dated schema changes and a minimum source size.
- [x] 3.2 Generate deterministic lifecycle events, labels, skew and duplicates; CI covers split/worker invariance.
- [x] 3.3 Implement atomic parts, day markers, immutable manifest publication and rerun refusals for local/S3 storage.
- [x] 3.4 Remove replaced generator and landing modules/tests; keep Arrow schema support.
- [x] 3.5 Add sharded `generate-historical` WorkflowTemplate, storage secret and task-result RBAC.
- [x] 3.6 Merge generator work to main and pass CI (#100).
- [x] 3.7 Publish dev v18: 11m50s, 823,963 events, 161,007 labels, 112.4 MB; project v19 at about 137 GB. Worker peak memory was not captured.
- [x] 3.8 Publish prod v19: 49 minutes, 983,727,083 events, 192,204,996 labels, 133.5 GB across 2,924 parts.

## 4. Batch project and Bronze

- [x] 4.1 Replace `bronze-ingestion` with the batch project, shared Spark/catalog settings and protobuf descriptor.
- [x] 4.2 Implement manifest inventory checks, merged schema reads, lossless casts, idempotent version replacement and per-file row checks; covered in CI.
- [x] 4.3 Implement Kafka offset resume, protobuf decoding, rejected records and empty-run handling; CI covers compaction and decoding parity.

## 5. Silver

- [x] 5.1 Collapse identical deliveries and quarantine whole conflicting payments; CI covers accepted/rejected completeness.
- [x] 5.2 Build attempt context, lifecycle and outcomes; CI covers retries, in-flight and invalid attempts.
- [x] 5.3 Build label observations, linkage and availability; CI covers linked/pending/violation cases.
- [x] 5.4 Write four tables with pinned Bronze snapshot properties and a run report; historical mode replaces all rows, incremental mode replaces affected payments. CI covers rerun equality.

## 6. Checks and governance

- [x] 6.1 Derive native test definitions from ODCS rules; fail preparation on uncovered rules.
- [x] 6.2 Implement discovery, current inline tests, result evaluation, lineage and pipeline status publication. Exclude saved stale test copies. Native ODCS/ODPS imports remain blocked under 13.3/13.4.
- [ ] 6.3 Make full-data Silver checks complete within existing ClickHouse resources. `dp2-silver-q6xgs` hit the server memory limit in `silver_unique_event`; retain the failure and verify every expected test before claiming acceptance.
- [ ] 6.4 Record a successful end-to-end DP2 workflow and native results for all four tables. Published snapshots and the independent join diagnostic do not satisfy this task.

## 7. Workflows, images and CI

- [x] 7.1 Add Bronze/Silver write-audit-publish DAGs, bounded retries, mutexes, benchmark template and suspended Kafka schedule.
- [x] 7.2 Remove landing archiver deployment, permissions, consumer group and tests.
- [x] 7.3 Update Spark/metadata/simulator images, pinned connector checksums and image/CI path coverage; builds passed on main.
- [x] 7.4 Implement on-demand `maintain-lakehouse` and suspended daily schedule: 256 MiB compaction, attempts Z-order, manifest rewrite, snapshot expiry and file-planning probes. CI covers maintenance behavior.

## 8. Benchmark

- [x] 8.1 Implement pinned-snapshot skew and dedup families, timings, task/stage metrics, spill and equal-output guards; covered in CI.
- [ ] 8.2 Deferred: run `dp2-benchmark` skew family on v19 (baseline, AQE skew join, 16-bucket salting). Record checksums, times, hot-key share and Spark plans/task distributions. If AQE does not split skew, inspect partition sizes before changing thresholds.
- [ ] 8.3 Deferred: run dedup family (`full_width_group`, `narrow_detect`) on pinned Bronze. Record equal output, timings, shuffle read/write and spill.

## 9. Docs, specs and final checks

- [x] 9.1 Document generator, Bronze/Silver behavior, measured results and remaining evidence in the three data pages; preserve generated contract references.
- [ ] 9.2 At archive, update the Purpose of `historical-bronze-ingestion` and `live-traffic-simulation` to remove landing batches.
- [x] 9.3 Implementation lint/render/spec checks and main CI passed. Validate this follow-up separately before merge.

## 10. ClickHouse instance

- [x] 10.1 Add the application-owned ClickHouse chart, users, secrets and setup Job.
- [x] 10.2 Keep the OpenMetadata user in ClickHouse and create the lakehouse DataLakeCatalog as admin.
- [x] 10.3 Chart defines no operator management user or NetworkPolicy.
- [ ] 10.4 Finish the design section 13 spike checklist: readiness/users, exact FQN, least-privilege discovery and credential masking. Do not print credentials or rerun destructive database experiments.

## 11. Lakehouse catalog

- [x] 11.1 Consolidate vendored Polaris 1.8.0, PostgreSQL, secrets, policies and registration/database jobs in `lakehouse`; render for dev/prod.
- [ ] 11.2 Confirm and record all original dev rollout assertions after platform Polaris removal, including successful registration/database hooks. Prod operation alone does not establish every dev assertion.

## 12. Evidence and archive

- [x] 12.1 Configure rolling Spark event logs for dev/prod under `runs/payment-gateway/spark-events/`.
- [ ] 12.2 Capture remaining screenshots under `docs/data/images/`: DP1/DP2 Spark jobs/stages; skew distributions/AQE plan/salted stage; dedup shuffle/spill; schema-evolution read plan or null counts; successful Argo graphs; OpenMetadata tables, lineage, pipeline statuses and all native results. Include the suspended Kafka CronWorkflow definition; a live run is deferred until traffic is enabled.
- [ ] 12.3 Complete the concise v19 evidence tables with final validation outcomes, phase/task metrics and embedded screenshots. Current Spark counts are recorded in `docs/data/offline.md`.
- [ ] 12.4 After verification, sync delta specs, validate strictly and archive this change in a docs-only follow-up.
- [x] 12.5 Merge #102 while keeping the change open for evidence and archive.
- [ ] 12.6 Deferred: run `maintain-lakehouse` after successful Silver validation, before benchmarks. Record before/after files/bytes and partition/Z-order planning probes; retain required snapshots.

## 13. Catalog registration

- [x] 13.1 Fold connector configuration, secrets, PostSync registration and on-demand discovery into `data-pipelines`; remove `metadata-integrations` chart/Application.
- [ ] 13.2 Confirm original dev post-fold checks, table discovery, native results and lineage; capture prod equivalents under 12.2.
- [ ] 13.3 Blocked on OpenMetadata 2.0.6 support: import ODCS v3 contracts without changing their meaning.
- [ ] 13.4 Blocked: publish ODPS products through supported catalog APIs.
- [ ] 13.5 At archive, move `metadata-integrations` requirements into `data-pipeline-orchestration` and remove the old capability.

## 14. Incremental Silver

- [x] 14.1 Add historical/incremental mode and wire the suspended Kafka schedule to incremental Silver.
- [x] 14.2 Implement append-snapshot scope, lookback/refusal rules and changelog consumption; CI covers equivalence, refusals and attempts changes.
- [ ] 14.3 Verify a prod incremental run with new Kafka-mode Bronze rows: input rows, affected payments, output equivalence, wall time and full checks. Wait for live traffic.
- [x] 14.4 Confirm no-change incremental execution: `dp2-silver-q6xgs` Spark completed in 2m02s; report has zero affected payments and a 2.73s scope phase. Full checks did not pass; tracked in 6.3/6.4.
