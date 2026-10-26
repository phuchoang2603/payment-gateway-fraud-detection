# Design

## Context

Start fresh from the published v16 current-contract source: Spark ingests immutable Parquet into RustFS-backed Iceberg; StarRocks and official OpenMetadata workflows own native quality/discovery. No legacy schemas, gate compatibility, old DP1 data or imported acceptance records are retained in the active path.

## Goals / Non-Goals

Use one shared SQL engine for persisted-data quality, profiling and analytics, with a path to gold star-schema marts and materialized views. Retain content preservation, stable input/run identity, safe replay, bounded execution and trustworthy acceptance. Remove superseded code and catalog artifacts outright.

This change does not implement gold marts, streaming simulation, Kafka landing, production onboarding or a second lake catalog. Roadmap adoption is not deployment or a performance claim. No migration, dual-engine fallback or legacy publisher is required.

## Decisions

### 1. Keep Spark responsible for ingestion

Retain Spark 4.1.3, Iceberg 1.12.0/Scala 2.13 and Apache Spark Operator-managed ingestion. Preserve every delivered event and label, including identical duplicates; silver owns curation. Accept the current event schema 3 and label schema 1 only. Preserve enriched AttemptStarted context, UTC microsecond timestamps and nested null semantics, and reject lossy numeric conversion or omitted business fields. Bronze contracts own normalized types and ingestion provenance separately from immutable source contracts.

Historical manifests must embed their file inventories. Verify file identity, SHA-256, bytes, footer schema and rows with bounded memory; ETags do not substitute for checksums. Never mutate accepted source data. Use the shared lakehouse runtime access key for source reads and lake/evidence writes, with a separate read-only StarRocks access key and generator writer; all are external generic secrets. The retired v7 companion path stays removed.

Persist immutable run context before writes. Serialize product writers. Historical writes replace only the selected source-version partition; recurring commits use immutable batch/input identities. Reconcile uncertain commits before retrying and record the exact event/label snapshot pair. Two table commits are not a transaction. Partial candidates cannot replace acceptance; replay of accepted input does not add deliveries. Preserve snapshots referenced by evidence.

### 2. StarRocks queries the existing catalog directly

Platform-owned StarRocks is the selected SQL validation and analytics engine. Create an external Iceberg REST catalog against RustFS using native SigV4, configurable signing service `s3`, signing region and scoped credentials; configure S3-compatible endpoint, HTTPS and path-style storage access separately. Use explicit storage credentials where the catalog does not vend them. No replacement catalog, proxy signer, snapshot synchronization service or full dataset import is required for direct queries.

Validate exact snapshots using StarRocks `FOR VERSION AS OF '<snapshot_id>'` on each table reference, including independent event/label snapshot IDs in joins. Ordinary analytical discovery may follow current catalog state; acceptance never infers its input from latest state or cache freshness. If metadata is cached, ensure the recorded snapshots are resolvable before checking them; a missing snapshot is an execution error.

Native SigV4 and time-travel are documented capabilities, not proof of our deployed interoperability. Verify RustFS authentication, snapshot reads and the native OpenMetadata external-catalog path once during implementation. Do not add disposable-table compatibility stages to every production batch or silently substitute another engine if an integration fails.

### 3. Git contracts define native SQL checks

Leave source ODCS/Data Contract CLI/DuckDB publication intact. Rewrite bronze execution definitions for StarRocks SQL with stable semantic rule IDs, normalized physical schema constraints and explicit cross-table context. Remove GX-native rules/exporters/suites and all GX runtime dependencies. Native OpenMetadata workflows execute schema/quality checks; there is no second Spark validation application.

Use native custom SQL tests with COUNT semantics: return an integer violation count, expected zero. Avoid ROWS-based unbounded failure collection. Derive native definitions from the Git-authoritative contract, including conditional nested requirements, and fail preparation on omitted/unsupported rules. Do not maintain an independently authored UI suite. Pin contract hashes and render snapshot-specific SQL from immutable run context; avoid concurrent mutation of shared test definitions by serializing DP1 runs and isolating execution identities.

Validate raw persisted schema and exact delivery conservation. Move duplicate consistency, lifecycle reconstruction, business amounts/risk/context, cross-table linkage and label maturity/as-of checks to DP2 silver; retained SQL definitions are in `contracts/quality/silver-payments.yaml` and do not establish silver acceptance. Generator cardinality/distribution/scenario bands are informational, never a reason alone to fail bronze acceptance. Simulation truth can be a training target; scenario/oracle-only fields are not inference-available features. Training joins respect event and label-availability times.

Ingestion verifies every manifest file's SHA-256, bytes, rows and footer schema before writing. Before native acceptance, Spark reads the recorded output snapshots and compares them bidirectionally with the selected normalized source frames using duplicate-preserving `exceptAll`, including provenance. A nonempty difference fails the candidate; only a matching manifest/snapshot conservation receipt permits acceptance. This distributed comparison remains an ingestion safeguard, not a second Spark validation application. StarRocks independently checks persisted per-day row counts against the complete manifest. Counts alone are never evidence of content equality. Measure comparison shuffle/spill and resources on v16 before making scale claims.

### 4. Native OpenMetadata owns discovery and execution results

Use the official ingestion image matched to the OpenMetadata server version, initially 2.0.6, scheduled as finite Argo container steps. Native StarRocks metadata discovery creates canonical table entities; native profiler and test-suite workflows own their results. Profile candidate data only under explicitly recorded scope; latest-state statistics are not snapshot-specific validation evidence.

Verify external catalog selection for metadata, profiler and test connections. The 2.0.6 connector uses MySQL-style discovery and has no explicit catalog selector: prove supported connection/session/default-catalog configuration on every connection, nested schema extraction and fully qualified SQL execution. A connector listing alone is insufficient. Resolve any missing support through supported upstream configuration/version support, not a custom discovery publisher, internal-table copy or hidden fallback. Record inability honestly before proceeding with acceptance.

Use discovered entity IDs/FQNs for products, contracts, lineage and readback. Retain logical source ports linked to immutable manifests rather than one asset per file. Native contracts retain original ODCS ID/version/hash, artifact reference and complete original YAML as provenance. Do not use the importer that rejected source environments/structured properties, or retain `dp1-compatibility` conversions. Native quality definitions reflect Git SQL; no custom GX test/result projection remains.

Argo HTTP templates perform the small set of explicit API operations for domain and asset membership, product, contract provenance and lineage with the Argo pipeline; writes run serially because they depend on each other. Configure Argo Agent/WorkflowTaskSet RBAC, one OpenMetadata bot secret and bounded HTTP retries. Do not recreate the removed Python client as a large YAML SDK. Acceptance readback of native results is a GET-only step in `dp1 accept`, because the results exceed Argo's output-parameter size limit. Native ingestion owns table schema, profiling and test execution. Argo lineage is explicit; do not assume Airflow instrumentation.

### 5. Acceptance requires complete results and durable evidence

A successful ingestion container exit does not mean checks passed: OpenMetadata can successfully execute a failed assertion. Read back the complete expected rule set and require each required result to be Success, matching run, contracts, source and both snapshot IDs. Failed, Aborted, missing, stale or incomplete results prevent acceptance. Persist bounded evidence and execution errors before signaling failure; optional failure samples remain bounded.

Keep data validation, execution and governance outcomes separate. API success or a healthy catalog alone is insufficient. Read back schemas, products, contracts, lineage and native quality results with bounded indexing retries. Metadata-only recovery uses the original evidence and outcomes without ingestion or validation reruns. Missing results remain execution errors, not invented passing/failed assertions.

Version acceptance by validation policy (`bronze-raw-v3` for new raw ingestion; earlier `bronze-starrocks-v2` evidence remains unchanged). Preserve v16's `result: skipped`, `gate: dp1-starrocks-openmetadata` declaration and reason, source contracts/product and feature-definition hash in run/evidence records. Source validation reports do not replace persisted-bronze checks. Retired GX gate/schema compatibility is not a current input path; old GX/native accepted records and previous failures remain historical and cannot satisfy current-contract acceptance.

### 6. Argo coordinates finite stages and safe cleanup

The DAG is input/run-context preparation -> operator-managed Spark ingestion -> native discovery/contract setup -> native SQL validation -> durable evidence/governance readback -> acceptance. Profiling is an explicit native workflow with bounded scope, not another Spark driver. Failure paths still persist and expose outcomes; deterministic bad data is not retried until it passes.

Retain bounded Argo retry authority, operator restart policy Never, the writer mutex and native admission guard rejecting active Workflow deletion/hard termination. Graceful stop and deadlines terminate Spark resources and verify cleanup before writer handoff. The validation queries are read-only, so StarRocks bounds them with a per-user `query_timeout` instead of explicit cancellation. If cleanup cannot be confirmed, the next writer must recover prior work under the mutex before ingestion. Terminal Workflow phase alone does not prove cleanup. Durable evidence survives deletion.

### 7. Platform ownership and resource budgets

Deploy StarRocks in `talos-proxmox`, reconciled by Argo CD using the official StarRocks Kubernetes operator and `kube-starrocks` Helm chart. Pin compatible chart/operator/server versions during implementation. The platform owns operator/CRDs, shared cluster, persistence, catalog connection, credentials and capacity. This repo owns workflow/SQL/contract definitions, OpenMetadata ingestion configuration, scoped workload identities and acceptance. Do not deploy Trino or an analytical ClickHouse service. Existing ClickHouse/HyperDX observability is unaffected.

Start with a bounded single FE/BE toy-project deployment with explicit durability/availability limitations and measured PVC/resource requirements; do not blindly apply large upstream defaults or claim HA. Retain the requested two one-core Spark executors, each 2 GiB heap plus 1 GiB overhead, and 2 GiB driver heap plus 512 MiB overhead as an initial ingestion profile. AQE and the 1,024 initial shuffle/64 MiB advisory settings remain provisional ingestion tuning. Remove validator-only persistence/cache settings when validation leaves Spark. Keep WARN/ERROR logging, Parquet/Iceberg formats and measured file sizing. Measure combined Spark, StarRocks and OpenMetadata capacity before scale execution.

### 8. Remove obsolete implementation and DP1 catalog artifacts

Remove the custom Python metadata HTTP client, PyIceberg discovery path, GX result/contract projection and dedicated `data-platform-metadata` image; retain any dependency genuinely needed by ingestion/recovery. Remove obsolete validation commands, Spark validation application, exporter/dependency/lock entries, image publishing jobs, charts, fixtures and config. No legacy flags, migration tools, old schema aliases or alternate publisher remain.

Delete only obsolete DP1-owned OpenMetadata services/table registrations, products/contracts, pipelines, GX definitions/cases/results and compatibility probes, notably `dp1-compatibility`. Recreate native assets cleanly. Remove old DP1 lake tables and run/evidence state as part of the authorized fresh start. Preserve unrelated shared metadata, authentication/backing services and the current immutable v16 source. This is scoped cleanup, not a wholesale OpenMetadata database wipe.

### 9. Current-contract v16 development, then later scale acceptance

Run native v16 development acceptance now using CI-built images, then exercise replay, snapshot isolation and recovery without waiting for a large source. Select the exact published manifest SHA-256 `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`, all 1,954 Parquet inventory files / 139,597,476 bytes / 795,718 events / 161,967 labels, including multi-date fragments. Purchase starts span July 1–September 28, 2025; complete event tails reach September 29 00:58:24 UTC and label-availability tails October 19 22:17:57.008 UTC. Never truncate to purchase dates or infer input selection from event-time maxima.

Use source product 6.0.1, events ODCS 4.0.1, labels 1.0.0 and enriched schema 3. Verify the manifest's `fraud-baseline-v1` SHA-256 `2ee2a5153a248bdc98df3b2643e10a65bc5693ea111129b6c65a67352aa4ec0b`. Preserve checkout-context equality, complete lifecycles, delivery multiplicity, label linkage and availability. Clean approvals may have empty reason codes; declines require reasons. Keep all generator distribution/cardinality bands diagnostic.

The archived `2026-10-26-finalize-feature-contract-and-90-day-smoke` source/live evidence is upstream acceptance only. V15's declared-rule failure remains failed evidence. V16's seven-day warmup, maturity-aware temporal splits, substantial entity overlap and missing stable campaign IDs constrain downstream evaluation; there is no model-quality claim. Live shutdown can lose in-memory delayed chargebacks. Feature/as-of join acceptance, training and serving remain separate.

After development ingestion/features/training/serving are exercised, separately select a fresh completed current-contract source meeting at least 100,000,000,000 physical inventory-listed Parquet bytes. No source generation is started or implemented here. Record resources and capacity failures honestly; never mark the coursework gate complete from v16.

### 10. Recurring ingestion uses the same boundary

Kafka-to-completed-landing belongs to the live-streaming change. DP1 consumes immutable completed files outside `synthetic-source/historical`, retaining origin, event/schema/time, supplied topic/partition/offset, input hashes and batch identity. Use one current environment/configuration for synthetic analytics/training rather than duplicate real/simulator deployments.

Agree completion/inventory and progress mechanics with the producer before implementation. Discover newly completed files even in older date folders; event-date maxima are not safe cursors. No-input runs are no-ops. Prevent retry-generated duplicates while retaining source-delivered duplicates. Events and labels may arrive independently; record changed and unchanged snapshots. DP2 evaluates history-dependent business rules and reports incomplete context; raw acceptance makes no lifecycle/linkage claim. Keep processing progress separate from governance acceptance so outages do not append data twice; unresolved predecessors cannot silently be bypassed. Do not promise cross-system exactly-once semantics.

### 11. Roadmap: StarRocks gold marts and materialized views

Retain Iceberg bronze/silver and reproducible feature inputs. StarRocks replaces the proposed analytical ClickHouse warehouse and Trino query engine. Model gold as explicit fact and dimension tables/views with declared grain, business/surrogate keys, join cardinality and SCD2 effective-time intervals. Columnar joins support star schemas; they do not automatically enforce business relationships or implement SCD2 history.

Prefer direct external-catalog queries before materializing selected gold joins/aggregates. Native StarRocks materialized views store derived data; account for storage, refresh compute, failures and lineage. External Iceberg views support fixed-interval or explicit refresh, not source-commit-triggered immediate refresh. Argo can request refresh after accepted upstream commits and wait for completion before advancing a gold acceptance reference; do not combine independent source snapshots without a recorded consistency boundary. Record freshness/lag and validate late-arrival correction, SCD2 changes and equal query results. Native ingestion into StarRocks is an optional later serving choice requiring a measured latency need, not a prerequisite for DP1 or a second mandatory copy of every table.

Gold transformations remain later DP2 work. Spark continues distributed ingestion/curation/features where appropriate; StarRocks SQL/MVs own analytical serving and suitable gold transformations. Neither engine speed nor real-time freshness is assumed without fixed-data/resource measurements. Training uses immutable snapshots/as-of labels, not an unversioned moving materialized view.

## Risks / Trade-offs

External-catalog discovery and nested types need end-to-end verification with the selected OpenMetadata/StarRocks versions. Snapshot-specific SQL and complete result correlation must remain intact across native execution. StarRocks FE/BE adds persistent resources; combined capacity and refresh cost need measurement. Content conservation cannot be weakened to row-count equality. Materialized-view freshness differs from lake acceptance and must be visible. Coursework acceptance needs equivalent governance and benchmark evidence; tool selection alone proves neither.

## Verification

Reuse existing CI entrypoints and behavioral fixtures for the distinct risks: normalization/conservation, uncertain commits/replay, snapshot isolation, nested SQL/linkage/label semantics, missing/failed/stale native results and lifecycle recovery. Delete tests for removed behavior. Do not add config-only tests, option matrices, new CI jobs or per-property assertions. Tests and images run only in CI; local checks are formatting/schema/OpenSpec validation and diff review. No live services or credentials in ordinary CI.

## References

- [StarRocks Iceberg catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/) and [SigV4 configuration](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_rest_s3/).
- [Iceberg time travel](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel/).
- [Materialized-view support and external refresh limits](https://docs.starrocks.io/docs/using_starrocks/async_mv/feature-support-asynchronous-materialized-views/).
- [Official Kubernetes deployment](https://docs.starrocks.io/docs/deployment/helm/).
- [OpenMetadata 2.0.6 StarRocks connector](https://github.com/open-metadata/OpenMetadata/tree/2.0.6-release/ingestion/src/metadata/ingestion/source/database/starrocks).

### Raw landing selection and daily execution

Use `bronze-raw-v3` in a separate evidence prefix. Native schema type and conditional-required checks are bundled per table; profiling is optional and disabled for recurring runs. The original 146-check v16 acceptance remains evidence under its original policy.

List immutable `streaming/_batches/epoch=<epoch>/` completion manifests, selecting only clean event/label topics without a business-date cutoff. Freeze a bounded selection before writes; record exact manifest digests, file inventory and disjoint topic/partition offset ranges. A durable epoch ledger advances only after native/governance acceptance. Pending selections recover before newer input. Deterministic selection IDs partition raw deliveries independently; retries recover the same commits and bidirectionally reconcile only that batch, including original Kafka bytes, key and ordered headers. Unchanged ports retain their prior snapshot; no-input skips Spark/native work. The daily UTC CronWorkflow shares the historical writer mutex and cleanup guard. Producer offsets and simulator state are unchanged.
