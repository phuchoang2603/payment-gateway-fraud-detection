# Design

## Context

See proposal.md for the integration gaps. Live readback found empty native contract schema/quality references, a pipeline with zero tasks and stale v11 execution metadata, missing owners, and fabricated events-to-labels ingestion lineage. Table-side product membership exists, so a UI drawer showing no product needs separate verification. Prior data checks do not establish complete catalog integration.

## Goals / Non-Goals

Make current raw data, contracts, ownership and execution provenance inspectable through supported OpenMetadata models, and keep the Argo stage graph readable. Reset scoped DP1 state without retaining compatibility paths or importing old acceptance.

No source generation, Kafka/landing reset, silver business validation, gold mart implementation, replacement Iceberg catalog or shared infrastructure wipe belongs to this change. Scale evidence and deliberate outage/failure exercises are not completion gates.

## Decisions

### 1. Retain the raw ingestion and SQL boundary

Spark 4.1.3/Iceberg 1.12.0 normalizes current schema-3 events and schema-1 labels without losing nested nulls, UTC microseconds, business fields or transport provenance. Preserve identical deliveries. Verify every selected immutable file's SHA-256, bytes, rows and schema with bounded workers; never substitute ETags for checksums. Pin source contracts/product/feature hashes and the original deferred validation declaration.

Persist immutable input/run identity before writes. Serialize product writers and recover uncertain commits; two table commits are not a transaction. Record independent changed/unchanged snapshots. Require bidirectional duplicate-preserving `exceptAll` against normalized source and recorded output snapshots, including provenance. Row counts alone do not prove content conservation.

Platform-owned StarRocks reads the existing RustFS REST catalog with native SigV4 signing service/region and separately configured S3 endpoint/credentials. No proxy, copied internal table or alternate engine fallback. SQL uses explicit `FOR VERSION AS OF` references for each output. The six bundled raw assertions remain Git-derived native COUNT-of-violations checks. DP2 owns duplicate business consistency, lifecycle, amounts/risk/context, linkage and label maturity/as-of joins; retained silver SQL does not imply acceptance.

### 2. One thin metadata image with clear responsibility

Build on the exact official OpenMetadata ingestion version matched to the server, initially 2.0.6, pinning the final image digest. Keep native StarRocks metadata, profiler and test-suite workflows and their native results. Use the official SDK for explicit product/contract mapping, ownership, source lineage, pipeline task/execution publication and readback. Keep this reusable data-platform metadata runtime free of Spark/JVM dependencies. Share the official SDK client, secret-safe configuration and native workflow runner across product-specific publication modules; keep DP1 contract/run/acceptance mapping in its own module. DP2/DP3 can add their publication modules to this image and invoke them through Argo without duplicating SDK plumbing. Separate pure contract mapping from modules importing PySpark.

Replace individual HTTP nodes and request-generation glue with one idempotent SDK synchronization path. Do not keep a second HTTP publisher or compatibility wrapper. Retain native discovery as the sole table identity authority and native workflows as the sole test/profile result authority. If supported external-catalog selection, nested schemas or a required API field fail, report incomplete integration; do not invent entities or fall back to custom discovery/results.

### 3. Map native contracts and products completely

Resolve canonical table IDs after native discovery. Map ODCS normalized nested schema into native contract schema, and active raw test-case IDs into quality expectations. Resolve declared team/user ownership rather than fabricating users or silently dropping an unresolved required owner. Map supported declared semantics, terms, source URL and security/SLA fields; do not invent absent guarantees. Preserve complete original YAML, ID/version/hash and artifact references as provenance, including supported ODCS extension fields where needed.

Contracts bind one entity. Product source dependencies and execution inputs/outputs express flow; do not invent native contract input/output properties. Synchronize source and bronze products, domains, assets and pipeline membership. Read back product asset references and entity-side membership, owners, nested schema and current test references; embedded YAML/hash alone is insufficient. Inspect the UI separately from API membership before claiming the drawer discrepancy fixed.

### 4. Record real source lineage and current executions

Represent logical historical and clean-landing source paths with supported native storage-container assets, without one entity per file. Discover/register actual storage paths through the supported storage model and resolve canonical references. Retain exact immutable manifests/batches in execution evidence. Each source port connects through the ingestion pipeline to its own bronze output; events are not an ingestion source for labels. Declared label/event contract dependence remains contract semantics for silver.

Publish pipeline task definitions and dependencies matching the visible Argo stages, declared owners/products, source URL and current execution ID/times/status. Publish supported task/execution inputs, outputs and metrics identifying selected source assets, row counts and independently pinned output snapshots. Store supplemental immutable identities in supported provenance fields or durable linked receipts. Do not claim unsupported native fields were populated. A failed run records the real stage outcome; metadata recovery must not invent a new successful data execution or replace original native result timestamps.

### 5. Compact Argo graph with bounded parallelism

Use a top-level DAG with select -> ingest -> discover -> validate/profile in parallel for historical runs -> publish/readback/accept. Express dependencies directly, reuse stage templates and provide readable supported display names. Keep API-level details in container logs instead of separate graph nodes. Preparation may return replay or no-input; no-input skips Spark, discovery, validation and profiling, while metadata-only recovery reuses original snapshots/results without ingesting or validating again.

Validation and profiling use isolated per-run configurations, native work directories and bounded resources/connections, sharing immutable candidate context. Join their declared outcomes before acceptance; a required profile failure is visible and cannot be hidden by SQL success. No concurrent mutation of test definitions or snapshot selection. If resource measurements require serialization, preserve the same stage dependencies and record the capacity limit.

Keep bounded Argo retry authority and Spark restart policy Never, the writer mutex, deadlines, stop-before-delete admission guard and exit cleanup. Graceful stop terminates Spark and verifies driver/executor cleanup. Bound and identify StarRocks queries, cancel remaining run-owned work where supported and verify query quiescence before handing off the writer. A timeout alone is not evidence that a query has stopped. Unconfirmed cleanup blocks handoff and subsequent ingestion until recovery under the mutex completes.

### 6. Meaningful bounded profiling

Enable native scalar-column metrics supported by the selected StarRocks connector, including useful null/distinct/range statistics where supported; exclude binary Kafka payloads and unsupported nested metrics. For v16, request bounded meaningful column profiling rather than only row/column counts. Declare sampled versus full coverage, eligible columns, row/sample limits, timeout, threads and observed cost. Daily recurring ingestion skips profiling explicitly. A separate on-demand Argo entrypoint collects bounded profiles when requested; no-input runs do not profile.

Record whether a profile describes latest catalog state or an explicitly supported snapshot scope, its collection time and sampling. Do not attach unscoped latest-state statistics as proof about the candidate snapshot. Native profiling supplements raw acceptance; it does not move expensive business-quality checks into DP1 or imply distribution/model-quality acceptance.

### 7. Strong acceptance and metadata-only recovery

Require complete expected native Success results matching run/source/contracts/policy and both snapshots, plus exact conservation. A zero container exit is insufficient; Failed, Aborted, stale, missing or incomplete results refuse acceptance. Read back native contract fields, active test references, ownership, product forward/reverse membership, real source edges, current task/execution I/O/status and declared profile coverage with bounded indexing retries.

Record data, execution, profiling and governance outcomes separately. Persist bounded receipts before failure. SDK retries address stable entities/executions without duplicating identities or changing data outcomes. Metadata-only recovery retains immutable context, native results and progress; acceptance advances only after complete readback. The rewritten governance policy has a fresh explicit identity and evidence namespace; old accepted records cannot short-circuit it. No old policy replay/alias is implemented.

### 8. Scoped fresh start without migration

Inventory exact DP1-owned catalog entities, bronze tables/warehouse locations, evidence prefixes and pending/accepted progress before deletion. Suspend the DP1 CronWorkflow and stop active writers gracefully; verify Spark and run-owned StarRocks work are quiescent under writer serialization. Delete inventoried obsolete DP1 artifacts, old suites/cases/results, incorrect lineage, stale executions, old DP1 lake data and run/acceptance/progress records; remove superseded repository receipts and their affected references. Do not retain compatibility schemas, migration utilities, dual publishers or imported acceptance.

The reset boundary excludes the RustFS Iceberg REST catalog service, shared StarRocks/OpenMetadata/authentication/backing services, unrelated catalog/lake assets, all simulator-owned historical sources including v16, completed streaming source objects, Kafka offsets and producer state. An unresolved ownership boundary stops deletion of that resource. Scope clarification precedes destructive operations; planning changes do not imply a reset has occurred.

Recreate current assets through native discovery and the single SDK path; use a clean acceptance namespace and explicit pending input selection. Preserve real pass/fail descriptions when reporting prior observations, but remove old DP1 data/evidence rather than importing them into current acceptance. New receipts describe only the fresh run. No rollback to the retired publisher is planned; fix forward from immutable source if acceptance fails.

### 9. Fresh development acceptance and later scale

Use the exact v16 manifest/hash and contract identities in proposal.md. Include all multi-date inventory fragments and complete event/availability tails, not only purchase dates. Source/live evidence is upstream acceptance and does not establish persisted-bronze governance. Re-run historical and recurring raw acceptance after the rewrite/reset, then replay, metadata-only recovery and no-input behavior. Verify rendered Argo usability and OpenMetadata contract/pipeline/profile views as well as API state.

Recurring input comes only from immutable clean `streaming/_batches/epoch=<epoch>/` manifests. Freeze bounded selection/digests and disjoint transport ranges; late files in older date folders remain discoverable. A durable epoch ledger advances only after full acceptance; recover pending selections before new inputs. Preserve original Kafka bytes/key/ordered headers and independently arriving labels. No producer offsets are reset. Daily UTC scheduling remains explicitly enabled only after fresh readiness is established.

Start with two one-core executors (2 GiB heap + 1 GiB overhead each), driver 2 GiB + 512 MiB overhead; sizing and shuffle settings remain provisional. Retain WARN/ERROR source logging and Kryo configuration where applicable. Measure combined Spark/StarRocks/metadata cost before scale claims. Later >=100,000,000,000-byte acceptance follows the whole pipeline. No generator is started.

Gold fact/dimension grain, keys, SCD2/as-of joins and selected StarRocks MVs remain DP2 roadmap work. External Iceberg MVs refresh on schedule or explicit request and store derived data. No Trino or analytical ClickHouse is added; observability ClickHouse is unrelated.

## Risks / Trade-offs

- SDK/server or external-catalog mismatch: pin matched versions and verify native connections/schema/results rather than substituting custom discovery.
- Incomplete native field/UI support: inspect the selected schema and UI; report unsupported required mapping as incomplete governance.
- Parallel scans exceed capacity: bound profile scope/concurrency and measure; preserve exact conservation and raw SQL checks.
- Reset affects shared assets: inventory ownership/locations, pause writers and verify quiescence; never delete broad shared buckets or service databases.
- Stale acceptance masks missing integration: use fresh policy/evidence identity and require current contract, execution, lineage and profile readback.

## Verification

Reuse existing CI behavioral fixtures for mapping gaps, unresolved owners, incorrect edges, stale execution I/O, missing/stale native results, idempotent SDK replay, profile scope and cleanup. Delete superseded HTTP-publisher coverage. Reuse the existing image workflow for the thin image; no redundant config-only suites, matrices or new check jobs. Tests and builds run only in CI. Local work is lint/format/schema/OpenSpec validation and diff review. Deliberate live failure/outage exercises and scale runs remain excluded.

## References

- [OpenMetadata 2.0.6 contract API schema](https://github.com/open-metadata/OpenMetadata/blob/2.0.6-release/openmetadata-spec/src/main/resources/json/schema/api/data/createDataContract.json).
- [OpenMetadata 2.0.6 pipeline schema](https://github.com/open-metadata/OpenMetadata/blob/2.0.6-release/openmetadata-spec/src/main/resources/json/schema/entity/data/pipeline.json).
- [OpenMetadata native StarRocks connector](https://github.com/open-metadata/OpenMetadata/tree/2.0.6-release/ingestion/src/metadata/ingestion/source/database/starrocks).
- [StarRocks Iceberg REST catalog](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_rest_s3/) and [time travel](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_timetravel/).
