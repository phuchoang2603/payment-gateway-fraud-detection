# Design

## Context

See [proposal.md](proposal.md) for motivation and scope. The current accepted dataset is `historical/v7`: 420,392 payments and 2,215,309 exported event rows, with about 237 MiB of event Parquet. This is correctness evidence, not a scale measurement.

The present generator has several independent scaling limits:

- `storage.generate` writes all intermediate Parquet locally, compacts it locally and validates it before any upload. `_upload` receives `Path.read_bytes()` for each whole file.
- `profiles.schedule_for_day` creates and sorts arrays for all purchases on a day. `build_profiles` builds Python string pools and repeatedly samples weighted merchants per customer; duplicating this state per process would multiply its cost.
- `DatasetStatistics` retains exact Python sets for every observed identity. Newly generated device and method fingerprints can grow these sets with traffic, beyond the configured pool sizes.
- `_compact` limits DuckDB memory, but `_validate` opens a separate connection without the same resource controls. Lifecycle checks use full-history distinct/window operations; label linkage joins both datasets across dates.
- `card_testing.rate` means expected bursts per day, whereas compromise rates mean per-customer daily probabilities. A uniform multiplier would change fraud prevalence incorrectly. The IPv4 network pool is intentionally capped at 65,536 `/24` networks within `10.0.0.0/8`.

The existing historical-payments ODCS contracts already express the required lifecycle, duplicate and delayed-label semantics. CardFraudPlatform's `generator/generate_scale.py` demonstrates independent day/chunk work, upload-and-release and a separate scale dataset; its simpler transaction records and throughput are not estimates for this generator. RecSys supplies pipeline patterns but no inspected proof of a 100 GB generation run.

The coursework source inspected for this plan is `EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5; it says offline simulation is at least 100 GB. Cell C8 asks for persisted source data before bronze ingestion; streaming starts separately at B9. The project [roadmap](../../../docs/overview/roadmap.md#references) links the published rubric. Neither the workbook wording nor the references establish whether its volume is compressed or uncompressed.

## Goals / Non-Goals

**Goals:** generate a large fixed dataset from a workstation using bounded active work and remote storage; recover completed work; retain exact source-contract publication gates; make CPU, memory, scratch and network costs measurable before committing to the full run.

**Non-Goals:** distributing generation through Spark or Kubernetes, rewriting ODCS SQL for another engine, a second GE rule suite, online fault injection, DP1, Iceberg tables, DataHub, or a guarantee that any laptop can validate 100 GB within an arbitrary small scratch budget. A larger validation host uses the same CLI and run state, not a separate platform stack.

## Decisions

### 1. One generator with a separate scale configuration

Extend `simulator historical` rather than add a second generator implementation. Keep `configs/historical.yaml` as the v7 record. Add `configs/historical-scale.yaml`, and create an explicit pilot configuration derived from it under a distinct dataset version. A pilot spans at least six generation days with an interior schema cutover and all resulting delayed labels; it uses the intended profile/entity scale so setup and cardinality costs are represented.

Separate logical inputs from runtime controls. Logical inputs include seed, dataset version, integration, dates, profile counts, behavior/scenarios, deterministic task granularity, layout and minimum accepted bytes. Runtime controls include workers, maximum in-flight tasks, scratch path/capacity, DuckDB memory/threads/spill capacity, destination and transfer concurrency. Add a generation identity hash over logical inputs plus algorithm/contract versions; retain the full normalized configuration and its hash for provenance. Resuming with different runtime controls records the new execution attempt without changing generation identity.

The normal `historical --config` command automatically selects new generation, compatible resume or completed-publication recovery. It cleans intermediates after durable publication and successful accepted-object protection. `--prepare-only` stops after all final Parquet files and their verified inventory exist, without a completion manifest; the normal command continues the recorded phase on the same or another host. Active ownership still refuses continuation, and crash takeover requires stopping the former writer and passing `--takeover-stopped-writer`.

### 2. Deterministic task streams and shared profiles

Use fixed logical tasks identified by generation day and customer block, plus separately identified card-testing burst groups. Derive stable purchase identities and RNG streams from the algorithm version, seed and logical identity, never from process IDs, wall clock or worker completion order. Purchase batches inside a task and worker concurrency are execution details. Preserve shared identity-vector derivation and the ability to rebuild a day's profiles/scenarios without replaying preceding days.

Replace whole-day schedule construction with bounded block schedules. Stable ordinals can come from deterministic block counts/prefixes; final global time ordering belongs to compaction. Keep each payment history in one task, including its retries, recovery and labels. Generate a card-testing burst as one logical group so its shared device and many distinct methods survive batching.

Build compact profile arrays once into read-only, memory-mapped host scratch, reopen them in workers, derive string IDs/fingerprints lazily and hash profile contents incrementally. Batch weighted sampling while preserving distinct familiar merchants per customer; avoid rebuilding a merchant probability distribution for every sample. The new algorithm may change generated rows and profile digests. Reject incompatible continuation rather than preserve two generators; old datasets and identity vectors remain untouched.

The parent bounds submitted tasks and collects only task receipts and additive counters. Begin with one worker; increase concurrency only after measuring process-tree RSS and scratch usage. Memory-mapped storage still consumes scratch and resident pages, so it is part of the budget and evidence.

### 3. Remote intermediate files, then bounded daily finalization

Use `historical/<version>/_run/` for versioned run state, task receipts, intermediate objects and diagnostic reports. `manifest.json` remains the sole consumer completion marker. Intermediate data is never enumerated as source data by consumers.

A worker writes a bounded task's event and label fragments, streams them to RustFS and releases local files after a verified receipt is durable. Group intermediate events by physical schema version; avoid creating a tiny fragment for every label-availability date. Receipts record object key, size, checksum, rows, schema version and time bounds. Stream hashes/uploads through bounded buffers, with explicit transfer concurrency. Do not interpret a multipart ETag as a content hash.

After all tasks finish, finalize one occurrence-date or label-availability-date partition at a time. Read the exact receipt-selected remote fragments, prune by time bounds, apply the existing deterministic sort, and produce the existing Zstandard, row-group-sized files. Upload finalized files and their inventory before reclaiming their scratch. Preserve rows and duplicates; do not concatenate already sorted chunks and call the result globally sorted. Daily compaction and spill must fit the declared scratch budget or fail resumably.

No dataset-wide local Parquet staging remains. Uploaded intermediate objects increase remote temporary storage, so report their peak bytes separately. A task writes one fragment per table and physical schema version and appends each purchase batch as a row group, so object count tracks tasks rather than batches. Completion reuses a conditional update of existing run-state objects.

### 3a. Bounded publication footprint

A version's object count and its leftovers after publication are part of the resource contract, not an afterthought. Three controls keep the bucket shaped like the accepted datasets only.

A configuration that declares `accepted_version` tags the manifest and every final object `accepted=true` at publication; pilots and candidates are published untagged. The storage policy can then deny overwrite and deletion of tagged objects, so immutability is enforced by the bucket rather than by operator discipline, it applies to any future version without naming one, and abandoned candidates stay removable.

The normal command removes `_run/` fragments, task receipts and partition receipts once the manifest is durable. Cleanup runs after publication, never before, so a failed delete cannot damage an accepted dataset; the outcome is recorded in a one-object `_run/publication.json` receipt with object and byte counts. Tagging or cleanup failure exits nonzero and leaves the version published. The receipt binds the manifest digest. Rerunning the same command verifies the completed inventory and logical configuration, reads existing tags to skip already protected objects, preserves unrelated tags and retries protection before cleanup. It does not rewrite final files or the manifest. Cleanup retains the receipt, examines S3 per-object errors even on HTTP success, and accumulates only successfully removed objects and bytes across retries. This requires read-tag permission as well as write-tag permission.

Probe and check datasets caused most of the historical bucket sprawl, because the same CLI published them under their own version names. Publication now refuses a dataset smaller than one configured customer block unless the operator passes `--allow-small-dataset`, and probes that need no remote evidence use `--output` on a local directory. The source identity therefore needs deletion permission only for `_run/` prefixes and abandoned probe/pilot prefixes, never for accepted data.

### 4. Resume verifies data and enforces a single writer

Task and final-partition receipts are durable only after their referenced uploads succeed. Verify checksums and lengths on resume; where RustFS cannot provide a trustworthy object checksum, stream and hash the object. An upload acknowledged remotely but not checkpointed can be adopted only after verification or regenerated. Truncated files and bare object existence never count as completed work.

Use conditional S3 writes for run ownership, checkpoint updates and completion publication; local output uses equivalent exclusive/atomic filesystem operations. Add a directly pinned S3 SDK only for these concrete needs and bounded transfers. Before implementation relies on it, verify RustFS supports the chosen create-if-absent and ETag-conditional update semantics using disposable keys. Refuse unsafe resume/publication if those primitives are unavailable.

Run ownership has no automatic timeout takeover. Normal shutdown releases it through a conditional state transition; after a crash, explicit recovery requires the operator to stop the former writer before replacing its ownership token conditionally. Check the current token before checkpointing or publishing. Store task identity, algorithm, contracts, profile digest and checksums, never credentials or session tokens. All participants treat published versions as immutable; publication uses create-if-absent so a competing completion cannot overwrite a manifest.

### 5. Reuse native ODCS validation with one controlled connection

Use the existing DuckDB SQL contracts unchanged. The installed Data Contract CLI 1.2.7 accepts a caller-owned `duckdb_connection`; use that API so memory, threads, spill path/cap and session-scoped S3 credentials also apply to native checks. The existing sanity notebook already reads source Parquet remotely through DuckDB. Verify this exact connection path against both contracts and nested schemas before the pilot.

Create `payment_events` and `fraud_labels` views over the inventory of finalized files, with `union_by_name` where needed and `hive_partitioning=false`. Use the complete immutable inventory, including payment transitions and labels outside the configured generation dates. Read remote Parquet with range requests, not by downloading the dataset or collecting it into Python. Secrets remain session-only and come from the environment. Pin the remote-read extension/version in the reproducible environment if required; never persist credentials in the validation database or report.

Run both full native contracts, store their reports and then check final source checksums/counts and the byte gate before completion. The inventory must not change between validation and manifest publication. A failed check, memory error or full spill disk leaves the run incomplete and resumable. A resumed validation attempt reruns both checks over the unchanged full inventory; it does not trust a partial previous success or replace full validation with sampling.

DuckDB's `memory_limit` is not a process-wide cap, and not every blocking operation can spill. Measure total memory and query spill; if the calibrated workload exceeds the workstation's declared validation budget, prepare there and resume validation on a larger host. Do not promise Spark validation from this change. A failed full-scale validation remains a failed acceptance result. The `v8-pilot` measurement on 2026-10-22 is the sizing datum: 4,843,810,164 uncompressed bytes needed 8,421,998,592 spill bytes, or 1.74 spill bytes per uncompressed byte, which puts the 365-day plan near 512 GB of spill. Budget validation capacity from that ratio before scheduling the full run. Because every native check is a per-payment invariant that the pilot already satisfied and that the downstream Spark and Great Expectations pipeline re-tests, the full run explicitly selects `validation_gate: dp1-spark-gx` in configuration. This keeps the checksum, partition row-count and byte gates, records `result: skipped`, `gate: dp1-spark-gx` and a reason in the manifest, and tags accepted output only when the configuration explicitly declares `accepted_version: true`. Native validation remains the default for configurations without a gate setting. The DP1 consumer explicitly recognizes this declaration only with an embedded inventory, preserves it in run/report evidence and still requires both persisted tables to pass Spark/GX plus verified governance before bronze acceptance. Source publication alone does not establish data-quality acceptance.

### 6. Replace unbounded statistics without weakening quality checks

Keep additive row/scenario/duplicate totals and bounded merchant/category counters in task receipts; combine each verified task once during resume. Compute distinct-identity summaries over final Parquet with DuckDB's bounded approximate aggregate for scale reports, identify the method as approximate and state each ratio's denominator. Small exact fixture checks establish expected semantics; exact distinct counts can be an explicitly budgeted diagnostic.

Approximation is only for descriptive cardinality statistics. Contract checks for event conflicts, lifecycle validity, duplicate identity and label linkage remain exact. Keep generator policy `sim-rules-v1`, separate scenario truth and `label_available_at`; runtime throughput optimization must not change payment rules.

### 7. Calibrate existing data problems instead of adding new ones

- **Volume and entity diversity:** scale customers and device/address/method pools together with purchases. Merely raising purchases per customer reduces distinct-to-row ratios and changes behavior. Increasing history duration is allowed within a documented plausible scenario, but it increases runtime and label partitions. Select a fixed combination from the pilot, not a byte-triggered loop.
- **Merchant/category skew:** retain explicit Zipf parameters, initially 1.1/1.2 from v7. Report top-1/top-10 merchant shares and category shares and declare acceptable bands before the full run. More merchants alone does not guarantee the intended observed skew because customers prefer familiar merchants.
- **High cardinality:** calibrate observed device/address/method cardinalities and their ratios, including new identities created by compromise and card-testing scenarios. Keep the existing IP-network bound; do not multiply it beyond 65,536 or change the fingerprint scheme just to create more bytes. Use the other keys to demonstrate high cardinality.
- **Schema evolution:** retain the physical `user_agent_family` cutover near the middle of the chosen history, with substantial rows on both sides. Check occurrence dates of transitions crossing midnight, not only the purchase start date.
- **Offline duplicates:** retain 2% identical exported-event rows. Resume must not add extra duplicates. Do not inflate duplicate rate to hit the byte target.
- **Fraud scenarios:** retain per-customer compromise probabilities, their windows, high-value probability and friendly-fraud noise as behavioral controls. Calibrate `card_testing.rate` separately from expected ordinary purchases per day to retain its intended attempt share; the chosen result remains an explicit bursts/day value. Keep burst size/duration meaningful. Record attempt-level truth proportions as well as existing payment-level counts.
- **Delayed labels:** retain 7–21 day delays initially and export all labels, including those beyond history end. Never compress label delays to simplify the size run. Burst arrivals in a synthetic history are business scenarios; transport lateness and duplicate delivery remain the live simulator's separate work.

The six-day pilot includes the warmup of seven-day stolen-card windows. A bounded first-block calibration on days 30–35 estimates a 2.67% stolen-card attempt share, so the full config uses a 2.0–3.2% band while the unchanged pilot retains 0.8–1.8%. This is subset calibration; full-run observations remain required.

The pilot report proposes concrete counts, traffic and card-testing rates and their acceptance bands, then the full config freezes them under a new version. These tuning values are intentionally measured rather than invented in this plan. If changing them materially alters compression or profile costs, repeat the pilot.

### 8. Explicit size accounting and measured acceptance

For this change choose `minimum_source_parquet_bytes: 100000000000` in the full scale configuration, zero in a pilot. This is a conservative project target for 100 decimal GB of physical source files, not a claim that the coursework specified compressed bytes. Sum each final event/label file once; exclude intermediates, state, reports, metadata, S3 replicas, streaming and all bronze/silver copies. Also report summed Parquet column-chunk uncompressed bytes, raw byte counts and GB/GiB conversions; call that metric uncompressed Parquet data, not an in-memory Python size.

Estimate planned payments using pilot physical bytes per payment, allowing measured variation across days and schema versions. Report projected generation/validation time, output storage, remote intermediate storage and upload time from observed throughput. A fixed plan that misses the byte target does not publish; change the logical plan under a new version and retain the failed run's measurements. Do not append arbitrary rows until the size counter passes.

The full acceptance run records wall time, process-tree peak RSS, scratch/spill peaks, bytes transferred, counts, distribution summaries, checksums and both table validation reports, naming the engine and gate. It must complete source generation and full-data contract validation, either source-native or the explicitly deferred DP1 Spark/GX path over pinned persisted bronze snapshots; a pilot, projected duration or benchmark over selected columns does not establish full-run acceptance. Keep CI on small fixtures.

## Risks / Trade-offs

- [Remote rescans and staging cost] → Record network/storage overhead; receipts prune compaction inputs and avoid one object per label date. Storage-owner retention is separate because source credentials cannot delete objects.
- [Full-history validation exceeds a small host] → Bound memory/spill, retain prepared data and support explicit handoff. A larger host may be needed; no automatic deployment or unmeasured completion guarantee.
- [Algorithm changes break old live continuation] → Version manifests, preserve identity vectors and immutable v7, fail on digest mismatch and generate a fresh continuation dataset with the new algorithm.
- [A larger dataset loses meaningful skew/cardinality/scenario mix] → Declare bands from a representative pilot and compare the final dataset; retain the 2% duplicate rate and the distinction between absolute and per-customer scenario rates.
- [Object-store conditional-write behavior differs from AWS] → Run a bounded compatibility check before relying on the protocol; stop rather than downgrade writer exclusivity.
- [Abandoned ownership after a crash] → Require explicit takeover after the previous process is stopped; never infer that elapsed time alone makes a second writer safe.

## Migration Plan

1. Add the new algorithm, configuration/run-state versions and bounded execution path; replace full local staging, growing identity sets and whole-file uploads rather than maintain a second scale implementation.
2. Reuse and extend the small simulator tests. Keep v7 files/configuration immutable; update new consumers of manifest statistics and reject unsupported continuation algorithms.
3. Verify remote-read/resource propagation and conditional writes with small disposable output in the existing source service. Run the representative pilot, record limits and freeze the full scale config.
4. Execute the explicitly configured large run, including full-data validation through the declared source-native or DP1 gate. Update only the affected existing data-product guide, sanity notebook and roadmap evidence after recording observed results. Explain the exact workbook cell and the project's chosen byte accounting there.

Rollback stops the current writer and leaves its output incomplete without a manifest. Consumers continue using the last accepted version. A previous binary cannot resume new run-state formats and must refuse; restoring source does not require rewriting accepted datasets or deleting remote output.
