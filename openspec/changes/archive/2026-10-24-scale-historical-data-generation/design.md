# Design

## Context

See [proposal.md](proposal.md) for motivation and scope. The historical accepted correctness dataset documented here is `historical/v7`: 420,392 payments and 2,215,309 exported event rows, with about 237 MiB of event Parquet. This is correctness evidence, not a scale measurement.

The original pre-scale generator had several independent scaling limits (historical motivation; bounded generation has since been implemented):

- `storage.generate` writes all intermediate Parquet locally, compacts it locally and validates it before any upload. `_upload` receives `Path.read_bytes()` for each whole file.
- `profiles.schedule_for_day` creates and sorts arrays for all purchases on a day. `build_profiles` builds Python string pools and repeatedly samples weighted merchants per customer; duplicating this state per process would multiply its cost.
- `DatasetStatistics` retains exact Python sets for every observed identity. Newly generated device and method fingerprints can grow these sets with traffic, beyond the configured pool sizes.
- `_compact` limits DuckDB memory, but `_validate` opens a separate connection without the same resource controls. Lifecycle checks use full-history distinct/window operations; label linkage joins both datasets across dates.
- `card_testing.rate` means expected bursts per day, whereas compromise rates mean per-customer daily probabilities. A uniform multiplier would change fraud prevalence incorrectly. The IPv4 network pool is intentionally capped at 65,536 `/24` networks within `10.0.0.0/8`.

The existing historical-payments ODCS contracts already express the required lifecycle, duplicate and delayed-label semantics. CardFraudPlatform's `generator/generate_scale.py` demonstrates independent day/chunk work, upload-and-release and a separate scale dataset; its simpler transaction records and throughput are not estimates for this generator. RecSys supplies pipeline patterns but no inspected proof of a 100 GB generation run.

The coursework source inspected for this plan is `EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5; it says offline simulation is at least 100 GB. Cell C8 asks for persisted source data before bronze ingestion; streaming starts separately at B9. The project [roadmap](../../../docs/overview/roadmap.md#references) links the published rubric. Neither the workbook wording nor the references establish whether its volume is compressed or uncompressed.

## Goals / Non-Goals

**Goals:** generate a large fixed dataset from a workstation using bounded active work and remote storage; recover completed work; retain source integrity/byte publication gates and exact downstream contract acceptance; make CPU, memory, scratch and network costs measurable before committing to the full run.

**Non-Goals:** distributing generation through Spark or Kubernetes, changing logical ODCS contracts, online fault injection, implementing DP1/Iceberg/DataHub or permanent legacy migration in the simulator. The one-time `v8` handoff runs separately after stopping its old finalizer. DP1 Spark/GX processing is the downstream quality boundary. Earlier native validation measurements remain historical evidence, not requirements for the new path.

## Decisions

### 1. One generator with a separate scale configuration

Extend `simulator historical` rather than add a second generator implementation. Keep `configs/historical.yaml` as the v7 record. Add `configs/historical-scale.yaml`, and create an explicit pilot configuration derived from it under a distinct dataset version. A pilot spans at least six generation days with an interior schema cutover and all resulting delayed labels; it uses the intended profile/entity scale so setup and cardinality costs are represented.

Separate logical inputs from runtime controls. Logical inputs include seed, dataset version, integration, dates, profile counts, behavior/scenarios, deterministic task granularity, layout and minimum accepted bytes. Runtime controls include workers, maximum in-flight tasks, scratch path/capacity, destination and transfer concurrency. Add a generation identity hash over logical inputs plus algorithm/contract versions; retain the full normalized configuration and its hash for provenance. Resuming with different runtime controls records the new execution attempt without changing generation identity.

The normal `historical --config` command automatically selects new generation, compatible resume or completed-publication recovery. It generates/uploads raw files, verifies checksums and the byte gate, writes the manifest last, then tags configured accepted output and cleans only `_run/`; persistent `raw/` files remain. `--prepare-only` stops after generated raw Parquet files and their verified inventory exist, without a completion manifest; the normal command continues the recorded phase on the same or another host. Active ownership still refuses continuation, and crash takeover requires stopping the former writer and passing `--takeover-stopped-writer`.

### 2. Deterministic task streams and shared profiles

Use fixed logical tasks identified by generation day and customer block, plus separately identified card-testing burst groups. Derive stable purchase identities and RNG streams from the algorithm version, seed and logical identity, never from process IDs, wall clock or worker completion order. Purchase batches inside a task and worker concurrency are execution details. Preserve shared identity-vector derivation and the ability to rebuild a day's profiles/scenarios without replaying preceding days.

Replace whole-day schedule construction with bounded block schedules. Stable ordinals can come from deterministic block counts/prefixes; source publication promises neither global ordering nor compaction. Keep each payment history in one task, including its retries, recovery and labels. Generate a card-testing burst as one logical group so its shared device and many distinct methods survive batching.

Build compact profile arrays once into read-only, memory-mapped host scratch, reopen them in workers, derive string IDs/fingerprints lazily and hash profile contents incrementally. Batch weighted sampling while preserving distinct familiar merchants per customer; avoid rebuilding a merchant probability distribution for every sample. The new algorithm may change generated rows and profile digests. Reject incompatible continuation rather than preserve two generators; old datasets and identity vectors remain untouched.

The parent bounds submitted tasks and collects only task receipts and additive counters. Begin with one worker; increase concurrency only after measuring process-tree RSS and scratch usage. Memory-mapped storage still consumes scratch and resident pages, so it is part of the budget and evidence.

### 3. Publish generated raw task fragments

Write persistent source files under `historical/<version>/raw/{table}/{task.key}/v{schema_version}.parquet`. Each task writes at most one fragment per table and physical schema version, appending bounded purchase batches as row groups. Stream uploads and hashes, persist verified receipts and reclaim local scratch. `_run/` contains transient run state, receipts and diagnostic reports; `manifest.json` remains the sole consumer completion marker.

Manifest format `manifest_version: 3` declares `source_layout: task-fragments-v1` and embeds the exact file inventory. Each entry records key, checksum, bytes, rows, `schema_version` and `partitions: {date: rows}`. Event dates come from occurrence timestamps and label dates from availability timestamps; one fragment may cover several dates, including dates beyond history end. These are row-count summaries, not physical daily partitions. Sum their counts to each file's row total.

There is no Python/DuckDB finalization, sorting, compaction or remote analytical scan. Preserve all generated rows, duplicate deliveries and delayed labels. Report physical and uncompressed Parquet bytes from receipts/footer metadata, with additive statistics combined once per task. Object count follows tasks rather than batches. Product version `5.0.0` records the changed physical publication boundary; ODCS logical contracts and identities remain unchanged.

### 3a. Bounded publication footprint

A version's object count and its leftovers after publication are part of the resource contract, not an afterthought. Three controls keep the bucket shaped like the accepted datasets only.

A configuration that declares `accepted_version` tags the manifest and every listed raw object `accepted=true` at publication; pilots and candidates are published untagged. The storage policy can then deny overwrite and deletion of tagged objects, so immutability is enforced by the bucket rather than by operator discipline, it applies to any future version without naming one, and abandoned candidates stay removable.

The normal command removes only `_run/` transient objects and receipts, retaining all inventory-listed `raw/` files once the manifest is durable. Cleanup runs after publication, never before, so a failed delete cannot damage an accepted dataset; the outcome is recorded in a one-object `_run/publication.json` receipt with object and byte counts. Tagging or cleanup failure exits nonzero and leaves the version published. The receipt binds the manifest digest. Rerunning the same command verifies the completed inventory and logical configuration, reads existing tags to skip already protected objects, preserves unrelated tags and retries protection before cleanup. It does not rewrite final files or the manifest. Cleanup retains the receipt, examines S3 per-object errors even on HTTP success, and accumulates only successfully removed objects and bytes across retries. This requires read-tag permission as well as write-tag permission.

Probe and check datasets caused most of the historical bucket sprawl, because the same CLI published them under their own version names. Publication now refuses a dataset smaller than one configured customer block unless the operator passes `--allow-small-dataset`, and probes that need no remote evidence use `--output` on a local directory. The source identity therefore needs deletion permission only for `_run/` prefixes and abandoned probe/pilot prefixes, never for accepted data.

### 4. Resume verifies data and enforces a single writer

Task receipts are durable only after their referenced uploads succeed. Verify checksums and lengths on resume; where RustFS cannot provide a trustworthy object checksum, stream and hash the object. An upload acknowledged remotely but not checkpointed can be adopted only after verification or regenerated. Truncated files and bare object existence never count as completed work.

Use conditional S3 writes for run ownership, checkpoint updates and completion publication; local output uses equivalent exclusive/atomic filesystem operations. Add a directly pinned S3 SDK only for these concrete needs and bounded transfers. Before implementation relies on it, verify RustFS supports the chosen create-if-absent and ETag-conditional update semantics using disposable keys. Refuse unsafe resume/publication if those primitives are unavailable.

Run ownership has no automatic timeout takeover. Normal shutdown releases it through a conditional state transition; after a crash, explicit recovery requires the operator to stop the former writer before replacing its ownership token conditionally. Check the current token before checkpointing or publishing. Store task identity, algorithm, contracts, profile digest and checksums, never credentials or session tokens. All participants treat published versions as immutable; publication uses create-if-absent so a competing completion cannot overwrite a manifest.

### 5. DP1 owns Spark/GX Bronze processing and quality acceptance

Source publication verifies complete generation receipts, exact raw inventory, checksums, row-count accounting and the configured physical-byte gate, then writes the completion manifest last. Record `validation: {result: skipped, gate: dp1-spark-gx, reason: ...}`. Remove the simulator's native validation and DuckDB remote-scan/finalization path rather than retain an optional native default. The configured `accepted_version` tag protects published storage; it does not certify data quality.

DP1 validates exactly the files listed in the manifest, never a prefix glob. It reconciles file integrity, physical schemas and inventory counts, preserves identical duplicate deliveries and complete delayed labels, and derives Bronze date partitions from timestamp columns rather than raw key paths or a task's generation date. Spark/GX checks on both persisted Bronze tables cover cross-date lifecycle and label linkage; verified governance and pinned snapshot reports are required for Bronze quality acceptance. Logical ODCS schemas and semantics remain unchanged.

The 2026-10-22 `v8-pilot` native-validation measurements remain historical: 4,843,810,164 uncompressed bytes required 8,421,998,592 spill bytes (1.74 per uncompressed byte); extrapolation put legacy full-run spill near 512 GB. This motivated the new boundary, but neither a pilot nor that extrapolation validates `v8`. Its old finalizer is stopped before the separate operational handoff.

### 6. Cheap additive statistics and one-time legacy handoff

Combine receipted row/scenario/duplicate/schema totals and bounded merchant/category counters exactly once. Report observed shares only where these counters support them. Record configured identity pools separately and mark observed cardinality validation as downstream; do not fabricate counts, ratios or approximate measurements and do not scan remote Parquet in Python/DuckDB to obtain them. DP1 computes cardinalities and exact lifecycle/conflict/linkage checks.

Do not maintain a legacy migration CLI, server-side copy adapter or retired-configuration normalization in the simulator. Its normal command generates raw files and publishes their inventory; compatible resume and publication retries remain part of that same path. Legacy `v8` requires a one-time operational handoff outside the repository after stopping its finalizer. Verify complete task receipts and original identity/configuration/profile/algorithm/contracts before writing copied source objects.

Copy receipted fragments server-side to persistent `raw/{table}/{task.key}/v{schema_version}.parquet` keys without generating rows, sorting, compacting or analytical rescans. Verify copied checksums and byte/row gates, preserve original producer provenance separately from the handoff execution, and publish the format-3/product-`5.0.0` manifest last. Retain original fragments and partial legacy output throughout this operation. This one-time publication is separate from DP1 quality acceptance.

### 7. Calibrate existing data problems instead of adding new ones

- **Volume and entity diversity:** scale customers and device/address/method pools together with purchases. Merely raising purchases per customer reduces distinct-to-row ratios and changes behavior. Increasing history duration is allowed within a documented plausible scenario, but it increases runtime and label partitions. Select a fixed combination from the pilot, not a byte-triggered loop.
- **Merchant/category skew:** retain explicit Zipf parameters, initially 1.1/1.2 from v7. Report top-1/top-10 merchant shares and category shares and declare acceptable bands before the full run. More merchants alone does not guarantee the intended observed skew because customers prefer familiar merchants.
- **High cardinality:** calibrate device/address/method pools and require downstream observed cardinalities and ratios, including new identities created by compromise and card-testing scenarios. Keep the existing IP-network bound; do not multiply it beyond 65,536 or change the fingerprint scheme just to create more bytes. Use the other keys to demonstrate high cardinality.
- **Schema evolution:** retain the physical `user_agent_family` cutover near the middle of the chosen history, with substantial rows on both sides. Check occurrence dates of transitions crossing midnight, not only the purchase start date.
- **Offline duplicates:** retain 2% identical exported-event rows. Resume must not add extra duplicates. Do not inflate duplicate rate to hit the byte target.
- **Fraud scenarios:** retain per-customer compromise probabilities, their windows, high-value probability and friendly-fraud noise as behavioral controls. Calibrate `card_testing.rate` separately from expected ordinary purchases per day to retain its intended attempt share; the chosen result remains an explicit bursts/day value. Keep burst size/duration meaningful. Record attempt-level truth proportions as well as existing payment-level counts.
- **Delayed labels:** retain 7–21 day delays initially and export all labels, including those beyond history end. Never compress label delays to simplify the size run. Burst arrivals in a synthetic history are business scenarios; transport lateness remains the live simulator's separate work; source duplicate deliveries must survive DP1 ingestion.

The historical six-day pilot included the warmup of seven-day stolen-card windows. A bounded first-block calibration on days 30–35 estimated a 2.67% stolen-card attempt share, so the legacy full config uses a 2.0–3.2% band while the unchanged pilot retains 0.8–1.8%. This is subset calibration; full-run observations remain required.

The pilot report proposes concrete counts, traffic and card-testing rates and their acceptance bands, then the full config freezes them under a new version. These tuning values are intentionally measured rather than invented in this plan. If changing them materially alters compression or profile costs, repeat the pilot.

### 8. Explicit size accounting and measured acceptance

For this change choose `minimum_source_parquet_bytes: 100000000000` in the full scale configuration, zero in a pilot. This is a conservative project target for 100 decimal GB of physical source files, not a claim that the coursework specified compressed bytes. Sum each inventory-listed raw event/label file once; exclude transient intermediates, state, reports, metadata, S3 replicas, streaming and all bronze/silver copies. Also report summed Parquet column-chunk uncompressed bytes, raw byte counts and GB/GiB conversions; call that metric uncompressed Parquet data, not an in-memory Python size.

Estimate planned payments using raw-layout pilot physical bytes per payment, allowing measured variation across days and schema versions. Keep the existing compacted pilot measurements historical and recalibrate if the layout materially changes compression or publication costs. Report projected generation/publication time, persistent raw storage, remote transient storage and upload time from observed throughput. A fixed plan that misses the byte target does not publish; change the logical plan under a new version and retain the failed run's measurements. Do not append arbitrary rows until the size counter passes.

The full acceptance run records wall time, process-tree peak RSS, scratch peaks, bytes transferred, counts, distribution summaries, checksums and both table validation reports, naming the engine and gate. Source publication and downstream quality acceptance are separate milestones. Quality acceptance requires DP1 Spark/GX reports over pinned persisted Bronze snapshots; a pilot, projected duration or benchmark over selected columns does not establish full-run acceptance. Keep CI on small fixtures.

## Risks / Trade-offs

- [Raw task layout increases downstream work] → DP1 reads the exact inventory and owns partitioning and processing; make no source sorting/compaction promise.
- [Source publication mistaken for quality acceptance] → Record skipped checks and require both DP1 persisted-table reports plus governance before acceptance.
- [Algorithm changes break old live continuation] → Version manifests, preserve identity vectors and immutable v7, fail on digest mismatch and generate a fresh continuation dataset with the new algorithm.
- [A larger dataset loses meaningful skew/cardinality/scenario mix] → Declare bands from a representative pilot and compare the final dataset; retain the 2% duplicate rate and the distinction between absolute and per-customer scenario rates.
- [Object-store conditional-write behavior differs from AWS] → Run a bounded compatibility check before relying on the protocol; stop rather than downgrade writer exclusivity.
- [Abandoned ownership after a crash] → Require explicit takeover after the previous process is stopped; never infer that elapsed time alone makes a second writer safe.

## Migration Plan

1. Implement generation-only raw publishing and manifest format 3/product `5.0.0`; remove simulator finalization, native remote scans and cardinality scan requirements. Keep logical ODCS contracts unchanged.
2. Extend small inventory, integrity, resume and publication fault tests. Adapt DP1 consumption to exact raw inventories, timestamp-derived Bronze partitions and Spark/GX quality gates.
3. Stop the old `v8` finalizer and perform its one-time, generation-complete source handoff outside the repository, retaining original fragments and provenance. Keep legacy migration code out of the simulator.
4. Preserve the v7 and measured legacy pilot/integration records. Recalibrate raw-layout bytes and publication costs before claiming new full-run evidence; record source publication separately from downstream acceptance.

Small local checks establish implementation behavior, not full-scale publication or downstream quality acceptance.

Rollback stops the current writer and leaves its output incomplete without a manifest. Consumers continue using the last accepted version. A previous binary cannot resume new run-state formats and must refuse; restoring source does not require rewriting accepted datasets or deleting remote output.
