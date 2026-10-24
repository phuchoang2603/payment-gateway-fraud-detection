# Proposal

## Why

The historical generator produces a useful correctness dataset, but stages the entire output locally, builds whole-day schedules and growing Python identity sets, and restarts from scratch after interruption. A workstation needs bounded generation and resumable uploads to the existing RustFS service before attempting the coursework's large offline dataset.

## What Changes

- Replace full-dataset local staging with deterministic generation tasks, bounded worker queues and scratch space, streamed uploads, durable progress and automatic compatible resume. Continue using `s3://synthetic-source` at `https://minio.home.phuchoang.sbs`; the Iceberg catalog is not involved in source generation.
- Bound schedule, profile and statistics memory as well as event batches. Share read-only profile data between local workers and remove growing Python distinct-ID sets and whole-file byte uploads.
- Publish generated schema-versioned raw task Parquet directly, preserving complete payment histories, identical duplicate deliveries and separately delayed labels. Remove Python/DuckDB sorting, compaction, finalization and remote analytical scans. DP1 owns Spark/GX processing and quality acceptance on persisted Bronze tables; logical ODCS contracts remain unchanged.
- Use manifest format 3 with `source_layout: task-fragments-v1`, exact raw-file inventory, per-file date-to-row partition counts, physical schema version, checksum, bytes and rows; make no sorting or compaction promise. Change the historical-payments product version to `5.0.0` for this physical publication boundary.
- Keep legacy migration out of the maintained simulator. Stop the old `v8` finalizer after generation completes and handle its existing fragments once outside the repository, verifying complete receipts, original provenance, checksums and publication gates without generating rows or deleting the original fragments.
- Integrate automatic publication repair into the normal command after failed accepted-object tagging or partial `_run/` cleanup, without rewriting completed output; persistent `raw/` files survive cleanup. `--prepare-only` leaves generated inventory ready, without sorting or a completion manifest. Downstream validation names the `dp1-spark-gx` consumer gate; bronze quality acceptance remains downstream.
- Add a measured pilot and a separate scale configuration. Freeze a deterministic generation plan using observed bytes per payment and measured resource use; reject publication when its configured byte target is unmet. Record physical Parquet bytes and uncompressed row-group bytes separately.
- Keep the existing offline problems: merchant/category skew, high-cardinality identities, physical schema evolution and identical duplicate exports. Calibrate entity counts, traffic and absolute card-testing burst counts at scale; do not introduce streaming lateness, arbitrary padding or new corrupt-data scenarios to increase volume.
- **BREAKING**: version the revised generation algorithm and manifest/run state. New outputs need not reproduce v7's rows or profile digest. Accepted datasets stay immutable and readable; live continuation must fail on an incompatible generator/profile digest rather than silently changing entities.
- Record the precise coursework source: `EDAI K11 - DE.xlsx`, sheet `edai-1 (50%)`, cell B5, says offline simulation is at least 100 GB. Compression accounting is unspecified there. This change proposes a conservative acceptance target of at least 100,000,000,000 physical source Parquet bytes, counted once across events and labels, independently of streaming, bronze copies and replicas.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `historical-data-generation`: Bounded local execution, deterministic parallelism, resumable unpublished output, generation-only raw publication, downstream quality validation, scale calibration and explicit volume/data-problem evidence.
- `focused-verification`: Small behavior tests for interrupted uploads, resume integrity, resource limits, execution-independent records and preservation of existing data problems; measured large-run acceptance outside CI.

## Impact

- `src/data-pipeline/simulator/simulator/{config,cli,profiles,generation,statistics,storage}.py`, focused task/checkpoint modules where responsibilities require them, and existing simulator tests.
- A separate `src/data-pipeline/simulator/configs/historical-scale.yaml`; retain `historical.yaml` as the documented v7 configuration and add narrowly scoped pilot/runtime settings without credentials. Retain dependencies needed for conditional S3 run-state ownership; remove the simulator's DuckDB finalization/remote-scan path.
- Minimal affected sections of `docs/data-ml/data-products.md` and the existing historical sanity notebook for commands, coursework provenance, byte accounting, calibrated distributions and measured resource results. Update the roadmap's generator evidence only after an actual accepted run.
- Reuses the existing RustFS service and source bucket. No gateway, live generator, Iceberg writes, Spark Operator, DataHub or platform deployment is implemented by the simulator. DP1 must validate exactly the listed raw files, preserve duplicate deliveries and delayed labels, derive Bronze partitions from timestamp columns, and require Spark/GX plus governance before quality acceptance. Stopping the old `v8` finalizer and its one-time source handoff do not establish downstream validation.
