## Context

Payments commits protobuf events to its append-only Postgres ledger/outbox. Debezium publishes those bytes to `payments.events.v1` through the existing Strimzi Kafka Connect worker; the notifier decodes protobuf. The historical Python generator already owns deterministic identities, profiles and behavior. See proposal.md for motivation.

V12 is a completed immutable -365-day calendar rewrite of v8, preserving its identities and profiles; it is not a new generation. Its payment history ends at `2026-07-01T00:59:58+00:00`, with delayed labels through July 21. Publication/protection does not establish DP1 Spark/GX acceptance. Preserve v8/v11/v12 and previous failed/passed evidence.

## Goals / Non-Goals

**Goals:** one Python live workload; current-time gateway traffic using the historical entities; unchanged committed-event delivery faults; Go-owned immutable Parquet landing; one common configuration and reusable DP1 interface.

**Non-Goals:** historical regeneration, gap filling, virtual gateway time, Avro/registry, a Go simulator, a separate fault deployment, actual-production isolation, and a durable cross-restart chargeback scheduler. This remains a bounded single-replica evidence workload.

## Decisions

### One Python package and process

Reuse the existing profile, identity and behavior modules directly rather than running the historical Parquet export pipeline or reproducing NumPy random streams in Go. Separate model logic from HTTP, Kafka, protobuf and storage adapters. Share only modules with actual historical/live use; do not make domain logic depend on generated protobuf.

One `live` entry point runs traffic generation, label scheduling and an optional delivery-fault loop. Kafka polling, bounded queues and blocking client work must not starve HTTP scheduling. A fatal loop failure terminates the workload visibly; bounded backpressure and shutdown report unfinished work. Use one common runtime configuration for dataset reference, traffic, labels, faults, Kafka and landing settings, with secrets injected at runtime. Existing historical generation config remains an immutable source reference rather than a duplicate live deployment configuration.

Compared with a separate fault service, this removes a deployment and configuration boundary while coupling restart and memory pressure. Keep the fault module independent enough to extract later, without implementing a second deployment mode now.

```text
Historical manifest -> shared Python profiles -> live traffic -> gateway APIs
                                                          |
                                                Payments ledger/outbox
                                                          |
                                                          v
                                                 payments.events.v1
                                                          |
                                     optional in-process fault loop
                                                          |
                                                          v
                                              payments.events.v1.faulty

Simulator labels -> fraud.labels.v1
Selected payment topic + labels -> Go protobuf-to-Parquet archiver -> RustFS
Historical exports + completed raw objects -> DP1 bronze -> DP2 -> DP3
```

### Machine time and historical identity continuity

The simulator starts new commands now, and gateway services retain their current machine-clock timestamp ownership. A gap after historical history is valid: do not enforce `max_gap`, backdate committed events, require histories ending today, or regenerate data to bridge missing days. Calendar continuity means stable entities and model semantics, not fabricated observations during downtime. Seeded choices are reproducible within a run; wall-clock times and gateway-generated IDs are not promised to be identical across runs.

Rebuild and verify profiles using the historical manifest's seed/configuration and profile digest. Do not reuse a finite historical scenario schedule as a repeating calendar: live scenario scheduling operates on current dates using reusable behavior parameters. Preserve historical entity assignments and account-creation context rather than recomputing identity-defining attributes relative to today's date.

Rewrite the vault's synthetic card mapping for the exact v8 identity space preserved by v12, including all 3.9 million cards. Remove obsolete fixture compatibility. Setup tokenizes selected cards through `POST /v1/payment-methods`; the live runner never accesses vault internals or credentials. New opaque tokens must resolve to the same historical method fingerprints. Keep card numbers and tokens out of logs, Kafka and manifests.

The HTTP client creates payments and attempts with unique run-scoped idempotency keys. Retries preserve key, token and request content. Bound concurrency; report arrival lag and gateway saturation instead of silently dropping intended commands. The traffic module does not manufacture payment events or use request retries as a substitute for Kafka delivery duplication.

### Protobuf shared with Go, without a registry

Root `contracts/` remains the source of truth. Extend existing codegen to generate importable Python event/label modules alongside Go, and package the generated Python bindings and compatible protobuf runtime in the image. Build/CI verify generated output and cross-language encode/decode behavior. No independent Python schemas, registry framing, Avro serializers or gateway event-format migration are introduced.

HTTP/JSON gateway calls need no protobuf binding. Label publishing uses generated `FraudLabel`; the fault loop decodes `PaymentEvent` for deterministic event-ID selection but forwards original bytes without reserialization. The Go archiver uses the same root contracts to decode landed business columns; retained protobuf bytes support replay. DP1 reads the decoded Parquet projection.

### Post-commit delivery faults within the process

The fault loop has its own durable consumer group on `payments.events.v1`, producing to `payments.events.v1.faulty`. Keep the same partition count and payment key, forwarding key/value and existing headers unchanged. Stamp the faulty Kafka record's delivery timestamp at publication; embedded event timestamps remain unchanged. Preserve physical source topic/partition/offset during landing; injected copies remain distinguishable by delivery offsets while retaining event identity.

For duplicate and late selection use the first eight SHA-256 bytes of `{seed}:{fault-name}:{event_id}`, scaled to [0,1). Named hashes select independent delays in configured ranges. Burst holds follow wall-clock intervals and release scheduled records together. Bound held record count and bytes; pause consumption at the limits while continuing required Kafka group polling. Reject malformed payloads visibly rather than silently dropping them.

A record completes only after all required publications are acknowledged. Commit the next Kafka offset only through the contiguous completed prefix of each partition. After a crash, held/unacknowledged events are reconsumed; extra duplicates are possible, loss is not acceptable within retained source offsets. With faults disabled, an uninterrupted pass publishes each consumed record once in order; restart recovery remains at least once. Logs distinguish configured faults, observed delivery counts, backlog and replay caveats.

These are delivery faults, not RecSys-style request-volume bursts or backdated source events. RecSys's Python `StreamProblemPipeline` demonstrates useful module organization, but writes generated records to Postgres with integrated backdating/replay/tick multipliers. The card-fraud producer directly replays generated Kafka records, with faults already present in its data. Neither replaces the gateway-owned committed-event boundary here.

### Independent labels and valid training targets

Publish simulation truth after the attempt response with the versioned `FraudLabel` contract. Schedule chargebacks using configured delays and optional evidence time compression; when due, query gateway state and publish only for a succeeded attempt. `label_available_at` is actual publication time. Delayed labels remain an independent input and the event batch must not require labels before they mature.

Label schedules are in memory for bounded runs. Report interrupted schedules and do not claim crash-safe label completeness; delivery-fault offset recovery does not make the label heap durable. Simulation truth is a valid training target. Scenario/oracle-only fields stay out of payment decision inputs and features, and training joins enforce as-of availability.

### Go protobuf-to-Parquet archiver

Use one small Go workload consuming selected payment topics and `fraud.labels.v1`. Existing Kafka Connect continues authoritative CDC unchanged. Aiven's timed multi-record files could overwrite previously visible data on replay; Confluent's conditional protection excludes native key/header sidecars; adding a custom Connect transformation is rejected in favor of a direct Go adapter. Flink can coordinate source/sink checkpoints but is unnecessary infrastructure solely for landing.

Decode using existing root-contract Go bindings and explicitly map to the historical Parquet business schemas, including nested payloads, enum names, absent optional fields, microsecond UTC timestamps and physical schema versions. Keep payment events and labels in separate table paths. Do not deduplicate, sort by event time, fabricate lifecycle transitions or otherwise curate business content. Reject unsupported schemas, malformed input and unrepresentable values visibly before committing their offsets.

Add a reserved `_transport` structure for source topic/partition/offset/timestamp, raw key/value and ordered binary headers. Retaining original protobuf supports exact replay and future decoding without making DP1 decode protobuf during ordinary ingestion. Historical exports remain unchanged and lack Kafka metadata; business columns are compatible, not physically identical entire schemas. Publish a machine-readable landing contract and bounded batch manifest containing file identity/checksum/size/count/schema and actual consumed Kafka offset bounds. Topic origin is not synonymous with real-versus-simulator identity; retain integration identity and supplied provenance without inventing real-world origin.

Bound buffered records, bytes and elapsed time; flush low-volume partitions. Process bounded partition batches, close Parquet and publish with conditional create. File names include table, topic, partition, offset bounds and content SHA-256 so retries with different batch boundaries cannot replace prior objects. Publish immutable batch manifests last. A collision is accepted only after confirming matching immutable metadata/size; mismatches fail. Commit a partition's next Kafka offset only after all its files and batch manifest are durably published. On interruption or rebalance replay unfinished work; overlapping batches and extra duplicates are allowed and retain Kafka positions. No cross-system exactly-once claim or database dependency is introduced.

Land in `s3://synthetic-source/streaming/` alongside `historical/`. Reuse the existing synthetic-source identity and Doppler credential pair through External Secrets; inspect the actual deployed policy before adding only streaming-prefix list/read/write and required multipart permissions. Preserve historical accepted-object protection, no deletion and no pipeline-artifact access. The archiver receives no gateway API, database or vault credentials. Keep CDC Secret API privileges unchanged. Verify actual RustFS conditional publication/retry behavior in CI/integration evidence before rollout.

### Shared landing and DP1 interface

One current environment, codebase and common configuration support synthetic historical and live truth. Future actual-production onboarding/isolation is outside scope. Retain source origin, IDs, schema versions, event/label-availability times, Kafka positions and batch identity for provenance/replay.

Recurring flow is Kafka -> completed immutable landing files -> scheduled bounded DP1 -> bronze -> DP2 silver/gold -> DP3 features. Historical exports/backfills remain another reusable DP1 input mode. Freeze completed-object selection and identity for each batch; durable file discovery must catch new files in older date folders and must not use event-date maxima as completeness evidence. DP1 owns ingestion, retry-safe Iceberg commit/recovery and per-batch validation/readback. Implementation of multi-source configuration, durable incremental discovery/commits and contextual validation is a subsequent DP1 slice; this slice supplies the typed landing boundary and written handoff only. No cross-system exactly-once promise is made.

Bronze enforces integrity/conservation/schema/contract/identity-content/lifecycle-context/linkage/leakage while preserving deliveries. Consult prior history and label maturity; do not run complete-history lifecycle rules independently on incomplete live batches. Curation/dedup belongs in silver. Generator distribution/cardinality bands are informational diagnostics under DP1's planned runtime policy, with published bands/contracts and old results unchanged. Policy implementation belongs to DP1, not this simulator change.

### Deployment and evidence

One simulator image and opt-in single-replica `Recreate` Deployment under the configured prod root, using existing Kafka, plus one independently enabled Go archiver Deployment. Do not create separate fault-injector Deployments, a second source-specific environment or a dev root. Declare `payments.events.v1.faulty` and `fraud.labels.v1`; keep the existing clean topic and notifier behavior. Use the shared PR/main image policy and extend existing docs only where behavior/config changes.

Retain accepted v11 historical evidence separately from live/v12 acceptance. The completed v12 manifest is `s3://synthetic-source/historical/v12/manifest.json`, SHA-256 `66aa2af7fecb1177e0d3891ea86054ff9c076ff6e8743b35b6bec0bd351511ce`. Its mixed verification policy and pending DP1 Spark/GX validation must not be relabeled by a simulator rollout. No rewrite/rescheduling of that completed one-off job is part of this change.

### Focused verification

Do not add dedicated config-validation, default/getter, logging-shape or repetitive rendering tests. Use existing validation and local non-test schema/render/diff checks where appropriate. Reuse existing CI coverage and add only distinct behavior checks for cross-language wire compatibility, historical identity mapping, idempotent retries, label leakage/maturity, fault recovery and typed Parquet/schema compatibility and byte/provenance-preserving immutable landing. Use representative mapping boundaries rather than millions of tokenization calls or exhaustive fixture tests. Consolidate archiver recovery/permission evidence instead of building duplicate harnesses. No local test execution.

## Risks / Trade-offs

- [Shared process restarts traffic and faults together] -> Commit only acknowledged fault prefixes, bound queues and fail visibly; independent deployments are deferred.
- [Label heap lost on crash] -> Limit evidence runs and record interruption; durable chargebacks require a later explicit scope decision.
- [Gap after history affects analytic windows] -> Treat it as missing observations, use prior-history context, and never invent bridge traffic.
- [Archiver retry mutates or omits raw data] -> Gate rollout on byte/provenance/completion/recovery checks and fail if immutable selected objects cannot be guaranteed.
- [Model reuse changes historical identities] -> Verify profile digest and exact vault mapping before sending traffic; remove obsolete fixture assumptions without changing datasets.

## Migration Plan

1. Add Python codegen/runtime packaging and implement the v8/v12 card mapping and profile reuse, with focused CI verification.
2. Add the live workload and in-process fault module, common config and topics; validate byte-preserving behavior and restart recovery in CI.
3. Build the Go archiver, extend only streaming-prefix access and agree the completed-object/Parquet interface before rollout.
4. Exercise bounded current-time gateway traffic, historical identities and labels through GitOps with the archiver disabled; then exercise each delivery fault and the combined configuration. This simulator-only session does not depend on streaming-write policy access.
5. Exercise Parquet landing and recovery after its policy/image prerequisites pass. Review the live evidence before starting the subsequent DP1 multi-source ingestion slice. An interface handoff alone is not a start signal. Preserve source datasets and historical results.

Rollback disables the simulator and Go archiver while leaving gateway publication, notifier, Kafka source history and already completed objects intact. Do not delete datasets, rewrite old records or retain obsolete simulator deployment/config paths for compatibility.

## Open Questions

- Tune traffic rate, bounded queue sizes, topic retention and file-rotation/batch cadence from measured workload behavior; these do not change the selected topology or contracts.
