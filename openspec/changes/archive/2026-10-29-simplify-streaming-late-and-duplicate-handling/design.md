## Context

Today Debezium publishes committed outbox rows to `payments.events.v1`. An optional forwarder copies them, delayed or duplicated, to `payments.events.v1.faulty`, and consumers pick one topic. Flink matches attempt starts with risk decisions and maintains 1h/24h/7d snapshots. Edge always sets `attempted_at` to server time, although Payments already keeps a caller-supplied value. The notifier, Flink and the archiver all consume `payments.events.v1`, and the publication spec already requires each consumer to deduplicate by `event_id`.

## Goals / Non-Goals

**Goals:**

- Produce late and duplicate inputs where they occur in practice, on one topic.
- Handle them in Flink with plain dedup and event-time windows.
- Simplify the feature contract to what streaming needs.
- End the streaming output at the feature journal and dead-letter topics.

**Non-Goals:**

- Label completeness changes, drift generation or detection.
- HybridSource bootstrapping.
- Spark/Silver changes.
- Restructuring the vault/processor simulator services.
- Migrating existing feature data.
- Feast online/offline stores, materialization, TTLs and the fraud service's feature retrieval. These move to the later feature-store change in the roadmap build order and stay on v1 code, disabled.

## Decisions

### One payment topic; faults originate at their real source

- **Late arrival** comes from store-and-forward clients. The simulator selects a share of attempts by a seeded hash of the run and ordinal. It records the click time, waits the configured delay, then sends the attempt with `simulated_client.attempted_at` set to that click time.
- **Duplicates** come from at-least-once redelivery. A simulator thread consumes `payments.events.v1` in its own group. For a seeded share by `event_id`, it produces byte-identical key, value and headers to the same partition, adding the header `x-simulator-redelivery`. It skips records carrying that header and commits offsets after delivery acknowledgment. A restart may add extra copies, which are still duplicates.
- **Why not a forwarder:** a forwarder on the shared topic can add copies but cannot delay originals, since CDC has already delivered them. The separate faulty topic existed only to hide originals.
- **Why not pausing CDC:** pausing Debezium delays every event uniformly, so no event arrives out of order relative to others.

### Client time is synthetic-only and never in the future

- Edge parses `simulated_client.attempted_at` (RFC 3339) only for synthetic integrations, like the existing IP fields. It rejects any time after server time, and sets no lower bound.
- A future attempt time is never valid. Rejecting it at Edge keeps attempt time at or before the event's recorded time, so Flink's existing availability check bounds it without a separate skew allowance.
- Old times need no edge limit because Flink classifies them.
- Real integrations keep server time.
- Fraud receives the supplied `attempted_at` for its rules; its feature retrieval stays disabled in this slice.

### Feature contract v2 lives beside v1

Customer quantities:
- `attempt_count_5m`, `attempt_count_1h`, `attempt_count_24h`
- `distinct_devices_24h`

Method quantities:
- `attempt_count_5m`, `attempt_count_1h`, `attempt_count_24h`

Unchanged request features: `amount_minor`, `customer_account_age_hours`, `ip_shipping_country_mismatch`.

Rules:
- Window membership uses attempt time in `[s-w, s)`, where `s` is the snapshot anchor.
- Coverage per window is unknown until the job has run for that window's length; unknown is never zero.

Contract and wire changes:
- `fraud-snapshot-v2.yaml` and its examples sit next to the unchanged v1 files; the v2 reference calculation is `features/reference_v2.py`, next to the v1 reference.
- The journal message is the new `features.v2` package. Its `SnapshotUpdate` drops `computed_at` and `published_at`, which always equaled `anchor`, and carries only what the journal needs, with no serving evidence messages.
- Only the streaming project generates `features.v2` bindings. Feast and the fraud service keep `features.v1` until the feature-store change.
- The epoch `live-v18` publishes to `fraud.feature-updates.v2.live-v18`, a new schema-registry subject, so no compatibility check against v1 applies.

Rejected alternative:
- Renumbering `features.v1` in place would break Feast and the fraud service, which still decode v1 on main.

### Flink counts unique attempt starts

- **Pipeline:** decode only `attempt_started` → dedup by `event_id` → key per view → rolling windows.
- **Eligibility:** every attempt counts, assessed or not. Every attempt is assessed synchronously before its result, so the decision gate added matching state and failure modes for negligible difference.
- **Current-attempt exclusion:** the contract keeps it for the later serving path: serving must accept only snapshots with `anchor <= t`, and windows exclude `s`.
- **Horizon:** 24h, the largest window. Accepted late inputs inside it produce new revisions; previously served evidence stays immutable.
- **Too late:** an input whose attempt time is before `now - 24h` cannot affect any window. It increments a `too_late` counter, is logged with its event identity and is dropped.
- **Dedup state:** expires one hour after the event's attempt time plus 24h. Any later copy is already too late, so state stays bounded by event time and duplicates can never double count.
- **Conflicts:** differing content under one `event_id`, or a second start for one attempt identity, is dead-lettered (below). Two small keyed dedup stages, by event then by attempt identity, keep the entity state free of conflicts.
- **Clock modes:** live processing-time and CI logical-clock replay both remain; the logical mode is test infrastructure.

### Per-record data errors go to a dead-letter topic

- **Problem:** raising on one bad record restores the checkpoint and re-reads the same record, so the job crash-loops and online features stop.
- **Classification:**
  - **Dead-lettered data errors:** undecodable protobuf, invalid or unsupported events, invalid attempt context, and conflicting content under one event or attempt identity.
  - **Still fatal:** entity state limits, clock or offset regression, partition-layout or bootstrap changes, and attempt times after observed availability. Edge prevents the last case, so it signals a bug.
- **Mechanism:**
  - `ObserveSource` and both dedup stages yield to one `OutputTag`. The job reads every operator's side output, because PyFlink otherwise leaks tagged values into the main stream.
  - Rejects go through a Table API Kafka sink in the same statement set and checkpoint as the journal: exactly-once, raw key and value, and the writable `headers` metadata column.
  - The record timestamp comes from the source table's `timestamp` metadata.
  - `ObserveSource` still advances offset progress for a rejected record.
- **Not the durable copy:** Bronze Kafka mode independently keeps every raw delivery in `bronze.rejected_records` or the Bronze tables. The dead-letter topic serves real-time triage.
- **Rejected alternative:** catching errors and silently dropping records hides data problems and removes the triage path.

### Bootstrap is a warmup, not HybridSource

- Historical v17 events are dated in 2025 and live traffic is current, so no historical event falls in a 24h window of live time.
- Windows therefore report unknown coverage until they warm up.
- Restarts recover from the checkpoint, and 7-day Kafka retention exceeds the 24h horizon.

## Risks / Trade-offs

- Counting undecided attempts → negligible gap, because assessment is synchronous; documented in the contract.
- The redelivery thread writes to the production-shaped topic → limited to the simulator workload, disabled by default, and every consumer already deduplicates by contract.
- Client time from synthetic integrations only → real merchants cannot backdate attempts.
- No consumer of the v2 journal in this slice → online features are unavailable to the fraud service until the feature-store change; rules stay authoritative. Feast and feature retrieval are disabled so nothing reads v2 with a v1 decoder.
- Existing v17 feature data is abandoned.

## Migration Plan

1. Land the contract, Flink, Edge and simulator changes together through CI.
2. Deploy with epoch `live-v18`: new v2 journal and reject topics, fresh Flink state, Feast and feature retrieval disabled.
3. Delete the v17 journal and `.faulty` topics through GitOps.
4. The feature-store change later adds the v2 online and offline stores and re-enables retrieval.

Rollback is a revert of the change.

## Open Questions

None.
