## Context

See proposal.md for motivation and scope. Current main has schema-3 self-contained AttemptStarted, committed RiskDecisionRecorded, a pure ten-feature reference under src/data-pipeline/features, Go Fraud with pure rules, and Payments' atomic state/ledger writes plus committed-event CDC publication. Payments already derives stable attempt IDs from payment/session idempotency and fences state transitions. Its current error fallback fabricates a FRAUD_UNAVAILABLE decline; Fraud itself has no durable execution record.

The existing exact feature definition anchors every history window at the scored attempt. This design intentionally changes that serving assumption. The deployed platform was inspected during exploration: no Flink deployment CRD or Feast deployment was present. The existing Payments Redis is an idempotency cache, not a durable assessment ledger or feature store.

## Goals / Non-Goals

**Goals:** independently deployable streaming/online components, a short synchronous authorization path, reproducible served evidence, and explicit eventual consistency. Preserve the ten feature quantities, not the superseded exact-window interpretation.

**Non-Goals:** model enforcement, coordinated customer/method snapshots, cross-entity Redis transactions, cross-system exactly-once claims, full offline DP3 implementation or new source generation. DP1 remains separately owned and may proceed in parallel.

## Decisions

### 1. One Python streaming job and supported Feast boundaries

The continuous flow is payments.events.v1 → PyFlink → a versioned fraud feature-update topic → Python Feast SDK writer → dedicated Feast Redis. The official Feast feature server supplies Go Fraud's retrieval adapter. Customer and method aggregation are two keyed branches inside one job, not two services. Payment protobuf decode/generated bindings stay at the transport edge; feature math remains framework independent.

Use a pinned PyFlink-compatible Python runtime independent of the simulator's Python 3.14. Exploration of upstream stable docs found Python 3.9–3.12 support; select and pin the exact supported Flink/Python/JVM/connector set during implementation. Reuse compatible pure contract calculations, not the simulator runtime or its orchestration.

The durable feature topic adds one journal and a small writer but isolates Feast downtime from aggregation and supports independent replay and a later offline consumer. Direct synchronous Flink-to-Feast HTTP pushes couple checkpoints to an external side effect and were rejected. No custom Redis feature serialization or extra scoring coordinator is needed.

The card-fraud-platform reference uses the topic/SDK-writer pattern, but includes current authorization and uses incompatible defaults. RecSys provides state/window examples but has a custom Redis layout/API we will not copy. Context7 was unavailable due to quota during exploration; official Flink/Feast docs and Feast Redis source informed this design. Pin-release behavior must be verified before rollout, especially equal timestamps and concurrent writes.

### 2. Snapshot semantics and coverage are explicit

Create a new definition identity, fraud-snapshot-v1, with a content hash and explicit snapshot semantics per historical feature. Replace active exact-baseline selectors/reference examples rather than retaining a compatibility execution path. Preserve prior archived evidence and published source bytes; no source regeneration is required.

Customer keys are (integration_id, customer_id); method keys use the existing payment-method fingerprint, not an invented method ID. Features remain three request quantities, five customer quantities and two method quantities. Historical names identify snapshot semantics. Amounts remain USD int64 minor units. Last-assessed stores the contributing attempt's start time, not decision-publication time; elapsed seconds are derived from the current gateway attempt time t, with null for no eligible timestamp, a timestamp after t, or one outside [t-7d,t).

For each snapshot anchor s, history uses start times in [s-window,s), and only start/committed-decision pairs available to the processor contribute. Declines count; undecided attempts do not. Retain per-window coverage and separate metadata for snapshot anchor, emitted/publication time, observed source progress, definition hash, source/run epoch and revision. A source progress timestamp is meaningful only with its documented partition/offset/heartbeat basis; an idle partition or watermark alone cannot prove source completeness.

A served view is available only if its definition and epoch match, coverage supports that window, its anchor is not after t, and anchor age plus measured pipeline progress satisfy configured budgets. Future, stale or unsupported views become unavailable. Known complete empty windows are zero; unknown windows stay null. Customer and method can have different anchors/statuses. Initial retrieval is shadow-only; feature-set/model fields must not imply these values contributed to the rules result.

Training later joins observed snapshot availability or actual served evidence, not a newly computed ideal history. Event-time anchors do not prove a snapshot was available at the decision. Labels remain separate and maturity-aware; oracle/scenario/assessment outcomes stay out of model inputs.

### 3. Hybrid emission with bounded exact state

Deduplicate event identity/content and match lifecycle pairs with scoped attempt identity. Retain event-time contributions through the longest seven-day window plus configured lateness/recovery margin. Unmatched facts, conflict fingerprints and dedup state have explicit retention; expired unmatched data degrades coverage or fails visibly, not silently counts. Inputs outside retained correction range require controlled replay/new epoch instead of mutating past serving evidence.

Emit on eligible assessment changes and scheduled expiration boundaries. In live mode timers advance the UTC wall-clock anchor even when traffic is idle, while source-progress/coverage may remain degraded. Historical conformance uses an injected logical clock and deterministic timer advancement. Implement lower-bound inclusion precisely at the chosen timestamp precision: a contribution exactly at s-window is still included. Deduplicate timer work per entity rather than sweeping every entity constantly.

Exact distinct devices retain expiring occurrences or equivalent per-device timestamp/refcount state; a set or non-expiring sketch is insufficient. State TTL is cleanup, not a substitute for exact window expiration. Separate heartbeat/progress telemetry from feature mutation; an unchanged count does not require a high-frequency full snapshot write. Measure timer/state cost on smoke before choosing production cadence/resources.

### 4. Ordered writes, recovery and epochs

Each update carries immutable snapshot ID, entity/view, semantic hash, epoch, anchor, revision, observed availability and coverage. Use checkpointed Flink state and a supported checkpoint-coordinated Kafka sink. Consume committed updates, write Feast first, then commit consumer progress. Delivery to Feast is at least once with idempotent replay, not a distributed transaction.

Start with one ordered writer for each entity/view; serialize retries and do not allow concurrent offline materialization into active keys. Assign a strictly increasing online write timestamp per entity/view for each revision, retaining the actual anchor separately. Equal-anchor late corrections receive newer write timestamps; retries retain identity/timestamp. Checkpoint/recovery must preserve this ordering. Verify the pinned Feast Redis implementation's older/equal timestamp behavior; do not assume its read/check/write path supplies atomic compare-and-set across multiple writers.

Activate live epochs explicitly. Historical replay uses a separate project/key namespace in the same shared deployment, never the live epoch. Changing Kafka offsets or losing state must not silently reuse a supposedly complete live namespace. Kafka retention, checkpoint retention and evidence retention must support the selected recovery horizon. Infrastructure checkpoints belong in owned runtime storage, separate from immutable synthetic-source inputs.

### 5. Immediate velocity registration uses separate Redis

The synchronous path is Payments' durable start → Fraud's durable assessment claim → parallel velocity registration and shadow Feast retrieval → rules evaluation → durable result → Payments' authoritative commit. Kafka/Flink are never awaited. KServe is a later direct Go adapter.

Use a dedicated authorization Redis deployment, separate from both Feast Redis and the existing Payments idempotency cache. Independent credentials, resource budgets, noeviction, health and recovery epochs prevent feature workload/eviction from affecting the guard. Prefer bounded durable Redis configuration, but do not claim failover cannot lose recent registrations; loss/recovery marks short-window coverage unknown until rebuilt or warmed.

Use one atomic script per scoped entity to deduplicate the attempt, record its original registration at Redis server time and return 10s/60s counts including that attempt. Define windows as (r-W,r] at the guard's millisecond precision; simultaneous distinct attempts share timestamps but remain distinct members. Registration binding/digest is stable across retries. Time-window membership and longer dedup retention are different data lifetimes; retries never refresh original registration. Retention must cover the bounded retry horizon, and expired unsafe retries fail rather than becoming new registrations.

Customer and method scripts may execute concurrently, but are independent transactions and can partially succeed. Retry safely completes the missing side; record each status. No global cross-slot Lua operation or lock is introduced. These are submission counts, not committed assessed counts: never add them to Feast or the model vector.

Start observe-only with configurable thresholds; missing counts are unknown. Enforcing thresholds or requiring guard availability is a separately explicit policy activation after measured smoke evidence. Existing deterministic rules can still decide during observation failures; an enabled mandatory guard must fail closed as an operational failure, not a fraud decline.

### 6. Durable assessment state and one authoritative outbox

Add a Fraud-owned assessment table/database on the existing Postgres cluster with least-privilege credentials. This storage has a concrete responsibility: stable results when snapshots/counters change between retries. Unique scoped payment/attempt identity and validated request digest bind the request. Use NEW → ASSESSING → COMPLETED or ASSESSMENT_ERROR, with fenced leases and bounded error recovery. COMPLETED has the existing APPROVE/DECLINE outcome; do not introduce REVIEW in this slice. Per-entity registration statuses are evidence, not a replacement durable state.

Persist completed evaluation and evidence before responding. Payments commits that result/evidence linkage with RiskDecisionRecorded and its state transition using the existing ledger/CDC transactional-outbox mechanism. Fraud's persisted completion is a reusable evaluation result; only the committed Payments event is authoritative historical eligibility. There is deliberately no second fraud.assessed publication and no cross-database transaction. A caller can recover the stored evaluation if its response or Payments commit was lost. Payments version fencing prevents a late result authorizing an expired payment; unused evaluations remain traceable.

On execution failure persist a distinct operational status; no processor intent or fabricated decline is created. Payments exposes recoverable assessment failure via its current attempt/query/RPC boundary and recovery resumes the same identity under configured backoff/budget. Before processor intent, normal payment expiry can end recovery; after intent, existing unknown-outcome recovery remains authoritative. Update affected callers, state queries, fixtures and documentation together, removing the old fallback path.

Store actual observed/used feature values, both view IDs/revisions/anchors, read/availability timestamps, coverage/freshness decisions, guard registration/count statuses, digest and policy/definition versions. Persist immutable evidence with the Fraud result and durable Payments linkage; protect retention so linkage remains resolvable. Separate observed-only features from inputs actually used. Never include raw credentials or oracle/label fields. Existing request validation stays intact.

### 7. Failure and rollout modes

| Dependency | Initial mode | Failure behavior |
|---|---|---|
| Authorization Redis | Observe-only | Unknown counter observations; healthy rules still run |
| Feast | Shadow retrieval | Missing/stale/incompatible values recorded; healthy rules still run |
| Fraud execution/result persistence | Required | Operational error; no processor call; same-identity recovery |
| Kafka/Flink/feature writer | Asynchronous | Payments independent; lag/coverage makes shadow history unavailable |
| Future enforced guard/model | Not activated here | Explicit policy and missing-input behavior required before activation |

Use separate configurable total/call deadlines under the existing Payments attempt budget, cancellation and bounded concurrency. Parallel calls avoid serial latency, but do not promise a numerical SLO before measurement. Never silently approve when a required assessment cannot complete. Observe-only degradation is explicitly permitted, not an accidental fail-open enforcement path.

## Risks / Trade-offs

- [Simultaneous assessments share historical snapshots] → Accept this as the versioned snapshot contract; synchronous short-window guard observations cover immediate submissions independently.
- [Redis failover loses submissions] → Persistence, coverage epochs and explicit mandatory-guard failure policy; no strict durable global-rate guarantee claimed.
- [Independent views disagree in time] → Preserve both actual vectors/statuses; no coordinated snapshot claim.
- [Timer/state and durable assessment writes cost latency/resources] → Measure smoke load, enforce bounded state/deadlines and retain evidence before tuning scale.
- [Feature source history or Kafka retention is incomplete] → Per-window unknown coverage and controlled epoch bootstrap; a watermark does not manufacture completeness.
- [Crash occurs across Fraud and Payments commits] → Durable result replay plus Payments fencing and existing at-least-once outbox; no distributed transaction claim.

## Migration Plan

1. Update active feature definition/reference/examples, coherent delta specs and affected documentation; leave published v16 bytes/evidence unchanged.
2. Pin supported runtime versions and coordinate Flink controller, owned checkpoint storage, Feast registry and private deployment prerequisites with the platform repo. Reuse existing Postgres/Kafka; provision separate authorization and Feast Redis.
3. Deploy aggregation/writer/server with explicit live epoch and current-time source. Run historical replay in a separate epoch through CI; do not bootstrap old July events as current history.
4. Roll out durable Fraud/Payments assessment behavior and shadow/observe-only adapters together. Keep model serving absent and existing valid rules active.
5. Run focused CI recovery/conformance and deployed smoke exercises, recording feature lag, request latency, resources and errors. Update roadmap status only from accepted evidence.
6. On rollout failure disable shadow reads and velocity observation first. Retain durable result replay and committed evidence. Stop writers before changing active epochs; never restore the obsolete error-to-decline behavior as an implicit fallback. Restore application releases only with an explicit compatible persistence plan.

## Open Questions

- Exact supported Flink/Feast releases and resource requests are implementation selections to verify against official docs and platform capacity.
- Numerical freshness, lateness, retention, timeout and velocity-threshold values require measured smoke results. They remain explicit configuration; enforcement is not activated by this proposal.
