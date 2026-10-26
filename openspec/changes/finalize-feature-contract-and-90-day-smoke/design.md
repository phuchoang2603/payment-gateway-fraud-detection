# Design

## Context

See [proposal.md](proposal.md) for motivation and scope. `payments.v1.PaymentEvent` already separates lifecycle facts. `PaymentCreated` carries merchant/customer/snapshot; `AttemptStarted` carries attempt ID/signals. Payments commits the start and durable idempotency claim before calling Fraud, then commits the risk assessment. The Fraud RPC already receives the complete context and currently evaluates deterministic rules only.

The Python generator builds the same lifecycle directly, and the archiver uses its Arrow schema to decode live protobuf to typed Parquet. The maintained smoke configuration is v13, six days and 100,000 customers; current Arrow shapes support versions 1/2. Published v13 is immutable. The operator has stopped the incomplete v14 scale run. Existing ML docs select Go Fraud -> supported Feast feature-server API -> KServe, without a Python scoring coordinator.

The current worktree has invalid Git metadata and lacks root contracts/docs. Planning reads those from `/home/felix/repos/realtime-credit-card-fraud-detection` without modifying that checkout. Restore a valid complete worktree before implementation; reconcile its HEAD and concurrent DP1 documentation rather than assuming the reference checkout is the implementation base.

## Goals / Non-Goals

**Goals:** define one current producer/landing contract and one modest feature set; publish and exercise a 90-day development source with explicit maturity and provenance; prepare concrete downstream interfaces without deploying them.

**Non-Goals:** implementing Flink, Feast, Spark DP1-DP3, training or KServe; changing rules-v1 outcomes; adding synchronous velocity reservations; claiming online/offline parity before both pipelines exist; generating 100 GB or deleting existing source datasets. Smoke production/acceptance runs via CI-driven/deployed execution, not workstation tests.

The operator has since removed the old source data. Current readers and configuration support schema 3 only; historical evidence remains a record, not a compatibility requirement. New manifests defer quality acceptance to `dp1-starrocks-openmetadata`, aligned with the owning DP1 plan, without relabeling older gate names or outcomes.

## Decisions

### 1. Enrich the existing attempt fact; retain payment durability

Keep existing lifecycle payloads and the current Fraud RPC. Add a shared `AttemptContext` containing existing `MerchantRef`, `CustomerRef` and immutable `CheckoutSnapshot`; add it to `AttemptStarted` alongside its existing `attempt_id` and `signals`. Use the same values as Payments state and the Fraud request. These are observable scoring inputs, not precomputed aggregates, outcome information or labels. Do not introduce raw IP/card/token fields or claim shipping geography is IP location. No new collection requirement or external lookup is needed.

Allocate a new additive protobuf field number and advance the event payload/physical schema to 3, retaining existing field meanings and package names. Require context on schema-3 attempts at validation boundaries; protobuf message presence alone cannot enforce it. Update the current Go/Python producers, persisted ledger serialization, readers, Arrow shapes, ODCS/product versions and schema references together. Do not rewrite stored events to enrich them. New generation and its live acceptance select the current shape only; unsupported old records fail visibly or remain in a retained legacy epoch handled by its pinned revision, without a new compatibility adapter. Do not delete old records or shared topics.

Reusing `PaymentCreated` only would retain a payment-context join. Combining lifecycle facts would destroy their different meanings. A separate independently published feature event would add dual-publication coordination. Enriching the durable existing event is the smallest coordinated change. Context remains plain domain data internally; generated messages stay in wire adapters.

### 2. One small feature contract, with reference computation rather than a framework

Add `contracts/features/fraud-baseline-v1.yaml` for machine-readable definitions and compact conformance vectors beside it. Provide a dependency-light pure Python reference calculation under `src/data-pipeline/features/` using ordinary typed inputs; no Spark/Flink imports, generic expression engine or new service. Definitions and examples specify names, types, units, entity keys, lifecycle eligibility, time anchors, exact window boundaries, availability cutoff, null/default behavior and a canonical definition hash. Future frameworks reuse pure transforms where appropriate and prove their native state/window implementations against the same examples; they need not execute identical framework code.

Initial feature set:

- Request features: amount in USD minor units; customer account age in whole elapsed hours; nullable IP/shipping-country mismatch. Account age uses observed UTC instants, not hidden profile ages. Unknown IP remains null with explicit missingness.
- History features: prior assessed-attempt counts over 1 hour and 24 hours for customer and payment-method fingerprint; prior assessed-attempt amount sum over 24 hours for customer; distinct observed devices over 7 days for customer; seconds since that customer's last eligible attempt within 7 days (null if none in a known complete window). Declined assessed attempts contribute. Success is not required. Missing devices do not form a synthetic device identity.
- Keys always include `integration_id`; customer and method identities remain distinct namespaces. Counts/sums are integer values; zero history is zero only for a known complete snapshot. Unknown/stale state is not fabricated as zero. Distinct-device count is exact for the reference contract; a later approximation requires a different feature definition/version.

This is deliberately smaller than a broad feature catalog. Scenario names, truth, chargebacks, risk scores/reasons/outcomes and future processor results are excluded from baseline model features. Merchant/device/network identifiers remain available contracted inputs for later feature versions. Do not add hidden IANA zones as observed user-location features; stored instants remain UTC and generator local-time calibration remains internal.

### 3. Separate business time, causal availability and serving freshness

The window anchor `t` is the gateway-owned `AttemptStarted.occurred_at` of the scored attempt (historical generation mirrors that meaning). A prior attempt contributes only after its committed `RiskDecisionRecorded` is available, using its own start timestamp in `[t - window, t)`. Exclude the scored attempt by scoped attempt identity regardless of timestamps. Deduplicate by event identity and reject conflicting identity/content; one assessed attempt contributes once. Both started context and risk confirmation must be available at cutoff `q`. An explicitly observed ingestion/feature-publication time further restricts availability; never replace it with event time.

Flink will read an unbounded stream with bounded windows, deduplication and unmatched-attempt state. Self-contained attempts remove the PaymentCreated join for baseline attempt features; they do not remove the bounded start/decision join. Missing context/decisions cannot create contributions silently. Later feature implementation must define timeout, quarantine/replay, watermark/lateness and stale-snapshot policies without treating expiry as a valid zero. Online updates carry feature definition, anchor/availability timestamps and monotonic revision metadata. Pipelined multi-entity reads are not an atomic global snapshot.

The source report distinguishes idealized historical availability (`recorded_at`) from observed live transport/serving availability. Historical replay cannot establish the exact vector a deployed service read. Later ML serving must persist the actual vector or immutable snapshot reference with definition/model/policy versions, cutoffs and missingness; a point-in-time event-time join alone is insufficient.

Payments continues durable start -> Fraud -> durable risk decision -> approved processor operation. It never waits for Kafka/Flink. Live state initially means prior assessed attempts, not strict counts of concurrent in-flight starts. An atomic risk reservation could be added separately if strict velocity controls become required.

### 4. Ninety days, modest population, complete tails

Use `historical/v15` provisionally; confirm absence of a manifest/owner before starting and choose another unused version on collision. Never repurpose v13 or resume stopped v14 with different contracts. Proposed maintained defaults: seed 2603; start 2025-07-01; 90 UTC purchase-start dates through 2025-09-28 inclusive; 10,000 customers, 2,000 merchants, 14,000 devices, 8,192 networks, 11,000 addresses and 13,000 payment methods. Retain US/USD electronics calibration and legitimate/fraud overlap. Set an explicit small customer block (1,000), zero minimum source-byte gate, no forced schema cutover and zero duplicate exports in the baseline. Preserve scenario probabilities/delays unless the smoke report identifies a concrete defect; do not silently inflate fraud for a desired metric.

At the current base rate, 10,000 * 90 * 0.17 gives roughly 153,000 ordinary purchases before scenarios/retries. This is a sizing estimate, not a row-count gate or throughput promise. Use measured free RAM/disk/CPU to bound workers, in-flight tasks and scratch. Keep existing resumability, verified receipts, row accounting, exclusive ownership and manifest-last publication. No tag protection or full-dataset local materialization is added.

Ninety days bounds purchase starts, not file timestamps. Include complete lifecycle tails and all scheduled label availability tails, even after September 28. Record separate payment-start, event occurrence, event recording and label occurrence/availability ranges. Preserve full label identity/linkage and distinguish oracle negatives from absent/unreported chargebacks.

Produce a compact manifest-bound report: actual rows/files/bytes; exact-versus-estimated counts; scenario/class and per-period diagnostics; feature-window warmup (maximum baseline lookback 7 days); label maturity; and candidate chronological train/validation/test cuts. At each training cutoff use only labels actually available then. Apply a label-maturity embargo where needed rather than using a random split or letting future labels cross a simulated training cutoff. Report entity/campaign overlap and usable positive/negative counts per split. Insufficient cohorts are a reported training limit, not permission to fabricate rows or claim performance. Ninety days enables pipeline/model development, not production-quality or annual-seasonality claims.

### 5. Exercise real producer/landing behavior, without resetting shared infrastructure

CI checks the new contract through representative Go/Python fixtures, generation and typed Parquet conversion; reuse existing suites. CI-built images drive a bounded Kubernetes acceptance run against the new reference manifest. Keep historical/live payment identities distinct, use current-time gateway commands, and pin manifest hash, generator/calibration/contract identities, run ID and source epoch. Do not replay historical July timestamps as current checkout time.

Where required for cutover, quiesce the owned archiver/live writer, confirm shutdown, record explicit Kafka boundary offsets and start a fresh epoch/group under `streaming/`. New-epoch selection excludes stale schema records; retained data/progress remain available for audit. No Kafka purge or old streaming-prefix deletion is needed or authorized. Check enriched event content against checkout context, source immutability, protobuf/Parquet parity, delayed labels, duplicates/delays/bursts, and restart after publication before offset acknowledgment. Retain `_transport` original bytes/key/ordered headers/topic/partition/offset and epoch/batch identity. Record duplicate overlap honestly; do not promise cross-system exactly-once.

Generation execution is an operational job. Automated tests and smoke/acceptance validation run only in CI or CI-driven deployed jobs. Source validation and live exercise reports do not replace the owning DP1 snapshot-pinned StarRocks SQL checks on persisted bronze, contextual lifecycle/linkage checks, governance readback or later training acceptance. The raw layout remains unsorted task fragments; remove the contradictory retired sorted/compacted-source requirement instead of reviving Python finalization.

### 6. Documentation and roadmap form part of completion

Update existing maintained pages, contract-generated references and linked commands after implementation; do not edit research or archive evidence into a different outcome. Audit README links, overview requirements/architecture/roadmap, payment flows/architecture, data product/source guide, data/ML pipeline guides, reference notebook defaults and affected GitOps/live run instructions. Remove obsolete six-day/current-v13 defaults, fixed v8/v12 3.9M-card requirements for new runtime selection, old schema branches, unsupported scale-start instructions and stale active-change links in the affected area. Keep v13 facts in the published-version history. Coordinate storage/DP1 status with its owning work; do not restore obsolete ClickHouse targets over an accepted StarRocks direction or mark an unverified component complete.

The updated roadmap orders work as: (1) this contract + 90-day source/live acceptance; (2) continuous Kafka/Flink feature processing and Feast online serving; (3) Spark source ingestion/curation/offline feature history and training-ready datasets, with steps 2/3 able to overlap once the contract is fixed; (4) Kubeflow training/evaluation/registration; (5) KServe integration through Go Fraud and measured latency/freshness/parity; (6) full-scale Python generation and rerun scale/coursework acceptance. Existing DP1 work continues on smoke-sized inputs. Scale is deferred until the end-to-end path is exercised, not removed from requirements.

## Risks / Trade-offs

- Duplicated checkout context increases event size -> carry existing relevant structured inputs once per attempt; measure bytes without inventing a separate event bus.
- A stopped v14 prefix may contain partial output -> leave it incomplete and unselected; cleanup needs a separate authorized operation.
- Schema-3 adoption can break current consumers -> coordinate rollout, freeze epoch boundaries and keep pinned old evidence; no in-place mutation or compatibility shim.
- A small source may underrepresent rare fraud -> report cohort counts and limitations; larger generation follows pipeline acceptance, not a synthetic model-quality claim.
- Historical replay has idealized availability -> distinguish it from live timing and later log actual served vectors.
- Assessed-history features miss concurrent pending attempts -> document the semantics; strict synchronous reservations remain separate.
- Current docs and another DP1 checkout may differ -> reconcile maintained architecture/status during application without changing immutable evidence.

## Migration Plan

1. Recover a valid complete implementation worktree, inspect existing revisions and check stopped v14 has no active writer; do not repair or resume it as a side effect.
2. Land contract/feature artifacts, coordinated producers/consumers, configs and docs with CI validation before changing source selection.
3. Generate/publish the unused 90-day version and collect CI/deployed validation reports. Select it only after its publication and required source checks succeed.
4. Roll out coordinated current-contract gateway/archiver revisions, establish explicit new-epoch offsets and run bounded live acceptance; report failure/pending states without starting scale.
5. Rollback by stopping the affected new run and restoring matched prior images/config/epoch selection if needed. Never rewrite stored ledger/source objects or let old readers consume new-shape epochs blindly.

## Open Questions

- Exact unused historical version, measured worker/scratch settings and final report URIs are execution facts to record during application; they do not change scope.
- Future Feast/Spark adapters and Flink lateness/state-recovery settings belong to their implementation proposals; the semantic and conformance contract here remains their input.
