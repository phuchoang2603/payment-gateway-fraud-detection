## 1. Shared contracts and reference

- [x] 1.1 Replace active exact-baseline definitions/examples with hashed fraud-snapshot-v1 semantics, unchanged ten quantities, scoped keys and per-window coverage; verify hand-checkable expected outputs and definition identity through focused CI conformance cases.
- [x] 1.2 Update the pure reference for snapshot anchors, eligibility, hybrid expiration, partial coverage and elapsed-time derivation; verify CI cases cover boundary inclusion, repeated devices, unknown history and historical logical clocks without duplicating existing assertions.
- [x] 1.3 Define feature-update/evidence contracts with epoch, revision, separate anchor/write/publication/availability times and exact units; update necessary protobuf assessment/status fields and generated callers together, preserving existing source rows; verify serialization and unsupported-version behavior in existing CI coverage.

## 2. Runtime prerequisites

- [x] 2.1 Pin compatible Flink/PyFlink/Python/JVM/Kafka and Feast versions using current official docs/Context7 where available; deliver locked dependencies/images and verify image builds in CI, including selected Feast timestamp/nullability behavior.
- [x] 2.2 Add repo-owned charts/config for the streaming job, feature topic, SDK writer, Feast registry/server and separate authorization/Feast Redis; coordinate controller/checkpoint-storage prerequisites with the platform owner and verify CI Helm checks plus deployed resource separation/readiness evidence.
- [x] 2.3 Provision a least-privilege Fraud-owned assessment database on the existing cluster, secrets and private network access; verify migrations/permissions through CI integration coverage and render review without adding config-only test suites.

## 3. Continuous assessed-history snapshots

- [x] 3.1 Implement protobuf decoding and bounded, conflict-aware event/attempt matching with observed availability; verify duplicate, reordered, undecided and expired-unmatched cases through focused CI coverage.
- [x] 3.2 Implement customer/method keyed state and hybrid live/logical-clock timers with exact seven-day device expiration; verify reference parity and quiet-entity expiration in CI, recording bounded state/timer behavior.
- [x] 3.3 Publish checkpoint-recoverable snapshot updates with stable identities, revisions, provenance and coverage; verify CI restart/replay and late-correction cases without cross-system exactly-once claims.

## 4. Feast writing and retrieval

- [x] 4.1 Add two scoped Feast views and an ordered SDK writer that commits Kafka progress after accepted writes, preserves monotonic write timestamps and isolates epochs; verify CI crash-after-write, older/equal update and incompatible-definition cases against the pinned Redis adapter.
- [x] 4.2 Add a bounded Go Feast adapter behind the existing Fraud boundary with independent customer/method statuses, coverage/freshness evaluation and shadow evidence; verify timeout, future/stale/partial views and request-time elapsed derivation in focused CI cases.

## 5. Synchronous velocity and durable assessment

- [x] 5.1 Implement atomic per-entity 10s/60s registration/count scripts with stable attempt binding, server time, bounded dedup retention and observe-only thresholds; verify CI concurrency, partial success, lost response, expired retry and degraded Redis recovery behavior.
- [x] 5.2 Add durable Fraud assessment claims, digest conflicts, fenced recovery and completed-result/evidence replay; verify CI response-loss, concurrent replica and stale-owner cases reuse results despite changed counters/snapshots.
- [x] 5.3 Integrate parallel guard/shadow feature reads under bounded deadlines while preserving pure rules; verify existing healthy outcomes and distinct per-dependency failure statuses in CI, with no additional model features or implicit enforcement.
- [x] 5.4 Replace Payments' fabricated unavailability-decline fallback with operational assessment status and same-identity recovery/expiry; atomically link finalized evidence to the existing decision ledger/outbox, and verify CI timeout, lost-commit response, late result after expiry and downstream duplicate-delivery behavior without a second assessment event.

## 6. Exercise and documentation

- [x] 6.1 Review and reuse existing CI logical-clock/reference-parity, expiration, epoch-isolation, SDK readback and checkpoint-recovery integration coverage; preserve immutable v16 source and historical evidence. Full 90-day replay is outside this slice's acceptance and remains unverified.
- [ ] 6.2 Review and retain the recorded bounded live verification of payment outcomes/retries, durable evidence linkage, Feast readback, populated-checkpoint recovery and resource/latency observations. Keep freshness/coverage/startup limitations explicit; no additional CI-managed cluster exercise or CI cluster/source credentials are required, and sustained-load/broad deployed outage acceptance is not claimed. Enable the requested low-rate continuous simulator and retain deployed payment/feature/checkpoint observations.
- [x] 6.3 Update only affected ML/data-pipeline/payment-flow guides and roadmap sections, removing superseded exact-online/stateless/error-decline references; verify links, diff scope and honest acceptance statuses, keeping DP1/offline training/model rollout separately pending.
- [ ] 6.4 Run relevant CI checks/builds, inspect every result and review cleanup for dead paths or redundant tests; use only formatting/schema/diff checks locally and report unresolved checks without claiming acceptance.
