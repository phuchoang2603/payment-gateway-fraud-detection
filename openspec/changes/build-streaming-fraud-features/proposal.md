## Why

The gateway now has self-contained attempt events and a published 90-day development source, but no deployed historical feature computation or retrieval. Asynchronous assessed-history features also cannot detect submissions arriving together before those assessments reach the stream processor.

## What Changes

- Build one continuous PyFlink job with independently keyed customer and payment-method branches, a durable feature-update Kafka journal, a Python Feast SDK writer and the supported Feast feature server.
- **BREAKING:** replace exact request-anchored baseline serving semantics with explicitly versioned timestamped snapshots, hybrid event/expiration emission, per-window coverage and per-view freshness. Keep the ten model feature quantities; add no velocity counters to the model vector.
- Add synchronous integration-scoped 10s/60s submission counters, atomically registered/deduplicated per entity, on an authorization Redis deployment separate from Feast Redis. Begin with observe-only rules.
- **BREAKING:** replace stateless assessment retries with durable, fenced assessment execution and replay of persisted results/evidence. Distinguish operational assessment errors from fraud declines while continuing to block authorization when no valid decision exists.
- Reuse Payments' transactional event ledger and committed-event publication as the outbox for authoritative RiskDecisionRecorded; do not introduce a competing fraud.assessed payment event.
- Exercise feature replay, online retrieval, concurrent submissions, response loss and recovery through CI using the existing 90-day smoke source and current-time live generation. Record real results and limitations before enforcement.

## Capabilities

### New Capabilities

- `streaming-fraud-features`: continuous assessed-history aggregation, immutable snapshot updates, replay-safe Feast writes and retrieval evidence.
- `authorization-velocity`: immediate per-entity submission registration and short-window rule observations, independent of assessed-history features.

### Modified Capabilities

- `fraud-feature-contracts`: snapshot semantics, unchanged ten-feature baseline, versioned definitions, availability/coverage and reproducible serving evidence.
- `fraud-decision`: durable assessment ownership/retries, bounded dependency adapters and independent degradation semantics.
- `payment-processing`: assessment failure/recovery without fabricated fraud declines, and authoritative decision/evidence commit through the existing ledger outbox.
- `payment-contracts`: eligibility of finalized assessments and retryable assessment-error behavior before the existing decision transition.

## Impact

Affected areas include contracts/features and its pure reference/examples; Go Fraud and Payments coordination, persistence and relevant protobuf response/evidence fields; new Python streaming/Feast runtime; Kafka feature topic; Helm/GitOps runtime prerequisites, checkpoints and two separate Redis deployments; existing ML/data-pipeline/roadmap guidance and focused CI coverage. Platform controller installation remains coordinated with the platform owner.

No payment-source regeneration, source deletion, legacy adapters, scale generation, Spark simulator, DP1 implementation, trained model, KServe rollout or strict cross-entity transaction is included. Historical sources and accepted evidence remain immutable. Snapshot offline computation and Kubeflow/KServe integration follow later; this change defines their evidence interface without claiming training readiness.
