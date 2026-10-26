Verification policy: reuse existing CI tests; add only distinct behavioral coverage. No dedicated config/default/getter/logging-shape or render-only tests, exhaustive card tests or duplicate evidence harnesses. Local checks are non-test checks only.

Execution order: first exercise the live simulator against the gateway with the archiver disabled, covering current-time payments, identity continuity, labels and each delivery fault. Then exercise Parquet landing/recovery. DP1 multi-source ingestion starts only after that live evidence is reviewed; an interface handoff alone does not authorize starting it. RustFS streaming-write policy work does not block the simulator-only session.

## 1. Shared contracts and historical model reuse

- [ ] 1.1 Extend existing root-contract codegen/build integration to produce importable Python event/label bindings and package the compatible protobuf runtime; verify deterministic generated output and Go/Python event/label roundtrips in CI, without Avro or registry dependencies
- [ ] 1.2 Reuse historical profile/identity and behavior modules in the live runner without invoking historical event generation/export; verify manifest digest refusal and stable historical entity assignments/account context in CI
- [ ] 1.3 Rewrite vault synthetic card mapping for the complete 3.9-million-card v8 identity domain preserved in v12, removing obsolete fixture assumptions and affected references; verify the mapping covers the configured domain and reuse focused CI coverage for representative boundary fingerprints/behavior and token reissuance, without exhaustive per-card/tokenization tests without modifying source datasets

## 2. Current-time traffic and labels

- [ ] 2.1 Add one live entry point and common validated configuration for dataset, traffic, labels, faults, Kafka and landing; remove max_gap, wall-clock calendar-continuation and regenerate-history requirements from affected config/docs; inspect config loading/schema with existing tooling and verify current-time traffic across a historical gap through existing live-runtime coverage; add no dedicated config/default tests
- [ ] 2.2 Implement seeded current-date behavior/scenario scheduling and bounded async gateway tokenization/payment/attempt calls, with run-scoped idempotency keys and original-request retries; verify timeout/503/504 retries record at most one attempt and report arrival lag in CI
- [ ] 2.3 Publish protobuf simulation truth and schedule delayed/compressed chargebacks with succeeded-attempt checks and actual publication availability; verify pending/succeeded outcomes, field exclusion and reported unfinished in-memory schedules in CI
- [ ] 2.4 Coordinate traffic/label/fault loop startup, bounded blocking work, fatal failure and shutdown in one Python process; verify a failed loop is visible and shared-process shutdown reports pending work in CI, without implementing a separate deployment mode
- [x] 2.5 Update the existing simulator/architecture guidance for current-time starts, common config, historical identity reuse, token privacy and label-heap limitations; verify links and remove superseded separate-workload/max-gap instructions

## 3. Embedded post-commit delivery faults

- [ ] 3.1 Add independent named-hash selection/delay logic for duplicate/late events and periodic burst holds; verify deterministic choices across reordered input and disabled-fault behavior in CI without exact wall-clock assertions
- [ ] 3.2 Add the optional Kafka fault loop with original key/value/header forwarding, count/byte queue limits, polling during pause and acknowledged contiguous-prefix commits; verify pass-through, backlog bounds and restart-with-held/unacknowledged-records against a CI Kafka broker
- [x] 3.3 Report configured/observed faults, backlog, publication acknowledgments, fatal malformed input and replay caveats; verify evidence distinguishes committed-event delivery faults from request bursts or timestamp backdating and contains no card/token material

## 4. Go Parquet landing and DP1 handoff

- [ ] 4.1 Add a Go archiver using generated root-contract bindings and explicit historical-compatible Parquet mappings for events/labels; preserve optional presence, enums, timestamps, payload variants and ordered raw transport metadata; verify representative cross-language Parquet schema/read compatibility in CI
- [ ] 4.2 Implement record/byte/time-bounded partition batches, conditional content-addressed file publication, manifest-last completion and post-publication offset commits; verify low-volume flush, duplicate conservation, collision refusal, publication failure and replay after crash/rebalance in focused CI coverage
- [ ] 4.3 Inspect and extend the existing synthetic-source user/policy for streaming-prefix list/read/write and required multipart access; reuse Doppler credentials, historical protection and no-delete/no-artifact restrictions; verify exact policy diff and RustFS publication/recovery evidence without exposing credentials
- [x] 4.4 Write the completed-batch, Parquet business/metadata and provenance interface and hand it to the DP1 owner; defer DP1 multi-source configuration, incremental discovery/commits and contextual validation implementation/evidence to the next slice
- [x] 4.5 Update affected pipeline guidance for decoded Parquet, immutable completion, at-least-once replay and later DP1 integration; preserve published source contracts/bands and previous evidence

## 5. Simulator and landing deployment and integrated evidence

- [x] 5.1 Declare faulty/label topics and one opt-in single-replica Recreate simulator workload plus one independently enabled Go archiver under the configured prod root; verify Helm rendering has no separate injector, S3 Connect plugin or registry or dev root and keeps the shared current environment/flow
- [ ] 5.2 Add the simulator image with generated Python bindings to existing CI/release workflows and add archiver image delivery without changing CDC Connect; verify CI tests/builds and PR/main publication/pinning follow shared policies, with no local tests
- [ ] 5.3 First run a bounded GitOps simulator-only session with the archiver disabled using current machine time and historical identities, labels, each delivery fault and the combined configuration; then exercise archiver landing/recovery before DP1 multi-source work; verify authoritative histories/notifier behavior remain unchanged and completed landed objects preserve all delivered bytes/provenance
- [x] 5.4 Record the downstream DP1 follow-up boundary and preserve v11 evidence and v12's pending DP1 acceptance; do not implement or require multi-source DP1 execution in this slice
- [x] 5.5 Remove superseded two-workload/config references from affected deployment/runtime docs, record rollback by disabling the workload and sink without deleting data, and verify links, git diff checks and strict OpenSpec validation; report all pending/failed CI or runtime evidence honestly

## Implementation evidence and remaining gates

Runtime, protobuf generation, vault mapping, Go landing, images and opt-in GitOps wiring are implemented in the worktree. Local Ruff/Go lint, affected Helm lint/render, shell syntax and strict OpenSpec checks passed; no tests or image builds ran locally. CI-dependent tasks above remain unchecked until their evidence exists. CI includes wire/Parquet interoperability, HTTP retries, label maturity/shutdown, Kafka held-record replay, conditional publication/collision and Kafka-to-manifest failure/restart coverage.

Actual RustFS policy read with the existing source identity returned `AccessDenied`; no deployed policy was changed. The explicit prod kubeconfig is reachable, but rollout remains disabled pending policy inspection/extension and CI/image/RustFS/GitOps evidence. DP1 interface and v12 lineage/mixed-check caveats were delivered to thread `c09d91fe-03ee-4d8b-8a71-8f36fbe96cf5`. DP1 multi-source execution remains next-slice work.
