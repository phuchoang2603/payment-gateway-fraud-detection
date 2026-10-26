## Purpose

Keep verification small and honest: behavior tests that protect fraud decisions and minimal independent CI without score-only tooling.

## Requirements

### Requirement: Behavior-directed tests

Tests SHALL protect fraud decision rules, their thresholds and required decision inputs. Fraud decision rule tests SHALL NOT cover configuration validation, observability or gRPC/HTTP transport. Tests SHALL NOT be added solely to raise scores.

#### Scenario: Decision boundaries

- **WHEN** fraud decisions are tested
- **THEN** tests distinguish the high-amount boundary, the account-age and amount boundaries of the geo-mismatch rule, and the absent-IP-country partition

#### Scenario: Required inputs

- **WHEN** a decision request lacks a required input
- **THEN** a test shows it is rejected instead of evaluated with an implicit default

### Requirement: Minimal CI

CI SHALL run independent Python, Go and Helm checks on PRs and main pushes. Go lint and tests SHALL use a module matrix in the Go workflow; Python lint/format and behavior tests SHALL use the Python workflow. Helm checks SHALL lint and render every tracked chart and its `values-*.yaml` overrides. Image builds SHALL reuse one image workflow through a service matrix, publishing same-repository PR images and main images under separate tag namespaces. Custom numerical coverage/mutation gates and screenshot generation SHALL NOT be required.

#### Scenario: Review a pull request

- **WHEN** a PR is opened or updated
- **THEN** CI verifies each service implementation, using checked-in protobuf bindings where the service consumes contracts
- **AND** the reusable image workflow builds each service, publishing same-repository PR images and keeping fork PR builds without publication

#### Scenario: Go defect or formatting drift

- **WHEN** any Go module fails to type-check, violates an enabled analyzer or is not gofumpt-formatted
- **THEN** that module's Go job fails with the reported findings

### Requirement: Honest evidence

The PR and CI logs SHALL record the revision, commands and results of current
verification. Claims SHALL cover only the behavior and environment exercised.
Local checks SHALL be proportional; CI SHALL own full image verification.

#### Scenario: Current verification scope

- **WHEN** a change reports passing checks or workload acceptance
- **THEN** its report identifies the current revision, executed checks and remaining unverified behavior

### Requirement: Generator behavior tests

Simulator tests SHALL protect reproducibility for a small fixed configuration, identity vectors, Arrow schema parity with ODCS, schema evolution, duplicates, format-3 raw task inventory, physical schema/date counts, truth separation and publication refusal on integrity or byte failure. Small tests SHALL also protect worker-independent records, verified resume, publication exclusivity and resource/size failure gates. They SHALL use local outputs and storage fault doubles without requiring object storage or network access.

#### Scenario: Offline test run

- **WHEN** simulator tests run in CI without storage credentials
- **THEN** they generate to a temporary directory and verify raw publication behaviors without native DuckDB validation

#### Scenario: Interrupted upload and changed execution settings

- **WHEN** a small run stops around upload/checkpoint acknowledgement and resumes with changed parallelism
- **THEN** its records, counts, physical schemas and duplicate behavior equal an uninterrupted run
- **AND** corrupt output, incompatible inputs or a competing writer cannot be silently accepted

#### Scenario: Bounded-resource failure

- **WHEN** an injected scratch or upload limit prevents completion
- **THEN** the test verifies diagnostic failure and absent completion manifest while valid checkpointed work remains resumable

#### Scenario: Normal command lifecycle

- **WHEN** the same command is run against prepared or completed output
- **THEN** it automatically resumes compatible preparation or finishes cleanup without additional mode flags
- **AND** prepare-only leaves generated inventory ready without sorting, and completion declares skipped source checks with the DP1 gate

#### Scenario: Publication failure and retry

- **WHEN** post-publication cleanup partially fails
- **THEN** tests verify a visible failure, accurate successful-delete accounting and recovery by rerunning the same command without rewriting completed output
- **AND** cleanup removes only transient `_run/` objects and retains every listed raw source file

### Requirement: Go service tests

The Go CI job SHALL lint and test every Go module.

#### Scenario: Complete go service tests contract

- **WHEN** the `Go service tests` capability is configured or exercised
- **THEN** The Go CI job SHALL lint and test every Go module. Integration dependencies SHALL use helpers in the single shared Go module both locally and in CI, without TEST_* overrides or CI service containers. Integration tests SHALL skip only under `go test -short`; missing Docker otherwise SHALL fail the test. End-to-end smoke and access checks SHALL run only on the prod Kubernetes cluster after GitOps rollout; services and databases SHALL NOT be manually started with Docker for end-to-end checks. Payments tests SHALL exercise the payment state machine with compact table-driven direct domain commands, and SHALL cover creation and attempt idempotency conflicts, concurrent same-key requests, retry after process restart, fail-closed fraud handling, unknown-outcome recovery including crash after intent, and expiry blocked by pending outcomes. Versioned state transitions, ledger immutability and idempotency persistence SHALL be tested against a real Postgres started by shared testcontainers-go helpers both locally and in CI, including rejected ledger updates/deletes and transactions whose global sequence allocation order differs from commit order. Processor simulator tests SHALL cover sale idempotency and timeout-then-status. Checkout and simulator boundary tests SHALL verify rejection of raw card credentials. Tests SHALL NOT exercise HTTP or gRPC transport except through these behaviors.

  Edge tests SHALL cover configured-integration authentication and the synthetic-only payment-method route. Vault tests SHALL cover integration scoping, test-card allow-list mapping, opaque token issuance, encrypted fixture persistence and stable fingerprint vectors. Payments recovery tests SHALL prove that private encrypted operation requests permit recovery after a crash without leaking tokens into events. Redis integration tests SHALL cover atomic key claims under concurrent requests, in-flight and final results, content conflicts, and flush/outage fallback to Postgres without another processor effect. Recorded processor status SHALL remain available during a vault outage. Notifier tests SHALL cover signature vectors, one delivery per event ID across re-consumption, and retry with a stable event ID and body after receiver errors. Outbox publication SHALL be verified in controlled evidence (commit order, payload equality, restart and broker outage without loss) rather than tests of connector configuration. Hosted page tests SHALL cover token-to-cookie exchange, cross-site submission rejection, double submit, decline-then-retry and rejection of card numbers outside the allow-list. Acceptance SHALL report the documented [functional](../../../docs/overview/requirements.md#functional-requirements) and [non-functional](../../../docs/overview/requirements.md#non-functional-requirements) targets with nominal and injected-fault evidence separately; bank-file reconciliation is outside scope.

#### Scenario: Stale transition in CI

- **WHEN** the store integration test applies a transition with a stale expected version against CI Postgres
- **THEN** the transition is rejected, no ledger event is inserted, and the test passes
### Requirement: Measured generator scale evidence

Scale acceptance SHALL record the source/configuration/contract revisions, pilot and full-run commands, hardware, seed, elapsed time, throughput, peak process-tree memory and scratch, physical/uncompressed raw source bytes, additive distributions and separately identified downstream validation reports. CI SHALL NOT generate 100 GB. Estimates and slices SHALL be labelled and SHALL NOT substitute for a measured full-source run.

#### Scenario: Accept the large dataset

- **WHEN** an operator reports completion of the 100 GB dataset
- **THEN** the evidence identifies its immutable completion manifest and actual source-file byte total, separate full-data Spark/GX results for both persisted Bronze tables plus verified governance through DP1 and measured resource use
- **AND** it distinguishes pilot projections and subset checks from full-run observations

### Requirement: Raw inventory behavior tests

Small tests SHALL verify `manifest_version: 3`, `source_layout: task-fragments-v1`, exact `raw/{table}/{task.key}/v{schema_version}.parquet` keys and per-file checksum/bytes/rows/schema version/`partitions: {date: rows}`. They SHALL exercise multi-date fragments, delayed labels, physical schema cutover and duplicate preservation without a sorting or compaction assumption. Reports SHALL distinguish configured pools from observed additive counters and mark cardinality validation downstream.

#### Scenario: Exact DP1 consumption

- **WHEN** a source root has listed raw files and additional unlisted objects
- **THEN** DP1 validates and consumes exactly the listed files, derives Bronze dates from row timestamps and preserves duplicate deliveries and delayed labels
- **AND** mismatched integrity/schema/count metadata or failed Spark/GX quality prevents Bronze acceptance

#### Scenario: No permanent legacy migration path

- **WHEN** inspecting the maintained simulator command and runtime surface
- **THEN** no legacy migration command, fragment-copy adapter or retired-configuration normalization remains
- **AND** core tests retain coverage of compatible generation resume, manifest integrity and publication retries

### Requirement: Historical evidence boundary

Legacy pilot, separate-process native-validation measurements and prior checked tasks SHALL remain explicitly historical. They SHALL NOT establish implementation, new raw-layout measurements or live `v8` validation. The product-version change to `5.0.0` SHALL preserve logical ODCS contracts.

#### Scenario: New scale evidence

- **WHEN** new raw-publication or full-run quality acceptance is reported
- **THEN** evidence distinguishes measured raw bytes and additive statistics from old compacted/native-validation measurements, projections and downstream cardinality checks
- **AND** no validation claim is inferred for existing live `v8`

### Requirement: Cross-language simulation contracts

CI SHALL reuse coverage and add only distinct wire/identity/retry checks: packaged generated protobuf contracts, historical-compatible Parquet decoding, profile mismatch, current-time starts, historical mapping and idempotency. It SHALL verify tokens/card numbers stay out of logs/Kafka. Dedicated config/default/getter tests, exhaustive per-card tests and exact wall-clock assertions SHALL NOT be required.

#### Scenario: Protobuf events and labels

- **WHEN** CI decodes representative contracted protobuf events/labels through the Python landing adapter
- **THEN** their Parquet business columns match the existing historical Arrow schemas while original wire bytes remain available without registry framing

### Requirement: Delivery-fault runtime verification

Verification SHALL reuse focused scenarios for deterministic selection, no-fault pass-through, bounded backlog and acknowledged-prefix recovery across shared-process restart. Broker-backed recovery tests SHALL remain opt-in against an explicitly supplied integration broker; default Python CI SHALL NOT start Kafka or require Testcontainers. It SHALL NOT add redundant logging-shape or render-only tests. Evidence SHALL distinguish deliberately injected duplicates from possible restart replay and SHALL NOT claim exactly-once recovery.

#### Scenario: Workload restarted with pending publications

- **WHEN** the integration recovery check restarts the workload while events are held or awaiting acknowledgments
- **THEN** every clean record is eventually forwarded and no committed position skips unfinished work

### Requirement: Immutable landing and DP1 interface evidence

Archiver verification SHALL demonstrate historical-compatible Parquet business schemas and byte/provenance preservation, duplicate conservation, low-traffic rotation, completed-object visibility and stable selected objects across retries. The written DP1 handoff SHALL specify late-file discovery, immutable batch retry/readback, contextual history and label maturity; implementation/evidence of DP1 multi-source ingestion is deferred to its next slice. A failed prerequisite SHALL block rollout.

#### Scenario: Archiver restart after publication

- **WHEN** archival restarts after writing a completed object selected by ingestion
- **THEN** that object remains unchanged and records/provenance remain recoverable with any replay duplicates identified

### Requirement: Historical and runtime evidence boundaries

Verification SHALL preserve source datasets, published contracts/bands and prior passed/failed results. Generator diagnostics SHALL remain distinct from enforced checks under the implemented DP1 policy. Historical acceptance and one-off publication SHALL NOT imply live or v12 DP1 acceptance. Tests/builds SHALL run in CI; local verification SHALL use non-test checks.

#### Scenario: Citing historical evidence

- **WHEN** an accepted v11 run or published v12 rewrite is cited during rollout
- **THEN** it remains separate from separately measured live recovery/landing and still-pending DP1 acceptance evidence
