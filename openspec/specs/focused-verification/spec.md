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

### Requirement: Go service tests

The Go CI job SHALL lint and test every Go module.

#### Scenario: Complete go service tests contract

- **WHEN** the `Go service tests` capability is configured or exercised
- **THEN** The Go CI job SHALL lint and test every Go module. Integration dependencies SHALL use helpers in the single shared Go module both locally and in CI, without TEST_* overrides or CI service containers. Integration tests SHALL skip only under `go test -short`; missing Docker otherwise SHALL fail the test. End-to-end smoke and access checks SHALL run only on the prod Kubernetes cluster after GitOps rollout; services and databases SHALL NOT be manually started with Docker for end-to-end checks. Payments tests SHALL exercise the payment state machine with compact table-driven direct domain commands, and SHALL cover creation and attempt idempotency conflicts, concurrent same-key requests, retry after process restart, fail-closed fraud handling, unknown-outcome recovery including crash after intent, and expiry blocked by pending outcomes. Versioned state transitions, ledger immutability and idempotency persistence SHALL be tested against a real Postgres started by shared testcontainers-go helpers both locally and in CI, including rejected ledger updates/deletes and transactions whose global sequence allocation order differs from commit order. Processor simulator tests SHALL cover sale idempotency and timeout-then-status. Checkout and simulator boundary tests SHALL verify rejection of raw card credentials. Tests SHALL NOT exercise HTTP or gRPC transport except through these behaviors.

  Edge tests SHALL cover configured-integration authentication and the synthetic-only payment-method route. Vault tests SHALL cover integration scoping, test-card allow-list mapping, opaque token issuance, encrypted fixture persistence and stable fingerprint vectors. Payments recovery tests SHALL prove that private encrypted operation requests permit recovery after a crash without leaking tokens into events. Redis integration tests SHALL cover atomic key claims under concurrent requests, in-flight and final results, content conflicts, and flush/outage fallback to Postgres without another processor effect. Recorded processor status SHALL remain available during a vault outage. Notifier tests SHALL cover signature vectors, one delivery per event ID across re-consumption, and retry with a stable event ID and body after receiver errors. Outbox publication SHALL be verified in controlled evidence (commit order, payload equality, restart and broker outage without loss) rather than tests of connector configuration. Hosted page tests SHALL cover token-to-cookie exchange, cross-site submission rejection, double submit, decline-then-retry and rejection of card numbers outside the allow-list. Acceptance SHALL report nominal and injected-fault evidence separately; bank-file reconciliation is outside scope.

#### Scenario: Stale transition in CI

- **WHEN** the store integration test applies a transition with a stale expected version against CI Postgres
- **THEN** the transition is rejected, no ledger event is inserted, and the test passes

### Requirement: Cross-language simulation contracts

CI SHALL reuse coverage and add only distinct wire, identity and retry checks: packaged generated protobuf contracts, protobuf decoding into the Bronze schema, current-time starts, historical mapping and idempotency. It SHALL verify that tokens and card numbers stay out of logs and Kafka. Dedicated config/default/getter tests, exhaustive per-card tests and exact wall-clock assertions SHALL NOT be required.

#### Scenario: Protobuf events and labels

- **WHEN** CI decodes representative contracted protobuf events and labels as DP1 Kafka mode does
- **THEN** their business columns have the same types as the same records ingested from generated historical Parquet

### Requirement: Native ClickHouse integration evidence
Implementation SHALL verify direct S3-compatible SigV4 access, external-catalog discovery, nested schemas, snapshot-specific SQL and native OpenMetadata passing/failing quality results with the selected versions. This SHALL be implementation acceptance rather than a per-batch disposable compatibility stage. Previous GX/native acceptance SHALL NOT establish acceptance of the rewritten integration or survive as an active compatibility path. SQL execution failure or unsupported discovery SHALL be reported without a hidden legacy fallback.

#### Scenario: Native external-catalog discovery is incomplete

- **WHEN** ClickHouse can query a table but OpenMetadata cannot natively discover or profile its external-catalog identity correctly
- **THEN** integration readiness remains unverified and DP1 does not claim completed native acceptance

### Requirement: Late and duplicate injection verification

Verification SHALL reuse focused scenarios for seeded late and redelivery selection, zero-rate pass-through, store-and-forward click-time preservation and byte-identical same-partition redelivery that skips its own copies. The broker-backed redelivery test SHALL start its own disposable Kafka through Testcontainers in default Python CI, without externally supplied broker configuration. It SHALL NOT add redundant logging-shape or render-only tests. Evidence SHALL distinguish deliberately injected duplicates from possible restart replay and SHALL NOT claim exactly-once recovery.

#### Scenario: Selected event is redelivered

- **WHEN** the CI broker check publishes one event with redelivery selected
- **THEN** exactly one marked, byte-identical copy appears on the same partition and is not copied again

### Requirement: Rebuilt generator behavior tests

Simulator tests SHALL protect reproducibility for a small fixed configuration across worker counts and interruption, identity vectors, the entity world rebuilt from a recorded configuration, the schema version change boundary, identical duplicates, the per-day layout and manifest, refusal of a changed configuration or published version, and truth/chargeback separation. They SHALL use local outputs without object storage or network access.

#### Scenario: Offline test run

- **WHEN** simulator tests run in CI without storage credentials
- **THEN** they generate to a temporary directory and verify per-day files, the manifest and the schema change boundary

#### Scenario: Interrupted run

- **WHEN** a small run stops after some days and is rerun with a different worker count
- **THEN** its records and manifest equal those of an uninterrupted run

### Requirement: Bronze and Silver behavioral verification
Bounded CI fixtures SHALL protect the distinct behavioral risks: historical listing, per-file row checks and idempotent rerun, Kafka offset continuity, empty runs, Kafka and historical decoding parity, and atomic offset commits, lossless casting, Silver identical-duplicate collapse and conflict failure, attempt outcomes and lifecycle validity, every label linkage class and availability violation, rerun equality, and contract-to-native-test preparation. Broker-backed checks SHALL stay opt-in like existing broker tests. No configuration-only suites, per-option matrices, new CI jobs or duplicate layer assertions SHALL be added. Tests and image builds SHALL run only in CI; local checks SHALL be limited to lint, format, schema and OpenSpec validation and diff review. Ordinary CI SHALL NOT require live credentials.

#### Scenario: Verify changes without infrastructure access

- **WHEN** pipeline behavioral checks run in CI
- **THEN** local Spark and fixture data exercise the failure paths without reading live sources or contacting deployed services

#### Scenario: Label linkage cases

- **WHEN** Silver curation runs on fixtures with linked, pending and violating labels
- **THEN** each label receives its expected class and a violation fails the Silver checks
