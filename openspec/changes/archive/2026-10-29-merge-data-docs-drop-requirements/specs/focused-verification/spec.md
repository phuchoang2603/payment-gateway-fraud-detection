## MODIFIED Requirements

### Requirement: Go service tests

The Go CI job SHALL lint and test every Go module.

#### Scenario: Complete go service tests contract

- **WHEN** the `Go service tests` capability is configured or exercised
- **THEN** The Go CI job SHALL lint and test every Go module. Integration dependencies SHALL use helpers in the single shared Go module both locally and in CI, without TEST_* overrides or CI service containers. Integration tests SHALL skip only under `go test -short`; missing Docker otherwise SHALL fail the test. End-to-end smoke and access checks SHALL run only on the prod Kubernetes cluster after GitOps rollout; services and databases SHALL NOT be manually started with Docker for end-to-end checks. Payments tests SHALL exercise the payment state machine with compact table-driven direct domain commands, and SHALL cover creation and attempt idempotency conflicts, concurrent same-key requests, retry after process restart, fail-closed fraud handling, unknown-outcome recovery including crash after intent, and expiry blocked by pending outcomes. Versioned state transitions, ledger immutability and idempotency persistence SHALL be tested against a real Postgres started by shared testcontainers-go helpers both locally and in CI, including rejected ledger updates/deletes and transactions whose global sequence allocation order differs from commit order. Processor simulator tests SHALL cover sale idempotency and timeout-then-status. Checkout and simulator boundary tests SHALL verify rejection of raw card credentials. Tests SHALL NOT exercise HTTP or gRPC transport except through these behaviors.

  Edge tests SHALL cover configured-integration authentication and the synthetic-only payment-method route. Vault tests SHALL cover integration scoping, test-card allow-list mapping, opaque token issuance, encrypted fixture persistence and stable fingerprint vectors. Payments recovery tests SHALL prove that private encrypted operation requests permit recovery after a crash without leaking tokens into events. Redis integration tests SHALL cover atomic key claims under concurrent requests, in-flight and final results, content conflicts, and flush/outage fallback to Postgres without another processor effect. Recorded processor status SHALL remain available during a vault outage. Notifier tests SHALL cover signature vectors, one delivery per event ID across re-consumption, and retry with a stable event ID and body after receiver errors. Outbox publication SHALL be verified in controlled evidence (commit order, payload equality, restart and broker outage without loss) rather than tests of connector configuration. Hosted page tests SHALL cover token-to-cookie exchange, cross-site submission rejection, double submit, decline-then-retry and rejection of card numbers outside the allow-list. Acceptance SHALL report nominal and injected-fault evidence separately; bank-file reconciliation is outside scope.

#### Scenario: Stale transition in CI

- **WHEN** the store integration test applies a transition with a stale expected version against CI Postgres
- **THEN** the transition is rejected, no ledger event is inserted, and the test passes

