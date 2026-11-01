## MODIFIED Requirements

### Requirement: Independent bounded dependency semantics

Historical-feature retrieval SHALL have a bounded deadline and explicit per-view statuses. Historical-feature retrieval SHALL be shadow-only wherever it is enabled. It SHALL be enabled only where the online feature store runs.

- **Clock.** Scoring SHALL capture the server decision time before retrieval, send it as scored_at, evaluate TTL and future timestamps against it, and persist the same value as EvaluatedAt. Client attempted_at SHALL NOT control freshness.
- **Request.** Retrieval SHALL request the configured model feature service in one call, keyed by the integration, customer and payment-method natural keys and raw request-context fields in the same entities map. Feast SHALL compute model request features in request_features; rules SHALL retain their own logic.
- **Statuses.** It SHALL record a status for each view:
  - `missing` when the entity has no row;
  - `expired` when the scoring time minus the view's event timestamp exceeds the view's TTL;
  - `future` when the event timestamp is after the scoring time;
  - `timeout` or `error` for dependency failures and malformed responses;
  - `available` otherwise.
- **Unknown values.** Values of a view that is not available SHALL be recorded as unknown, never as zero. Null values in an available view SHALL remain unknown.
- **Evidence.** Evidence SHALL record the feature service name, each view's status, event timestamp, read time and values, including nullable booleans from request_features. Request features SHALL be available at the scoring time, independently of stored-view TTLs.

Retrieval failure SHALL be recorded without changing the existing valid rules decision. A required dependency or assessment execution failure SHALL block authorization with an operational error, not fabricate a fraud decline or silently approve. Future model enforcement SHALL require an explicit compatible missing-feature policy.

#### Scenario: Feast unavailable during shadow rollout

- **WHEN** Feast times out while the rules evaluator remains healthy
- **THEN** the rules result remains valid and evidence records historical features as unavailable

#### Scenario: Assessment execution fails

- **WHEN** the assessment cannot complete durably
- **THEN** it records an operational error and returns no successful fraud decision

#### Scenario: Dormant customer beyond the TTL

- **WHEN** the customer's latest daily row is older than the daily view's TTL at the scoring time
- **THEN** evidence records that view as expired with unknown values, and the rules decision is unchanged

#### Scenario: Stream snapshot already counts the scored attempt

- **WHEN** the stored stream snapshot's event timestamp is after the scored attempt's start
- **THEN** evidence records that view as future and uses none of its values

#### Scenario: Store-and-forward attempt

- **WHEN** client attempted_at is minutes before the server decision time
- **THEN** request features, TTL and future classification use the server time persisted as EvaluatedAt
