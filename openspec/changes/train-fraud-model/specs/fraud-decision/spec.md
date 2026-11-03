## MODIFIED Requirements

### Requirement: Independent bounded dependency semantics

Historical-feature retrieval SHALL have a bounded deadline and explicit per-view statuses. Retrieval itself SHALL NOT change a decision; only the model policy in the requirement "Model scoring and its missing-feature policy" may use the retrieved values. It SHALL be enabled only where the online feature store runs.

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

Retrieval failure SHALL be recorded without changing the existing valid rules decision. A required dependency or assessment execution failure SHALL block authorization with an operational error, not fabricate a fraud decline or silently approve. Model enforcement is governed by the missing-feature policy in the requirement "Model scoring and its missing-feature policy".

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

## ADDED Requirements

### Requirement: Model scoring and its missing-feature policy

When a model endpoint is configured, the service SHALL, after the feature read, send one feature row to the registered model on Triton within a bounded deadline, and SHALL record the model name, the version the model server reports, the call's status, the score, the read time, the threshold, and whether the score applied under the policy and whether it changed the outcome. The model call is always enforced; there is no mode that only records it. The deadline for the feature read plus the model call SHALL NOT exceed the assessment deadline.

- **Row.** Its values SHALL follow the feature order the served model was packaged with, read from the model server. A value SHALL be NaN when its view is not `available` or holds no number for it. A score from a model version other than the one the order was read for SHALL be discarded and the order read again.
- **Statuses.** `available` when the model server answers HTTP 200 with a two-class probability tensor whose fraud class is between 0 and 1 and a numeric model version; `timeout` when the deadline passes; `error` for any other failure or malformed answer.
- **Missing-feature policy.** The score SHALL apply only when the status is `available` and all of `customer_daily`, `payment_method_daily`, `customer_stream`, `payment_method_stream` and `request_features` are `available`. Otherwise the rules decision SHALL stand and the evidence SHALL record that the score did not apply.
- **Decision.** When the score applies, a score at or above the threshold on an attempt the rules approve SHALL decline it with the additional reason `MODEL_RISK_HIGH`. An applied score SHALL NOT approve an attempt the rules decline. A decision the score applied to SHALL carry policy version `rules-v1+model`, and the response SHALL carry the model version, the feature service as the feature-set version, and the score. A decision the score did not apply to SHALL carry policy version `rules-v1` and none of those fields.
- **Replay.** A replayed assessment SHALL return the recorded result without calling the model, and evidence recorded before this requirement SHALL keep its recorded hash.

#### Scenario: Enforced decline

- **WHEN** all five views are available, the rules approve and the score is at or above the threshold
- **THEN** the attempt is declined with reason `MODEL_RISK_HIGH`, policy version `rules-v1+model`, and the model version and score in the response

#### Scenario: Enforced score below the threshold

- **WHEN** all five views are available and the score is below the threshold
- **THEN** the rules approve, the policy version is `rules-v1+model`, and the score is in the response and the evidence

#### Scenario: Incomplete features

- **WHEN** a stream view is expired
- **THEN** the rules decision stands, the policy version is `rules-v1`, and the evidence records that the score did not apply

#### Scenario: Rules decline

- **WHEN** a rule declines and the score is below the threshold
- **THEN** the attempt stays declined with the rules' reasons

#### Scenario: Predictor unavailable

- **WHEN** the model server times out or answers malformed
- **THEN** the rules decision stands and the evidence records status `timeout` or `error` and no score

### Requirement: Score under the version that answered
The fraud service SHALL read each served model version's feature order and decline threshold from that version's own model configuration, SHALL score an answer under the threshold and layout of the version that produced it, and SHALL discard a score from a version whose configuration it does not yet hold, reading the configuration again on the next call. The version and threshold used SHALL be recorded in the assessment evidence, and the model-call counter SHALL carry the model version and whether the score declined. A replayed assessment SHALL return its recorded result.

#### Scenario: Two versions answer
- **WHEN** an experiment serves two versions and consecutive assessments are answered by different ones
- **THEN** each is scored under its own version's threshold and recorded with that version

#### Scenario: Unseen version
- **WHEN** an answer comes from a version whose configuration the service has not read
- **THEN** that score is discarded, the rules decision stands for that assessment, and the configuration is read for the next call

#### Scenario: Challenger unavailable
- **WHEN** the challenger does not answer
- **THEN** the rules decision stands, the evidence records status `timeout` or `error`, and no score approves what the rules decline

### Requirement: Feature distribution counters
After each model call the fraud service SHALL count every scored feature into the bin its served version's reference profile assigns it, with missing and unavailable values in the missing bin, labelled by feature, bin and model version, through the existing telemetry export.

#### Scenario: Missing feature
- **WHEN** a feature view is unavailable for an assessment
- **THEN** its features are counted in the missing bin of the version that scored it
