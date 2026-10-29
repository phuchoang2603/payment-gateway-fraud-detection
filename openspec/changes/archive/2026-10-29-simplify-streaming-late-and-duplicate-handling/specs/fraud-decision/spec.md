## MODIFIED Requirements

### Requirement: Independent bounded dependency semantics

Velocity and historical-feature retrieval SHALL have separate deadlines, statuses and policy modes. Initial velocity checks SHALL be observe-only. Historical-feature retrieval SHALL be disabled in every environment until the feature-store change; once enabled it SHALL be shadow-only. Failure of either SHALL be recorded without changing the existing valid rules decision. A required dependency or assessment execution failure SHALL block authorization with an operational error, not fabricate a fraud decline or silently approve. Future model enforcement SHALL require an explicit compatible missing-feature policy. Counter observations and snapshot values SHALL remain separate populations.

#### Scenario: Feast unavailable during shadow rollout

- **WHEN** Feast times out while the rules evaluator remains healthy
- **THEN** the rules result remains valid and evidence records historical features as unavailable

#### Scenario: Assessment execution fails

- **WHEN** the assessment cannot complete durably
- **THEN** it records an operational error and returns no successful fraud decision
