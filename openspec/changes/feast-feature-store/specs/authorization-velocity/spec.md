## REMOVED Requirements

### Requirement: Atomic per-entity submission registration

**Reason:** Simplification: the 5-minute Flink stream window covers bursts, at the cost of seconds-level detection.

**Migration:** Remove synchronous submission registration, its dedicated Redis instance, credentials and evidence. Use the existing Flink stream features through Feast; shadow retrieval does not change rules outcomes.

### Requirement: Submission and decision populations remain distinct

**Reason:** Simplification: the 5-minute Flink stream window covers bursts, at the cost of seconds-level detection.

**Migration:** Remove synchronous submission registration, its dedicated Redis instance, credentials and evidence. Use the existing Flink stream features through Feast; shadow retrieval does not change rules outcomes.

### Requirement: Isolated bounded guard and rollout

**Reason:** Simplification: the 5-minute Flink stream window covers bursts, at the cost of seconds-level detection.

**Migration:** Remove synchronous submission registration, its dedicated Redis instance, credentials and evidence. Use the existing Flink stream features through Feast; shadow retrieval does not change rules outcomes.
