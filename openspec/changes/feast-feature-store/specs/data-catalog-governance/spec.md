## ADDED Requirements

### Requirement: Streaming job catalogue and lineage
The catalog SHALL record the streaming feature job as a pipeline with its runs, through OpenMetadata's Flink pipeline connector against the JobManager REST API. The catalog SHALL show lineage from `payments.events.v1` through the streaming job to the feature journal and reject topics. Where the connector does not resolve that Kafka lineage, a published SDK lineage SHALL add it. Neither the streaming job nor the feature store SHALL depend on the catalog at runtime.

#### Scenario: Trace a journal topic

- **WHEN** a user inspects the `fraud.feature-updates.v2.<epoch>` topic's lineage
- **THEN** they can trace it through the streaming job to `payments.events.v1`

#### Scenario: Catalog outage

- **WHEN** OpenMetadata is unavailable
- **THEN** the streaming job, the stream writer and the feature server keep running

### Requirement: Feature store lineage and status
The catalog SHALL show lineage from the feature journal topic through the `feature-writer` pipeline to `gold.feat_stream_customer` and `gold.feat_stream_payment_method`. It SHALL also show lineage from `gold.feat_customer_daily` and `gold.feat_payment_method_daily` into the `feast-materialize` pipeline. `feast-materialize` SHALL record each run's status, materialization windows, optional since, and the sample count. The online store is not a catalogued asset.

#### Scenario: Trace the journal downstream

- **WHEN** a user inspects the journal topic's lineage
- **THEN** they see `feature-writer` downstream, and the streaming job and `payments.events.v1` upstream

#### Scenario: Trace a daily feature table downstream

- **WHEN** a user inspects `gold.feat_customer_daily` lineage
- **THEN** they see `feast-materialize` downstream, with its latest run's status and window

### Requirement: Stream history lineage
The catalog SHALL show lineage from Bronze `payment_events`, through the `stream-history` pipeline, to the stream feature tables. Each unit's execution SHALL record its range, epoch, input and output counts, and gate outcome.

#### Scenario: Trace a stream feature table to its history

- **WHEN** a user inspects `gold.feat_stream_customer` lineage
- **THEN** they see both `feature-writer` from the journal and `stream-history` from Bronze `payment_events`, with the latest unit's range and gate outcome
