## ADDED Requirements

### Requirement: DP3 lineage
The catalog SHALL expose lineage through the DP3 pipelines:

- from `silver.attempts` and `silver.payment_events` to `gold.dim_customer`;
- from `silver.attempts` to the `gold.feat_` tables;
- from `silver.attempts` and `silver.labels` to `gold.label_attempt`;
- from `silver.attempts` and `silver.labels` to `marts.fact_payment_attempt`;
- from `marts.fact_payment_attempt` to `marts.fact_merchant_day`.

The pipeline execution SHALL identify the Silver snapshot IDs the run read.

#### Scenario: Trace a feature table

- **WHEN** a user inspects `gold.feat_customer_daily` lineage
- **THEN** they can trace it through DP3 to `silver.attempts`, and from there through DP2 and DP1 to the historical source and Kafka topics

### Requirement: Gold validation evidence
Every quality rule in the `gold-features` and `payment-marts` contracts SHALL run as a native OpenMetadata test case on its table, through the shared ClickHouse connection. A DP3 run SHALL succeed only when the latest result of every expected test for its tables is Success.

#### Scenario: Overlapping dimension versions

- **WHEN** a DP3 run writes two overlapping versions for one customer
- **THEN** the dimension's no-overlap test fails in OpenMetadata and the DP3 run fails

### Requirement: Scoped mart profiling
After the marts are published, DP3 SHALL run native OpenMetadata profiling for the `marts` tables only, recording row counts and column metrics with each run. It SHALL NOT profile Bronze, Silver or gold Iceberg tables. A profiling failure SHALL fail the stage.

#### Scenario: Profile after a rebuild

- **WHEN** a DP3 run publishes its marts
- **THEN** OpenMetadata shows a fresh profile for `fact_payment_attempt` and `fact_merchant_day` with the current row counts
