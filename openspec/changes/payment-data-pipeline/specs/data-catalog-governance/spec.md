## MODIFIED Requirements


### Requirement: Clean native catalog artifacts
Implementation SHALL remove obsolete DP1-owned catalog registrations, contracts, products, pipelines, test definitions, results, lineage and executions from the previous pipeline, including archive assets. Deletion SHALL be limited to explicitly owned DP1 assets and SHALL NOT remove the catalog service, shared backing services, unrelated assets, historical source objects or Kafka topics. Current artifacts SHALL be recreated without migration or imported results. The publication SDK path SHALL own explicit governance metadata; native workflows SHALL own discovery and quality results.

#### Scenario: Replace old DP1 registrations

- **WHEN** obsolete DP1 catalog artifacts are removed
- **THEN** only the rebuilt pipeline's assets remain for Bronze, and unrelated assets and source objects are intact

## ADDED Requirements


### Requirement: Product discovery
The catalog SHALL expose the Bronze and Silver tables with their schemas, the native quality test results written by each run, and their lineage from the historical source and the payment topics through DP1 and DP2. ODCS contracts and ODPS data products SHALL NOT be imported into OpenMetadata until its importer accepts the ODCS v3 files as written. Git SHALL remain the authority for contracts and products.

#### Scenario: Discover a Silver table

- **WHEN** a user opens a Silver table in OpenMetadata
- **THEN** they can see its schema, its native test results from the latest run, and its lineage to the Bronze tables

### Requirement: Bronze and Silver lineage
The catalog SHALL expose lineage from the historical source and from the `payments.events.v1` and `fraud.labels.v1` topics through the DP1 pipeline to the corresponding Bronze tables, and from the Bronze tables through the DP2 pipeline to the Silver tables. Events and labels SHALL be independent Bronze outputs; the catalog SHALL NOT fabricate an events-to-labels ingestion edge. The run's dataset version, Kafka offsets or Bronze snapshot IDs SHALL be available from the associated pipeline execution.

#### Scenario: Trace a Silver label table

- **WHEN** a user inspects `silver.labels` lineage
- **THEN** they can trace it through DP2 to both Bronze tables and through DP1 to the label topic and the historical source

### Requirement: Native assertion evidence
The catalog SHALL retain native quality-test definitions and Success, Failed or Aborted results linked to contract rules, tables and run identity. Run reports SHALL retain the exact input, contract and snapshot references. Failed assertions and execution errors SHALL remain distinct.

#### Scenario: Contract violation

- **WHEN** a committed table fails a contract rule
- **THEN** OpenMetadata shows that failed test on the table with the run identity, and the run is reported as failed

### Requirement: Run acceptance from native results
A run SHALL succeed only when every expected native test for that run's tables reports Success. Publication SHALL write products, contracts, lineage and the pipeline execution status; a publication failure SHALL fail the publish stage without changing the recorded data or check outcome. Full catalog asset readback SHALL NOT be a run stage.

#### Scenario: Native test result missing

- **WHEN** the checks stage cannot find a Success result for an expected test of the run
- **THEN** the run fails and does not report passing checks

### Requirement: Kafka run governance identity
Kafka-mode runs SHALL publish their topic offset ranges, committed snapshots and check outcomes on the same canonical assets as historical runs. No fabricated dataset version or manifest SHALL be attributed to Kafka input.

#### Scenario: Publish a Kafka-mode run

- **WHEN** a Kafka-mode DP1 run completes
- **THEN** its pipeline execution identifies the topics, partitions, start and end offsets and the Bronze snapshots it committed

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

### Requirement: Range-scoped validation
Each Silver and Gold contract rule SHALL audit only the rows the run could write. The checks stage SHALL read the range recorded on the audited commit (`dp2.range` or `dp3.range`) and restrict the rule's table to it through ClickHouse `additional_table_filters`. Other audited tables a rule references SHALL be pinned to their audited snapshots. A commit without a recorded range SHALL be audited in full.

#### Scenario: One month of a backfill

- **WHEN** the checks run for a backfill month after earlier months are published
- **THEN** each rule reads that month's partitions only, and its duration does not grow with the months before it

### Requirement: Lineage on every run
The exit handler SHALL publish each pipeline's lineage edges whether the workflow succeeded or failed, because the edges describe the pipeline's structure and a stopped backfill has already published earlier months. The execution status SHALL still record the run's outcome.

#### Scenario: Backfill stops at a later month

- **WHEN** a backfill publishes some months and then fails
- **THEN** OpenMetadata shows lineage from the historical source through Bronze and Silver to the gold tables and marts, and a failed execution for the run

## REMOVED Requirements


### Requirement: Product and contract discovery
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Product discovery".

### Requirement: Source pipeline and table lineage
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Bronze and Silver lineage".

### Requirement: Reproducible assertion evidence
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Native assertion evidence".

### Requirement: Catalog acceptance readback
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Run acceptance from native results".

### Requirement: Recurring batch governance identity
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Kafka run governance identity".

### Requirement: Meaningful scoped native profiling
**Reason**: The rebuilt pipelines run no native profiling; the requirement has no implementation in this change.
**Migration**: None.
