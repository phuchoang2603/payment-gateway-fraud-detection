# data-catalog-governance Specification

## Purpose
Make payment data products discoverable and auditable through a persistent shared catalog with consistent asset identity, pipeline lineage and reproducible validation evidence.

## Requirements

### Requirement: Verified shared catalog deployment
OpenMetadata with persistent PostgreSQL and OpenSearch SHALL be the catalog under platform ownership. Existing services SHALL be reused only after compatibility, state isolation and readiness verification. Native discovery, profiling and SQL quality execution SHALL use the server-version-matched metadata runtime based on the official OpenMetadata ingestion image through existing Argo workflows without requiring Kafka or Airflow. Deployment SHALL include authenticated access, persistence, resource/storage budgets and capacity for concurrent pipeline execution.

#### Scenario: Catalog is not deployed

- **WHEN** no compatible live OpenMetadata instance is verified
- **THEN** rollout provisions the catalog and required backing services, completes setup/migrations and verifies authenticated publication/readback before claiming governance readiness

#### Scenario: Catalog restarts

- **WHEN** deployed catalog services restart
- **THEN** previously published product, table, contract, lineage and validation metadata remains available from persistent state

### Requirement: Authoritative contract publication
Git-versioned ODCS contracts SHALL remain authoritative. OpenMetadata SHALL expose native contracts with the complete original ODCS YAML, identities, versions, hashes and artifact references. Native contracts SHALL populate inspectable normalized nested schema, active raw quality-test references, declared owners and supported declared semantics/security/SLA fields, without inventing absent guarantees. Native contract source URLs SHALL use stable canonical repository paths. Contract provenance SHALL retain the full Git revision, immutable artifact URL and content hash separately; execution parameters SHALL retain the publication Git revision. An unrelated deployment revision SHALL NOT change the canonical contract link or require a shared server fork. Original YAML alone SHALL NOT satisfy native mapping. Native schemas and quality-rule metadata SHALL preserve the declared rules without modifying Git contracts or immutable source manifests. Native OpenMetadata quality workflows SHALL execute Git-derived ClickHouse SQL as the sole bronze quality path. Catalog configuration SHALL NOT become an independent rule source or retain a custom GX result projection.

#### Scenario: Inspect a published contract

- **WHEN** a user opens a bronze table's contract in OpenMetadata
- **THEN** its identity/version/hash match Git, its nested schema and active native quality references are inspectable, its declared owners resolve and its validation evidence traces to the selected contract

#### Scenario: Unsupported contract or validation metadata

- **WHEN** the selected catalog integration cannot represent a required contract or validation outcome
- **THEN** publication reports incomplete governance rather than silently omitting it or claiming acceptance

### Requirement: Canonical table identities
Native ClickHouse discovery, profiling, quality execution and lineage SHALL address the same OpenMetadata table entities. ClickHouse SHALL use the existing Polaris REST catalog and separately configured warehouse access. External-catalog selection and nested schema fidelity SHALL be verified for each native workflow connection; no custom discovery, duplicate Spark/custom registrations or mandatory internal-table copies SHALL substitute for native discovery.

#### Scenario: Execute native SQL validation

- **WHEN** a native OpenMetadata SQL workflow checks an Iceberg table discovered through ClickHouse
- **THEN** its native test definitions and results appear on the discovered entity with matching run, contract and snapshot references

### Requirement: Single governance catalog
OpenMetadata SHALL be the sole governance catalog and publication target, with a single publisher path and no dual publication. Ordinary catalog publication SHALL NOT mutate source data or lake snapshots. A separately scoped fresh-start reset SHALL follow the ownership and quiescence requirements below.

#### Scenario: Publish governance metadata

- **WHEN** DP1 publishes governance metadata
- **THEN** every product, contract, lineage and validation result is written to OpenMetadata only, with shared services and immutable sources unaffected

### Requirement: Private platform access and administrator identity
OpenMetadata SHALL be reachable at its HTTPS Cloudflare Tunnel hostname, authenticate the existing platform user through Dex OAuth2/OIDC and grant that identity administrator permissions. It SHALL NOT require Kubeflow's OAuth2 Proxy session or expose an unauthenticated metadata API. Existing Kubeflow and Grafana authentication SHALL remain functional.

#### Scenario: Platform administrator signs in

- **WHEN** the configured platform user completes Dex authentication for OpenMetadata
- **THEN** OpenMetadata identifies that user as an administrator without a separate local password

#### Scenario: Anonymous API request

- **WHEN** a caller requests protected metadata without authentication
- **THEN** the API rejects the request and does not disclose catalog data

### Requirement: Clean native catalog artifacts
Implementation SHALL remove obsolete DP1-owned catalog registrations, contracts, products, pipelines, test definitions, results, lineage and executions from the previous pipeline, including archive assets. Deletion SHALL be limited to explicitly owned DP1 assets and SHALL NOT remove the catalog service, shared backing services, unrelated assets, historical source objects or Kafka topics. Current artifacts SHALL be recreated without migration or imported results. The publication SDK path SHALL own explicit governance metadata; native workflows SHALL own discovery and quality results.

#### Scenario: Replace old DP1 registrations

- **WHEN** obsolete DP1 catalog artifacts are removed
- **THEN** only the rebuilt pipeline's assets remain for Bronze, and unrelated assets and source objects are intact

### Requirement: Current pipeline execution metadata
The catalog SHALL expose pipeline tasks/dependencies, owners, product membership, source links and current execution status. Execution and supported task inputs/outputs SHALL identify actual source assets and canonical outputs; linked immutable receipts SHALL identify selected manifests/batches, counts, run/contract identities and independent snapshots. A new run SHALL NOT display an unrelated old execution as its own. Metadata-only recovery SHALL preserve original native outcomes and execution identity.

#### Scenario: Inspect the current ingestion execution

- **WHEN** a user opens the pipeline after a fresh accepted run
- **THEN** its tasks, status, Argo links, actual inputs and independent outputs match that run rather than a historical execution

### Requirement: Knowledge-base product in the catalog
The RAG knowledge pipeline SHALL be governed like the other data pipelines:
- an ODPS product and ODCS contract owned by the `rag-pipeline` chart, whose SQL quality rules the shared checks job runs against `knowledge.chunks`;
- the Argo pipeline `rag-knowledge` with its run status;
- lineage from the corpus version container through `rag-knowledge` to `knowledge.chunks`.

#### Scenario: Successful knowledge run
- **WHEN** a `rag-knowledge` run completes
- **THEN** OpenMetadata shows the run status, the passing contract test results on `knowledge.chunks`, and lineage from the corpus version container to that table

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
