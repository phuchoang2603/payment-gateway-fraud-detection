## MODIFIED Requirements

### Requirement: Environment data-pipeline deployment

Dev SHALL enable the historical generator, DP1 Bronze and DP2 Silver workflow templates and the on-demand benchmark template in `data`. Prod SHALL enable the data-pipeline application with only the historical generator template, using the full-scale configuration and the gateway object identity; Bronze, Silver, schedules and their credentials SHALL stay disabled in prod until explicitly enabled. All templates SHALL use externally supplied credentials, commit-tagged images and explicit resource budgets. Historical source selection SHALL default to v18 in dev. The scheduled Kafka-mode workflow SHALL remain suspended in both environments until explicitly enabled.

#### Scenario: Fresh environment rollout

- **WHEN** either environment is reconciled before v18 is generated
- **THEN** workflow templates are available without running any pipeline or enabling the schedule

#### Scenario: Prod generator only

- **WHEN** prod is reconciled
- **THEN** only the historical generator template is rendered in `data`, and no pipeline runs until it is submitted

### Requirement: Recurring DP1 schedule ownership
The application deployment SHALL own the scheduled Kafka-mode workflow and a Kafka client identity scoped to read `payments.events.v1` and `fraud.labels.v1`. Argo CD SHALL reconcile declared deployment resources; Argo Workflows SHALL execute finite runs. Ingestion progress SHALL live only in Bronze commits, so a deployment or schedule change SHALL NOT reset it.

#### Scenario: Change a batch schedule

- **WHEN** the schedule is reconciled with a different cadence
- **THEN** the next run continues from the offsets recorded in Bronze without replaying or skipping records

### Requirement: Environment-configurable Python entrypoints
Python services SHALL read runtime settings from configuration files and environment variables without command-line overrides. Historical and live simulation SHALL allow `SIMULATOR_CONFIG` to select a YAML file, with defaults suited to their dedicated entrypoints. Helm SHALL configure pipeline stages, metadata stages and connector selection through environment variables; the Feast writer SHALL have a dedicated entrypoint.

#### Scenario: Select an environment configuration
- **WHEN** a simulator is started with `SIMULATOR_CONFIG` selecting a valid environment YAML
- **THEN** it uses that file's output and execution settings without CLI flags, while storage credentials remain external environment secrets

### Requirement: Single simulator workload

Deployment SHALL provide one opt-in single-replica simulator workload running traffic, labels and optional delivery faults from one image/configuration. It SHALL use the selected environment root without duplicate source-specific topology. Delivery faults SHALL remain embedded.

#### Scenario: Render live workload

- **WHEN** simulation is enabled in the configured prod target
- **THEN** exactly one simulator workload is rendered using a strategy that prevents overlapping replicas, without a separate injector workload or new public route

#### Scenario: Simulation disabled

- **WHEN** simulation is disabled
- **THEN** the simulator workload is absent while existing gateway/Kafka services and completed data remain intact

### Requirement: Data-platform ownership boundary
Application deployment SHALL own DP1 workloads in the platform-owned data namespace. The shared platform SHALL own Argo, the native Workflow lifecycle admission policy and binding scoped to `data`, Apache Spark Operator and its CRDs, ClickHouse and its Altinity operator and CRDs, OpenMetadata and its PostgreSQL/OpenSearch backing services. ClickHouse SHALL be reconciled in the platform repository through Argo CD and the Altinity operator. Application deployment SHALL own its SQL/contracts and native ingestion workflow configuration, not shared engine installation. The final deployment SHALL have no dedicated Kafka requirement for governance. Removing DP1 SHALL NOT adopt or remove those shared services or destroy source, lake or governance state.

#### Scenario: Remove the data-pipeline application

- **WHEN** the application-owned DP1 deployment is removed
- **THEN** shared controllers, OpenMetadata, backing-service persistence, accepted source objects and committed lake data remain under their existing owners

### Requirement: Gateway-owned lakehouse integration
The gateway repository SHALL own gateway Doppler credential references, Polaris catalog registration and ClickHouse DataLakeCatalog and reader setup. The platform SHALL own storage policies, provision production storage identities and publish credentials to gateway Doppler. The platform SHALL own servers, durable databases, administrative authentication and generated ClickHouse reader passwords. The gateway integration SHALL provide the storage Secret referenced by the platform Polaris Deployment without creating a second storage identity.

#### Scenario: Reconcile the gateway integration
- **WHEN** the selected environment is reconciled with gateway credentials available
- **THEN** Polaris and ClickHouse use the same environment gateway storage identity as Flink, metadata storage connections and the synthetic writer
- **AND** gateway removal does not destroy shared server databases, PVCs or the platform bucket

### Requirement: Separate pipeline credentials
DP1 SHALL receive the gateway access key scoped to gateway datasets, run evidence and the lakehouse, plus separate ClickHouse query and metadata-publication credentials. ClickHouse catalog/storage credentials SHALL be scoped separately from workload SQL access. Gateway data stages SHALL reuse the gateway object identity; catalog OAuth2 and ClickHouse SQL credentials SHALL remain separate. Rendered configuration SHALL contain secret references rather than secret values.

#### Scenario: Inspect or render pipeline configuration

- **WHEN** the DP1 charts are rendered without live secret values
- **THEN** source, lake and catalog credentials remain external references with independently scoped access, and no plaintext replacement credentials are embedded

### Requirement: Shared analytical SQL service
The platform SHALL provide a bounded persistent ClickHouse server for SQL quality and analytics, reading the platform Polaris Iceberg REST catalog through a DataLakeCatalog database with OAuth2 and environment S3 access. Deployment SHALL NOT add Trino, a second analytical engine, a replacement catalog or a signing proxy for this path. Existing observability ClickHouse SHALL remain unaffected. Resource/PVC budgets SHALL be measured with Spark and OpenMetadata; a single-node toy deployment SHALL NOT claim HA. Shared engine deletion SHALL NOT be coupled to pipeline application removal.

#### Scenario: Query accepted lake data

- **WHEN** a native workflow validates a candidate
- **THEN** ClickHouse reads the recorded Iceberg snapshots directly without a mandatory imported copy and acceptance checks matching native results

## ADDED Requirements

### Requirement: Simulator credentials and image delivery

Application credentials SHALL come from the environment's gateway Doppler config through `gateway-doppler`. Object credentials SHALL come from gateway Doppler through `gateway-doppler`, scoped to `datasets/payment-gateway/`, `runs/payment-gateway/` and `lakehouse/` in the environment platform bucket. Kubeflow artifacts SHALL retain their separate platform identity. The simulator SHALL NOT access vault or database internals. Historical generation and live simulation SHALL use the same package and image with separate entry points.

#### Scenario: Generator permissions

- **WHEN** historical generation runs against an environment
- **THEN** it receives gateway-owned object credentials and writes below its configured dataset root
- **AND** no bucket-name allowlist or fixed storage endpoint is required by application code

### Requirement: Shared payment topics
The Kafka application SHALL declare the payment event and fraud label topics with configured partitions and retention, preserving the authoritative event publisher, CDC Connect worker and notifier. Protobuf payloads SHALL NOT require Avro conversion or registry framing. Topic retention SHALL be documented as the maximum interval between Kafka-mode DP1 runs. No workload that copies payment topics to object storage SHALL be deployed.

#### Scenario: Render Kafka topics

- **WHEN** the Kafka application is rendered
- **THEN** the payment event and label topics are declared with their retention, and no copy workload, S3 sink connector or registry framing is added

## REMOVED Requirements

### Requirement: Runtime credentials and image delivery
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Simulator credentials and image delivery".

### Requirement: Shared topics and Python Parquet landing
**Reason**: The Python Parquet archiver is removed; topic declarations move to "Shared payment topics".
**Migration**: See "Shared payment topics" and `historical-bronze-ingestion` "Kafka source ingestion".
