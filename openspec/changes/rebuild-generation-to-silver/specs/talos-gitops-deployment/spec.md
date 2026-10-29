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

## ADDED Requirements

### Requirement: ClickHouse instance and users
The data-pipelines application SHALL define one `ClickHouseInstallation` named `clickhouse` in `data`, with one shard and one replica, image `clickhouse/clickhouse-server:26.10.2.103`, and a `clickhouse` Service exposing HTTP 8123 and native 9000. The default profile SHALL set `allow_database_iceberg` to 1. Users `admin`, `langfuse` and `openmetadata` SHALL take their passwords from ExternalSecrets on ClusterSecretStore `doppler` (`CLICKHOUSE_ADMIN_PASSWORD`, `CLICKHOUSE_LANGFUSE_PASSWORD`, `CLICKHOUSE_OPENMETADATA_PASSWORD`). The chart SHALL NOT define the Altinity operator's management user, which the operator adds itself, and SHALL NOT override its profile or network settings. A setup Job SHALL create database `langfuse` after the instance accepts connections.

#### Scenario: Render the instance

- **WHEN** the clickhouse chart is rendered for any environment
- **THEN** it contains one ClickHouseInstallation, the Service with both ports, one ExternalSecret per user, and no reference to the operator's management user or its password

#### Scenario: OpenMetadata user limits

- **WHEN** the `openmetadata` user connects
- **THEN** its profile is readonly with `allow_database_iceberg` set, and its grants are limited to `lakehouse.*`, `system.tables` and `system.columns`


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

### Requirement: Lakehouse catalog application
The `lakehouse` application SHALL own the Polaris REST catalog in `data`, with its PostgreSQL store, signing keys, object-storage secret and network policy, the `lakehouse` warehouse registration and the ClickHouse `lakehouse` DataLakeCatalog database. It SHALL sync in wave 1, after the secret stores and before the data-pipeline application, and SHALL NOT depend on ClickHouse being synced later. Its catalog endpoint `iceberg-catalog.data.svc.cluster.local:8181`, realm `PLATFORM` and client `platform` SHALL NOT change. Its registration and database jobs SHALL be idempotent and SHALL wait or retry until their dependency answers.

#### Scenario: Sync order
- **WHEN** the root application reconciles dev or prod
- **THEN** the catalog secrets and the Polaris store exist before the Polaris Deployment starts, and the registration and database jobs run after Polaris and ClickHouse are reachable

#### Scenario: Catalog endpoint
- **WHEN** a client connects to the catalog
- **THEN** it uses `iceberg-catalog.data.svc.cluster.local:8181` with the existing realm and client

## REMOVED Requirements

### Requirement: Runtime credentials and image delivery
**Reason**: Rewritten for the rebuilt pipeline; its scenarios describe replaced behavior.
**Migration**: See "Simulator credentials and image delivery".

### Requirement: Shared topics and Python Parquet landing
**Reason**: The Python Parquet archiver is removed; topic declarations move to "Shared payment topics".
**Migration**: See "Shared payment topics" and `historical-bronze-ingestion` "Kafka source ingestion".
