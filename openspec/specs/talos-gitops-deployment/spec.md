## Purpose

Deploy fraud-service into the existing Talos environments with independent application ownership.

## Requirements

### Requirement: Environment-specific deployment ownership

Dev and prod SHALL use the same application charts with environment-specific endpoint, hostname, storage-class, secret-config and application-enablement values. Prod SHALL enable:

- gateway services, PostgreSQL, Redis and Kafka/CDC;
- the live simulator, at an environment-configured rate;
- the feature configuration for a later shadow rollout.

The data-pipelines chart SHALL own Flink, Feast Redis, the writer, server, apply hook and workflows under one features.enabled flag, on in dev and off in prod until provisioned. Exactly one Feast Redis ExternalSecret SHALL be rendered. Data-pipeline enablement SHALL follow the environment data-pipeline deployment requirement. Existing shared lakehouse credential and catalog wiring SHALL remain available. Gateway and Kafka SHALL reside in `payment-gateway`; data, Flink and Feast workloads SHALL reside in platform-owned `data`. The platform SHALL own Kubeflow and its `ml` workspace. Application deployment SHALL NOT create or adopt shared namespaces, Profiles or controllers.

#### Scenario: Render both environments

- **WHEN** the dev and prod roots are rendered
- **THEN** each has environment-qualified Application identities and tracks its own release branch
- **AND** both reuse shared platform services without installing a second workspace

#### Scenario: Prod feature store

- **WHEN** the prod root is rendered
- **THEN** features.enabled is false and feature infrastructure is absent; enabling that flag renders Flink on the prod epoch, the writer, server and prod-sized Redis in data

### Requirement: Internal fraud-service deployment

The deployment SHALL provide internal gRPC access on port 8000 and configured OTLP metric/trace export, without an application metrics listener on port 8010 and SHALL NOT provision public ingress or TLS controllers. Liveness SHALL use the standard gRPC health service named `liveness`; readiness SHALL use `readiness` and SHALL fail once shutdown begins. The deployment SHALL NOT supply model artifacts or model configuration.

#### Scenario: Internal deployment

- **WHEN** fraud-service is deployed with either environment configuration
- **THEN** it is exposed through a ClusterIP service without model path configuration
- **AND** no public ingress, cert-manager, or Traefik resources are created

#### Scenario: Pod termination

- **WHEN** a fraud-service pod is terminating
- **THEN** readiness is withdrawn before the gRPC listener closes, while liveness remains SERVING until health checks are no longer accepted
- **AND** it stops qualifying as a ready decision endpoint

### Requirement: Shared platform ownership isolation

Synchronizing or deleting this repository's applications SHALL NOT create, adopt, or delete shared observability infrastructure, its operators, or its namespace. The application deployment SHALL NOT require CRDs or data sources from the removed observability stack and SHALL NOT install an application-owned collector or telemetry store. Application-owned namespace-local dashboard ConfigMaps, metric scrape definitions and alert rules SHALL use the current platform's shared discovery and data sources. Shared rollout controllers, CRDs and traffic-router plugins SHALL remain platform-owned.

#### Scenario: Remove a payment-gateway application

- **WHEN** its application-owned resources are removed
- **THEN** its dashboard, scrape and alert definitions are removed while the platform's telemetry collectors, stores, UI and rollout controllers remain under the platform owner's control

#### Scenario: Render fraud deployment during the transition

- **WHEN** the dev and prod application configurations are rendered
- **THEN** neither requires resources from the removed observability stack
- **AND** the Go fraud workload exports enabled application measurements over OTLP to the shared endpoints without exposing the replaced Python metrics listener

### Requirement: Service quality and automated image release

Every PR and main push SHALL run independent Python, Go and Helm check jobs covering Python lint/format/tests, Go lint and tests for every module, and Helm validation, using the checked-in protobuf bindings. Each service image SHALL use the shared image workflow.

#### Scenario: Pull request or branch push

- **WHEN** a PR is opened or updated
- **THEN** independent checks validate all services and the image matrix builds affected service images

#### Scenario: Merged service changes

- **WHEN** changes reach main
- **THEN** affected service images are built and published with their new main commit tag; unchanged images retain their deployed tags

### Requirement: Gateway workload deployment

The deployment SHALL target Edge, Payments, fraud, notifier, vault simulator, processor simulator, Redis and Kafka in `payment-gateway` through the selected environment root, with ClusterIP services.

#### Scenario: Complete gateway workload deployment contract

- **WHEN** the `Gateway workload deployment` capability is configured or exercised
- **THEN** The deployment SHALL target Edge, Payments, fraud, notifier, vault simulator, processor simulator, Redis and Kafka in `payment-gateway` through the selected environment root, with ClusterIP services. One application chart and Argo CD child SHALL own the six Go services, including fraud, through a services map plus gateway CNPG, Valkey, secrets, routing and security resources; Kafka SHALL retain its separate chart and child. Fraud SHALL use the common service templates with its own Deployment and Service; no dedicated fraud chart or Argo CD child SHALL be created. Edge SHALL additionally have one Gateway API HTTPRoute on the cluster ingress gateway exposing only the hosted checkout paths `/checkout/*`, and the prod values SHALL publish that gateway host through the platform's Cloudflare ClusterTunnel `talos-proxmox` at `pay-gateway.phuchoang.sbs` so a marketplace buyer can reach the hosted page; the JSON API SHALL remain cluster-internal. Dev SHALL publish `pay-gateway-dev.phuchoang.sbs` through its environment tunnel. A CloudNativePG cluster owned by this repository SHALL provide separate databases and owner roles for Payments, notifier, vault and processor. Payments SHALL run migrations as its owner role and serve requests with a separate runtime role that has only `SELECT` and `INSERT` on the payment event ledger. No service's application credentials SHALL grant access to another service's database or logical replication. Application secrets SHALL come from External Secrets through the application `gateway-doppler` ClusterSecretStore, whose namespace allow-list includes `payment-gateway` and whose bootstrap token lives in `payment-gateway`, not from the repository. Network policy SHALL default-deny vault/database/Redis access except the required callers, and SHALL allow Edge only from the ingress gateway and the namespaces of configured integrations. Redis SHALL be a standalone `Redis` resource of the platform's Redis operator (no sidecar), with bounded memory, `noeviction`, TTL-based idempotency records, a password from External Secrets and Payments-only access; Postgres remains the authoritative idempotency store. Internal gRPC SHALL be plaintext h2c secured by the Istio ambient mesh (STRICT mTLS and principal-based AuthorizationPolicy configured through the chart's `mesh.rules`), and workloads SHALL use native gRPC health probes. A repository-owned Kafka cluster, managed by the platform's Strimzi operator, SHALL run in `payment-gateway` with automatic topic creation disabled and `payments.events.v1` declared, and Kafka Connect SHALL publish only the Payments event table through dedicated CDC credentials granted by a Role scoped to that one Secret, excluding application credentials, private operation requests and vault data. Network policy SHALL admit Kafka clients from the gateway and data namespaces, and SHALL allow the notifier egress only to configured webhook destinations. Deleting the Kafka application SHALL NOT delete the Strimzi operator or its CRDs.

#### Scenario: Render gateway target

- **WHEN** the configured prod root is rendered
- **THEN** each Application has an environment-qualified identity, and the only public surface is the Edge `/checkout/*` route, published through the Cloudflare tunnel

#### Scenario: Cross-database access

- **WHEN** Payments' database role attempts to read the notifier database
- **THEN** access is denied

#### Scenario: Vault database isolation

- **WHEN** Edge, Payments or processor credentials attempt to access the vault database directly
- **THEN** access is denied and only mesh-authorized vault API calls on their own port can obtain their scoped response

#### Scenario: Connector secret access

- **WHEN** the Connect service account requests a Secret other than its dedicated CDC Secret
- **THEN** access is denied

### Requirement: Separate PR and main image tags

Same-repository PRs SHALL publish affected application images with `pr-<number>-<full-sha>` using the built merge commit. Fork PRs SHALL build without publication. Main SHALL publish `main-<full-sha>`. Deployment SHALL use each application image’s retained commit-based tag, without mutable `latest` tags. Commit tags SHALL NOT be repurposed for different source code.

#### Scenario: Publish PR images

- **WHEN** a same-repository PR is updated
- **THEN** every affected application image is published for that source revision and only the dev release may change

### Requirement: Complete environment releases

After all affected application images publish successfully, CI SHALL publish one complete release to `deploy/dev` for a trusted PR or `deploy/prod` for main. The release SHALL select child chart sources at the release commit and per-service commit tags. Only services whose build dependencies changed since their deployed image commit SHALL rebuild; unchanged images SHALL retain their existing tags. Chart-only changes SHALL publish without image builds. Publication SHALL validate commit tags and protect the planned deployment baseline and skip superseded source revisions. Per-image manifests and digest/revision overrides SHALL NOT be required. CI SHALL NOT contact Kubernetes or commit image pins to main. Argo CD SHALL reconcile the release branch.

#### Scenario: Publish a complete release

- **WHEN** all affected application images for the current source revision have published
- **THEN** one release records the chart revision and each application image’s commit tag on the corresponding deployment branch

#### Scenario: Superseded source

- **WHEN** an image build completes after its source ref has advanced
- **THEN** it does not replace the environment release

### Requirement: Single simulator workload

Deployment SHALL provide one opt-in single-replica simulator workload running traffic, labels and optional delivery faults from one image/configuration. It SHALL use the selected environment root without duplicate source-specific topology. Delivery faults SHALL remain embedded.

#### Scenario: Render live workload

- **WHEN** simulation is enabled in the configured prod target
- **THEN** exactly one simulator workload is rendered using a strategy that prevents overlapping replicas, without a separate injector workload or new public route

#### Scenario: Simulation disabled

- **WHEN** simulation is disabled
- **THEN** the simulator workload is absent while existing gateway/Kafka services and completed data remain intact

### Requirement: Environment data-pipeline deployment

Dev and prod SHALL enable the historical generator, DP1 Bronze, `dp-pipeline`, `dp-backfill`, `maintain-lakehouse` and the on-demand benchmark templates in `data`, with the suspended `dp-daily` and nightly maintenance schedules. All templates SHALL use externally supplied credentials, commit-tagged images and explicit resource budgets. Historical source selection SHALL default to v18 in dev and v19 in prod. In prod, workflow step pods and Spark drivers SHALL run on the on-demand `cpu` node pool and Spark executors on the `spot` node pool. The schedules SHALL remain suspended in both environments until explicitly enabled.

#### Scenario: Fresh environment rollout

- **WHEN** either environment is reconciled before its dataset is generated
- **THEN** workflow templates are available without running any pipeline or enabling a schedule

#### Scenario: Prod driver placement

- **WHEN** prod renders a Spark application
- **THEN** its driver selects the `cpu` node pool and its executors the `spot` node pool

### Requirement: Data-platform ownership boundary
Application deployment SHALL own DP1 workloads in the platform-owned data namespace. The shared platform SHALL own Argo, the native Workflow lifecycle admission policy and binding scoped to `data`, Apache Spark Operator and its CRDs, the Altinity ClickHouse operator 0.27.4 and its CRDs watching `data`, OpenMetadata and its PostgreSQL/OpenSearch backing services. The operator SHALL be reconciled in the platform repository through Argo CD. Application deployment SHALL own its SQL/contracts, native ingestion workflow configuration and the ClickHouse instance and users it queries, but not the operator. The final deployment SHALL have no dedicated Kafka requirement for governance. Removing DP1 SHALL NOT adopt or remove those shared services or destroy source, lake or governance state.

#### Scenario: Remove the data-pipeline application

- **WHEN** the application-owned DP1 deployment is removed
- **THEN** shared controllers, OpenMetadata, backing-service persistence, accepted source objects and committed lake data remain under their existing owners

### Requirement: Separate pipeline credentials
DP1 SHALL receive the gateway access key scoped to gateway datasets, run evidence and the lakehouse, plus separate ClickHouse query and metadata-publication credentials. ClickHouse catalog/storage credentials SHALL be scoped separately from workload SQL access. Gateway data stages SHALL reuse the gateway object identity; catalog OAuth2 and ClickHouse SQL credentials SHALL remain separate. Rendered configuration SHALL contain secret references rather than secret values.

#### Scenario: Inspect or render pipeline configuration

- **WHEN** the DP1 charts are rendered without live secret values
- **THEN** source, lake and catalog credentials remain external references with independently scoped access, and no plaintext replacement credentials are embedded

### Requirement: Scoped Apache Spark Operator
The platform SHALL install pinned Apache Spark Operator 1.0.0 with Helm chart 1.8.0 for both environments, watching `data` with namespace-scoped operator RBAC. It SHALL NOT adopt the application-owned workload namespace, service accounts or driver permissions, or launch application jobs during controller installation. Shared Kubeflow workloads SHALL remain independently owned.

#### Scenario: Reconcile the operator

- **WHEN** the prod platform reconciles the operator component
- **THEN** the `spark.apache.org/v1` API and controller become ready with access scoped to the data namespace, independently of DP1 workload rollout

### Requirement: Recurring DP1 schedule ownership
The application deployment SHALL own the scheduled Kafka-mode workflow and a Kafka client identity scoped to read `payments.events.v1` and `fraud.labels.v1`. Argo CD SHALL reconcile declared deployment resources; Argo Workflows SHALL execute finite runs. Ingestion progress SHALL live only in Bronze commits, so a deployment or schedule change SHALL NOT reset it.

#### Scenario: Change a batch schedule

- **WHEN** the schedule is reconciled with a different cadence
- **THEN** the next run continues from the offsets recorded in Bronze without replaying or skipping records

### Requirement: Shared analytical SQL service
The data-pipelines application SHALL run one bounded persistent ClickHouse instance for SQL quality, analytics and Langfuse, reading the platform Polaris Iceberg REST catalog through a DataLakeCatalog database with OAuth2 and environment S3 access. Deployment SHALL NOT add Trino, a second analytical engine, a replacement catalog or a signing proxy for this path. Existing observability ClickHouse SHALL remain unaffected. Resource budgets SHALL be checked against node allocatable capacity before rollout; a single-node instance SHALL NOT claim HA. Removing the application SHALL NOT delete the lake data, the Polaris catalog or the operator.

#### Scenario: Query accepted lake data

- **WHEN** a native workflow validates a candidate
- **THEN** ClickHouse reads the recorded Iceberg snapshots directly without a mandatory imported copy and acceptance checks matching native results

### Requirement: Matched metadata runtime ownership
The application SHALL own the thin metadata image and workflow configuration, use its own service commit tag and match its official ingestion base/SDK to the shared OpenMetadata server. Generic externally managed metadata and lakehouse credential references SHALL remain reusable across pipeline stages. Shared platform services SHALL NOT be replaced or removed to reset DP1-owned application data.

#### Scenario: Deploy the rewritten metadata stages

- **WHEN** application configuration selects the thin metadata runtime
- **THEN** native workflows and SDK synchronization use the matched version through external secret references without installing a second catalog or retaining a parallel HTTP publisher

### Requirement: Gateway-owned lakehouse integration
The gateway repository SHALL own gateway Doppler credential references, Polaris catalog registration and the ClickHouse `lakehouse` DataLakeCatalog database, which SHALL be created as admin after the instance accepts connections and re-created only when its definition changes. The platform SHALL own storage policies, provision production storage identities and publish credentials to gateway Doppler. The lakehouse application SHALL own its Polaris and PostgreSQL servers and durable database resources; the platform SHALL own storage provisioning and the Altinity operator's management credential. The gateway integration SHALL provide the storage Secret referenced by its Polaris Deployment without creating a second storage identity.

#### Scenario: Reconcile the gateway integration
- **WHEN** the selected environment is reconciled with gateway credentials available
- **THEN** Polaris and ClickHouse use the same environment gateway storage identity as Flink, metadata storage connections and the synthetic writer
- **AND** gateway removal does not destroy shared server databases, PVCs or the platform bucket

### Requirement: Environment-configurable Python entrypoints
Python services SHALL read runtime settings from configuration files and environment variables without command-line overrides. Historical and live simulation SHALL allow `SIMULATOR_CONFIG` to select a YAML file, with defaults suited to their dedicated entrypoints. Helm SHALL configure pipeline stages, metadata stages and connector selection through environment variables; the Feast writer SHALL have a dedicated entrypoint.

#### Scenario: Select an environment configuration
- **WHEN** a simulator is started with `SIMULATOR_CONFIG` selecting a valid environment YAML
- **THEN** it uses that file's output and execution settings without CLI flags, while storage credentials remain external environment secrets

### Requirement: LLM platform application
The app-of-apps SHALL deploy chart `infra/charts/llm-platform` as application `llm-platform` into the platform-owned `llm` namespace, which is enrolled in the ambient mesh at the `restricted` Pod Security level. It SHALL be enabled in dev and disabled in prod until the user explicitly confirms enabling it. Its credentials SHALL come from `gateway-doppler`. It SHALL consume the platform's KServe, agentgateway, kagent, agentregistry and Agent Substrate controllers and CRDs, and SHALL NOT install, adopt or delete them, their namespaces or GPU worker infrastructure.

#### Scenario: Render both environments
- **WHEN** the dev and prod roots are rendered
- **THEN** dev contains the `llm-platform` Application and prod does not

#### Scenario: Remove the application
- **WHEN** the `llm-platform` application is deleted
- **THEN** shared controllers, CRDs, GatewayClasses, node pools and the `llm` namespace remain

### Requirement: RAG pipeline application
The app-of-apps SHALL deploy chart `infra/charts/rag-pipeline` as application `rag-pipeline` into the platform-owned `llm` namespace, enabled where `llm-platform` is enabled and disabled in prod until the user confirms. Its credentials SHALL come from `gateway-doppler`. Its workflow SHALL run only after the platform has added `llm` to the Argo Workflows controller's namespaces.

#### Scenario: Render both environments
- **WHEN** the dev and prod roots are rendered
- **THEN** dev contains the `rag-pipeline` Application and prod does not

### Requirement: ClickHouse instance and users
The data-pipelines application SHALL define one `ClickHouseInstallation` named `clickhouse` in `data`, with one shard and one replica, image `clickhouse/clickhouse-server:26.10.2.103`, and a `clickhouse` Service exposing HTTP 8123 and native 9000. The default profile SHALL set `allow_database_iceberg` to 1. Users `admin`, `langfuse` and `openmetadata` SHALL take their passwords from ExternalSecrets on ClusterSecretStore `gateway-doppler` (`CLICKHOUSE_ADMIN_PASSWORD`, `CLICKHOUSE_LANGFUSE_PASSWORD`, `CLICKHOUSE_OPENMETADATA_PASSWORD`). The chart SHALL NOT define the Altinity operator's management user, which the operator adds itself, and SHALL NOT override its profile or network settings. A setup Job SHALL create database `langfuse` after the instance accepts connections.

#### Scenario: Render the instance

- **WHEN** the clickhouse chart is rendered for any environment
- **THEN** it contains one ClickHouseInstallation, the Service with both ports, one ExternalSecret per user, and no reference to the operator's management user or its password

#### Scenario: OpenMetadata user limits

- **WHEN** the `openmetadata` user connects
- **THEN** its profile is readonly with `allow_database_iceberg` set, and its grants are limited to `lakehouse.*`, `system.tables` and `system.columns`

### Requirement: Simulator credentials and image delivery

Application credentials SHALL come from the environment's gateway Doppler config through `gateway-doppler`. Object credentials SHALL come from gateway Doppler through `gateway-doppler`, used under `datasets/payment-gateway/`, `runs/payment-gateway/` and `lakehouse/` in the environment platform bucket; in prod the GCS HMAC key is scoped to that bucket. Kubeflow artifacts SHALL retain their separate platform identity. The simulator SHALL NOT access vault or database internals. Historical generation and live simulation SHALL use the same package and image with separate entry points.

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
The `lakehouse` application SHALL own the Polaris REST catalog in `data`, with its PostgreSQL store, signing keys, object-storage secret and network policy, the `lakehouse` warehouse registration and the ClickHouse `lakehouse` DataLakeCatalog database. Its wave orders its creation after the secret stores and before the data-pipeline application; Argo CD SHALL NOT be relied on to wait for an earlier wave's health, so its registration and database jobs retry until Polaris and ClickHouse answer. Its catalog endpoint `iceberg-catalog.data.svc.cluster.local:8181`, realm `PLATFORM` and client `platform` SHALL NOT change. Its registration and database jobs SHALL be idempotent and SHALL wait or retry until their dependency answers.

#### Scenario: Sync order
- **WHEN** the root application reconciles dev or prod
- **THEN** the catalog secrets and the Polaris store exist before the Polaris Deployment starts, and the registration and database jobs retry until Polaris and ClickHouse are reachable

#### Scenario: Catalog endpoint
- **WHEN** a client connects to the catalog
- **THEN** it uses `iceberg-catalog.data.svc.cluster.local:8181` with the existing realm and client
