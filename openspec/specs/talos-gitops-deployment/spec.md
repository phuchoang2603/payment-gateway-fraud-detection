## Purpose

Deploy fraud-service into the existing Talos environments with independent application ownership.

## Requirements

### Requirement: Environment-specific deployment ownership

Dev and prod SHALL use the same application charts with environment-specific endpoint, hostname, storage-class and secret-config values. Gateway and Kafka SHALL reside in `payment-gateway`; data, Flink and Feast workloads SHALL reside in platform-owned `data`. The platform SHALL own Kubeflow and its `ml` workspace. Application deployment SHALL NOT create or adopt shared namespaces, Profiles or controllers.

#### Scenario: Render both environments

- **WHEN** the dev and prod roots are rendered
- **THEN** each has environment-qualified Application identities and tracks its own release branch
- **AND** both reuse shared platform services without installing a second workspace

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

Synchronizing or deleting this repository's applications SHALL NOT create, adopt, or delete shared observability infrastructure, its operators, or its namespace. The application deployment SHALL NOT require CRDs or data sources from the removed observability stack and SHALL NOT install an application-owned collector, dashboard or alerting resource.

#### Scenario: Remove a payment-gateway application

- **WHEN** its application-owned resources are removed
- **THEN** the platform's telemetry collectors, store, and UI remain under the platform owner's control

#### Scenario: Render fraud deployment during the transition

- **WHEN** the dev and prod application configurations are rendered
- **THEN** neither requires resources from the removed observability stack
- **AND** the Go fraud workload exports enabled application measurements over OTLP to the shared node agent without exposing the replaced Python metrics listener

### Requirement: Service quality and automated image release

Every PR and main push SHALL run independent Python, Go and Helm check jobs covering Python lint/format/tests, Go lint and tests for every module, and Helm validation, using the checked-in protobuf bindings. Each service image SHALL use the shared image workflow.

#### Scenario: Pull request or branch push

- **WHEN** a PR is opened or updated
- **THEN** independent checks validate all services and the image matrix builds every service image

#### Scenario: Merged service changes

- **WHEN** changes reach main
- **THEN** fraud, edge, Payments, notifier, vault simulator, processor simulator and Debezium Connect images are built and published with immutable digests and the main commit tag

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

Same-repository PRs SHALL publish every application image with `pr-<number>-<full-sha>` using the built merge commit. Fork PRs SHALL build without publication. Main SHALL publish `main-<full-sha>`. Deployment SHALL use immutable digests, without mutable `latest` tags.

#### Scenario: Publish PR images

- **WHEN** a same-repository PR is updated
- **THEN** every application image is published for that source revision and only the dev release may change

### Requirement: Complete environment releases

After all application images publish successfully, CI SHALL publish one complete release to `deploy/dev` for a trusted PR or `deploy/prod` for main. The release SHALL select immutable child chart sources and every owned image digest from the same revision. Publication SHALL reject missing, malformed or mismatched image manifests and skip superseded source revisions. CI SHALL NOT contact Kubernetes or commit image pins to main. Argo CD SHALL reconcile the release branch.

#### Scenario: Publish a complete release

- **WHEN** all application images for the current source revision have published
- **THEN** one release records that revision and every application image digest on the corresponding deployment branch

#### Scenario: Superseded source

- **WHEN** an image build completes after its source ref has advanced
- **THEN** it does not replace the environment release



### Requirement: Single simulator workload

Deployment SHALL provide one opt-in single-replica simulator workload running traffic, labels and optional delivery faults from one image/configuration. It SHALL use the selected environment root without duplicate source-specific topology. Delivery faults SHALL remain embedded; an independently enabled Python archiver SHALL provide landing.

#### Scenario: Render live workload

- **WHEN** simulation is enabled in the configured prod target
- **THEN** exactly one simulator workload is rendered using a strategy that prevents overlapping replicas, without a separate injector workload or new public route

#### Scenario: Simulation disabled

- **WHEN** simulation is disabled
- **THEN** the simulator workload is absent while existing gateway/Kafka services and completed data remain intact

### Requirement: Shared topics and Python Parquet landing

The Kafka application SHALL retain payments.events.v1 and declare payments.events.v1.faulty and fraud.labels.v1 with configured partitions/retention. S3-compatible archival SHALL run as a Python workload producing historical-compatible decoded Parquet with transport metadata, preserving the authoritative event publisher, CDC Connect worker and notifier. Protobuf payloads SHALL NOT require Avro conversion or a registry.

#### Scenario: Render landing archiver

- **WHEN** landing is enabled
- **THEN** one Python archiver workload consumes selected payment topics and labels, with no S3 Connect plugin, Flink deployment or registry

### Requirement: Runtime credentials and image delivery

Application credentials SHALL come from the environment's gateway Doppler config through `gateway-doppler`. Object credentials SHALL come from gateway Doppler through `gateway-doppler`, scoped to `datasets/payment-gateway/`, `runs/payment-gateway/` and `lakehouse/` in the environment platform bucket. Kubeflow artifacts SHALL retain their separate platform identity. The simulator SHALL NOT access vault or database internals. The simulator and archiver SHALL use the same package/image with separate entry points.

#### Scenario: Archiver permissions

- **WHEN** the archiver is configured
- **THEN** it receives gateway-owned object credentials and writes completed immutable batches below its configured dataset root
- **AND** no bucket-name allowlist or fixed storage endpoint is required by application code

### Requirement: Environment data-pipeline deployment

Both environment values SHALL enable DP1 workflow templates in `data`, with externally supplied credentials, immutable images and explicit resource budgets. Fresh-start source selection SHALL use v17. Live simulation and recurring ingestion SHALL remain disabled until the new source and its actual manifest digest are validated. No historical objects or acceptance receipts SHALL be copied or relabelled as fresh acceptance.

#### Scenario: Fresh environment rollout

- **WHEN** either environment is reconciled before data generation
- **THEN** workflow templates are available without enabling simulation or scheduled ingestion

### Requirement: Data-platform ownership boundary
Application deployment SHALL own DP1 workloads in the platform-owned data namespace. The shared platform SHALL own Argo, the native Workflow lifecycle admission policy and binding scoped to `data`, Apache Spark Operator and its CRDs, StarRocks and its official operator/CRDs, OpenMetadata and its PostgreSQL/OpenSearch backing services. StarRocks SHALL be reconciled in the platform repository through Argo CD and the official operator/Helm chart. Application deployment SHALL own its SQL/contracts and native ingestion workflow configuration, not shared engine installation. The final deployment SHALL have no dedicated Kafka requirement for governance. Removing DP1 SHALL NOT adopt or remove those shared services or destroy source, lake or governance state.

#### Scenario: Remove the data-pipeline application

- **WHEN** the application-owned DP1 deployment is removed
- **THEN** shared controllers, OpenMetadata, backing-service persistence, accepted source objects and committed lake data remain under their existing owners

### Requirement: Separate pipeline credentials
DP1 SHALL receive the gateway access key scoped to gateway datasets, run evidence and the lakehouse, plus separate StarRocks query and metadata-publication credentials. StarRocks catalog/storage credentials SHALL be scoped separately from workload SQL access. Gateway data stages SHALL reuse the gateway object identity; catalog OAuth2 and StarRocks SQL credentials SHALL remain separate. Rendered configuration SHALL contain secret references rather than secret values.

#### Scenario: Inspect or render pipeline configuration

- **WHEN** the DP1 charts are rendered without live secret values
- **THEN** source, lake and catalog credentials remain external references with independently scoped access, and no plaintext replacement credentials are embedded

### Requirement: Scoped Apache Spark Operator
The platform SHALL install pinned Apache Spark Operator 1.0.0 with Helm chart 1.8.0 for both environments, watching `data` with namespace-scoped operator RBAC. It SHALL NOT adopt the application-owned workload namespace, service accounts or driver permissions, or launch application jobs during controller installation. Shared Kubeflow workloads SHALL remain independently owned.

#### Scenario: Reconcile the operator

- **WHEN** the prod platform reconciles the operator component
- **THEN** the `spark.apache.org/v1` API and controller become ready with access scoped to the data namespace, independently of DP1 workload rollout

### Requirement: Recurring DP1 schedule ownership
The application deployment SHALL own explicitly configured DP1 CronWorkflows, persistent input-progress configuration and scoped landing-read permissions. Argo CD SHALL reconcile declared deployment resources; Argo Workflows SHALL execute finite batches. Shared Kafka/landing producers SHALL remain separately owned, and a deployment or schedule change SHALL NOT reset input progress or select incomplete inputs.

#### Scenario: Change a batch schedule

- **WHEN** the recurring schedule is reconciled with a different cadence
- **THEN** completed input tracking and accepted evidence remain intact, with no accidental full replay or concurrent unguarded writers

### Requirement: Shared analytical SQL service
The platform SHALL provide bounded persistent StarRocks FE/BE resources for SQL quality and analytics, using the platform Polaris Iceberg REST catalog with OAuth2 and environment S3 access. Deployment SHALL NOT add Trino, analytical ClickHouse, a replacement catalog or a signing proxy for this path. Existing observability ClickHouse SHALL remain unaffected. Resource/PVC budgets SHALL be measured with Spark and OpenMetadata; a single-node toy deployment SHALL NOT claim HA. Shared engine deletion SHALL NOT be coupled to pipeline application removal.

#### Scenario: Query accepted lake data

- **WHEN** a native workflow validates a candidate
- **THEN** StarRocks reads the recorded Iceberg snapshots directly without a mandatory imported copy and acceptance checks matching native results

### Requirement: Matched metadata runtime ownership
The application SHALL own the thin metadata image and workflow configuration, pin its immutable digest and match its official ingestion base/SDK to the shared OpenMetadata server. Generic externally managed metadata and lakehouse credential references SHALL remain reusable across pipeline stages. Shared platform services SHALL NOT be replaced or removed to reset DP1-owned application data.

#### Scenario: Deploy the rewritten metadata stages

- **WHEN** application configuration selects the thin metadata runtime
- **THEN** native workflows and SDK synchronization use the matched version through external secret references without installing a second catalog or retaining a parallel HTTP publisher

### Requirement: Gateway-owned lakehouse integration
The gateway repository SHALL own the unified object storage IAM policy, production identity, gateway Doppler credential keys, Polaris catalog registration and StarRocks catalog/reader setup. The platform SHALL own servers, durable databases and administrative authentication. The gateway integration SHALL provide the storage Secret referenced by the platform Polaris Deployment without creating a second storage identity.

#### Scenario: Reconcile the gateway integration
- **WHEN** the selected environment is reconciled with gateway credentials available
- **THEN** Polaris and StarRocks use the same environment gateway storage identity as Flink, metadata storage connections and the synthetic writer
- **AND** gateway removal does not destroy shared server databases, PVCs or the platform bucket
