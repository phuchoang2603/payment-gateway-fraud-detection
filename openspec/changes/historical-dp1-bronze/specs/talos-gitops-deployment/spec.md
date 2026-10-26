## ADDED Requirements

### Requirement: Opt-in prod data-pipeline deployment

The deployment SHALL provide an explicitly enabled prod DP1 application in `fraud-data-prod`, separate from payment-service and Kubeflow Profile namespaces. Existing dev and initial service-rollout defaults SHALL remain unchanged. Deployment SHALL configure platform endpoints, external secret references, immutable image digests and resource budgets without a manual rollout-verification flag.

#### Scenario: Render initial defaults

- **WHEN** existing dev and prod defaults are rendered before DP1 is enabled
- **THEN** no DP1 workloads are introduced and the existing initial rollout remains unchanged

#### Scenario: Enable historical DP1

- **WHEN** prod DP1 is explicitly enabled after prerequisites pass
- **THEN** its namespace, configuration, scoped identities, external secrets and workflow templates are reconciled independently of payment services and the ML Profile

### Requirement: Data-platform ownership boundary

Application deployment SHALL own DP1 workloads and its data namespace. The shared platform SHALL own Argo, the native Workflow lifecycle admission policy and binding scoped to `fraud-data-prod`, Apache Spark Operator and its CRDs, StarRocks and its official operator/CRDs, OpenMetadata and its PostgreSQL/OpenSearch backing services. StarRocks SHALL be reconciled in the platform repository through Argo CD and the official operator/Helm chart. Application deployment SHALL own its SQL/contracts and native ingestion workflow configuration, not shared engine installation. The final deployment SHALL have no dedicated Kafka requirement for governance. Removing DP1 SHALL NOT adopt or remove those shared services or destroy source, lake or governance state.

#### Scenario: Remove the data-pipeline application

- **WHEN** the application-owned DP1 deployment is removed
- **THEN** shared controllers, OpenMetadata, backing-service persistence, accepted source objects and committed lake data remain under their existing owners

### Requirement: Separate pipeline credentials

DP1 SHALL receive one generic externally managed lakehouse runtime access key scoped to source reads and lake/run-evidence writes, plus separate StarRocks query and metadata-publication credentials. StarRocks catalog/storage credentials SHALL be scoped separately from workload SQL access. The generator's source-writer identity SHALL NOT be reused. Rendered configuration SHALL contain secret references rather than secret values.

#### Scenario: Inspect or render pipeline configuration

- **WHEN** the DP1 charts are rendered without live secret values
- **THEN** source, lake and catalog credentials remain external references with independently scoped access, and no plaintext replacement credentials are embedded

### Requirement: Scoped Apache Spark Operator

The platform SHALL install pinned Apache Spark Operator 1.0.0 with Helm chart 1.8.0 for prod, watching only `fraud-data-prod` with namespace-scoped operator RBAC. It SHALL NOT adopt the application-owned workload namespace, service accounts or driver permissions, or launch application jobs during controller installation. Existing dev defaults and Kubeflow workloads SHALL remain unchanged.

#### Scenario: Reconcile the operator

- **WHEN** the prod platform reconciles the operator component
- **THEN** the `spark.apache.org/v1` API and controller become ready with access scoped to the data namespace, independently of DP1 workload rollout

### Requirement: Recurring DP1 schedule ownership

The application deployment SHALL own explicitly configured DP1 CronWorkflows, persistent input-progress configuration and scoped landing-read permissions. Argo CD SHALL reconcile declared deployment resources; Argo Workflows SHALL execute finite batches. Shared Kafka/landing producers SHALL remain separately owned, and a deployment or schedule change SHALL NOT reset input progress or select incomplete inputs.

#### Scenario: Change a batch schedule

- **WHEN** the recurring schedule is reconciled with a different cadence
- **THEN** completed input tracking and accepted evidence remain intact, with no accidental full replay or concurrent unguarded writers

### Requirement: Shared analytical SQL service

The platform SHALL provide bounded persistent StarRocks FE/BE resources for SQL quality and analytics, using the existing RustFS Iceberg REST catalog with native SigV4 and S3 access. Deployment SHALL NOT add Trino, analytical ClickHouse, a replacement catalog or a signing proxy for this path. Existing observability ClickHouse SHALL remain unaffected. Resource/PVC budgets SHALL be measured with Spark and OpenMetadata; a single-node toy deployment SHALL NOT claim HA. Shared engine deletion SHALL NOT be coupled to pipeline application removal.

#### Scenario: Query accepted lake data

- **WHEN** a native workflow validates a candidate
- **THEN** StarRocks reads the recorded Iceberg snapshots directly without a mandatory imported copy and acceptance checks matching native results
