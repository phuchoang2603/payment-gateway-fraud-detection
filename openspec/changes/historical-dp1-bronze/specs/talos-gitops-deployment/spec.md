## ADDED Requirements

### Requirement: Opt-in prod data-pipeline deployment

The deployment SHALL provide an explicitly enabled prod DP1 application in `fraud-data-prod`, separate from payment-service and Kubeflow Profile namespaces. Existing dev and initial service-rollout defaults SHALL remain unchanged. Enabling DP1 SHALL require verified platform, storage, secret and capacity prerequisites.

#### Scenario: Render initial defaults

- **WHEN** existing dev and prod defaults are rendered before DP1 is enabled
- **THEN** no DP1 workloads are introduced and the existing initial rollout remains unchanged

#### Scenario: Enable historical DP1

- **WHEN** prod DP1 is explicitly enabled after prerequisites pass
- **THEN** its namespace, configuration, scoped identities, external secrets and workflow templates are reconciled independently of payment services and the ML Profile

### Requirement: Data-platform ownership boundary

Application deployment SHALL own DP1 workloads and its data namespace. The shared platform SHALL own Argo, the native Workflow lifecycle admission policy and binding scoped to `fraud-data-prod`, Apache Spark Operator and its CRDs, OpenMetadata and its PostgreSQL/OpenSearch backing services. The final deployment SHALL have no dedicated Kafka requirement for governance. Removing DP1 SHALL NOT adopt or remove those shared services or destroy source, lake or governance state.

#### Scenario: Remove the data-pipeline application

- **WHEN** the application-owned DP1 deployment is removed
- **THEN** shared controllers, OpenMetadata, backing-service persistence, accepted source objects and committed lake data remain under their existing owners

### Requirement: Separate pipeline credentials

DP1 SHALL receive externally managed identities for source reading, lake/run-evidence writing and metadata publication. The generator's source-writer identity SHALL NOT be reused. Rendered configuration SHALL contain secret references rather than secret values.

#### Scenario: Inspect or render pipeline configuration

- **WHEN** the DP1 charts are rendered without live secret values
- **THEN** source, lake and catalog credentials remain external references with independently scoped access, and no plaintext replacement credentials are embedded

### Requirement: Scoped Apache Spark Operator

The platform SHALL install pinned Apache Spark Operator 1.0.0 with Helm chart 1.8.0 for prod, watching only `fraud-data-prod` with namespace-scoped operator RBAC. It SHALL NOT adopt the application-owned workload namespace, service accounts or driver permissions, or launch application jobs during controller installation. Existing dev defaults and Kubeflow workloads SHALL remain unchanged.

#### Scenario: Reconcile the operator

- **WHEN** the prod platform reconciles the operator component
- **THEN** the `spark.apache.org/v1` API and controller become ready with access scoped to the data namespace, independently of DP1 workload rollout
