## MODIFIED Requirements

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
