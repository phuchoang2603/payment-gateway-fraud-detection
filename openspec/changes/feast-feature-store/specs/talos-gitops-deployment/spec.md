## MODIFIED Requirements

### Requirement: Environment-specific deployment ownership

Dev and prod SHALL use the same application charts with environment-specific endpoint, hostname, storage-class, secret-config and application-enablement values. Prod SHALL enable:

- gateway services, PostgreSQL, Redis and Kafka/CDC;
- the streaming feature job under its own prod epoch;
- the live simulator, at an environment-configured rate;
- the feature store: Redis sized for the prod dataset, the push jobs, the feature server, materialization and the stream history;
- Fraud feature retrieval in shadow mode.

Data-pipeline enablement SHALL follow the environment data-pipeline deployment requirement. Existing shared lakehouse credential and catalog wiring SHALL remain available. Gateway and Kafka SHALL reside in `payment-gateway`; data, Flink and Feast workloads SHALL reside in platform-owned `data`. The platform SHALL own Kubeflow and its `ml` workspace. Application deployment SHALL NOT create or adopt shared namespaces, Profiles or controllers.

#### Scenario: Render both environments

- **WHEN** the dev and prod roots are rendered
- **THEN** each has environment-qualified Application identities and tracks its own release branch
- **AND** both reuse shared platform services without installing a second workspace

#### Scenario: Prod feature store

- **WHEN** the prod root is rendered
- **THEN** Flink targets the prod epoch's journal topic, the push jobs, feature server and prod-sized Redis are rendered in `data`, and the fraud service receives feature-retrieval configuration for `fraud_model_v1`
