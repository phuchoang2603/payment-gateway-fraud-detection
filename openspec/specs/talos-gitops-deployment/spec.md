## Purpose

Deploy fraud-service into the existing Talos environments with independent application ownership.

## Requirements

### Requirement: Environment-specific deployment ownership

The deployment configuration SHALL use the configured prod Argo CD root, targeting only that environment's cluster without restoring a dev root. Gateway and fraud-service workloads SHALL reside in the payment-gateway namespace; the prod Kubeflow workspace SHALL reside in its own Profile namespace. Application identities SHALL NOT collide with marketplace applications. The gateway rollout SHALL retain the separately owned workspace and required prod secret store.

#### Scenario: Render both environments

- **WHEN** the repository's configured deployment roots are rendered
- **THEN** the prod root and enabled children have distinct environment-qualified identities and target only the corresponding workload cluster, without creating a dev root
- **AND** fraud-service resources remain in payment-gateway, while prod workspace resources target its Profile namespace

#### Scenario: Initial prod-only rollout

- **WHEN** the prod root is rendered with its checked-in gateway rollout configuration
- **THEN** prod renders the enabled gateway and fraud workloads alongside the workspace and supporting prod secret store
- **AND** no dev root or dev child applications are created

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
- **THEN** fraud, edge, Payments, notifier, vault simulator, processor simulator and Debezium Connect images are built and published with the main commit tag and `latest`, without manifest version comparison scripts

### Requirement: Gateway workload deployment

The deployment SHALL target Edge, Payments, fraud, notifier, vault simulator, processor simulator, Redis and Kafka in `payment-gateway` through the configured prod root, with ClusterIP services.

#### Scenario: Complete gateway workload deployment contract

- **WHEN** the `Gateway workload deployment` capability is configured or exercised
- **THEN** The deployment SHALL target Edge, Payments, fraud, notifier, vault simulator, processor simulator, Redis and Kafka in `payment-gateway` through the configured prod root, with ClusterIP services. One application chart and Argo CD child SHALL own the six Go services, including fraud, through a services map plus gateway CNPG, Valkey, secrets, routing and security resources; Kafka SHALL retain its separate chart and child. Fraud SHALL use the common service templates with its own Deployment and Service; no dedicated fraud chart or Argo CD child SHALL be created. Edge SHALL additionally have one Gateway API HTTPRoute on the cluster ingress gateway exposing only the hosted checkout paths `/checkout/*`, and the prod values SHALL publish that gateway host through the platform's Cloudflare ClusterTunnel `talos-proxmox` at `pay-gateway.phuchoang.sbs` so a marketplace buyer can reach the hosted page; the JSON API SHALL remain cluster-internal. It SHALL NOT restore a dev app-of-apps root. A CloudNativePG cluster owned by this repository SHALL provide separate databases and owner roles for Payments, notifier, vault and processor. Payments SHALL run migrations as its owner role and serve requests with a separate runtime role that has only `SELECT` and `INSERT` on the payment event ledger. No service's application credentials SHALL grant access to another service's database or logical replication. Application secrets SHALL come from External Secrets through the shared `fraud-doppler` ClusterSecretStore, whose namespace allow-list includes `payment-gateway` and whose bootstrap token lives in `operators`, not from the repository. Network policy SHALL default-deny vault/database/Redis access except the required callers, and SHALL allow Edge only from the ingress gateway and the namespaces of configured integrations. Redis SHALL be a standalone `Redis` resource of the platform's Redis operator (no sidecar), with bounded memory, `noeviction`, TTL-based idempotency records, a password from External Secrets and Payments-only access; Postgres remains the authoritative idempotency store. Internal gRPC SHALL be plaintext h2c secured by the Istio ambient mesh (STRICT mTLS and principal-based AuthorizationPolicy configured through the chart's `mesh.rules`), and workloads SHALL use native gRPC health probes. A repository-owned Kafka cluster, managed by the platform's Strimzi operator, SHALL run in `payment-gateway` with automatic topic creation disabled and `payments.events.v1` declared, and Kafka Connect SHALL publish only the Payments event table through dedicated CDC credentials granted by a Role scoped to that one Secret, excluding application credentials, private operation requests and vault data. Network policy SHALL admit Kafka clients only from the gateway namespace, and SHALL allow the notifier egress only to configured webhook destinations. Deleting the Kafka application SHALL NOT delete the Strimzi operator or its CRDs.

#### Scenario: Render gateway target

- **WHEN** the configured prod root is rendered
- **THEN** each workload, the database cluster, Kafka, Connect, the connector and the topic have prod-qualified identities, no dev root is created, and the only public surface is the Edge `/checkout/*` route, published through the Cloudflare tunnel

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

Same-repository PRs SHALL publish every service image with `pr-<number>-<full-sha>` using the built merge commit. Fork PRs SHALL build without publication. Main pushes SHALL publish `main-<full-sha>` and `latest`; feature-branch pushes SHALL NOT publish images.

#### Scenario: Publish PR images

- **WHEN** a same-repository PR is opened or updated
- **THEN** all service images are published with that PR's tag and no production image pin changes

### Requirement: Production image pinning

Only after every main service image has published successfully, one CI job SHALL pin the gateway, Kafka and live-traffic production chart tags to that commit's `main-<full-sha>`. PR and feature-branch runs SHALL NOT update production pins. The pin commit SHALL use `[skip ci]` so its image-only production-values update does not trigger rebuilds. Manual production-values changes SHALL trigger image builds normally. No image-updater controller SHALL be required.

#### Scenario: New main image

- **WHEN** every service image for a main commit has published successfully
- **THEN** one CI job commits the same main tag to all three production charts, with the gateway pin covering all six Go service images and Argo CD reconciles them

#### Scenario: PR image is newer

- **WHEN** a PR image is newer than the latest main image
- **THEN** production pins remain unchanged

### Requirement: PR image cleanup

On PR closure, cleanup SHALL cancel that PR's active image build and delete repository-linked GHCR versions carrying that PR's `pr-<number>-<full-sha>` tag, including versions with additional tags. Versions without a matching tag SHALL be retained. Cleanup SHALL NOT inspect production chart pins or require a service list.

#### Scenario: Shared package version

- **WHEN** a version has a closing PR tag and also a main, latest or another PR tag
- **THEN** cleanup deletes the version and all its tags

### Requirement: Single simulator workload

Deployment SHALL provide one opt-in single-replica simulator workload running traffic, labels and optional delivery faults from one image/configuration. It SHALL use the shared current environment and configured prod root without a dev root or duplicate source-specific topology. Delivery faults SHALL remain embedded; an independently enabled Python archiver SHALL provide landing.

#### Scenario: Render live workload

- **WHEN** simulation is enabled in the configured prod target
- **THEN** exactly one simulator workload is rendered using a strategy that prevents overlapping replicas, without a separate injector workload or new public route

#### Scenario: Simulation disabled

- **WHEN** simulation is disabled
- **THEN** the simulator workload is absent while existing gateway/Kafka services and completed data remain intact

### Requirement: Shared topics and Python Parquet landing

The Kafka application SHALL retain payments.events.v1 and declare payments.events.v1.faulty and fraud.labels.v1 with configured partitions/retention. S3/RustFS archival SHALL run as a Python workload producing historical-compatible decoded Parquet with transport metadata, preserving the authoritative event publisher, CDC Connect worker and notifier. Protobuf payloads SHALL NOT require Avro conversion or a registry.

#### Scenario: Render landing archiver

- **WHEN** landing is enabled
- **THEN** one Python archiver workload consumes selected payment topics and labels, with no S3 Connect plugin, Flink deployment or registry

### Requirement: Runtime credentials and image delivery

The simulator SHALL receive only its gateway/Kafka/dataset runtime credentials and SHALL NOT access vault/database internals. The archiver SHALL reuse the existing synthetic-source identity with streaming-prefix policy access, preserving completed historical source immutability without object tags, streaming no-delete/artifact restrictions and CDC/Secret API limits. The simulator and archiver SHALL use the same Python package/image with separate entry points and SHALL follow shared PR/main build and production-pin policies.

#### Scenario: Archiver permissions

- **WHEN** the Python archiver is configured
- **THEN** it receives the existing synthetic-source runtime credential, streaming-prefix access and object-store egress while historical immutability and CDC access remain constrained and the simulator gains no vault/database access

#### Scenario: Image publication

- **WHEN** CI builds same-repository PR, fork PR and main images
- **THEN** same-repository PRs publish disposable PR tags, fork PRs build without publication and main images follow the existing production selection policy
