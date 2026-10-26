## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Production image pinning

Only after every main service image has published successfully, one CI job SHALL pin the gateway, Kafka and live-traffic production chart tags to that commit's `main-<full-sha>`. PR and feature-branch runs SHALL NOT update production pins. The pin commit SHALL use `[skip ci]` so its image-only production-values update does not trigger rebuilds. Manual production-values changes SHALL trigger image builds normally. No image-updater controller SHALL be required.

#### Scenario: New main image

- **WHEN** every service image for a main commit has published successfully
- **THEN** one CI job commits the same main tag to all three production charts, with the gateway pin covering all six Go service images and Argo CD reconciles them

#### Scenario: PR image is newer

- **WHEN** a PR image is newer than the latest main image
- **THEN** production pins remain unchanged
