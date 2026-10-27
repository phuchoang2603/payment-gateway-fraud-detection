## Purpose

Make shared service metadata discoverable through Git-managed connections independently of individual data-pipeline executions.

## ADDED Requirements

### Requirement: Shared connector ownership
The metadata integration deployment SHALL declare canonical service connections, external credential references and bounded native discovery schedules through GitOps. Pipeline runs SHALL reference the same connections without independently registering competing services. Registration SHALL be idempotent and preserve existing canonical identities and product-owned assets.

#### Scenario: Add a service
- **WHEN** an enabled supported connector is added in Git with valid credentials
- **THEN** Argo CD reconciles its configuration and registration execution and native discovery can catalog its assets independently of DP1

### Requirement: Operational service discovery
Kafka SHALL have explicit connector configuration, scoped broker network access and a connection to the deployed schema registry for registered topic schemas. Operational databases and Kubeflow SHALL remain outside this change. Discovery SHALL NOT require a data product, consume Kafka payloads, enable sampling/profiling or imply complete application lineage. Kafka discovery SHALL NOT require operational database credentials.

#### Scenario: Inspect operational assets
- **WHEN** native discovery completes for an operational service
- **THEN** its tables or topics are cataloged without being advertised as accepted analytical products or business-quality results

#### Scenario: Discover a registered topic schema
- **WHEN** an allowlisted topic has a registered value schema in the configured registry
- **THEN** native discovery uses that registry to retrieve schema metadata without changing producer encoding, consuming message payloads or registering schemas itself

### Requirement: Explicit rollout prerequisites
Missing configured connector credentials or an unavailable matched runtime SHALL prevent successful registration/discovery rather than silently skip a required connection. Configuration validation and CI SHALL remain separate from live acceptance, and extraction SHALL preserve existing DP1 evidence and immutable sources.

#### Scenario: Connector secret is missing
- **WHEN** a discovery pod cannot load the configured connector credentials
- **THEN** its execution cannot report success and existing accepted pipeline evidence remains unchanged
