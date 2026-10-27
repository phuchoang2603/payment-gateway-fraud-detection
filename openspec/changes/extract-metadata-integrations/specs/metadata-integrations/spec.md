## Purpose

Make shared service metadata discoverable through Git-managed connections independently of individual data-pipeline executions.

## ADDED Requirements

### Requirement: Shared connector ownership
The metadata integration deployment SHALL declare canonical service connections, external credential references and bounded native discovery schedules through GitOps. Pipeline runs SHALL reference the same connections without independently registering competing services. Registration SHALL be idempotent and preserve existing canonical identities and product-owned assets.

#### Scenario: Add a service
- **WHEN** an enabled supported connector is added in Git with valid credentials
- **THEN** Argo CD reconciles its configuration and registration execution and native discovery can catalog its assets independently of DP1

### Requirement: Operational service discovery
Gateway Postgres databases and Kafka SHALL have explicit connector configuration and scoped access. Discovery SHALL NOT require a data product, consume Kafka payloads, enable sampling/profiling or imply complete application lineage. Database-owning deployments SHALL provision reader permissions; catalog execution SHALL NOT use database-owner credentials.

#### Scenario: Inspect operational assets
- **WHEN** native discovery completes for an operational service
- **THEN** its tables or topics are cataloged without being advertised as accepted analytical products or business-quality results

### Requirement: Explicit rollout prerequisites
Missing reader credentials or an unavailable matched runtime SHALL prevent successful registration/discovery rather than silently skip a required connection. Configuration validation and CI SHALL remain separate from live acceptance, and extraction SHALL preserve existing DP1 evidence and immutable sources.

#### Scenario: Reader secret is missing
- **WHEN** a discovery pod cannot load the configured reader credentials
- **THEN** its execution cannot report success and existing accepted pipeline evidence remains unchanged
