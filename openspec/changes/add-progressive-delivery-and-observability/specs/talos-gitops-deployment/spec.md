## MODIFIED Requirements

### Requirement: Shared platform ownership isolation

Synchronizing or deleting this repository's applications SHALL NOT create, adopt, or delete shared observability infrastructure, its operators, or its namespace. The application deployment SHALL NOT require CRDs or data sources from the removed observability stack and SHALL NOT install an application-owned collector or telemetry store. Application-owned namespace-local dashboard ConfigMaps, metric scrape definitions and alert rules SHALL use the current platform's shared discovery and data sources. Shared rollout controllers, CRDs and traffic-router plugins SHALL remain platform-owned.

#### Scenario: Remove a payment-gateway application

- **WHEN** its application-owned resources are removed
- **THEN** its dashboard, scrape and alert definitions are removed while the platform's telemetry collectors, stores, UI and rollout controllers remain under the platform owner's control

#### Scenario: Render fraud deployment during the transition

- **WHEN** the dev and prod application configurations are rendered
- **THEN** neither requires resources from the removed observability stack
- **AND** the Go fraud workload exports enabled application measurements over OTLP to the shared endpoints without exposing the replaced Python metrics listener
