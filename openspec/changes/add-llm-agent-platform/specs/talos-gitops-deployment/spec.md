## ADDED Requirements

### Requirement: LLM platform application
The app-of-apps SHALL deploy chart `infra/charts/llm-platform` as application `llm-platform` into the platform-owned `ml` namespace. It SHALL be enabled in dev and disabled in prod until the user explicitly confirms enabling it. Its credentials SHALL come from `gateway-doppler`. It SHALL consume the platform's KServe, agentgateway, kagent, agentregistry and Agent Substrate controllers and CRDs, and SHALL NOT install, adopt or delete them, their namespaces or the GPU NodePool.

#### Scenario: Render both environments
- **WHEN** the dev and prod roots are rendered
- **THEN** dev contains the `llm-platform` Application and prod does not

#### Scenario: Remove the application
- **WHEN** the `llm-platform` application is deleted
- **THEN** shared controllers, CRDs, GatewayClasses, NodePools and the `ml` namespace remain
