## Implementation

- [x] 1. Extract shared connector configuration and registration/native-discovery runner; remove duplicate service registration from DP1.
- [x] 2. Add metadata-integrations chart, GitOps application, external credentials and discovery schedules; wire pipeline stages to shared configuration.
- [x] 3. Include gateway/model-registry Postgres readers and Kafka topic discovery with metadata-only settings and database-owned permissions.
- [x] 4. Adapt existing behavioral fixtures, update affected operation docs and validate lint/schema/rendering locally; run tests/builds in existing CI only.
- [ ] 5. Inspect CI images/checks and report explicit rollout prerequisites without rerunning DP1 or claiming unperformed live discovery.

Local verification: Nix devenv, Ruff, Helm lint/rendering and strict OpenSpec validation passed. Reader passwords, the new CI image digest and live connector readback remain rollout prerequisites. No local tests/builds or pipeline runs were performed.
