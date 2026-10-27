## Implementation

- [x] 1. Extract shared connector configuration and registration/native-discovery runner; remove duplicate service registration from DP1.
- [x] 2. Add metadata-integrations chart, GitOps application, external credentials and discovery schedules; wire pipeline stages to shared configuration.
- [x] 3. Include gateway Postgres readers and Kafka topic discovery with metadata-only settings and database-owned permissions.
- [x] 4. Adapt existing behavioral fixtures, update affected operation docs and validate lint/schema/rendering locally; run tests/builds in existing CI only.
- [x] 5. Inspect CI images/checks and report explicit rollout prerequisites without rerunning DP1 or claiming unperformed live discovery.

Local verification: Nix devenv, Ruff, Helm lint/rendering and strict OpenSpec validation passed. The gateway reader password key was generated directly into consumer Doppler without changing existing keys. The unused registry reader key from the earlier scope remains in Doppler but is not referenced or deployed. All 31 checks on source commit 8e78dcf passed (30 successful, image pin intentionally skipped). CI build 40461418301 published metadata image sha256:52aa98f8509ffe8f6e4c58a5744e920a53bfc02d9d474e6ded6e33bf35a572f0, now pinned in both production charts. Database-owned reader grants and shared metadata registration must roll out before pipeline consumers switch; live connector readback remains unperformed. Follow-up CI must validate the image pin and component-local Secret configuration. Kubeflow connector and reader provisioning are excluded. No local tests/builds or pipeline runs were performed.
