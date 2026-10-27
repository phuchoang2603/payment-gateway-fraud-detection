## Implementation

- [x] 1. Extract shared connector configuration and registration/native-discovery runner; remove duplicate service registration from DP1.
- [x] 2. Add metadata-integrations chart, GitOps application, external credentials and discovery schedules; wire pipeline stages to shared configuration.
- [x] 3. Include Kafka topic discovery with metadata-only settings and scoped network access; exclude operational databases and Kubeflow.
- [x] 4. Adapt existing behavioral fixtures, update affected operation docs and validate lint/schema/rendering locally; run tests/builds in existing CI only.
- [x] 5. Inspect CI images/checks and report explicit rollout prerequisites without rerunning DP1 or claiming unperformed live discovery.

Local verification: Nix devenv, Ruff, Helm lint/rendering and strict OpenSpec validation passed. Both newly introduced reader password keys were deleted from consumer Doppler and absence verified; all operational database connector/provisioning changes were removed. All 31 checks on source commit 8e78dcf passed (30 successful, image pin intentionally skipped). CI build 40461418301 published metadata image sha256:52aa98f8509ffe8f6e4c58a5744e920a53bfc02d9d474e6ded6e33bf35a572f0, now pinned in both production charts. Shared metadata registration must roll out before pipeline consumers switch; live connector readback remains unperformed. Final CI on 36de3cd passed (30 successful, image pin intentionally skipped). Follow-up CI must validate removal of operational database scope. Kubeflow connector and reader provisioning are excluded. No local tests/builds or pipeline runs were performed.
