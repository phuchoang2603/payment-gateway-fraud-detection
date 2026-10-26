# Tasks

All tests and image builds run in CI; smoke/acceptance validation runs in CI or CI-driven deployed jobs. Local checks are limited to non-test schema validation, formatting/lint and diff review. Reuse existing suites, without config-only suites, a Python-proto check script or new mandatory Kafka infrastructure in generic CI.

## 1. Contract and gateway producer

- [x] 1.1 Restore/select a valid complete implementation worktree without discarding these artifacts or unrelated work; record its revision, inspect coordinated DP1 changes and confirm no active v14 writer before any source operation. Verify Git status, required contracts/docs and writer identity; do not resume/delete v14.
- [x] 1.2 Add shared immutable AttemptContext to the existing AttemptStarted protobuf and assign the explicit schema-3 shape; regenerate Go/Python bindings and representative fixtures through existing tooling. Verify cross-language context/optional-field preservation in existing CI coverage without a standalone proto-check script.
- [x] 1.3 Update Payments domain/application/wire and persisted event mapping to capture context from committed state; retain durable start/idempotency -> Fraud -> durable decision -> processor ordering. Verify context consistency and retry/crash behavior through existing gateway CI tests, keeping Fraud rules outcomes unchanged.
- [x] 1.4 Update ODPS/ODCS versions, physical schema declarations and generated references for current attempt context; update affected payment-flow/architecture docs. Verify native contract lint/reference checks in CI and review that documented fields/order match the new contract.

## 2. Shared feature definitions

- [x] 2.1 Add fraud-baseline-v1 definitions with exact feature names/types/units, scoped keys, windows, lifecycle eligibility, q/t cutoffs, missingness/freshness and definition hashing. Verify a complete reviewed artifact and schema validation without adding a generic feature DSL or runtime dependency.
- [x] 2.2 Implement the small framework-independent reference computation and shared conformance vectors for boundaries, duplicate/conflicting content, reordered confirmation, current-attempt exclusion, unavailable state and integration separation. Verify expected outputs in focused existing Python CI coverage; do not implement Spark/Flink/Feast jobs.
- [x] 2.3 Update data/ML pipeline guidance with the baseline definitions, assessed-attempt semantics, unbounded stream/bounded state distinction, training availability and future served-vector evidence. Verify that maintained docs link the canonical definitions and do not imply current online/offline parity or strict concurrent velocity.

## 3. Generator and typed landing adoption

- [x] 3.1 Update generated lifecycle records, current Arrow/Parquet schema and relevant generator/landing validation for enriched attempts. Remove superseded current-schema branches/config switches while leaving published sources untouched; verify generation/decoding/nullability/row preservation in existing Python CI cases.
- [x] 3.2 Update live reference selection and vault mapping assumptions to use the selected supported profile rather than fixed v8/v12 cardinality. Verify profile-digest mismatch refusal and token/fingerprint mapping in existing CI coverage; keep gateway APIs authoritative and payment IDs run-scoped.
- [x] 3.3 Update historical/live source guides, affected deployment/run instructions and reference notebook defaults for the current contract; retain old published-version facts. Verify links, matching schema/version names, optional signals and absence of stale active selection or legacy compatibility instructions by diff/reference review.

## 4. Ninety-day development source

- [x] 4.1 Replace maintained smoke defaults with the proposed 90-day/10,000-customer US/USD profile, explicit resource budgets, current schema and zero scale-byte minimum; reserve an unused version provisionally v16 without touching v13/v14. Verify config schema and remote version/ownership absence as non-test checks, and document exact settings in the source guide.
- [x] 4.2 Generate the new source using the existing bounded Python operational job with private environment credentials and progress/resource logs. Verify manifest-last completion, inventory hashes/rows/bytes, publication receipt and complete lifecycle/label tails; record actual manifest URI/hash without claiming DP1 acceptance.
- [x] 4.3 Exercise the new source with CI-driven contract/source validation, reusing existing validation paths for context equality, identity/content, lifecycle, linkage, label availability and leakage. Verify machine-readable results against the exact manifest; report unavailable downstream checks as pending rather than replacing downstream DP1 acceptance.
- [x] 4.4 Produce a compact manifest-bound development report with class/scenario/cohort diagnostics, observed time ranges, 7-day warmup, maturity-aware chronological split guidance and entity/campaign overlap. Verify usable positive/negative counts and actual-versus-estimated measurements; update the maintained source guide with results and limits, without a model-quality claim.

## 5. Current-time live and archival exercise

- [x] 5.1 Deploy matched CI-built gateway/simulator/archiver revisions; where cutover requires it, quiesce owned writers and record frozen offsets/new epoch/group before restarting. Verify shutdown/rollout and explicit epoch selection, preserving shared Kafka history, old objects and unrelated consumers.
- [x] 5.2 Run a bounded CI-driven Kubernetes live exercise pinned to the new manifest/profile/contract identity; cover checkout context, nullable signals and actual label availability. Verify gateway-produced events and historical-compatible Parquet fields/provenance; update current live instructions with run/epoch evidence and keep historical/live payment IDs distinct.
- [x] 5.3 Exercise configured delay/duplicate/burst transport and archiver recovery after publication before offset acknowledgment. Verify immutable completed objects, Kafka position recovery and reported overlap in deployed acceptance evidence; reuse current fault/landing harnesses rather than adding another service.

## 6. Roadmap alignment and handoff

- [x] 6.1 Update affected README entry links, overview requirements/architecture/roadmap and data/ML pipeline guidance to order contract/smoke -> Flink/Feast -> Spark offline -> Kubeflow -> KServe -> later scale generation. Verify consistent stage dependencies and truthful implemented/planned/pending statuses; reconcile the owning DP1 storage direction without changing historical evidence or removing coursework targets.
- [x] 6.2 Reconcile remaining affected current docs/config/code references and delta specs, including retired sorted/compacted-source guidance, hard-coded legacy card counts and instructions to start scale now. Verify targeted reference search/diff review; preserve archived records, research and published manifests rather than rewriting their results.
- [x] 6.3 Verify the exact new source and streaming evidence inventory, CI results, documentation links and explicit pending downstream gates. Deliver manifest/hash, rows/bytes/date ranges, source epoch/offsets, feature-definition identity and acceptance limits for subsequent pipeline work; do not start scale or claim a model was trained.

## Workflow follow-up

- After implementation and actual source/live evidence are reviewed, sync these deltas and archive this change through the normal OpenSpec workflow.
- Use separate implementation changes for Flink/Feast, Spark ingestion/curation/features and Kubeflow/KServe, each consuming this pinned contract and smoke evidence. Full-scale Python generation follows the exercised end-to-end path.
