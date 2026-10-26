# Verification in progress

Implementation branch: `feat/feature-contract-90-day-smoke`, [PR #90](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/90). Source generation is not downstream DP1 acceptance. No scale generation is running.

The complete implementation worktree was recovered from local main `261918e067bebbfa3ce2dc742786b8a9c54b1f43`; surviving content was backed up outside the repository. Three pre-existing production image overrides remain excluded from this change. No recorded v14 writer was active. The operator reports that old sources were removed; no compatibility readers or additional source deletions are implemented.

## Contract and reference checks

At `187a873`, [Python CI](https://github.com/phuchoang2603/payment-gateway-fraud-detection/actions/runs/40392809040) passed the existing simulator/landing tests and the shared feature conformance cases. [Contract and Helm CI](https://github.com/phuchoang2603/payment-gateway-fraud-detection/actions/runs/40392808794) passed. The live-simulator image built successfully. Go CI caught a test variable shadowing the context package and stale vault rejection fixtures after expanding the synthetic encoding domain. Both were corrected at `760fc6d`; the subsequent `021e37c` run passed all 29 checks, including the corrected Go coverage and image builds. The earlier failures are not passes.

Local non-test checks passed: native ODCS/ODPS lint and reference export comparison, Python lint/format, OpenSpec validation, notebook code-cell syntax, feature-definition JSON Schema validation and diff whitespace checks. No tests or image builds ran locally.

## Development source

Before generation, the synthetic writer listed zero objects under `s3://synthetic-source/historical/v15/`. Config schema parsing confirms 90 purchase-start dates from July 1 through September 28, 2025, 10,000 customers and three workers. Fresh machine inspection found four cores, 11 GiB total RAM / 8.2 GiB available and 35 GiB free disk. Scratch is capped at 8 GiB, with four in-flight tasks and four transfer threads.

The completed v15 operational generator ran from a frozen committed source copy under `/home/felix/.local/state/dataset-fixes/feature-contract-v15/`, with private Doppler credentials and progress/resource logs. V15 publication/source results are recorded below; replacement v16 publication and current-time deployed live/archiver recovery evidence remain pending. Current chart source selection will change only after successful publication and source checks.

## First candidate failure and replacement

All 29 checks passed at `021e37c`. The associated CI image tag uses the PR merge SHA: `pr-90-f8cae5148c3d8e8989c00757f00f49d885574bce`, not the head SHA. A source-validation job initially could not pull a head-SHA tag; it never started and was replaced with the actual CI image.

V15 published 795,718 event rows and 161,967 label rows across 1,954 files / 139,596,351 physical bytes. Manifest SHA-256: `5aa667564df3beb0ac9189856d9dd0d501ee57d3f907cabc990af5e71c234310`. The publication receipt uses canonical JSON SHA-256 `44b30a750d4208a592d6738ab88b466860c32a88de013a2492f531e39aebacb8`; this is intentionally distinct from the serialized manifest-byte hash. Cleanup completed.

The Kubernetes job verified every inventory file checksum, byte/row count, Arrow shape and UTC date partition. Existing contract SQL found **155,620 risk-assessment violations**: the contract incorrectly disallowed empty reason lists on clean approvals. Integrity, envelope, lifecycle, attempt identity, amount, context equality, label availability and linkage rules returned zero violations. V15 is **not accepted**; its manifest/contract hashes and failed outcome remain unchanged. Contract 4.0.1 corrects the rule to require reasons for declines while allowing clean approvals. A focused existing-suite regression case now exercises the rule against generated records. V16 is the replacement candidate; no scale run is started.

Argo CD now selects this branch for the gateway and live-traffic applications. The old archiver was pruned and zero archiver pods remained before the current-contract cutover. Gateway image rollout uses the verified CI merge-SHA tag above. Both live traffic and archiving remain disabled while the replacement source is pending. No shared Kafka history or source objects were deleted.

The [manifest-bound v15 report](evidence/v15-failed-development-report.json) preserves the failed check, measured time ranges, class/split counts and overlap limits. Labels extend through 2025-10-19; all nine source SQL rules completed, with only the risk-assessment rule failing. The next candidate prefix `historical/v16/` was confirmed empty before generation; the next run remains gated on corrected-contract CI.
