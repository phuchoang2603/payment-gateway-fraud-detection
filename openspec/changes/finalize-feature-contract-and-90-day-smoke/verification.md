# Verification in progress

Implementation branch: `feat/feature-contract-90-day-smoke`, [PR #90](https://github.com/phuchoang2603/payment-gateway-fraud-detection/pull/90). Source generation is not downstream DP1 acceptance. No scale generation is running.

The complete implementation worktree was recovered from local main `261918e067bebbfa3ce2dc742786b8a9c54b1f43`; surviving content was backed up outside the repository. Three pre-existing production image overrides remain excluded from this change. No recorded v14 writer was active. The operator reports that old sources were removed; no compatibility readers or additional source deletions are implemented.

## Contract and reference checks

At `187a873`, [Python CI](https://github.com/phuchoang2603/payment-gateway-fraud-detection/actions/runs/40392809040) passed the existing simulator/landing tests and the shared feature conformance cases. [Contract and Helm CI](https://github.com/phuchoang2603/payment-gateway-fraud-detection/actions/runs/40392808794) passed. The live-simulator image built successfully. Go CI caught a test variable shadowing the context package and stale vault rejection fixtures after expanding the synthetic encoding domain. Both were corrected at `760fc6d`; current-head Go checks and image builds remain pending. The earlier failures are not passes.

Local non-test checks passed: native ODCS/ODPS lint and reference export comparison, Python lint/format, OpenSpec validation, notebook code-cell syntax, feature-definition JSON Schema validation and diff whitespace checks. No tests or image builds ran locally.

## Development source

Before generation, the synthetic writer listed zero objects under `s3://synthetic-source/historical/v15/`. Config schema parsing confirms 90 purchase-start dates from July 1 through September 28, 2025, 10,000 customers and three workers. Fresh machine inspection found four cores, 11 GiB total RAM / 8.2 GiB available and 35 GiB free disk. Scratch is capped at 8 GiB, with four in-flight tasks and four transfer threads.

The operational generator runs from a frozen committed source copy under `/home/felix/.local/state/dataset-fixes/feature-contract-v15/`, with private Doppler credentials and progress/resource logs. Publication, manifest/hash/row totals, source validation, the development report and current-time deployed live/archiver recovery evidence remain pending. Current chart source selection will change only after successful publication and source checks.
