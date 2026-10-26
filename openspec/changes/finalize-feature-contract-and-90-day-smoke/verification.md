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

## V16 replacement publication

All 29 checks passed at `5eed19ec287b368baccc656a37d41b5034c2e984`, including the generated-decision contract regression. The CI merge-image tag is `pr-90-b905998d1cf304f7ecae4db8bead3e0aa59a9447`.

V16 was generated from a frozen copy of that revision. Its published manifest is `s3://synthetic-source/historical/v16/manifest.json`, byte SHA-256 `719815e3f0a90e27bbc8303feaacdddb4858c604325656982fd443e49b05882e`. It contains 795,718 event rows and 161,967 label rows across 1,954 files / 139,597,476 physical bytes. The publication receipt confirms cleanup and canonical JSON hash `83d94df53eca351e275d684286fe4da5c6419a64fdf83f1fbbc9a50b2fad61a9`. The feature definition remains `fraud-baseline-v1` / SHA-256 `2ee2a5153a248bdc98df3b2643e10a65bc5693ea111129b6c65a67352aa4ec0b`.

The CI-image Kubernetes job `feature-contract-v16-validation` is checking the exact inventory and corrected source contract. This publication is not yet source or DP1 acceptance. Gateway images and the disabled live/archiver configurations are staged to the matched current CI tag.

V16 source validation **passed**: every inventory file passed SHA-256, physical bytes, footer row count, Arrow schema and UTC partition row accounting; all nine source SQL rules returned zero violations. The [complete development report](evidence/v16-development-report.json) includes actual cohort/range/split/overlap measurements and explicitly pending DP1/feature/model acceptance. The source-validation Job completed successfully and its logs were retained outside the repository. No full-scale source was generated.

## Current-time live and recovery acceptance

The CI-image Kubernetes Job `feature-contract-v16-live-acceptance` completed successfully. Run `02af49c81643432b9ef61a32aa304c08` completed all 240 admitted payments at two arrivals/second. Historical and live payment identities remained distinct, and all attempt contexts matched their authoritative checkout. Observed absent signals: ten devices, five network fingerprints and six user-agent families.

The [live report](evidence/v16-live-report.json) binds the exact source manifest, run ID, initial/final topic/partition offsets and fault/landing groups. It verified 193 completed batch manifests/files, 1,627 event deliveries / 1,199 distinct events, and 251 label deliveries including replay. The original run published 240 truth labels and one chargeback. Configured duplicate/delay/burst behavior was observed. Six manifests were published before an injected failure prevented offset acknowledgment; their bytes remained unchanged after restart, 49 Kafka positions were replayed, and every selected position landed. The original fault loop stopped with pending work; a second pass using the same group drained it before the final coverage check. No exactly-once claim is made.

One chargeback remained pending in the in-memory schedule at bounded shutdown. This remains an explicit live-scheduler limitation; source v16 includes its complete scheduled label tail. Live acceptance checks typed schema, preserved protobuf identities, context/linkage/availability, checksums and transport positions. CI covers representative full field/optional-presence projection; this does not establish online/offline feature parity or DP1 acceptance.

Current clean-topic archiving uses epoch `v16-20261026`, group `payments-parquet-landing-v16-20261026`; the [initial-offset receipt](evidence/v16-landing-start.json) was recorded after quiescing the old archiver and before the exercise. Re-enabling that archiver is staged through Argo CD; rollout/catch-up readback remains pending. The bounded simulator and fault-forwarding processes exited successfully.
