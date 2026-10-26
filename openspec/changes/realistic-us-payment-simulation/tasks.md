## 1. Calibration and behavior

- [x] 1.1 Add a compact versioned US calibration profile with source references and explicit assumptions; verify the profile/provenance distinguishes population measurements from approximations and participates in generation identity.
- [x] 1.2 Fix entity birth timestamps and improve repeat purchasing, activity/spend variation and baskets in the existing generator; verify fixed identities, money conservation and reproducibility through focused existing CI coverage.
- [x] 1.3 Schedule plausible regional/local-hour/weekend patterns using IANA rules while storing UTC; verify a meaningful DST boundary in existing generation CI coverage without adding checkout fields or changing gateway APIs.
- [x] 1.4 Improve device/network sharing, changes and missingness using existing collection semantics, removing scenario-specific visible shortcuts; verify diagnostic samples and affected existing identity/generation CI cases.

## 2. Fraud and dataset correctness

- [x] 2.1 Implement overlapping legitimate/fraud behavior and campaign patterns; verify configured scenario presence/absence and diagnostic amount/context overlap without imposing model-score gates.
- [x] 2.2 Align historical risk semantics with the existing gateway policy and preserve complete retries/unknown-resolution/expiry histories; verify meaningful policy/lifecycle boundaries using existing CI coverage.
- [x] 2.3 Add explicit chargeback reporting/delay and label-maturity accounting while preserving separate per-attempt truth; verify succeeded-attempt linkage, friendly disputes and availability ordering in existing label CI cases.
- [x] 2.4 Keep baseline realism on the current schema and make forced schema changes/duplicate exports explicit experiments; verify existing schema/delivery coverage, removing superseded generator behavior without a legacy compatibility path.

## 3. Verify and generate

- [x] 3.1 Obtain relevant CI results and run only non-test lint/format/schema checks locally; verify no new config/getter tests, proto-check script, broker CI infrastructure or image builds are run locally. All 29 checks passed on `9b0c8fa` (image pin skipped).
- [x] 3.2 Record measured generation/upload throughput and bounded smoke worker/scratch settings; retain superseded full-run estimates as evidence, with full-volume Spark generation outside this slice.
- [x] 3.3 Delete the unpublished partial v13 entirely, remove the local full-scale config and publish v13 as a small smoke/reference dataset through immutable manifest-last publication; verify deletion, receipts, manifest URI/hash and file/row/byte/date totals without modifying v8/v11/v12.
- [x] 3.4 Deliver the concise smoke report and DP1 handoff with diagnostics, source limitations and label maturity; explicitly exclude full-volume training acceptance, defer distributed Spark generation and downstream acceptance, and leave live deployments unchanged.


## 4. Tag-free publication and fresh streaming smoke

- [x] 4.1 Remove accepted-version/tag operations and affected config/tests/docs while retaining immutable publication and source integrity; obtain CI results without local tests/builds.
- [x] 4.2 Quiesce affected writers; inventory and delete only the owned streaming source, versions and multipart uploads; freeze/reset progress without deleting Kafka/shared assets or historical datasets.
- [x] 4.3 Exercise v13 historical integrity/linkage/lifecycle/as-of/leakage and current-time live behavior, delivery faults, landing and restart/recovery with explicit epoch/model provenance; report concrete results and limits.
- [x] 4.4 Push PR #89 evidence and hand off readiness to the StarRocks-native DP1 thread before any full-scale distributed generation.

### Fresh-source smoke evidence (October 26)

User-authorized admin cleanup deleted only `synthetic-source/streaming/`: 19 objects / 1,591,076 bytes and 19 versions, no multipart uploads; empty-prefix checks passed after both writers stopped. Historical v8/v11/v12/v13 manifest hashes stayed unchanged. Fresh landing/fault/recovery groups start at recorded Kafka high watermarks under epoch `v13-smoke-20261026`; no Kafka/shared data was deleted.

All 29 checks passed on runtime `7e483fb` and deployment pin `7a3117a` (pin job skipped); Python reported 76 passed / one opt-in Kafka test skipped. No local tests or image builds ran. The deployed shared image is `pr-89-8c30ea5dbba18acd742df40d6036c3d4bd27b2f3`.

Cluster v13 audit checked all 20 file hashes/schemas/rows/UTC partitions, profile digest, money conservation, ordered complete lifecycles, one truth label per attempt and chargeback linkage/availability: 98,728 payments, 101,128 attempts, 510,017 events and 102,826 labels, including 1,698 delayed chargebacks.

Current-time gateway sessions completed 90 baseline + 60 combined-fault payments, 750 authoritative events and 150 truth labels with zero failed loops and no historical payment-ID overlap. Fault forwarding preserved every clean bytes/key/ordered-header envelope: 1,123 deliveries / 373 extras, including delay and burst holds. After a bounded fault restart/drain, no pending records remained. Final landing audit read all 180 objects (90 Parquet files + 90 manifests, 1,942,691 bytes), verified every new Kafka position/transport envelope and business schema, with zero missing positions/mismatches/overlapping transport positions and all nine audited consumer positions caught up. Default archiver still selects clean payments plus labels; faulty-topic objects are explicit evidence.

An archiver restart preserved 108 completed objects in checksum/ETag/modification time. An intentional exit 91 after upload/before manifest advanced no offsets; recovery reused the original Parquet unchanged and completed 1,123 deliveries. The first temporary live Job lacked the simulator's allowed label/service account and completed zero payments; the corrected Job produced the successful baseline. These failures remain recorded.

Limits: 17 live payments lacked terminal events during bounded observation; no live chargebacks were observed, and one pending baseline chargeback was lost when the in-memory schedule stopped. Label delay scaling was explicitly accelerated by 86,400. Live truth occurrence records runner dispatch (up to 2.268042 seconds before the authoritative attempt here), with availability verified after the attempt; consumers join by identity and availability, not timestamp equality. Oracle fields remain labels-only; actual feature projections/as-of joins and downstream DP1 acceptance are not established. This is smoke/reference evidence, not full-volume or distribution-quality training acceptance. Temporary jobs/secrets/service account were removed; archiver remains healthy and simulator disabled. Full report and exact logs: `/home/felix/.local/state/live-simulator/pr89/v13-smoke/result.json`.
