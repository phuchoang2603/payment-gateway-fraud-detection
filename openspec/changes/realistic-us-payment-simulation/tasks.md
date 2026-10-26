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
