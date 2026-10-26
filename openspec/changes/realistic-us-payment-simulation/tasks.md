## 1. Calibration and behavior

- [ ] 1.1 Add a compact versioned US calibration profile with source references and explicit assumptions; verify the profile/provenance distinguishes population measurements from approximations and participates in generation identity.
- [ ] 1.2 Fix entity birth timestamps and improve repeat purchasing, activity/spend variation and baskets in the existing generator; verify fixed identities, money conservation and reproducibility through focused existing CI coverage.
- [ ] 1.3 Schedule plausible regional/local-hour/weekend patterns using IANA rules while storing UTC; verify a meaningful DST boundary in existing generation CI coverage without adding checkout fields or changing gateway APIs.
- [ ] 1.4 Improve device/network sharing, changes and missingness using existing collection semantics, removing scenario-specific visible shortcuts; verify diagnostic samples and affected existing identity/generation CI cases.

## 2. Fraud and dataset correctness

- [ ] 2.1 Implement overlapping legitimate/fraud behavior and campaign patterns; verify configured scenario presence/absence and diagnostic amount/context overlap without imposing model-score gates.
- [ ] 2.2 Align historical risk semantics with the existing gateway policy and preserve complete retries/unknown-resolution/expiry histories; verify meaningful policy/lifecycle boundaries using existing CI coverage.
- [ ] 2.3 Add explicit chargeback reporting/delay and label-maturity accounting while preserving separate per-attempt truth; verify succeeded-attempt linkage, friendly disputes and availability ordering in existing label CI cases.
- [ ] 2.4 Keep baseline realism on the current schema and make forced schema changes/duplicate exports explicit experiments; verify existing schema/delivery coverage, removing superseded generator behavior without a legacy compatibility path.

## 3. Verify and generate

- [ ] 3.1 Obtain relevant CI results and run only non-test lint/format/schema checks locally; verify no new config/getter tests, proto-check script, broker CI infrastructure or image builds are run locally.
- [ ] 3.2 Inspect current machine resources and measure bounded generation/upload throughput; deliver full-run time/storage estimates and worker/scratch settings for a fixed unused-version/date/scale plan retaining the existing physical-volume target.
- [ ] 3.3 Generate/resume and publish through the existing immutable manifest-last flow; verify publication receipts, manifest URI/hash, event/label ranges and file/row/byte totals without modifying v8/v11/v12.
- [ ] 3.4 Deliver the concise dataset report and DP1 handoff with distribution/shortcut diagnostics, source limitations, label maturity and temporal/entity/campaign split guidance; verify source checks and downstream Spark/GX acceptance are reported separately, leaving live deployment and feature pipelines unchanged.
