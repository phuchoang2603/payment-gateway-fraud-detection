## Context

The existing Python generator already writes reproducible payment-event and label Parquet with bounded execution, resume and manifest-last publication. Its main training-data weaknesses are simplistic customer activity, account creation dates recalculated per purchase, rigid device/network patterns, conspicuous fraud scenarios and differences from gateway risk semantics.

## Goals / Non-Goals

**Goals:** a plausible US electronics ecommerce population with useful temporal/entity relationships, reliable labels, valid histories and clear generation provenance.

**Non-Goals:** simulate every real-world payment mechanism, change checkout collection/contracts, redesign live traffic, implement Flink/Feast or train a model. Reliable synthetic training data is the deliverable.

## Decisions

### 1. Improve the existing model

Keep the current Python package and publication flow. Fix account birth timestamps once per entity. Give customers different purchase frequencies, spending ranges and merchant/category preferences. Generate repeat visits, plausible category/condition/quantity/price combinations, simple sessions and weekend/seasonal variation. Retain shared devices/networks and occasional replacement or rotation without requiring a detailed household/demographic simulator.

Use deterministic entity/day random streams and bounded campaign windows so parallelism and resume do not change the records. Reuse helpers available to the live runner, but defer live scheduling, gateway API changes and deployment selection.

### 2. Local behavior, UTC storage

Assign synthetic customers a US region and an IANA timezone internally. Generate purchases in plausible local hours, then convert to UTC for storage using pinned timezone rules. DST changes must not create invalid or ambiguous event instants. UTC partitions remain UTC partitions.

The timezone is generation context, not a claim that checkout currently observes it. Do not add browser timezone/offset fields or expose hidden home zones as model features. Future collection/feature work can revisit that separately. [IANA tzdb](https://www.iana.org/time-zones) is the reference for timezone rules.

### 3. Make fraud learnable from behavior

Keep configurable stolen-card, account-takeover, card-testing and friendly-dispute scenarios. Legitimate customers can spend heavily, change devices or share networks; fraud can use ordinary amounts and familiar context. Use campaign timing and relationships across attempts/cards rather than reserved fingerprint/subnet formats or mandatory foreign-country markers. Keep obvious-high-value stress cases optional and separate from baseline realism.

Use the gateway's existing browser-family vocabulary and address/network fingerprint semantics where applicable. Keep IP country absent when the collection path cannot derive it; shipping country is not a substitute. Do not invent AVS/CVV, issuer, GPS or other uncollected signals.

### 4. Reliable histories and labels

Keep complete per-payment histories, same-payment retries and consistent IDs, item totals and timestamps. Align historical risk outcomes with the gateway's existing versioned policy rather than maintaining contradictory sim-rules-v1 behavior; this requires generator changes, not a gateway rewrite. Keep processor outcomes separate from fraud intent.

Retain simulation-truth labels per attempt and delayed chargebacks only for succeeded attempts. Reporting probability and delay are explicit parameters; friendly disputes can label legitimate successes. No chargeback yet is not a confirmed negative. Define label occurrence consistently from the generated attempt's business timestamp, schedule reporting after success, and record availability separately. Scenario/truth fields and future payment outcomes cannot be predecision features. This slice does not change live label scheduling.

### 5. Calibrate enough to support the dataset

Maintain a compact versioned calibration profile: source URL, reference period, reuse terms, checksum and which parameters it informs. Clearly mark assumptions where suitable measurements do not exist. Freeze inputs for generation; no internet request per transaction.

The checked [Federal Reserve diary](https://www.frbservices.org/news/research/2025-findings-from-the-diary-of-consumer-payment-choice) is broad US payment context, not an electronics checkout frequency estimate. [UCI Online Retail](https://archive.ics.uci.edu/dataset/352/online+retail) is an old UK gifts/wholesale population and is only a methodological reference. Choose usable US retail/calendar references during implementation; do not claim inaccessible or mismatched sources provide measured checkout parameters.

Produce one concise report of activity/amount/time patterns, device/network sharing, scenario proportions, retries and label maturity, with explicit payment/attempt/customer denominators. Inspect obvious generator shortcuts. These are diagnostics, not arbitrary model-score or statistical acceptance gates. Recommend a forward-time split with mature labels and an entity/campaign holdout to assess memorization; do not build an evaluation platform.

### 6. Publish a reliable new source

Use the current physical schema for the realism baseline. Forced schema cutovers and injected duplicate exports remain optional, explicitly named data-problem experiments rather than natural-traffic claims. Preserve deliveries and label linkage regardless of mode.

Use `historical/v13` only for a six-day, 100,000-customer smoke/reference export from July 1, 2025. Delete the stopped unpublished full-scale v13, including raw files and run metadata, before creating the smoke source. Remove the local full-scale configuration. The smoke configuration has no physical-byte floor and uses no object tags; preserve schema/integrity checks and immutable manifest-last publication. Preserve v8/v11/v12 unchanged. Do not run local automated smoke tests; dataset generation is an operational publication task, while tests remain in CI.

The measured workstation evidence remains in `/home/felix/.local/state/dataset-fixes/us-electronics-v1/`: a six-day 100,000-customer export produced about 61.5 MB / 618,000 rows in 47 seconds with four workers and about 804 MB aggregate RSS at progress samples. Use four workers/in-flight tasks/transfers, single-threaded Arrow/BLAS and an 8 GiB scratch cap. The superseded full-scale estimate was about 118 GB / 24–36 hours; its stopped run is not source acceptance evidence. Full-volume generation and the project >=100 GB requirement move to a separate distributed Spark generation job coordinated with the StarRocks-native DP1 thread, without changing that ingestion architecture in this slice.

Source publication and downstream DP1 Spark/GX quality acceptance remain separate. Enforce schema, integrity, conservation, lifecycle, linkage and leakage checks; keep generator distribution bands diagnostic under DP1's implemented policy. Reuse focused existing CI coverage, adding only meaningful missing cases; no local tests, config/getter tests, new proto-check script or new CI infrastructure.

## Risks / Trade-offs

- Public data cannot certify a real checkout's joint fraud distribution -> describe the source as plausible synthetic data, with explicit calibration limits.
- Artificially easy scenarios inflate model performance -> include legitimate/fraud overlap, inspect shortcuts and recommend temporal/grouped evaluation.
- Delayed reporting makes recent labels incomplete -> publish maturity information and keep oracle truth distinguishable from observed chargebacks.
- Regeneration can exceed workstation resources -> retain measured throughput and bounded smoke worker/scratch budgets; distribute the later full run separately.

## Migration Plan

1. Implement the generator improvements and obtain focused CI results, leaving live deployments unchanged.
2. Record calibration assumptions and smoke resource budgets; remove partial full-scale v13.
3. Generate/publish the immutable v13 smoke source and its report; do not claim full-volume training acceptance.
4. Hand the manifest/hash, date ranges, row totals and training limitations to DP1. Any live source switch is separate work under the existing identity/manifest checks.

## Authorized smoke exercise and source reset

Remove accepted-version configuration and all tag operations; keep completed-manifest/configuration/inventory integrity checks and manifest-last publication. Published manifests remain unchanged; runtime models project the supported configuration fields from immutable manifest snapshots. Preserve historical v8/v11/v12/v13.

Quiesce simulator/archiver through GitOps and confirm no affected writer is running before an admin-only deletion of the inventoried synthetic-source/streaming/ objects, versions and multipart uploads. Freeze Kafka high watermarks; use a new landing group and source epoch initialized explicitly at those offsets, with fault progress similarly bounded. Retain Kafka/shared assets. The writer receives no delete permission.

Exercise the published v13 reference model with current UTC gateway traffic, distinct live payment IDs and run provenance. Landing batches retain source epoch, schema versions, Kafka positions and exact transport bytes; retries preserve immutable files. Capture historical integrity/linkage/lifecycle/as-of/leakage evidence and live delivery/landing/recovery results before distributed Spark generation starts. Source checks do not establish downstream DP1 or full-volume training acceptance.
