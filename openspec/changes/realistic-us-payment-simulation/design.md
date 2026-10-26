## Context

See proposal.md for motivation and scope. The simulator already has reusable profiles, deterministic daily schedules, historical Parquet publication, a current-time API runner and a Python protobuf-to-Parquet archiver. Reuse these boundaries rather than introducing more services.

Observed gaps are concrete: historical account creation timestamps are calculated relative to each purchase; live timestamps are anchored to the dataset start. Live customers are selected uniformly, browser values differ between paths, and scenario-specific device/network choices and forced foreign countries expose fraud shortcuts. The historical risk rules also differ from the gateway policy. The existing product is electronics ecommerce, not a representative sample of every consumer payment.

## Goals / Non-Goals

**Goals:** build a reproducible, source-informed US electronics ecommerce population; share behavioral and observation semantics across history and live traffic; preserve bounded generation and current-time operation; distinguish assumptions from measured calibration.

**Non-Goals:** reproduce a real bank's joint fraud distribution, collect personal browsing/payment records, expand to other markets, introduce GeoIP infrastructure, train a model, or implement DP1 multi-source ingestion. Preserve the existing physical-volume requirement unless separately changed.

## Decisions

### 1. Versioned offline calibration, with explicit limits

Use compact calibration profiles in the simulator package, backed by immutable source snapshots or retrievable source references and hashes. Record publisher, URL, reference period, retrieval date, reuse terms, extraction method and applicable population for each parameter. Separate measured values, transferred approximations and scenario assumptions. Include calibration identity in logical generation identity and manifests.

The checked [Federal Reserve 2025 Diary findings](https://www.frbservices.org/news/research/2025-findings-from-the-diary-of-consumer-payment-choice) can inform US payment/channel context, but cover broad consumer payments observed in October 2024. Do not convert its all-payment frequency directly into electronics checkout frequency. The checked [UCI Online Retail dataset](https://archive.ics.uci.edu/dataset/352/online+retail), CC BY 4.0, is a 2010-2011 UK gifts/wholesale retailer; it is a methodological reference for basket/repeat-purchase analysis, not the US calibration population. Select suitable US retail seasonality, regional population and browser/device aggregate releases during calibration; retain unsupported fraud, network-sharing and reporting parameters as explicit assumptions rather than inventing empirical backing.

Prefer public aggregates and synthetic sampling to copying customer-level records. No generation-time internet dependency; refreshed calibration means a new logical generation identity. A single imported fraud dataset would be simpler, but would not reproduce our checkout population or observation boundary.

### 2. Shared deterministic behavior with stable entity timelines

Extend existing profiles/schedules/behavior modules. Profiles hold fixed birth timestamps, household membership, merchant/category preferences, spending/activity propensities and home region/time zone. Deterministic dated changes describe device replacement, fingerprint resets, domestic travel, shared household connections and rotating mobile networks. Model sessions and campaign bursts explicitly instead of independent uniform purchases. Keep customer and merchant IDs stable across all days and continuation.

Use entity/date/stage-scoped random streams, indexed dated transitions and bounded campaign windows so requesting a day does not require replaying the full history. Worker count and task order must not affect outputs. Both historical and live runners consume this same behavioral schedule; a live run's bounded target throughput scales arrivals while preserving relative customer, time-of-day and campaign behavior. Live starts at machine time with gateway timestamps, without a calendar-gap limit or fabricated catch-up.

US-only means US customers/merchants, USD and domestic location variation in this release. Remove active foreign-country scenario branches. Preserve the existing electronics category scope; source-backed prices and seasonality must identify their population and limitations.

### 3. Separate hidden state from observable checkout signals

Keep latent household/device/network identities inside generation state. Produce observed browser fingerprints with sharing, resets and missingness matching checkout collection. Use the gateway's browser-family vocabulary and fingerprint/network normalization semantics in both paths. The existing simulated-client boundary supplies generated observations; it must not inject oracle-only knowledge. Calibrate observation loss independently from fraud intent, with documented scenario-dependent behavior only where justified.

Shipping location remains order data. IP country stays absent where the deployed collector has no GeoIP enrichment; it must not be inferred from shipping country. Domestic movement can still affect modeled network reuse and purchase timing without adding a fabricated location feature. Raw IP values and addresses remain generated and must not cause outbound traffic to those addresses. Prefer existing optional fields to a contract expansion; any unavoidable breaking schema change must update protobuf, Parquet mapping and affected consumers together.

### 4. Fraud intent, decisions and evidence are separate

Retain stolen-card, account-takeover, card-testing and friendly-dispute experiments, but generate overlapping legitimate/fraud amounts, familiar/new devices and network reuse. Keep an explicitly enabled obvious-fraud stress scenario outside realism claims. Campaigns coordinate attempts across cards/devices/merchants without reserved identifier ranges visible as fraud indicators.

Fraud intent is hidden truth, not the gateway decision. Historical risk outcomes must use the same versioned policy inputs and semantics as the gateway; Python does not import Go domain code, so parity is protected at the contract boundary with a small shared set of behavioral examples. Processor success/decline/unknown behavior remains a distinct mechanism. Live events remain gateway-produced.

Retain simulation-truth labels per attempt in the separate label product. Model chargeback reporting probability, delay and right censoring separately: no chargeback yet does not establish a negative, and a legitimate purchase can receive a friendly-dispute label without changing the underlying intent. Only succeeded attempts can receive chargebacks. Use explicit observed-label versus oracle-target selections in downstream handoff; oracle/scenario fields never become features. Live availability is actual publication time. Pending in-memory labels remain honestly reported; durable scheduling is not silently promised by this redesign.

### 5. New immutable generation with coordinated selection

Use a new algorithm/profile identity and an unused dataset version discovered at implementation time. Remove superseded runtime branches instead of preserving old-algorithm support. Keep old datasets and their accepted/failed evidence intact. Vault synthetic-card mapping must cover the newly generated method identities and behavior assignments before live selection; a shared count alone is insufficient.

Retain historical manifest-last Parquet publication and the existing archiver's protobuf decoding, raw transport provenance and immutable streaming completion manifests. This plan does not alter their recovery/verification policies. Keep distribution/cardinality comparisons diagnostic and integrity, schema, conservation, lifecycle-context, linkage and leakage enforced; source publication remains separate from downstream DP1 acceptance.

Measure a bounded generation calibration using the new algorithm before full generation: live machine CPU/available memory/disk, aggregate worker memory, scratch peak, rows/bytes per second, compressed output and upload throughput. Record estimated full-run duration/storage and resource budgets. The former calendar rewrite is not a generation benchmark. This is operational sizing, not a new local test suite; behavioral tests and image builds run in CI.

## Risks / Trade-offs

- Public aggregates cannot identify the joint ecommerce fraud distribution -> publish assumptions and sensitivity ranges; describe the result as source-informed synthetic data, not production-validated fraud data.
- Shared networks and observed fingerprint collisions increase linkage ambiguity -> retain stable business IDs and hidden identity truth separately; never treat one observed fingerprint as a unique person.
- More realistic overlap may reduce apparent model/rule performance -> evaluate the combined prefilter/model population later; do not restore shortcuts to improve scores.
- Calendar and behavior correlations increase computation -> use bounded indexed timelines, explicit resource admission and measured sizing before full generation.
- Replacing the active model breaks old live manifest reconstruction -> coordinate the image, vault and manifest switch, retaining the prior image/config for rollback rather than an in-process compatibility layer.

## Migration Plan

1. Implement and review calibration/behavior changes with focused CI coverage; leave current source selection and deployments unchanged.
2. Measure resource needs, reserve an unused version and generate under the existing bounded/resumable publication flow. Preserve published v8/v11/v12.
3. Publish lineage, counts, date/label ranges, calibration assumptions and source checks. Request separate DP1 quality acceptance; do not relabel old evidence.
4. Coordinate vault mapping, simulator image and explicit manifest/hash selection. Exercise live behavior through the gateway and existing archiver, reporting unfinished labels and any observation differences.
5. Roll back by restoring the previous image/config selection without rewriting either source dataset. DP1 multi-source consumption remains owned by its follow-up slice.
